from __future__ import annotations

import uuid
from dataclasses import dataclass, field
from typing import Any

from .data_sources import normalize_symbol
from .metrics import calculate_metrics
from .storage import DataPortal
from .strategy import Context, Strategy


@dataclass
class Position:
    symbol: str
    quantity: int = 0
    avg_cost: float = 0.0
    avg_price: float = 0.0


@dataclass
class Trade:
    trade_date: str
    symbol: str
    side: str
    quantity: int
    price: float
    amount: float
    commission: float
    tax: float
    reason: str = ""
    pnl: float = 0.0
    pnl_pct: float = 0.0
    price_return: float = 0.0
    name: str = ""


@dataclass
class Portfolio:
    cash: float
    positions: dict[str, Position] = field(default_factory=dict)

    def position(self, symbol: str) -> Position:
        if symbol not in self.positions:
            self.positions[symbol] = Position(symbol=symbol)
        return self.positions[symbol]

    def market_value(self, prices: dict[str, float]) -> float:
        return sum(pos.quantity * prices.get(symbol, 0.0) for symbol, pos in self.positions.items())

    def total_value(self, prices: dict[str, float]) -> float:
        return self.cash + self.market_value(prices)


class BacktestEngine:
    def __init__(self, portal: DataPortal, strategy: Strategy, config: dict[str, Any]) -> None:
        self.portal = portal
        self.strategy = strategy
        self.config = self._normalize_config(config)
        self.portfolio = Portfolio(float(self.config["initial_cash"]))
        self.trades: list[Trade] = []
        self.equity_curve: list[dict[str, Any]] = []
        self.positions_history: list[dict[str, Any]] = []
        self.symbol_names: dict[str, str] = {}

    def run(self) -> dict[str, Any]:
        symbols = self.config["symbols"]
        calendar = self.portal.get_trade_calendar(symbols, self.config["start_date"], self.config["end_date"])
        if len(calendar) < 2:
            raise ValueError("可用行情数据不足，请先下载更长时间范围的数据")

        data_start_date = self.config.get("data_start_date", self.config["start_date"])
        history = self.portal.get_prices(symbols, data_start_date, self.config["end_date"])
        symbols = [symbol for symbol in symbols if symbol in history]
        self.config["symbols"] = symbols
        profiles = self.portal.db.symbol_profiles(symbols)
        self.symbol_names = {symbol: profiles.get(symbol, {}).get("name", "") for symbol in symbols}
        self.config["calendar"] = calendar
        self.config["calendar_index"] = {trade_date: index for index, trade_date in enumerate(calendar)}
        context = Context(self.config, history)
        context.portfolio = self.portfolio
        self.strategy.init(context)
        orders_to_execute = []
        rows_by_symbol_date = self._rows_by_symbol_date(history)

        for index, trade_date in enumerate(calendar):
            day_data = {
                symbol: rows_by_date[trade_date]
                for symbol, rows_by_date in rows_by_symbol_date.items()
                if trade_date in rows_by_date
            }
            open_prices = {symbol: float(row["open"]) for symbol, row in day_data.items()}
            close_prices = {symbol: float(row["close"]) for symbol, row in day_data.items()}

            context.current_date = trade_date
            if hasattr(self.strategy, "before_open"):
                self.strategy.before_open(context, day_data)
                open_orders = context.drain_orders()
            else:
                open_orders = []

            if orders_to_execute:
                self._execute_orders(trade_date, orders_to_execute, open_prices)
                orders_to_execute = []
            if open_orders:
                self._execute_orders(trade_date, open_orders, open_prices)

            self.strategy.on_bar(context, day_data)
            bar_orders = context.drain_orders()
            immediate_orders = [order for order in bar_orders if getattr(order, "execute_now", False)]
            orders_to_execute = [order for order in bar_orders if not getattr(order, "execute_now", False)]
            if immediate_orders:
                self._execute_orders(trade_date, immediate_orders, close_prices)

            self._snapshot(trade_date, close_prices)

        metrics = calculate_metrics(self.equity_curve, [trade.__dict__ for trade in self.trades])
        return {
            "id": str(uuid.uuid4())[:8],
            "config": self.config,
            "metrics": metrics,
            "equity_curve": self.equity_curve,
            "positions_history": self.positions_history,
            "trades": [trade.__dict__ for trade in self.trades],
            "assumptions": [
                "日线前复权价格",
                "T 日收盘后生成信号，T+1 日开盘成交",
                "未处理涨跌停、停牌和幸存者偏差",
                "普通 A 股按 100 股、创业板按 200 股整数手成交",
                "手续费、滑点、印花税按固定比例近似",
            ],
        }

    def _normalize_config(self, config: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(config)
        normalized["symbols"] = [normalize_symbol(symbol) for symbol in config.get("symbols", [])]
        normalized.setdefault("initial_cash", 1_000_000)
        normalized.setdefault("commission_rate", 0.0003)
        normalized.setdefault("slippage_rate", 0.0005)
        normalized.setdefault("stamp_tax_rate", 0.001)
        if not normalized["symbols"]:
            raise ValueError("股票池不能为空")
        return normalized

    def _rows_by_symbol_date(self, history: dict[str, list[dict[str, Any]]]) -> dict[str, dict[str, dict[str, Any]]]:
        return {symbol: {row["trade_date"]: row for row in rows} for symbol, rows in history.items()}

    def _execute_orders(self, trade_date: str, orders: list[Any], prices: dict[str, float]) -> None:
        total_value = self.portfolio.total_value(prices)
        commission_rate = float(self.config["commission_rate"])
        slippage_rate = float(self.config["slippage_rate"])
        stamp_tax_rate = float(self.config["stamp_tax_rate"])

        for order in sorted(orders, key=lambda item: item.target_percent):
            symbol = order.symbol
            order_price = getattr(order, "price", None)
            raw_price = float(order_price) if order_price else prices.get(symbol, 0.0)
            if raw_price <= 0:
                continue
            position = self.portfolio.position(symbol)
            current_value = position.quantity * raw_price
            target_value = total_value * float(order.target_percent)
            diff_value = target_value - current_value
            if abs(diff_value) < raw_price * 100:
                continue
            if diff_value > 0:
                price = raw_price * (1 + slippage_rate)
                lot_size = board_lot_size(symbol)
                quantity = int(diff_value / price / lot_size) * lot_size
                if quantity <= 0:
                    continue
                gross = quantity * price
                commission = gross * commission_rate
                affordable = int(self.portfolio.cash / (price * (1 + commission_rate)) / lot_size) * lot_size
                quantity = min(quantity, affordable)
                if quantity <= 0:
                    continue
                gross = quantity * price
                commission = gross * commission_rate
                self.portfolio.cash -= gross + commission
                old_cost = position.avg_cost * position.quantity
                old_price_value = position.avg_price * position.quantity
                position.quantity += quantity
                position.avg_cost = (old_cost + gross + commission) / position.quantity
                position.avg_price = (old_price_value + gross) / position.quantity
                self.trades.append(
                    Trade(
                        trade_date,
                        symbol,
                        "buy",
                        quantity,
                        round(price, 4),
                        gross,
                        commission,
                        0.0,
                        getattr(order, "reason", ""),
                        name=self.symbol_names.get(symbol, ""),
                    )
                )
            else:
                price = raw_price * (1 - slippage_rate)
                lot_size = board_lot_size(symbol)
                quantity = int(abs(diff_value) / price / lot_size) * lot_size
                quantity = min(quantity, position.quantity)
                if quantity <= 0:
                    continue
                gross = quantity * price
                commission = gross * commission_rate
                tax = gross * stamp_tax_rate
                cost_basis = position.avg_cost * quantity
                net_proceeds = gross - commission - tax
                pnl = net_proceeds - cost_basis
                pnl_pct = pnl / cost_basis if cost_basis else 0.0
                price_return = price / position.avg_price - 1 if position.avg_price else 0.0
                self.portfolio.cash += gross - commission - tax
                position.quantity -= quantity
                if position.quantity == 0:
                    position.avg_cost = 0.0
                    position.avg_price = 0.0
                self.trades.append(
                    Trade(
                        trade_date,
                        symbol,
                        "sell",
                        quantity,
                        round(price, 4),
                        gross,
                        commission,
                        tax,
                        getattr(order, "reason", ""),
                        round(pnl, 2),
                        round(pnl_pct, 6),
                        round(price_return, 6),
                        name=self.symbol_names.get(symbol, ""),
                    )
                )
    def _snapshot(self, trade_date: str, prices: dict[str, float]) -> None:
        market_value = self.portfolio.market_value(prices)
        total_value = self.portfolio.cash + market_value
        previous = self.equity_curve[-1]["total_value"] if self.equity_curve else total_value
        daily_return = total_value / previous - 1 if previous else 0.0
        self.equity_curve.append(
            {
                "trade_date": trade_date,
                "cash": round(self.portfolio.cash, 2),
                "market_value": round(market_value, 2),
                "total_value": round(total_value, 2),
                "daily_return": daily_return,
            }
        )
        for symbol, position in self.portfolio.positions.items():
            price = prices.get(symbol, 0.0)
            self.positions_history.append(
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "quantity": position.quantity,
                    "price": price,
                    "market_value": round(position.quantity * price, 2),
                }
            )


def board_lot_size(symbol: str) -> int:
    normalized = normalize_symbol(symbol)
    code, exchange = normalized.split(".")
    return 200 if exchange == "SZ" and code.startswith(("300", "301")) else 100
