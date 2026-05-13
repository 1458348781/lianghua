from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

try:
    import pandas as pd
except Exception:  # pragma: no cover - optional parquet support
    pd = None  # type: ignore[assignment]

from .data_sources import normalize_symbol
from .metrics import calculate_metrics
from .storage import MarketDatabase
from .strategy import DivergenceStrategy


DEFAULT_MINUTE_PARQUET_ROOT = Path(r"D:\BaiduNetdiskDownload\1m_price")


@dataclass
class MinutePosition:
    symbol: str
    entry_date: str
    entry_time: str
    entry_price: float
    amount: float
    quantity: float


class DivergenceMinuteBacktestEngine:
    def __init__(self, db: MarketDatabase, config: dict[str, Any]) -> None:
        self.db = db
        self.config = self._normalize_config(config)
        self.strategy = DivergenceStrategy(**self.config.get("params", {}))
        self.symbol_names: dict[str, str] = {}
        self.positions_history: list[dict[str, Any]] = []
        self.minute_days_loaded = 0
        self.minute_days_missing = 0

    def run(self) -> dict[str, Any]:
        symbols = self.config["symbols"]
        start_date = self.config["start_date"]
        end_date = self.config["end_date"]
        data_start_date = self.config.get("data_start_date", start_date)
        history = self.db.query_many(symbols, data_start_date, end_date)
        symbols = [symbol for symbol in symbols if symbol in history]
        calendar = self.db.get_calendar(symbols, start_date, end_date)
        if len(calendar) < 2:
            raise ValueError("可用行情数据不足，请先下载更长时间范围的数据")

        self.config["symbols"] = symbols
        self.config["calendar"] = calendar
        self.config["calendar_index"] = {trade_date: index for index, trade_date in enumerate(calendar)}
        self.config["execution_mode"] = "minute"
        profiles = self.db.symbol_profiles(symbols)
        self.symbol_names = {symbol: profiles.get(symbol, {}).get("name", "") for symbol in symbols}

        candidates_by_date = self._build_candidates_by_date(history, start_date, end_date)
        cash = float(self.config["initial_cash"])
        commission_rate = float(self.config["commission_rate"])
        slippage_rate = float(self.config["slippage_rate"])
        stamp_tax_rate = float(self.config["stamp_tax_rate"])
        max_positions = max(1, int(self.strategy.params.get("max_positions") or 1))
        hold_days = int(self.strategy.params.get("hold_days") or 0)
        positions: dict[str, MinutePosition] = {}
        trades: list[dict[str, Any]] = []
        equity_curve: list[dict[str, Any]] = []
        daily_rows = {symbol: {row["trade_date"]: row for row in rows} for symbol, rows in history.items()}

        for trade_date in calendar:
            day_candidates = candidates_by_date.get(trade_date, {})
            minute_symbols = sorted(set(day_candidates) | set(positions))
            minute_rows = self._query_minutes(trade_date, minute_symbols)
            if minute_symbols:
                if minute_rows:
                    self.minute_days_loaded += 1
                else:
                    self.minute_days_missing += 1

            if day_candidates:
                triggers = self._find_intraday_triggers(minute_rows, day_candidates, trade_date)
                for trigger in triggers:
                    symbol = trigger["symbol"]
                    if symbol in positions or len(positions) >= max_positions:
                        continue
                    remaining_slots = max(1, max_positions - len(positions))
                    budget = cash / remaining_slots
                    entry_price = float(trigger["entry_price"]) * (1 + slippage_rate)
                    if entry_price <= 0:
                        continue
                    lot_size = board_lot_size(symbol)
                    quantity = int(budget / entry_price / lot_size) * lot_size
                    gross = quantity * entry_price
                    commission = gross * commission_rate
                    if gross + commission > cash:
                        quantity = int(cash / (entry_price * (1 + commission_rate)) / lot_size) * lot_size
                        gross = quantity * entry_price
                        commission = gross * commission_rate
                    if quantity <= 0:
                        continue
                    cash -= gross + commission
                    positions[symbol] = MinutePosition(
                        symbol=symbol,
                        entry_date=trade_date,
                        entry_time=str(trigger["trade_time"]),
                        entry_price=entry_price,
                        amount=gross + commission,
                        quantity=quantity,
                    )
                    trades.append(
                        {
                            "trade_date": trade_date,
                            "trade_time": str(trigger["trade_time"]),
                            "symbol": symbol,
                            "name": self.symbol_names.get(symbol, ""),
                            "side": "buy",
                            "quantity": round(quantity, 4),
                            "price": round(entry_price, 4),
                            "amount": round(gross, 2),
                            "commission": round(commission, 2),
                            "tax": 0.0,
                            "reason": "minute breakout entry",
                        }
                    )

            if positions:
                cash += self._process_intraday_exits(
                    minute_rows=minute_rows,
                    positions=positions,
                    trades=trades,
                    trade_date=trade_date,
                    daily_rows=daily_rows,
                    hold_days=hold_days,
                    commission_rate=commission_rate,
                    stamp_tax_rate=stamp_tax_rate,
                    slippage_rate=slippage_rate,
                )

            close_prices = self._close_prices_for_date(daily_rows, trade_date, positions)
            market_value = sum(pos.quantity * close_prices.get(symbol, pos.entry_price) for symbol, pos in positions.items())
            total_value = cash + market_value
            previous = equity_curve[-1]["total_value"] if equity_curve else float(self.config["initial_cash"])
            equity_curve.append(
                {
                    "trade_date": trade_date,
                    "cash": round(cash, 2),
                    "market_value": round(market_value, 2),
                    "total_value": round(total_value, 2),
                    "daily_return": total_value / previous - 1 if previous else 0.0,
                }
            )
            self._snapshot_positions(trade_date, positions, close_prices)

        metrics = calculate_metrics(equity_curve, trades)
        metrics["buy_count"] = sum(1 for trade in trades if trade.get("side") == "buy")
        metrics["sell_count"] = sum(1 for trade in trades if trade.get("side") == "sell")
        return {
            "id": str(uuid.uuid4())[:8],
            "config": self.config,
            "metrics": metrics,
            "equity_curve": equity_curve,
            "positions_history": self.positions_history,
            "trades": trades,
            "assumptions": [
                "分歧2页面回算使用分钟级触发：日线筛选 T/T+1 候选，分钟线盘中突破后买入。",
                "买入金额按剩余现金 / 剩余可开仓数量动态分配，普通 A 股按 100 股、创业板按 200 股向下取整。",
                "买入当天不卖；次日起先按开盘破止损检查，再按盘中止损、14:55 后炸板/到期退出检查。",
                "分钟数据优先读本地 SQLite stock_minute，不足时读取 D:\\BaiduNetdiskDownload\\1m_price 年度 parquet。",
                f"分钟数据命中交易日 {self.minute_days_loaded} 个，缺失交易日 {self.minute_days_missing} 个。",
            ],
        }

    def _normalize_config(self, config: dict[str, Any]) -> dict[str, Any]:
        normalized = dict(config)
        normalized["symbols"] = [normalize_symbol(symbol) for symbol in config.get("symbols", [])]
        normalized.setdefault("initial_cash", 1_000_000)
        normalized.setdefault("commission_rate", 0.0003)
        normalized.setdefault("slippage_rate", 0.001)
        normalized.setdefault("stamp_tax_rate", 0.001)
        normalized.setdefault("params", {})
        normalized.setdefault("minute_parquet_root", str(DEFAULT_MINUTE_PARQUET_ROOT))
        if not normalized["symbols"]:
            raise ValueError("股票池不能为空")
        return normalized

    def _build_candidates_by_date(
        self,
        history: dict[str, list[dict[str, Any]]],
        start_date: str,
        end_date: str,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        candidates: dict[str, dict[str, dict[str, Any]]] = {}
        for symbol, rows in history.items():
            if len(rows) < 35:
                continue
            for index in range(34, len(rows)):
                t = rows[index - 2]
                t1 = rows[index - 1]
                t2 = rows[index]
                trade_date = str(t2["trade_date"])
                if trade_date < start_date or trade_date > end_date:
                    continue
                if self.strategy._setup_ok(symbol, t, t1, rows[index - 34 : index + 1]):  # type: ignore[attr-defined]
                    candidates.setdefault(trade_date, {})[symbol] = {"previous_row": t1}
        return candidates

    def _query_minutes(self, trade_date: str, symbols: list[str]) -> list[dict[str, Any]]:
        if not symbols:
            return []
        rows_by_key: dict[tuple[str, str], dict[str, Any]] = {}
        for row in self._query_minutes_sqlite(trade_date, symbols):
            rows_by_key[(str(row["symbol"]), str(row["trade_time"]))] = row
        for row in self._query_minutes_parquet(trade_date, symbols):
            rows_by_key.setdefault((str(row["symbol"]), str(row["trade_time"])), row)
        return sorted(rows_by_key.values(), key=lambda item: (str(item["trade_time"]), str(item["symbol"])))

    def _query_minutes_sqlite(self, trade_date: str, symbols: list[str]) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        normalized = sorted({normalize_symbol(symbol) for symbol in symbols})
        with self.db.connect() as conn:
            for chunk in chunks(normalized, 700):
                marks = ",".join("?" for _ in chunk)
                sql = f"""
                    select symbol, trade_time, trade_date, open, high, low, close, volume, amount
                    from stock_minute
                    where trade_date = ? and symbol in ({marks})
                    order by trade_time, symbol
                """
                rows.extend(dict(row) for row in conn.execute(sql, [trade_date, *chunk]))
        return rows

    def _query_minutes_parquet(self, trade_date: str, symbols: list[str]) -> list[dict[str, Any]]:
        if pd is None:
            return []
        root = Path(str(self.config.get("minute_parquet_root") or DEFAULT_MINUTE_PARQUET_ROOT))
        path = root / trade_date[:4] / f"{trade_date.replace('-', '')}.parquet"
        if not path.exists():
            return []
        wanted = sorted({normalize_symbol(symbol) for symbol in symbols})
        columns = ["code", "trade_time", "date", "open", "high", "low", "close", "vol", "amount"]
        try:
            frame = pd.read_parquet(path, columns=columns, filters=[("code", "in", wanted)])
        except Exception:
            frame = pd.read_parquet(path, columns=columns)
            frame = frame[frame["code"].isin(wanted)]
        if frame.empty:
            return []
        frame = frame.rename(columns={"code": "symbol", "vol": "volume"})
        frame["trade_date"] = (
            frame["date"].astype(str).str.slice(0, 4)
            + "-"
            + frame["date"].astype(str).str.slice(4, 6)
            + "-"
            + frame["date"].astype(str).str.slice(6, 8)
        )
        frame = frame[["symbol", "trade_time", "trade_date", "open", "high", "low", "close", "volume", "amount"]]
        frame = frame.sort_values(["trade_time", "symbol"])
        return frame.to_dict("records")

    def _find_intraday_triggers(
        self,
        minute_rows: list[dict[str, Any]],
        candidates: dict[str, dict[str, Any]],
        trade_date: str,
    ) -> list[dict[str, Any]]:
        state: dict[str, dict[str, float]] = {}
        triggers: list[dict[str, Any]] = []
        triggered_symbols: set[str] = set()
        for row in minute_rows:
            symbol = str(row["symbol"])
            if symbol in triggered_symbols or symbol not in candidates:
                continue
            previous = candidates[symbol]["previous_row"]
            item = state.setdefault(
                symbol,
                {
                    "open": float(row["open"] or 0),
                    "high": float(row["high"] or 0),
                    "low": float(row["low"] or 0),
                    "volume": 0.0,
                    "amount": 0.0,
                },
            )
            item["high"] = max(float(item["high"]), float(row["high"] or 0))
            low = float(row["low"] or 0)
            item["low"] = min(float(item["low"]), low) if item["low"] else low
            item["volume"] += float(row.get("volume") or 0)
            item["amount"] += float(row.get("amount") or 0)
            pre_close = float(previous.get("close") or 0)
            close = float(row["close"] or 0)
            entry_row = {
                "symbol": symbol,
                "trade_date": trade_date,
                "open": item["open"],
                "high": item["high"],
                "low": item["low"],
                "close": close,
                "pre_close": pre_close,
                "volume": item["volume"],
                "amount": item["amount"],
                "turnover": 0.0,
                "pct_chg": close / pre_close * 100 - 100 if pre_close else 0.0,
                "is_st": 0,
            }
            if self.strategy._entry_day_ok(previous, entry_row):  # type: ignore[attr-defined]
                entry_price = self.strategy._entry_price(previous, entry_row)  # type: ignore[attr-defined]
                triggers.append({"symbol": symbol, "trade_time": str(row["trade_time"]), "entry_price": entry_price})
                triggered_symbols.add(symbol)
        return sorted(triggers, key=lambda item: (item["trade_time"], item["symbol"]))

    def _process_intraday_exits(
        self,
        minute_rows: list[dict[str, Any]],
        positions: dict[str, MinutePosition],
        trades: list[dict[str, Any]],
        trade_date: str,
        daily_rows: dict[str, dict[str, dict[str, Any]]],
        hold_days: int,
        commission_rate: float,
        stamp_tax_rate: float,
        slippage_rate: float,
    ) -> float:
        cash_delta = 0.0
        state: dict[str, dict[str, float]] = {}
        for row in minute_rows:
            symbol = str(row["symbol"])
            position = positions.get(symbol)
            if position is None:
                continue
            item = state.setdefault(
                symbol,
                {
                    "high": float(row["high"] or 0),
                    "low": float(row["low"] or 0),
                    "first_open": float(row["open"] or 0),
                    "first_seen": 1.0,
                },
            )
            item["high"] = max(float(item["high"]), float(row["high"] or 0))
            low = float(row["low"] or 0)
            item["low"] = min(float(item["low"]), low) if item["low"] else low
            current_time = str(row["trade_time"])
            if trade_date == position.entry_date:
                continue
            exit_price = 0.0
            reason = ""
            stop_price = position.entry_price * (1 + float(self.strategy.params["stop_loss"]))
            if item.get("first_seen") == 1.0 and float(item.get("first_open") or 0) > 0 and float(item["first_open"]) <= stop_price:
                exit_price = float(item["first_open"])
                reason = "stop_open"
            elif item["low"] <= stop_price:
                exit_price = stop_price
                reason = "stop"
            elif current_time[-8:] >= "14:55:00":
                item["first_seen"] = 0.0
                daily_row = daily_rows.get(symbol, {}).get(trade_date, {})
                pre_close = float(daily_row.get("pre_close") or daily_row.get("open") or 0)
                minute_day = {
                    "trade_date": trade_date,
                    "open": 0,
                    "high": item["high"],
                    "low": item["low"],
                    "close": float(row["close"] or 0),
                    "pre_close": pre_close,
                    "pct_chg": float(row["close"] or 0) / pre_close * 100 - 100 if pre_close else 0.0,
                }
                if self.strategy._is_limit_up(symbol, minute_day):  # type: ignore[attr-defined]
                    continue
                if self.strategy._hit_limit_up(symbol, minute_day):  # type: ignore[attr-defined]
                    exit_price = float(row["close"] or 0)
                    reason = "limit_failed"
                elif hold_days > 0 and self._held_days_after_entry(position.entry_date, trade_date) >= hold_days:
                    exit_price = float(row["close"] or 0)
                    reason = "expiry"
            item["first_seen"] = 0.0
            if exit_price <= 0:
                continue
            exit_price *= 1 - slippage_rate
            gross = position.quantity * exit_price
            commission = gross * commission_rate
            tax = gross * stamp_tax_rate
            proceeds = gross - commission - tax
            pnl = proceeds - position.amount
            trades.append(
                {
                    "trade_date": trade_date,
                    "trade_time": current_time,
                    "symbol": symbol,
                    "name": self.symbol_names.get(symbol, ""),
                    "side": "sell",
                    "quantity": round(position.quantity, 4),
                    "price": round(exit_price, 4),
                    "amount": round(gross, 2),
                    "commission": round(commission, 2),
                    "tax": round(tax, 2),
                    "reason": reason,
                    "pnl": round(pnl, 2),
                    "pnl_pct": pnl / position.amount if position.amount else 0.0,
                    "price_return": exit_price / position.entry_price - 1 if position.entry_price else 0.0,
                }
            )
            cash_delta += proceeds
            positions.pop(symbol, None)
        return cash_delta

    def _calendar_distance(self, entry_date: str, trade_date: str) -> int:
        calendar_index = self.config.get("calendar_index", {})
        return int(calendar_index.get(trade_date, 0)) - int(calendar_index.get(entry_date, 0)) + 1

    def _held_days_after_entry(self, entry_date: str, trade_date: str) -> int:
        calendar_index = self.config.get("calendar_index", {})
        return max(0, int(calendar_index.get(trade_date, 0)) - int(calendar_index.get(entry_date, 0)))

    def _close_prices_for_date(
        self,
        daily_rows: dict[str, dict[str, dict[str, Any]]],
        trade_date: str,
        positions: dict[str, MinutePosition],
    ) -> dict[str, float]:
        prices: dict[str, float] = {}
        for symbol, position in positions.items():
            row = daily_rows.get(symbol, {}).get(trade_date)
            prices[symbol] = float(row["close"]) if row else position.entry_price
        return prices

    def _snapshot_positions(
        self,
        trade_date: str,
        positions: dict[str, MinutePosition],
        prices: dict[str, float],
    ) -> None:
        for symbol, position in positions.items():
            price = prices.get(symbol, position.entry_price)
            self.positions_history.append(
                {
                    "trade_date": trade_date,
                    "symbol": symbol,
                    "quantity": round(position.quantity, 4),
                    "price": price,
                    "market_value": round(position.quantity * price, 2),
                }
            )


def chunks(items: list[str], size: int) -> list[list[str]]:
    return [items[index : index + size] for index in range(0, len(items), size)]


def board_lot_size(symbol: str) -> int:
    normalized = normalize_symbol(symbol)
    code, exchange = normalized.split(".")
    return 200 if exchange == "SZ" and code.startswith(("300", "301")) else 100
