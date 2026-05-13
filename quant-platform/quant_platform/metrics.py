from __future__ import annotations

import math
from statistics import mean, pstdev
from typing import Any


def calculate_metrics(equity_curve: list[dict[str, Any]], trades: list[dict[str, Any]]) -> dict[str, float | int]:
    if not equity_curve:
        return {}
    values = [float(row["total_value"]) for row in equity_curve]
    returns = [float(row.get("daily_return", 0)) for row in equity_curve][1:]
    start = values[0]
    end = values[-1]
    cumulative_return = end / start - 1 if start else 0.0
    periods = max(1, len(values))
    annual_return = (end / start) ** (252 / periods) - 1 if start and end > 0 else 0.0
    volatility = pstdev(returns) * math.sqrt(252) if len(returns) > 1 else 0.0
    sharpe = annual_return / volatility if volatility else 0.0
    max_drawdown = _max_drawdown(values)
    wins = _estimate_wins(trades)
    closed_trades = wins["wins"] + wins["losses"]
    win_rate = wins["wins"] / closed_trades if closed_trades else 0.0
    sells = [trade for trade in trades if trade.get("side") == "sell"]
    net_trade_returns = [float(trade.get("pnl_pct") or 0) for trade in sells]
    price_returns = [float(trade.get("price_return") or trade.get("pnl_pct") or 0) for trade in sells]
    return {
        "cumulative_return": round(cumulative_return, 6),
        "annual_return": round(annual_return, 6),
        "max_drawdown": round(max_drawdown, 6),
        "sharpe": round(sharpe, 4),
        "volatility": round(volatility, 6),
        "win_rate": round(win_rate, 6),
        "trade_count": len(trades),
        "final_value": round(end, 2),
        "avg_trade_return": round(mean(net_trade_returns), 6) if net_trade_returns else 0.0,
        "best_trade_return": round(max(net_trade_returns), 6) if net_trade_returns else 0.0,
        "worst_trade_return": round(min(net_trade_returns), 6) if net_trade_returns else 0.0,
        "best_price_return": round(max(price_returns), 6) if price_returns else 0.0,
    }


def _max_drawdown(values: list[float]) -> float:
    peak = values[0]
    drawdown = 0.0
    for value in values:
        peak = max(peak, value)
        if peak:
            drawdown = min(drawdown, value / peak - 1)
    return drawdown


def _estimate_wins(trades: list[dict[str, Any]]) -> dict[str, int]:
    avg_cost: dict[str, float] = {}
    qty: dict[str, int] = {}
    wins = 0
    losses = 0
    for trade in trades:
        symbol = trade["symbol"]
        quantity = int(trade["quantity"])
        price = float(trade["price"])
        if trade["side"] == "buy":
            old_qty = qty.get(symbol, 0)
            old_cost = avg_cost.get(symbol, 0) * old_qty
            qty[symbol] = old_qty + quantity
            avg_cost[symbol] = (old_cost + price * quantity) / qty[symbol]
        else:
            if price >= avg_cost.get(symbol, price):
                wins += 1
            else:
                losses += 1
            qty[symbol] = max(0, qty.get(symbol, 0) - quantity)
            if qty[symbol] == 0:
                avg_cost[symbol] = 0.0
    return {"wins": wins, "losses": losses}


def equity_with_drawdown(equity_curve: list[dict[str, Any]]) -> list[dict[str, Any]]:
    peak = None
    result = []
    for row in equity_curve:
        value = float(row["total_value"])
        peak = value if peak is None else max(peak, value)
        drawdown = value / peak - 1 if peak else 0.0
        item = dict(row)
        item["net_value"] = value / float(equity_curve[0]["total_value"])
        item["drawdown"] = drawdown
        result.append(item)
    return result


def monthly_returns(equity_curve: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if not equity_curve:
        return []
    grouped: dict[str, list[float]] = {}
    for row in equity_curve:
        grouped.setdefault(row["trade_date"][:7], []).append(float(row["total_value"]))
    output = []
    for month, values in sorted(grouped.items()):
        if len(values) >= 2 and values[0]:
            output.append({"month": month, "return": values[-1] / values[0] - 1})
    return output
