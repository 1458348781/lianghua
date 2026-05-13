from __future__ import annotations

import argparse
import json
import math
import time
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache" / "limit_up_pullback"
REPORT_DIR = ROOT / "reports" / "limit_up_pullback"


@dataclass
class BacktestConfig:
    start_date: str
    end_date: str
    initial_cash: float = 1_000_000
    max_positions: int = 5
    hold_days: int = 3
    stop_loss: float = -0.05
    take_profit: float = 0.16
    commission_rate: float = 0.0003
    slippage_rate: float = 0.0005
    stamp_tax_rate: float = 0.001
    min_list_days: int = 250
    min_float_mv: float = 2_000_000_000
    max_float_mv: float = 20_000_000_000
    min_price: float = 3
    max_price: float = 500
    min_turnover: float = 3.0
    max_turnover: float = 25.0
    day1_min_volume_ratio: float = 1.2
    day1_max_volume_ratio: float = 6.0
    range_min_amplitude_30: float = 0.18
    range_min_return_20: float = 0.05
    day2_min_pct_chg: float = -3.0
    day2_max_pct_chg: float = 8.0
    entry_min_open_gap_pct_chg: float = 1.0
    entry_max_open_gap_pct_chg: float = 5.0
    entry_min_high_from_open_pct_chg: float = 3.0
    strong_close_pct_chg: float = 3.0
    max_symbols: int = 0


def request_json(url: str, timeout: int = 15) -> dict[str, Any]:
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 quant-platform/0.1",
            "Referer": "https://quote.eastmoney.com/",
        },
    )
    last_error: Exception | None = None
    for attempt in range(4):
        try:
            with urllib.request.urlopen(req, timeout=timeout) as resp:
                return json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            last_error = exc
            time.sleep(0.7 + attempt * 0.8)
    raise RuntimeError(f"HTTP request failed after retries: {last_error}")


def normalize_symbol(code: str) -> str:
    code = code.strip()
    if code.startswith(("6", "9")):
        return f"{code}.SH"
    if code.startswith(("8", "4")):
        return f"{code}.BJ"
    return f"{code}.SZ"


def secid(symbol: str) -> str:
    code, exchange = symbol.split(".")
    if exchange == "SH":
        market = "1"
    else:
        market = "0"
    return f"{market}.{code}"


def compact_date(value: str) -> str:
    return value.replace("-", "")


def fetch_stock_pool(config: BacktestConfig) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    page = 1
    while True:
        params = {
            "pn": page,
            "pz": 100,
            "po": 1,
            "np": 1,
            "ut": "bd1d9ddb04089700cf9c27f6f7426281",
            "fltt": 2,
            "invt": 2,
            "fid": "f3",
            "fs": "m:0+t:6,m:0+t:80,m:1+t:2,m:1+t:23",
            "fields": "f12,f14,f2,f3,f8,f20,f21,f26",
        }
        url = "https://push2.eastmoney.com/api/qt/clist/get?" + urllib.parse.urlencode(params)
        payload = request_json(url)
        data = payload.get("data") or {}
        diff = data.get("diff") or []
        rows.extend(diff)
        total = int(data.get("total") or 0)
        if page * 100 >= total or not diff:
            break
        page += 1

    frame = pd.DataFrame(rows)
    if frame.empty:
        raise RuntimeError("No stock pool data returned from EastMoney.")

    frame = frame.rename(
        columns={
            "f12": "code",
            "f14": "name",
            "f2": "price",
            "f21": "float_mv",
            "f26": "list_date",
        }
    )
    frame["symbol"] = frame["code"].map(normalize_symbol)
    frame["list_date"] = pd.to_datetime(frame["list_date"].astype(str), format="%Y%m%d", errors="coerce")
    start = pd.to_datetime(config.start_date)
    frame["list_days_at_start"] = (start - frame["list_date"]).dt.days

    name = frame["name"].astype(str)
    filtered = frame[
        ~name.str.contains("ST|退", regex=True, na=False)
        & frame["list_days_at_start"].ge(config.min_list_days)
        & frame["float_mv"].between(config.min_float_mv, config.max_float_mv)
        & frame["price"].between(config.min_price, config.max_price)
    ].copy()
    filtered = filtered.sort_values("float_mv")
    if config.max_symbols > 0:
        filtered = filtered.head(config.max_symbols)
    return filtered[["symbol", "code", "name", "price", "float_mv", "list_date", "list_days_at_start"]]


def fetch_daily(symbol: str, start_date: str, end_date: str, refresh: bool = False) -> pd.DataFrame:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{symbol}_{start_date}_{end_date}.csv".replace(":", "")
    if path.exists() and not refresh:
        return pd.read_csv(path, parse_dates=["trade_date"])

    params = {
        "secid": secid(symbol),
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "101",
        "fqt": "1",
        "beg": compact_date(start_date),
        "end": compact_date(end_date),
    }
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?" + urllib.parse.urlencode(params)
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            payload = request_json(url)
            klines = ((payload.get("data") or {}).get("klines")) or []
            records = []
            for item in klines:
                parts = item.split(",")
                records.append(
                    {
                        "trade_date": parts[0],
                        "open": float(parts[1]),
                        "close": float(parts[2]),
                        "high": float(parts[3]),
                        "low": float(parts[4]),
                        "volume": float(parts[5]),
                        "amount": float(parts[6]),
                        "amplitude": float(parts[7]),
                        "pct_chg": float(parts[8]),
                        "change": float(parts[9]),
                        "turnover": float(parts[10]),
                    }
                )
            frame = pd.DataFrame(records)
            if frame.empty:
                return frame
            frame["symbol"] = symbol
            frame["trade_date"] = pd.to_datetime(frame["trade_date"])
            frame.to_csv(path, index=False, encoding="utf-8-sig")
            return frame
        except Exception as exc:
            last_error = exc
            time.sleep(0.8 + attempt * 0.8)
    raise RuntimeError(f"{symbol} daily data failed: {last_error}")


def limit_threshold(symbol: str) -> float:
    code = symbol.split(".")[0]
    if code.startswith(("300", "301", "688")):
        return 19.5
    if code.startswith(("8", "4")):
        return 29.0
    return 9.75


def build_signals(daily: pd.DataFrame, pool: pd.DataFrame, config: BacktestConfig) -> pd.DataFrame:
    metadata = pool.set_index("symbol").to_dict("index")
    signals: list[dict[str, Any]] = []

    for symbol, group in daily.groupby("symbol"):
        df = group.sort_values("trade_date").reset_index(drop=True).copy()
        if len(df) < 40 or symbol not in metadata:
            continue
        df["avg_vol_5_pre"] = df["volume"].shift(1).rolling(5).mean()
        df["amp_30"] = df["high"].shift(1).rolling(30).max() / df["low"].shift(1).rolling(30).min() - 1
        df["ret_20"] = df["close"].shift(1) / df["close"].shift(21) - 1
        for i in range(30, len(df) - 3):
            t = df.iloc[i]
            t1 = df.iloc[i + 1]
            t2 = df.iloc[i + 2]
            if not math.isfinite(float(t["avg_vol_5_pre"])):
                continue

            is_limit_up = t["pct_chg"] >= limit_threshold(symbol) and t["close"] >= t["high"] * 0.995
            not_one_price = not (
                abs(t["open"] - t["high"]) < 1e-6
                and abs(t["high"] - t["low"]) < 1e-6
                and abs(t["low"] - t["close"]) < 1e-6
            )
            volume_ratio = t["volume"] / t["avg_vol_5_pre"] if t["avg_vol_5_pre"] else 0
            day1_ok = (
                is_limit_up
                and not_one_price
                and config.day1_min_volume_ratio <= volume_ratio <= config.day1_max_volume_ratio
                and config.min_turnover <= t["turnover"] <= config.max_turnover
            )
            range_ok = t["amp_30"] >= config.range_min_amplitude_30 and t["ret_20"] >= config.range_min_return_20
            close_pos = (t1["close"] - t1["low"]) / (t1["high"] - t1["low"]) if t1["high"] > t1["low"] else 0
            upper_shadow = (t1["high"] - t1["close"]) / t1["close"] if t1["close"] else 1
            entry_pre_close = t2.get("pre_close", t1["close"])
            entry_open_gap_pct_chg = t2["open"] / entry_pre_close * 100 - 100 if entry_pre_close else 0
            entry_high_from_open_pct_chg = t2["high"] / t2["open"] * 100 - 100 if t2["open"] else 0
            day2_ok = (
                config.day2_min_pct_chg <= t1["pct_chg"] <= config.day2_max_pct_chg
                and t1["volume"] <= t["volume"] * 1.8
                and close_pos >= 0.45
                and upper_shadow <= 0.06
                and t1["close"] >= t["close"] * 0.97
            )
            entry_ok = (
                config.entry_min_open_gap_pct_chg
                <= entry_open_gap_pct_chg
                <= config.entry_max_open_gap_pct_chg
                and entry_high_from_open_pct_chg >= config.entry_min_high_from_open_pct_chg
                and t2["high"] > t1["high"]
            )

            if day1_ok and range_ok and day2_ok and entry_ok:
                signals.append(
                    {
                        "symbol": symbol,
                        "name": metadata[symbol]["name"],
                        "signal_date": t["trade_date"],
                        "confirm_date": t1["trade_date"],
                        "entry_date": t2["trade_date"],
                        "entry_open": t2["open"],
                        "limit_pct": t["pct_chg"],
                        "volume_ratio": volume_ratio,
                        "turnover": t["turnover"],
                        "amp_30": t["amp_30"],
                        "ret_20": t["ret_20"],
                        "day2_pct": t1["pct_chg"],
                        "entry_open_gap_pct": entry_open_gap_pct_chg,
                        "entry_high_from_open_pct": entry_high_from_open_pct_chg,
                        "breakout_price": max(t2["open"], t1["high"]),
                        "day2_close_pos": close_pos,
                        "score": volume_ratio + close_pos,
                    }
                )
    if not signals:
        return pd.DataFrame()
    return pd.DataFrame(signals).sort_values(["entry_date", "score"], ascending=[True, False])


def run_backtest(daily: pd.DataFrame, signals: pd.DataFrame, config: BacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    by_symbol = {symbol: group.sort_values("trade_date").reset_index(drop=True) for symbol, group in daily.groupby("symbol")}
    rows_by_date: dict[pd.Timestamp, pd.DataFrame] = {
        date: group.set_index("symbol") for date, group in daily.groupby("trade_date")
    }
    calendar = sorted(rows_by_date)
    signals_by_date = {date: group for date, group in signals.groupby("entry_date")} if not signals.empty else {}

    cash = config.initial_cash
    positions: dict[str, dict[str, Any]] = {}
    trades: list[dict[str, Any]] = []
    equity: list[dict[str, Any]] = []

    for current_date in calendar:
        day_rows = rows_by_date[current_date]

        for symbol in list(positions):
            if symbol not in day_rows.index:
                continue
            pos = positions[symbol]
            row = day_rows.loc[symbol]
            exit_reason = ""
            stop_price = pos["entry_price"] * (1 + config.stop_loss)
            hit_limit = row["high"] / row["pre_close"] - 1 >= limit_threshold(symbol) / 100 if row["pre_close"] else False
            close_limit = row["pct_chg"] >= limit_threshold(symbol) and row["close"] >= row["high"] * 0.995
            strong_close = row["close"] >= pos["entry_price"] * (1 + config.strong_close_pct_chg / 100)
            if row["low"] <= stop_price:
                exit_reason = "stop_loss"
                exit_price = stop_price
            elif close_limit:
                exit_price = 0
            elif hit_limit:
                exit_reason = "limit_failed"
                exit_price = row["close"]
            elif not strong_close:
                exit_reason = "weak_close_exit"
                exit_price = row["close"]
            if exit_reason:
                price = exit_price * (1 - config.slippage_rate)
                gross = pos["quantity"] * price
                commission = gross * config.commission_rate
                tax = gross * config.stamp_tax_rate
                cash += gross - commission - tax
                trades.append(
                    {
                        "trade_date": current_date,
                        "symbol": symbol,
                        "name": pos["name"],
                        "side": "sell",
                        "quantity": pos["quantity"],
                        "price": price,
                        "amount": gross,
                        "cost": commission + tax,
                        "reason": exit_reason,
                        "pnl": gross - commission - tax - pos["cost_basis"],
                        "return": (gross - commission - tax) / pos["cost_basis"] - 1,
                    }
                )
                del positions[symbol]

        todays_signals = signals_by_date.get(current_date)
        if todays_signals is not None:
            available_slots = max(0, config.max_positions - len(positions))
            selected_signals = todays_signals.head(available_slots).to_dict("records")
            final_count = len(positions) + len(selected_signals)
            target_weight = min(0.5, 1 / final_count) if final_count > 0 else 0
            total_value = cash + sum(
                pos["quantity"] * day_rows.loc[symbol]["open"]
                for symbol, pos in positions.items()
                if symbol in day_rows.index
            )
            for signal in selected_signals:
                symbol = signal["symbol"]
                if symbol in positions or symbol not in day_rows.index:
                    continue
                row = day_rows.loc[symbol]
                target_cash = min(cash, total_value * target_weight)
                price = signal.get("breakout_price", row["open"]) * (1 + config.slippage_rate)
                quantity = int(target_cash / (price * (1 + config.commission_rate)) / 100) * 100
                if quantity <= 0:
                    continue
                gross = quantity * price
                commission = gross * config.commission_rate
                cash -= gross + commission
                positions[symbol] = {
                    "name": signal["name"],
                    "quantity": quantity,
                    "entry_price": price,
                    "entry_date": current_date,
                    "cost_basis": gross + commission,
                }
                trades.append(
                    {
                        "trade_date": current_date,
                        "symbol": symbol,
                        "name": signal["name"],
                        "side": "buy",
                        "quantity": quantity,
                        "price": price,
                        "amount": gross,
                        "cost": commission,
                        "reason": "entry",
                        "pnl": 0.0,
                        "return": 0.0,
                    }
                )

        market_value = 0.0
        for symbol, pos in positions.items():
            if symbol in day_rows.index:
                market_value += pos["quantity"] * day_rows.loc[symbol]["close"]
        total_value = cash + market_value
        previous = equity[-1]["total_value"] if equity else total_value
        equity.append(
            {
                "trade_date": current_date,
                "cash": cash,
                "market_value": market_value,
                "total_value": total_value,
                "daily_return": total_value / previous - 1 if previous else 0,
                "positions": len(positions),
            }
        )

    return pd.DataFrame(equity), pd.DataFrame(trades)


def summarize(equity: pd.DataFrame, trades: pd.DataFrame) -> dict[str, Any]:
    if equity.empty:
        return {}
    values = equity["total_value"]
    returns = equity["daily_return"].iloc[1:]
    cumulative = values.iloc[-1] / values.iloc[0] - 1
    annual = (values.iloc[-1] / values.iloc[0]) ** (252 / max(1, len(values))) - 1
    volatility = returns.std(ddof=0) * math.sqrt(252) if len(returns) > 1 else 0
    sharpe = annual / volatility if volatility else 0
    drawdown = values / values.cummax() - 1
    sells = trades[trades["side"] == "sell"] if not trades.empty else trades
    return {
        "start": str(equity["trade_date"].iloc[0].date()),
        "end": str(equity["trade_date"].iloc[-1].date()),
        "final_value": round(float(values.iloc[-1]), 2),
        "cumulative_return": round(float(cumulative), 6),
        "annual_return": round(float(annual), 6),
        "max_drawdown": round(float(drawdown.min()), 6),
        "volatility": round(float(volatility), 6),
        "sharpe": round(float(sharpe), 4),
        "buy_count": int((trades["side"] == "buy").sum()) if not trades.empty else 0,
        "closed_trades": int(len(sells)),
        "win_rate": round(float((sells["return"] > 0).mean()), 6) if len(sells) else 0,
        "avg_trade_return": round(float(sells["return"].mean()), 6) if len(sells) else 0,
    }


def save_outputs(config: BacktestConfig, pool: pd.DataFrame, signals: pd.DataFrame, equity: pd.DataFrame, trades: pd.DataFrame) -> dict[str, str]:
    REPORT_DIR.mkdir(parents=True, exist_ok=True)
    stamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    paths = {
        "pool": REPORT_DIR / f"pool_{stamp}.csv",
        "signals": REPORT_DIR / f"signals_{stamp}.csv",
        "equity": REPORT_DIR / f"equity_{stamp}.csv",
        "trades": REPORT_DIR / f"trades_{stamp}.csv",
    }
    pool.to_csv(paths["pool"], index=False, encoding="utf-8-sig")
    signals.to_csv(paths["signals"], index=False, encoding="utf-8-sig")
    equity.to_csv(paths["equity"], index=False, encoding="utf-8-sig")
    trades.to_csv(paths["trades"], index=False, encoding="utf-8-sig")
    return {key: str(path) for key, path in paths.items()}


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--max-symbols", type=int, default=0)
    parser.add_argument("--hold-days", type=int, default=3)
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    warmup_start = (datetime.strptime(args.start_date, "%Y-%m-%d") - timedelta(days=80)).strftime("%Y-%m-%d")
    config = BacktestConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        max_symbols=args.max_symbols,
        hold_days=args.hold_days,
        initial_cash=args.initial_cash,
    )

    print("Fetching stock pool...")
    pool = fetch_stock_pool(config)
    print(f"Filtered pool: {len(pool)} symbols")

    frames = []
    failures = []
    for index, symbol in enumerate(pool["symbol"], start=1):
        try:
            frame = fetch_daily(symbol, warmup_start, config.end_date, refresh=args.refresh)
            if not frame.empty:
                frames.append(frame)
        except Exception as exc:
            failures.append({"symbol": symbol, "error": str(exc)})
        if index % 50 == 0:
            print(f"Downloaded {index}/{len(pool)} symbols, failures={len(failures)}")

    if not frames:
        raise RuntimeError("No daily data available.")
    daily = pd.concat(frames, ignore_index=True)
    daily = daily[daily["trade_date"].between(pd.to_datetime(warmup_start), pd.to_datetime(config.end_date))]
    signals = build_signals(daily, pool, config)
    trade_daily = daily[daily["trade_date"].between(pd.to_datetime(config.start_date), pd.to_datetime(config.end_date))]
    signals = signals[signals["entry_date"].between(pd.to_datetime(config.start_date), pd.to_datetime(config.end_date))] if not signals.empty else signals
    equity, trades = run_backtest(trade_daily, signals, config)
    summary = summarize(equity, trades)
    paths = save_outputs(config, pool, signals, equity, trades)

    print("\nSummary")
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    print("\nCounts")
    print(json.dumps({"pool": len(pool), "daily_symbols": daily["symbol"].nunique(), "signals": len(signals), "failures": len(failures)}, ensure_ascii=False, indent=2))
    if failures:
        print("\nFirst failures")
        print(json.dumps(failures[:5], ensure_ascii=False, indent=2))
    print("\nOutput files")
    print(json.dumps(paths, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
