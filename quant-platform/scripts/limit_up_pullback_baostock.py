from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any

import baostock as bs
import pandas as pd

from limit_up_pullback_strategy import (
    BacktestConfig,
    build_signals,
    run_backtest,
    save_outputs,
    summarize,
)


ROOT = Path(__file__).resolve().parents[1]
CACHE_DIR = ROOT / "data" / "cache" / "limit_up_pullback_baostock"


def bs_to_symbol(code: str) -> str:
    exchange, raw = code.split(".")
    return f"{raw}.{'SH' if exchange == 'sh' else 'SZ'}"


def symbol_to_bs(symbol: str) -> str:
    raw, exchange = symbol.split(".")
    return f"{'sh' if exchange == 'SH' else 'sz'}.{raw}"


def fetch_pool(config: BacktestConfig) -> pd.DataFrame:
    rs = bs.query_stock_basic()
    rows = []
    while rs.next():
        rows.append(dict(zip(rs.fields, rs.get_row_data())))
    if not rows:
        raise RuntimeError("BaoStock returned empty stock basic table.")

    frame = pd.DataFrame(rows)
    frame["symbol"] = frame["code"].map(bs_to_symbol)
    frame["list_date"] = pd.to_datetime(frame["ipoDate"], errors="coerce")
    start = pd.to_datetime(config.start_date)
    frame["list_days_at_start"] = (start - frame["list_date"]).dt.days
    name = frame["code_name"].astype(str)
    code = frame["symbol"].astype(str)

    filtered = frame[
        frame["type"].eq("1")
        & frame["status"].eq("1")
        & ~name.str.contains("ST|退", regex=True, na=False)
        & frame["list_days_at_start"].ge(config.min_list_days)
        & code.str.startswith(("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688"))
    ].copy()
    filtered = filtered.sort_values("symbol")
    if config.max_symbols > 0:
        filtered = filtered.head(config.max_symbols)
    filtered["price"] = 0.0
    filtered["float_mv"] = 0.0
    return filtered.rename(columns={"code_name": "name"})[
        ["symbol", "code", "name", "price", "float_mv", "list_date", "list_days_at_start"]
    ]


def fetch_daily(symbol: str, start_date: str, end_date: str, refresh: bool = False) -> pd.DataFrame:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{symbol}_{start_date}_{end_date}.csv"
    if path.exists() and not refresh:
        return pd.read_csv(path, parse_dates=["trade_date"])

    fields = "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST"
    rs = bs.query_history_k_data_plus(
        symbol_to_bs(symbol),
        fields,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2",
    )
    rows = []
    while rs.next():
        rows.append(dict(zip(rs.fields, rs.get_row_data())))
    if not rows:
        return pd.DataFrame()

    frame = pd.DataFrame(rows)
    frame = frame.rename(
        columns={
            "date": "trade_date",
            "turn": "turnover",
            "pctChg": "pct_chg",
            "preclose": "pre_close",
        }
    )
    for column in ["open", "high", "low", "close", "pre_close", "volume", "amount", "turnover", "pct_chg"]:
        frame[column] = pd.to_numeric(frame[column], errors="coerce")
    frame["trade_date"] = pd.to_datetime(frame["trade_date"])
    frame["symbol"] = symbol
    frame["amplitude"] = (frame["high"] - frame["low"]) / frame["pre_close"] * 100
    frame["change"] = frame["close"] - frame["pre_close"]
    frame = frame.dropna(subset=["open", "high", "low", "close", "pre_close", "volume", "turnover", "pct_chg"])
    frame.to_csv(path, index=False, encoding="utf-8-sig")
    return frame


def apply_price_filter(pool: pd.DataFrame, daily: pd.DataFrame, config: BacktestConfig) -> tuple[pd.DataFrame, pd.DataFrame]:
    first_window = daily[daily["trade_date"].ge(pd.to_datetime(config.start_date))]
    first_prices = first_window.sort_values("trade_date").groupby("symbol").first()["close"]
    keep = first_prices[first_prices.between(config.min_price, config.max_price)].index
    return pool[pool["symbol"].isin(keep)].copy(), daily[daily["symbol"].isin(keep)].copy()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2024-01-01")
    parser.add_argument("--end-date", default="2024-12-31")
    parser.add_argument("--max-symbols", type=int, default=300)
    parser.add_argument("--sample-seed", type=int, default=42)
    parser.add_argument("--hold-days", type=int, default=3)
    parser.add_argument("--initial-cash", type=float, default=1_000_000)
    parser.add_argument("--refresh", action="store_true")
    args = parser.parse_args()

    warmup_start = (datetime.strptime(args.start_date, "%Y-%m-%d") - timedelta(days=100)).strftime("%Y-%m-%d")
    config = BacktestConfig(
        start_date=args.start_date,
        end_date=args.end_date,
        max_symbols=0,
        hold_days=args.hold_days,
        initial_cash=args.initial_cash,
    )

    lg = bs.login()
    if lg.error_code != "0":
        raise RuntimeError(lg.error_msg)
    try:
        print("Fetching BaoStock stock pool...", flush=True)
        pool = fetch_pool(config)
        if args.max_symbols > 0 and len(pool) > args.max_symbols:
            pool = pool.sample(n=args.max_symbols, random_state=args.sample_seed).sort_values("symbol")
        print(f"Pool before price filter: {len(pool)} symbols", flush=True)
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
                print(f"Downloaded {index}/{len(pool)} symbols, failures={len(failures)}", flush=True)
            time.sleep(0.03)

        daily = pd.concat(frames, ignore_index=True)
        pool, daily = apply_price_filter(pool, daily, config)
        print(f"Pool after price filter: {len(pool)} symbols")
        daily = daily[daily["isST"].astype(str).eq("0")]
        signals = build_signals(daily, pool, config)
        trade_daily = daily[daily["trade_date"].between(pd.to_datetime(config.start_date), pd.to_datetime(config.end_date))]
        if not signals.empty:
            signals = signals[signals["entry_date"].between(pd.to_datetime(config.start_date), pd.to_datetime(config.end_date))]
        equity, trades = run_backtest(trade_daily, signals, config)
        summary = summarize(equity, trades)
        paths = save_outputs(config, pool, signals, equity, trades)

        print("\nSummary")
        print(json.dumps(summary, ensure_ascii=False, indent=2))
        print("\nCounts")
        print(
            json.dumps(
                {
                    "provider": "baostock",
                    "pool": len(pool),
                    "daily_symbols": daily["symbol"].nunique(),
                    "signals": len(signals),
                    "failures": len(failures),
                    "float_market_value_filter": "not_available_in_baostock",
                },
                ensure_ascii=False,
                indent=2,
            )
        )
        if failures:
            print("\nFirst failures")
            print(json.dumps(failures[:5], ensure_ascii=False, indent=2))
        print("\nOutput files")
        print(json.dumps(paths, ensure_ascii=False, indent=2))
    finally:
        bs.logout()


if __name__ == "__main__":
    main()
