from __future__ import annotations

import argparse
from datetime import date, datetime
import multiprocessing as mp
from pathlib import Path
import queue
import sys
import time

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quant_platform.data_sources import DailyBar
from quant_platform.storage import MarketDatabase


def compact_date(value: str) -> str:
    return value.replace("-", "")


def symbol_to_akshare_sina(symbol: str) -> str:
    code, exchange = symbol.split(".")
    return f"{exchange.lower()}{code}"


def current_range(db: MarketDatabase, symbol: str) -> tuple[str, str, int]:
    with db.connect() as conn:
        row = conn.execute(
            """
            select min(trade_date) as start_date, max(trade_date) as end_date, count(*) as rows
            from stock_daily
            where symbol = ?
            """,
            (symbol,),
        ).fetchone()
    return (row["start_date"] or "", row["end_date"] or "", int(row["rows"] or 0))


def covers_requested_range(
    current_start: str,
    current_end: str,
    requested_start: str,
    requested_end: str,
    list_date: str = "",
) -> bool:
    if not current_start or not current_end:
        return False
    effective_start = requested_start
    if list_date and list_date > requested_start:
        effective_start = list_date
    start_gap = (datetime.strptime(current_start, "%Y-%m-%d") - datetime.strptime(effective_start, "%Y-%m-%d")).days
    end_gap = (datetime.strptime(requested_end, "%Y-%m-%d") - datetime.strptime(current_end, "%Y-%m-%d")).days
    return start_gap <= 7 and end_gap <= 7


def fetch_daily(symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
    import akshare as ak

    ak_symbol = symbol_to_akshare_sina(symbol)
    frame = ak.stock_zh_a_daily(
        symbol=ak_symbol,
        start_date=compact_date(start_date),
        end_date=compact_date(end_date),
        adjust="qfq",
    )
    if frame is None or frame.empty:
        raise RuntimeError(f"{symbol} no rows from AKShare Sina")

    frame = frame.sort_values("date")
    bars: list[DailyBar] = []
    previous_close = 0.0
    for row in frame.to_dict("records"):
        trade_date = str(row.get("date"))
        open_ = float(row.get("open") or 0)
        high = float(row.get("high") or 0)
        low = float(row.get("low") or 0)
        close = float(row.get("close") or 0)
        volume = float(row.get("volume") or 0)
        amount = float(row.get("amount") or 0)
        turnover = float(row.get("turnover") or 0)
        if turnover and turnover <= 1:
            turnover *= 100
        pre_close = previous_close or open_
        pct_chg = ((close / pre_close - 1) * 100) if pre_close else 0.0
        bars.append(
            DailyBar(
                symbol=symbol,
                trade_date=trade_date,
                open=open_,
                high=high,
                low=low,
                close=close,
                pre_close=pre_close,
                volume=volume,
                amount=amount,
                source="akshare_sina",
                turnover=turnover,
                pct_chg=pct_chg,
                is_st=0,
            )
        )
        previous_close = close
    return bars


def _fetch_worker(symbol: str, start_date: str, end_date: str, output: mp.Queue) -> None:
    try:
        bars = fetch_daily(symbol, start_date, end_date)
        output.put({"ok": True, "bars": [bar.__dict__ for bar in bars]})
    except Exception as exc:
        output.put({"ok": False, "error": str(exc)})


def fetch_daily_with_timeout(symbol: str, start_date: str, end_date: str, timeout: int) -> list[DailyBar]:
    output: mp.Queue = mp.Queue()
    process = mp.Process(target=_fetch_worker, args=(symbol, start_date, end_date, output))
    process.start()
    try:
        payload = output.get(timeout=timeout)
    except queue.Empty as exc:
        process.terminate()
        process.join(5)
        raise TimeoutError(f"{symbol} download timed out after {timeout}s") from exc
    process.join(5)
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error", "unknown AKShare error"))
    return [DailyBar(**row) for row in payload.get("bars", [])]


def load_stock_pool(db: MarketDatabase) -> list[dict]:
    with db.connect() as conn:
        rows = conn.execute(
            """
            select symbol, name, list_date
            from stock_basic
            where is_st = 0
            order by symbol
            """
        ).fetchall()
    return [dict(row) for row in rows]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--sleep", type=float, default=0.5)
    parser.add_argument("--timeout", type=int, default=20)
    parser.add_argument("--max-consecutive-failures", type=int, default=80)
    args = parser.parse_args()

    db = MarketDatabase()
    pool = load_stock_pool(db)
    if args.limit:
        pool = pool[: args.limit]

    print(f"AKShare Sina missing-data pool: {len(pool)}", flush=True)
    downloaded_rows = 0
    skipped = 0
    failed = 0
    consecutive_failures = 0
    processed_missing = 0

    for index, item in enumerate(pool, start=1):
        symbol = item["symbol"]
        name = item.get("name", "")
        current_start, current_end, rows = current_range(db, symbol)
        if rows > 0 and covers_requested_range(
            current_start,
            current_end,
            args.start_date,
            args.end_date,
            item.get("list_date", ""),
        ):
            skipped += 1
        else:
            processed_missing += 1
            try:
                print(f"[fetch] {index}/{len(pool)} {symbol} {name}", flush=True)
                bars = fetch_daily_with_timeout(symbol, args.start_date, args.end_date, args.timeout)
                db.upsert_daily(bars)
                downloaded_rows += len(bars)
                consecutive_failures = 0
                print(f"[ok] {symbol} {name}: rows={len(bars)}", flush=True)
            except Exception as exc:
                failed += 1
                consecutive_failures += 1
                print(f"[failed] {symbol} {name}: {exc}", flush=True)
                if consecutive_failures >= args.max_consecutive_failures:
                    print(
                        f"[stop] consecutive failures reached {consecutive_failures}; data source may be blocked",
                        flush=True,
                    )
                    break
            time.sleep(args.sleep)

        if index % 50 == 0 or index == len(pool):
            print(
                f"{index}/{len(pool)} scanned, missing_processed={processed_missing}, "
                f"downloaded_rows={downloaded_rows}, skipped={skipped}, failed={failed}",
                flush=True,
            )


if __name__ == "__main__":
    main()
