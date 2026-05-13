from __future__ import annotations

import argparse
from datetime import date, datetime
import multiprocessing as mp
from pathlib import Path
import queue
import sys
import time

import baostock as bs

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from quant_platform.data_sources import DailyBar
from quant_platform.storage import MarketDatabase


def bs_to_symbol(code: str) -> str:
    exchange, raw = code.split(".")
    return f"{raw}.{'SH' if exchange == 'sh' else 'SZ'}"


def symbol_to_bs(symbol: str) -> str:
    raw, exchange = symbol.split(".")
    return f"{'sh' if exchange == 'SH' else 'sz'}.{raw}"


def is_a_share(symbol: str) -> bool:
    return symbol.startswith(("000", "001", "002", "003", "300", "301", "600", "601", "603", "605", "688"))


def fetch_basic() -> list[dict]:
    rs = bs.query_stock_basic()
    rows = []
    while rs.next():
        item = dict(zip(rs.fields, rs.get_row_data()))
        symbol = bs_to_symbol(item["code"])
        name = item.get("code_name", "")
        if item.get("type") != "1" or item.get("status") != "1":
            continue
        if not is_a_share(symbol):
            continue
        if "ST" in name.upper() or "退" in name:
            continue
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "exchange": symbol.split(".")[1],
                "industry": "",
                "list_date": item.get("ipoDate", ""),
                "delist_date": item.get("outDate", ""),
                "is_st": 0,
                "status": item.get("status", ""),
                "float_mv": 0,
            }
        )
    return sorted(rows, key=lambda row: row["symbol"])


def fetch_daily(symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
    fields = "date,code,open,high,low,close,preclose,volume,amount,turn,pctChg,isST"
    rs = bs.query_history_k_data_plus(
        symbol_to_bs(symbol),
        fields,
        start_date=start_date,
        end_date=end_date,
        frequency="d",
        adjustflag="2",
    )
    if rs.error_code != "0":
        raise RuntimeError(rs.error_msg)
    bars: list[DailyBar] = []
    while rs.next():
        row = dict(zip(rs.fields, rs.get_row_data()))
        if not row.get("open"):
            continue
        try:
            bars.append(
                DailyBar(
                    symbol=symbol,
                    trade_date=row["date"],
                    open=float(row["open"]),
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=float(row["close"]),
                    pre_close=float(row["preclose"] or row["open"]),
                    volume=float(row["volume"] or 0),
                    amount=float(row["amount"] or 0),
                    source="baostock",
                    turnover=float(row["turn"] or 0),
                    pct_chg=float(row["pctChg"] or 0),
                    is_st=int(float(row["isST"] or 0)),
                )
            )
        except ValueError:
            continue
    return bars


def _fetch_daily_worker(symbol: str, start_date: str, end_date: str, output: mp.Queue) -> None:
    login = bs.login()
    if login.error_code != "0":
        output.put({"ok": False, "error": login.error_msg})
        return
    try:
        bars = fetch_daily(symbol, start_date, end_date)
        output.put({"ok": True, "bars": [bar.__dict__ for bar in bars]})
    except Exception as exc:
        output.put({"ok": False, "error": str(exc)})
    finally:
        bs.logout()


def fetch_daily_with_timeout(symbol: str, start_date: str, end_date: str, timeout: int) -> list[DailyBar]:
    output: mp.Queue = mp.Queue()
    process = mp.Process(target=_fetch_daily_worker, args=(symbol, start_date, end_date, output))
    process.start()
    process.join(timeout)
    if process.is_alive():
        process.terminate()
        process.join(5)
        raise TimeoutError(f"{symbol} download timed out after {timeout}s")
    try:
        payload = output.get_nowait()
    except queue.Empty as exc:
        raise RuntimeError(f"{symbol} worker exited without result, exitcode={process.exitcode}") from exc
    if not payload.get("ok"):
        raise RuntimeError(payload.get("error", "unknown BaoStock error"))
    return [DailyBar(**row) for row in payload.get("bars", [])]


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


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=date.today().isoformat())
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--refresh", action="store_true")
    parser.add_argument("--sleep", type=float, default=0.03)
    parser.add_argument("--timeout", type=int, default=5)
    parser.add_argument("--retries", type=int, default=0)
    args = parser.parse_args()

    db = MarketDatabase()
    login = bs.login()
    if login.error_code != "0":
        raise RuntimeError(login.error_msg)

    try:
        basics = fetch_basic()
        if args.limit:
            basics = basics[: args.limit]
        db.upsert_basic(basics)
        print(f"Non-ST A-share pool: {len(basics)}", flush=True)

        downloaded = 0
        skipped = 0
        failed = 0
        for index, item in enumerate(basics, start=1):
            symbol = item["symbol"]
            start, end, rows = current_range(db, symbol)
            if not args.refresh and rows > 0 and covers_requested_range(
                start,
                end,
                args.start_date,
                args.end_date,
                item.get("list_date", ""),
            ):
                skipped += 1
            else:
                try:
                    print(f"[fetch] {index}/{len(basics)} {symbol} {item['name']}", flush=True)
                    bars = fetch_daily_with_timeout(symbol, args.start_date, args.end_date, args.timeout)
                    db.upsert_daily(bars)
                    downloaded += len(bars)
                except Exception as exc:
                    retry_exc: Exception | None = None
                    for attempt in range(args.retries):
                        try:
                            time.sleep(0.5)
                            print(
                                f"[retry] {index}/{len(basics)} {symbol} {item['name']} attempt={attempt + 1} after {exc}",
                                flush=True,
                            )
                            bars = fetch_daily_with_timeout(symbol, args.start_date, args.end_date, args.timeout)
                            db.upsert_daily(bars)
                            downloaded += len(bars)
                            retry_exc = None
                            break
                        except Exception as retry_error:
                            retry_exc = retry_error
                    if retry_exc is not None or args.retries == 0:
                        failed += 1
                        print(f"[failed] {symbol} {item['name']}: {exc}; retry: {retry_exc}", flush=True)
            if index % 50 == 0 or index == len(basics):
                print(
                    f"{index}/{len(basics)} done, downloaded_rows={downloaded}, skipped={skipped}, failed={failed}",
                    flush=True,
                )
            time.sleep(args.sleep)
    finally:
        bs.logout()


if __name__ == "__main__":
    main()
