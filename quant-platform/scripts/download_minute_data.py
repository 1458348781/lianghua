from __future__ import annotations

import argparse
import contextlib
import io
import os
import sqlite3
import sys
import time
import urllib.parse
import urllib.request
import json
from dataclasses import dataclass
from datetime import datetime, timedelta
from multiprocessing import Pool, current_process
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from quant_platform.config import DATABASE_PATH  # noqa: E402
from quant_platform.data_sources import compact_date, normalize_symbol  # noqa: E402
from quant_platform.screener import scan_start_with_buffer  # noqa: E402
from quant_platform.storage import DataPortal, MarketDatabase  # noqa: E402
from quant_platform.strategy import DivergenceStrategy  # noqa: E402


DEFAULT_PARAMS: dict[str, Any] = {
    "max_positions": 2,
    "hold_days": 5,
    "stop_loss": -0.02,
    "strong_close_pct_chg": 5.0,
    "min_price": 3,
    "max_price": 500,
    "min_turnover": 1.9,
    "max_turnover": 33.2,
    "day1_min_volume_ratio": 1.03,
    "day1_max_volume_ratio": 3.2,
    "range_min_amplitude_30": 0.214,
    "range_min_return_20": -0.005,
    "day2_min_pct_chg": -4.0,
    "day2_max_pct_chg": 8.0,
    "day2_max_volume_ratio": 1.94,
    "day2_min_close_position": 0.41,
    "day2_max_upper_shadow": 0.086,
    "day2_min_close_vs_day1_close": 0.963,
    "entry_min_open_gap_pct_chg": 1.3,
    "entry_max_open_gap_pct_chg": 6.5,
    "entry_min_high_from_open_pct_chg": 3.3,
}


@dataclass(frozen=True)
class MinuteTask:
    symbol: str
    start_date: str
    end_date: str
    mode: str
    source_key: str


def main() -> None:
    parser = argparse.ArgumentParser(description="Download A-share 1-minute data into the local quant database.")
    parser.add_argument("--mode", choices=["candidate", "year"], default="candidate", help="candidate first, then year for broad backfill.")
    parser.add_argument("--start-date", default="2020-01-01")
    parser.add_argument("--end-date", default=datetime.now().date().isoformat())
    parser.add_argument("--year", type=int, help="Year to backfill in --mode year, for example 2025.")
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--workers", type=int, default=10)
    parser.add_argument("--min-rows", type=int, default=30)
    parser.add_argument("--candidate-forward-days", type=int, default=5, help="Download T+2 through the next N calendar days for each candidate.")
    parser.add_argument("--limit", type=int, default=0, help="Optional task limit for testing.")
    parser.add_argument("--overwrite", action="store_true", help="Download even if the same symbol/date range was logged before.")
    parser.add_argument("--sleep", type=float, default=0.05, help="Small per-task sleep to avoid hammering the data source.")
    parser.add_argument("--source", choices=["sina", "eastmoney", "auto"], default="sina", help="Minute source. sina uses AkShare Sina intraday ticks and aggregates to 1-minute bars.")
    parser.add_argument("--akshare-fallback", action="store_true", help="Use AkShare as fallback. Default is off because it may read broken system proxy settings.")
    args = parser.parse_args()
    configure_network_env()
    validate_runtime(args.source, args.akshare_fallback)

    init_minute_schema(DATABASE_PATH)
    tasks = build_candidate_tasks(args) if args.mode == "candidate" else build_year_tasks(args)
    if args.limit > 0:
        tasks = tasks[: args.limit]
    if not args.overwrite:
        tasks = skip_logged_tasks(tasks)
    print(
        f"[minute] mode={args.mode} source={args.source} tasks={len(tasks)} workers={args.workers} python={sys.executable} db={DATABASE_PATH}",
        flush=True,
    )
    if not tasks:
        return

    workers = max(1, int(args.workers))
    ok = failed = rows = 0
    started = time.perf_counter()
    result_iter: Any
    if workers == 1:
        worker_init(str(DATABASE_PATH), args.sleep, bool(args.akshare_fallback), args.source)
        result_iter = (run_task(task) for task in tasks)
        for item in result_iter:
            ok, failed, rows = handle_progress(item, ok, failed, rows, len(tasks), started)
    else:
        with Pool(
            processes=workers,
            initializer=worker_init,
            initargs=(str(DATABASE_PATH), args.sleep, bool(args.akshare_fallback), args.source),
        ) as pool:
            for item in pool.imap_unordered(run_task, tasks, chunksize=1):
                ok, failed, rows = handle_progress(item, ok, failed, rows, len(tasks), started)


def handle_progress(
    item: dict[str, Any],
    ok: int,
    failed: int,
    rows: int,
    total: int,
    started: float,
) -> tuple[int, int, int]:
    if item["ok"]:
        ok += 1
        rows += int(item["rows"])
    else:
        failed += 1
    done = ok + failed
    if done == 1 or done % 20 == 0 or done == total:
        elapsed = time.perf_counter() - started
        print(
            f"[minute] done={done}/{total} ok={ok} failed={failed} rows={rows} elapsed={elapsed:.1f}s last={item['symbol']} {item['start_date']}..{item['end_date']} {item['message']}",
            flush=True,
        )
    return ok, failed, rows


def init_minute_schema(path: Path) -> None:
    with sqlite3.connect(path, timeout=60) as conn:
        conn.execute("pragma journal_mode=wal")
        conn.executescript(
            """
            create table if not exists stock_minute (
                symbol text not null,
                trade_time text not null,
                trade_date text not null,
                open real not null,
                high real not null,
                low real not null,
                close real not null,
                volume real not null default 0,
                amount real not null default 0,
                source text not null,
                updated_at text not null default current_timestamp,
                primary key (symbol, trade_time)
            );
            create index if not exists idx_stock_minute_date on stock_minute(trade_date);
            create index if not exists idx_stock_minute_symbol_date on stock_minute(symbol, trade_date);
            create table if not exists minute_download_log (
                source_key text primary key,
                symbol text not null,
                start_date text not null,
                end_date text not null,
                mode text not null,
                rows integer not null default 0,
                status text not null,
                message text not null default '',
                updated_at text not null default current_timestamp
            );
            """
        )


def build_candidate_tasks(args: argparse.Namespace) -> list[MinuteTask]:
    db = MarketDatabase()
    history_start = scan_start_with_buffer(args.start_date)
    pool = db.list_backtest_symbols(history_start, args.end_date, args.min_rows, args.board)
    symbols = [item["symbol"] for item in pool]
    history = DataPortal(db).get_prices(symbols, history_start, args.end_date)
    strategy = DivergenceStrategy(**DEFAULT_PARAMS)
    tasks: dict[str, MinuteTask] = {}
    for symbol, rows in history.items():
        if len(rows) < 35:
            continue
        dates = [row["trade_date"] for row in rows]
        for index in range(33, len(rows) - 1):
            t = rows[index - 1]
            t1 = rows[index]
            t2 = rows[index + 1]
            signal_date = t2["trade_date"]
            if signal_date < args.start_date or signal_date > args.end_date:
                continue
            window = rows[index - 33 : index + 2]
            if not strategy._setup_ok(symbol, t, t1, window):  # type: ignore[attr-defined]
                continue
            end_index = min(len(dates) - 1, index + 1 + int(args.candidate_forward_days))
            start_date = dates[index + 1]
            end_date = dates[end_index]
            key = f"candidate:{symbol}:{start_date}:{end_date}"
            tasks[key] = MinuteTask(symbol=symbol, start_date=start_date, end_date=end_date, mode="candidate", source_key=key)
    return sorted(tasks.values(), key=lambda item: (item.start_date, item.symbol))


def build_year_tasks(args: argparse.Namespace) -> list[MinuteTask]:
    if not args.year:
        raise SystemExit("--mode year 需要指定 --year，例如 --year 2025")
    db = MarketDatabase()
    start_date = f"{args.year}-01-01"
    end_date = f"{args.year}-12-31"
    pool = db.list_backtest_symbols(start_date, end_date, args.min_rows, args.board)
    tasks = []
    for item in pool:
        symbol = item["symbol"]
        key = f"year:{symbol}:{start_date}:{end_date}"
        tasks.append(MinuteTask(symbol=symbol, start_date=start_date, end_date=end_date, mode="year", source_key=key))
    return tasks


def skip_logged_tasks(tasks: list[MinuteTask]) -> list[MinuteTask]:
    if not tasks:
        return tasks
    keys = [task.source_key for task in tasks]
    done: set[str] = set()
    with sqlite3.connect(DATABASE_PATH, timeout=60) as conn:
        for index in range(0, len(keys), 500):
            chunk = keys[index : index + 500]
            placeholders = ",".join("?" for _ in chunk)
            rows = conn.execute(
                f"select source_key from minute_download_log where status = 'ok' and rows > 0 and source_key in ({placeholders})",
                tuple(chunk),
            ).fetchall()
            done.update(row[0] for row in rows)
    return [task for task in tasks if task.source_key not in done]


WORKER_DB = ""
WORKER_SLEEP = 0.0
WORKER_AKSHARE_FALLBACK = False
WORKER_SOURCE = "sina"


def worker_init(db_path: str, sleep_seconds: float, akshare_fallback: bool = False, source: str = "sina") -> None:
    global WORKER_DB, WORKER_SLEEP, WORKER_AKSHARE_FALLBACK, WORKER_SOURCE
    configure_network_env()
    WORKER_DB = db_path
    WORKER_SLEEP = sleep_seconds
    WORKER_AKSHARE_FALLBACK = akshare_fallback
    WORKER_SOURCE = source


def run_task(task: MinuteTask) -> dict[str, Any]:
    symbol = normalize_symbol(task.symbol)
    try:
        if WORKER_SLEEP > 0:
            time.sleep(WORKER_SLEEP)
        rows = fetch_minute_rows(symbol, task.start_date, task.end_date)
        upsert_minute_rows(Path(WORKER_DB), rows)
        status = "ok" if rows else "empty"
        message = "ok" if rows else "no rows returned"
        log_download(Path(WORKER_DB), task, len(rows), status, message)
        return result(task, True, len(rows), message)
    except Exception as exc:
        message = f"{type(exc).__name__}: {exc}"
        log_download(Path(WORKER_DB), task, 0, "failed", message)
        return result(task, False, 0, message)


def result(task: MinuteTask, ok: bool, rows: int, message: str) -> dict[str, Any]:
    return {
        "ok": ok,
        "rows": rows,
        "message": message,
        "symbol": task.symbol,
        "start_date": task.start_date,
        "end_date": task.end_date,
        "worker": current_process().name,
    }


def fetch_minute_rows(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    if WORKER_SOURCE == "sina":
        return fetch_minute_rows_sina_intraday(symbol, start_date, end_date)
    if WORKER_SOURCE == "eastmoney":
        return fetch_minute_rows_eastmoney(symbol, start_date, end_date)

    eastmoney_error: Exception | None = None
    try:
        rows = fetch_minute_rows_eastmoney(symbol, start_date, end_date)
        if rows:
            return rows
        return []
    except Exception as exc:
        eastmoney_error = exc
    try:
        rows = fetch_minute_rows_sina_intraday(symbol, start_date, end_date)
        if rows:
            return rows
    except Exception:
        pass
    if WORKER_AKSHARE_FALLBACK:
        return fetch_minute_rows_akshare(symbol, start_date, end_date)
    if eastmoney_error:
        raise eastmoney_error
    return []


def fetch_minute_rows_sina_intraday(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        raise RuntimeError("AkShare 未安装，无法下载新浪分时数据") from exc

    symbol = normalize_symbol(symbol)
    sina_symbol = symbol_to_akshare_sina(symbol)
    rows: list[dict[str, Any]] = []
    for trade_date in local_trade_dates(symbol, start_date, end_date):
        try:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                frame = ak.stock_intraday_sina(symbol=sina_symbol, date=compact_date(trade_date))
        except Exception as exc:
            message = str(exc)
            if "ticktime" in message or "No columns" in message:
                continue
            raise
        if frame is None or frame.empty or "ticktime" not in frame.columns or "price" not in frame.columns:
            continue
        rows.extend(aggregate_sina_ticks(symbol, trade_date, frame))
    return rows


def aggregate_sina_ticks(symbol: str, trade_date: str, frame: Any) -> list[dict[str, Any]]:
    records: dict[str, list[tuple[float, float]]] = {}
    for row in frame.to_dict("records"):
        ticktime = str(row.get("ticktime") or "")
        if not ("09:30:00" <= ticktime <= "15:00:00"):
            continue
        price = to_float(row.get("price"))
        volume = to_float(row.get("volume"))
        if price <= 0:
            continue
        minute = ticktime[:5]
        key = f"{trade_date} {minute}:00"
        records.setdefault(key, []).append((price, volume))

    bars: list[dict[str, Any]] = []
    for trade_time, ticks in sorted(records.items()):
        prices = [item[0] for item in ticks]
        volume = sum(item[1] for item in ticks)
        close = prices[-1]
        bars.append(
            {
                "symbol": symbol,
                "trade_time": trade_time,
                "trade_date": trade_date,
                "open": prices[0],
                "high": max(prices),
                "low": min(prices),
                "close": close,
                "volume": volume,
                "amount": close * volume,
                "source": "akshare_sina_intraday",
            }
        )
    return bars


def local_trade_dates(symbol: str, start_date: str, end_date: str) -> list[str]:
    with sqlite3.connect(WORKER_DB, timeout=60) as conn:
        rows = conn.execute(
            """
            select trade_date
            from stock_daily
            where symbol = ? and trade_date between ? and ?
            order by trade_date
            """,
            (symbol, start_date, end_date),
        ).fetchall()
    return [row[0] for row in rows]


def symbol_to_akshare_sina(symbol: str) -> str:
    code, exchange = normalize_symbol(symbol).split(".")
    return f"{exchange.lower()}{code}"


def configure_network_env() -> None:
    for key in (
        "HTTP_PROXY",
        "HTTPS_PROXY",
        "ALL_PROXY",
        "http_proxy",
        "https_proxy",
        "all_proxy",
    ):
        os.environ.pop(key, None)
    os.environ["NO_PROXY"] = "*"
    os.environ["no_proxy"] = "*"


def validate_runtime(source: str, akshare_fallback: bool) -> None:
    if source != "sina" and not akshare_fallback:
        return
    try:
        import akshare  # noqa: F401
    except Exception as exc:
        raise SystemExit(
            "当前 Python 环境没有安装 AkShare。请用项目虚拟环境运行：\n"
            "  D:\\lianghua\\quant-platform\\.venv\\Scripts\\python.exe "
            "D:\\lianghua\\quant-platform\\scripts\\download_minute_data.py --mode candidate --start-date 2026-01-01 --end-date 2026-05-10 --board all --workers 10 --source sina\n"
            "或者直接运行：\n"
            "  D:\\lianghua\\quant-platform\\scripts\\run_minute_candidate.ps1"
        ) from exc


def fetch_minute_rows_eastmoney(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    symbol = normalize_symbol(symbol)
    code, exchange = symbol.split(".")
    secid = f"1.{code}" if exchange == "SH" else f"0.{code}"
    params = {
        "secid": secid,
        "fields1": "f1,f2,f3,f4,f5,f6",
        "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
        "klt": "1",
        "fqt": "1",
        "beg": f"{compact_date(start_date)}093000",
        "end": f"{compact_date(end_date)}150000",
    }
    url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?" + urllib.parse.urlencode(params)
    req = urllib.request.Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 quant-platform/0.1",
            "Referer": "https://quote.eastmoney.com/",
        },
    )
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
    last_error: Exception | None = None
    for attempt in range(3):
        try:
            with opener.open(req, timeout=20) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
            data = payload.get("data") or {}
            klines = data.get("klines") or []
            rows: list[dict[str, Any]] = []
            for line in klines:
                parts = line.split(",")
                if len(parts) < 7:
                    continue
                trade_time, open_, close, high, low, volume, amount = parts[:7]
                trade_time = normalize_trade_time(trade_time)
                rows.append(
                    {
                        "symbol": symbol,
                        "trade_time": trade_time,
                        "trade_date": trade_time[:10],
                        "open": to_float(open_),
                        "high": to_float(high),
                        "low": to_float(low),
                        "close": to_float(close),
                        "volume": to_float(volume),
                        "amount": to_float(amount),
                        "source": "eastmoney_min_qfq",
                    }
                )
            return [row for row in rows if row["open"] > 0 and row["close"] > 0]
        except Exception as exc:
            last_error = exc
            time.sleep(0.5 * (attempt + 1))
    if last_error:
        raise last_error
    return []


def fetch_minute_rows_akshare(symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
    try:
        import akshare as ak  # type: ignore
    except Exception as exc:
        raise RuntimeError("AkShare 未安装，无法下载历史分钟线") from exc

    code = normalize_symbol(symbol).split(".")[0]
    start = f"{start_date} 09:30:00"
    end = f"{end_date} 15:00:00"
    frame = ak.stock_zh_a_hist_min_em(
        symbol=code,
        start_date=start,
        end_date=end,
        period="1",
        adjust="qfq",
    )
    if frame is None or frame.empty:
        return []
    records: list[dict[str, Any]] = []
    for row in frame.to_dict("records"):
        trade_time = pick(row, "时间", "日期", "day", "datetime")
        if not trade_time:
            continue
        trade_time = normalize_trade_time(str(trade_time))
        records.append(
            {
                "symbol": symbol,
                "trade_time": trade_time,
                "trade_date": trade_time[:10],
                "open": to_float(pick(row, "开盘", "open")),
                "high": to_float(pick(row, "最高", "high")),
                "low": to_float(pick(row, "最低", "low")),
                "close": to_float(pick(row, "收盘", "close")),
                "volume": to_float(pick(row, "成交量", "volume", "vol")),
                "amount": to_float(pick(row, "成交额", "amount")),
                "source": "akshare_min_em_qfq",
            }
        )
    return [row for row in records if row["open"] > 0 and row["close"] > 0]


def upsert_minute_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    if not rows:
        return
    with sqlite3.connect(path, timeout=60) as conn:
        conn.execute("pragma journal_mode=wal")
        conn.executemany(
            """
            insert into stock_minute
            (symbol, trade_time, trade_date, open, high, low, close, volume, amount, source, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            on conflict(symbol, trade_time) do update set
                trade_date=excluded.trade_date,
                open=excluded.open,
                high=excluded.high,
                low=excluded.low,
                close=excluded.close,
                volume=excluded.volume,
                amount=excluded.amount,
                source=excluded.source,
                updated_at=current_timestamp
            """,
            [
                (
                    row["symbol"],
                    row["trade_time"],
                    row["trade_date"],
                    row["open"],
                    row["high"],
                    row["low"],
                    row["close"],
                    row["volume"],
                    row["amount"],
                    row["source"],
                )
                for row in rows
            ],
        )


def log_download(path: Path, task: MinuteTask, rows: int, status: str, message: str) -> None:
    with sqlite3.connect(path, timeout=60) as conn:
        conn.execute(
            """
            insert into minute_download_log
            (source_key, symbol, start_date, end_date, mode, rows, status, message, updated_at)
            values (?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
            on conflict(source_key) do update set
                rows=excluded.rows,
                status=excluded.status,
                message=excluded.message,
                updated_at=current_timestamp
            """,
            (task.source_key, task.symbol, task.start_date, task.end_date, task.mode, rows, status, message[:800]),
        )


def pick(row: dict[str, Any], *names: str) -> Any:
    for name in names:
        if name in row:
            return row[name]
    return None


def to_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def normalize_trade_time(value: str) -> str:
    value = value.replace("/", "-")
    if len(value) == 10:
        return f"{value} 00:00:00"
    try:
        return datetime.fromisoformat(value).strftime("%Y-%m-%d %H:%M:%S")
    except ValueError:
        return value[:19]


if __name__ == "__main__":
    main()
