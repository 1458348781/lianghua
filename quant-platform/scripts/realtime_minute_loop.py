from __future__ import annotations

import argparse
import json
import sys
import time
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor, as_completed
from datetime import datetime, time as dt_time, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(ROOT / "scripts") not in sys.path:
    sys.path.insert(0, str(ROOT / "scripts"))

from download_minute_data import (  # noqa: E402
    configure_network_env,
)
from download_missing_akshare_sina import fetch_daily as fetch_akshare_sina_daily  # noqa: E402
from quant_platform.realtime import fetch_sina_minutes, fetch_sina_realtime  # noqa: E402
from quant_platform.data_sources import DailyBar, get_source, normalize_symbol  # noqa: E402
from quant_platform.storage import MarketDatabase  # noqa: E402
from quant_platform.strategy import DivergenceStrategy  # noqa: E402


RUNTIME_DIR = ROOT / "data" / "runtime"
INITIAL_CAPITAL = 1_000_000.0

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


def main() -> None:
    parser = argparse.ArgumentParser(description="Realtime minute data loop for divergence candidates.")
    parser.add_argument("--board", choices=["all", "main", "chinext", "star"], default="all")
    parser.add_argument("--interval", type=int, default=30, help="Refresh interval in seconds.")
    parser.add_argument("--limit", type=int, default=0, help="Limit candidate symbols for testing.")
    parser.add_argument("--once", action="store_true", help="Run one refresh and exit.")
    parser.add_argument("--include-invalid", action="store_true", help="Also download candidates with invalid open gap.")
    parser.add_argument("--candidate-dir", default=str(RUNTIME_DIR), help="Directory for tomorrow candidate pool files.")
    parser.add_argument("--use-candidate-pool", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--fallback-full-scan", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--minute-refresh-interval", type=int, default=30, help="Minimum seconds between minute downloads for the same candidate.")
    parser.add_argument("--candidate-minute-workers", type=int, default=4, help="Worker count for candidate minute downloads.")
    parser.add_argument("--start", default="09:25", help="Loop start time, HH:MM.")
    parser.add_argument("--end", default="15:00", help="Candidate loop end time, HH:MM.")
    parser.add_argument("--after-close-download", action="store_true", help="Download all market minute data after close.")
    parser.add_argument("--close-download-start", default="15:00", help="Full-market minute download start time, HH:MM.")
    parser.add_argument("--after-close-workers", type=int, default=8, help="Worker count for full-market minute download.")
    parser.add_argument("--after-close-limit", type=int, default=0, help="Limit full-market symbols for testing.")
    parser.add_argument("--after-close-skip-min-rows", type=int, default=100000, help="Skip full download if date already has enough minute rows.")
    parser.add_argument("--after-close-daily-download", action=argparse.BooleanOptionalAction, default=True, help="Download full-market daily bars after close.")
    parser.add_argument("--after-close-skip-min-daily-rows", type=int, default=4800, help="Skip daily download if date already has enough daily rows.")
    parser.add_argument("--after-close-daily-complete-ratio", type=float, default=0.98, help="Minimum daily row ratio before treating after-close daily data as complete.")
    parser.add_argument("--after-close-daily-source", choices=["auto", "akshare_sina", "eastmoney"], default="akshare_sina")
    parser.add_argument("--after-close-daily-workers", type=int, default=8, help="Worker count for full-market daily download.")
    parser.add_argument("--after-close-daily-retries", type=int, default=3, help="Retry rounds for failed daily downloads.")
    parser.add_argument("--after-close-daily-retry-workers", type=int, default=1, help="Worker count for retry rounds.")
    parser.add_argument("--after-close-daily-retry-sleep", type=float, default=3.0, help="Sleep seconds before each retry round.")
    parser.add_argument("--exit-after-close", action="store_true", help="Exit after one after-close full-market download.")
    parser.add_argument("--position-file", default=str(ROOT / "config" / "watch_positions.json"))
    parser.add_argument("--initial-capital", type=float, default=INITIAL_CAPITAL, help="Paper capital used for dynamic position sizing.")
    parser.add_argument("--position-amount", type=float, default=0.0, help=argparse.SUPPRESS)
    parser.add_argument("--no-auto-track-positions", action="store_true", help="Do not add triggered symbols to positions.")
    args = parser.parse_args()

    configure_network_env()
    db = MarketDatabase()

    print(
        f"[rt-minute] board={args.board} interval={args.interval}s once={args.once} python={sys.executable} db={db.path}",
        flush=True,
    )
    completed_full_dates: set[str] = set()
    runtime_state: dict[str, Any] = {"minute_download_at": {}}
    while True:
        now = datetime.now()
        if args.once or in_time_window(now, args.start, args.end):
            try:
                run_once(args, runtime_state)
            except Exception as exc:
                print(f"[rt-minute] refresh failed: {type(exc).__name__}: {exc}", flush=True)
        elif should_run_after_close(now, args, completed_full_dates):
            try:
                trade_date = run_after_close_full_market(args)
                if trade_date:
                    completed_full_dates.add(trade_date)
                    if args.exit_after_close:
                        break
            except Exception as exc:
                print(f"[rt-minute] after-close full download failed: {type(exc).__name__}: {exc}", flush=True)
        else:
            print(
                f"[rt-minute] waiting for candidate window {args.start}-{args.end}"
                f"{' or after-close download' if args.after_close_download else ''}, now={now:%H:%M:%S}",
                flush=True,
            )

        if args.once:
            break
        time.sleep(max(1, int(args.interval)))


def run_once(args: argparse.Namespace, state: dict[str, Any] | None = None) -> None:
    started = time.perf_counter()
    db = MarketDatabase()
    strategy = DivergenceStrategy(**DEFAULT_PARAMS)
    state = state if state is not None else {"minute_download_at": {}}
    target_date = datetime.now().date().isoformat()
    pool = load_candidate_pool_for_date(Path(args.candidate_dir), target_date, db) if args.use_candidate_pool else None
    if pool and not candidate_pool_daily_complete(pool, args):
        pool = None
    if not pool and args.use_candidate_pool:
        pool = maybe_generate_candidate_pool_for_date(args, target_date, db, state)
    if pool:
        symbols = [item["symbol"] for item in pool.get("candidates", [])]
        print(
            f"[rt-minute] using candidate pool target={pool.get('target_date')} "
            f"setup={pool.get('setup_date')} symbols={len(symbols)}",
            flush=True,
        )
    elif args.fallback_full_scan:
        symbols = [item["symbol"] for item in db.list_realtime_symbols(args.board, 34)]
    else:
        print("[rt-minute] no valid candidate pool and fallback full scan is disabled", flush=True)
        return
    if args.limit > 0:
        symbols = symbols[: args.limit]
    quotes = fetch_sina_realtime(symbols)
    if not quotes:
        print("[rt-minute] no realtime quotes returned", flush=True)
        return

    quote_date = max((quote.get("quote_date") or "") for quote in quotes.values())
    if quote_date != target_date:
        print(
            f"[rt-minute] waiting for today quotes target={target_date} quote={quote_date or '-'} symbols={len(symbols)}",
            flush=True,
        )
        return
    active_quotes = {symbol: quote for symbol, quote in quotes.items() if quote.get("quote_date") == quote_date}
    history = db.query_recent_before(list(active_quotes), quote_date, 40)
    profiles = db.symbol_profiles(list(active_quotes))
    candidates: list[str] = []
    position_rows: list[dict[str, Any]] = []
    triggered = invalid = 0

    for symbol, quote in active_quotes.items():
        rows = history.get(symbol, [])
        if len(rows) < 34:
            continue
        t = rows[-2]
        t1 = rows[-1]
        t2 = {
            "symbol": symbol,
            "trade_date": quote_date,
            "open": quote["open"],
            "high": quote["high"],
            "low": quote["low"],
            "close": quote["price"],
            "pre_close": quote["pre_close"],
            "volume": quote["volume"],
            "amount": quote["amount"],
            "turnover": 0,
            "pct_chg": quote["pct_chg"],
            "is_st": 0,
            "source": "sina_realtime",
        }
        if not strategy._setup_ok(symbol, t, t1, (rows + [t2])[-35:]):  # type: ignore[attr-defined]
            continue
        if strategy._entry_day_ok(t1, t2):  # type: ignore[attr-defined]
            triggered += 1
            candidates.append(symbol)
            buy_price = strategy._entry_price(t1, t2)  # type: ignore[attr-defined]
            current_vs_buy = quote["price"] / buy_price * 100 - 100 if buy_price else 0
            current_from_open = quote["price"] / quote["open"] * 100 - 100 if quote.get("open") else 0
            min_high_from_open = float(DEFAULT_PARAMS["entry_min_high_from_open_pct_chg"])
            status = (
                "tail_ready"
                if is_after_tail_window(str(quote.get("quote_time") or ""))
                and current_vs_buy >= 0
                and current_from_open >= min_high_from_open
                else "triggered"
            )
            position_rows.append(
                {
                    "symbol": symbol,
                    "name": profiles.get(symbol, {}).get("name") or quote.get("name", ""),
                    "entry_date": quote_date,
                    "entry_price": round(float(buy_price), 4),
                    "hold_days": int(DEFAULT_PARAMS.get("hold_days", 5)),
                    "active": True,
                    "source": f"realtime_{status}",
                    "status": status,
                    "quote_time": quote.get("quote_time", ""),
                }
            )
        elif args.include_invalid:
            invalid += 1
            candidates.append(symbol)

    candidates = sorted(set(candidates))
    minute_symbols = sorted(set(symbols if pool else candidates))
    due_minute_symbols = select_due_minute_symbols(minute_symbols, args, state)
    stats = download_minute_batch(
        db=db,
        symbols=due_minute_symbols,
        trade_date=quote_date,
        workers=int(args.candidate_minute_workers),
        label="candidate minute",
    )
    mark_minute_attempted(due_minute_symbols, args, state)
    downloaded_rows = stats["rows"]
    ok = stats["ok"]
    empty = stats["empty"]
    failed = stats["failed"]

    tracked = 0
    if position_rows and not args.no_auto_track_positions:
        attach_trigger_times(db, position_rows)
        tracked = auto_track_positions(Path(args.position_file), position_rows, float(args.initial_capital or INITIAL_CAPITAL))

    elapsed = time.perf_counter() - started
    print(
        f"[rt-minute] {datetime.now():%Y-%m-%d %H:%M:%S} quote={quote_date} pool={len(symbols) if pool else 0} "
        f"setup={len(candidates)} minute_symbols={len(minute_symbols)} minute_due={len(due_minute_symbols)} triggered={triggered} "
        f"invalid_included={invalid} ok={ok} empty={empty} failed={failed} rows={downloaded_rows} "
        f"tracked={tracked} elapsed={elapsed:.1f}s",
        flush=True,
    )


def auto_track_positions(position_file: Path, rows: list[dict[str, Any]], initial_capital: float = INITIAL_CAPITAL) -> int:
    payload = load_position_payload(position_file)
    positions = payload.setdefault("positions", [])
    max_positions = max(1, int(DEFAULT_PARAMS.get("max_positions", 4)))
    existing = {
        normalize_symbol(str(item.get("symbol") or ""))
        for item in positions
        if item.get("active", True) and item.get("symbol")
    }
    slots = max(0, max_positions - len(existing))
    if slots <= 0:
        return 0
    added = 0
    sorted_rows = sorted(
        rows,
        key=lambda item: (
            str(item.get("trigger_time") or item.get("quote_time") or ""),
            normalize_symbol(str(item.get("symbol") or "")),
        ),
    )
    for row in sorted_rows:
        if added >= slots:
            break
        symbol = normalize_symbol(str(row.get("symbol") or ""))
        entry_price = float(row.get("entry_price") or 0)
        if not symbol or symbol in existing or entry_price <= 0:
            continue
        amount = next_position_amount(payload, max_positions, initial_capital)
        lot_size = board_lot_size(symbol)
        quantity = int(amount / entry_price / lot_size) * lot_size
        if amount <= 0 or quantity <= 0:
            continue
        amount = quantity * entry_price
        positions.append(
            {
                "symbol": symbol,
                "name": row.get("name") or "",
                "entry_date": row.get("entry_date") or datetime.now().date().isoformat(),
                "entry_price": round(entry_price, 4),
                "quantity": quantity,
                "lot_size": lot_size,
                "amount": round(amount, 2),
                "entry_amount": round(amount, 2),
                "hold_days": int(row.get("hold_days") or DEFAULT_PARAMS.get("hold_days", 5)),
                "active": True,
                "source": row.get("source") or "realtime_triggered",
                "trigger_time": row.get("trigger_time") or "",
                "created_at": datetime.now().isoformat(timespec="seconds"),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )
        existing.add(symbol)
        added += 1
    if added:
        save_position_payload(position_file, payload)
    return added


def next_position_amount(payload: dict[str, Any], max_positions: int, initial_capital: float) -> float:
    positions = payload.get("positions", [])
    active_positions = [
        item
        for item in positions
        if item.get("active", True) and item.get("symbol")
    ]
    active_count = len(active_positions)
    remaining_slots = max(1, max_positions - active_count)
    used_amount = sum(position_entry_amount(item) for item in active_positions)
    realized_pnl = sum(closed_realized_pnl(item) for item in payload.get("closed_positions", []))
    available_cash = max(0.0, float(initial_capital or INITIAL_CAPITAL) + realized_pnl - used_amount)
    return available_cash / remaining_slots


def position_entry_amount(position: dict[str, Any]) -> float:
    amount = safe_float(position.get("entry_amount")) or safe_float(position.get("amount"))
    if amount > 0:
        return amount
    quantity = safe_float(position.get("quantity"))
    entry_price = safe_float(position.get("entry_price"))
    return max(0.0, quantity * entry_price)


def board_lot_size(symbol: str) -> int:
    normalized = normalize_symbol(symbol)
    code, exchange = normalized.split(".")
    return 200 if exchange == "SZ" and code.startswith(("300", "301")) else 100


def closed_realized_pnl(position: dict[str, Any]) -> float:
    pnl = safe_float(position.get("realized_pnl"))
    if pnl:
        return pnl
    exit_amount = safe_float(position.get("exit_amount"))
    entry_amount = position_entry_amount(position)
    return exit_amount - entry_amount if exit_amount > 0 and entry_amount > 0 else 0.0


def safe_float(value: Any) -> float:
    try:
        return float(value or 0)
    except (TypeError, ValueError):
        return 0.0


def attach_trigger_times(db: MarketDatabase, rows: list[dict[str, Any]]) -> None:
    for row in rows:
        symbol = normalize_symbol(str(row.get("symbol") or ""))
        trade_date = str(row.get("entry_date") or "")
        entry_price = float(row.get("entry_price") or 0)
        trigger_time = first_minute_trigger_time(db, symbol, trade_date, entry_price)
        if trigger_time:
            row["trigger_time"] = trigger_time
        elif row.get("quote_time") and trade_date:
            row["trigger_time"] = f"{trade_date} {row.get('quote_time')}"


def first_minute_trigger_time(db: MarketDatabase, symbol: str, trade_date: str, entry_price: float) -> str:
    if not symbol or not trade_date or entry_price <= 0:
        return ""
    with db.connect() as conn:
        row = conn.execute(
            """
            select min(trade_time) as trade_time
            from stock_minute
            where symbol = ? and trade_date = ? and high >= ?
            """,
            (symbol, trade_date, entry_price),
        ).fetchone()
    return str(row["trade_time"] or "") if row else ""


def load_position_payload(position_file: Path) -> dict[str, Any]:
    if not position_file.exists():
        return {"positions": [], "closed_positions": []}
    try:
        payload = json.loads(position_file.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return {"positions": [], "closed_positions": []}
    if isinstance(payload, list):
        payload = {"positions": payload}
    if not isinstance(payload, dict):
        payload = {}
    payload.setdefault("positions", [])
    payload.setdefault("closed_positions", [])
    return payload


def save_position_payload(position_file: Path, payload: dict[str, Any]) -> None:
    position_file.parent.mkdir(parents=True, exist_ok=True)
    position_file.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def is_after_tail_window(value: str) -> bool:
    try:
        return datetime.strptime(value[:5], "%H:%M").time() >= dt_time(14, 45)
    except ValueError:
        return False


def select_due_minute_symbols(
    symbols: list[str],
    args: argparse.Namespace,
    state: dict[str, Any],
) -> list[str]:
    if not symbols:
        return []
    refresh_interval = max(0, int(args.minute_refresh_interval))
    if args.once or refresh_interval <= 0:
        return symbols
    last_by_symbol = state.setdefault("minute_download_at", {})
    now_ts = time.time()
    return [
        symbol
        for symbol in symbols
        if now_ts - float(last_by_symbol.get(symbol) or 0) >= refresh_interval
    ]


def mark_minute_attempted(
    symbols: list[str],
    args: argparse.Namespace,
    state: dict[str, Any],
) -> None:
    if not symbols or args.once or int(args.minute_refresh_interval) <= 0:
        return
    last_by_symbol = state.setdefault("minute_download_at", {})
    now_ts = time.time()
    for symbol in symbols:
        last_by_symbol[symbol] = now_ts


def should_run_after_close(now: datetime, args: argparse.Namespace, completed_full_dates: set[str]) -> bool:
    if not args.after_close_download or args.once or now.weekday() >= 5:
        return False
    if now.time() < parse_hhmm(args.close_download_start):
        return False
    today = now.date().isoformat()
    if today in completed_full_dates:
        return False
    existing_rows = minute_rows_for_date(today)
    existing_daily_rows = daily_rows_for_date(today)
    enough_minutes = existing_rows >= int(args.after_close_skip_min_rows)
    daily_threshold = int(args.after_close_skip_min_daily_rows)
    if args.after_close_daily_download:
        try:
            symbol_count = len(MarketDatabase().list_realtime_symbols("all", 34))
            if int(args.after_close_limit) > 0:
                symbol_count = min(symbol_count, int(args.after_close_limit))
            daily_threshold = daily_complete_threshold(args, symbol_count)
        except Exception:
            daily_threshold = int(args.after_close_skip_min_daily_rows)
    enough_daily = (not args.after_close_daily_download) or existing_daily_rows >= daily_threshold
    if enough_minutes and enough_daily:
        completed_full_dates.add(today)
        print(
            f"[rt-minute] after-close skipped: {today} already has "
            f"{existing_rows} minute rows and {existing_daily_rows}/{daily_threshold} daily rows",
            flush=True,
        )
        return False
    return True


def minute_rows_for_date(trade_date: str) -> int:
    db = MarketDatabase()
    with db.connect() as conn:
        row = conn.execute("select count(*) as count from stock_minute where trade_date = ?", (trade_date,)).fetchone()
    return int(row["count"] if row else 0)


def daily_rows_for_date(trade_date: str) -> int:
    db = MarketDatabase()
    with db.connect() as conn:
        row = conn.execute("select count(*) as count from stock_daily where trade_date = ?", (trade_date,)).fetchone()
    return int(row["count"] if row else 0)


def daily_complete_threshold(args: argparse.Namespace, symbol_count: int = 0) -> int:
    configured = max(0, int(args.after_close_skip_min_daily_rows))
    if symbol_count <= 0:
        return configured
    if int(args.after_close_limit) > 0:
        configured = min(configured, symbol_count)
    ratio = min(1.0, max(0.0, float(args.after_close_daily_complete_ratio)))
    ratio_required = int(symbol_count * ratio)
    return min(symbol_count, max(configured, ratio_required))


def run_after_close_full_market(args: argparse.Namespace) -> str:
    started = time.perf_counter()
    db = MarketDatabase()
    symbols = [item["symbol"] for item in db.list_realtime_symbols("all", 34)]
    if args.after_close_limit > 0:
        symbols = symbols[: args.after_close_limit]
    if not symbols:
        print("[rt-minute] after-close skipped: no symbols", flush=True)
        return ""

    quotes = fetch_sina_realtime(symbols)
    if not quotes:
        print("[rt-minute] after-close skipped: no realtime quote date", flush=True)
        return ""
    trade_date = max((quote.get("quote_date") or "") for quote in quotes.values())
    if not trade_date:
        print("[rt-minute] after-close skipped: empty quote date", flush=True)
        return ""

    workers = max(1, int(args.after_close_workers))
    print(
        f"[rt-minute] after-close full download start date={trade_date} symbols={len(symbols)} workers={workers}",
        flush=True,
    )
    ok = empty = failed = downloaded_rows = 0
    existing_minute_rows = minute_rows_for_date(trade_date)
    if existing_minute_rows >= int(args.after_close_skip_min_rows):
        downloaded_rows = existing_minute_rows
        print(
            f"[rt-minute] after-close minute skipped: {trade_date} already has {existing_minute_rows} rows",
            flush=True,
        )
    else:
        with ProcessPoolExecutor(max_workers=workers) as executor:
            futures = [executor.submit(fetch_minute_payload, symbol, trade_date) for symbol in symbols]
            for index, future in enumerate(as_completed(futures), start=1):
                payload = future.result()
                symbol = payload.get("symbol", "")
                if payload.get("ok"):
                    rows = payload.get("rows", [])
                    db.upsert_minutes(rows)
                    if rows:
                        ok += 1
                        downloaded_rows += len(rows)
                    else:
                        empty += 1
                else:
                    failed += 1
                    print(f"[rt-minute] after-close failed {symbol}: {payload.get('error')}", flush=True)

                if index % 100 == 0 or index == len(futures):
                    print(
                        f"[rt-minute] after-close {index}/{len(futures)} ok={ok} empty={empty} "
                        f"failed={failed} rows={downloaded_rows}",
                        flush=True,
                    )

    if args.after_close_daily_download:
        run_after_close_daily_market(args, symbols, trade_date)

    pool = generate_tomorrow_candidates(args, symbols, trade_date)
    print(
        f"[rt-minute] tomorrow candidates target={pool.get('target_date')} "
        f"setup={pool.get('setup_date')} count={len(pool.get('candidates', []))}",
        flush=True,
    )

    elapsed = time.perf_counter() - started
    print(
        f"[rt-minute] after-close full download done date={trade_date} ok={ok} empty={empty} "
        f"failed={failed} rows={downloaded_rows} elapsed={elapsed:.1f}s",
        flush=True,
    )
    return trade_date


def generate_tomorrow_candidates(args: argparse.Namespace, symbols: list[str], setup_date: str) -> dict[str, Any]:
    db = MarketDatabase()
    strategy = DivergenceStrategy(**DEFAULT_PARAMS)
    profiles = db.symbol_profiles(symbols)
    history = query_recent_through(db, symbols, setup_date, 40)
    candidates: list[dict[str, Any]] = []
    for symbol in symbols:
        rows = history.get(symbol, [])
        if len(rows) < 34 or rows[-1].get("trade_date") != setup_date:
            continue
        t = rows[-2]
        t1 = rows[-1]
        setup_history = (rows + [{"symbol": symbol, "trade_date": "__next__"}])[-35:]
        if not strategy._setup_ok(symbol, t, t1, setup_history):  # type: ignore[attr-defined]
            continue
        candidates.append(
            {
                "symbol": symbol,
                "name": profiles.get(symbol, {}).get("name") or "",
                "setup_date": setup_date,
                "day1_date": t.get("trade_date", ""),
                "day1_pct_chg": round(strategy._pct_chg(t), 4),  # type: ignore[attr-defined]
                "day1_high": round(float(t.get("high") or 0), 4),
                "day2_date": t1.get("trade_date", ""),
                "day2_pct_chg": round(strategy._pct_chg(t1), 4),  # type: ignore[attr-defined]
                "day2_high": round(float(t1.get("high") or 0), 4),
                "day2_close": round(float(t1.get("close") or 0), 4),
                "entry_min_open_gap_pct_chg": float(DEFAULT_PARAMS["entry_min_open_gap_pct_chg"]),
                "entry_max_open_gap_pct_chg": float(DEFAULT_PARAMS["entry_max_open_gap_pct_chg"]),
                "entry_min_high_from_open_pct_chg": float(DEFAULT_PARAMS["entry_min_high_from_open_pct_chg"]),
            }
        )

    target_date = next_business_day(setup_date)
    payload = {
        "mode": "divergence_tactic_tomorrow_candidates",
        "board": args.board,
        "setup_date": setup_date,
        "target_date": target_date,
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "params": DEFAULT_PARAMS,
        "source_symbols": len(symbols),
        "candidates": sorted(candidates, key=lambda item: (str(item.get("symbol")))),
    }
    candidate_dir = Path(args.candidate_dir)
    candidate_dir.mkdir(parents=True, exist_ok=True)
    dated_path = candidate_dir / f"tomorrow_candidates_{target_date}.json"
    latest_path = candidate_dir / "latest_tomorrow_candidates.json"
    text = json.dumps(payload, ensure_ascii=False, indent=2)
    dated_path.write_text(text, encoding="utf-8")
    latest_path.write_text(text, encoding="utf-8")
    return payload


def load_candidate_pool_for_date(candidate_dir: Path, target_date: str, db: MarketDatabase) -> dict[str, Any] | None:
    paths = [
        candidate_dir / f"tomorrow_candidates_{target_date}.json",
        candidate_dir / "latest_tomorrow_candidates.json",
    ]
    for index, path in enumerate(paths):
        if not path.exists():
            continue
        try:
            payload = json.loads(path.read_text(encoding="utf-8-sig"))
        except (OSError, json.JSONDecodeError):
            continue
        if not isinstance(payload, dict):
            continue
        if index == 0 and payload.get("target_date") not in ("", None, target_date):
            continue
        setup_date = str(payload.get("setup_date") or "")
        latest_before = latest_daily_date_before(db, target_date)
        if setup_date and latest_before and setup_date != latest_before:
            print(
                f"[rt-minute] candidate pool stale: setup={setup_date} latest_before={latest_before}",
                flush=True,
            )
            continue
        candidates = payload.get("candidates") or []
        if not candidates:
            continue
        return payload
    return None


def candidate_pool_daily_complete(pool: dict[str, Any], args: argparse.Namespace) -> bool:
    setup_date = str(pool.get("setup_date") or "")
    if not setup_date:
        return True
    source_symbols = int(pool.get("source_symbols") or len(pool.get("candidates") or []))
    min_daily_rows = daily_complete_threshold(args, source_symbols)
    setup_rows = daily_rows_for_date(setup_date)
    if setup_rows >= min_daily_rows:
        return True
    print(
        f"[rt-minute] candidate pool ignored: setup={setup_date} daily_rows={setup_rows}/{min_daily_rows}",
        flush=True,
    )
    return False


def maybe_generate_candidate_pool_for_date(
    args: argparse.Namespace,
    target_date: str,
    db: MarketDatabase,
    state: dict[str, Any],
) -> dict[str, Any] | None:
    attempt_key = f"candidate_pool_attempted:{target_date}"
    if state.get(attempt_key):
        return None
    state[attempt_key] = True
    setup_date = latest_daily_date_before(db, target_date)
    if not setup_date:
        return None
    symbols = [item["symbol"] for item in db.list_realtime_symbols(args.board, 34)]
    if args.limit > 0:
        symbols = symbols[: args.limit]
    min_daily_rows = daily_complete_threshold(args, len(symbols))
    setup_rows = daily_rows_for_date(setup_date)
    if setup_rows < min_daily_rows:
        print(
            f"[rt-minute] candidate pool auto-generate skipped: setup={setup_date} "
            f"daily_rows={setup_rows}/{min_daily_rows}",
            flush=True,
        )
        return None
    print(
        f"[rt-minute] candidate pool missing; auto-generating from setup={setup_date} symbols={len(symbols)}",
        flush=True,
    )
    return generate_tomorrow_candidates(args, symbols, setup_date)


def query_recent_through(db: MarketDatabase, symbols: list[str], through_date: str, limit: int = 40) -> dict[str, list[dict[str, Any]]]:
    normalized = [normalize_symbol(symbol) for symbol in symbols]
    result: dict[str, list[dict[str, Any]]] = {}
    with db.connect() as conn:
        for symbol in normalized:
            rows = conn.execute(
                """
                select symbol, trade_date, open, high, low, close, pre_close, volume, amount,
                       turnover, pct_chg, is_st, source
                from stock_daily
                where symbol = ? and trade_date <= ?
                order by trade_date desc
                limit ?
                """,
                (symbol, through_date, limit),
            ).fetchall()
            if rows:
                result[symbol] = [dict(row) for row in reversed(rows)]
    return result


def latest_daily_date_before(db: MarketDatabase, target_date: str) -> str:
    with db.connect() as conn:
        row = conn.execute(
            "select max(trade_date) as trade_date from stock_daily where trade_date < ?",
            (target_date,),
        ).fetchone()
    return str(row["trade_date"] or "") if row else ""


def next_business_day(value: str) -> str:
    day = datetime.strptime(value, "%Y-%m-%d").date() + timedelta(days=1)
    while day.weekday() >= 5:
        day += timedelta(days=1)
    return day.isoformat()


def run_after_close_daily_market(args: argparse.Namespace, symbols: list[str], trade_date: str) -> None:
    started = time.perf_counter()
    db = MarketDatabase()
    existing_daily_rows = daily_rows_for_date(trade_date)
    min_daily_rows = daily_complete_threshold(args, len(symbols))
    if existing_daily_rows >= min_daily_rows:
        print(
            f"[rt-minute] after-close daily skipped: {trade_date} already has {existing_daily_rows}/{min_daily_rows} rows",
            flush=True,
        )
        return
    workers = max(1, int(args.after_close_daily_workers))
    print(
        f"[rt-minute] after-close daily download start date={trade_date} symbols={len(symbols)} "
        f"workers={workers} source={args.after_close_daily_source}",
        flush=True,
    )
    ok = empty = downloaded_rows = 0
    failed_payloads: list[dict[str, Any]] = []
    stats = download_daily_batch(
        db=db,
        symbols=symbols,
        trade_date=trade_date,
        source_name=args.after_close_daily_source,
        workers=workers,
        label="after-close daily",
    )
    ok += stats["ok"]
    empty += stats["empty"]
    downloaded_rows += stats["rows"]
    failed_payloads = stats["failed_payloads"]

    for retry_round in range(1, max(0, int(args.after_close_daily_retries)) + 1):
        if not failed_payloads:
            break
        retry_symbols = [str(item.get("symbol") or "") for item in failed_payloads if item.get("symbol")]
        if not retry_symbols:
            break
        sleep_seconds = max(0.0, float(args.after_close_daily_retry_sleep))
        if sleep_seconds:
            time.sleep(sleep_seconds)
        retry_workers = max(1, int(args.after_close_daily_retry_workers))
        print(
            f"[rt-minute] after-close daily retry {retry_round}/{args.after_close_daily_retries} "
            f"symbols={len(retry_symbols)} workers={retry_workers}",
            flush=True,
        )
        stats = download_daily_batch(
            db=db,
            symbols=retry_symbols,
            trade_date=trade_date,
            source_name=args.after_close_daily_source,
            workers=retry_workers,
            label=f"after-close daily retry{retry_round}",
        )
        ok += stats["ok"]
        empty += stats["empty"]
        downloaded_rows += stats["rows"]
        failed_payloads = stats["failed_payloads"]

    failed = len(failed_payloads)
    if failed_payloads:
        failed_symbols = ",".join(str(item.get("symbol") or "") for item in failed_payloads[:50])
        print(
            f"[rt-minute] after-close daily final failed={failed} symbols={failed_symbols}",
            flush=True,
        )
    final_daily_rows = daily_rows_for_date(trade_date)
    if final_daily_rows < min_daily_rows:
        print(
            f"[rt-minute] after-close daily warning: {trade_date} has {final_daily_rows}/{min_daily_rows} rows; "
            "tomorrow candidate pool may be incomplete",
            flush=True,
        )

    print(
        f"[rt-minute] after-close daily done date={trade_date} ok={ok} empty={empty} "
        f"failed={failed} rows={downloaded_rows} total_daily_rows={final_daily_rows} elapsed={time.perf_counter() - started:.1f}s",
        flush=True,
    )


def download_minute_batch(
    db: MarketDatabase,
    symbols: list[str],
    trade_date: str,
    workers: int,
    label: str,
) -> dict[str, int]:
    ok = empty = failed = downloaded_rows = 0
    if not symbols:
        return {"ok": ok, "empty": empty, "failed": failed, "rows": downloaded_rows}

    max_workers = max(1, min(int(workers), len(symbols)))
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(fetch_minute_payload, symbol, trade_date) for symbol in symbols]
        for index, future in enumerate(as_completed(futures), start=1):
            payload = future.result()
            symbol = payload.get("symbol", "")
            if payload.get("ok"):
                rows = payload.get("rows", [])
                db.upsert_minutes(rows)
                if rows:
                    ok += 1
                    downloaded_rows += len(rows)
                else:
                    empty += 1
            else:
                failed += 1
                print(f"[rt-minute] {label} failed {symbol}: {payload.get('error')}", flush=True)

            if index % 20 == 0 or index == len(futures):
                print(
                    f"[rt-minute] {label} {index}/{len(futures)} ok={ok} empty={empty} "
                    f"failed={failed} rows={downloaded_rows}",
                    flush=True,
                )
    return {"ok": ok, "empty": empty, "failed": failed, "rows": downloaded_rows}


def download_daily_batch(
    db: MarketDatabase,
    symbols: list[str],
    trade_date: str,
    source_name: str,
    workers: int,
    label: str,
) -> dict[str, Any]:
    ok = empty = downloaded_rows = 0
    failed_payloads: list[dict[str, Any]] = []
    with ProcessPoolExecutor(max_workers=max(1, workers)) as executor:
        futures = [
            executor.submit(fetch_daily_payload, symbol, trade_date, source_name)
            for symbol in symbols
        ]
        for index, future in enumerate(as_completed(futures), start=1):
            payload = future.result()
            symbol = payload.get("symbol", "")
            if payload.get("ok"):
                bars = [DailyBar(**row) for row in payload.get("bars", [])]
                db.upsert_daily(bars)
                if bars:
                    ok += 1
                    downloaded_rows += len(bars)
                else:
                    empty += 1
            else:
                failed_payloads.append(payload)
                print(f"[rt-minute] {label} failed {symbol}: {payload.get('error')}", flush=True)

            if index % 100 == 0 or index == len(futures):
                print(
                    f"[rt-minute] {label} {index}/{len(futures)} ok={ok} empty={empty} "
                    f"failed={len(failed_payloads)} rows={downloaded_rows}",
                    flush=True,
                )
    return {"ok": ok, "empty": empty, "rows": downloaded_rows, "failed_payloads": failed_payloads}


def fetch_minute_payload(symbol: str, trade_date: str) -> dict[str, Any]:
    try:
        configure_network_env()
        rows = [row for row in fetch_sina_minutes(symbol, "1") if row.get("trade_date") == trade_date]
        return {"ok": True, "symbol": symbol, "rows": rows}
    except Exception as exc:
        return {"ok": False, "symbol": symbol, "error": f"{type(exc).__name__}: {exc}"}


def fetch_daily_payload(symbol: str, trade_date: str, source_name: str) -> dict[str, Any]:
    try:
        configure_network_env()
        if source_name in ("auto", "akshare_sina"):
            bars = [bar for bar in fetch_akshare_sina_daily(symbol, trade_date, trade_date) if bar.trade_date == trade_date]
        else:
            source = get_source(source_name)
            bars = [bar for bar in source.fetch_daily(symbol, trade_date, trade_date) if bar.trade_date == trade_date]
        return {"ok": True, "symbol": symbol, "bars": [bar.__dict__ for bar in bars]}
    except Exception as exc:
        return {"ok": False, "symbol": symbol, "error": f"{type(exc).__name__}: {exc}"}


def in_time_window(now: datetime, start: str, end: str) -> bool:
    start_time = parse_hhmm(start)
    end_time = parse_hhmm(end)
    current = now.time()
    return start_time <= current <= end_time


def parse_hhmm(value: str) -> dt_time:
    return datetime.strptime(value, "%H:%M").time()


if __name__ == "__main__":
    main()
