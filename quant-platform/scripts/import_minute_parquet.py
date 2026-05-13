from __future__ import annotations

import argparse
import sqlite3
import sys
import time
from concurrent.futures import ProcessPoolExecutor, as_completed
from pathlib import Path
from typing import Iterable

import pandas as pd


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_SOURCE_ROOT = Path(r"D:\BaiduNetdiskDownload\1m_price")
DEFAULT_DB = ROOT / "data" / "database" / "minute_1m_2023_2026.sqlite"
DEFAULT_YEAR_DB_TEMPLATE = ROOT / "data" / "database" / "minute_1m_{year}.sqlite"


def main() -> None:
    parser = argparse.ArgumentParser(description="Import historical 1-minute parquet files into a SQLite stock_minute table.")
    parser.add_argument("--source-root", default=str(DEFAULT_SOURCE_ROOT))
    parser.add_argument("--years", nargs="+", default=["2023", "2024", "2025", "2026"])
    parser.add_argument("--db", default=str(DEFAULT_DB), help="Target SQLite DB. Use market.sqlite only if you really want to merge into main DB.")
    parser.add_argument("--start-date", default="", help="YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--end-date", default="", help="YYYY-MM-DD or YYYYMMDD.")
    parser.add_argument("--symbols", default="", help="Comma-separated symbols, or a txt/csv file with one symbol per line.")
    parser.add_argument("--exchanges", default="SZ,SH", help="Comma-separated exchange suffixes to import. Empty means all, including BJ.")
    parser.add_argument("--replace", action="store_true", help="Replace existing rows on symbol+trade_time conflict.")
    parser.add_argument("--limit-files", type=int, default=0, help="For smoke testing.")
    parser.add_argument("--chunk-size", type=int, default=100_000)
    parser.add_argument("--workers", type=int, default=1, help="Parallel import workers. Workers write separate shard DBs.")
    parser.add_argument("--split-by-year", action="store_true", help="Write one SQLite DB per year. Use --db with {year} for a custom template.")
    parser.add_argument("--shard-dir", default="", help="Directory for worker shard DBs when --workers > 1.")
    parser.add_argument("--reset-shards", action="store_true", help="Delete existing shard DB files before importing.")
    parser.add_argument("--merge-shards", action="store_true", help="Merge shard DBs into --db after parallel import.")
    parser.add_argument("--fast", action=argparse.BooleanOptionalAction, default=True, help="Use faster SQLite pragmas during import.")
    args = parser.parse_args()

    source_root = Path(args.source_root)
    db_path = Path(args.db)
    symbols = load_symbols(args.symbols)
    exchanges = {item.strip().upper() for item in args.exchanges.split(",") if item.strip()}
    files = list_files(source_root, args.years, normalize_date(args.start_date), normalize_date(args.end_date))
    if args.limit_files > 0:
        files = files[: args.limit_files]
    if not files:
        raise SystemExit("No parquet files matched.")

    db_path.parent.mkdir(parents=True, exist_ok=True)
    started = time.perf_counter()
    if args.split_by_year:
        import_by_year(
            source_root=source_root,
            years=args.years,
            db_template=str(DEFAULT_YEAR_DB_TEMPLATE) if args.db == str(DEFAULT_DB) else args.db,
            start_date=normalize_date(args.start_date),
            end_date=normalize_date(args.end_date),
            symbols=symbols,
            exchanges=exchanges,
            replace=args.replace,
            chunk_size=max(1_000, int(args.chunk_size)),
            fast=args.fast,
            workers=max(1, int(args.workers or 1)),
            started=started,
        )
        return

    workers = max(1, int(args.workers or 1))
    if workers > 1:
        shard_dir = Path(args.shard_dir) if args.shard_dir else db_path.with_name(f"{db_path.stem}_shards")
        shard_dir.mkdir(parents=True, exist_ok=True)
        if args.reset_shards:
            for path in shard_dir.glob(f"{db_path.stem}_part*.sqlite*"):
                path.unlink(missing_ok=True)
        shards = import_parallel(
            files=files,
            db_path=db_path,
            shard_dir=shard_dir,
            workers=workers,
            symbols=symbols,
            exchanges=exchanges,
            replace=args.replace,
            chunk_size=max(1_000, int(args.chunk_size)),
            fast=args.fast,
            started=started,
        )
        if args.merge_shards:
            merge_shards(db_path, shards, replace=args.replace, fast=args.fast)
        else:
            print(f"[import-1m] sharded import done. shard_dir={shard_dir}", flush=True)
        return

    total_rows = 0
    total_files = 0
    with sqlite3.connect(db_path) as conn:
        init_db(conn, fast=args.fast, create_indexes=False)
        for index, path in enumerate(files, start=1):
            rows = import_file(
                conn=conn,
                path=path,
                symbols=symbols,
                exchanges=exchanges,
                replace=args.replace,
                chunk_size=max(1_000, int(args.chunk_size)),
            )
            total_rows += rows
            total_files += 1
            elapsed = time.perf_counter() - started
            print(
                f"[import-1m] {index}/{len(files)} {path.name} rows={rows} total={total_rows} elapsed={elapsed:.1f}s db={db_path}",
                flush=True,
            )
        ensure_indexes(conn)
        conn.execute("pragma optimize")

    print(f"[import-1m] done files={total_files} rows={total_rows} db={db_path}", flush=True)


def import_by_year(
    source_root: Path,
    years: list[str],
    db_template: str,
    start_date: str,
    end_date: str,
    symbols: set[str],
    exchanges: set[str],
    replace: bool,
    chunk_size: int,
    fast: bool,
    workers: int,
    started: float,
) -> None:
    jobs = []
    for year in years:
        year_files = list_files(source_root, [year], start_date, end_date)
        if not year_files:
            continue
        jobs.append((str(year), year_files, year_db_path(db_template, str(year))))
    if not jobs:
        raise SystemExit("No yearly parquet files matched.")

    max_workers = min(max(1, workers), len(jobs))
    print(f"[import-1m] yearly import years={','.join(job[0] for job in jobs)} workers={max_workers}", flush=True)
    total_rows = 0
    total_files = 0
    with ProcessPoolExecutor(max_workers=max_workers) as pool:
        futures = [
            pool.submit(
                import_worker,
                index,
                files,
                db_path,
                symbols,
                exchanges,
                replace,
                chunk_size,
                fast,
            )
            for index, (_year, files, db_path) in enumerate(jobs, start=1)
        ]
        for future in as_completed(futures):
            result = future.result()
            total_rows += int(result["rows"])
            total_files += int(result["files"])
            elapsed = time.perf_counter() - started
            print(
                f"[import-1m] yearly worker={result['worker']} done files={result['files']} rows={result['rows']} "
                f"total_files={total_files}/{sum(len(job[1]) for job in jobs)} total_rows={total_rows} elapsed={elapsed:.1f}s db={result['db']}",
                flush=True,
            )
    print(f"[import-1m] yearly import done files={total_files} rows={total_rows}", flush=True)


def year_db_path(template: str, year: str) -> Path:
    if "{year}" in template:
        return Path(template.format(year=year))
    path = Path(template)
    return path.with_name(f"{path.stem}_{year}{path.suffix or '.sqlite'}")


def import_parallel(
    files: list[Path],
    db_path: Path,
    shard_dir: Path,
    workers: int,
    symbols: set[str],
    exchanges: set[str],
    replace: bool,
    chunk_size: int,
    fast: bool,
    started: float,
) -> list[Path]:
    groups = split_files(files, workers)
    shards = [shard_dir / f"{db_path.stem}_part{index:02d}.sqlite" for index in range(1, len(groups) + 1)]
    total_rows = 0
    total_files = 0
    print(
        f"[import-1m] parallel workers={len(groups)} files={len(files)} shard_dir={shard_dir}",
        flush=True,
    )
    with ProcessPoolExecutor(max_workers=len(groups)) as pool:
        futures = [
            pool.submit(
                import_worker,
                index,
                group,
                shards[index - 1],
                symbols,
                exchanges,
                replace,
                chunk_size,
                fast,
            )
            for index, group in enumerate(groups, start=1)
            if group
        ]
        for future in as_completed(futures):
            result = future.result()
            total_rows += int(result["rows"])
            total_files += int(result["files"])
            elapsed = time.perf_counter() - started
            print(
                f"[import-1m] worker={result['worker']} done files={result['files']} rows={result['rows']} "
                f"total_files={total_files}/{len(files)} total_rows={total_rows} elapsed={elapsed:.1f}s db={result['db']}",
                flush=True,
            )
    print(f"[import-1m] parallel done files={total_files} rows={total_rows}", flush=True)
    return shards


def import_worker(
    worker: int,
    files: list[Path],
    db_path: Path,
    symbols: set[str],
    exchanges: set[str],
    replace: bool,
    chunk_size: int,
    fast: bool,
) -> dict[str, int | str]:
    db_path.parent.mkdir(parents=True, exist_ok=True)
    total_rows = 0
    with sqlite3.connect(db_path) as conn:
        init_db(conn, fast=fast, create_indexes=False)
        for path in files:
            total_rows += import_file(
                conn=conn,
                path=path,
                symbols=symbols,
                exchanges=exchanges,
                replace=replace,
                chunk_size=chunk_size,
            )
        ensure_indexes(conn)
        conn.execute("pragma optimize")
    return {"worker": worker, "files": len(files), "rows": total_rows, "db": str(db_path)}


def split_files(files: list[Path], workers: int) -> list[list[Path]]:
    groups: list[list[Path]] = [[] for _ in range(max(1, workers))]
    sizes = [0 for _ in groups]
    for path in sorted(files, key=lambda item: item.stat().st_size, reverse=True):
        index = min(range(len(groups)), key=lambda item: sizes[item])
        groups[index].append(path)
        sizes[index] += path.stat().st_size
    return [sorted(group) for group in groups if group]


def merge_shards(target_db: Path, shards: list[Path], replace: bool, fast: bool) -> None:
    target_db.parent.mkdir(parents=True, exist_ok=True)
    verb = "replace" if replace else "ignore"
    started = time.perf_counter()
    with sqlite3.connect(target_db) as conn:
        init_db(conn, fast=fast, create_indexes=False)
        for index, shard in enumerate(shards, start=1):
            if not shard.exists():
                continue
            conn.execute("attach database ? as shard", (str(shard),))
            row_count = conn.execute("select count(*) from shard.stock_minute").fetchone()[0]
            conn.execute(
                f"""
                insert or {verb} into stock_minute
                (symbol, trade_time, trade_date, open, high, low, close, volume, amount, source, updated_at)
                select symbol, trade_time, trade_date, open, high, low, close, volume, amount, source, updated_at
                from shard.stock_minute
                """
            )
            conn.execute("detach database shard")
            conn.commit()
            elapsed = time.perf_counter() - started
            print(f"[import-1m] merged {index}/{len(shards)} rows={row_count} shard={shard.name} elapsed={elapsed:.1f}s", flush=True)
        ensure_indexes(conn)
        conn.execute("pragma optimize")
    print(f"[import-1m] merge done db={target_db}", flush=True)


def normalize_date(value: str) -> str:
    value = value.strip()
    if not value:
        return ""
    return value.replace("-", "")


def list_files(source_root: Path, years: Iterable[str], start_date: str, end_date: str) -> list[Path]:
    files: list[Path] = []
    for year in years:
        files.extend(sorted((source_root / str(year)).glob("*.parquet")))
    if start_date:
        files = [path for path in files if path.stem >= start_date]
    if end_date:
        files = [path for path in files if path.stem <= end_date]
    return sorted(files)


def load_symbols(value: str) -> set[str]:
    value = value.strip()
    if not value:
        return set()
    path = Path(value)
    if path.exists():
        text = path.read_text(encoding="utf-8-sig")
        items = []
        for line in text.splitlines():
            first = line.strip().split(",")[0].strip()
            if first and first.lower() != "symbol":
                items.append(first)
        return {normalize_symbol(item) for item in items}
    return {normalize_symbol(item) for item in value.split(",") if item.strip()}


def normalize_symbol(value: str) -> str:
    value = value.strip().upper()
    if "." in value:
        code, exchange = value.split(".", 1)
        return f"{code.zfill(6)}.{exchange}"
    if value.startswith(("60", "68", "69", "51", "52", "56", "58")):
        return f"{value.zfill(6)}.SH"
    if value.startswith(("00", "30", "15", "16", "18")):
        return f"{value.zfill(6)}.SZ"
    if value.startswith(("8", "4", "9")):
        return f"{value.zfill(6)}.BJ"
    return value


def init_db(conn: sqlite3.Connection, fast: bool, create_indexes: bool = True) -> None:
    if fast:
        conn.execute("pragma journal_mode=wal")
        conn.execute("pragma synchronous=normal")
        conn.execute("pragma temp_store=memory")
        conn.execute("pragma cache_size=-200000")
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
        """
    )
    if create_indexes:
        ensure_indexes(conn)


def ensure_indexes(conn: sqlite3.Connection) -> None:
    conn.executescript(
        """
        create index if not exists idx_stock_minute_date on stock_minute(trade_date);
        create index if not exists idx_stock_minute_symbol_date on stock_minute(symbol, trade_date);
        """
    )


def import_file(
    conn: sqlite3.Connection,
    path: Path,
    symbols: set[str],
    exchanges: set[str],
    replace: bool,
    chunk_size: int,
) -> int:
    columns = ["code", "trade_time", "date", "open", "high", "low", "close", "vol", "amount"]
    frame = pd.read_parquet(path, columns=columns)
    frame = frame.rename(columns={"code": "symbol", "vol": "volume"})
    frame["symbol"] = frame["symbol"].astype(str).str.upper()
    if exchanges:
        frame = frame[frame["symbol"].str.rsplit(".", n=1).str[-1].isin(exchanges)]
    if symbols:
        frame = frame[frame["symbol"].isin(symbols)]
    if frame.empty:
        return 0

    frame["trade_date"] = frame["date"].astype(str).str.slice(0, 4) + "-" + frame["date"].astype(str).str.slice(4, 6) + "-" + frame["date"].astype(str).str.slice(6, 8)
    frame["source"] = "parquet_1m"
    frame = frame[
        ["symbol", "trade_time", "trade_date", "open", "high", "low", "close", "volume", "amount", "source"]
    ]
    frame = frame.dropna(subset=["symbol", "trade_time", "open", "high", "low", "close"])

    sql = (
        """
        insert or replace into stock_minute
        (symbol, trade_time, trade_date, open, high, low, close, volume, amount, source, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """
        if replace
        else
        """
        insert or ignore into stock_minute
        (symbol, trade_time, trade_date, open, high, low, close, volume, amount, source, updated_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
        """
    )
    total = 0
    rows = frame.itertuples(index=False, name=None)
    while True:
        batch = []
        try:
            for _ in range(chunk_size):
                batch.append(next(rows))
        except StopIteration:
            pass
        if not batch:
            break
        conn.executemany(sql, batch)
        total += len(batch)
    conn.commit()
    return total


if __name__ == "__main__":
    if sys.version_info < (3, 10):
        raise SystemExit("Python 3.10+ is required.")
    main()
