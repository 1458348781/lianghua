from __future__ import annotations

import sqlite3
from collections import defaultdict
from pathlib import Path
from typing import Any

from .config import DATABASE_PATH, ensure_directories
from .data_sources import DailyBar, normalize_symbol


def _chunks(items: list[str], size: int = 500):
    for index in range(0, len(items), size):
        yield items[index : index + size]


def board_label(board: str | None) -> str:
    labels = {
        "all": "全部本地股票",
        "chinext": "创业板",
        "star": "科创板",
        "main": "主板及其他",
    }
    return labels.get(board or "all", "全部本地股票")


def board_matches(symbol: str, board: str | None) -> bool:
    normalized = normalize_symbol(symbol)
    code, exchange = normalized.split(".")
    if board in (None, "", "all"):
        return True
    if board == "chinext":
        return exchange == "SZ" and code.startswith(("300", "301"))
    if board == "star":
        return exchange == "SH" and code.startswith(("688", "689"))
    if board == "main":
        return not (
            exchange == "SZ"
            and code.startswith(("300", "301"))
            or exchange == "SH"
            and code.startswith(("688", "689"))
        )
    return True


def filter_by_board(items: list[dict[str, Any]], board: str | None) -> list[dict[str, Any]]:
    return [item for item in items if board_matches(str(item.get("symbol", "")), board)]


class MarketDatabase:
    def __init__(self, path: Path = DATABASE_PATH) -> None:
        ensure_directories()
        self.path = path
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._init_schema()

    def connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def _init_schema(self) -> None:
        with self.connect() as conn:
            conn.executescript(
                """
                create table if not exists stock_basic (
                    symbol text primary key,
                    name text not null default '',
                    exchange text not null default '',
                    industry text not null default '',
                    list_date text not null default '',
                    delist_date text not null default '',
                    is_st integer not null default 0,
                    status text not null default '',
                    float_mv real not null default 0,
                    updated_at text not null default current_timestamp
                );
                create table if not exists stock_daily (
                    symbol text not null,
                    trade_date text not null,
                    open real not null,
                    high real not null,
                    low real not null,
                    close real not null,
                    pre_close real not null,
                    volume real not null,
                    amount real not null,
                    turnover real not null default 0,
                    pct_chg real not null default 0,
                    is_st integer not null default 0,
                    source text not null,
                    updated_at text not null default current_timestamp,
                    primary key (symbol, trade_date)
                );
                create index if not exists idx_stock_daily_date on stock_daily(trade_date);
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
                """
            )
            self._ensure_column(conn, "stock_daily", "turnover", "real not null default 0")
            self._ensure_column(conn, "stock_daily", "pct_chg", "real not null default 0")
            self._ensure_column(conn, "stock_daily", "is_st", "integer not null default 0")

    def _ensure_column(self, conn: sqlite3.Connection, table: str, column: str, definition: str) -> None:
        columns = {row["name"] for row in conn.execute(f"pragma table_info({table})").fetchall()}
        if column not in columns:
            conn.execute(f"alter table {table} add column {column} {definition}")

    def upsert_basic(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into stock_basic
                (symbol, name, exchange, industry, list_date, delist_date, is_st, status, float_mv, updated_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                on conflict(symbol) do update set
                    name=excluded.name,
                    exchange=excluded.exchange,
                    industry=excluded.industry,
                    list_date=excluded.list_date,
                    delist_date=excluded.delist_date,
                    is_st=excluded.is_st,
                    status=excluded.status,
                    float_mv=excluded.float_mv,
                    updated_at=current_timestamp
                """,
                [
                    (
                        row.get("symbol", ""),
                        row.get("name", ""),
                        row.get("exchange", ""),
                        row.get("industry", ""),
                        row.get("list_date", ""),
                        row.get("delist_date", ""),
                        int(row.get("is_st") or 0),
                        row.get("status", ""),
                        float(row.get("float_mv") or 0),
                    )
                    for row in rows
                ],
            )
        return len(rows)

    def upsert_daily(self, bars: list[DailyBar]) -> int:
        if not bars:
            return 0
        with self.connect() as conn:
            conn.executemany(
                """
                insert into stock_daily
                (symbol, trade_date, open, high, low, close, pre_close, volume, amount, turnover, pct_chg, is_st, source, updated_at)
                values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)
                on conflict(symbol, trade_date) do update set
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    pre_close=excluded.pre_close,
                    volume=excluded.volume,
                    amount=excluded.amount,
                    turnover=excluded.turnover,
                    pct_chg=excluded.pct_chg,
                    is_st=excluded.is_st,
                    source=excluded.source,
                    updated_at=current_timestamp
                """,
                [
                    (
                        bar.symbol,
                        bar.trade_date,
                        bar.open,
                        bar.high,
                        bar.low,
                        bar.close,
                        bar.pre_close,
                        bar.volume,
                        bar.amount,
                        bar.turnover,
                        bar.pct_chg,
                        bar.is_st,
                        bar.source,
                    )
                    for bar in bars
                ],
            )
        return len(bars)

    def upsert_minutes(self, rows: list[dict[str, Any]]) -> int:
        if not rows:
            return 0
        with self.connect() as conn:
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
                        normalize_symbol(str(row.get("symbol", ""))),
                        str(row.get("trade_time") or row.get("datetime") or ""),
                        str(row.get("trade_date") or str(row.get("datetime") or "")[:10]),
                        float(row.get("open") or 0),
                        float(row.get("high") or 0),
                        float(row.get("low") or 0),
                        float(row.get("close") or 0),
                        float(row.get("volume") or 0),
                        float(row.get("amount") or 0),
                        str(row.get("source") or ""),
                    )
                    for row in rows
                    if row.get("trade_time") or row.get("datetime")
                ],
            )
        return len(rows)

    def list_symbols(self) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select d.symbol, coalesce(b.name, '') as name,
                       min(d.trade_date) as start_date, max(d.trade_date) as end_date,
                       count(*) as rows, max(d.updated_at) as updated_at,
                       group_concat(distinct d.source) as sources
                from stock_daily d
                left join stock_basic b on b.symbol = d.symbol
                group by d.symbol, b.name
                order by d.symbol
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def list_realtime_symbols(self, board: str | None = None, min_rows: int = 34) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select b.symbol, coalesce(b.name, '') as name, count(d.trade_date) as rows
                from stock_basic b
                join stock_daily d on d.symbol = b.symbol
                where b.is_st = 0
                  and (b.status = '' or b.status = '1')
                  and b.name not like '%退%'
                group by b.symbol, b.name
                having rows >= ?
                order by b.symbol
                """,
                (min_rows,),
            ).fetchall()
        return filter_by_board([dict(row) for row in rows], board)

    def list_backtest_symbols(
        self,
        start_date: str,
        end_date: str,
        min_rows: int = 30,
        board: str | None = None,
    ) -> list[dict[str, Any]]:
        with self.connect() as conn:
            rows = conn.execute(
                """
                select b.symbol, coalesce(b.name, '') as name,
                       min(d.trade_date) as start_date, max(d.trade_date) as end_date,
                       count(d.trade_date) as rows
                from stock_basic b
                join stock_daily d on d.symbol = b.symbol
                where b.is_st = 0
                  and (b.status = '' or b.status = '1')
                  and b.name not like '%退%'
                  and (b.float_mv <= 0 or b.float_mv between 2000000000 and 20000000000)
                  and d.trade_date between ? and ?
                group by b.symbol, b.name
                having rows >= ?
                order by b.symbol
                """,
                (start_date, end_date, min_rows),
            ).fetchall()
        return filter_by_board([dict(row) for row in rows], board)

    def search_symbols(self, query: str = "", limit: int = 30) -> list[dict[str, Any]]:
        query = query.strip()
        like = f"%{query}%"
        params: tuple[Any, ...]
        where = ""
        if query:
            where = "where b.symbol like ? or b.name like ?"
            params = (like.upper(), like)
        else:
            params = ()
        with self.connect() as conn:
            rows = conn.execute(
                f"""
                select b.symbol, b.name, b.exchange, b.list_date, b.is_st, b.status,
                       min(d.trade_date) as start_date, max(d.trade_date) as end_date,
                       count(d.trade_date) as rows
                from stock_basic b
                left join stock_daily d on d.symbol = b.symbol
                {where}
                group by b.symbol
                order by rows desc, b.symbol
                limit ?
                """,
                (*params, limit),
            ).fetchall()
        return [dict(row) for row in rows]

    def symbol_profiles(self, symbols: list[str]) -> dict[str, dict[str, Any]]:
        normalized = [normalize_symbol(symbol) for symbol in symbols]
        profiles: dict[str, dict[str, Any]] = {}
        with self.connect() as conn:
            for chunk in _chunks(normalized):
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    select symbol, name, exchange, list_date, is_st, status
                    from stock_basic
                    where symbol in ({placeholders})
                    """,
                    tuple(chunk),
                ).fetchall()
                profiles.update({row["symbol"]: dict(row) for row in rows})
        return profiles

    def query_daily(self, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        symbol = normalize_symbol(symbol)
        with self.connect() as conn:
            rows = conn.execute(
                """
                select symbol, trade_date, open, high, low, close, pre_close, volume, amount,
                       turnover, pct_chg, is_st, source
                from stock_daily
                where symbol = ? and trade_date between ? and ?
                order by trade_date
                """,
                (symbol, start_date, end_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def query_minute(self, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        symbol = normalize_symbol(symbol)
        with self.connect() as conn:
            rows = conn.execute(
                """
                select m.symbol, m.trade_time, m.trade_date, m.open, m.high, m.low, m.close,
                       coalesce(d.pre_close, prev.close) as pre_close,
                       case
                         when coalesce(d.pre_close, prev.close) > 0 then (m.close / coalesce(d.pre_close, prev.close) - 1) * 100
                         else 0
                       end as pct_chg,
                       m.volume, m.amount, m.source
                from stock_minute m
                left join stock_daily d on d.symbol = m.symbol and d.trade_date = m.trade_date
                left join stock_daily prev on prev.symbol = m.symbol
                  and prev.trade_date = (
                    select max(p.trade_date)
                    from stock_daily p
                    where p.symbol = m.symbol and p.trade_date < m.trade_date
                  )
                where m.symbol = ? and m.trade_date between ? and ?
                order by m.trade_time
                """,
                (symbol, start_date, end_date),
            ).fetchall()
        return [dict(row) for row in rows]

    def query_many(self, symbols: list[str], start_date: str, end_date: str) -> dict[str, list[dict[str, Any]]]:
        result: dict[str, list[dict[str, Any]]] = {normalize_symbol(symbol): [] for symbol in symbols}
        normalized = [normalize_symbol(symbol) for symbol in symbols]
        with self.connect() as conn:
            for chunk in _chunks(normalized):
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    select symbol, trade_date, open, high, low, close, pre_close, volume, amount,
                           turnover, pct_chg, is_st, source
                    from stock_daily
                    where symbol in ({placeholders}) and trade_date between ? and ?
                    order by symbol, trade_date
                    """,
                    (*chunk, start_date, end_date),
                ).fetchall()
                for row in rows:
                    result[row["symbol"]].append(dict(row))
        result = {symbol: rows for symbol, rows in result.items() if rows}
        return result

    def query_recent_before(self, symbols: list[str], before_date: str, limit: int = 40) -> dict[str, list[dict[str, Any]]]:
        normalized = [normalize_symbol(symbol) for symbol in symbols]
        result: dict[str, list[dict[str, Any]]] = {}
        with self.connect() as conn:
            for symbol in normalized:
                rows = conn.execute(
                    """
                    select symbol, trade_date, open, high, low, close, pre_close, volume, amount,
                           turnover, pct_chg, is_st, source
                    from stock_daily
                    where symbol = ? and trade_date < ?
                    order by trade_date desc
                    limit ?
                    """,
                    (symbol, before_date, limit),
                ).fetchall()
                if rows:
                    result[symbol] = [dict(row) for row in reversed(rows)]
        return result

    def get_calendar(self, symbols: list[str], start_date: str, end_date: str) -> list[str]:
        normalized = [normalize_symbol(symbol) for symbol in symbols]
        dates: set[str] = set()
        with self.connect() as conn:
            for chunk in _chunks(normalized):
                placeholders = ",".join("?" for _ in chunk)
                rows = conn.execute(
                    f"""
                    select distinct trade_date
                    from stock_daily
                    where symbol in ({placeholders}) and trade_date between ? and ?
                    """,
                    (*chunk, start_date, end_date),
                ).fetchall()
                dates.update(row["trade_date"] for row in rows)
        return sorted(dates)


class DataPortal:
    def __init__(self, db: MarketDatabase | None = None) -> None:
        self.db = db or MarketDatabase()

    def get_price(self, symbol: str, start_date: str, end_date: str) -> list[dict[str, Any]]:
        return self.db.query_daily(symbol, start_date, end_date)

    def get_symbols(self) -> list[dict[str, Any]]:
        return self.db.list_symbols()

    def get_backtest_symbols(
        self,
        start_date: str,
        end_date: str,
        min_rows: int = 30,
        board: str | None = None,
    ) -> list[dict[str, Any]]:
        return self.db.list_backtest_symbols(start_date, end_date, min_rows, board)

    def get_trade_calendar(self, symbols: list[str], start_date: str, end_date: str) -> list[str]:
        return self.db.get_calendar(symbols, start_date, end_date)

    def get_prices(self, symbols: list[str], start_date: str, end_date: str) -> dict[str, list[dict[str, Any]]]:
        return self.db.query_many(symbols, start_date, end_date)

    def get_history_by_date(
        self,
        symbols: list[str],
        start_date: str,
        end_date: str,
    ) -> dict[str, dict[str, dict[str, Any]]]:
        by_date: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
        for symbol, rows in self.db.query_many(symbols, start_date, end_date).items():
            for row in rows:
                by_date[row["trade_date"]][symbol] = row
        return dict(by_date)
