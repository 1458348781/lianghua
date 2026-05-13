from __future__ import annotations

import argparse
import json
import mimetypes
from datetime import date, datetime
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

from .backtest import BacktestEngine
from .config import PROJECT_ROOT, WEB_DIR, ensure_directories
from .data_sources import DataSourceError, SampleSource, get_source, normalize_symbol
from .metrics import equity_with_drawdown, monthly_returns
from .minute_backtest import DivergenceMinuteBacktestEngine
from .realtime import fetch_sina_realtime, scan_realtime_divergence, scan_realtime_signal_engine
from .screener import scan_start_with_buffer, scan_strategy_signals
from .storage import DataPortal, MarketDatabase, board_label
from .strategy import create_strategy, strategy_catalog


RESULTS: dict[str, dict] = {}
BOARD_UNIVERSES = {"all", "chinext", "star", "main"}
POSITION_FILE = PROJECT_ROOT / "config" / "watch_positions.json"
RUNTIME_DIR = PROJECT_ROOT / "data" / "runtime"
INITIAL_CAPITAL = 1_000_000.0


class ApiHandler(BaseHTTPRequestHandler):
    db = MarketDatabase()
    portal = DataPortal(db)

    def log_message(self, format: str, *args) -> None:
        print("%s - %s" % (self.address_string(), format % args))

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        try:
            if parsed.path == "/api/health":
                self.write_json({"ok": True, "database": str(self.db.path)})
            elif parsed.path == "/api/strategies":
                self.write_json({"strategies": strategy_catalog()})
            elif parsed.path == "/api/symbols":
                self.write_json({"symbols": self.db.list_symbols()})
            elif parsed.path == "/api/candidate-pool":
                self.handle_candidate_pool_get(parse_qs(parsed.query))
            elif parsed.path == "/api/stocks/search":
                params = parse_qs(parsed.query)
                query = params.get("q", [""])[0]
                limit = int(params.get("limit", ["30"])[0])
                self.write_json({"stocks": self.db.search_symbols(query, limit)})
            elif parsed.path == "/api/market-data/daily":
                params = parse_qs(parsed.query)
                symbol = params.get("symbol", [""])[0]
                start_date = params.get("start_date", ["1900-01-01"])[0]
                end_date = params.get("end_date", ["2999-12-31"])[0]
                self.write_json({"data": self.db.query_daily(symbol, start_date, end_date)})
            elif parsed.path == "/api/market-data/minute":
                params = parse_qs(parsed.query)
                symbol = params.get("symbol", [""])[0]
                start_date = params.get("start_date", ["1900-01-01"])[0]
                end_date = params.get("end_date", ["2999-12-31"])[0]
                self.write_json({"data": self.db.query_minute(symbol, start_date, end_date)})
            elif parsed.path == "/api/positions":
                self.handle_positions_get()
            elif parsed.path.startswith("/api/backtests/"):
                backtest_id = parsed.path.rsplit("/", 1)[-1]
                if backtest_id not in RESULTS:
                    self.write_error(HTTPStatus.NOT_FOUND, "回测结果不存在")
                    return
                self.write_json(RESULTS[backtest_id])
            elif parsed.path == "/" or not parsed.path.startswith("/api/"):
                self.serve_static(parsed.path)
            else:
                self.write_error(HTTPStatus.NOT_FOUND, "接口不存在")
        except Exception as exc:
            self.write_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            body = self.read_json()
            if parsed.path == "/api/data/download":
                self.handle_download(body)
            elif parsed.path == "/api/backtests":
                self.handle_backtest(body)
            elif parsed.path == "/api/strategy-signals":
                self.handle_strategy_signals(body)
            elif parsed.path == "/api/realtime/divergence":
                self.handle_realtime_divergence(body)
            elif parsed.path == "/api/realtime/signal-engine":
                self.handle_realtime_signal_engine(body)
            elif parsed.path == "/api/positions":
                self.handle_position_upsert(body)
            elif parsed.path == "/api/positions/close":
                self.handle_position_close(body)
            else:
                self.write_error(HTTPStatus.NOT_FOUND, "接口不存在")
        except Exception as exc:
            self.write_error(HTTPStatus.INTERNAL_SERVER_ERROR, str(exc))

    def handle_candidate_pool_get(self, params: dict) -> None:
        target_date = params.get("target_date", [""])[0]
        if target_date:
            pool = load_candidate_pool_for_date(RUNTIME_DIR, target_date, self.db)
        else:
            pool = load_latest_candidate_pool(RUNTIME_DIR, self.db)
        if not pool:
            self.write_json({"pool": None, "candidates": []})
            return
        self.write_json(
            {
                "pool": {
                    "mode": pool.get("mode", ""),
                    "setup_date": pool.get("setup_date", ""),
                    "target_date": pool.get("target_date", ""),
                    "generated_at": pool.get("generated_at", ""),
                    "source_symbols": pool.get("source_symbols", 0),
                    "candidate_count": len(pool.get("candidates") or []),
                },
                "candidates": pool.get("candidates") or [],
            }
        )

    def handle_download(self, body: dict) -> None:
        symbols = [normalize_symbol(symbol) for symbol in body.get("symbols", [])]
        start_date = body.get("start_date", "2020-01-01")
        end_date = body.get("end_date", "2024-12-31")
        source_name = body.get("source", "auto")
        allow_sample = bool(body.get("allow_sample", False))
        if not symbols:
            self.write_error(HTTPStatus.BAD_REQUEST, "至少选择一个股票代码")
            return

        downloaded = []
        warnings = []
        for symbol in symbols:
            try:
                source = get_source(source_name)
                bars = source.fetch_daily(symbol, start_date, end_date)
            except DataSourceError as exc:
                if not allow_sample:
                    self.write_error(HTTPStatus.BAD_GATEWAY, str(exc))
                    return
                bars = SampleSource().fetch_daily(symbol, start_date, end_date)
                warnings.append(f"{symbol} 真实行情下载失败，已写入演示数据：{exc}")
            count = self.db.upsert_daily(bars)
            downloaded.append({"symbol": symbol, "rows": count, "source": bars[0].source if bars else source_name})
        self.write_json({"downloaded": downloaded, "warnings": warnings, "symbols": self.db.list_symbols()})

    def handle_backtest(self, body: dict) -> None:
        universe = body.get("universe", "selected")
        start_date = body.get("start_date", "2020-01-01")
        end_date = body.get("end_date", "2024-12-31")
        history_start = scan_start_with_buffer(start_date)
        body = dict(body)
        body["data_start_date"] = history_start
        if universe in BOARD_UNIVERSES:
            symbols = self.db.list_backtest_symbols(
                history_start,
                end_date,
                int(body.get("min_rows", 30)),
                universe,
            )
            if not symbols:
                self.write_error(HTTPStatus.BAD_REQUEST, f"当前日期范围内没有可回算的{board_label(universe)}数据")
                return
            body["symbols"] = [item["symbol"] for item in symbols]
            body["universe_symbol_count"] = len(symbols)
            body["universe_label"] = board_label(universe)

        strategy_name = body.get("strategy_name", "moving_average")
        if strategy_name == "divergence_tactic":
            engine = DivergenceMinuteBacktestEngine(self.db, body)
        else:
            strategy = create_strategy(strategy_name, body.get("params", {}))
            engine = BacktestEngine(self.portal, strategy, body)
        result = engine.run()
        result["equity_curve"] = equity_with_drawdown(result["equity_curve"])
        result["monthly_returns"] = monthly_returns(result["equity_curve"])
        RESULTS[result["id"]] = result
        self.write_json(result)

    def handle_strategy_signals(self, body: dict) -> None:
        start_date = body.get("start_date", "2024-01-01")
        end_date = body.get("end_date", "2024-12-31")
        strategy_name = body.get("strategy_name", "divergence_tactic")
        universe = body.get("universe", "selected")
        history_start = scan_start_with_buffer(start_date)
        if universe in BOARD_UNIVERSES:
            pool = self.db.list_backtest_symbols(history_start, end_date, int(body.get("min_rows", 30)), universe)
            symbols = [item["symbol"] for item in pool]
        else:
            symbols = [normalize_symbol(symbol) for symbol in body.get("symbols", [])]
        if not symbols:
            self.write_error(HTTPStatus.BAD_REQUEST, "没有可扫描的股票，请先选择股票或切换股票池")
            return

        history = self.portal.get_prices(symbols, history_start, end_date)
        symbols = [symbol for symbol in symbols if symbol in history]
        profiles = self.db.symbol_profiles(symbols)
        signals = scan_strategy_signals(
            strategy_name,
            body.get("params", {}),
            history,
            profiles,
            start_date,
            end_date,
            int(body.get("limit", 1000)),
        )
        self.write_json(
            {
                "strategy_name": strategy_name,
                "universe": universe,
                "universe_label": board_label(universe),
                "start_date": start_date,
                "end_date": end_date,
                "scanned_symbols": len(symbols),
                "signals": signals,
            }
        )

    def handle_realtime_divergence(self, body: dict) -> None:
        result = scan_realtime_divergence(
            self.db,
            body.get("params", {}),
            int(body.get("limit", 300)),
            body.get("universe", "all"),
        )
        self.write_json(result)

    def handle_realtime_signal_engine(self, body: dict) -> None:
        symbols = None
        if body.get("use_candidate_pool", True):
            pool = load_candidate_pool_for_date(RUNTIME_DIR, datetime.now().date().isoformat(), self.db)
            if pool:
                symbols = [
                    normalize_symbol(str(item.get("symbol") or ""))
                    for item in pool.get("candidates", [])
                    if item.get("symbol")
                ]
        result = scan_realtime_signal_engine(
            self.db,
            body.get("params", {}),
            int(body.get("limit", 500)),
            body.get("universe", "all"),
            symbols=symbols,
        )
        self.write_json(result)

    def handle_positions_get(self) -> None:
        payload = self.load_position_payload()
        active = self.enrich_positions(payload.get("positions", []))
        closed = self.enrich_closed_positions(payload.get("closed_positions", []))
        self.write_json(
            {
                "positions": active,
                "closed_positions": closed,
                "totals": self.position_totals(active, closed),
                "updated_at": datetime.now().isoformat(timespec="seconds"),
            }
        )

    def handle_position_upsert(self, body: dict) -> None:
        raw_symbol = str(body.get("symbol") or "").strip()
        symbol = normalize_symbol(raw_symbol) if raw_symbol else ""
        entry_price = self.float_value(body.get("entry_price"))
        quantity = self.float_value(body.get("quantity"))
        amount = self.float_value(body.get("amount"))
        if not symbol or entry_price <= 0:
            self.write_error(HTTPStatus.BAD_REQUEST, "请填写股票代码和有效买入价")
            return
        if quantity <= 0 and amount <= 0:
            self.write_error(HTTPStatus.BAD_REQUEST, "请至少填写数量或买入金额")
            return

        payload = self.load_position_payload()
        profiles = self.db.symbol_profiles([symbol])
        now = datetime.now().isoformat(timespec="seconds")
        item = {
            "symbol": symbol,
            "name": body.get("name") or profiles.get(symbol, {}).get("name") or symbol,
            "entry_date": body.get("entry_date") or date.today().isoformat(),
            "entry_price": entry_price,
            "quantity": quantity,
            "amount": amount,
            "hold_days": int(body.get("hold_days") or 2),
            "active": True,
            "source": body.get("source") or "manual",
            "updated_at": now,
        }
        positions = payload.get("positions", [])
        existing = next((position for position in positions if position.get("symbol") == symbol), {})
        item["created_at"] = existing.get("created_at") or now
        payload["positions"] = [position for position in positions if position.get("symbol") != symbol] + [{**existing, **item}]
        payload.setdefault("closed_positions", [])
        self.save_position_payload(payload)
        self.handle_positions_get()

    def handle_position_close(self, body: dict) -> None:
        raw_symbol = str(body.get("symbol") or "").strip()
        symbol = normalize_symbol(raw_symbol) if raw_symbol else ""
        payload = self.load_position_payload()
        positions = payload.get("positions", [])
        position = next((item for item in positions if item.get("symbol") == symbol), None)
        if not position:
            self.write_error(HTTPStatus.NOT_FOUND, "持仓不存在")
            return

        try:
            quote = fetch_sina_realtime([symbol]).get(symbol, {})
        except Exception:
            quote = {}
        exit_price = self.float_value(body.get("exit_price")) or self.float_value(quote.get("price"))
        if exit_price <= 0:
            self.write_error(HTTPStatus.BAD_REQUEST, "请填写有效清仓价，或等实时行情返回价格")
            return

        enriched = self.enrich_one_position(position, quote)
        quantity = self.float_value(enriched.get("quantity"))
        entry_amount = self.float_value(enriched.get("entry_amount"))
        entry_price = self.float_value(enriched.get("entry_price"))
        exit_amount = exit_price * quantity if quantity > 0 else entry_amount * exit_price / entry_price if entry_price > 0 else 0
        realized_pnl = exit_amount - entry_amount
        closed_item = {
            **position,
            "quantity": quantity,
            "entry_amount": entry_amount,
            "active": False,
            "exit_date": body.get("exit_date") or date.today().isoformat(),
            "exit_price": exit_price,
            "exit_amount": exit_amount,
            "realized_pnl": realized_pnl,
            "realized_return": realized_pnl / entry_amount if entry_amount > 0 else 0,
            "close_reason": body.get("reason") or "manual",
            "closed_at": datetime.now().isoformat(timespec="seconds"),
        }

        payload["positions"] = [item for item in positions if item.get("symbol") != symbol]
        payload.setdefault("closed_positions", []).append(closed_item)
        self.save_position_payload(payload)
        self.handle_positions_get()

    def load_position_payload(self) -> dict:
        if not POSITION_FILE.exists():
            return {"positions": [], "closed_positions": []}
        try:
            data = json.loads(POSITION_FILE.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {"positions": [], "closed_positions": []}
        if isinstance(data, list):
            data = {"positions": data}
        if not isinstance(data, dict):
            data = {}
        data.setdefault("positions", [])
        data.setdefault("closed_positions", [])
        return data

    def save_position_payload(self, payload: dict) -> None:
        POSITION_FILE.parent.mkdir(parents=True, exist_ok=True)
        POSITION_FILE.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")

    def enrich_positions(self, positions: list[dict]) -> list[dict]:
        symbols = [normalize_symbol(item.get("symbol", "")) for item in positions if item.get("symbol")]
        try:
            quotes = fetch_sina_realtime(symbols) if symbols else {}
        except Exception:
            quotes = {}
        return [self.enrich_one_position(item, quotes.get(normalize_symbol(item.get("symbol", "")), {})) for item in positions]

    def enrich_one_position(self, position: dict, quote: dict | None = None) -> dict:
        quote = quote or {}
        entry_price = self.float_value(position.get("entry_price"))
        quantity = self.float_value(position.get("quantity"))
        amount = self.float_value(position.get("amount"))
        if quantity <= 0 and amount > 0 and entry_price > 0:
            quantity = amount / entry_price
        entry_amount = amount if amount > 0 else quantity * entry_price
        current_price = self.float_value(quote.get("price")) or entry_price
        market_value = quantity * current_price if quantity > 0 else entry_amount * current_price / entry_price
        pnl = market_value - entry_amount
        return {
            **position,
            "symbol": normalize_symbol(position.get("symbol", "")),
            "name": position.get("name") or quote.get("name") or position.get("symbol", ""),
            "entry_price": entry_price,
            "quantity": quantity,
            "entry_amount": entry_amount,
            "current_price": current_price,
            "market_value": market_value,
            "pnl": pnl,
            "pnl_pct": pnl / entry_amount if entry_amount > 0 else 0,
            "quote_date": quote.get("quote_date", ""),
            "quote_time": quote.get("quote_time", ""),
        }

    def enrich_closed_positions(self, positions: list[dict]) -> list[dict]:
        enriched = []
        for item in positions:
            entry_price = self.float_value(item.get("entry_price"))
            quantity = self.float_value(item.get("quantity"))
            entry_amount = self.float_value(item.get("amount")) or self.float_value(item.get("entry_amount"))
            if entry_amount <= 0 and quantity > 0 and entry_price > 0:
                entry_amount = quantity * entry_price
            if quantity <= 0 and entry_amount > 0 and entry_price > 0:
                quantity = entry_amount / entry_price
            enriched.append(
                {
                **item,
                "symbol": normalize_symbol(item.get("symbol", "")),
                "entry_price": entry_price,
                "quantity": quantity,
                "entry_amount": entry_amount,
                "exit_price": self.float_value(item.get("exit_price")),
                "exit_amount": self.float_value(item.get("exit_amount")),
                "realized_pnl": self.float_value(item.get("realized_pnl")),
                "realized_return": self.float_value(item.get("realized_return")),
                }
            )
        return enriched

    def position_totals(self, positions: list[dict], closed_positions: list[dict] | None = None) -> dict:
        closed_positions = closed_positions or []
        entry_amount = sum(self.float_value(item.get("entry_amount")) for item in positions)
        market_value = sum(self.float_value(item.get("market_value")) for item in positions)
        pnl = market_value - entry_amount
        realized_pnl = sum(self.float_value(item.get("realized_pnl")) for item in closed_positions)
        total_pnl = realized_pnl + pnl
        cash_available = INITIAL_CAPITAL - entry_amount + realized_pnl
        return {
            "count": len(positions),
            "initial_capital": INITIAL_CAPITAL,
            "entry_amount": entry_amount,
            "cash_available": cash_available,
            "market_value": market_value,
            "pnl": pnl,
            "pnl_pct": pnl / entry_amount if entry_amount > 0 else 0,
            "realized_pnl": realized_pnl,
            "total_pnl": total_pnl,
            "total_return": total_pnl / INITIAL_CAPITAL if INITIAL_CAPITAL > 0 else 0,
            "total_equity": cash_available + market_value,
        }

    @staticmethod
    def float_value(value) -> float:
        try:
            if value in (None, ""):
                return 0.0
            return float(value)
        except (TypeError, ValueError):
            return 0.0

    def serve_static(self, request_path: str) -> None:
        relative = "index.html" if request_path == "/" else request_path.lstrip("/")
        target = (WEB_DIR / relative).resolve()
        if WEB_DIR.resolve() not in target.parents and target != WEB_DIR.resolve():
            self.write_error(HTTPStatus.FORBIDDEN, "非法路径")
            return
        if not target.exists() or target.is_dir():
            target = WEB_DIR / "index.html"
        content_type = mimetypes.guess_type(str(target))[0] or "application/octet-stream"
        data = target.read_bytes()
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def read_json(self) -> dict:
        length = int(self.headers.get("Content-Length", "0"))
        if length == 0:
            return {}
        return json.loads(self.rfile.read(length).decode("utf-8"))

    def write_json(self, data: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        payload = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def write_error(self, status: HTTPStatus, message: str) -> None:
        self.write_json({"error": message}, status)


def load_candidate_pool_for_date(candidate_dir, target_date: str, db: MarketDatabase) -> dict | None:
    paths = [
        candidate_dir / f"tomorrow_candidates_{target_date}.json",
        candidate_dir / "latest_tomorrow_candidates.json",
    ]
    latest_before = latest_daily_date_before(db, target_date)
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
        if setup_date and latest_before and setup_date != latest_before:
            continue
        if payload.get("candidates") and candidate_pool_daily_complete(db, payload):
            return payload
    return None


def load_latest_candidate_pool(candidate_dir, db: MarketDatabase) -> dict | None:
    path = candidate_dir / "latest_tomorrow_candidates.json"
    if not path.exists():
        return None
    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except (OSError, json.JSONDecodeError):
        return None
    if not isinstance(payload, dict):
        return None
    if not payload.get("candidates"):
        return None
    if not candidate_pool_daily_complete(db, payload):
        return None
    return payload


def candidate_pool_daily_complete(db: MarketDatabase, payload: dict) -> bool:
    setup_date = str(payload.get("setup_date") or "")
    if not setup_date:
        return True
    source_symbols = int(payload.get("source_symbols") or len(payload.get("candidates") or []))
    min_rows = daily_complete_threshold(source_symbols)
    return daily_rows_for_date(db, setup_date) >= min_rows


def daily_complete_threshold(source_symbols: int) -> int:
    if source_symbols <= 0:
        return 4800
    return min(source_symbols, max(4800, int(source_symbols * 0.98)))


def daily_rows_for_date(db: MarketDatabase, trade_date: str) -> int:
    with db.connect() as conn:
        row = conn.execute("select count(*) as count from stock_daily where trade_date = ?", (trade_date,)).fetchone()
    return int(row["count"] if row else 0)


def latest_daily_date_before(db: MarketDatabase, target_date: str) -> str:
    with db.connect() as conn:
        row = conn.execute(
            "select max(trade_date) as trade_date from stock_daily where trade_date < ?",
            (target_date,),
        ).fetchone()
    return str(row["trade_date"] or "") if row else ""


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8765)
    args = parser.parse_args()
    ensure_directories()
    server = ThreadingHTTPServer((args.host, args.port), ApiHandler)
    print(f"Quant platform running at http://{args.host}:{args.port}")
    server.serve_forever()
