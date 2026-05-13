from __future__ import annotations

import csv
import json
import math
import random
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import date, datetime, timedelta
from typing import Iterable


@dataclass(frozen=True)
class DailyBar:
    symbol: str
    trade_date: str
    open: float
    high: float
    low: float
    close: float
    pre_close: float
    volume: float
    amount: float
    source: str
    turnover: float = 0.0
    pct_chg: float = 0.0
    is_st: int = 0


class DataSourceError(RuntimeError):
    pass


def normalize_symbol(symbol: str) -> str:
    raw = symbol.strip().upper()
    if "." in raw:
        code, exchange = raw.split(".", 1)
        return f"{code.zfill(6)}.{exchange}"
    if raw.startswith(("6", "9")):
        return f"{raw.zfill(6)}.SH"
    return f"{raw.zfill(6)}.SZ"


def compact_date(value: str) -> str:
    return value.replace("-", "")


class EastMoneySource:
    name = "eastmoney"

    def fetch_daily(self, symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
        symbol = normalize_symbol(symbol)
        secid = self._secid(symbol)
        params = {
            "secid": secid,
            "fields1": "f1,f2,f3,f4,f5,f6",
            "fields2": "f51,f52,f53,f54,f55,f56,f57,f58,f59,f60,f61",
            "klt": "101",
            "fqt": "1",
            "beg": compact_date(start_date),
            "end": compact_date(end_date),
        }
        url = "https://push2his.eastmoney.com/api/qt/stock/kline/get?" + urllib.parse.urlencode(params)
        req = urllib.request.Request(
            url,
            headers={
                "User-Agent": "Mozilla/5.0 quant-platform/0.1",
                "Referer": "https://quote.eastmoney.com/",
            },
        )
        try:
            with urllib.request.urlopen(req, timeout=18) as resp:
                payload = json.loads(resp.read().decode("utf-8"))
        except Exception as exc:
            raise DataSourceError(f"东方财富行情接口请求失败：{exc}") from exc

        data = payload.get("data") or {}
        klines = data.get("klines") or []
        if not klines:
            raise DataSourceError(f"没有获取到 {symbol} 的日线数据")

        bars: list[DailyBar] = []
        previous_close = 0.0
        for row in klines:
            parts = row.split(",")
            trade_date, open_, close, high, low, volume, amount = parts[:7]
            pct_chg = float(parts[8]) if len(parts) > 8 and parts[8] else 0.0
            turnover = float(parts[10]) if len(parts) > 10 and parts[10] else 0.0
            pre_close = previous_close or float(open_)
            bars.append(
                DailyBar(
                    symbol=symbol,
                    trade_date=trade_date,
                    open=float(open_),
                    high=float(high),
                    low=float(low),
                    close=float(close),
                    pre_close=pre_close,
                    volume=float(volume),
                    amount=float(amount),
                    source=self.name,
                    turnover=turnover,
                    pct_chg=pct_chg,
                )
            )
            previous_close = float(close)
        return bars

    def _secid(self, symbol: str) -> str:
        code, exchange = symbol.split(".")
        if exchange == "SH":
            return f"1.{code}"
        if exchange == "BJ":
            return f"0.{code}"
        return f"0.{code}"


class AkShareSource:
    name = "akshare"

    def __init__(self) -> None:
        try:
            import akshare as ak  # type: ignore
        except Exception as exc:
            raise DataSourceError("AkShare 未安装") from exc
        self.ak = ak

    def fetch_daily(self, symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
        symbol = normalize_symbol(symbol)
        code = symbol.split(".")[0]
        try:
            frame = self.ak.stock_zh_a_hist(
                symbol=code,
                period="daily",
                start_date=compact_date(start_date),
                end_date=compact_date(end_date),
                adjust="qfq",
            )
        except Exception as exc:
            raise DataSourceError(f"AkShare 行情接口请求失败：{exc}") from exc
        if frame is None or frame.empty:
            raise DataSourceError(f"AkShare 没有获取到 {symbol} 的日线数据")

        bars: list[DailyBar] = []
        previous_close = 0.0
        for row in frame.to_dict("records"):
            trade_date = str(row.get("日期"))
            open_ = float(row.get("开盘"))
            close = float(row.get("收盘"))
            high = float(row.get("最高"))
            low = float(row.get("最低"))
            volume = float(row.get("成交量", 0) or 0)
            amount = float(row.get("成交额", 0) or 0)
            pct_chg = float(row.get("涨跌幅", 0) or 0)
            turnover = float(row.get("换手率", 0) or 0)
            bars.append(
                DailyBar(
                    symbol=symbol,
                    trade_date=trade_date,
                    open=open_,
                    high=high,
                    low=low,
                    close=close,
                    pre_close=previous_close or open_,
                    volume=volume,
                    amount=amount,
                    source=self.name,
                    turnover=turnover,
                    pct_chg=pct_chg,
                )
            )
            previous_close = close
        return bars


class CsvSource:
    name = "csv"

    def fetch_daily_from_path(self, symbol: str, path: str) -> list[DailyBar]:
        symbol = normalize_symbol(symbol)
        with open(path, "r", newline="", encoding="utf-8-sig") as fh:
            rows = list(csv.DictReader(fh))
        bars: list[DailyBar] = []
        previous_close = 0.0
        for row in rows:
            close = float(row["close"])
            open_ = float(row["open"])
            bars.append(
                DailyBar(
                    symbol=symbol,
                    trade_date=row["trade_date"],
                    open=open_,
                    high=float(row["high"]),
                    low=float(row["low"]),
                    close=close,
                    pre_close=float(row.get("pre_close") or previous_close or open_),
                    volume=float(row.get("volume") or 0),
                    amount=float(row.get("amount") or 0),
                    source=self.name,
                    turnover=float(row.get("turnover") or 0),
                    pct_chg=float(row.get("pct_chg") or 0),
                    is_st=int(row.get("is_st") or 0),
                )
            )
            previous_close = close
        return bars


class SampleSource:
    name = "sample"

    def fetch_daily(self, symbol: str, start_date: str, end_date: str) -> list[DailyBar]:
        symbol = normalize_symbol(symbol)
        start = datetime.strptime(start_date, "%Y-%m-%d").date()
        end = datetime.strptime(end_date, "%Y-%m-%d").date()
        seed = sum(ord(ch) for ch in symbol)
        rng = random.Random(seed)
        price = 20 + seed % 60
        previous_close = price
        bars: list[DailyBar] = []
        for day in _business_days(start, end):
            drift = math.sin((day.toordinal() + seed) / 17) * 0.012
            shock = rng.uniform(-0.018, 0.018)
            open_ = max(1, previous_close * (1 + rng.uniform(-0.008, 0.008)))
            close = max(1, open_ * (1 + drift + shock))
            high = max(open_, close) * (1 + rng.uniform(0.002, 0.018))
            low = min(open_, close) * (1 - rng.uniform(0.002, 0.018))
            volume = 2_000_000 + rng.randint(0, 3_000_000)
            bars.append(
                DailyBar(
                    symbol=symbol,
                    trade_date=day.isoformat(),
                    open=round(open_, 3),
                    high=round(high, 3),
                    low=round(low, 3),
                    close=round(close, 3),
                    pre_close=round(previous_close, 3),
                    volume=volume,
                    amount=round(volume * close, 2),
                    source=self.name,
                    turnover=round(rng.uniform(0.5, 8), 4),
                    pct_chg=round((close / previous_close - 1) * 100, 4) if previous_close else 0,
                )
            )
            previous_close = close
        return bars


def _business_days(start: date, end: date) -> Iterable[date]:
    day = start
    while day <= end:
        if day.weekday() < 5:
            yield day
        day += timedelta(days=1)


def get_source(name: str = "auto"):
    lowered = name.lower()
    if lowered == "akshare":
        return AkShareSource()
    if lowered == "eastmoney":
        return EastMoneySource()
    if lowered == "sample":
        return SampleSource()
    if lowered == "auto":
        try:
            return AkShareSource()
        except DataSourceError:
            return EastMoneySource()
    raise DataSourceError(f"未知数据源：{name}")
