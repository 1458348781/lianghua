from __future__ import annotations

from bisect import bisect_right
from dataclasses import dataclass
from typing import Any


class Strategy:
    name = "base"
    label = "基础策略"
    description = ""
    params: dict[str, Any] = {}

    def __init__(self, **params: Any) -> None:
        merged = dict(self.params)
        merged.update({k: v for k, v in params.items() if v is not None})
        self.params = merged

    def init(self, context: "Context") -> None:
        pass

    def on_bar(self, context: "Context", data: dict[str, dict[str, Any]]) -> None:
        raise NotImplementedError


@dataclass
class TargetOrder:
    symbol: str
    target_percent: float
    reason: str = ""
    price: float | None = None
    execute_now: bool = False


class Context:
    def __init__(self, config: dict[str, Any], history: dict[str, list[dict[str, Any]]]) -> None:
        self.config = config
        self.history = history
        self._dates = {symbol: [row["trade_date"] for row in rows] for symbol, rows in history.items()}
        self.current_date = ""
        self.portfolio: Any = None
        self._orders: list[TargetOrder] = []

    def order_target_percent(
        self,
        symbol: str,
        percent: float,
        reason: str = "",
        price: float | None = None,
        execute_now: bool = False,
    ) -> None:
        bounded = max(0.0, min(1.0, float(percent)))
        self._orders.append(
            TargetOrder(symbol=symbol, target_percent=bounded, reason=reason, price=price, execute_now=execute_now)
        )

    def drain_orders(self) -> list[TargetOrder]:
        orders = self._orders
        self._orders = []
        return orders

    def bars_until(self, symbol: str, date: str) -> list[dict[str, Any]]:
        rows = self.history.get(symbol, [])
        index = bisect_right(self._dates.get(symbol, []), date)
        return rows[:index]

    def bars_window_until(self, symbol: str, date: str, count: int) -> list[dict[str, Any]]:
        rows = self.history.get(symbol, [])
        index = bisect_right(self._dates.get(symbol, []), date)
        return rows[max(0, index - count) : index]


class MovingAverageStrategy(Strategy):
    name = "moving_average"
    label = "均线择时"
    description = "短均线上穿长均线买入，下穿卖出；信号日后一个交易日开盘成交。"
    params = {"short_window": 5, "long_window": 20, "position_ratio": 1.0}

    def on_bar(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        short = int(self.params["short_window"])
        long = int(self.params["long_window"])
        ratio = float(self.params["position_ratio"])
        for symbol in context.config["symbols"]:
            history = context.bars_window_until(symbol, context.current_date, long + 1)
            if len(history) < long + 1:
                continue
            closes = [float(row["close"]) for row in history]
            prev_short = sum(closes[-short - 1 : -1]) / short
            prev_long = sum(closes[-long - 1 : -1]) / long
            now_short = sum(closes[-short:]) / short
            now_long = sum(closes[-long:]) / long
            if prev_short <= prev_long and now_short > now_long:
                context.order_target_percent(symbol, ratio / len(context.config["symbols"]))
            elif prev_short >= prev_long and now_short < now_long:
                context.order_target_percent(symbol, 0.0)


class MomentumStrategy(Strategy):
    name = "momentum"
    label = "动量月度调仓"
    description = "每月第一个交易日选过去 N 日涨幅最高的前 K 只股票，等权持有。"
    params = {"lookback": 60, "top_k": 3}

    def on_bar(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        calendar = context.config.get("calendar", [])
        index = context.config.get("calendar_index", {}).get(context.current_date, 0)
        previous_date = calendar[index - 1] if index > 0 else ""
        if previous_date[:7] == context.current_date[:7]:
            return

        lookback = int(self.params["lookback"])
        top_k = int(self.params["top_k"])
        scores: list[tuple[str, float]] = []
        for symbol in context.config["symbols"]:
            history = context.bars_window_until(symbol, context.current_date, lookback + 1)
            if len(history) <= lookback:
                continue
            old = float(history[-lookback - 1]["close"])
            now = float(history[-1]["close"])
            if old > 0:
                scores.append((symbol, now / old - 1))

        winners = {symbol for symbol, _ in sorted(scores, key=lambda item: item[1], reverse=True)[:top_k]}
        weight = 1 / len(winners) if winners else 0
        for symbol in context.config["symbols"]:
            context.order_target_percent(symbol, weight if symbol in winners else 0)


class DivergenceStrategy(Strategy):
    name = "divergence_tactic"
    label = "分歧流2"
    description = "分歧流2：T/T+1 可控分歧后，T+2 盘中突破即买。"
    params = {
        "max_positions": 2,
        "hold_days": 5,
        "stop_loss": -0.02,
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

    def init(self, context: Context) -> None:
        self.entries: dict[str, dict[str, Any]] = {}
        self.signals_by_date: dict[str, list[tuple[str, float]]] = {}
        start_date = context.config.get("start_date", "0000-00-00")
        end_date = context.config.get("end_date", "9999-99-99")
        for symbol, rows in context.history.items():
            if len(rows) < 35:
                continue
            for index in range(34, len(rows)):
                t = rows[index - 2]
                t1 = rows[index - 1]
                t2 = rows[index]
                trade_date = t2["trade_date"]
                if trade_date < start_date or trade_date > end_date:
                    continue
                window = rows[index - 34 : index + 1]
                if self._matches(symbol, t, t1, t2, window):
                    self.signals_by_date.setdefault(trade_date, []).append((symbol, self._entry_price(t1, t2)))

    def before_open(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        max_positions = max(1, int(self.params["max_positions"]))
        slots = max(0, max_positions - len(self.entries))
        if slots <= 0:
            return
        candidates = [
            (symbol, price)
            for symbol, price in self.signals_by_date.get(context.current_date, [])
            if symbol not in self.entries and symbol in data
        ][:slots]
        if not candidates:
            return
        final_count = len(self.entries) + len(candidates)
        target_weight = self._target_weight(final_count)
        for symbol in self.entries:
            context.order_target_percent(symbol, target_weight, "position rebalance")
        for symbol, entry_price in candidates:
            context.order_target_percent(symbol, target_weight, "breakout entry", price=entry_price)
            self.entries[symbol] = {"entry_date": context.current_date, "entry_price": entry_price}

    def on_bar(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        for symbol, entry in list(self.entries.items()):
            if symbol not in data:
                continue
            row = data[symbol]
            entry_price = float(entry["entry_price"])
            close = self._safe_float(row["close"])
            stop_price = entry_price * (1 + float(self.params["stop_loss"]))
            if row["trade_date"] != entry["entry_date"] and self._safe_float(row["low"]) <= stop_price:
                context.order_target_percent(symbol, 0, "stop loss -3%", price=stop_price, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            if self._is_limit_up(symbol, row):
                continue
            if self._hit_limit_up(symbol, row):
                context.order_target_percent(symbol, 0, "limit failed", price=close, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            hold_days = int(self.params.get("hold_days", 0) or 0)
            if hold_days > 0 and self._held_days_after_entry(context, entry["entry_date"], row["trade_date"]) >= hold_days:
                context.order_target_percent(symbol, 0, "hold days expired", price=close, execute_now=True)
                self.entries.pop(symbol, None)

    def _matches(
        self,
        symbol: str,
        t: dict[str, Any],
        t1: dict[str, Any],
        t2: dict[str, Any],
        history: list[dict[str, Any]],
    ) -> bool:
        return self._setup_ok(symbol, t, t1, history) and self._entry_day_ok(t1, t2)

    def _setup_ok(
        self,
        symbol: str,
        t: dict[str, Any],
        t1: dict[str, Any],
        history: list[dict[str, Any]],
    ) -> bool:
        close = self._safe_float(t["close"])
        if not (float(self.params["min_price"]) <= close <= float(self.params["max_price"])):
            return False
        if int(self._safe_float(t.get("is_st", 0))) == 1 or int(self._safe_float(t1.get("is_st", 0))) == 1:
            return False

        pre_five = history[-8:-3]
        if len(pre_five) < 5:
            return False
        avg_vol_5 = sum(self._safe_float(row["volume"]) for row in pre_five) / 5
        volume_ratio = self._safe_float(t["volume"]) / avg_vol_5 if avg_vol_5 else 0
        turnover = self._safe_float(t.get("turnover", 0))
        day1_ok = (
            self._is_limit_up(symbol, t)
            and not self._is_one_price_board(t)
            and float(self.params["day1_min_volume_ratio"])
            <= volume_ratio
            <= float(self.params["day1_max_volume_ratio"])
            and float(self.params["min_turnover"]) <= turnover <= float(self.params["max_turnover"])
        )
        if not day1_ok:
            return False

        last_30 = history[-33:-3]
        last_20 = history[-23:-3]
        if len(last_30) < 30 or len(last_20) < 20:
            return False
        high_30 = max(self._safe_float(row["high"]) for row in last_30)
        low_30 = min(self._safe_float(row["low"]) for row in last_30)
        ret_20_base = self._safe_float(last_20[0]["close"])
        ret_20 = close / ret_20_base - 1 if ret_20_base else 0
        if (
            low_30 <= 0
            or high_30 / low_30 - 1 < float(self.params["range_min_amplitude_30"])
            or ret_20 < float(self.params["range_min_return_20"])
        ):
            return False

        day2_pct_chg = self._pct_chg(t1)
        day2_ok = (
            float(self.params["day2_min_pct_chg"])
            <= day2_pct_chg
            <= float(self.params["day2_max_pct_chg"])
            and self._safe_float(t1["volume"]) <= self._safe_float(t["volume"]) * float(self.params["day2_max_volume_ratio"])
            and self._close_position(t1) >= float(self.params["day2_min_close_position"])
            and self._upper_shadow(t1) <= float(self.params["day2_max_upper_shadow"])
            and self._safe_float(t1["close"]) >= self._safe_float(t["close"]) * float(self.params["day2_min_close_vs_day1_close"])
        )
        return day2_ok

    def _entry_day_ok(self, previous_row: dict[str, Any], entry_row: dict[str, Any]) -> bool:
        previous_close = self._safe_float(entry_row.get("pre_close", 0)) or self._safe_float(previous_row["close"])
        open_ = self._safe_float(entry_row["open"])
        high = self._safe_float(entry_row["high"])
        if previous_close <= 0 or open_ <= 0 or high <= 0:
            return False
        open_gap_pct_chg = open_ / previous_close * 100 - 100
        high_from_open_pct_chg = high / open_ * 100 - 100
        return (
            float(self.params["entry_min_open_gap_pct_chg"])
            <= open_gap_pct_chg
            <= float(self.params["entry_max_open_gap_pct_chg"])
            and high_from_open_pct_chg >= float(self.params["entry_min_high_from_open_pct_chg"])
            and high > self._safe_float(previous_row["high"])
        )

    def _entry_price(self, previous_row: dict[str, Any], entry_row: dict[str, Any]) -> float:
        open_ = self._safe_float(entry_row["open"])
        trigger_from_open = open_ * (1 + float(self.params["entry_min_high_from_open_pct_chg"]) / 100)
        return max(self._safe_float(previous_row["high"]), trigger_from_open)

    def _is_limit_up(self, symbol: str, row: dict[str, Any]) -> bool:
        threshold = 19.5 if symbol.startswith(("300", "301", "688", "689")) else 9.75
        return self._pct_chg(row) >= threshold and self._safe_float(row["close"]) >= self._safe_float(row["high"]) * 0.995

    def _hit_limit_up(self, symbol: str, row: dict[str, Any]) -> bool:
        threshold = 19.5 if symbol.startswith(("300", "301", "688", "689")) else 9.75
        pre_close = self._safe_float(row.get("pre_close", 0))
        return (self._safe_float(row["high"]) / pre_close - 1) * 100 >= threshold if pre_close else False

    def _is_one_price_board(self, row: dict[str, Any]) -> bool:
        open_ = self._safe_float(row["open"])
        high = self._safe_float(row["high"])
        low = self._safe_float(row["low"])
        close = self._safe_float(row["close"])
        return abs(open_ - high) < 1e-8 and abs(high - low) < 1e-8 and abs(low - close) < 1e-8

    def _pct_chg(self, row: dict[str, Any]) -> float:
        pct = self._safe_float(row.get("pct_chg", 0))
        if pct:
            return pct
        pre_close = self._safe_float(row.get("pre_close", 0))
        return (self._safe_float(row["close"]) / pre_close - 1) * 100 if pre_close else 0

    def _close_position(self, row: dict[str, Any]) -> float:
        high = self._safe_float(row["high"])
        low = self._safe_float(row["low"])
        return (self._safe_float(row["close"]) - low) / (high - low) if high > low else 0

    def _upper_shadow(self, row: dict[str, Any]) -> float:
        close = self._safe_float(row["close"])
        return (self._safe_float(row["high"]) - close) / close if close else 1

    def _target_weight(self, position_count: int) -> float:
        return min(0.5, 1 / position_count) if position_count > 0 else 0.0

    def _calendar_distance(self, context: Context, start: str, end: str) -> int:
        calendar_index = context.config.get("calendar_index", {})
        if start not in calendar_index or end not in calendar_index:
            return 0
        return calendar_index[end] - calendar_index[start] + 1

    def _held_days_after_entry(self, context: Context, start: str, end: str) -> int:
        calendar_index = context.config.get("calendar_index", {})
        if start not in calendar_index or end not in calendar_index:
            return 0
        return max(0, calendar_index[end] - calendar_index[start])

    def _ma(self, rows: list[dict[str, Any]], count: int) -> float:
        if len(rows) < count:
            return 0.0
        return sum(self._safe_float(row["close"]) for row in rows[-count:]) / count

    def _safe_float(self, value: Any) -> float:
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0


class GapTStrategy(DivergenceStrategy):
    name = "gap_t_tactic"
    label = "隔日T"
    description = "隔日T日线代理：回踩5日线支撑后，脱离支撑确认买入；遇乌云盖顶、放量长上影、止损或到期卖出。"
    params = {
        "max_positions": 3,
        "hold_days": 3,
        "stop_loss": -0.03,
        "min_price": 3,
        "max_price": 500,
        "support_window": 5,
        "support_confirm_pct_chg": 1.0,
        "support_near_pct_chg": 1.0,
        "long_lower_shadow_ratio": 3.0,
        "high_position_lookback": 20,
        "upper_shadow_sell": 0.08,
        "volume_sell_ratio": 1.8,
    }

    def init(self, context: Context) -> None:
        self.entries: dict[str, dict[str, Any]] = {}
        self.signals_by_date: dict[str, list[tuple[str, float]]] = {}
        start_date = context.config.get("start_date", "0000-00-00")
        end_date = context.config.get("end_date", "9999-99-99")
        lookback = max(8, int(self.params["high_position_lookback"]) + 2)
        for symbol, rows in context.history.items():
            if len(rows) < lookback:
                continue
            for index in range(lookback, len(rows)):
                row = rows[index]
                trade_date = row["trade_date"]
                if trade_date < start_date or trade_date > end_date:
                    continue
                window = rows[: index + 1]
                if self._gap_t_entry_ok(row, window):
                    self.signals_by_date.setdefault(trade_date, []).append((symbol, self._gap_t_entry_price(window)))

    def _gap_t_entry_ok(self, row: dict[str, Any], history: list[dict[str, Any]]) -> bool:
        close = self._safe_float(row["close"])
        if not (float(self.params["min_price"]) <= close <= float(self.params["max_price"])):
            return False
        ma = self._ma(history[:-1], int(self.params["support_window"]))
        if ma <= 0:
            return False
        low = self._safe_float(row["low"])
        high = self._safe_float(row["high"])
        near = float(self.params["support_near_pct_chg"]) / 100
        confirm = float(self.params["support_confirm_pct_chg"]) / 100
        touched = low <= ma * (1 + near)
        confirmed = high >= ma * (1 + confirm) and close >= ma
        exact_weak_touch = abs(low / ma - 1) <= 0.002 and close < ma * (1 + confirm)
        long_lower = self._lower_shadow_ratio(row) >= float(self.params["long_lower_shadow_ratio"])
        return touched and confirmed and not exact_weak_touch and not long_lower

    def _gap_t_entry_price(self, history: list[dict[str, Any]]) -> float:
        ma = self._ma(history[:-1], int(self.params["support_window"]))
        return ma * (1 + float(self.params["support_confirm_pct_chg"]) / 100)

    def on_bar(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        for symbol, entry in list(self.entries.items()):
            row = data.get(symbol)
            if not row:
                continue
            close = self._safe_float(row["close"])
            entry_price = float(entry["entry_price"])
            stop_price = entry_price * (1 + float(self.params["stop_loss"]))
            history = context.bars_window_until(symbol, row["trade_date"], int(self.params["high_position_lookback"]) + 2)
            if row["trade_date"] != entry["entry_date"] and self._safe_float(row["low"]) <= stop_price:
                context.order_target_percent(symbol, 0, "gap t stop loss", price=stop_price, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            if len(history) >= 2 and self._dark_cloud(history[-2], row):
                context.order_target_percent(symbol, 0, "dark cloud sell", price=close, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            if self._high_volume_upper_shadow(row, history):
                context.order_target_percent(symbol, 0, "high volume upper shadow sell", price=close, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            hold_days = int(self.params.get("hold_days", 0) or 0)
            if hold_days > 0 and self._calendar_distance(context, entry["entry_date"], row["trade_date"]) >= hold_days:
                context.order_target_percent(symbol, 0, "hold days expired", price=close, execute_now=True)
                self.entries.pop(symbol, None)

    def _dark_cloud(self, prev: dict[str, Any], row: dict[str, Any]) -> bool:
        return self._safe_float(row["open"]) > self._safe_float(prev["close"]) and self._safe_float(row["close"]) < self._safe_float(prev["close"])

    def _high_volume_upper_shadow(self, row: dict[str, Any], history: list[dict[str, Any]]) -> bool:
        if len(history) < 6:
            return False
        recent_high = max(self._safe_float(item["high"]) for item in history[:-1])
        avg_vol = sum(self._safe_float(item["volume"]) for item in history[-6:-1]) / 5
        return (
            self._safe_float(row["high"]) >= recent_high * 0.98
            and avg_vol > 0
            and self._safe_float(row["volume"]) >= avg_vol * float(self.params["volume_sell_ratio"])
            and self._upper_shadow(row) >= float(self.params["upper_shadow_sell"])
        )

    def _lower_shadow_ratio(self, row: dict[str, Any]) -> float:
        open_ = self._safe_float(row["open"])
        close = self._safe_float(row["close"])
        low = self._safe_float(row["low"])
        body = abs(close - open_)
        lower = min(open_, close) - low
        return lower / body if body > 0 else 0

class DivergenceFlowStrategy(DivergenceStrategy):
    name = "divergence_flow"
    label = "分歧流"
    description = "完整策略方案中的分歧流：Day1涨停、Day2可控分歧，后续回调5日线附近承接买入。"
    params = {
        "max_positions": 3,
        "hold_days": 3,
        "stop_loss": -0.03,
        "min_price": 3,
        "max_price": 500,
        "day2_high_limit_pct_chg": 8.0,
        "day2_low_limit_pct_chg": -3.0,
        "day2_volume_limit_ratio": 2.0,
        "pre_sideways_days": 22,
        "pullback_ma_window": 5,
        "pullback_near_pct_chg": 1.0,
        "max_wait_days": 8,
    }

    def init(self, context: Context) -> None:
        self.entries: dict[str, dict[str, Any]] = {}
        self.signals_by_date: dict[str, list[tuple[str, float]]] = {}
        self.signal_reasons_by_date: dict[str, dict[str, str]] = {}
        start_date = context.config.get("start_date", "0000-00-00")
        end_date = context.config.get("end_date", "9999-99-99")
        max_wait = int(self.params["max_wait_days"])
        for symbol, rows in context.history.items():
            if len(rows) < 35:
                continue
            for day2_index in range(23, len(rows)):
                day1 = rows[day2_index - 1]
                day2 = rows[day2_index]
                if not self._flow_setup_ok(symbol, day1, day2, rows[: day2_index + 1]):
                    continue
                if not self._flow_sideways_too_long(rows[:day2_index]):
                    trade_date = day2["trade_date"]
                    if start_date <= trade_date <= end_date:
                        self._add_flow_signal(trade_date, symbol, self._safe_float(day2["close"]), "flow day2 close entry")
                    continue
                end_index = min(len(rows) - 1, day2_index + max_wait)
                for entry_index in range(day2_index + 1, end_index + 1):
                    entry = rows[entry_index]
                    trade_date = entry["trade_date"]
                    if trade_date < start_date or trade_date > end_date:
                        continue
                    window = rows[: entry_index + 1]
                    if self._flow_entry_ok(entry, window):
                        price = self._ma(window[:-1], int(self.params["pullback_ma_window"])) * (
                            1 + float(self.params["pullback_near_pct_chg"]) / 100
                        )
                        self._add_flow_signal(trade_date, symbol, price, "flow 5ma pullback entry")
                        break

    def before_open(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        return None

    def _add_flow_signal(self, trade_date: str, symbol: str, price: float, reason: str) -> None:
        self.signals_by_date.setdefault(trade_date, []).append((symbol, price))
        self.signal_reasons_by_date.setdefault(trade_date, {})[symbol] = reason

    def _flow_setup_ok(self, symbol: str, day1: dict[str, Any], day2: dict[str, Any], history: list[dict[str, Any]]) -> bool:
        if not self._is_main_board(symbol):
            return False
        close = self._safe_float(day1["close"])
        if not (float(self.params["min_price"]) <= close <= float(self.params["max_price"])):
            return False
        if int(self._safe_float(day1.get("is_st", 0))) == 1 or int(self._safe_float(day2.get("is_st", 0))) == 1:
            return False
        if not self._is_limit_up(symbol, day1):
            return False
        if self._flow_sideways_too_long(history[:-1]):
            return False
        base = self._safe_float(day1["close"])
        if base <= 0:
            return False
        day2_high_pct = self._safe_float(day2["high"]) / base * 100 - 100
        day2_low_pct = self._safe_float(day2["low"]) / base * 100 - 100
        if day2_high_pct >= float(self.params["day2_high_limit_pct_chg"]):
            return False
        if day2_low_pct <= float(self.params["day2_low_limit_pct_chg"]):
            return False
        if self._hit_limit_up(symbol, day2):
            return False
        if self._safe_float(day2["volume"]) >= self._safe_float(day1["volume"]) * float(self.params["day2_volume_limit_ratio"]):
            return False
        if self._safe_float(day2["close"]) <= base:
            return False
        return True

    def _flow_entry_ok(self, entry: dict[str, Any], history: list[dict[str, Any]]) -> bool:
        ma = self._ma(history[:-1], int(self.params["pullback_ma_window"]))
        if ma <= 0:
            return False
        near = float(self.params["pullback_near_pct_chg"]) / 100
        return self._safe_float(entry["low"]) <= ma * (1 + near) and self._safe_float(entry["close"]) >= ma

    def on_bar(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        for symbol, entry in list(self.entries.items()):
            row = data.get(symbol)
            if not row:
                continue
            close = self._safe_float(row["close"])
            entry_price = float(entry["entry_price"])
            stop_price = entry_price * (1 + float(self.params["stop_loss"]))
            history = context.bars_window_until(symbol, row["trade_date"], 8)
            ma = self._ma(history[:-1], int(self.params["pullback_ma_window"]))
            if row["trade_date"] != entry["entry_date"] and self._safe_float(row["low"]) <= stop_price:
                context.order_target_percent(symbol, 0, "flow stop loss", price=stop_price, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            if self._hit_limit_up(symbol, row) and not self._is_limit_up(symbol, row):
                context.order_target_percent(symbol, 0, "flow limit failed", price=close, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            if ma > 0 and close < ma:
                context.order_target_percent(symbol, 0, "flow lost 5ma", price=close, execute_now=True)
                self.entries.pop(symbol, None)
                continue
            hold_days = int(self.params.get("hold_days", 0) or 0)
            if hold_days > 0 and self._calendar_distance(context, entry["entry_date"], row["trade_date"]) >= hold_days:
                context.order_target_percent(symbol, 0, "hold days expired", price=close, execute_now=True)
                self.entries.pop(symbol, None)
        self._enter_flow_signals(context, data)

    def _enter_flow_signals(self, context: Context, data: dict[str, dict[str, Any]]) -> None:
        max_positions = max(1, int(self.params["max_positions"]))
        slots = max(0, max_positions - len(self.entries))
        if slots <= 0:
            return
        candidates = [
            (symbol, price)
            for symbol, price in self.signals_by_date.get(context.current_date, [])
            if symbol not in self.entries and symbol in data
        ][:slots]
        if not candidates:
            return
        final_count = len(self.entries) + len(candidates)
        target_weight = self._target_weight(final_count)
        for symbol in self.entries:
            context.order_target_percent(symbol, target_weight, "flow position rebalance", execute_now=True)
        reasons = self.signal_reasons_by_date.get(context.current_date, {})
        for symbol, entry_price in candidates:
            reason = reasons.get(symbol, "flow entry")
            context.order_target_percent(symbol, target_weight, reason, price=entry_price, execute_now=True)
            self.entries[symbol] = {"entry_date": context.current_date, "entry_price": entry_price}

    def _flow_sideways_too_long(self, rows: list[dict[str, Any]]) -> bool:
        days = int(self.params["pre_sideways_days"])
        if len(rows) < days:
            return False
        window = rows[-days:]
        high = max(self._safe_float(row["high"]) for row in window)
        low = min(self._safe_float(row["low"]) for row in window)
        return low > 0 and high / low - 1 < float(self.params.get("pre_sideways_min_amplitude", 0.12))

    def _is_main_board(self, symbol: str) -> bool:
        code = symbol.split(".")[0]
        return code.startswith(("000", "001", "002", "003", "600", "601", "603", "605"))

STRATEGIES: dict[str, type[Strategy]] = {
    MovingAverageStrategy.name: MovingAverageStrategy,
    MomentumStrategy.name: MomentumStrategy,
    GapTStrategy.name: GapTStrategy,
    DivergenceFlowStrategy.name: DivergenceFlowStrategy,
    DivergenceStrategy.name: DivergenceStrategy,
}


def strategy_catalog() -> list[dict[str, Any]]:
    return [
        {
            "name": cls.name,
            "label": cls.label,
            "description": cls.description,
            "params": cls.params,
        }
        for cls in STRATEGIES.values()
    ]


def create_strategy(name: str, params: dict[str, Any] | None = None) -> Strategy:
    if name not in STRATEGIES:
        raise ValueError(f"未知策略：{name}")
    return STRATEGIES[name](**(params or {}))
