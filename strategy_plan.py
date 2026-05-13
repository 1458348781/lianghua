"""基于《策略.docx》整理的两个可实现短线策略方法。"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal


Signal = Literal["buy", "sell", "hold", "avoid"]
MarketBoard = Literal["main", "chinext", "star", "other"]
TMode = Literal["buy", "sell"]


@dataclass(frozen=True)
class StrategyDecision:
    signal: Signal
    score: int
    reasons: list[str] = field(default_factory=list)
    actions: list[str] = field(default_factory=list)
    invalid_reasons: list[str] = field(default_factory=list)

    @property
    def is_valid(self) -> bool:
        return not self.invalid_reasons


@dataclass(frozen=True)
class Candle:
    open: float
    high: float
    low: float
    close: float
    volume: float = 0.0

    @property
    def body(self) -> float:
        return abs(self.close - self.open)

    @property
    def lower_shadow(self) -> float:
        return min(self.open, self.close) - self.low

    @property
    def upper_shadow(self) -> float:
        return self.high - max(self.open, self.close)


def _pct(current: float, base: float) -> float:
    if base <= 0:
        raise ValueError("base must be greater than 0")
    return (current - base) / base * 100


def _limit_up_price(previous_close: float, limit_pct: float = 10.0) -> float:
    return previous_close * (1 + limit_pct / 100)


def next_day_t_strategy(
    *,
    mode: TMode,
    market_expectation_bad: bool = False,
    market_cap_billion: float | None = None,
    current_price: float | None = None,
    support_price: float | None = None,
    yesterday: Candle | None = None,
    today: Candle | None = None,
    yesterday_close: float | None = None,
    near_yesterday_low_pct: float = 1.0,
) -> StrategyDecision:
    """
    隔日 T 策略：mode="buy" 判断低吸接回，mode="sell" 判断高抛/减仓。

    买入核心：不在精确支撑位挂单，等待价格脱离支撑 1%~1.5% 作为确认费。
    卖出核心：大盘预期差、乌云盖顶、高位放量长上影优先卖出。
    """
    reasons: list[str] = []
    actions: list[str] = []
    invalid_reasons: list[str] = []
    score = 0

    if market_expectation_bad:
        return StrategyDecision(
            signal="sell" if mode == "sell" else "avoid",
            score=100,
            reasons=["大盘预期不好时，个股技术支撑优先级失效"],
            actions=["无条件卖出、减仓或暂停低吸，不迷信个股支撑形态"],
        )

    if mode == "buy":
        if current_price is None or support_price is None:
            invalid_reasons.append("买入模式必须提供 current_price 和 support_price")
            return StrategyDecision("avoid", 0, invalid_reasons=invalid_reasons)

        confirm_pct = 1.5 if market_cap_billion is not None and market_cap_billion >= 400 else 1.0
        if market_cap_billion is not None and market_cap_billion < 200:
            confirm_pct = 1.0

        distance_from_support = _pct(current_price, support_price)
        if distance_from_support >= confirm_pct:
            score += 45
            reasons.append(f"当前价已脱离支撑位 {distance_from_support:.2f}%，达到 {confirm_pct:.1f}% 确认费要求")
            actions.append("可分批接回，买在支撑被验证后的右侧反弹")
        elif 0 <= distance_from_support < confirm_pct:
            score += 10
            reasons.append(f"当前价仅脱离支撑位 {distance_from_support:.2f}%，确认不足")
            actions.append(f"继续等待价格高于支撑位至少 {confirm_pct:.1f}% 后再买")
        else:
            reasons.append(f"当前价低于支撑位 {abs(distance_from_support):.2f}%，仍在破位风险区")
            actions.append("不接下跌飞刀，等待重新站回并脱离支撑")

        if today is not None:
            support_tolerance = max(support_price * 0.001, 0.01)
            if abs(today.low - support_price) <= support_tolerance:
                score -= 25
                reasons.append("最低价几乎采实均线/支撑，说明承接冷漠，按破位风险处理")
                actions.append("降低仓位或放弃本次低吸")

        if yesterday is not None and yesterday.body > 0 and yesterday.lower_shadow > yesterday.body * 3:
            if current_price <= yesterday.low * (1 + near_yesterday_low_pct / 100):
                score += 25
                reasons.append(f"昨日下影线超过实体 3 倍，今日回踩至昨日最低价 {near_yesterday_low_pct:.1f}% 内")
                actions.append("可按二次探底支撑做小仓试探")
            else:
                score -= 10
                reasons.append("昨日下影线过长但今日尚未二次探底到位")
                actions.append("等待接近昨日最低价 1% 内再考虑")

        signal: Signal = "buy" if score >= 45 else "hold" if score > 0 else "avoid"
        return StrategyDecision(signal, score, reasons, actions, invalid_reasons)

    if mode == "sell":
        if today is None:
            invalid_reasons.append("卖出模式必须提供 today")
            return StrategyDecision("avoid", 0, invalid_reasons=invalid_reasons)

        if yesterday_close is not None and today.open > yesterday_close and today.close < yesterday_close:
            score += 45
            reasons.append("出现乌云盖顶：高开后收盘跌破昨日收盘价")
            actions.append("及时卖出或至少降低 T 仓，防止次日惯性下杀")

        if today.body > 0 and today.upper_shadow > today.body * 2:
            score += 20
            reasons.append("出现长上影，冲高后被卖盘压回")
            actions.append("若处于近期高位且放量，应优先高抛")

        if today.volume > 0 and yesterday is not None and yesterday.volume > 0 and today.volume >= yesterday.volume * 1.8:
            score += 20
            reasons.append("相对昨日明显放量，上影或冲高回落的出货风险增强")
            actions.append("不追高，优先兑现利润")

        signal = "sell" if score >= 45 else "hold"
        if not reasons:
            reasons.append("未出现明确卖出形态")
            actions.append("继续按原计划持仓或等待更清晰的高抛信号")
        return StrategyDecision(signal, score, reasons, actions, invalid_reasons)

    invalid_reasons.append("mode must be 'buy' or 'sell'")
    return StrategyDecision("avoid", 0, invalid_reasons=invalid_reasons)


def divergence_flow_strategy(
    *,
    day0_sideways_days: int,
    day0_average_close: float,
    day1: Candle,
    day1_previous_close: float,
    day2: Candle,
    board: MarketBoard = "main",
    strongest_sector_related: bool = False,
    day2_touched_limit_up: bool = False,
    day2_is_second_divergence: bool = False,
    day2_high_level_sideways: bool = False,
    current_price: float | None = None,
    ma5: float | None = None,
    limit_pct: float = 10.0,
) -> StrategyDecision:
    """
    分歧流策略：先校验 Day1 涨停、Day2 分歧安全区间、量能、横盘天数和收盘强度。

    买点不是 Day2 直接追入，而是所有条件满足后，后续回调到 5 日均线附近介入。
    """
    reasons: list[str] = []
    actions: list[str] = []
    invalid_reasons: list[str] = []
    score = 0

    if board != "main":
        invalid_reasons.append("该战法仅建议用于主板，创业板/科创板波动区间过大")

    day1_limit_up = _limit_up_price(day1_previous_close, limit_pct)
    if day1.close < day1_limit_up * 0.995:
        invalid_reasons.append("Day 1 未有效涨停，无法吸引足够人气")
    else:
        score += 15
        reasons.append("Day 1 有效涨停，满足一致性和人气条件")

    day2_high_pct = _pct(day2.high, day1.close)
    day2_low_pct = _pct(day2.low, day1.close)
    if day2_high_pct >= 8:
        invalid_reasons.append(f"Day 2 最高涨幅 {day2_high_pct:.2f}% 不小于 +8%，存在冲板失败埋人风险")
    else:
        score += 15
        reasons.append(f"Day 2 最高涨幅 {day2_high_pct:.2f}% 小于 +8%，未过强诱发打板套牢")

    if day2_low_pct <= -3:
        invalid_reasons.append(f"Day 2 最深跌幅 {day2_low_pct:.2f}% 不大于 -3%，跌破短线心理盾")
    else:
        score += 15
        reasons.append(f"Day 2 最深跌幅 {day2_low_pct:.2f}% 高于 -3%，分歧未崩溃")

    if day2_touched_limit_up or day2.high >= _limit_up_price(day1.close, limit_pct) * 0.995:
        invalid_reasons.append("Day 2 触碰或接近涨停后未封住，属于冲板失败")
    else:
        score += 10
        reasons.append("Day 2 未触碰涨停，规避冲板失败抛压")

    if day1.volume <= 0 or day2.volume <= 0:
        invalid_reasons.append("必须提供 Day 1 和 Day 2 的成交量")
    elif day2.volume >= day1.volume * 2:
        invalid_reasons.append("Day 2 成交量达到 Day 1 的 2 倍以上，按倍量出货处理")
    else:
        score += 15
        reasons.append("Day 2 成交量小于 Day 1 的 2 倍，未触发倍量出货红线")

    if day0_sideways_days >= 22:
        invalid_reasons.append("Day 1 前横盘时间不小于 22 个交易日，容易转为 N 字/龙回头洗盘")
    else:
        score += 10
        reasons.append(f"Day 1 前横盘 {day0_sideways_days} 天，小于 22 天")

    if day2.close <= day1.close:
        invalid_reasons.append("Day 2 收盘价未高于 Day 1 涨停价，有乌云盖顶风险")
    else:
        score += 10
        reasons.append("Day 2 收盘价高于 Day 1 涨停价，维持强势")

    if strongest_sector_related:
        score += 20
        reasons.append("标的与当日最强板块高度相关，情绪借势加分")
    else:
        score -= 10
        reasons.append("未确认与最强板块相关，情绪战法胜率下降")

    if day2_high_level_sideways:
        score += 10
        reasons.append("Day 2 高位横盘，说明有资金承接并维持涨停价附近强势")

    if day2_is_second_divergence:
        score -= 20
        reasons.append("出现二次分歧，正常市场中原则上回避")

    if current_price is not None and ma5 is not None:
        distance_to_ma5 = abs(_pct(current_price, ma5))
        if distance_to_ma5 <= 1.0:
            score += 15
            reasons.append(f"当前价距离 5 日均线 {distance_to_ma5:.2f}%，到达回调买点附近")
            actions.append("可在 5 日均线附近分批介入，跌破 -3% 心理盾及时止损")
        else:
            reasons.append(f"当前价距离 5 日均线 {distance_to_ma5:.2f}%，买点尚未贴近")
            actions.append("等待回调到 5 日均线附近，不在高位追入")
    else:
        actions.append("形态成立后，等待后续回调到 5 日均线附近再介入")

    if invalid_reasons:
        return StrategyDecision("avoid", score, reasons, actions, invalid_reasons)

    signal: Signal = "buy" if score >= 80 and current_price is not None and ma5 is not None and abs(_pct(current_price, ma5)) <= 1.0 else "hold"
    if signal == "hold" and score >= 80:
        actions.append("条件整体合格，但仍需等待 5 日均线附近的触发买点")

    return StrategyDecision(signal, score, reasons, actions, invalid_reasons)


if __name__ == "__main__":
    t_result = next_day_t_strategy(
        mode="buy",
        market_cap_billion=150,
        current_price=101.2,
        support_price=100,
        yesterday=Candle(open=102, high=103, low=99, close=101, volume=100000),
        today=Candle(open=100.2, high=101.5, low=100.1, close=101.2, volume=80000),
    )
    print("隔日T策略示例：", t_result)

    flow_result = divergence_flow_strategy(
        day0_sideways_days=12,
        day0_average_close=9.2,
        day1=Candle(open=9.5, high=10.0, low=9.4, close=10.0, volume=280000),
        day1_previous_close=9.09,
        day2=Candle(open=10.15, high=10.7, low=9.85, close=10.2, volume=420000),
        strongest_sector_related=True,
        current_price=10.05,
        ma5=10.0,
    )
    print("分歧流策略示例：", flow_result)
