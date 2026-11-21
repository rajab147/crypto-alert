#!/usr/bin/env python3
"""
orderflow_choch_live.py
TREND-FOLLOWING Spot order-flow alerts + CHoCH trend filter.
Rules (CHoCH gate -> order-flow legs):
* "30m+1h"   : 1h+30m CONFLUENCE, gated by 4h CHoCH
* "1h_daily" : 1h flow,  gated by 1D CHoCH
* "4h_daily" : 4h flow,  gated by 1D CHoCH
* "4h_weekly": 4h flow,  gated by 1W CHoCH
* "1d_weekly": 1d (24h) flow, gated by 1W CHoCH  [separate CVD thresholds]
Universe: FIXED 100-coin whitelist (top-volume selection OFF).
Coins failing daily liquidity/trade limits are skipped for that day.
Cooldowns: same rule+side 120/180/240min | same side any rule 30min |
opposite side blocked 30min | any two alerts on a symbol >= 60s.
CHoCH display: NO [weak] tag. Candle-count limit REMOVED (no pruning).
1h CHoCH shown only when 1h order flow confirms it (display rule).
Anti-chop layer: displacement + range-skip + chop filter + flip gap.
Retest confirmation: POSTPONED (not implemented).
Alert clock: 12-hour AM/PM. Session-aware rest. Spot WS only. Alerts only.
"""
from __future__ import annotations

import asyncio
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import Any

import aiohttp
from dotenv import load_dotenv

import orderflow_paper as of

log = of.log

# ============================ OI + FUNDING (Bybit -> OKX fallback) ============================
class OIPoller:
    BYBIT = "https://api.bybit.com/v5/market/tickers?category=linear"
    OKX = "https://www.okx.com/api/v5/public/open-interest?instType=SWAP"

    def __init__(self) -> None:
        self.hist: dict[str, deque] = {}   # sym -> (ts, oi_usd, funding, source)
        self.order = ["bybit", "okx"]
        self.source = ""

    async def _get_json(self, session, url):
        async with session.get(url) as r:
            r.raise_for_status()
            return await r.json(content_type=None)

    async def _fetch_bybit(self, session):
        data = await self._get_json(session, self.BYBIT)
        if str(data.get("retCode")) != "0":
            raise RuntimeError(f"retCode {data.get('retCode')}")
        out = {}
        for t in data["result"]["list"]:
            sym = t.get("symbol", "")
            if not sym.endswith("USDT"):
                continue
            oi = float(t.get("openInterestValue") or 0)
            if oi > 0:
                out[sym] = (oi, float(t.get("fundingRate") or 0))
        if not out:
            raise RuntimeError("empty response")
        return out

    async def _fetch_okx(self, session):
        data = await self._get_json(session, self.OKX)
        if str(data.get("code")) != "0":
            raise RuntimeError(f"code {data.get('code')}")
        out = {}
        for t in data["data"]:
            inst = t.get("instId", "")
            if not inst.endswith("-USDT-SWAP"):
                continue
            sym = inst[: -len("-USDT-SWAP")].replace("-", "") + "USDT"
            oi = float(t.get("oiUsd") or 0)
            if oi > 0:
                out[sym] = (oi, None)   # OKX bulk has no funding
        if not out:
            raise RuntimeError("empty response")
        return out

    async def _poll_once(self, session) -> bool:
        for name in self.order:
            try:
                fn = self._fetch_bybit if name == "bybit" else self._fetch_okx
                data = await fn(session)
            except Exception as exc:
                log.warning("[OI] %s failed: %s", name, exc)
                continue
            now = time.time()
            for sym, (oi, fund) in data.items():
                d = self.hist.setdefault(sym, deque(maxlen=400))
                d.append((now, oi, fund, name))
            if name != self.source:
                log.info("[OI] source -> %s (%d symbols)", name, len(data))
            self.source = name
            return True
        return False

    async def run(self, session, stop) -> None:
        while not stop.is_set():
            ok = await self._poll_once(session)
            if not ok:
                log.warning("[OI] both Bybit and OKX failed")
            try:
                await asyncio.wait_for(stop.wait(), timeout=60)
            except asyncio.TimeoutError:
                pass

    def change(self, sym: str, minutes: float):
        d = self.hist.get(sym)
        if not d or len(d) < 2:
            return None
        now_ts, now_oi, _, src = d[-1]
        target = now_ts - minutes * 60
        # same source only: Bybit and OKX OI sizes differ
        old = next((x for x in d if x[0] >= target and x[3] == src), None)
        if old is None or old[0] > target + 120 or old[1] <= 0:
            return None
        return (now_oi - old[1]) / old[1] * 100.0

    def line(self, sym: str, price_move_pct: float) -> str:
        d = self.hist.get(sym)
        if not d:
            return "OI: n/a (no perp or warming up)"
        if time.time() - d[-1][0] > 180:
            return "OI: n/a (source down)"
        c15, c60 = self.change(sym, 15), self.change(sym, 60)
        if c60 is None:
            return f"OI ({d[-1][3]}): warming up"
        fund = d[-1][2]
        fund_s = f"{fund * 100:+.4f}%" if fund is not None else "n/a"
        if price_move_pct < 0:
            tag = "NEW SHORTS (strong)" if c60 > 0.3 else "LONG CLOSING (weak)" if c60 < -0.3 else "flat"
        else:
            tag = "NEW LONGS (strong)" if c60 > 0.3 else "SHORT COVERING (weak)" if c60 < -0.3 else "flat"
        c15_s = f"{c15:+.2f}%" if c15 is not None else "n/a"
        return f"OI ({d[-1][3]}): 15m {c15_s} | 1h {c60:+.2f}% | funding {fund_s} | {tag}"


MERGE: dict[str, Any] = {
    "choch_timeframes": {
        "1h": {"seconds": 3600, "interval": "1h", "initial_candles": 1000},
        "4h": {"seconds": 14400, "interval": "4h", "initial_candles": 1000},
        "1d": {"seconds": 86400, "interval": "1d", "initial_candles": 1000},
        "1w": {"seconds": 604800, "interval": "1w", "initial_candles": 1000},
    },
    "context_frames": ["1d", "1w"],
    "gate_frames": ["1h", "4h"],
    "flow_gated_frames": ["1h"],
    "flow_gate_leg": "1h",
    "flow_gate_rule": "1h_daily",
    "refresh_delay_sec": 5,
    "loop_sec": 5,
    "concurrency": 3,
    "use_choch_context": True,
    "use_process_pool": True,
    "pool_workers": 2,
}

# ============================ CHoCH CONFIG ============================
SWING_STRENGTH = 3
USE_CLOSE_BREAK = True
STRICT_ONLY = False
USE_SWING_COUNT_STRENGTH = True
MIN_CONFIRM_SWINGS = 2
MAX_CONFIRM_SWINGS = 3
RANGE_FILTER_ENABLED = True
RANGE_TOLERANCE = 0.30          # 0.25 -> 0.30: زیادہ box پکڑے گا
RANGE_MODE = "skip"             # "weak" -> "skip": range میں CHoCH/trend flip بند
RANGE_WEAK_REASON = "range/sideways box"
WEEKEND_RANGE_FILTER = True
POST_MOVE_SIDEWAYS_FILTER = True
POST_MOVE_PCT = 2.0
POST_MOVE_LOOKBACK = 3
REQUIRE_DISPLACEMENT = True     # False -> True: مضبوط break ہی مانا جائے گا
ATR_PERIOD = 14
MIN_CLOSE_BEYOND_ATR = 0.25
MIN_BODY_ATR = 0.50
MIN_BODY_RATIO = 0.60
MIN_BREAK_DISTANCE_PCT = 0.15
MIN_BODY_PCT = 0.25
PUMP_DUMP_LOOKBACK = 6
PUMP_DUMP_MOVE_PCT = 4.0
SINGLE_CANDLE_PUMP_DUMP_PCT = 3.0
MARK_SAME_CANDLE_WEAK = True

# ===== Anti-chop layer =====
CHOP_FILTER_ENABLED = True
CHOP_LOOKBACK = 20
CHOP_ER_MAX = 0.30
CHOP_BOX_ATR_MULT = 3.0
CHOP_MIN_CANDLES = 30
MIN_FLIP_GAP_CANDLES = 2        # الٹا flip کم از کم 2 candles بعد (سوائے strong)
BLOCK_ON_UNKNOWN_TREND = False

# ============================ CHoCH LOGIC ============================
def is_swing_high(candles, idx, n):
    high = candles[idx][2]
    for k in range(idx - n, idx + n + 1):
        if k != idx and candles[k][2] >= high:
            return False
    return True


def is_swing_low(candles, idx, n):
    low = candles[idx][3]
    for k in range(idx - n, idx + n + 1):
        if k != idx and candles[k][3] <= low:
            return False
    return True


def was_high_broken_before(candles, level_idx, level_price, current_i, use_close=True):
    for k in range(level_idx + 1, current_i):
        px = candles[k][4] if use_close else candles[k][2]
        if px > level_price:
            return True
    return False


def was_low_broken_before(candles, level_idx, level_price, current_i, use_close=True):
    for k in range(level_idx + 1, current_i):
        px = candles[k][4] if use_close else candles[k][3]
        if px < level_price:
            return True
    return False


def calculate_atr(candles, period=14):
    atr = [None] * len(candles)
    if len(candles) < period:
        return atr
    trs = []
    for i in range(len(candles)):
        high, low = candles[i][2], candles[i][3]
        prev_close = candles[i - 1][4] if i > 0 else candles[i][4]
        trs.append(max(high - low, abs(high - prev_close), abs(low - prev_close)))
    atr[period - 1] = sum(trs[:period]) / period
    for i in range(period, len(trs)):
        atr[i] = (atr[i - 1] * (period - 1) + trs[i]) / period
    return atr


def break_quality_ok(candles, i, level_price, direction, atr_values):
    if not REQUIRE_DISPLACEMENT:
        return True
    _, o, h, l, c, _ = candles[i]
    beyond = (c - level_price) if direction == "bullish" else (level_price - c)
    if beyond <= 0:
        return False
    body = abs(c - o)
    candle_range = h - l
    body_ratio = body / candle_range if candle_range > 0 else 1.0
    atr_val = None
    if atr_values and i < len(atr_values) and atr_values[i]:
        atr_val = atr_values[i]
    if atr_val is not None:
        if beyond < atr_val * MIN_CLOSE_BEYOND_ATR:
            return False
        if body < atr_val * MIN_BODY_ATR:
            return False
    elif level_price > 0:
        if (beyond / level_price) * 100.0 < MIN_BREAK_DISTANCE_PCT:
            return False
        if (body / level_price) * 100.0 < MIN_BODY_PCT:
            return False
    return body_ratio >= MIN_BODY_RATIO


def find_strict_higher_low(candles, highs, lows, current_i, use_close=True):
    if len(highs) < 2 or not lows:
        return None
    h1, h2 = highs[-2], highs[-1]
    if h2["price"] <= h1["price"]:
        return None
    between = [x for x in lows if h1["idx"] < x["idx"] < h2["idx"]]
    if not between:
        return None
    lowest = min(between, key=lambda x: x["price"])
    if was_low_broken_before(candles, lowest["idx"], lowest["price"], current_i, use_close):
        return None
    out = dict(lowest)
    out["mode"] = "strict"
    return out


def find_strict_lower_high(candles, lows, highs, current_i, use_close=True):
    if len(lows) < 2 or not highs:
        return None
    l1, l2 = lows[-2], lows[-1]
    if l2["price"] >= l1["price"]:
        return None
    between = [x for x in highs if l1["idx"] < x["idx"] < l2["idx"]]
    if not between:
        return None
    highest = max(between, key=lambda x: x["price"])
    if was_high_broken_before(candles, highest["idx"], highest["price"], current_i, use_close):
        return None
    out = dict(highest)
    out["mode"] = "strict"
    return out


def find_last_unbroken_low(candles, lows, current_i, use_close=True):
    if not lows:
        return None
    x = lows[-1]
    if was_low_broken_before(candles, x["idx"], x["price"], current_i, use_close):
        return None
    out = dict(x)
    out["mode"] = "relaxed"
    return out


def find_last_unbroken_high(candles, highs, current_i, use_close=True):
    if not highs:
        return None
    x = highs[-1]
    if was_high_broken_before(candles, x["idx"], x["price"], current_i, use_close):
        return None
    out = dict(x)
    out["mode"] = "relaxed"
    return out


def detect_range_box(highs, lows, tolerance):
    if len(highs) < 2 or len(lows) < 2:
        return None
    h1, h2 = highs[-2]["price"], highs[-1]["price"]
    l1, l2 = lows[-2]["price"], lows[-1]["price"]
    box_top = max(h1, h2)
    box_bottom = min(l1, l2)
    box_height = box_top - box_bottom
    if box_height <= 0:
        return None
    if abs(h1 - h2) <= tolerance * box_height and abs(l1 - l2) <= tolerance * box_height:
        return box_top, box_bottom
    return None


def detect_choch(candles, n=3, close_break=True, atr_values=None):
    events = []
    bos_events = []
    swing_log = []
    trend = None
    highs, lows = [], []
    broken_high_idxs = set()
    broken_low_idxs = set()
    zone_active = False
    zone_high = None
    zone_low = None
    in_range = False
    in_range_cause = ""
    last_flip_idx = -10**9

    def reset_swing_counts():
        broken_high_idxs.clear()
        broken_low_idxs.clear()

    def add_choch_event(event):
        if in_range:
            if RANGE_MODE == "skip":
                return
            event["weak"] = True
            event["weak_reason"] = in_range_cause
        events.append(event)

    def flip_blocked(i, strength):
        # FIX: range کے اندر trend flip ہی نہیں ہوگا
        if in_range and RANGE_MODE == "skip":
            return True
        # whipsaw guard: فوری الٹا flip صرف strong ہو تو
        if (MIN_FLIP_GAP_CANDLES > 0
                and (i - last_flip_idx) < MIN_FLIP_GAP_CANDLES
                and strength != "strong"):
            return True
        return False

    for i in range(2 * n, len(candles)):
        ts, o, h, l, c, v = candles[i]
        center = i - n
        if is_swing_high(candles, center, n):
            highs.append({"idx": center, "ts": candles[center][0], "price": candles[center][2]})
        if is_swing_low(candles, center, n):
            lows.append({"idx": center, "ts": candles[center][0], "price": candles[center][3]})
        up_price = c if close_break else h
        down_price = c if close_break else l
        last_high = find_last_unbroken_high(candles, highs, i, close_break)
        last_low = find_last_unbroken_low(candles, lows, i, close_break)
        weekend = False
        if WEEKEND_RANGE_FILTER:
            wd = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).weekday()
            weekend = wd >= 5
        big_move = False
        if POST_MOVE_SIDEWAYS_FILTER and o != 0:
            single_pct = ((c - o) / o) * 100.0
            base_i = max(0, i - POST_MOVE_LOOKBACK)
            base_close = candles[base_i][4]
            move_pct = ((c - base_close) / base_close) * 100.0 if base_close else 0.0
            if abs(single_pct) >= POST_MOVE_PCT or abs(move_pct) >= POST_MOVE_PCT:
                big_move = True
        breakout = False
        if big_move:
            zone_active = True
            zone_high, zone_low = h, l
            breakout = True
        elif zone_active:
            if c > zone_high or c < zone_low:
                breakout = True
                zone_active = False
        sideways_zone = zone_active and not breakout
        weekend_sideways = weekend and not breakout
        if zone_active and not breakout and not big_move:
            zone_high = max(zone_high, h)
            zone_low = min(zone_low, l)
        box_in_range = False
        if RANGE_FILTER_ENABLED:
            box = detect_range_box(highs, lows, RANGE_TOLERANCE)
            if box:
                box_top, box_bottom = box
                box_in_range = box_bottom <= c <= box_top
        in_range = False
        in_range_cause = ""
        if weekend_sideways:
            in_range = True
            in_range_cause = "weekend (UTC) range"
        elif sideways_zone:
            in_range = True
            in_range_cause = "sideways zone after big move"
        elif box_in_range:
            in_range = True
            in_range_cause = RANGE_WEAK_REASON
        if trend is None:
            if last_high and up_price > last_high["price"]:
                trend = "bullish"
                last_flip_idx = i
                bos_events.append({
                    "type": "BULLISH", "kind": "INITIAL", "idx": i, "ts": ts,
                    "level": last_high["price"], "price": c,
                })
                reset_swing_counts()
            elif last_low and down_price < last_low["price"]:
                trend = "bearish"
                last_flip_idx = i
                bos_events.append({
                    "type": "BEARISH", "kind": "INITIAL", "idx": i, "ts": ts,
                    "level": last_low["price"], "price": c,
                })
                reset_swing_counts()
            continue
        if trend == "bullish":
            for low in lows:
                if low["idx"] in broken_low_idxs or low["idx"] >= i:
                    continue
                if down_price < low["price"] and not was_low_broken_before(
                    candles, low["idx"], low["price"], i, close_break
                ):
                    broken_low_idxs.add(low["idx"])
            protected_low = find_strict_higher_low(candles, highs, lows, i, close_break)
            if protected_low is None and not STRICT_ONLY:
                protected_low = last_low
            if protected_low and down_price < protected_low["price"]:
                if break_quality_ok(candles, i, protected_low["price"], "bearish", atr_values):
                    strength, strength_reason, broken_count = "", "", None
                    if USE_SWING_COUNT_STRENGTH:
                        broken_count = len(broken_low_idxs)
                        if MIN_CONFIRM_SWINGS <= broken_count <= MAX_CONFIRM_SWINGS:
                            strength, strength_reason = "strong", f"{broken_count} swing lows break"
                        elif broken_count > MAX_CONFIRM_SWINGS:
                            strength, strength_reason = "extended", f"{broken_count} swing lows break"
                        else:
                            strength, strength_reason = "normal", f"{broken_count} swing low break"
                    if flip_blocked(i, strength):
                        continue
                    add_choch_event({
                        "type": "BEARISH", "idx": i, "ts": ts,
                        "level": protected_low["price"], "level_ts": protected_low["ts"],
                        "price": c, "mode": protected_low["mode"],
                        "strength": strength, "strength_reason": strength_reason,
                        "broken_count": broken_count,
                    })
                    trend = "bearish"
                    last_flip_idx = i
                    lvl = protected_low["idx"]
                    highs = [x for x in highs if x["idx"] >= lvl]
                    lows = [x for x in lows if x["idx"] >= lvl]
                    reset_swing_counts()
                    continue
            if last_high and up_price > last_high["price"]:
                bos_events.append({
                    "type": "BULLISH", "kind": "BOS", "idx": i, "ts": ts,
                    "level": last_high["price"], "price": c,
                })
                continue
        if trend == "bearish":
            for high in highs:
                if high["idx"] in broken_high_idxs or high["idx"] >= i:
                    continue
                if up_price > high["price"] and not was_high_broken_before(
                    candles, high["idx"], high["price"], i, close_break
                ):
                    broken_high_idxs.add(high["idx"])
            protected_high = find_strict_lower_high(candles, lows, highs, i, close_break)
            if protected_high is None and not STRICT_ONLY:
                protected_high = last_high
            if protected_high and up_price > protected_high["price"]:
                if break_quality_ok(candles, i, protected_high["price"], "bullish", atr_values):
                    strength, strength_reason, broken_count = "", "", None
                    if USE_SWING_COUNT_STRENGTH:
                        broken_count = len(broken_high_idxs)
                        if MIN_CONFIRM_SWINGS <= broken_count <= MAX_CONFIRM_SWINGS:
                            strength, strength_reason = "strong", f"{broken_count} swing highs break"
                        elif broken_count > MAX_CONFIRM_SWINGS:
                            strength, strength_reason = "extended", f"{broken_count} swing highs break"
                        else:
                            strength, strength_reason = "normal", f"{broken_count} swing high break"
                    if flip_blocked(i, strength):
                        continue
                    add_choch_event({
                        "type": "BULLISH", "idx": i, "ts": ts,
                        "level": protected_high["price"], "level_ts": protected_high["ts"],
                        "price": c, "mode": protected_high["mode"],
                        "strength": strength, "strength_reason": strength_reason,
                        "broken_count": broken_count,
                    })
                    trend = "bullish"
                    last_flip_idx = i
                    lvl = protected_high["idx"]
                    highs = [x for x in highs if x["idx"] >= lvl]
                    lows = [x for x in lows if x["idx"] >= lvl]
                    reset_swing_counts()
                    continue
            if last_low and down_price < last_low["price"]:
                bos_events.append({
                    "type": "BEARISH", "kind": "BOS", "idx": i, "ts": ts,
                    "level": last_low["price"], "price": c,
                })
                continue
    return events, bos_events, trend, swing_log


def detect_pump_dump_flags(candles):
    flags = [None] * len(candles)
    lookback = max(1, PUMP_DUMP_LOOKBACK)
    for i in range(len(candles)):
        o, c = candles[i][1], candles[i][4]
        if o == 0 or c == 0:
            continue
        single_pct = ((c - o) / o) * 100.0
        base_close = candles[max(0, i - lookback + 1)][4]
        if base_close == 0:
            continue
        move_pct = ((c - base_close) / base_close) * 100.0
        if move_pct >= PUMP_DUMP_MOVE_PCT or single_pct >= SINGLE_CANDLE_PUMP_DUMP_PCT:
            flags[i] = "PUMP"
        elif move_pct <= -PUMP_DUMP_MOVE_PCT or single_pct <= -SINGLE_CANDLE_PUMP_DUMP_PCT:
            flags[i] = "DUMP"
    return flags


def label_weak_chochs(candles, events, pump_flags):
    by_idx = {}
    for e in events:
        e.setdefault("weak", False)
        e.setdefault("weak_reason", "")
        by_idx.setdefault(e["idx"], []).append(e)
    if not MARK_SAME_CANDLE_WEAK or not pump_flags:
        return
    for i, flag in enumerate(pump_flags):
        if flag is None:
            continue
        for e in by_idx.get(i, []):
            e["weak"] = True
            reason = f"{flag} candle (possible fake)"
            if e.get("weak_reason"):
                e["weak_reason"] += "; " + reason
            else:
                e["weak_reason"] = reason


def market_is_choppy(candles, atr_values) -> bool:
    """Efficiency Ratio + ATR-box: کیا مارکیٹ واقعی range میں ہے؟"""
    n = CHOP_LOOKBACK
    if len(candles) < max(CHOP_MIN_CANDLES, n + 2):
        return False
    closes = [c[4] for c in candles[-(n + 1):]]
    net = abs(closes[-1] - closes[0])
    path = sum(abs(closes[k] - closes[k - 1]) for k in range(1, len(closes)))
    if path <= 0:
        return True
    er = net / path
    if er >= CHOP_ER_MAX:
        return False                      # صاف trend ہے
    seg = candles[-(n + 1):-1]
    box_hi = max(c[2] for c in seg)
    box_lo = min(c[3] for c in seg)
    if candles[-1][4] > box_hi or candles[-1][4] < box_lo:
        return False                      # box سے باہر close = اصلی breakout
    atr_val = atr_values[-1] if atr_values and atr_values[-1] else None
    if atr_val and (box_hi - box_lo) >= CHOP_BOX_ATR_MULT * atr_val:
        return False                      # چوڑا volatile range، تنگ box نہیں
    return True


def analyze_choch(candles: list[list[float]]) -> tuple[list[dict[str, Any]], str | None]:
    atr = calculate_atr(candles, ATR_PERIOD)
    events, _bos, trend, _sw = detect_choch(candles, SWING_STRENGTH, USE_CLOSE_BREAK, atr)
    label_weak_chochs(candles, events, detect_pump_dump_flags(candles))
    if CHOP_FILTER_ENABLED and market_is_choppy(candles, atr):
        cutoff = len(candles) - CHOP_LOOKBACK
        events = [e for e in events if e["idx"] < cutoff]
        trend = "range"
    return events, trend


# ============================ FIXED 100-COIN WHITELIST ============================
WHITELIST = frozenset({
    "ETH", "BNB", "XRP", "SOL", "ADA", "DOGE", "TRX", "LINK", "AVAX", "DOT",
    "LTC", "BCH", "XLM", "ATOM", "ETC", "XMR", "NEAR", "UNI", "APT", "ARB",
    "OP", "FIL", "ICP", "HBAR", "VET", "INJ", "SUI", "SEI", "TIA", "AAVE",
    "MKR", "LDO", "CRV", "SNX", "COMP", "SUSHI", "1INCH", "DYDX", "GMX", "RUNE",
    "ALGO", "EGLD", "FLOW", "XTZ", "EOS", "NEO", "IOTA", "ZEC", "DASH", "KAVA",
    "THETA", "AXS", "SAND", "MANA", "GALA", "ENJ", "CHZ", "IMX", "APE", "BLUR",
    "SHIB", "PEPE", "FLOKI", "BONK", "WIF", "TON", "FET", "RENDER", "GRT", "TAO",
    "ENA", "PENDLE", "JUP", "PYTH", "STX", "ORDI", "WLD", "ONDO", "JTO", "STRK",
    "MINA", "ZIL", "QTUM", "ONT", "BAT", "ZRX", "ANKR", "CELO", "ROSE", "KSM",
    "CFX", "WOO", "LRC", "YFI", "BAL", "CAKE", "HYPE", "AR", "RVN", "TRB",
})


class WhitelistSelector(of.SymbolSelector):
    """صرف دی ہوئی 100 کوئنز؛ top-volume سلیکشن بند۔
    جو کوئن اس دن لیکویڈیٹی/ٹریڈز کی حد پاس نہ کرے وہ اس دن skip۔"""

    async def select(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        tickers, info = await asyncio.gather(
            self._get(session, of.TICKER_URL), self._get(session, of.INFO_URL)
        )
        symbols = [s for s in info.get("symbols", []) if s.get("status") == "TRADING"]
        ticks: dict[str, float] = {}
        for s in symbols:
            for flt in s.get("filters", []):
                if flt.get("filterType") == "PRICE_FILTER":
                    try:
                        ticks[s["symbol"]] = float(flt.get("tickSize", 0.0))
                    except (TypeError, ValueError):
                        pass
        trading = {s["symbol"] for s in symbols}
        trading_bases = {str(s.get("baseAsset", "")) for s in symbols}
        picked: list[dict[str, Any]] = []
        seen: set[str] = set()
        skipped_liquidity: list[str] = []
        for t in tickers:
            sym = str(t.get("symbol", ""))
            if not sym.endswith("USDT") or sym not in trading:
                continue
            base = sym[:-4]
            if base not in WHITELIST:
                continue
            item = self._check(t, trading, trading_bases)
            if item is None:
                skipped_liquidity.append(base)
                continue
            seen.add(base)
            item["tick"] = ticks.get(sym, 0.0)
            picked.append(item)
        not_listed = sorted(WHITELIST - seen - set(skipped_liquidity))
        picked.sort(key=lambda x: x["quote_volume"], reverse=True)
        picked = picked[: int(self.cfg["max_symbols"])]
        log.info(
            "[ENGINE] Whitelist selection: %d of %d coins | liquidity-skipped today: %s | not listed/trading: %s",
            len(picked), len(WHITELIST),
            ", ".join(sorted(skipped_liquidity)) or "none",
            ", ".join(not_listed) or "none",
        )
        return picked


class StateWith1D(of.OrderFlowState):
    """1d (24h) ونڈو 5 منٹ کے buckets کے ساتھ — memory محفوظ رہے۔"""

    def __init__(self, *args: Any, **kw: Any) -> None:
        super().__init__(*args, **kw)
        w = self.windows.get("1d")
        if w is not None:
            self.windows["1d"] = of.RollingWindow(w.window, 300.0, w.whale_mult, w.retail_mult)


# ============================ ENGINE ============================
class ChochOrderflowEngine(of.Engine):
    def __init__(self) -> None:
        super().__init__()
        self.alerter = of.NtfyAlerter(self.cfg)
        self.health.alerter = self.alerter
        self.selector = of.SymbolSelector(self.cfg)
        self.choch: dict[str, dict[str, dict[str, Any]]] = {}
        self._pool: ProcessPoolExecutor | None = None
        self._boot_ms = int(time.time() * 1000)
        self.oi = OIPoller()

    @staticmethod
    def _side_from_type(t: str) -> str:
        return "BUY" if t == "BULLISH" else "SELL"

    @staticmethod
    def _type_from_side(side: str) -> str:
        return "BULLISH" if side == "BUY" else "BEARISH"

    # ===== TREND FILTER + OPPOSITE-SIDE 30m BLOCK =====
    def _trend_allows(self, sym: str, rname: str, side: str, rule: dict[str, Any]) -> tuple[bool, str]:
        # نیا اصول: مخالف سمت کا الرٹ 30 منٹ کے لیے بند
        blk = float(self.cfg.get("opposite_side_block_min", 0))
        if blk > 0:
            opp = "SELL" if side == "BUY" else "BUY"
            if self.book.on_cooldown(f"{sym}|ANY|{opp}", time.time(), blk):
                return False, f"opp_side_{int(blk)}m"
        trend_frame = rule.get("trend_frame")
        if not trend_frame:
            return True, ""
        st = self.choch.get(sym, {}).get(trend_frame)
        if not st:
            return True, "no_data"
        trend = st.get("trend")
        if trend == "range":
            return False, f"{trend_frame}_chop_range"
        if trend is None:
            if BLOCK_ON_UNKNOWN_TREND:
                return False, f"{trend_frame}_unknown"
            return True, "unknown"
        if trend == "bullish" and side == "SELL":
            return False, f"sell_against_{trend_frame}_bullish"
        if trend == "bearish" and side == "BUY":
            return False, f"buy_against_{trend_frame}_bearish"
        return True, ""

    # ===== 1h CHoCH DISPLAY GATE =====
    def _flow_confirms_frame(self, sym: str, fr: str, ev: dict[str, Any]) -> bool:
        """1h CHoCH counts only if 1h order flow exists in the SAME direction."""
        if fr not in MERGE.get("flow_gated_frames", []):
            return True
        st = self.states.get(sym)
        if st is None:
            return False
        leg = str(MERGE.get("flow_gate_leg", "1h"))
        w = st.windows.get(leg)
        if w is None:
            return False
        rule_name = str(MERGE.get("flow_gate_rule", "1h_daily"))
        rule = self.cfg["orderflow_signals"]["rules"].get(rule_name, {})
        thr = float((rule.get("cvd_pct", {}) or {}).get(leg, 2.5))
        min_fill = float(rule.get("min_window_fill", self.cfg["min_window_fill"]))
        if w.fill(time.time()) < min_fill:
            return False
        cvd = w.cvd_pct
        if ev.get("type") == "BULLISH":
            return cvd >= thr
        return cvd <= -thr

    def _format_choch_event(self, ev: dict[str, Any], fr: str, now: float) -> str:
        # ===== [weak] ٹیگ نہیں دکھایا جاتا =====
        tfc = MERGE["choch_timeframes"][fr]
        close_sec = float(ev["ts"]) / 1000.0 + float(tfc["seconds"])
        age_h = max(now - close_sec, 0.0) / 3600.0
        return (
            f"{fr} {ev['type'].title()} CHoCH "
            f"({age_h:.1f}h ago, level {ev['level']:.8g}, close {ev['price']:.8g}, "
            f"{ev.get('strength') or 'n/a'})"
        )

    def _htf_label(self, sym: str, tf_name: str) -> str:
        st = self.choch.get(sym, {}).get(tf_name)
        if not st:
            return "Neutral"
        if st.get("trend") == "range":
            return "Range/Chop"
        ev = st.get("latest")
        if ev:
            return "Bullish" if ev["type"] == "BULLISH" else "Bearish"
        trend = st.get("trend")
        if trend == "bullish":
            return "Bullish"
        if trend == "bearish":
            return "Bearish"
        return "Neutral"

    def _trend_label(self, sym: str, tf_name: str) -> str:
        st = self.choch.get(sym, {}).get(tf_name)
        if not st:
            return "Unknown"
        trend = st.get("trend")
        if trend == "range":
            return "Range/Chop"
        if trend == "bullish":
            return "Bullish"
        if trend == "bearish":
            return "Bearish"
        return "Neutral"

    def _context_line(self, sym: str) -> str:
        parts = []
        for ctf in MERGE["context_frames"]:
            parts.append(f"{ctf.upper()}: {self._htf_label(sym, ctf)}")
        return " | ".join(parts)

    def _active_choch(self, sym: str, side: str, frame: str) -> dict[str, Any] | None:
        want = self._type_from_side(side)
        stt = self.choch.get(sym, {}).get(frame)
        ev = stt.get("active") if stt else None
        if not ev:
            return None
        if ev["type"] != want:
            return None
        return ev

    def _any_choch(self, sym: str, frame: str) -> dict[str, Any] | None:
        stt = self.choch.get(sym, {}).get(frame)
        if not stt:
            return None
        return stt.get("active") or stt.get("latest")

    def _gate_line(self, sym: str, side: str, now: float) -> str:
        hits = []
        for fr in MERGE["gate_frames"]:
            ev = self._active_choch(sym, side, fr)
            if ev:
                if not self._flow_confirms_frame(sym, fr, ev):
                    continue
                hits.append(self._format_choch_event(ev, fr, now))
            else:
                any_ev = self._any_choch(sym, fr)
                if any_ev:
                    if not self._flow_confirms_frame(sym, fr, any_ev):
                        continue
                    hits.append(f"[opposite] {self._format_choch_event(any_ev, fr, now)}")
        return "; ".join(hits) if hits else "none"

    def _signal_context_lines(self, sym: str, tf_name: str, side: str, now: float) -> list[str]:
        if not MERGE.get("use_choch_context", True):
            return []
        lines = []
        st_oi = self.states.get(sym)
        if st_oi:
            lead = self.cfg["orderflow_signals"]["rules"].get(tf_name, {}).get("legs", ["1h"])[0]
            w_oi = st_oi.windows.get(lead)
            if w_oi:
                lines.append(self.oi.line(sym, w_oi.move_pct(st_oi.last_price)))
        rules = self.cfg["orderflow_signals"]["rules"]
        rule = rules.get(tf_name, {})
        trend_frame = rule.get("trend_frame")
        if trend_frame:
            trend = self._trend_label(sym, trend_frame)
            lines.append(f"Trend ({trend_frame.upper()}): {trend}")
        gate = self._gate_line(sym, side, now)
        if gate and gate != "none":
            lines.append(f"CHoCH context: {gate}")
        elif MERGE.get("flow_gated_frames"):
            lines.append("CHoCH context: none (1h needs 1h order flow)")
        ctx = self._context_line(sym)
        if ctx:
            lines.append(f"HTF context: {ctx}")
        return lines

    def _session_extra_lines(self, now: float, sessions: list[str]) -> list[str]:
        if not MERGE.get("use_choch_context", True):
            return []
        active = 0
        for sym in self.states:
            for fr in MERGE["gate_frames"]:
                ev = self.choch.get(sym, {}).get(fr, {}).get("active")
                if ev and self._flow_confirms_frame(sym, fr, ev):
                    active += 1
        return [f"Active CHoCH contexts (flow-confirmed): {active}"]

    async def _choch_fetch(
        self,
        session: aiohttp.ClientSession,
        sym: str,
        tf_name: str,
        st: dict[str, Any],
    ) -> bool:
        tfc = MERGE["choch_timeframes"][tf_name]
        sec_ms = tfc["seconds"] * 1000
        now_ms = int(time.time() * 1000)
        candles = st["candles"]
        if candles:
            start = candles[-1][0] + sec_ms
        else:
            n0 = max(2, int(tfc.get("initial_candles", 1000)))
            start = now_ms - (n0 - 1) * sec_ms
        rows: list[list[Any]] = []
        while True:
            url = (
                f"{of.REST_BASE}/api/v3/klines"
                f"?symbol={sym}&interval={tfc['interval']}"
                f"&startTime={start}&limit=1000"
            )
            data = await self.selector._get(session, url)
            rows.extend(data)
            if len(data) < 1000:
                break
            start = int(data[-1][0]) + sec_ms
        added = False
        for r in rows:
            if int(r[6]) >= now_ms:
                continue
            ts = int(r[0])
            if candles and ts <= candles[-1][0]:
                continue
            candles.append([
                ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5]),
            ])
            added = True
        return added

    async def _run_analysis(self, candles: list[list[float]]) -> tuple[list[dict[str, Any]], str | None]:
        payload = [list(c) for c in candles]
        loop = asyncio.get_running_loop()
        if self._pool is not None:
            try:
                return await loop.run_in_executor(self._pool, analyze_choch, payload)
            except Exception as exc:
                log.warning("[CHOCH] process pool failed (%s); falling back to thread", exc)
                self._pool = None
        return await asyncio.to_thread(analyze_choch, payload)

    async def _choch_update(
        self,
        session: aiohttp.ClientSession,
        sem: asyncio.Semaphore,
        sym: str,
        tf_name: str,
    ) -> None:
        st = self.choch[sym][tf_name]
        if st["busy"]:
            return
        st["busy"] = True
        tfc = MERGE["choch_timeframes"][tf_name]
        sec_ms = tfc["seconds"] * 1000
        delay_ms = MERGE["refresh_delay_sec"] * 1000
        now_ms = int(time.time() * 1000)
        try:
            async with sem:
                prev_last = st["candles"][-1][0] if st["candles"] else None
                try:
                    added = await self._choch_fetch(session, sym, tf_name, st)
                except Exception as exc:
                    log.warning("[%s] CHoCH %s fetch failed: %s", sym, tf_name, exc)
                    st["next_due_ms"] = now_ms + 60_000
                    return
                candles = st["candles"]
                if not candles:
                    st["next_due_ms"] = now_ms + 300_000
                    return
                if prev_last is not None and not added and candles[-1][0] == prev_last:
                    st["next_due_ms"] = max(now_ms + 30_000, candles[-1][0] + sec_ms + delay_ms)
                    return
                if len(candles) < SWING_STRENGTH * 2 + 15:
                    st["next_due_ms"] = now_ms + 300_000
                    return
                t0 = time.perf_counter()
                events, trend = await self._run_analysis(candles)
                took = time.perf_counter() - t0
                if took > 1.0:
                    log.info("[CHOCH][BENCH] %s %s n=%d took=%.2fs", sym, tf_name, len(candles), took)
                latest = events[-1] if events else None
                active = latest
                old_active = st.get("active")
                st["latest"] = latest
                st["trend"] = trend
                st["active"] = active
                if (
                    prev_last is not None
                    and active is not None
                    and (
                        old_active is None
                        or active["ts"] != old_active["ts"]
                        or active["type"] != old_active["type"]
                    )
                ):
                    log.info(
                        "[%s] ACTIVE %s CHoCH %s level=%.8g close=%.8g strength=%s | TREND=%s",
                        sym, active["type"], tf_name, active["level"], active["price"],
                        active.get("strength"), trend,
                    )
                st["next_due_ms"] = max(now_ms + 30_000, candles[-1][0] + sec_ms + delay_ms)
        except Exception as exc:
            log.exception("[%s] CHoCH %s update error: %s", sym, tf_name, exc)
            st["next_due_ms"] = int(time.time() * 1000) + 60_000
        finally:
            st["busy"] = False

    async def _choch_loop(self) -> None:
        self.choch = {
            sym: {
                tf: {
                    "candles": [], "latest": None, "trend": None,
                    "active": None, "busy": False, "next_due_ms": 0,
                }
                for tf in MERGE["choch_timeframes"]
            }
            for sym in self.states
        }
        if MERGE.get("use_process_pool", True) and self._pool is None:
            try:
                self._pool = ProcessPoolExecutor(max_workers=int(MERGE.get("pool_workers", 2)))
                log.info("[CHOCH] CPU analysis moved to process pool (%d workers)", int(MERGE.get("pool_workers", 2)))
            except Exception as exc:
                log.warning("[CHOCH] process pool unavailable (%s); using threads", exc)
                self._pool = None
        if await self._sleep_or_stop(10):
            return
        sem = asyncio.Semaphore(int(MERGE["concurrency"]))
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        loaded_logged = False
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self.stop.is_set():
                try:
                    now_ms = int(time.time() * 1000)
                    jobs = []
                    for sym in self.states:
                        for tf_name in MERGE["choch_timeframes"]:
                            st = self.choch[sym][tf_name]
                            if st["busy"]:
                                continue
                            if now_ms >= st["next_due_ms"]:
                                jobs.append(self._choch_update(session, sem, sym, tf_name))
                    if jobs:
                        await asyncio.gather(*jobs)
                    if not loaded_logged:
                        ready = sum(1 for s in self.choch.values() if all(v["candles"] for v in s.values()))
                        if ready == len(self.choch):
                            loaded_logged = True
                            bull_4h = sum(1 for s in self.choch.values() if s.get("4h", {}).get("trend") == "bullish")
                            bear_4h = sum(1 for s in self.choch.values() if s.get("4h", {}).get("trend") == "bearish")
                            bull_1d = sum(1 for s in self.choch.values() if s.get("1d", {}).get("trend") == "bullish")
                            bear_1d = sum(1 for s in self.choch.values() if s.get("1d", {}).get("trend") == "bearish")
                            n1h = len(next(iter(self.choch.values()))["1h"]["candles"]) if self.choch else 0
                            log.info(
                                "[CHOCH] Loaded %d symbols x %d TFs (no candle pruning, 1h kept=%d) | "
                                "4h trends: %d bull / %d bear | 1d trends: %d bull / %d bear",
                                ready, len(MERGE["choch_timeframes"]), n1h,
                                bull_4h, bear_4h, bull_1d, bear_1d,
                            )
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("[CHOCH] loop error: %s", exc)
                if await self._sleep_or_stop(MERGE["loop_sec"]):
                    break

    async def _oi_task(self) -> None:
        timeout = aiohttp.ClientTimeout(total=20)
        async with aiohttp.ClientSession(timeout=timeout) as s:
            await self.oi.run(s, self.stop)

    async def _ws_loop(self, url: str) -> None:
        tasks = [
            asyncio.create_task(self._choch_loop()),
            asyncio.create_task(self._oi_task()),
        ]
        try:
            await super()._ws_loop(url)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
            if self._pool is not None:
                try:
                    self._pool.shutdown(wait=False, cancel_futures=True)
                except Exception:
                    pass
                self._pool = None


def main() -> None:
    load_dotenv()
    of.setup_logging()
    # ===== universe + 1d window: base engine کو patch (دوسری فائل نہیں بدلتی) =====
    of.SymbolSelector = WhitelistSelector
    of.OrderFlowState = StateWith1D

    of.CONFIG["max_symbols"] = 100
    of.CONFIG["spread_max_bps"] = 40.0
    of.CONFIG["depth_levels"] = 20
    of.CONFIG["depth_update_ms"] = 100
    of.CONFIG["max_alerts_per_day"] = 300
    of.CONFIG["max_alerts_per_symbol_day"] = 8
    of.CONFIG["sessions"]["enabled"] = True
    of.CONFIG["sessions"]["alert_on_change"] = True

    # ===== DUPLICATE SUPPRESSION =====
    of.CONFIG["symbol_cooldown_min"] = 30          # same side, any rule (برقرار)
    of.CONFIG["symbol_dup_sec"] = 60
    of.CONFIG["block_duplicate_legs"] = True
    of.CONFIG["opposite_side_block_min"] = 30      # نیا: مخالف سمت 30 منٹ بند

    # ===== EXCLUSIONS =====
    of.CONFIG["exclude_commodities"] = True
    of.CONFIG["exclude_equities"] = True
    of.CONFIG["exclude_non_ascii"] = True
    of.CONFIG["exclude_extra_bases"] = []

    # ===== SIZE BUCKETS =====
    of.CONFIG["whale_mult"] = 20.0
    of.CONFIG["retail_mult"] = 2.0

    # ===== CLOCK: 12-hour AM/PM =====
    of.CONFIG["alert_tz_offset_hours"] = 5.0
    of.CONFIG["alert_tz_label"] = "PKT"

    # ===== SESSION-AWARE REST =====
    of.CONFIG["rest_schedule"] = {
        "enabled": True,
        "peak_hours_utc": [[7, 8], [12, 16]],
        "slow_work_min": 50,
        "slow_rest_min": 10,
        "off_session_rest": True,
        "notify_off_session": True,
    }

    # ===== SOFT FOOTPRINT =====
    of.CONFIG["footprint_imbalance_ratio"] = 2.5
    of.CONFIG["footprint_min_stacked"] = 1

    ofs = of.CONFIG["orderflow_signals"]
    ofs["enabled"] = True
    ofs["require_footprint"] = False
    ofs["min_volume_usdt"] = 30_000
    ofs["min_trades"] = 50
    ofs["timeframes"]["1d"] = {"seconds": 86400}   # نیا: 24 گھنٹے کی ونڈو

    rules = ofs["rules"]
    # same rule + same side: کم از کم 1-2 گھنٹے (یہاں 2 گھنٹے)
    rules["30m+1h"]["enabled"] = True
    rules["30m+1h"]["cooldown_min"] = 120
    rules["1h_daily"]["enabled"] = True
    rules["1h_daily"]["cooldown_min"] = 120
    r4 = rules.pop("4h")
    r4["cooldown_min"] = 180
    r4["trend_frame"] = "1d"
    rules["4h_daily"] = r4
    rules["4h_weekly"] = {
        "enabled": True, "legs": ["4h"], "trend_frame": "1w",
        "cooldown_min": 180, "min_window_fill": 0.50,
        "cvd_pct": {"4h": 8.0}, "short_cvd_pct": 4.0,
        "obi": 0.05, "obi_source": "4h",
        "edge_trigger": False, "rearm_frac": 0.50, "freshness_min": 0.0,
        "max_move_pct": {}, "max_vwap_dist_pct": 1e9,
    }
    rules["1d_weekly"] = {
        "enabled": True, "legs": ["1d"], "trend_frame": "1w",
        "cooldown_min": 240, "min_window_fill": 0.50,
        "cvd_pct": {"1d": 3.0}, "short_cvd_pct": 2.0,
        "obi": 0.02, "obi_source": "short",
        "edge_trigger": False, "rearm_frac": 0.50, "freshness_min": 0.0,
        "max_move_pct": {}, "max_vwap_dist_pct": 1e9,
    }

    rs = of.CONFIG["rest_schedule"]
    log.info(
        "[MERGE] TREND-FOLLOWING | whitelist %d coins | "
        "rules: 30m+1h(4h) + 1h_daily(1d) + 4h_daily(1d) + 4h_weekly(1w) + 1d_weekly(1w) | "
        "cooldowns: same-rule 120/180/240min, same-side 30min, opposite-side 30min | "
        "CHoCH: no [weak] tag, no pruning, anti-chop ON | 1h CHoCH needs 1h flow | "
        "clock AM/PM | rest: peak=%s slow=%d/%dmin off=%s",
        len(WHITELIST),
        rs["peak_hours_utc"], rs["slow_work_min"], rs["slow_rest_min"], rs["off_session_rest"],
    )
    code = 0
    try:
        code = asyncio.run(ChochOrderflowEngine().run())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")
        raise SystemExit(code)


if __name__ == "__main__":
    main()
