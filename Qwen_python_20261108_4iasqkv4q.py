#!/usr/bin/env python3
"""
orderflow_choch_live.py - v3 LIVE merged CHoCH + Orderflow alerts.
v3 CHANGES:
- CHoCH is now LABEL ONLY (never blocks signals).
- New Order Flow quality filters (OBI alignment, counter-pressure,
  stacked imbalance, delta climax check).
- Timing fixes: next_due_ms 2x->1x, fresh_limit 900->3600,
  max_delay_sec 120->300, cand_max_age_candles 1->2.
- Hidden divergence trend opposition is now informational only.

Implemented logic:
- CHoCH only on CLOSED candles.
- CHoCH volume confirmation (informational, not blocking).
- 4H CHoCH close alert: regular divergence NOT required.
- 1H alert: regular divergence + 1H candle close (CHoCH as label).
- Hidden divergence: trend direction + orderflow direction (informational).
- Orderflow quality filters gate all signals.
- Keeps original orderflow_paper.py unchanged, only imports it.

Run:
    python orderflow_choch_live.py

Required files in same folder:
- orderflow_paper.py
- orderflow_choch_live.py
"""
from __future__ import annotations

import asyncio
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
from dotenv import load_dotenv

import orderflow_paper as of

log = of.log


# ============================ MERGE CONFIG ============================

MERGE: dict[str, Any] = {
    "choch_timeframes": {
        # Gate timeframes (now label-only, never block)
        "1h": {
            "seconds": 3600,
            "interval": "1h",
            "lookback_days": 180,
        },
        "4h": {
            "seconds": 14400,
            "interval": "4h",
            "lookback_days": 365,
        },
        # Context-only higher timeframes
        "1d": {
            "seconds": 86400,
            "interval": "1d",
            "lookback_days": 730,
        },
        "1w": {
            "seconds": 604800,
            "interval": "1w",
            "lookback_days": 1825,
        },
    },

    # Gate frames for CHoCH-based divergence logic (now label-only)
    "gate_frames": ["1h", "4h"],

    # Context shown in alert body
    "context_frames": ["1d", "1w"],

    # any = one of gate frames enough, all = all gate frames required
    "gate_mode": "any",

    # v3: CHoCH is LABEL ONLY. Weak/volume-fail never block.
    "choch_as_gate": False,
    "allow_weak_choch": True,

    # Live CHoCH refresh behavior
    "refresh_delay_sec": 5,
    "loop_sec": 5,
    "concurrency": 3,

    # False = only divergence/CHoCH alerts; old CVD/OBI flow signals off
    "flow_signals": False,

    # Alert switches
    "alerts": {
        # 4H CHoCH close alert, no regular divergence required
        "4h_choc_close": True,
        # 1H regular divergence on 1H candle close (CHoCH as label)
        "1h_choc_regular_div_close": True,
        # Hidden divergence alerts based only on trend direction + orderflow direction
        "hidden_div_trend": True,
    },

    # CHoCH candle volume confirmation (informational only in v3)
    "choch_volume": {
        "enabled": True,
        "average_period": 20,
        "mode": "any_change",
        "min_ratio": 1.15,
        "any_change_pct": 20.0,
    },

    # Orderflow trend-direction filter
    "orderflow_trend": {
        "enabled": True,
        "min_cvd_pct": 0.10,
        "same_sign": True,
        "oppose_cvd_pct": 3.0,
        "require_obi": False,
        "min_obi": 0.0,
        "frames_4h_choc": ["4h", "1h"],
        "frames_1h_regular": ["1h", "15m"],
        "frames_hidden": ["4h", "1h"],
    },

    # Hidden divergence trend source
    "hidden_trend_tf": "4h",

    # 1H regular divergence close-trigger settings
    "hourly_regular": {
        # v3: 120 -> 300 (more tolerant of delays)
        "max_delay_sec": 300,
        "require_color": True,
        "require_delta": True,
        # v3: 1 -> 2 (allow swing B to be 2 candles behind)
        "cand_max_age_candles": 2,
    },

    # ===== v3 NEW: Order Flow Quality Filters =====
    "orderflow_filters": {
        "enabled": True,
        # BUY requires OBI > 0, SELL requires OBI < 0
        "obi_alignment": True,
        # Counter-pressure must be below this fraction
        "max_counter_pressure": 0.25,
        # Minimum stacked imbalance count
        "min_stacked_imbalance": 1,
        # Delta climax: if current delta > mult * avg, block
        "delta_climax_mult": 3.0,
        # Number of previous candles for average delta
        "delta_avg_lookback": 10,
    },
}


# ============================ CHoCH CONFIG ============================

SWING_STRENGTH = 3
USE_CLOSE_BREAK = True
STRICT_ONLY = False
USE_SWING_COUNT_STRENGTH = True
MIN_CONFIRM_SWINGS = 2
MAX_CONFIRM_SWINGS = 3
RANGE_FILTER_ENABLED = True
RANGE_TOLERANCE = 0.25
RANGE_MODE = "weak"
RANGE_WEAK_REASON = "range/sideways box"
WEEKEND_RANGE_FILTER = True
POST_MOVE_SIDEWAYS_FILTER = True
POST_MOVE_PCT = 2.0
POST_MOVE_LOOKBACK = 3
REQUIRE_DISPLACEMENT = False
ATR_PERIOD = 14
MIN_CLOSE_BEYOND_ATR = 0.10
MIN_BODY_ATR = 0.45
MIN_BODY_RATIO = 0.55
MIN_BREAK_DISTANCE_PCT = 0.15
MIN_BODY_PCT = 0.25
PUMP_DUMP_LOOKBACK = 6
PUMP_DUMP_MOVE_PCT = 4.0
SINGLE_CANDLE_PUMP_DUMP_PCT = 3.0
MARK_SAME_CANDLE_WEAK = True


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

    for i in range(2 * n, len(candles)):
        ts, o, h, l, c, v = candles[i]
        center = i - n

        if is_swing_high(candles, center, n):
            highs.append({
                "idx": center,
                "ts": candles[center][0],
                "price": candles[center][2],
            })
        if is_swing_low(candles, center, n):
            lows.append({
                "idx": center,
                "ts": candles[center][0],
                "price": candles[center][3],
            })

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
                bos_events.append({
                    "type": "BULLISH",
                    "kind": "INITIAL",
                    "idx": i,
                    "ts": ts,
                    "level": last_high["price"],
                    "price": c,
                })
                reset_swing_counts()
            elif last_low and down_price < last_low["price"]:
                trend = "bearish"
                bos_events.append({
                    "type": "BEARISH",
                    "kind": "INITIAL",
                    "idx": i,
                    "ts": ts,
                    "level": last_low["price"],
                    "price": c,
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

                    add_choch_event({
                        "type": "BEARISH",
                        "idx": i,
                        "ts": ts,
                        "level": protected_low["price"],
                        "level_ts": protected_low["ts"],
                        "price": c,
                        "mode": protected_low["mode"],
                        "strength": strength,
                        "strength_reason": strength_reason,
                        "broken_count": broken_count,
                    })
                    trend = "bearish"
                    lvl = protected_low["idx"]
                    highs = [x for x in highs if x["idx"] >= lvl]
                    lows = [x for x in lows if x["idx"] >= lvl]
                    reset_swing_counts()
                    continue

            if last_high and up_price > last_high["price"]:
                bos_events.append({
                    "type": "BULLISH",
                    "kind": "BOS",
                    "idx": i,
                    "ts": ts,
                    "level": last_high["price"],
                    "price": c,
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

                    add_choch_event({
                        "type": "BULLISH",
                        "idx": i,
                        "ts": ts,
                        "level": protected_high["price"],
                        "level_ts": protected_high["ts"],
                        "price": c,
                        "mode": protected_high["mode"],
                        "strength": strength,
                        "strength_reason": strength_reason,
                        "broken_count": broken_count,
                    })
                    trend = "bullish"
                    lvl = protected_high["idx"]
                    highs = [x for x in highs if x["idx"] >= lvl]
                    lows = [x for x in lows if x["idx"] >= lvl]
                    reset_swing_counts()
                    continue

            if last_low and down_price < last_low["price"]:
                bos_events.append({
                    "type": "BEARISH",
                    "kind": "BOS",
                    "idx": i,
                    "ts": ts,
                    "level": last_low["price"],
                    "price": c,
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


def annotate_choch_volume(candles, events):
    cfg = MERGE["choch_volume"]
    if not cfg.get("enabled", True):
        for e in events:
            e.setdefault("volume_ok", True)
        return

    period = max(2, int(cfg.get("average_period", 20)))
    mode = str(cfg.get("mode", "any_change")).lower()
    min_ratio = float(cfg.get("min_ratio", 1.15))
    any_change_pct = float(cfg.get("any_change_pct", 20.0))

    for e in events:
        i = e.get("idx")
        if i is None or i >= len(candles):
            e["volume_ok"] = True
            continue

        v = float(candles[i][5])
        start = max(0, i - period)
        prev = [float(candles[k][5]) for k in range(start, i)]
        avg = sum(prev) / len(prev) if prev else v
        ratio = v / avg if avg > 0 else 1.0

        e["volume"] = v
        e["avg_volume_prev"] = avg
        e["volume_ratio"] = ratio

        if mode == "off":
            e["volume_ok"] = True
            continue

        if mode == "high":
            ok = ratio >= min_ratio
            reason = f"volume ratio {ratio:.2f} below required {min_ratio:.2f}"
        else:
            change_pct = abs(ratio - 1.0) * 100.0
            ok = change_pct >= any_change_pct
            reason = f"volume ratio {ratio:.2f} too close to average (change {change_pct:.1f}% < {any_change_pct:.1f}%)"

        e["volume_ok"] = ok
        if not ok:
            e["weak"] = True
            if e.get("weak_reason"):
                e["weak_reason"] += "; " + reason
            else:
                e["weak_reason"] = reason


def analyze_choch(candles: list[list[float]]) -> tuple[list[dict[str, Any]], str | None]:
    atr = calculate_atr(candles, ATR_PERIOD)
    events, _bos, trend, _sw = detect_choch(candles, SWING_STRENGTH, USE_CLOSE_BREAK, atr)
    label_weak_chochs(candles, events, detect_pump_dump_flags(candles))
    annotate_choch_volume(candles, events)
    return events, trend


# ============================ LIVE ENGINE ============================

class ChochOrderflowEngine(of.Engine):

    def __init__(self) -> None:
        super().__init__()
        self.alerter = of.NtfyAlerter(self.cfg)
        self.health.alerter = self.alerter
        self.selector = of.SymbolSelector(self.cfg)
        self.choch: dict[str, dict[str, dict[str, Any]]] = {}
        self.choch_alerted: set[str] = set()
        self._boot_ms = int(time.time() * 1000)
        self._last_hourly_close_id: int | None = None

    # ---------- small helpers ----------

    @staticmethod
    def _side_from_type(t: str) -> str:
        return "BUY" if t == "BULLISH" else "SELL"

    @staticmethod
    def _type_from_side(side: str) -> str:
        return "BULLISH" if side == "BUY" else "BEARISH"

    def _tf_seconds(self, fr: str) -> int:
        return int(MERGE["choch_timeframes"][fr]["seconds"])

    def _event_close_sec(self, ev: dict[str, Any], fr: str) -> float:
        return float(ev["ts"]) / 1000.0 + float(self._tf_seconds(fr))

    def _format_choch_event(self, ev: dict[str, Any], fr: str, now: float) -> str:
        tfc = MERGE["choch_timeframes"][fr]
        close_sec = float(ev["ts"]) / 1000.0 + float(tfc["seconds"])
        age_h = max(now - close_sec, 0.0) / 3600.0
        vol_ratio = float(ev.get("volume_ratio", 0.0))
        weak_tag = " [weak]" if ev.get("weak") else ""
        vol_tag = "" if ev.get("volume_ok", True) else " [vol-fail]"
        return (
            f"{fr} {ev['type'].title()} CHoCH{weak_tag}{vol_tag} "
            f"({age_h:.1f}h ago, level {ev['level']:.8g}, close {ev['price']:.8g}, "
            f"vol x{vol_ratio:.2f}, {ev.get('strength') or 'n/a'})"
        )

    def _htf_label(self, sym: str, tf_name: str) -> str:
        st = self.choch.get(sym, {}).get(tf_name)
        if not st:
            return "Neutral"
        ev = st.get("latest")
        if ev:
            label = "Bullish" if ev["type"] == "BULLISH" else "Bearish"
            if ev.get("weak"):
                label += " (weak)"
            if not ev.get("volume_ok", True):
                label += " (vol-fail)"
            return label
        trend = st.get("trend")
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
        """v3: Returns active CHoCH regardless of weak/volume status.
        CHoCH is now label-only, never blocks."""
        want = self._type_from_side(side)
        stt = self.choch.get(sym, {}).get(frame)
        ev = stt.get("active") if stt else None
        if not ev:
            return None
        if ev["type"] != want:
            return None
        # v3: Do NOT filter by weak or volume_ok. Return as-is for label.
        return ev

    def _any_choch(self, sym: str, frame: str) -> dict[str, Any] | None:
        """Return latest CHoCH of any type for label display."""
        stt = self.choch.get(sym, {}).get(frame)
        if not stt:
            return None
        return stt.get("active") or stt.get("latest")

    def _gate_line(self, sym: str, side: str, now: float) -> str:
        """v3: Shows CHoCH info as context label, never blocks."""
        hits = []
        for fr in MERGE["gate_frames"]:
            ev = self._active_choch(sym, side, fr)
            if ev:
                hits.append(self._format_choch_event(ev, fr, now))
            else:
                # Show opposite or any CHoCH as context
                any_ev = self._any_choch(sym, fr)
                if any_ev:
                    hits.append(
                        f"[opposite] {self._format_choch_event(any_ev, fr, now)}"
                    )
        return "; ".join(hits) if hits else "none"

    def _flow_snapshot(self, st: Any, frames: list[str]) -> str:
        if st is None:
            return "n/a"
        parts = []
        for fr in frames:
            w = st.frame_window(fr)
            if w is None:
                continue
            obi = w.avg_obi
            obi_s = f"{obi:+.3f}" if obi is not None else "n/a"
            parts.append(
                f"{fr}: CVD% {w.cvd_pct:+.2f}, OBI {obi_s}, "
                f"Vol ${w.window_volume:,.0f}, Trades {w.trade_count}"
            )
        return " | ".join(parts) if parts else "n/a"

    def _flow_trend_side(self, st: Any, frames: list[str]) -> str | None:
        if st is None:
            return None
        cfg = MERGE["orderflow_trend"]
        min_cvd = float(cfg.get("min_cvd_pct", 0.1))
        score = 0
        for fr in frames:
            w = st.frame_window(fr)
            if w is None:
                continue
            cvd = w.cvd_pct
            if cvd > min_cvd:
                score += 1
            elif cvd < -min_cvd:
                score -= 1
        if score > 0:
            return "BUY"
        if score < 0:
            return "SELL"
        return None

    def _structure_side(self, sym: str, tf_name: str) -> str | None:
        """Trend direction from CHoCH structure (informational)."""
        st = self.choch.get(sym, {}).get(tf_name)
        if st:
            for key in ("latest", "active"):
                ev = st.get(key)
                if ev:
                    # v3: Return direction regardless of weak/volume status
                    return self._side_from_type(ev["type"])
            trend = st.get("trend")
            if trend == "bullish":
                return "BUY"
            if trend == "bearish":
                return "SELL"
        flow_st = self.states.get(sym)
        return self._flow_trend_side(flow_st, MERGE["orderflow_trend"]["frames_hidden"])

    def _orderflow_aligns(self, st: Any, side: str, frames: list[str]) -> tuple[bool, str]:
        cfg = MERGE["orderflow_trend"]
        if not cfg.get("enabled", True):
            return True, ""
        if st is None:
            return False, "no_state"

        sign = 1.0 if side == "BUY" else -1.0
        min_cvd = float(cfg.get("min_cvd_pct", 0.1))
        min_obi = float(cfg.get("min_obi", 0.0))
        require_obi = bool(cfg.get("require_obi", False))
        same_sign = bool(cfg.get("same_sign", True))
        oppose = float(cfg.get("oppose_cvd_pct", 3.0))

        for fr in frames:
            w = st.frame_window(fr)
            if w is None:
                continue
            cvd = w.cvd_pct
            obi = w.avg_obi
            if same_sign:
                if sign * cvd < min_cvd:
                    return False, f"{fr}_cvd_oppose_or_flat"
                if require_obi:
                    if obi is None:
                        return False, f"{fr}_obi_missing"
                    if sign * obi < min_obi:
                        return False, f"{fr}_obi_oppose_or_flat"
            else:
                if side == "BUY" and cvd <= -oppose:
                    return False, f"{fr}_cvd_oppose"
                if side == "SELL" and cvd >= oppose:
                    return False, f"{fr}_cvd_oppose"

        return True, ""

    # ---------- v3 NEW: Order Flow Quality Check ----------

    def _orderflow_quality_check(
        self, st: Any, side: str, frames: list[str],
        closed: dict[str, Any] | None = None,
    ) -> tuple[bool, str]:
        """
        v3 quality filters applied BEFORE sending any alert:
        1. OBI Alignment: BUY needs positive OBI, SELL needs negative OBI
        2. Counter Pressure: opposing footprint volume < 25%
        3. Stacked Imbalance: at least x1
        4. Delta Climax: current delta not > 3x average
        """
        filters = MERGE.get("orderflow_filters", {})
        if not filters.get("enabled", True):
            return True, ""
        if st is None:
            return False, "no_state"

        sign = 1.0 if side == "BUY" else -1.0

        # --- 1. OBI Alignment ---
        if filters.get("obi_alignment", True):
            obi_ok = False
            for fr in frames:
                w = st.frame_window(fr)
                if w is None:
                    continue
                obi = w.avg_obi
                if obi is not None and sign * obi > 0:
                    obi_ok = True
                    break
            if not obi_ok:
                return False, "obi_divergence"

        # --- 2. Counter Pressure (from footprint of last candle) ---
        max_cp = float(filters.get("max_counter_pressure", 0.25))
        for _name, group in st.candles.items():
            for _sec, series in group.items():
                if not series.candles:
                    continue
                c = series.candles[-1]
                if c is None or not c.fp or c.partial:
                    continue
                total = sum(a + b for a, b in c.fp.values())
                if total <= 0:
                    continue
                if side == "BUY":
                    counter = sum(v[0] for v in c.fp.values())  # sell vol
                else:
                    counter = sum(v[1] for v in c.fp.values())  # buy vol
                cp = counter / total
                if cp > max_cp:
                    return False, f"counter_pressure_{cp:.0%}"

        # --- 3. Stacked Imbalance >= min ---
        min_stacked = int(filters.get("min_stacked_imbalance", 1))
        stacked_ok = False
        for _name, group in st.candles.items():
            for _sec, series in group.items():
                fpf = series.flow_footprint_check(
                    side, st.last_price, self.cfg["footprint"]
                )
                if fpf.get("available") and fpf.get("stacked", 0) >= min_stacked:
                    stacked_ok = True
                    break
            if stacked_ok:
                break
        if not stacked_ok:
            return False, "no_stacked_imbalance"

        # --- 4. Delta Climax Check ---
        if closed is not None:
            climax_mult = float(filters.get("delta_climax_mult", 3.0))
            lookback = int(filters.get("delta_avg_lookback", 10))
            t_delta = abs(
                float(closed.get("buy", 0)) - float(closed.get("sell", 0))
            )
            deltas: list[float] = []
            for _name, group in st.candles.items():
                for _sec, series in group.items():
                    cs = list(series.candles)
                    recent = cs[-lookback - 1:-1] if len(cs) > lookback else cs[:-1]
                    for rc in recent:
                        deltas.append(abs(rc.buy - rc.sell))
            if deltas:
                avg_delta = sum(deltas) / len(deltas)
                if avg_delta > 0 and t_delta > climax_mult * avg_delta:
                    return False, "delta_climax"

        return True, ""

    # ---------- alert helpers ----------

    def _commit_alert(
        self,
        key: str,
        sym: str,
        tf: str,
        side: str,
        now: float,
        label: str,
        title: str,
        body: str,
        tags: str,
    ) -> None:
        if self.book.cap_reached():
            log.info(
                "[%s] %s (daily cap %d reached, log only) | %s",
                sym, title, self.cfg["max_alerts_per_day"],
                body.replace("\n", " | "),
            )
            self.book.last_alert[key] = now
            return

        self.book.register(key, sym, tf, side, now, label=label)
        self.health.note_signal()
        log.info("[%s] %s | %s", sym, title, body.replace("\n", " | "))
        self._spawn(self.alerter.send(title, body, "high", tags))

    # ---------- divergence body ----------

    def _div_body(
        self,
        sym: str,
        tf: str,
        cl: str,
        d: dict[str, Any],
        st: Any,
        now: float,
        closed: dict[str, Any],
        extra_lines: list[str],
    ) -> str:
        side, kind = d["side"], d["kind"]
        fp = d.get("fp") or {
            "available": False, "absorb": False, "stacked": 0,
            "frac": 0.0, "confirms": 0, "row": 0.0, "span": 0,
        }
        w = st.windows[tf]
        trig_label = of.candle_label(
            int(self.cfg["timeframes"][tf]["confirm_candle_seconds"])
        )
        extreme = "low" if side == "BUY" else "high"

        if fp.get("available"):
            heavy = "sell" if side == "BUY" else "buy"
            imb = "buy" if side == "BUY" else "sell"
            fp_line = (
                f"Footprint (row {fp.get('row', 0):.8g}): absorption "
                f"{'YES' if fp.get('absorb') else 'no'} "
                f"({heavy} vol at {extreme} rows {fp.get('frac', 0) * 100:.1f}% of candle), "
                f"stacked {imb} imbalances x{fp.get('stacked', 0)}"
            )
        else:
            fp_line = "Footprint: n/a"

        t_delta = float(closed.get("buy", 0.0)) - float(closed.get("sell", 0.0))

        lines = [
            f"Symbol: {sym}",
            f"Timeframe: {tf} (CVD swing divergence on {cl} candles)",
            f"Trigger: {trig_label} candle CLOSED "
            f"({'green' if closed.get('close', 0) > closed.get('open', 0) else 'red'}, "
            f"delta {t_delta:+,.0f} USDT)",
            f"Side: {side}",
            f"Signal: {of.DIV_TEXT[(side, kind)]}",
            f"Price: {st.last_price:.8g}",
            f"Swing price A->B: {d['p1']:.8g} -> {d['p2']:.8g} "
            f"({d.get('gap', 0)} candles apart"
            f"{', B forming' if d.get('forming') else ''})",
            f"Move since swing: {d.get('since', 0):+.2f}% "
            f"(swing B {max(now - float(d.get('ts', 0)) / 1000.0, 0.0) / 60:.0f} min ago)",
            f"CVD swing value (USDT): {d['v1']:+,.0f} -> {d['v2']:+,.0f}",
            fp_line,
            f"CVD% ({tf}): {w.cvd_pct:+.1f}",
            f"Regime 4h CVD%: {st.windows['4h'].cvd_pct:+.1f}",
            f"Spread bps: {st.spread_bps:.2f}",
            f"Volume USDT ({tf}): {w.window_volume:,.0f}",
            f"Trades ({tf}): {w.trade_count}",
        ]
        lines.extend(extra_lines)
        return "\n".join(lines)

    def _send_div_alert(
        self,
        sym: str,
        tf: str,
        cl: str,
        d: dict[str, Any],
        st: Any,
        now: float,
        key: str,
        closed: dict[str, Any],
        extra_lines: list[str],
        label: str,
    ) -> None:
        trig_label = of.candle_label(
            int(self.cfg["timeframes"][tf]["confirm_candle_seconds"])
        )
        title = f"{sym} {d['side']} {d['kind']} Divergence [{tf}] {trig_label} close"
        body = self._div_body(sym, tf, cl, d, st, now, closed, extra_lines)
        tags = "green_circle" if d["side"] == "BUY" else "red_circle"
        self._commit_alert(key, sym, tf, d["side"], now, label, title, body, tags)

    # ---------- 4H CHoCH close alert ----------

    def _send_4h_choch_alert(
        self, sym: str, tf_name: str, ev: dict[str, Any], now: float
    ) -> None:
        side = self._side_from_type(ev["type"])
        st = self.states.get(sym)
        frames = MERGE["orderflow_trend"]["frames_4h_choc"]

        ok, reason = self._orderflow_aligns(st, side, frames)
        if not ok:
            return

        # v3 NEW: quality filter
        qok, qreason = self._orderflow_quality_check(st, side, frames)
        if not qok:
            log.info(
                "[%s] 4H CHoCH %s blocked by quality filter: %s",
                sym, side, qreason,
            )
            return

        title = f"{sym} 4H CHoCH {side}"
        body = "\n".join([
            f"Symbol: {sym}",
            f"Alert type: 4H CHoCH CLOSE",
            f"Side: {side}",
            f"CHoCH (label): {self._format_choch_event(ev, tf_name, now)}",
            f"CHoCH level: {ev['level']:.8g}",
            f"CHoCH close price: {ev['price']:.8g}",
            f"Volume ratio: {float(ev.get('volume_ratio', 0)):.2f}x average",
            f"Average prev volume: {float(ev.get('avg_volume_prev', 0)):,.0f}",
            f"CHoCH candle volume: {float(ev.get('volume', 0)):,.0f}",
            f"Strength: {ev.get('strength') or 'n/a'} ({ev.get('strength_reason') or 'n/a'})",
            f"Mode: {ev.get('mode') or 'n/a'}",
            f"Broken count: {ev.get('broken_count') if ev.get('broken_count') is not None else 'n/a'}",
            f"Orderflow trend check: {self._flow_snapshot(st, frames)}",
            f"Higher timeframe context: {self._context_line(sym)}",
            f"Price: {st.last_price:.8g}" if st else "Price: n/a",
            f"Spread bps: {st.spread_bps:.2f}" if st else "Spread bps: n/a",
        ])

        key = f"{sym}|{tf_name}|CHoCH|{ev['type']}|{ev['ts']}"
        label = f"{tf_name} CHoCH {side}"
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._commit_alert(key, sym, tf_name, side, now, label, title, body, tags)

    def _dispatch_new_choch_alerts(
        self, sym: str, tf_name: str,
        events: list[dict[str, Any]], now: float,
    ) -> None:
        if tf_name != "4h":
            return
        if not MERGE["alerts"].get("4h_choc_close", True):
            return

        tfc = MERGE["choch_timeframes"][tf_name]
        tf_sec = float(tfc["seconds"])
        boot_sec = self._boot_ms / 1000.0

        # v3: fresh limit 900 -> 3600 (1 hour grace)
        fresh_limit_sec = now - (tf_sec + 3600.0)

        for ev in reversed(events):
            close_sec = float(ev["ts"]) / 1000.0 + tf_sec

            if close_sec <= boot_sec:
                break

            key = f"{sym}|{tf_name}|CHoCH|{ev['type']}|{ev['ts']}"
            if key in self.choch_alerted:
                continue

            if close_sec > now:
                continue

            if close_sec < fresh_limit_sec:
                self.choch_alerted.add(key)
                continue

            # v3: weak and volume-fail do NOT block anymore.
            # They are just labels in the alert body.

            side = self._side_from_type(ev["type"])
            st = self.states.get(sym)

            ok, reason = self._orderflow_aligns(
                st, side, MERGE["orderflow_trend"]["frames_4h_choc"]
            )
            if not ok:
                if now - close_sec > 3600.0:
                    self.choch_alerted.add(key)
                continue

            self.choch_alerted.add(key)
            self._send_4h_choch_alert(sym, tf_name, ev, now)

    # ---------- 1H regular divergence on 1H close ----------

    async def _process_hourly_close(self, close_id: int, now: float) -> None:
        if not MERGE["alerts"].get("1h_choc_regular_div_close", True):
            return

        tf_name = "1h"
        tf = self.cfg["timeframes"][tf_name]
        sec = 3600
        hcfg = MERGE["hourly_regular"]
        max_age = int(hcfg.get("cand_max_age_candles", 2))
        require_color = bool(hcfg.get("require_color", True))
        require_delta = bool(hcfg.get("require_delta", True))
        frames = MERGE["orderflow_trend"]["frames_1h_regular"]

        for sym, st in list(self.states.items()):
            group = st.candles.get(tf_name, {})
            series = group.get(sec)
            if series is None:
                continue

            closed_c = None
            prev_c = None
            for c in reversed(series.candles):
                if c.id == close_id and not c.partial:
                    closed_c = c
                elif c.id == close_id - 1 and not c.partial:
                    prev_c = c
                if closed_c is not None and prev_c is not None:
                    break

            if closed_c is None:
                continue

            closed_dict = {
                "id": closed_c.id,
                "open": closed_c.open,
                "high": closed_c.high,
                "low": closed_c.low,
                "close": closed_c.close,
                "buy": closed_c.buy,
                "sell": closed_c.sell,
                "partial": closed_c.partial,
            }
            prev_dict = None
            if prev_c is not None:
                prev_dict = {
                    "id": prev_c.id,
                    "open": prev_c.open,
                    "high": prev_c.high,
                    "low": prev_c.low,
                    "close": prev_c.close,
                    "buy": prev_c.buy,
                    "sell": prev_c.sell,
                    "partial": prev_c.partial,
                }

            w = st.windows[tf_name]
            if not (
                w.window_volume > tf["min_window_volume_usdt"]
                and w.trade_count >= tf["min_trade_count"]
            ):
                self._count(f"{tf_name}/1h-div:volume")
                continue

            for d in series.find_divergences():
                if d["kind"] != "regular":
                    continue
                if close_id - int(d["cand"].id) > max_age:
                    continue

                side = d["side"]
                tag = f"{tf_name}/1h-div"

                if require_color:
                    if side == "BUY" and not closed_c.close > closed_c.open:
                        self._count(f"{tag}:hour_color")
                        continue
                    if side == "SELL" and not closed_c.close < closed_c.open:
                        self._count(f"{tag}:hour_color")
                        continue

                if require_delta:
                    if side == "BUY" and not closed_c.buy > closed_c.sell:
                        self._count(f"{tag}:hour_delta")
                        continue
                    if side == "SELL" and not closed_c.sell > closed_c.buy:
                        self._count(f"{tag}:hour_delta")
                        continue

                # v3: CHoCH is LABEL ONLY, never blocks
                active = self._active_choch(sym, side, "1h")
                choch_label = (
                    self._format_choch_event(active, "1h", now)
                    if active else "No active 1H CHoCH"
                )

                ok, reason = self._orderflow_aligns(st, side, frames)
                if not ok:
                    self._count(f"{tag}:{reason}")
                    continue

                # v3 NEW: quality filter
                qok, qreason = self._orderflow_quality_check(
                    st, side, frames, closed_dict
                )
                if not qok:
                    self._count(f"{tag}:quality_{qreason}")
                    continue

                gate_reason = super()._div_gate(d, series, st, w, tf, now)
                if gate_reason:
                    self._count(f"{tag}:{gate_reason}")
                    continue

                treason = self._trigger_check(side, closed_dict, prev_dict)
                if treason:
                    self._count(f"{tag}:{treason}")
                    continue

                key = f"{sym}|{tf_name}|1h|{side}|regular-div-hour-close"
                if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                    self._count(f"{tag}:cooldown")
                    continue

                series.mark(d)
                self._count(f"{tag}:signal")

                extra = [
                    f"1H CHoCH (label): {choch_label}",
                    f"Gate line (context): {self._gate_line(sym, side, now)}",
                    f"Orderflow trend check: {self._flow_snapshot(st, frames)}",
                    f"Higher timeframe context: {self._context_line(sym)}",
                    f"Alert rule: 1H regular divergence + 1H candle close + OF quality",
                ]

                self._send_div_alert(
                    sym=sym,
                    tf=tf_name,
                    cl="1h",
                    d=d,
                    st=st,
                    now=now,
                    key=key,
                    closed=closed_dict,
                    extra_lines=extra,
                    label=f"{tf_name} 1h {side} regular-div + CHoCH-label",
                )

    async def _hourly_loop(self) -> None:
        if await self._sleep_or_stop(5):
            return

        max_delay = float(MERGE["hourly_regular"].get("max_delay_sec", 300))

        while not self.stop.is_set():
            try:
                now = time.time()
                close_id = int(now // 3600) - 1

                if self._last_hourly_close_id is None:
                    if now - (close_id + 1) * 3600.0 <= max_delay:
                        await self._process_hourly_close(close_id, now)
                    self._last_hourly_close_id = close_id
                elif close_id != self._last_hourly_close_id:
                    await self._process_hourly_close(close_id, now)
                    self._last_hourly_close_id = close_id
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.exception("[HOURLY] loop error: %s", exc)

            if await self._sleep_or_stop(1):
                break

    # ---------- modified divergence check for hidden only ----------

    def _check_divergence(
        self, sym: str, name: str, tf: dict[str, Any],
        st: of.OrderFlowState, now: float,
    ) -> None:
        """
        v3: Hidden divergence alerts.
        - regular divergence is NOT alerted here.
        - hidden divergence: trend opposition is now informational only.
        - Order flow quality filters applied.
        """
        tr = st.trig[name]
        closed = tr.poll(now)
        if closed is None:
            return

        trig_sec = int(tf["confirm_candle_seconds"])
        if now - (closed["id"] + 1) * trig_sec > self.cfg["divergence"]["trigger_max_delay_sec"]:
            self._count(f"{name}/div:trigger_stale")
            return

        w = st.windows[name]
        if not (
            w.window_volume > tf["min_window_volume_usdt"]
            and w.trade_count >= tf["min_trade_count"]
        ):
            self._count(f"{name}/div:volume")
            return

        for sec, series in st.candles[name].items():
            cl = of.candle_label(sec)

            for d in series.find_divergences():
                tag = f"{name}/{cl}-div"

                if d["kind"] == "regular":
                    if name == "1h":
                        self._count(f"{tag}:regular_wait_1h_close")
                    else:
                        self._count(f"{tag}:regular_disabled_in_trigger_loop")
                    continue

                if d["kind"] != "hidden":
                    continue

                if not MERGE["alerts"].get("hidden_div_trend", True):
                    self._count(f"{tag}:hidden_disabled")
                    continue

                trend_tf = MERGE["hidden_trend_tf"]
                trend_side = self._structure_side(sym, trend_tf)

                # v3: trend opposition is now informational, not blocking
                trend_note = (
                    f"Aligned ({trend_side})"
                    if trend_side == d["side"]
                    else f"OPPOSED (trend={trend_side}, signal={d['side']})"
                )

                frames = MERGE["orderflow_trend"]["frames_hidden"]
                ok, reason = self._orderflow_aligns(st, d["side"], frames)
                if not ok:
                    self._count(f"{tag}:{reason}")
                    continue

                # v3 NEW: quality filter
                qok, qreason = self._orderflow_quality_check(
                    st, d["side"], frames, closed
                )
                if not qok:
                    self._count(f"{tag}:quality_{qreason}")
                    continue

                gate_reason = super()._div_gate(d, series, st, w, tf, now)
                if gate_reason:
                    self._count(f"{tag}:{gate_reason}")
                    continue

                treason = self._trigger_check(d["side"], closed, tr.prev_closed)
                if treason:
                    self._count(f"{tag}:{treason}")
                    continue

                key = f"{sym}|{name}|{cl}|{d['side']}|{d['kind']}-div"
                if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                    self._count(f"{tag}:cooldown")
                    continue

                series.mark(d)
                self._count(f"{tag}:signal")

                extra = [
                    f"Hidden rule: trend direction + orderflow direction",
                    f"Trend source: {trend_tf}",
                    f"Trend alignment: {trend_note}",
                    f"Orderflow trend check: {self._flow_snapshot(st, frames)}",
                    f"CHoCH context: {self._context_line(sym)}",
                    f"Gate line (context only): {self._gate_line(sym, d['side'], now)}",
                ]

                self._send_div_alert(
                    sym=sym,
                    tf=name,
                    cl=cl,
                    d=d,
                    st=st,
                    now=now,
                    key=key,
                    closed=closed,
                    extra_lines=extra,
                    label=f"{name}/{cl} {d['side']} hidden-div trend",
                )

    # ---------- CHoCH data ----------

    async def _choch_fetch(
        self, session: aiohttp.ClientSession, sym: str, tf_name: str,
        st: dict[str, Any],
    ) -> bool:
        tfc = MERGE["choch_timeframes"][tf_name]
        sec_ms = tfc["seconds"] * 1000
        now_ms = int(time.time() * 1000)
        lookback_ms = int(tfc["lookback_days"]) * 86_400_000
        candles = st["candles"]

        start = candles[-1][0] + sec_ms if candles else now_ms - lookback_ms
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
                ts,
                float(r[1]),
                float(r[2]),
                float(r[3]),
                float(r[4]),
                float(r[5]),
            ])
            added = True

        cutoff = now_ms - lookback_ms
        drop = 0
        while drop < len(candles) and candles[drop][0] < cutoff:
            drop += 1
        if drop:
            del candles[:drop]

        return added

    async def _choch_update(
        self, session: aiohttp.ClientSession, sem: asyncio.Semaphore,
        sym: str, tf_name: str,
    ) -> None:
        st = self.choch[sym][tf_name]
        if st["busy"]:
            return
        st["busy"] = True
        tfc = MERGE["choch_timeframes"][tf_name]
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
                    st["next_due_ms"] = now_ms + max(60_000, MERGE["loop_sec"] * 4 * 1000)
                    return

                if len(candles) < SWING_STRENGTH * 2 + 15:
                    st["next_due_ms"] = now_ms + 300_000
                    return

                events, trend = await asyncio.to_thread(
                    analyze_choch, [list(c) for c in candles]
                )

                latest = events[-1] if events else None

                # v3: active = latest event regardless of weak/volume
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
                        "[%s] ACTIVE %s CHoCH %s level=%.8g close=%.8g "
                        "weak=%s vol_ok=%s strength=%s",
                        sym,
                        active["type"],
                        tf_name,
                        active["level"],
                        active["price"],
                        active.get("weak"),
                        active.get("volume_ok", True),
                        active.get("strength"),
                    )

                self._dispatch_new_choch_alerts(sym, tf_name, events, time.time())

                # v3 FIX: 2 * tfc["seconds"] -> 1 * tfc["seconds"]
                st["next_due_ms"] = (
                    candles[-1][0]
                    + 1 * tfc["seconds"] * 1000
                    + MERGE["refresh_delay_sec"] * 1000
                )

        except Exception as exc:
            log.exception("[%s] CHoCH %s update error: %s", sym, tf_name, exc)
            st["next_due_ms"] = int(time.time() * 1000) + 60_000
        finally:
            st["busy"] = False

    async def _choch_loop(self) -> None:
        self.choch = {
            sym: {
                tf: {
                    "candles": [],
                    "latest": None,
                    "trend": None,
                    "active": None,
                    "busy": False,
                    "next_due_ms": 0,
                }
                for tf in MERGE["choch_timeframes"]
            }
            for sym in self.states
        }

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
                                jobs.append(
                                    self._choch_update(session, sem, sym, tf_name)
                                )

                    if jobs:
                        await asyncio.gather(*jobs)

                    if not loaded_logged:
                        ready = sum(
                            1
                            for s in self.choch.values()
                            if all(v["candles"] for v in s.values())
                        )
                        if ready == len(self.choch):
                            loaded_logged = True
                            active = sum(
                                1
                                for s in self.choch.values()
                                for tf in MERGE["gate_frames"]
                                if s.get(tf, {}).get("active")
                            )
                            log.info(
                                "[CHOCH] Loaded %d symbols x %d TFs | "
                                "active CHoCHs (label): %d",
                                ready,
                                len(MERGE["choch_timeframes"]),
                                active,
                            )

                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("[CHOCH] loop error: %s", exc)

                if await self._sleep_or_stop(MERGE["loop_sec"]):
                    break

    async def _ws_loop(self, url: str) -> None:
        tasks = [
            asyncio.create_task(self._choch_loop()),
            asyncio.create_task(self._hourly_loop()),
        ]
        try:
            await super()._ws_loop(url)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


def main() -> None:
    load_dotenv()
    of.setup_logging()

    log.info(
        "[MERGE] v3 CHoCH-as-label + OF quality filters | "
        "max_symbols=%s | 4H CHoCH close=%s | 1H regular=%s | hidden trend=%s | "
        "quality_filters=%s",
        of.CONFIG.get("max_symbols"),
        MERGE["alerts"]["4h_choc_close"],
        MERGE["alerts"]["1h_choc_regular_div_close"],
        MERGE["alerts"]["hidden_div_trend"],
        MERGE["orderflow_filters"]["enabled"],
    )

    of.CONFIG["flow_signals"]["enabled"] = bool(MERGE["flow_signals"])

    code = 0
    try:
        code = asyncio.run(ChochOrderflowEngine().run())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")
    raise SystemExit(code)


if __name__ == "__main__":
    main()