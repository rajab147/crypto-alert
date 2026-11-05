#!/usr/bin/env python3
"""
orderflow_choch_live.py - corrected full version
Binance Spot LIVE order-flow + CHoCH alert engine.

Implemented rules:
- Volume is completely ignored as a blocking filter.
- CHoCH is checked only on 1D and 4H.
- 1H CHoCH is removed completely.
- 1D CHoCH -> entry/order-flow confirmation on 1H candle close.
- 4H CHoCH -> entry/order-flow confirmation on 30m candle close.
- Lower-timeframe BUY/SELL order-flow is allowed even if it is opposite to HTF CHoCH side.
- Alert includes HTF CHoCH side + LTF order-flow side label.
- One active CHoCH remains valid until a new CHoCH appears.
- Same coin / same active CHoCH / same entry TF / same side alerts only once until new CHoCH.
- Uses original orderflow_paper.py for Binance spot aggTrade + depth + CVD/OBI/footprint data.

Run:
    python orderflow_choch_live.py

Required files in same folder:
    - orderflow_paper.py
    - orderflow_choch_live.py
    - .env with NTFY_URL / NTFY_TOKEN if push alerts are wanted
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from datetime import datetime, timezone
from typing import Any

import aiohttp
from dotenv import load_dotenv

import orderflow_paper as of

log = of.log

ENTRY_STATE_FILE = "choch_entry_state.json"


# =====================================================================
# PATCH BASE ORDERFLOW CONFIG: VOLUME IGNORED, NEW ENTRY TIMEFRAMES
# =====================================================================

CFG = of.CONFIG

# Symbol selection: do not block by volume/trades.
CFG["max_symbols"] = int(CFG.get("max_symbols", 50))
CFG["min_quote_volume_24h"] = 0.0
CFG["min_trades_24h"] = 0
CFG["max_price_change_24h_pct"] = 100.0

# IMPORTANT:
# These are NOT volume gates anymore.
# They are only set to the minimum legal values accepted by orderflow_paper.validate_config().
# Actual alert blocking is disabled by overridden _global_block/_evaluate logic.
CFG["cvd_threshold"] = 0.1
CFG["obi_threshold"] = 0.01
CFG["obi_window_average"] = True
CFG["spread_max_bps"] = 200.0
CFG["eval_interval_sec"] = int(CFG.get("eval_interval_sec", 3))
CFG["bucket_seconds"] = int(CFG.get("bucket_seconds", 5))
CFG["min_window_fill"] = 0.1
CFG["diag_interval_min"] = int(CFG.get("diag_interval_min", 30))
CFG["div_history_candles"] = 60
CFG["short_window_seconds"] = 900
CFG["short_cvd_threshold"] = 0.1
CFG["short_max_move_pct"] = 10.0
CFG["fresh_max_age_sec"] = 999_999
CFG["mtf_cvd_min_abs_pct"] = 0.0
CFG["mtf_obi_min_abs"] = 0.0
CFG["max_alerts_per_day"] = 10_000
CFG["depth_levels"] = 10
CFG["stale_seconds"] = 10
CFG["ws_recv_timeout_sec"] = 30
CFG["reconnect_wipe_after_sec"] = 120
CFG["reconnect_backoff_max_sec"] = 60
CFG["reconnect_fail_alert_after"] = 3
CFG["state_save_interval_sec"] = 60
CFG["http_timeout_sec"] = 60
CFG["http_retries"] = 3
CFG["ntfy_timeout_sec"] = 10
CFG["warmup_seconds"] = 0

# Old standalone flow signals are disabled.
# Alerts come only from CHoCH + LTF order-flow logic.
CFG["flow_signals"] = {
    "enabled": False,
}

# Adaptive thresholds disabled; volume regime should not block signals.
CFG["adaptive"] = {
    "enabled": False,
    "sample_interval_sec": 30,
    "high_vol_move15": 1.0,
    "low_vol_move15": 0.25,
    "high_mult": 1.25,
    "low_mult": 0.80,
}

# Health.
CFG["health"] = {
    "silent_limit_sec": 300,
    "check_interval_sec": 60,
    "alert_on_silent": True,
}

# Footprint: keep data available, but never required/blocking.
CFG["footprint"] = {
    "row_bps": 3.0,
    "keep_candles": 60,
    "min_rows": 1,
    "zone_rows": 1,
    "absorb_min_frac": 0.0001,
    "imbalance_ratio": 1.01,
    "imbalance_min_frac": 0.004,
    "min_stacked": 1,
    "min_confirms": 0,
    "flow_zone_rows": 1,
    "flow_min_ratio": 1.01,
    "required": False,
}

# Divergence settings: relaxed, no regime block, no color/delta hard block.
CFG["divergence"] = {
    "fractal_left": 2,
    "fractal_right": 1,
    "use_forming_swing": True,
    "min_swing_gap_candles": 2,
    "max_swing_gap_candles": 120,
    "regime_mode": "off",
    "regime_tf": "4h",
    "regime_oppose_cvd_pct": 0.1,
    "trigger_max_delay_sec": 300,
    "trigger_require_color": False,
    "trigger_require_delta": False,
    "trigger_break": "off",
}

# Timeframes:
# - 1h: entry TF for 1D CHoCH
# - 30m: entry TF for 4H CHoCH
# - 4h: kept for base compatibility/context/regime, not used as CHoCH entry gate directly
CFG["timeframes"] = {
    "1h": {
        "seconds": 3600,
        "cooldown_min": 0,
        "div_candle_seconds": [3600],
        "div_base_seconds": 60,
        "confirm_candle_seconds": 3600,
        "min_window_volume_usdt": 0.0,
        "min_trade_count": 0,
        "max_recent_move_pct": 999.0,
        "div_max_move_since_swing_pct": 999.0,
        "div_min_price_diff_pct": 0.0,
        "div_min_cvd_diff_frac": 0.0,
        "max_swing_age_sec": 10_800_000,
        "mtf_frames": [],
    },
    "30m": {
        "seconds": 1800,
        "cooldown_min": 0,
        "div_candle_seconds": [1800],
        "div_base_seconds": 60,
        "confirm_candle_seconds": 1800,
        "min_window_volume_usdt": 0.0,
        "min_trade_count": 0,
        "max_recent_move_pct": 999.0,
        "div_max_move_since_swing_pct": 999.0,
        "div_min_price_diff_pct": 0.0,
        "div_min_cvd_diff_frac": 0.0,
        "max_swing_age_sec": 2_700_000,
        "mtf_frames": [],
    },
    "4h": {
        "seconds": 14400,
        "cooldown_min": 0,
        "div_candle_seconds": [14400],
        "div_base_seconds": 300,
        "confirm_candle_seconds": 14400,
        "min_window_volume_usdt": 0.0,
        "min_trade_count": 0,
        "max_recent_move_pct": 999.0,
        "div_max_move_since_swing_pct": 999.0,
        "div_min_price_diff_pct": 0.0,
        "div_min_cvd_diff_frac": 0.0,
        "max_swing_age_sec": 43_200_000,
        "mtf_frames": [],
    },
}


# =====================================================================
# MERGE / CHoCH CONFIG
# =====================================================================

MERGE: dict[str, Any] = {
    # CHoCH is checked only on these closed-candle timeframes.
    "choch_timeframes": {
        "4h": {
            "seconds": 14400,
            "interval": "4h",
            "lookback_days": 365,
        },
        "1d": {
            "seconds": 86400,
            "interval": "1d",
            "lookback_days": 730,
        },
    },

    # HTF CHoCH frames used as setup context/gate.
    "gate_frames": ["1d", "4h"],

    # Context shown in alert body.
    "context_frames": ["1d", "4h"],

    # HTF CHoCH -> LTF entry timeframe.
    "entry_map": {
        "1d": "1h",
        "4h": "30m",
    },

    # Because volume is ignored and you want both sides possible,
    # keep latest structural CHoCH active.
    "allow_weak_choch": True,

    # Live CHoCH refresh.
    "refresh_delay_sec": 5,
    "loop_sec": 5,
    "concurrency": 3,

    # Old standalone flow signals off.
    "flow_signals": False,

    # Alert switches.
    "alerts": {
        "1d_choc_1h_entry": True,
        "4h_choc_30m_entry": True,
    },

    # CHoCH volume confirmation disabled completely.
    "choch_volume": {
        "enabled": False,
        "average_period": 20,
        "mode": "off",
        "min_ratio": 1.0,
        "any_change_pct": 0.0,
    },

    # Order-flow trend direction filter disabled because LTF side may oppose HTF CHoCH.
    "orderflow_trend": {
        "enabled": False,
        "min_cvd_pct": 0.0,
        "same_sign": False,
        "oppose_cvd_pct": 0.0,
        "require_obi": False,
        "min_obi": 0.0,
        "frames_4h_choc": ["4h", "1h"],
        "frames_1h_regular": ["1h", "15m"],
        "frames_hidden": ["4h", "1h"],
    },

    "hidden_trend_tf": "4h",

    "hourly_regular": {
        "max_delay_sec": 120,
        "require_color": False,
        "require_delta": False,
        "cand_max_age_candles": 1,
    },
}


# =====================================================================
# CHoCH STRUCTURE CONSTANTS (same core logic, 1H removed from usage)
# =====================================================================

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


# =====================================================================
# CHoCH HELPERS
# =====================================================================

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
    """
    Volume confirmation is disabled by MERGE config.
    This function still exists to keep CHoCH logic compatible.
    """
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


# =====================================================================
# LIVE ENGINE
# =====================================================================

class ChochOrderflowEngine(of.Engine):
    ENTRY_TO_HTF = {
        "1h": "1d",
        "30m": "4h",
    }

    def __init__(self) -> None:
        super().__init__()

        self.alerter = of.NtfyAlerter(self.cfg)
        self.health.alerter = self.alerter
        self.selector = of.SymbolSelector(self.cfg)

        # CHoCH state:
        # self.choch[symbol][timeframe] = {
        #   "candles": [...],
        #   "latest": event|None,
        #   "active": event|None,
        #   "trend": "bullish"|"bearish"|None,
        #   "busy": bool,
        #   "next_due_ms": int,
        # }
        self.choch: dict[str, dict[str, dict[str, Any]]] = {}

        # One alert per active CHoCH + entry TF + side.
        # key example: BTCUSDT|1h|1d|1712345600000|BUY
        # Persisted across restarts.
        self.entry_alerted: dict[str, float] = self._load_entry_state()

        self._boot_ms = int(time.time() * 1000)

    # ------------------------------------------------------------------
    # Entry-alert persistence
    # ------------------------------------------------------------------

    def _load_entry_state(self) -> dict[str, float]:
        if not os.path.exists(ENTRY_STATE_FILE):
            return {}

        try:
            with open(ENTRY_STATE_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)

            if not isinstance(data, dict):
                return {}

            now = time.time()
            cutoff = now - 30 * 86400
            out: dict[str, float] = {}
            for k, v in data.items():
                try:
                    tv = float(v)
                except Exception:
                    continue
                if tv > cutoff:
                    out[str(k)] = tv
            return out
        except Exception as exc:
            log.warning("[STATE] Failed to load %s: %s", ENTRY_STATE_FILE, exc)
            return {}

    async def _save_entry_state(self) -> None:
        try:
            now = time.time()
            cutoff = now - 30 * 86400

            # Prune old keys.
            self.entry_alerted = {
                k: v for k, v in self.entry_alerted.items()
                if v > cutoff
            }

            # Hard cap to avoid unbounded growth.
            if len(self.entry_alerted) > 100_000:
                items = sorted(
                    self.entry_alerted.items(),
                    key=lambda kv: kv[1],
                    reverse=True,
                )[:100_000]
                self.entry_alerted = dict(items)

            snapshot = dict(self.entry_alerted)
            tmp = ENTRY_STATE_FILE + ".tmp"

            def write() -> None:
                with open(tmp, "w", encoding="utf-8") as f:
                    json.dump(snapshot, f, indent=2)
                os.replace(tmp, ENTRY_STATE_FILE)

            await asyncio.to_thread(write)
        except Exception as exc:
            log.error("[STATE] Failed to save %s: %s", ENTRY_STATE_FILE, exc)

    async def _save_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["state_save_interval_sec"]):
            await self.book.save()
            await self._save_entry_state()
            log.info(
                "[ENGINE] State saved | alerts_today=%d entry_keys=%d connected=%s",
                self.book.alerts_today,
                len(self.entry_alerted),
                self.connected,
            )

    # ------------------------------------------------------------------
    # Small helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _side_from_type(t: str) -> str:
        return "BUY" if t == "BULLISH" else "SELL"

    @staticmethod
    def _type_from_side(side: str) -> str:
        return "BULLISH" if side == "BUY" else "BEARISH"

    def _tf_seconds(self, fr: str) -> int:
        return int(MERGE["choch_timeframes"][fr]["seconds"])

    def _entry_seconds(self, entry_tf: str) -> int:
        return int(self.cfg["timeframes"][entry_tf]["div_candle_seconds"][0])

    def _format_choch_event(self, ev: dict[str, Any], fr: str, now: float) -> str:
        tfc = MERGE["choch_timeframes"][fr]
        close_sec = float(ev["ts"]) / 1000.0 + float(tfc["seconds"])
        age = max(now - close_sec, 0.0)

        if fr == "1d":
            age_str = f"{age / 86400.0:.1f}d ago"
        else:
            age_str = f"{age / 3600.0:.1f}h ago"

        if MERGE["choch_volume"].get("enabled", True):
            vol_ratio = float(ev.get("volume_ratio", 0.0))
            vol_txt = f"vol x{vol_ratio:.2f}"
        else:
            vol_txt = "vol ignored"

        return (
            f"{fr.upper()} {ev['type'].title()} CHoCH "
            f"({age_str}, level {ev['level']:.8g}, close {ev['price']:.8g}, "
            f"{vol_txt}, {ev.get('strength') or 'n/a'})"
        )

    def _htf_label(self, sym: str, tf_name: str) -> str:
        st = self.choch.get(sym, {}).get(tf_name)
        if not st:
            return "Neutral"

        ev = st.get("active") or st.get("latest")
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

    def _active_choch(self, sym: str, htf_tf: str) -> dict[str, Any] | None:
        stt = self.choch.get(sym, {}).get(htf_tf)
        if not stt:
            return None

        ev = stt.get("active") or stt.get("latest")
        if not ev:
            return None

        # Volume is ignored. If allow_weak_choch is False, weak events are skipped.
        if ev.get("weak") and not MERGE["allow_weak_choch"]:
            return None
        if not ev.get("volume_ok", True):
            return None

        return ev

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

    def _fp_line(self, fp: dict[str, Any] | None, side: str, extreme: str) -> str:
        if not fp or not fp.get("available"):
            return "Footprint: n/a (insufficient rows/live data; not blocking)"

        heavy = "sell" if side == "BUY" else "buy"
        imb = "buy" if side == "BUY" else "sell"

        return (
            f"Footprint (row {fp.get('row', 0):.8g}): absorption "
            f"{'YES' if fp.get('absorb') else 'no'} "
            f"({heavy} vol at {extreme} rows {fp.get('frac', 0.0) * 100.0:.1f}% of candle), "
            f"stacked {imb} imbalances x{fp.get('stacked', 0)}"
        )

    def _closed_footprint_info(
        self,
        st: Any,
        entry_tf: str,
        closed: dict[str, Any],
        side: str,
    ) -> dict[str, Any]:
        """
        Best-effort footprint info for the just-closed LTF candle.
        Never blocks the alert.
        """
        res: dict[str, Any] = {
            "available": False,
            "row": 0.0,
            "ratio": 0.0,
            "stacked": 0,
            "frac": 0.0,
        }

        try:
            entry_sec = self._entry_seconds(entry_tf)
            series = st.candles.get(entry_tf, {}).get(entry_sec)
            if series is None:
                return res

            closed_id = int(closed["id"])
            cand = None
            for c in reversed(series.candles):
                if c.id == closed_id and not c.partial:
                    cand = c
                    break
                if c.id < closed_id:
                    break

            if cand is None or not cand.fp or cand.row <= 0:
                return res

            f = self.cfg["footprint"]
            fp = cand.fp
            total = sum(a + b for a, b in fp.values())
            if total <= 0:
                return res

            close_price = float(closed.get("close", st.last_price))
            cur = int(close_price // cand.row)
            z = max(1, int(f.get("flow_zone_rows", 1)))

            if side == "BUY":
                levels = range(cur - z + 1, cur + 1)
            else:
                levels = range(cur, cur + z)

            def vol(level: int, idx: int) -> float:
                cell = fp.get(level)
                return cell[idx] if cell else 0.0

            buy = sum(vol(lv, 1) for lv in levels)
            sell = sum(vol(lv, 0) for lv in levels)
            mine, other = (buy, sell) if side == "BUY" else (sell, buy)

            res["available"] = True
            res["row"] = cand.row
            res["ratio"] = mine / other if other > 0 else float("inf")

            min_vol = float(f.get("imbalance_min_frac", 0.0)) * total
            ratio_req = float(f.get("imbalance_ratio", 1.01))

            best = run = 0
            levels_list = list(levels)
            seq = levels_list if side == "BUY" else list(reversed(levels_list))

            for lv in seq:
                if side == "BUY":
                    hit = vol(lv, 1) >= min_vol and vol(lv, 1) >= ratio_req * vol(lv - 1, 0)
                else:
                    hit = vol(lv, 0) >= min_vol and vol(lv, 0) >= ratio_req * vol(lv + 1, 1)

                run = run + 1 if hit else 0
                if run > best:
                    best = run

            res["stacked"] = best
            res["frac"] = (buy + sell) / total
            return res
        except Exception:
            return res

    def _fp_line_closed(self, info: dict[str, Any], side: str) -> str:
        if not info or not info.get("available"):
            return "Footprint: n/a for closed candle (not blocking)"

        heavy = "sell" if side == "BUY" else "buy"
        imb = "buy" if side == "BUY" else "sell"
        ratio = float(info.get("ratio", 0.0))
        ratio_s = "inf" if ratio == float("inf") else f"{ratio:.2f}"

        return (
            f"Footprint closed candle (row {info.get('row', 0):.8g}): "
            f"{heavy} pressure in zone {info.get('frac', 0.0) * 100.0:.1f}% of candle, "
            f"{imb} imbalance ratio {ratio_s}, stacked x{info.get('stacked', 0)}"
        )

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
                sym,
                title,
                self.cfg["max_alerts_per_day"],
                body.replace("\n", " | "),
            )
            self.book.last_alert[key] = now
            return

        self.book.register(key, sym, tf, side, now, label=label)
        self.health.note_signal()
        log.info("[%s] %s | %s", sym, title, body.replace("\n", " | "))
        self._spawn(self.alerter.send(title, body, "high", tags))

    # ------------------------------------------------------------------
    # Override base blocking: volume/spread/depth no longer block alerts
    # ------------------------------------------------------------------

    def _global_block(self, st: of.OrderFlowState, now: float) -> str | None:
        if st.last_price <= 0:
            return "no_price"
        if now - self.stream_start < self.cfg.get("warmup_seconds", 0):
            return "warmup"
        return None

    # ------------------------------------------------------------------
    # Main evaluation loop override
    # ------------------------------------------------------------------

    def _evaluate(self, sym: str, st: of.OrderFlowState, now: float) -> None:
        st.evict(now)

        gb = self._global_block(st, now)
        if gb:
            self._count(f"all:{gb}")
            return

        try:
            smove = st.short.recent_move_pct(
                st.last_price,
                st.last_trade_ts,
                self.cfg["min_window_fill"],
            )
        except Exception:
            smove = 999.0

        if smove < 999.0:
            self.adapt.observe(smove, now)

        for entry_tf in ("1h", "30m"):
            try:
                self._process_entry(sym, st, entry_tf, now)
            except Exception as exc:
                log.exception("[%s] %s entry processing error: %s", sym, entry_tf, exc)

    # ------------------------------------------------------------------
    # Entry processing on LTF candle close
    # ------------------------------------------------------------------

    def _process_entry(
        self,
        sym: str,
        st: of.OrderFlowState,
        entry_tf: str,
        now: float,
    ) -> None:
        if entry_tf == "1h" and not MERGE["alerts"].get("1d_choc_1h_entry", True):
            return
        if entry_tf == "30m" and not MERGE["alerts"].get("4h_choc_30m_entry", True):
            return

        tr = st.trig.get(entry_tf)
        if tr is None:
            return

        closed = tr.poll(now)
        if closed is None:
            return

        if closed.get("partial"):
            self._count(f"{entry_tf}/entry:partial_close")
            return

        trig_sec = int(tr.sec)
        max_delay = float(self.cfg["divergence"]["trigger_max_delay_sec"])
        if now - (closed["id"] + 1) * trig_sec > max_delay:
            self._count(f"{entry_tf}/entry:trigger_stale")
            return

        htf_tf = self.ENTRY_TO_HTF[entry_tf]
        choc = self._active_choch(sym, htf_tf)
        if choc is None:
            self._count(f"{entry_tf}/entry:no_{htf_tf}_choch")
            return

        choc_ts = int(choc["ts"])
        candidates = self._entry_candidates(sym, st, entry_tf, closed, now)
        if not candidates:
            self._count(f"{entry_tf}/entry:no_orderflow")
            return

        entry_sec = self._entry_seconds(entry_tf)
        series = st.candles.get(entry_tf, {}).get(entry_sec)

        seen_sides: set[str] = set()
        for side, kind, source, d in candidates:
            if side in seen_sides:
                continue
            seen_sides.add(side)

            key = f"{sym}|{entry_tf}|{htf_tf}|{choc_ts}|{side}"
            if key in self.entry_alerted:
                if d is not None and series is not None:
                    try:
                        series.mark(d)
                    except Exception:
                        pass
                self._count(f"{entry_tf}/entry:already_alerted_{side}")
                continue

            self.entry_alerted[key] = now

            if d is not None and series is not None:
                try:
                    series.mark(d)
                except Exception:
                    pass

            self._send_entry_alert(
                sym=sym,
                entry_tf=entry_tf,
                htf_tf=htf_tf,
                choc=choc,
                side=side,
                kind=kind,
                source=source,
                d=d,
                st=st,
                closed=closed,
                now=now,
                key=key,
            )

    def _entry_candidates(
        self,
        sym: str,
        st: of.OrderFlowState,
        entry_tf: str,
        closed: dict[str, Any],
        now: float,
    ) -> list[tuple[str, str, str, dict[str, Any] | None]]:
        candidates: list[tuple[str, str, str, dict[str, Any] | None]] = []

        entry_sec = self._entry_seconds(entry_tf)
        series = st.candles.get(entry_tf, {}).get(entry_sec)

        # 1) Divergence candidates from LTF candle series.
        if series is not None:
            try:
                divs = series.find_divergences()
            except Exception as exc:
                log.warning("[%s] %s find_divergences error: %s", sym, entry_tf, exc)
                divs = []

            for d in divs:
                try:
                    cand_id = int(d["cand"].id)
                except Exception:
                    continue

                # Only accept divergence whose swing B is the just-closed candle
                # or the previous closed candle. This avoids firing on far-old swings.
                if cand_id > int(closed["id"]):
                    continue
                if int(closed["id"]) - cand_id > 1:
                    continue

                self._prepare_div(d, series, st, entry_tf, now)
                candidates.append((d["side"], d["kind"], "divergence", d))

        # 2) Pure order-flow imbalance candidate from closed candle + rolling CVD/OBI.
        imbalance_side = self._imbalance_side(st, entry_tf, closed)
        if imbalance_side is not None:
            candidates.append((imbalance_side, "imbalance", "orderflow_imbalance", None))

        return candidates

    def _prepare_div(
        self,
        d: dict[str, Any],
        series: of.CandleSeries,
        st: of.OrderFlowState,
        entry_tf: str,
        now: float,
    ) -> None:
        f = self.cfg["footprint"]

        p1 = float(d.get("p1", 0.0))
        p2 = float(d.get("p2", 0.0))

        if p1 > 0 and p2 > 0:
            move = (st.last_price - p2) if d["side"] == "BUY" else (p2 - st.last_price)
            d["since"] = move / p2 * 100.0
        else:
            d["since"] = 0.0

        try:
            d["fp"] = series.footprint_check(d, st.last_price, f)
        except Exception:
            d["fp"] = {
                "available": False,
                "absorb": False,
                "stacked": 0,
                "frac": 0.0,
                "confirms": 0,
                "row": 0.0,
                "span": 0,
            }

    def _imbalance_side(
        self,
        st: of.OrderFlowState,
        entry_tf: str,
        closed: dict[str, Any],
    ) -> str | None:
        """
        Simple LTF order-flow direction score.
        Volume amount is not required; only sign of delta/CVD/OBI is used.
        """
        score = 0

        delta = float(closed.get("buy", 0.0)) - float(closed.get("sell", 0.0))
        if delta > 0:
            score += 1
        elif delta < 0:
            score -= 1

        w = st.windows.get(entry_tf)
        if w is not None:
            cvd = w.cvd_pct
            if cvd > 0:
                score += 1
            elif cvd < 0:
                score -= 1

            obi = w.avg_obi
            if obi is not None:
                if obi > 0:
                    score += 1
                elif obi < 0:
                    score -= 1

        if score > 0:
            return "BUY"
        if score < 0:
            return "SELL"
        return None

    def _send_entry_alert(
        self,
        sym: str,
        entry_tf: str,
        htf_tf: str,
        choc: dict[str, Any],
        side: str,
        kind: str,
        source: str,
        d: dict[str, Any] | None,
        st: of.OrderFlowState,
        closed: dict[str, Any],
        now: float,
        key: str,
    ) -> None:
        htf_side = self._side_from_type(choc["type"])
        alignment = "SAME" if htf_side == side else "OPPOSITE"

        entry_sec = self._entry_seconds(entry_tf)
        entry_label = of.candle_label(entry_sec)

        w = st.windows.get(entry_tf)
        cvd = w.cvd_pct if w is not None else 0.0
        obi = w.avg_obi if w is not None else None
        obi_s = f"{obi:+.3f}" if obi is not None else "n/a"
        vol = w.window_volume if w is not None else 0.0
        trades = w.trade_count if w is not None else 0

        candle_color = "green" if float(closed.get("close", 0)) > float(closed.get("open", 0)) else "red"
        candle_delta = float(closed.get("buy", 0.0)) - float(closed.get("sell", 0.0))

        if source == "divergence" and d is not None:
            signal_text = of.DIV_TEXT.get(
                (side, d.get("kind", "regular")),
                f"{side} {d.get('kind', '')} divergence",
            )
            extreme = "low" if side == "BUY" else "high"
            fp_line = self._fp_line(d.get("fp"), side, extreme)
        else:
            signal_text = "LTF order-flow imbalance (taker delta + CVD + OBI sign)"
            fp_info = self._closed_footprint_info(st, entry_tf, closed, side)
            fp_line = self._fp_line_closed(fp_info, side)

        title = f"{sym} {side} {entry_tf.upper()} Order-flow [{htf_tf.upper()} CHoCH]"

        lines = [
            f"Symbol: {sym}",
            f"Alert type: {htf_tf.upper()} CHoCH -> {entry_tf.upper()} order-flow entry",
            f"HTF CHoCH: {self._format_choch_event(choc, htf_tf, now)}",
            f"HTF CHoCH side: {htf_side}",
            f"Entry timeframe: {entry_tf.upper()} ({entry_label} candle)",
            f"Entry side: {side}",
            f"Alignment: {alignment} (HTF CHoCH {htf_side} vs LTF order-flow {side})",
            f"Signal: {signal_text}",
            f"Price: {st.last_price:.8g}",
            f"{entry_label} candle CLOSED: {candle_color}, delta {candle_delta:+,.0f} USDT",
            f"CVD% ({entry_tf}): {cvd:+.2f}",
            f"OBI ({entry_tf}): {obi_s}",
            f"Volume USDT ({entry_tf}): {vol:,.0f} (volume ignored as filter)",
            f"Trades ({entry_tf}): {trades} (trades ignored as filter)",
        ]

        if source == "divergence" and d is not None:
            lines.extend([
                f"Swing price A->B: {float(d.get('p1', 0)):.8g} -> {float(d.get('p2', 0)):.8g} "
                f"({int(d.get('gap', 0))} candles apart{', B forming' if d.get('forming') else ''})",
                f"Move since swing: {float(d.get('since', 0)):+.2f}%",
                f"CVD swing value (USDT): {float(d.get('v1', 0)):+,.0f} -> {float(d.get('v2', 0)):+,.0f}",
                fp_line,
            ])
        else:
            lines.append(fp_line)

        lines.extend([
            f"Higher timeframe context: {self._context_line(sym)}",
            "Rule: HTF CHoCH side and LTF order-flow side may be opposite; alert shows both labels.",
            "Repeat control: one alert per active CHoCH per entry TF per side until new CHoCH.",
        ])

        body = "\n".join(lines)
        tags = "green_circle" if side == "BUY" else "red_circle"
        label = f"{htf_tf} {choc['type'].title()} CHoCH -> {entry_tf} {side} {kind}"

        self._commit_alert(key, sym, entry_tf, side, now, label, title, body, tags)

    # ------------------------------------------------------------------
    # CHoCH data fetch/update
    # ------------------------------------------------------------------

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
            # Skip still-forming candle. CHoCH only on closed candles.
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
                    analyze_choch,
                    [list(c) for c in candles],
                )

                latest = events[-1] if events else None

                active = None
                for e in reversed(events):
                    if MERGE["allow_weak_choch"] or not e.get("weak"):
                        if e.get("volume_ok", True):
                            active = e
                            break

                if active is None and latest is not None and MERGE["allow_weak_choch"]:
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
                        "[%s] ACTIVE %s CHoCH %s level=%.8g close=%.8g weak=%s vol_ok=%s",
                        sym,
                        active["type"],
                        tf_name,
                        active["level"],
                        active["price"],
                        active.get("weak"),
                        active.get("volume_ok", True),
                    )

                st["next_due_ms"] = (
                    candles[-1][0]
                    + 2 * tfc["seconds"] * 1000
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

        # Let order-flow backfill breathe first.
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
                                "[CHOCH] Loaded %d symbols x %d TFs | active gate CHoCHs: %d",
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
        ]

        try:
            await super()._ws_loop(url)
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)


# =====================================================================
# MAIN
# =====================================================================

def main() -> None:
    load_dotenv()
    of.setup_logging()

    log.info(
        "[MERGE] corrected CHoCH logic | max_symbols=%s | 1D CHoCH -> 1H entry=%s | 4H CHoCH -> 30m entry=%s | volume gates disabled",
        of.CONFIG.get("max_symbols"),
        MERGE["alerts"]["1d_choc_1h_entry"],
        MERGE["alerts"]["4h_choc_30m_entry"],
    )

    # Ensure old standalone flow signals stay off.
    of.CONFIG["flow_signals"]["enabled"] = bool(MERGE["flow_signals"])

    code = 0
    try:
        code = asyncio.run(ChochOrderflowEngine().run())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")

    raise SystemExit(code)


if __name__ == "__main__":
    main()