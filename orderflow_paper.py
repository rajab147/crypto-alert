#!/usr/bin/env python3
"""
orderflow_choch_live.py - LIVE: CHoCH (1h/4h) gate + Order-flow divergence alerts.

Ye file `orderflow_paper.py` (aap ka order-flow script) ke SAATH ek hi folder me rakhein.
- Coins: bilkul wohi jo order-flow script select karta hai (SymbolSelector, max_symbols=50).
- CHoCH: har coin ke liye 1h aur 4h par live (Binance klines, last 90 din, har candle close par refresh).
- Gate: divergence alert tabhi jab us coin par 1h YA 4h (MERGE["gate_mode"]) par
        usi direction ka CHoCH mojood ho:  BULLISH CHoCH -> BUY divergence, BEARISH CHoCH -> SELL divergence.
- Divergence logic, trigger candle, footprint, cooldown, ntfy - sab order-flow script ka original hai.

Chalane ka tareeqa:   python orderflow_choch_live.py
(.env me NTFY_URL / NTFY_TOKEN pehle ki tarah)
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
        # max_age_hours: CHoCH ke baad gate kitni der khula rahe (None = jab tak ulta CHoCH na aaye)
        "1h": {"seconds": 3600, "interval": "1h", "max_age_hours": 24},
        "4h": {"seconds": 14400, "interval": "4h", "max_age_hours": 72},
    },
    "gate_frames": ["1h", "4h"],
    "gate_mode": "any",          # "any" = 1h ya 4h me se koi bhi | "both" = dono par same direction CHoCH
    "allow_weak_choch": False,   # False = range/weekend/pump wale weak CHoCH gate nahi kholte
    "choch_alerts": True,        # naya CHoCH banne par alag ntfy alert
    "lookback_days": 90,
    "refresh_delay_sec": 3,      # candle close ke baad kitni der baad klines mangwayen
    "loop_sec": 15,
    "concurrency": 8,
    "flow_signals": False,       # False = sirf divergence alerts (purane CVD/OBI flow signals band)
}

# ============================ CHoCH CONFIG (v3.2 wali) ============================
SWING_STRENGTH = 3
USE_CLOSE_BREAK = True
STRICT_ONLY = False
USE_SWING_COUNT_STRENGTH = True
MIN_CONFIRM_SWINGS = 2
MAX_CONFIRM_SWINGS = 3
RANGE_FILTER_ENABLED = True
RANGE_TOLERANCE = 0.25
RANGE_MODE = "weak"            # "weak" | "skip"
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


# ============================ CHoCH LOGIC (original, unchanged) ============================
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
            weekend = (wd >= 5)

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
                box_in_range = (box_bottom <= c <= box_top)

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
                bos_events.append({"type": "BULLISH", "kind": "INITIAL", "idx": i, "ts": ts,
                                   "level": last_high["price"], "price": c})
                reset_swing_counts()
            elif last_low and down_price < last_low["price"]:
                trend = "bearish"
                bos_events.append({"type": "BEARISH", "kind": "INITIAL", "idx": i, "ts": ts,
                                   "level": last_low["price"], "price": c})
                reset_swing_counts()
            continue

        if trend == "bullish":
            for low in lows:
                if low["idx"] in broken_low_idxs or low["idx"] >= i:
                    continue
                if down_price < low["price"] and not was_low_broken_before(
                        candles, low["idx"], low["price"], i, close_break):
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
                    add_choch_event({"type": "BEARISH", "idx": i, "ts": ts,
                                     "level": protected_low["price"], "level_ts": protected_low["ts"],
                                     "price": c, "mode": protected_low["mode"], "strength": strength,
                                     "strength_reason": strength_reason, "broken_count": broken_count})
                    trend = "bearish"
                    lvl = protected_low["idx"]
                    highs = [x for x in highs if x["idx"] >= lvl]
                    lows = [x for x in lows if x["idx"] >= lvl]
                    reset_swing_counts()
                continue

            if last_high and up_price > last_high["price"]:
                bos_events.append({"type": "BULLISH", "kind": "BOS", "idx": i, "ts": ts,
                                   "level": last_high["price"], "price": c})
            continue

        if trend == "bearish":
            for high in highs:
                if high["idx"] in broken_high_idxs or high["idx"] >= i:
                    continue
                if up_price > high["price"] and not was_high_broken_before(
                        candles, high["idx"], high["price"], i, close_break):
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
                    add_choch_event({"type": "BULLISH", "idx": i, "ts": ts,
                                     "level": protected_high["price"], "level_ts": protected_high["ts"],
                                     "price": c, "mode": protected_high["mode"], "strength": strength,
                                     "strength_reason": strength_reason, "broken_count": broken_count})
                    trend = "bullish"
                    lvl = protected_high["idx"]
                    highs = [x for x in highs if x["idx"] >= lvl]
                    lows = [x for x in lows if x["idx"] >= lvl]
                    reset_swing_counts()
                continue

            if last_low and down_price < last_low["price"]:
                bos_events.append({"type": "BEARISH", "kind": "BOS", "idx": i, "ts": ts,
                                   "level": last_low["price"], "price": c})
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
            e["weak_reason"] = f"{flag} candle (possible fake)"


def analyze_choch(candles: list[list[float]]) -> tuple[list[dict[str, Any]], str | None]:
    atr = calculate_atr(candles, ATR_PERIOD)
    events, _bos, trend, _sw = detect_choch(candles, SWING_STRENGTH, USE_CLOSE_BREAK, atr)
    label_weak_chochs(candles, events, detect_pump_dump_flags(candles))
    return events, trend


# ============================ LIVE ENGINE ============================
class ChochAlerter(of.NtfyAlerter):
    """Divergence alert ki body me CHoCH ki info upar jor deta hai."""
    def __init__(self, cfg: dict[str, Any]) -> None:
        super().__init__(cfg)
        self.extra: dict[str, str] = {}

    async def send(self, title: str, body: str, priority: str = "high", tags: str = "") -> bool:
        e = self.extra.pop(title, "")
        if e:
            body = f"{e}\n{body}"
        return await super().send(title, body, priority, tags)


class ChochOrderflowEngine(of.Engine):
    def __init__(self) -> None:
        super().__init__()
        self.alerter = ChochAlerter(self.cfg)
        self.health.alerter = self.alerter
        self.choch: dict[str, dict[str, dict[str, Any]]] = {}

    # ---------- CHoCH gate ----------
    def _choch_gate(self, sym: str, side: str, now: float) -> list[str] | None:
        want = "BULLISH" if side == "BUY" else "BEARISH"
        frames = MERGE["gate_frames"]
        hits: list[str] = []
        for fr in frames:
            stt = self.choch.get(sym, {}).get(fr)
            ev = stt["latest"] if stt else None
            if not ev or ev["type"] != want:
                continue
            if ev.get("weak") and not MERGE["allow_weak_choch"]:
                continue
            tfc = MERGE["choch_timeframes"][fr]
            age_h = max(now - ev["ts"] / 1000.0 - tfc["seconds"], 0.0) / 3600.0
            if tfc["max_age_hours"] and age_h > tfc["max_age_hours"]:
                continue
            hits.append(f"{fr} {ev['type']} CHoCH ({age_h:.1f}h ago, level {ev['level']:.8g}, "
                        f"{ev.get('strength') or 'n/a'})")
        need = 1 if MERGE["gate_mode"] == "any" else len(frames)
        return hits if len(hits) >= need else None

    def _div_gate(self, d, series, st, w, tf, now):
        hits = self._choch_gate(st.symbol, d["side"], now)
        if hits is None:
            return "no_choch"
        d["choch"] = hits
        return super()._div_gate(d, series, st, w, tf, now)

    def _alert_div(self, sym, tf, cl, d, st, now, key, closed):
        if not self.book.cap_reached():
            trig = of.candle_label(int(self.cfg["timeframes"][tf]["confirm_candle_seconds"]))
            title = f"{sym} {d['side']} Divergence [{tf}] {trig} close"
            self.alerter.extra[title] = "CHoCH gate: " + "; ".join(d.get("choch", []))
        super()._alert_div(sym, tf, cl, d, st, now, key, closed)

    # ---------- live CHoCH data ----------
    async def _choch_fetch(self, session: aiohttp.ClientSession, sym: str, tf_name: str,
                           st: dict[str, Any]) -> None:
        tfc = MERGE["choch_timeframes"][tf_name]
        sec_ms = tfc["seconds"] * 1000
        now_ms = int(time.time() * 1000)
        lookback_ms = MERGE["lookback_days"] * 86_400_000
        candles = st["candles"]
        start = candles[-1][0] + sec_ms if candles else now_ms - lookback_ms
        rows: list[list[Any]] = []
        sel = of.SymbolSelector(self.cfg)
        while True:
            data = await sel._get(
                session, f"{of.REST_BASE}/api/v3/klines?symbol={sym}&interval={tfc['interval']}"
                         f"&startTime={start}&limit=1000")
            rows.extend(data)
            if len(data) < 1000:
                break
            start = int(data[-1][0]) + sec_ms
        for r in rows:
            if int(r[6]) >= now_ms:          # forming candle
                continue
            ts = int(r[0])
            if candles and ts <= candles[-1][0]:
                continue
            candles.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])])
        cutoff = now_ms - lookback_ms
        drop = 0
        while drop < len(candles) and candles[drop][0] < cutoff:
            drop += 1
        if drop:
            del candles[:drop]

    async def _choch_update(self, session: aiohttp.ClientSession, sem: asyncio.Semaphore,
                            sym: str, tf_name: str, cid: int) -> None:
        st = self.choch[sym][tf_name]
        tfc = MERGE["choch_timeframes"][tf_name]
        async with sem:
            try:
                await self._choch_fetch(session, sym, tf_name, st)
            except Exception as exc:
                log.warning("[%s] CHoCH %s fetch failed: %s", sym, tf_name, exc)
                return
        candles = st["candles"]
        if len(candles) < SWING_STRENGTH * 2 + 15:
            return
        if candles[-1][0] < (cid - 1) * tfc["seconds"] * 1000:
            return                                   # last closed candle abhi available nahi
        events, trend = await asyncio.to_thread(analyze_choch, [list(c) for c in candles])
        latest = events[-1] if events else None
        st["latest"], st["trend"], st["cid"] = latest, trend, cid
        prev = st["notified_ts"]
        if latest is None:
            if prev is None:
                st["notified_ts"] = 0
            return
        if prev is None:
            st["notified_ts"] = latest["ts"]         # pehli load: purane CHoCH par alert nahi
            return
        if latest["ts"] > prev:
            st["notified_ts"] = latest["ts"]
            log.info("[%s] NEW %s CHoCH %s level=%.8g close=%.8g weak=%s", sym, latest["type"], tf_name,
                     latest["level"], latest["price"], latest.get("weak"))
            if MERGE["choch_alerts"]:
                weak = f"\nWEAK: {latest['weak_reason']}" if latest.get("weak") else ""
                body = (f"Symbol: {sym}\nTimeframe: {tf_name}\nCHoCH: {latest['type']}\n"
                        f"Broken level: {latest['level']:.8g}\nBreak close: {latest['price']:.8g}\n"
                        f"Strength: {latest.get('strength') or 'n/a'} ({latest.get('strength_reason') or ''})\n"
                        f"Mode: {latest.get('mode')}{weak}\n"
                        f"Candle time: {datetime.fromtimestamp(latest['ts'] / 1000, timezone.utc):%Y-%m-%d %H:%M} UTC\n"
                        f"Order-flow divergence alerts for this direction are now enabled.")
                tags = "green_circle" if latest["type"] == "BULLISH" else "red_circle"
                self._spawn(self.alerter.send(f"{sym} {latest['type']} CHoCH [{tf_name}]", body,
                                              "default", tags))

    async def _choch_loop(self) -> None:
        self.choch = {sym: {tf: {"candles": [], "latest": None, "trend": None, "cid": -1,
                                 "notified_ts": None}
                            for tf in MERGE["choch_timeframes"]} for sym in self.states}
        sem = asyncio.Semaphore(int(MERGE["concurrency"]))
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        loaded_logged = False
        async with aiohttp.ClientSession(timeout=timeout) as session:
            while not self.stop.is_set():
                try:
                    now = time.time()
                    jobs = []
                    for tf_name, tfc in MERGE["choch_timeframes"].items():
                        cid = int(now // tfc["seconds"])
                        if now - cid * tfc["seconds"] < MERGE["refresh_delay_sec"]:
                            continue
                        for sym in self.states:
                            if self.choch[sym][tf_name]["cid"] < cid:
                                jobs.append(self._choch_update(session, sem, sym, tf_name, cid))
                    if jobs:
                        await asyncio.gather(*jobs)
                        if not loaded_logged:
                            ready = sum(1 for s in self.choch.values()
                                        if all(v["cid"] >= 0 for v in s.values()))
                            if ready == len(self.choch):
                                loaded_logged = True
                                active = sum(1 for s in self.choch.values()
                                             for v in s.values() if v["latest"])
                                log.info("[CHOCH] Loaded %d symbols x %d TFs (%d with a CHoCH in last %dd)",
                                         ready, len(MERGE["choch_timeframes"]), active, MERGE["lookback_days"])
                except asyncio.CancelledError:
                    raise
                except Exception as exc:
                    log.exception("[CHOCH] loop error: %s", exc)
                if await self._sleep_or_stop(MERGE["loop_sec"]):
                    break

    async def _ws_loop(self, url: str) -> None:
        task = asyncio.create_task(self._choch_loop())
        try:
            await super()._ws_loop(url)
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)


def main() -> None:
    load_dotenv()
    of.setup_logging()
    of.CONFIG["flow_signals"]["enabled"] = bool(MERGE["flow_signals"])
    code = 0
    try:
        code = asyncio.run(ChochOrderflowEngine().run())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")
    raise SystemExit(code)


if __name__ == "__main__":
    main()
