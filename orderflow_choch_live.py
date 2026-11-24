#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORDERFLOW + CHoCH LIVE — Binance Spot USDT.
Base: orderflow_paper.Engine
اس ورژن میں:
  - Rule "30m+1h" = 30m + 1h order-flow confluence
  - Trend gate = 1h CHoCH (نیا شامل کردہ)
  - CHoCH context: 1h / 4h / 1d / 1w
  - CHoCH logic وہی منظور شدہ (close-break, کوئی time/count expiry نہیں) + backwards-scan bug-fix
"""
import asyncio
import os
import sys
import time
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

import orderflow_paper as of

load_dotenv()

# Binance REST/WS (region-safe data endpoints کی جگہ standard)
of.REST_BASE = os.getenv("BINANCE_REST_BASE", "https://api.binance.com").rstrip("/")
of.TICKER_URL = of.REST_BASE + "/api/v3/ticker/24hr"
of.INFO_URL = of.REST_BASE + "/api/v3/exchangeInfo"
of.WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://stream.binance.com:9443/stream")

# ============================ CHoCH CONFIG ============================
SWING_STRENGTH = int(os.getenv("SWING_STRENGTH", "3"))
USE_CLOSE_BREAK = os.getenv("USE_CLOSE_BREAK", "1") == "1"
REQUIRE_DISPLACEMENT = os.getenv("REQUIRE_DISPLACEMENT", "0") == "1"
DISP_BREAK_ATR = float(os.getenv("DISP_BREAK_ATR", "0.25"))
DISP_BODY_ATR = float(os.getenv("DISP_BODY_ATR", "0.50"))
DISP_BODY_RATIO = float(os.getenv("DISP_BODY_RATIO", "0.60"))
RANGE_MODE = os.getenv("RANGE_MODE", "weak")
RANGE_TOLERANCE = float(os.getenv("RANGE_TOLERANCE", "0.25"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
STRICT_ONLY = os.getenv("STRICT_ONLY", "0") == "1"
CHOCH_WORKERS = int(os.getenv("CHOCH_WORKERS", "2"))

CHOCH_FRAMES: Dict[str, Dict[str, Any]] = {
    "1h": {"seconds": 3600, "interval": "1h", "initial_candles": 1000},
    "4h": {"seconds": 14400, "interval": "4h", "initial_candles": 1000},
    "1d": {"seconds": 86400, "interval": "1d", "initial_candles": 1000},
    "1w": {"seconds": 604800, "interval": "1w", "initial_candles": 1000},
}
CONTEXT_FRAMES = ["1h", "4h"]

# ============================ RULE: 30m+1h → 1h CHoCH ============================
of.CONFIG["orderflow_signals"]["rules"] = {
    "30m+1h": {
        "enabled": True,
        "legs": ["30m", "1h"],
        "trend_frame": "1h",            # <-- نیا: 1h CHoCH gate
        "cooldown_min": 45,
        "min_window_fill": 0.85,
        "cvd_pct": {"30m": 4.0, "1h": 3.0},
        "short_cvd_pct": 3.0,
        "obi": 0.03,
        "obi_source": "short",
        "edge_trigger": True,
        "rearm_frac": 0.50,
        "freshness_min": 0.0,
        "max_move_pct": {"30m": 2.5, "1h": 4.0},
        "max_vwap_dist_pct": 1.5,
    },
}


# ============================ REST helper ============================
async def _get_json(session: aiohttp.ClientSession, url: str, retries: int = 3) -> Any:
    last: Optional[Exception] = None
    for attempt in range(1, retries + 1):
        try:
            async with session.get(url) as resp:
                if resp.status in (418, 429):
                    await asyncio.sleep(2.0 * attempt)
                    continue
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            last = exc
            await asyncio.sleep(1.0 * attempt)
    raise RuntimeError(f"REST failed: {url}: {last}")


# ============================ CHoCH LOGIC (bug-fix سمیت) ============================
def is_swing_high(candles, i, n):
    if i - n < 0 or i + n >= len(candles):
        return False
    h = candles[i][2]
    for j in range(i - n, i + n + 1):
        if j != i and candles[j][2] >= h:
            return False
    return True


def is_swing_low(candles, i, n):
    if i - n < 0 or i + n >= len(candles):
        return False
    l = candles[i][3]
    for j in range(i - n, i + n + 1):
        if j != i and candles[j][3] <= l:
            return False
    return True


def was_high_broken_before(candles, level_idx, level_price, current_i, use_close=True):
    for k in range(level_idx + 1, current_i):
        val = candles[k][4] if use_close else candles[k][2]
        if val > level_price:
            return True
    return False


def was_low_broken_before(candles, level_idx, level_price, current_i, use_close=True):
    for k in range(level_idx + 1, current_i):
        val = candles[k][4] if use_close else candles[k][3]
        if val < level_price:
            return True
    return False


# ---- BUG FIX: پیچھے کی طرف تازہ ترین غیر ٹوٹا ہوا swing ----
def find_last_unbroken_high(candles, highs, current_i, use_close=True):
    for x in reversed(highs):
        if x["idx"] >= current_i:
            continue
        if not was_high_broken_before(candles, x["idx"], x["price"], current_i, use_close):
            out = dict(x)
            out["mode"] = "relaxed"
            return out
    return None


def find_last_unbroken_low(candles, lows, current_i, use_close=True):
    for x in reversed(lows):
        if x["idx"] >= current_i:
            continue
        if not was_low_broken_before(candles, x["idx"], x["price"], current_i, use_close):
            out = dict(x)
            out["mode"] = "relaxed"
            return out
    return None


def find_strict_higher_low(candles, highs, lows, current_i, use_close=True):
    if len(highs) < 2:
        return None
    h1, h2 = highs[-2], highs[-1]
    if h2["price"] <= h1["price"]:
        return None
    between = [x for x in lows if h1["idx"] < x["idx"] < h2["idx"]]
    if not between:
        return None
    low = min(between, key=lambda x: x["price"])
    if was_low_broken_before(candles, low["idx"], low["price"], current_i, use_close):
        return None
    out = dict(low)
    out["mode"] = "strict"
    return out


def find_strict_lower_high(candles, highs, lows, current_i, use_close=True):
    if len(lows) < 2:
        return None
    l1, l2 = lows[-2], lows[-1]
    if l2["price"] >= l1["price"]:
        return None
    between = [x for x in highs if l1["idx"] < x["idx"] < l2["idx"]]
    if not between:
        return None
    high = max(between, key=lambda x: x["price"])
    if was_high_broken_before(candles, high["idx"], high["price"], current_i, use_close):
        return None
    out = dict(high)
    out["mode"] = "strict"
    return out


def calculate_atr(candles, period=14):
    n = len(candles)
    if n < period + 1:
        return 0.0
    trs = []
    for i in range(n - period, n):
        h, l = candles[i][2], candles[i][3]
        pc = candles[i - 1][4]
        tr = max(h - l, abs(h - pc), abs(l - pc))
        trs.append(tr)
    return sum(trs) / float(period)


def break_quality_ok(candles, i, atr, require_displacement, disp_break_atr, disp_body_atr, disp_body_ratio):
    if not require_displacement:
        return True
    if i <= 0 or i >= len(candles):
        return False
    o, h, l, c = candles[i][1], candles[i][2], candles[i][3], candles[i][4]
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    if atr <= 0:
        return False
    move = abs(c - candles[i - 1][4])
    if move < disp_break_atr * atr:
        return False
    if body < disp_body_atr * atr:
        return False
    if body / rng < disp_body_ratio:
        return False
    return True


def in_range(candles, i, highs, lows, tolerance):
    if len(highs) < 2 or len(lows) < 2:
        return False
    h1, h2 = highs[-2]["price"], highs[-1]["price"]
    l1, l2 = lows[-2]["price"], lows[-1]["price"]
    box_top, box_bot = max(h1, h2), min(l1, l2)
    box_h = box_top - box_bot
    if box_h <= 0:
        return False
    return (abs(h1 - h2) <= tolerance * box_h) and (abs(l1 - l2) <= tolerance * box_h)


def detect_choch(candles, n=3, close_break=True, atr_values=None):
    events = []
    bos_events = []
    broken_high_idxs = set()
    broken_low_idxs = set()
    trend = None
    highs = []
    lows = []
    for i in range(2 * n, len(candles)):
        ts, o, h, l, c, v = candles[i]
        center = i - n
        if is_swing_high(candles, center, n):
            highs.append({"idx": center, "price": candles[center][2], "ts": candles[center][0]})
        if is_swing_low(candles, center, n):
            lows.append({"idx": center, "price": candles[center][3], "ts": candles[center][0]})
        up_price = c if close_break else h
        down_price = c if close_break else l
        last_high = find_last_unbroken_high(candles, highs, i, close_break)
        last_low = find_last_unbroken_low(candles, lows, i, close_break)
        atr = atr_values[i] if atr_values is not None and i < len(atr_values) else 0.0

        if trend is None:
            if last_high and up_price > last_high["price"]:
                trend = "bullish"
                lvl = last_high["idx"]
                highs = [x for x in highs if x["idx"] >= lvl]
                lows = [x for x in lows if x["idx"] >= lvl]
            elif last_low and down_price < last_low["price"]:
                trend = "bearish"
                lvl = last_low["idx"]
                highs = [x for x in highs if x["idx"] >= lvl]
                lows = [x for x in lows if x["idx"] >= lvl]
            continue

        if trend == "bullish":
            for low in lows:
                if low["idx"] in broken_low_idxs or low["idx"] >= i:
                    continue
                if down_price < low["price"] and not was_low_broken_before(candles, low["idx"], low["price"], i, close_break):
                    broken_low_idxs.add(low["idx"])
            protected_low = find_strict_higher_low(candles, highs, lows, i, close_break)
            if protected_low is None and not STRICT_ONLY:
                protected_low = last_low
            if protected_low and down_price < protected_low["price"]:
                if break_quality_ok(candles, i, atr, REQUIRE_DISPLACEMENT, DISP_BREAK_ATR, DISP_BODY_ATR, DISP_BODY_RATIO):
                    rng_state = in_range(candles, i, highs, lows, RANGE_TOLERANCE)
                    weak = (RANGE_MODE == "weak") and rng_state
                    skip = (RANGE_MODE == "skip") and rng_state
                    if not skip:
                        events.append({
                            "ts": ts, "type": "bearish", "level": protected_low["price"],
                            "close": c, "price_move": c - protected_low["price"],
                            "candle_range": (h - l), "strength": ("weak" if weak else "strong"),
                            "mode": protected_low.get("mode", "strict"),
                        })
                trend = "bearish"
                lvl = protected_low["idx"]
                highs = [x for x in highs if x["idx"] >= lvl]
                lows = [x for x in lows if x["idx"] >= lvl]
                continue
            if last_high and up_price > last_high["price"]:
                bos_events.append({"ts": ts, "type": "bullish_continuation", "level": last_high["price"], "close": c})
                continue

        if trend == "bearish":
            for high in highs:
                if high["idx"] in broken_high_idxs or high["idx"] >= i:
                    continue
                if up_price > high["price"] and not was_high_broken_before(candles, high["idx"], high["price"], i, close_break):
                    broken_high_idxs.add(high["idx"])
            protected_high = find_strict_lower_high(candles, highs, lows, i, close_break)
            if protected_high is None and not STRICT_ONLY:
                protected_high = last_high
            if protected_high and up_price > protected_high["price"]:
                if break_quality_ok(candles, i, atr, REQUIRE_DISPLACEMENT, DISP_BREAK_ATR, DISP_BODY_ATR, DISP_BODY_RATIO):
                    rng_state = in_range(candles, i, highs, lows, RANGE_TOLERANCE)
                    weak = (RANGE_MODE == "weak") and rng_state
                    skip = (RANGE_MODE == "skip") and rng_state
                    if not skip:
                        events.append({
                            "ts": ts, "type": "bullish", "level": protected_high["price"],
                            "close": c, "price_move": c - protected_high["price"],
                            "candle_range": (h - l), "strength": ("weak" if weak else "strong"),
                            "mode": protected_high.get("mode", "strict"),
                        })
                trend = "bullish"
                lvl = protected_high["idx"]
                highs = [x for x in highs if x["idx"] >= lvl]
                lows = [x for x in lows if x["idx"] >= lvl]
                continue
            if last_low and down_price < last_low["price"]:
                bos_events.append({"ts": ts, "type": "bearish_continuation", "level": last_low["price"], "close": c})
                continue

    return events, trend


def _sync_analyze(candles: List[List[float]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    atr_values = [0.0] * len(candles)
    for i in range(1, len(candles)):
        atr_values[i] = calculate_atr(candles[:i + 1], ATR_PERIOD)
    return detect_choch(candles, n=SWING_STRENGTH, close_break=USE_CLOSE_BREAK, atr_values=atr_values)


# ============================ OI POLLER (OKX primary) ============================
class OIPoller:
    def __init__(self) -> None:
        self.engine: Optional[Any] = None
        self.last: Dict[str, Dict[str, Any]] = {}
        self._hist: Dict[str, List[Tuple[float, float]]] = {}
        self._task: Optional[asyncio.Task] = None
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15))
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._session and not self._session.closed:
            await self._session.close()

    async def _loop(self) -> None:
        while True:
            try:
                await self._poll()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                of.log.debug("[OI] poll error: %s", exc)
            await asyncio.sleep(60.0)

    async def _poll(self) -> None:
        if self.engine is None or not self.engine.states or self._session is None:
            return
        now = time.time()
        for sym in list(self.engine.states.keys()):
            base = sym[:-4]
            oi: Optional[float] = None
            src = "okx"
            try:
                data = await _get_json(
                    self._session,
                    f"https://www.okx.com/api/v5/public/open-interest?instId={base}-USDT-SWAP", 2)
                oi = float(data["data"][0]["oi"])
            except Exception:  # noqa: BLE001
                src = "bybit"
                try:
                    data = await _get_json(
                        self._session,
                        f"https://api.bybit.com/v5/market/tickers?category=linear&symbol={base}USDT", 2)
                    oi = float(data["result"]["list"][0]["openInterest"])
                except Exception:  # noqa: BLE001
                    continue
            hist = self._hist.setdefault(sym, [])
            hist.append((now, oi))
            del hist[:-130]
            self.last[sym] = {"oi15": self._chg(sym, 900), "oi60": self._chg(sym, 3600), "src": src}
            await asyncio.sleep(0.05)

    def _chg(self, sym: str, secs: float) -> float:
        hist = self._hist.get(sym, [])
        if len(hist) < 2:
            return 0.0
        now_ts, now_oi = hist[-1]
        target = now_ts - secs
        ref = hist[0]
        for ts, oi in reversed(hist):
            if ts <= target:
                ref = (ts, oi)
                break
        if ref[1] <= 0:
            return 0.0
        return (now_oi - ref[1]) / ref[1] * 100.0


# ============================ ENGINE ============================
class ChochOrderflowEngine(of.Engine):
    def __init__(self) -> None:
        super().__init__()                       # base Engine() — کوئی cfg نہیں
        self.choch: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._choch_executor = ProcessPoolExecutor(max_workers=CHOCH_WORKERS)
        self._choch_busy: Dict[str, bool] = {}
        self._choch_session: Optional[aiohttp.ClientSession] = None
        self._choch_sem = asyncio.Semaphore(4)
        self.oi = OIPoller()
        self.oi.engine = self

    async def run(self) -> int:
        await self.oi.start()
        try:
            return await super().run()
        finally:
            await self.oi.stop()
            self._choch_executor.shutdown(wait=False)
            if self._choch_session is not None and not self._choch_session.closed:
                await self._choch_session.close()

    def _get_session(self) -> aiohttp.ClientSession:
        if self._choch_session is None or self._choch_session.closed:
            self._choch_session = aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"]))
        return self._choch_session

    def _choch_state(self, sym: str, frame: str) -> Dict[str, Any]:
        return self.choch.setdefault(sym, {}).setdefault(frame, {
            "candles": [], "events": [], "trend": None,
            "latest": None, "active": None, "next_due_ms": 0,
        })

    async def _choch_fetch(self, session: aiohttp.ClientSession, sym: str, frame: str, st: Dict[str, Any]) -> bool:
        tfc = CHOCH_FRAMES[frame]
        sec_ms = tfc["seconds"] * 1000
        now_ms = int(time.time() * 1000)
        candles = st["candles"]
        if candles:
            start = candles[-1][0] + sec_ms
        else:
            n0 = max(2, int(tfc.get("initial_candles", 1000)))
            start = now_ms - (n0 - 1) * sec_ms
        rows: List[Any] = []
        while True:
            url = (f"{of.REST_BASE}/api/v3/klines?symbol={sym}"
                   f"&interval={tfc['interval']}&startTime={start}&limit=1000")
            data = await _get_json(session, url)
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
            candles.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])])
            added = True
        return added

    async def _choch_update(self, session: aiohttp.ClientSession, sym: str, frame: str) -> None:
        st = self._choch_state(sym, frame)
        tfc = CHOCH_FRAMES[frame]
        now_ms = int(time.time() * 1000)
        if now_ms < st["next_due_ms"]:
            return
        prev_last = st["candles"][-1][0] if st["candles"] else None
        added = await self._choch_fetch(session, sym, frame, st)
        candles = st["candles"]
        if not candles:
            st["next_due_ms"] = now_ms + 30_000
            return
        if prev_last is not None and not added and candles[-1][0] == prev_last:
            st["next_due_ms"] = candles[-1][0] + tfc["seconds"] * 1000 + 5_000
            return
        if len(candles) < SWING_STRENGTH * 2 + 15:
            st["next_due_ms"] = now_ms + 60_000
            return
        loop = asyncio.get_running_loop()
        events, trend = await loop.run_in_executor(self._choch_executor, _sync_analyze, candles)
        st["events"] = events
        st["trend"] = trend
        st["latest"] = events[-1] if events else None
        st["active"] = st["latest"]
        st["next_due_ms"] = candles[-1][0] + tfc["seconds"] * 1000 + 5_000
        if events:
            ev = events[-1]
            of.log.info("[CHOCH] %s %s %s CHoCH | level %.8f close %.8f strength %s",
                        sym, frame, ev["type"].upper(), ev["level"], ev["close"], ev["strength"])

    async def _choch_refresh(self, sym: str) -> None:
        try:
            async with self._choch_sem:
                session = self._get_session()
                for frame in CHOCH_FRAMES:
                    try:
                        await self._choch_update(session, sym, frame)
                    except Exception as exc:  # noqa: BLE001
                        of.log.debug("[CHOCH] %s %s: %s", sym, frame, exc)
                    await asyncio.sleep(0.05)
        finally:
            self._choch_busy[sym] = False

    # ---------- base hook: sync per-symbol eval ----------
    def _evaluate(self, sym: str, st: Any, now: float) -> None:
        now_ms = int(now * 1000)
        if not self._choch_busy.get(sym, False):
            if any(now_ms >= self._choch_state(sym, fr)["next_due_ms"] for fr in CHOCH_FRAMES):
                self._choch_busy[sym] = True
                self._spawn(self._choch_refresh(sym))
        super()._evaluate(sym, st, now)

    # ---------- base hook: trend gate (1h CHoCH) ----------
    def _trend_allows(self, sym: str, rname: str, side: str, rule: Dict[str, Any]) -> Tuple[bool, str]:
        frame = rule.get("trend_frame")
        if not frame:
            return True, ""
        trend = self._choch_state(sym, frame).get("trend")
        if trend is None:
            return False, f"no_{frame}_trend"
        want = "bullish" if side == "BUY" else "bearish"
        if trend != want:
            return False, f"{side.lower()}_against_{frame}_{trend}"
        return True, ""

    # ---------- context helpers ----------
    def _format_choch_event(self, ev: Dict[str, Any], frame: str, now: float) -> str:
        age_h = (now - float(ev["ts"]) / 1000.0) / 3600.0
        return (f"{frame} {ev['type'].capitalize()} CHoCH ({age_h:.1f}h ago, "
                f"level {ev['level']:g}, close {ev['close']:g}, {ev['strength']})")

    def _gate_line(self, sym: str, side: str, now: float) -> str:
        hits: List[str] = []
        want = "bullish" if side == "BUY" else "bearish"
        for fr in CONTEXT_FRAMES:
            ev = self._choch_state(sym, fr).get("active")
            if not ev:
                continue
            if ev["type"] == want:
                hits.append(self._format_choch_event(ev, fr, now))
            else:
                hits.append(f"[opposite] {self._format_choch_event(ev, fr, now)}")
        return "; ".join(hits) if hits else "none"

    @staticmethod
    def _oi_tag(oi60: float, move: float) -> str:
        thr = 0.3
        if oi60 > thr:
            return "NEW LONGS (strong)" if move >= 0 else "NEW SHORTS (strong)"
        if oi60 < -thr:
            return "SHORT COVERING (weak)" if move >= 0 else "LONG CLOSING (weak)"
        return "flat"

    # ---------- base hook: alert context lines ----------
    def _signal_context_lines(self, sym: str, rname: str, side: str, now: float) -> List[str]:
        lines: List[str] = []
        st = self.states.get(sym)
        oi = self.oi.last.get(sym)
        if oi and st is not None:
            w = st.windows.get("1h")
            move = w.move_pct(st.last_price) if w is not None else 0.0
            lines.append(f"OI ({oi['src']}): 15m {oi['oi15']:+.2f}% | 1h {oi['oi60']:+.2f}% | "
                         f"funding n/a | {self._oi_tag(oi['oi60'], move)}")
        rule = of.CONFIG["orderflow_signals"]["rules"].get(rname, {})
        frame = rule.get("trend_frame", "")
        if frame:
            tr = self._choch_state(sym, frame).get("trend")
            if tr:
                lines.append(f"Trend ({frame.upper()}): {tr.capitalize()}")
        lines.append(f"CHoCH context: {self._gate_line(sym, side, now)}")
        htfs = []
        for fr in ("1d", "1w"):
            tr = self._choch_state(sym, fr).get("trend")
            if tr:
                htfs.append(f"{fr}: {tr.capitalize()}")
        if htfs:
            lines.append("HTF context: " + " | ".join(htfs))
        return lines

    def _session_extra_lines(self, now: float, sessions: List[str]) -> List[str]:
        return []


# ============================ MAIN ============================
def main() -> None:
    load_dotenv()
    of.setup_logging()
    of.log.info("[BOOT] orderflow+choch live (1h CHoCH gate + 30m/1h flow)")
    code = 0
    try:
        code = asyncio.run(ChochOrderflowEngine().run())
    except KeyboardInterrupt:
        of.log.info("[ENGINE] Interrupted (graceful stop)")
    sys.exit(code)


if __name__ == "__main__":
    main()
