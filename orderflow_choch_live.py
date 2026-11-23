#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
ORDERFLOW + CHoCH CONFLUENCE ALERTS (LIVE) — Binance Spot USDT pairs.

Data:
  - WebSocket: {symbol}@aggTrade (taker-side flow) and {symbol}@depth{levels}@{ms} (order book)
  - REST klines for CHoCH structure on 1h / 4h / 1d / 1w

Signals (orderflow):
  - CVD% per window (30m / 1h / 4h) vs threshold, aligned across windows
  - OBI (order book imbalance) aligned with flow direction
  - 15m CVD confirmation
  - Cooldowns + daily caps to avoid spam

CHoCH (trend filter):
  - Swing highs/lows with strength N, protected levels, CHoCH events with strength
  - Optional displacement filter and range-box skip/weak tagging
  - Trend-following gate: BUY only when CHoCH trend bullish, SELL only when bearish
  - Each timeframe keeps its own independent trend (no cross-TF invalidation)

Alerts:
  - ntfy topic push with full context (flow, OBI, footprint, CHoCH context, sessions)
"""

import asyncio
import json
import math
import os
import random
import time
from collections import deque
from concurrent.futures import ProcessPoolExecutor
from datetime import datetime, timezone
from typing import Any, Deque, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

import orderflow_paper as of

load_dotenv()

# ============================ CONFIG ============================
CONFIG: Dict[str, Any] = {
    "ws_url": os.getenv("BINANCE_WS_URL", "wss://stream.binance.com:9443/stream"),
    "rest_base": os.getenv("BINANCE_REST_BASE", "https://api.binance.com"),
    "ntfy_topic": os.getenv("NTFY_TOPIC", ""),
    "ntfy_url": os.getenv("NTFY_URL", "https://ntfy.sh"),

    "symbols": [s.strip().upper() for s in os.getenv("SYMBOLS", "").split(",") if s.strip()],
    "quote": os.getenv("QUOTE", "USDT"),
    "max_symbols": int(os.getenv("MAX_SYMBOLS", "30")),
    "min_quote_volume_24h": float(os.getenv("MIN_QUOTE_VOLUME_24H", "10000000")),

    "depth_levels": int(os.getenv("DEPTH_LEVELS", "20")),
    "depth_ms": os.getenv("DEPTH_MS", "100ms"),

    "eval_interval_sec": float(os.getenv("EVAL_INTERVAL_SEC", "2.0")),
    "stale_seconds": float(os.getenv("STALE_SECONDS", "30")),
    "spread_max_bps": float(os.getenv("SPREAD_MAX_BPS", "25")),

    "reconnect_base_sec": float(os.getenv("RECONNECT_BASE_SEC", "2.0")),
    "reconnect_max_sec": float(os.getenv("RECONNECT_MAX_SEC", "30.0")),
    "reconnect_wipe_after_sec": float(os.getenv("RECONNECT_WIPE_AFTER_SEC", "120")),

    "rest_timeout_sec": float(os.getenv("REST_TIMEOUT_SEC", "10")),
    "rest_retries": int(os.getenv("REST_RETRIES", "3")),

    "process_workers": int(os.getenv("PROCESS_WORKERS", "2")),

    "state_path": os.getenv("STATE_PATH", "state_orderflow_choch.json"),
    "state_save_sec": float(os.getenv("STATE_SAVE_SEC", "60")),

    "rest_schedule_enabled": os.getenv("REST_SCHEDULE_ENABLED", "1") == "1",
    "rest_utc_hours": [int(x) for x in os.getenv("REST_UTC_HOURS", "2,3,4,5,6,7,8,9,10,11,18,19,20,21,22,23").split(",") if x.strip() != ""],
    "rest_min_hours": float(os.getenv("REST_MIN_HOURS", "2.0")),
    "rest_max_hours": float(os.getenv("REST_MAX_HOURS", "4.0")),

    "session_alerts_enabled": os.getenv("SESSION_ALERTS_ENABLED", "1") == "1",
    "session_alert_cooldown_min": float(os.getenv("SESSION_ALERT_COOLDOWN_MIN", "240")),
    "sessions_utc": {
        "Asia": (1, 10),
        "London": (7, 16),
        "NewYork": (12, 21),
    },

    "health_alert_cooldown_min": float(os.getenv("HEALTH_ALERT_COOLDOWN_MIN", "15")),
    "diag_log_sec": float(os.getenv("DIAG_LOG_SEC", "1800")),

    "peak_hours_utc": [[7, 8], [12, 16]],
    "offpeak_eval_interval_sec": float(os.getenv("OFFPEAK_EVAL_INTERVAL_SEC", "4.0")),
}

# ============================ CHoCH CONFIG ============================
SWING_STRENGTH = int(os.getenv("SWING_STRENGTH", "3"))
USE_CLOSE_BREAK = os.getenv("USE_CLOSE_BREAK", "1") == "1"
REQUIRE_DISPLACEMENT = os.getenv("REQUIRE_DISPLACEMENT", "0") == "1"
DISP_BREAK_ATR = float(os.getenv("DISP_BREAK_ATR", "0.25"))
DISP_BODY_ATR = float(os.getenv("DISP_BODY_ATR", "0.50"))
DISP_BODY_RATIO = float(os.getenv("DISP_BODY_RATIO", "0.60"))
RANGE_MODE = os.getenv("RANGE_MODE", "weak")          # weak | skip | off
RANGE_TOLERANCE = float(os.getenv("RANGE_TOLERANCE", "0.25"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))
STRICT_ONLY = os.getenv("STRICT_ONLY", "0") == "1"
CHOCH_WORKERS = int(os.getenv("CHOCH_WORKERS", "2"))

# ============================ SYMBOL WHITELIST ============================
SYMBOL_WHITELIST = [
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT", "ADAUSDT", "DOGEUSDT", "AVAXUSDT",
    "LINKUSDT", "DOTUSDT", "MATICUSDT", "LTCUSDT", "BCHUSDT", "UNIUSDT", "ATOMUSDT", "ETCUSDT",
    "XLMUSDT", "NEARUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "FILUSDT", "INJUSDT", "SUIUSDT",
    "SEIUSDT", "TIAUSDT", "RUNEUSDT", "AAVEUSDT", "MKRUSDT", "SNXUSDT",
]

# ============================ RULES CONFIG ============================
RULES: Dict[str, Any] = {
    "30m+1h": {
        "enabled": True,
        "legs": ["30m", "1h"],
        "cvd_pct": {"30m": 4.0, "1h": 3.0},
        "short_cvd_pct": 2.0,
        "obi": 0.05,
        "obi_source": "short",
        "cooldown_min": 45,
        "max_alerts_per_symbol_day": 6,
    },
    "1h_daily": {
        "enabled": True,
        "legs": ["1h"],
        "cvd_pct": {"1h": 2.5},
        "short_cvd_pct": 2.0,
        "obi": 0.02,
        "obi_source": "short",
        "trend_frame": "1d",
        "cooldown_min": 90,
        "max_alerts_per_symbol_day": 4,
    },
    "4h_daily": {
        "enabled": True,
        "legs": ["4h"],
        "cvd_pct": {"4h": 8.0},
        "short_cvd_pct": 4.0,
        "obi": 0.05,
        "obi_source": "4h",
        "trend_frame": "1d",
        "cooldown_min": 180,
        "max_alerts_per_symbol_day": 3,
    },
    "4h_weekly": {
        "enabled": True,
        "legs": ["4h"],
        "cvd_pct": {"4h": 8.0},
        "short_cvd_pct": 4.0,
        "obi": 0.05,
        "obi_source": "4h",
        "trend_frame": "1w",
        "cooldown_min": 240,
        "max_alerts_per_symbol_day": 2,
    },
}

MERGE: Dict[str, Any] = {
    "timeframes": {
        "30m": 1800,
        "1h": 3600,
        "4h": 14400,
    },
    "short_window_sec": 900,
    "whale_mult": 20.0,
    "retail_mult": 2.0,
    "min_window_fill": {"30m+1h": 0.85, "1h_daily": 0.50, "4h_daily": 0.50, "4h_weekly": 0.50},
    "min_trades": 50,
    "min_volume_usdt": 30000.0,
    "footprint_row_bps": 3.0,
    "footprint_min_frac": 0.10,
    "footprint_imbalance_ratio": 2.0,
    "footprint_min_stacked": 1,
    "require_footprint": False,
    "max_move_pct": {"30m+1h": {"30m": 2.5, "1h": 4.0}, "1h_daily": {}, "4h_daily": {}, "4h_weekly": {}},
    "max_vwap_dist_pct": {"30m+1h": 1.5, "1h_daily": 1e9, "4h_daily": 1e9, "4h_weekly": 1e9},
    "freshness_min": {"30m+1h": 0.0, "1h_daily": 0.0, "4h_daily": 0.0, "4h_weekly": 0.0},
    "edge_trigger": True,
    "rearm_frac": 0.5,
    "choch_timeframes": {
        "1h": {"seconds": 3600, "interval": "1h", "initial_candles": 1000},
        "4h": {"seconds": 14400, "interval": "4h", "initial_candles": 1000},
        "1d": {"seconds": 86400, "interval": "1d", "initial_candles": 1000},
        "1w": {"seconds": 604800, "interval": "1w", "initial_candles": 1000},
    },
    "gate_frames": ["1h", "4h"],
    "flow_gated_frames": ["1h"],
    "flow_gate_cvd": 1.0,
}


# ============================ REST SESSION ============================
_rest_session: Optional[aiohttp.ClientSession] = None


async def _rest_session_get() -> aiohttp.ClientSession:
    global _rest_session
    if _rest_session is None or _rest_session.closed:
        timeout = aiohttp.ClientTimeout(total=CONFIG["rest_timeout_sec"])
        _rest_session = aiohttp.ClientSession(timeout=timeout)
    return _rest_session


async def _rest_get_json(url: str) -> Any:
    sess = await _rest_session_get()
    last_exc: Optional[Exception] = None
    for attempt in range(CONFIG["rest_retries"]):
        try:
            async with sess.get(url) as resp:
                if resp.status == 429 or resp.status == 418:
                    await asyncio.sleep(2.0 * (attempt + 1))
                    continue
                resp.raise_for_status()
                return await resp.json(content_type=None)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            await asyncio.sleep(1.0 * (attempt + 1))
    raise RuntimeError(f"REST failed: {url}: {last_exc}")


# ============================ SYMBOL SELECTOR ============================
class SymbolSelector:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg
        self.symbols: List[str] = []
        self._last_select_ts = 0.0

    async def _get(self, session: aiohttp.ClientSession, url: str) -> Any:
        last_exc: Optional[Exception] = None
        for attempt in range(self.cfg["rest_retries"]):
            try:
                async with session.get(url) as resp:
                    if resp.status == 429 or resp.status == 418:
                        await asyncio.sleep(2.0 * (attempt + 1))
                        continue
                    resp.raise_for_status()
                    return await resp.json(content_type=None)
            except Exception as exc:  # noqa: BLE001
                last_exc = exc
                await asyncio.sleep(1.0 * (attempt + 1))
        raise RuntimeError(f"REST failed: {url}: {last_exc}")

    async def select(self) -> List[str]:
        url = f"{self.cfg['rest_base']}/api/v3/ticker/24hr"
        timeout = aiohttp.ClientTimeout(total=self.cfg["rest_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            data = await self._get(session, url)
        rows = [r for r in data if isinstance(r, dict) and r.get("symbol", "").endswith(self.cfg["quote"])]
        picked: List[Tuple[float, str]] = []
        for r in rows:
            sym = r["symbol"]
            base = sym[: -len(self.cfg["quote"])]
            if base in ("UP", "DOWN", "BULL", "BEAR"):
                continue
            qv = float(r.get("quoteVolume", 0.0) or 0.0)
            if qv < self.cfg["min_quote_volume_24h"]:
                continue
            picked.append((qv, sym))
        picked.sort(key=lambda x: x[0], reverse=True)
        out = [s for _, s in picked[: self.cfg["max_symbols"]]]
        if self.cfg["symbols"]:
            wl = [s if s.endswith(self.cfg["quote"]) else s + self.cfg["quote"] for s in self.cfg["symbols"]]
            out = [s for s in wl if s in {x for _, x in picked}] or out
        self.symbols = out
        self._last_select_ts = time.time()
        return out


# ============================ OI POLLER ============================
class OIPoller:
    def __init__(self, cfg: Dict[str, Any]) -> None:
        self.cfg = cfg
        self.last: Dict[str, Dict[str, Any]] = {}
        self._lock = asyncio.Lock()
        self._task: Optional[asyncio.Task] = None
        self.enabled = True

    async def start(self) -> None:
        self._task = asyncio.create_task(self._loop())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass

    async def _loop(self) -> None:
        while True:
            try:
                await self._poll_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
                of.log.warning("[OI] poll error: %s", exc)
            await asyncio.sleep(60.0)

    async def _poll_once(self) -> None:
        if not self.enabled:
            return
        timeout = aiohttp.ClientTimeout(total=self.cfg["rest_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for sym in list(self.last.keys()) or []:
                pass
        # poll for symbols we track
        syms = list(getattr(self, "_symbols", []) or [])
        if not syms:
            return
        timeout = aiohttp.ClientTimeout(total=self.cfg["rest_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for sym in syms:
                try:
                    url = f"https://fapi.binance.com/futures/data/openInterestHist?symbol={sym}&period=5m&limit=2"
                    data = await _rest_get_json(url)
                    if isinstance(data, list) and len(data) >= 2:
                        a, b = float(data[-2]["sumOpenInterestValue"]), float(data[-1]["sumOpenInterestValue"])
                        chg = (b - a) / a * 100.0 if a else 0.0
                        async with self._lock:
                            self.last[sym] = {"oi_chg_5m": chg, "ts": time.time()}
                except Exception:  # noqa: BLE001
                    continue


# ============================ HEALTH MONITOR ============================
class HealthMonitor:
    def __init__(self, cfg: Dict[str, Any], alerter: Any) -> None:
        self.cfg = cfg
        self.alerter = alerter
        self.started = time.time()
        self.msg_count = 0
        self.eval_cycles = 0
        self.eval_errors = 0
        self.signals_sent = 0
        self.rest_hours_total = 0.0
        self._last_alert_ts: Dict[str, float] = {}
        self._last_diag_ts = 0.0
        self.state = "ACTIVE"

    def tick_msg(self, n: int = 1) -> None:
        self.msg_count += n

    def tick_eval(self, err: bool = False) -> None:
        self.eval_cycles += 1
        if err:
            self.eval_errors += 1

    def tick_signal(self) -> None:
        self.signals_sent += 1

    def add_rest_hours(self, h: float) -> None:
        self.rest_hours_total += h

    def _rate(self) -> float:
        up = max(1e-9, time.time() - self.started)
        return self.msg_count / up

    async def maybe_alert(self, kind: str, text: str) -> None:
        now = time.time()
        last = self._last_alert_ts.get(kind, 0.0)
        if now - last < self.cfg["health_alert_cooldown_min"] * 60.0:
            return
        self._last_alert_ts[kind] = now
        try:
            await self.alerter.send(f"[HEALTH] {kind}", text, priority="high", tags="rotating_light")
        except Exception:  # noqa: BLE001
            pass

    async def check(self, connected: bool, resting: bool) -> None:
        now = time.time()
        rate = self._rate()
        if not resting:
            if not connected:
                await self.maybe_alert("disconnected", "WebSocket disconnected.")
            if rate < 1.0 and (now - self.started) > 120:
                await self.maybe_alert("low_rate", f"Message rate low: {rate:.2f}/s")
        if now - self._last_diag_ts >= self.cfg["diag_log_sec"]:
            self._last_diag_ts = now
            of.log.info(
                "[DIAG] health: uptime=%.1fh msg_rate=%.1f/s signals=%d rest=%.1fh eval_cycles=%d eval_errors=%d | state=%s",
                (now - self.started) / 3600.0, rate, self.signals_sent, self.rest_hours_total,
                self.eval_cycles, self.eval_errors, self.state,
            )


# ============================ CHoCH LOGIC ============================
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


# ---- BUG FIX: تازہ ترین "غير ٹوٹا ہوا" swing پیچھے کی طرف ڈھونڈا جاتا ہے ----
# پہلے صرف highs[-1]/lows[-1] دیکھا جاتا تھا؛ وہ ٹوٹ چکا ہوتا تو None ملتا اور
# BOS کے بعد کوڈ نیا flip پکڑ ہی نہیں سکتا تھا (اندھا وقفہ)۔ اب صرف structure
# فیصلہ کرتی ہے — نہ وقت، نہ کینڈلز کی گنتی۔
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


async def analyze_choch(candles):
    atr_values = [0.0] * len(candles)
    for i in range(1, len(candles)):
        atr_values[i] = calculate_atr(candles[:i + 1], ATR_PERIOD)
    events, trend = detect_choch(
        candles, n=SWING_STRENGTH, close_break=USE_CLOSE_BREAK, atr_values=atr_values)
    return events, trend


# ============================ CHOCH ENGINE ============================
class ChochOrderflowEngine(of.OrderFlowEngine):
    def __init__(self, cfg: Dict[str, Any]) -> None:
        super().__init__(cfg)
        self.choch: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._choch_executor = ProcessPoolExecutor(max_workers=CHOCH_WORKERS)
        self.selector = SymbolSelector(cfg)
        self.oi = OIPoller(cfg)
        self.health = HealthMonitor(cfg, self.alerter)
        self._resting = False
        self._rest_until = 0.0
        self._session_alert_ts: Dict[str, float] = {}
        self._last_session: Optional[str] = None

    # ---------- CHoCH state helpers ----------
    def _choch_state(self, sym: str, frame: str) -> Dict[str, Any]:
        return self.choch.setdefault(sym, {}).setdefault(frame, {
            "candles": [], "events": [], "trend": None,
            "latest": None, "active": None, "next_due_ms": 0,
        })

    async def _choch_fetch(self, session: aiohttp.ClientSession, sym: str, frame: str, st: Dict[str, Any]) -> bool:
        tfc = MERGE["choch_timeframes"][frame]
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
            url = (f"{self.cfg['rest_base']}/api/v3/klines?symbol={sym}"
                   f"&interval={tfc['interval']}&startTime={start}&limit=1000")
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
            candles.append([ts, float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[5])])
            added = True
        return added

    async def _run_analysis(self, candles: List[List[float]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(self._choch_executor, _sync_analyze, candles)

    async def _choch_update(self, session: aiohttp.ClientSession, sym: str, frame: str) -> None:
        st = self._choch_state(sym, frame)
        tfc = MERGE["choch_timeframes"][frame]
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
        events, trend = await self._run_analysis(candles)
        st["events"] = events
        st["trend"] = trend
        st["latest"] = events[-1] if events else None
        st["active"] = st["latest"]
        st["next_due_ms"] = candles[-1][0] + tfc["seconds"] * 1000 + 5_000
        if events:
            ev = events[-1]
            of.log.info("[CHOCH] %s %s %s CHoCH | level %.8f close %.8f strength %s",
                        sym, frame, ev["type"].upper(), ev["level"], ev["close"], ev["strength"])

    def _active_choch(self, sym: str, side: str, frame: str) -> Optional[Dict[str, Any]]:
        st = self._choch_state(sym, frame)
        ev = st.get("active")
        if not ev:
            return None
        want = "bullish" if side == "BUY" else "bearish"
        return ev if ev["type"] == want else None

    def _any_choch(self, sym: str, frame: str) -> Optional[Dict[str, Any]]:
        st = self._choch_state(sym, frame)
        return st.get("active")

    def _flow_confirms_frame(self, sym: str, frame: str, ev: Dict[str, Any]) -> bool:
        if frame not in MERGE["flow_gated_frames"]:
            return True
        st = self.states.get(sym)
        if not st:
            return True
        w = st.windows.get(frame)
        if not w:
            return True
        cvd = w.cvd_pct
        thr = MERGE["flow_gate_cvd"]
        if ev["type"] == "bullish":
            return cvd >= thr
        return cvd <= -thr

    def _format_choch_event(self, ev: Dict[str, Any], frame: str, now: float) -> str:
        age_h = (now - float(ev["ts"]) / 1000.0) / 3600.0
        return (f"{frame} {ev['type'].capitalize()} CHoCH ({age_h:.1f}h ago, "
                f"level {ev['level']:g}, close {ev['close']:g}, {ev['strength']})")

    def _gate_line(self, sym: str, side: str, now: float) -> str:
        hits: List[str] = []
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

    def _trend_allows(self, sym: str, side: str, rule: Dict[str, Any]) -> Tuple[bool, str]:
        frame = rule.get("trend_frame")
        if not frame:
            return True, ""
        st = self._choch_state(sym, frame)
        trend = st.get("trend")
        if trend is None:
            return False, f"no_{frame}_trend"
        want = "bullish" if side == "BUY" else "bearish"
        if trend != want:
            return False, f"trend_{side.lower()}_against_{frame}_{trend}"
        return True, ""

    # ---------- sessions / rest ----------
    def _current_session(self, now_utc: datetime) -> Optional[str]:
        h = now_utc.hour
        for name, (a, b) in self.cfg["sessions_utc"].items():
            if a <= h < b:
                return name
        return None

    def _in_peak(self, now_utc: datetime) -> bool:
        h = now_utc.hour
        for a, b in self.cfg["peak_hours_utc"]:
            if a <= h < b:
                return True
        return False

    async def _maybe_rest(self, now: float) -> None:
        if not self.cfg["rest_schedule_enabled"]:
            return
        now_utc = datetime.fromtimestamp(now, tz=timezone.utc)
        if self._resting:
            if now >= self._rest_until:
                self._resting = False
                of.log.info("[REST] resumed")
            return
        if now_utc.hour in self.cfg["rest_utc_hours"] and not self._in_peak(now_utc):
            if random.random() < 0.02:
                hrs = random.uniform(self.cfg["rest_min_hours"], self.cfg["rest_max_hours"])
                self._resting = True
                self._rest_until = now + hrs * 3600.0
                self.health.add_rest_hours(hrs)
                of.log.info("[REST] sleeping %.2fh", hrs)

    # ---------- main loop overrides ----------
    async def run(self) -> None:
        self.symbols = await self.selector.select()
        of.log.info("[ENGINE] symbols=%d %s", len(self.symbols), ",".join(self.symbols[:12]))
        self.oi._symbols = self.symbols
        await self.oi.start()
        timeout = aiohttp.ClientTimeout(total=self.cfg["rest_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for sym in self.symbols:
                for frame in MERGE["choch_timeframes"]:
                    try:
                        await self._choch_update(session, sym, frame)
                    except Exception as exc:  # noqa: BLE001
                        of.log.warning("[CHOCH] init %s %s failed: %s", sym, frame, exc)
        await super().run()

    async def _evaluate(self, sym: str, now: float) -> None:
        self.health.tick_eval()
        await self._maybe_rest(now)
        if self._resting:
            return
        now_utc = datetime.fromtimestamp(now, tz=timezone.utc)
        sess = self._current_session(now_utc)
        if sess and sess != self._last_session and self.cfg["session_alerts_enabled"]:
            self._last_session = sess
            last = self._session_alert_ts.get(sess, 0.0)
            if now - last >= self.cfg["session_alert_cooldown_min"] * 60.0:
                self._session_alert_ts[sess] = now
                try:
                    await self.alerter.send(f"[SESSION] {sess} open", f"{sess} session started (UTC hour {now_utc.hour}).",
                                            priority="default", tags="clock")
                except Exception:  # noqa: BLE001
                    pass
        timeout = aiohttp.ClientTimeout(total=self.cfg["rest_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            for frame in MERGE["choch_timeframes"]:
                try:
                    await self._choch_update(session, sym, frame)
                except Exception as exc:  # noqa: BLE001
                    of.log.debug("[CHOCH] %s %s: %s", sym, frame, exc)
        await super()._evaluate(sym, now)

    def _signal_context_lines(self, sym: str, side: str, rule_name: str, rule: Dict[str, Any],
                              st: Any, now: float) -> List[str]:
        lines = super()._signal_context_lines(sym, side, rule_name, rule, st, now)
        lines.append(f"CHoCH context: {self._gate_line(sym, side, now)}")
        htfs = []
        for fr in ("1d", "1w"):
            stf = self._choch_state(sym, fr)
            tr = stf.get("trend")
            if tr:
                htfs.append(f"{fr}: {tr.capitalize()}")
        if htfs:
            lines.append("HTF context: " + " | ".join(htfs))
        oi = self.oi.last.get(sym)
        if oi:
            lines.append(f"OI (5m): {oi['oi_chg_5m']:+.2f}%")
        return lines

    def _trend_gate(self, sym: str, side: str, rule: Dict[str, Any]) -> Tuple[bool, str]:
        return self._trend_allows(sym, side, rule)


def _sync_analyze(candles: List[List[float]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    atr_values = [0.0] * len(candles)
    for i in range(1, len(candles)):
        atr_values[i] = calculate_atr(candles[:i + 1], ATR_PERIOD)
    return detect_choch(candles, n=SWING_STRENGTH, close_break=USE_CLOSE_BREAK, atr_values=atr_values)


# ============================ MAIN ============================
async def _amain() -> None:
    eng = ChochOrderflowEngine(CONFIG)
    try:
        await eng.run()
    finally:
        await eng.oi.stop()
        eng._choch_executor.shutdown(wait=False)
        global _rest_session
        if _rest_session and not _rest_session.closed:
            await _rest_session.close()


def main() -> None:
    of.log.info("[BOOT] orderflow+choch live starting")
    asyncio.run(_amain())


if __name__ == "__main__":
    main()
