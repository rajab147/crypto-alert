#!/usr/bin/env python3
"""
orderflow_alerts_1h4h_v7.py - LIVE order-flow BUY/SELL SIGNAL ALERTS (Binance Spot 1h/4h).
v7 CHANGES (swing-based divergence + lower-TF close trigger):
* SWING-BASED DIVERGENCE: no fixed candle count. Divergence is always between TWO swings:
  swing A = last confirmed fractal pivot, swing B = the newest swing (confirmed fractal OR the
  still-forming extreme). Distance between A and B is free (only min/max sanity bounds).
* FORMING SWING: swing B may be the live extreme (fractal_right=1 + use_forming_swing), so the
  signal is no longer 2+ candles late.
* TRIGGER ON LOWER-TF CLOSE (MTF as you suggested):
  4h divergence -> alert only when a 30m candle CLOSES and confirms the reversal
  1h divergence -> alert only when a 15m candle CLOSES and confirms the reversal
  Confirmation = candle colour + candle delta (taker buy-sell) + close beyond previous close.
* REGIME FILTER softened: it no longer kills regular (reversal) divergences, which by nature
  appear in a falling/rising market. Default mode "hidden_only".
* MARKET STRUCTURE (BOS/CHoCH) is intentionally NOT used anywhere.
* HISTORY FIX: forming kline is now included in backfill (live candle used to miss its first
  part -> wrong low/high), CVD-at-extreme taken from the base bar that printed the extreme,
  and trigger candles are seeded from klines so the first close after start works.
* FLOW SIGNALS: thresholds relaxed, per-timeframe MTF frame list (fewer missed signals).
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import signal
import sys
import time
from collections import deque
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from typing import Any

import aiohttp
import websockets
from dotenv import load_dotenv

CONFIG: dict[str, Any] = {
    "max_symbols": 50,
    "max_price_change_24h_pct": 25.0,
    "cvd_threshold": 18.0,               # v7: was 25 (signals came after the move)
    "obi_threshold": 0.15,               # v7: was 0.20
    "obi_window_average": True,
    "spread_max_bps": 15.0,
    "eval_interval_sec": 3,
    "warmup_seconds": 5,
    "bucket_seconds": 5,
    "min_window_fill": 0.75,
    "diag_interval_min": 30,
    "div_history_candles": 48,
    "short_window_seconds": 900,
    "short_cvd_threshold": 15.0,         # v7: was 20
    "short_max_move_pct": 0.5,
    "fresh_max_age_sec": 900,
    "mtf_cvd_min_abs_pct": 4.0,
    "mtf_obi_min_abs": 0.04,
    "flow_signals": {"enabled": True},   # set False to receive divergence alerts only
    "adaptive": {
        "enabled": True, "sample_interval_sec": 30,
        "high_vol_move15": 1.0, "low_vol_move15": 0.25,
        "high_mult": 1.25, "low_mult": 0.80,
    },
    "health": {"silent_limit_sec": 120, "check_interval_sec": 60, "alert_on_silent": True},
    "divergence": {
        "fractal_left": 2,                 # candles needed left of a confirmed swing
        "fractal_right": 1,                # candles needed right (1 = earlier than v6's 2)
        "use_forming_swing": True,         # live extreme can be swing B (earliest signal)
        "min_swing_gap_candles": 2,        # swing A -> swing B sanity bounds (NOT a fixed count)
        "max_swing_gap_candles": 40,
        "regime_mode": "hidden_only",      # "off" | "hidden_only" | "all"
        "regime_tf": "4h",
        "regime_oppose_cvd_pct": 8.0,
        "trigger_max_delay_sec": 180,       # trigger candle must be evaluated this soon after close
        "trigger_require_color": False,    # BUY: close>open, SELL: close<open
        "trigger_require_delta": False,    # BUY: taker buy>sell inside trigger candle
        "trigger_break": "off",            # "off" | "prev_close" | "prev_high" (low for SELL)
    },
    "footprint": {
        "row_bps": 3.0, "keep_candles": 12, "min_rows": 5, "zone_rows": 3,
        "absorb_min_frac": 0.10, "imbalance_ratio": 3.0, "imbalance_min_frac": 0.004,
        "min_stacked": 2, "min_confirms": 1, "flow_zone_rows": 6, "flow_min_ratio": 1.5,
        "required": False,
    },
    "timeframes": {
        "1h": {
            "seconds": 3600, "cooldown_min": 45,
            "div_candle_seconds": [3600], "div_base_seconds": 60,
            "confirm_candle_seconds": 900,        # 1h divergence -> 15m candle close trigger
            "max_recent_move_pct": 1.2,
            "div_max_move_since_swing_pct": 1.5,
            "div_min_price_diff_pct": 0.05,
            "max_swing_age_sec": 18_000,
            "mtf_frames": ["1h", "15m"],
        },
        "4h": {
            "seconds": 14400, "cooldown_min": 180,
            "div_candle_seconds": [14400], "div_base_seconds": 300,
            "confirm_candle_seconds": 1800,       # 4h divergence -> 30m candle close trigger
            "max_recent_move_pct": 2.0,
            "div_max_move_since_swing_pct": 3.0,
            "div_min_price_diff_pct": 0.08,
            "max_swing_age_sec": 72_000,
            "mtf_frames": ["4h", "1h"],
        },
    },
    "max_alerts_per_day": 300,
    "depth_levels": 10,
    "stale_seconds": 10,
    "ws_recv_timeout_sec": 30,
    "reconnect_wipe_after_sec": 120,
    "reconnect_backoff_max_sec": 60,
    "reconnect_fail_alert_after": 3,
    "state_save_interval_sec": 60,
    "http_timeout_sec": 60,
    "http_retries": 3,
    "ntfy_timeout_sec": 10,
}

REST_BASE = os.getenv("BINANCE_REST_BASE", "https://data-api.binance.vision").rstrip("/")
TICKER_URL = REST_BASE + "/api/v3/ticker/24hr"
INFO_URL = REST_BASE + "/api/v3/exchangeInfo"
WS_BASE = os.getenv("BINANCE_WS_BASE", "wss://data-stream.binance.vision/stream")
STATE_FILE = "alerts_state.json"
LOG_FILE = "orderflow.log"

STABLE_BASES = {
    "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "AEUR", "EURI", "USDP", "USDE",
    "USDS", "USDG", "USD1", "PYUSD", "RLUSD", "RWUSD", "BFUSD", "XUSD", "U", "TRY", "BIDR",
    "UST", "USTC", "USDD", "FRAX", "LUSD", "GUSD", "USDB", "CRVUSD", "GHO", "USDJ",
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
KLINE_INTERVALS = {60: "1m", 180: "3m", 300: "5m", 900: "15m", 1800: "30m", 3600: "1h",
                   7200: "2h", 14400: "4h"}
MTF_FRAME_NAMES = ("4h", "1h", "30m", "15m")


def candle_label(sec: float) -> str:
    return KLINE_INTERVALS.get(int(sec), f"{int(sec)}s")


log = logging.getLogger("orderflow")


def setup_logging() -> None:
    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    fmt.converter = time.gmtime
    log.setLevel(logging.INFO)
    log.handlers.clear()
    fh = RotatingFileHandler(LOG_FILE, maxBytes=10 * 1024 * 1024, backupCount=5, encoding="utf-8")
    ch = logging.StreamHandler(sys.stdout)
    for h in (fh, ch):
        h.setFormatter(fmt)
        log.addHandler(h)


def utc_day(ts: float | None = None) -> str:
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).strftime("%Y-%m-%d")


class AdaptiveThresholds:
    def __init__(self, cfg: dict[str, Any]) -> None:
        a = cfg.get("adaptive", {})
        self.enabled = bool(a.get("enabled", True))
        self.sample_interval = float(a.get("sample_interval_sec", 30))
        self.high_vol = float(a.get("high_vol_move15", 1.0))
        self.low_vol = float(a.get("low_vol_move15", 0.25))
        self.high_mult = float(a.get("high_mult", 1.25))
        self.low_mult = float(a.get("low_mult", 0.80))
        self.base_cvd = float(cfg["cvd_threshold"])
        self.base_short_cvd = float(cfg["short_cvd_threshold"])
        self.base_obi = float(cfg["obi_threshold"])
        self.samples: deque[float] = deque(maxlen=240)
        self.mult = 1.0
        self.regime = "normal"
        self.last_sample = 0.0

    def observe(self, move15: float, now: float) -> None:
        if not self.enabled or move15 >= 999.0:
            return
        if now - self.last_sample < self.sample_interval:
            return
        self.last_sample = now
        self.samples.append(move15)
        if len(self.samples) < 20:
            return
        avg = sum(self.samples) / len(self.samples)
        if avg >= self.high_vol:
            self.mult, self.regime = self.high_mult, "high-vol"
        elif avg <= self.low_vol:
            self.mult, self.regime = self.low_mult, "low-vol"
        else:
            self.mult, self.regime = 1.0, "normal"

    @property
    def cvd(self) -> float: return self.base_cvd * self.mult

    @property
    def short_cvd(self) -> float: return self.base_short_cvd * self.mult

    @property
    def obi(self) -> float: return self.base_obi * self.mult


def validate_config(cfg: dict[str, Any]) -> None:
    errs: list[str] = []

    def num(key: str, lo: float, hi: float | None = None) -> None:
        v = cfg.get(key)
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            errs.append(f"'{key}' must be a number, got {v!r}"); return
        if v < lo or (hi is not None and v > hi):
            errs.append(f"'{key}' must be in [{lo}, {hi}], got {v}")

    if cfg.get("depth_levels") not in (5, 10, 20):
        errs.append(f"depth_levels must be 5, 10 or 20, got {cfg.get('depth_levels')}")
    num("max_symbols", 1, 200)
    num("max_price_change_24h_pct", 0, 100); num("cvd_threshold", 0.1, 90)
    num("obi_threshold", 0.01, 0.95); num("spread_max_bps", 0.1, 200)
    num("eval_interval_sec", 1, 60); num("bucket_seconds", 1, 60)
    num("min_window_fill", 0.1, 1.0); num("short_window_seconds", 60)
    num("short_cvd_threshold", 0.1, 90); num("short_max_move_pct", 0.01, 10)
    num("fresh_max_age_sec", 30); num("max_alerts_per_day", 1); num("stale_seconds", 1)

    if not isinstance(cfg.get("flow_signals"), dict):
        errs.append("flow_signals must be a dict")

    dv = cfg.get("divergence")
    if not isinstance(dv, dict):
        errs.append("divergence must be a dict")
    else:
        for k in ("fractal_left", "fractal_right", "min_swing_gap_candles", "max_swing_gap_candles"):
            v = dv.get(k)
            if not isinstance(v, int) or v < 1:
                errs.append(f"divergence['{k}'] must be int >= 1, got {v!r}")
        if isinstance(dv.get("min_swing_gap_candles"), int) and isinstance(dv.get("max_swing_gap_candles"), int):
            if dv["min_swing_gap_candles"] > dv["max_swing_gap_candles"]:
                errs.append("divergence min_swing_gap_candles > max_swing_gap_candles")
        if dv.get("regime_mode") not in ("off", "hidden_only", "all"):
            errs.append(f"divergence['regime_mode'] invalid: {dv.get('regime_mode')!r}")
        if dv.get("trigger_break") not in ("off", "prev_close", "prev_high"):
            errs.append(f"divergence['trigger_break'] invalid: {dv.get('trigger_break')!r}")
        if dv.get("regime_tf") not in cfg.get("timeframes", {}):
            errs.append(f"divergence['regime_tf'] must be a timeframe key, got {dv.get('regime_tf')!r}")
        for k in ("regime_oppose_cvd_pct", "trigger_max_delay_sec"):
            v = dv.get(k)
            if not isinstance(v, (int, float)) or v <= 0:
                errs.append(f"divergence['{k}'] must be > 0, got {v!r}")

    f = cfg.get("footprint")
    if not isinstance(f, dict):
        errs.append("footprint must be a dict")
    else:
        for k, lo in (("row_bps", 0.1), ("keep_candles", 1), ("min_rows", 1), ("zone_rows", 1),
                      ("imbalance_ratio", 1.01), ("min_stacked", 1), ("min_confirms", 0),
                      ("flow_zone_rows", 1), ("flow_min_ratio", 1.01)):
            v = f.get(k)
            if not isinstance(v, (int, float)) or v < lo:
                errs.append(f"footprint['{k}'] must be >= {lo}, got {v!r}")
        av = f.get("absorb_min_frac")
        if not isinstance(av, (int, float)) or not 0 < av <= 1:
            errs.append(f"footprint['absorb_min_frac'] must be in (0,1], got {av!r}")

    tfs = cfg.get("timeframes")
    if not isinstance(tfs, dict) or not tfs:
        errs.append("timeframes must be a non-empty dict")
    else:
        if "4h" not in tfs:
            errs.append("timeframes must contain '4h' (used for regime display)")
        for name, tf in tfs.items():
            if not isinstance(tf, dict):
                errs.append(f"timeframes['{name}'] must be a dict"); continue
            base = tf.get("div_base_seconds")
            if base not in KLINE_INTERVALS:
                errs.append(f"timeframes['{name}']['div_base_seconds'] must be a kline size")
            for cs in tf.get("div_candle_seconds", []):
                if not isinstance(cs, int) or cs <= 0 or (isinstance(base, int) and base and cs % base):
                    errs.append(f"timeframes['{name}'] div candle {cs} invalid vs base {base}")
            ct = tf.get("confirm_candle_seconds")
            if not isinstance(ct, int) or ct <= 0 or (isinstance(base, int) and base and ct % base):
                errs.append(f"timeframes['{name}']['confirm_candle_seconds'] must be a multiple of base")
            elif ct not in KLINE_INTERVALS:
                errs.append(f"timeframes['{name}']['confirm_candle_seconds'] must be a kline size")
            for fr in tf.get("mtf_frames", []):
                if fr not in MTF_FRAME_NAMES:
                    errs.append(f"timeframes['{name}']['mtf_frames'] invalid frame {fr!r}")
            for k in ("seconds", "cooldown_min",
                      "max_recent_move_pct", "div_max_move_since_swing_pct",
                      "div_min_price_diff_pct", "max_swing_age_sec"):
                v = tf.get(k)
                if not isinstance(v, (int, float)) or v < 0:
                    errs.append(f"timeframes['{name}']['{k}'] invalid: {v!r}")

    if errs:
        raise ValueError(" | ".join(errs))


class SymbolSelector:
    def __init__(self, cfg: dict[str, Any]) -> None: self.cfg = cfg

    @staticmethod
    def is_excluded_base(base: str, trading_bases: set[str]) -> bool:
        if base in STABLE_BASES or base.startswith("USD"): return True
        if base.endswith("USD") and len(base) > 3: return True
        for suf in LEVERAGED_SUFFIXES:
            if base.endswith(suf) and base[: -len(suf)] in trading_bases: return True
        return False

    async def _get(self, session: aiohttp.ClientSession, url: str) -> Any:
        last: Exception | None = None
        for attempt in range(1, self.cfg["http_retries"] + 1):
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status(); return await resp.json()
            except Exception as exc:
                last = exc
                log.warning("[ENGINE] REST %s failed (attempt %d): %s", url, attempt, exc)
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"REST request failed: {url}: {last}")

    def _check(self, t: dict[str, Any], trading: set[str], trading_bases: set[str]) -> dict[str, Any] | None:
        sym = str(t["symbol"])
        if not sym.endswith("USDT") or sym not in trading: return None
        base = sym[: -len("USDT")]
        if not base or self.is_excluded_base(base, trading_bases): return None
        qv, cnt, chg = float(t["quoteVolume"]), int(t["count"]), float(t["priceChangePercent"])
        if abs(chg) > self.cfg["max_price_change_24h_pct"]: return None
        return {"symbol": sym, "quote_volume": qv, "count": cnt, "change_pct": chg}

    async def select(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        tickers, info = await asyncio.gather(self._get(session, TICKER_URL), self._get(session, INFO_URL))
        symbols = [s for s in info.get("symbols", []) if s.get("status") == "TRADING"]
        ticks: dict[str, float] = {}
        for s in symbols:
            for flt in s.get("filters", []):
                if flt.get("filterType") == "PRICE_FILTER":
                    try: ticks[s["symbol"]] = float(flt.get("tickSize", 0.0))
                    except (TypeError, ValueError): pass
        trading = {s["symbol"] for s in symbols}
        trading_bases = {str(s.get("baseAsset", "")) for s in symbols}
        usdt = [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")]
        picked: list[dict[str, Any]] = []
        for t in usdt:
            try: item = self._check(t, trading, trading_bases)
            except (KeyError, ValueError, TypeError): continue
            if item:
                item["tick"] = ticks.get(item["symbol"], 0.0); picked.append(item)
        picked.sort(key=lambda x: x["quote_volume"], reverse=True)
        picked = picked[: self.cfg["max_symbols"]]
        log.info("[ENGINE] Selected %d symbols after filtering %d total USDT pairs", len(picked), len(usdt))
        for i, p in enumerate(picked, 1):
            log.info("[%s] #%d rank=$%s trades24h=%s change24h=%+.2f%%", p["symbol"], i,
                     f"{p['quote_volume']:,.0f}", f"{p['count']:,}", p["change_pct"])
        return picked


class RollingWindow:
    def __init__(self, seconds: float, bucket_sec: float) -> None:
        self.window, self.bucket = float(seconds), float(bucket_sec); self.reset()

    def reset(self) -> None:
        self.buckets: deque[list[float]] = deque()
        self.buy_vol = self.sell_vol = self.obi_sum = 0.0
        self.count = self.obi_n = 0

    def _current(self, now: float) -> list[float]:
        bid = int(now // self.bucket)
        if not self.buckets or self.buckets[-1][0] != bid:
            self.buckets.append([bid, 0.0, 0.0, 0, 0.0, 0.0, 0])
        return self.buckets[-1]

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        b = self._current(now)
        if is_buy: b[1] += quote; self.buy_vol += quote
        else: b[2] += quote; self.sell_vol += quote
        b[3] += 1; self.count += 1
        if b[4] == 0.0: b[4] = price
        self.evict(now)

    def add_obi(self, obi: float, now: float) -> None:
        b = self._current(now); b[5] += obi; b[6] += 1
        self.obi_sum += obi; self.obi_n += 1

    def evict(self, now: float) -> None:
        limit = now - self.window
        while self.buckets and (self.buckets[0][0] + 1) * self.bucket <= limit:
            _, buy, sell, cnt, _, osum, on = self.buckets.popleft()
            self.buy_vol -= buy; self.sell_vol -= sell; self.count -= int(cnt)
            self.obi_sum -= osum; self.obi_n -= int(on)
            if self.count <= 0: self.count, self.buy_vol, self.sell_vol = 0, 0.0, 0.0
            if self.obi_n <= 0: self.obi_n, self.obi_sum = 0.0, 0.0

    @property
    def window_volume(self) -> float: return max(self.buy_vol + self.sell_vol, 0.0)

    @property
    def trade_count(self) -> int: return self.count

    @property
    def cvd_pct(self) -> float:
        total = self.buy_vol + self.sell_vol
        return (self.buy_vol - self.sell_vol) / total * 100.0 if total > 0 else 0.0

    @property
    def avg_obi(self) -> float | None: return self.obi_sum / self.obi_n if self.obi_n > 0 else None

    def recent_move_pct(self, last_price: float, last_trade_ts: float, min_fill: float) -> float:
        for b in self.buckets:
            if b[4] > 0:
                if last_trade_ts - b[0] * self.bucket < self.window * min_fill: return 999.0
                return abs(last_price - b[4]) / b[4] * 100.0
        return 999.0


DIV_TEXT = {
    ("BUY", "regular"): "Regular Bullish Divergence (price lower low, CVD higher low)",
    ("BUY", "hidden"): "Hidden Bullish Divergence (price higher low, CVD lower low)",
    ("SELL", "regular"): "Regular Bearish Divergence (price higher high, CVD lower high)",
    ("SELL", "hidden"): "Hidden Bearish Divergence (price lower high, CVD higher high)",
}


class TriggerTracker:
    """OHLC + taker delta of the lower-TF trigger candle (30m for 4h, 15m for 1h).
    Reports each candle exactly once, right after it closes."""

    def __init__(self, sec: float) -> None:
        self.sec = float(sec); self.reset()

    def reset(self) -> None:
        self.cur: dict[str, Any] | None = None
        self.last_closed: dict[str, Any] | None = None
        self.prev_closed: dict[str, Any] | None = None
        self.pending: dict[str, Any] | None = None

    def _close(self) -> None:
        if self.cur is None: return
        self.prev_closed, self.last_closed = self.last_closed, self.cur
        self.pending, self.cur = self.cur, None

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        cid = int(now // self.sec)
        if self.cur is not None and self.cur["id"] < cid: self._close()
        if self.cur is None:
            consecutive = self.last_closed is not None and self.last_closed["id"] == cid - 1
            partial = (now - cid * self.sec) > 5.0 and not consecutive
            self.cur = {"id": cid, "open": price, "high": price, "low": price, "close": price,
                        "buy": 0.0, "sell": 0.0, "partial": partial}
        c = self.cur
        c["high"] = max(c["high"], price); c["low"] = min(c["low"], price); c["close"] = price
        if is_buy: c["buy"] += quote
        else: c["sell"] += quote

    def poll(self, now: float) -> dict[str, Any] | None:
        cid = int(now // self.sec)
        if self.cur is not None and self.cur["id"] < cid: self._close()
        p, self.pending = self.pending, None
        if p is None or p["id"] != cid - 1: return None
        return p

    def seed(self, rows: list[list[Any]]) -> None:
        """Build trigger candles from base klines so the first close after start works."""
        groups: dict[int, dict[str, Any]] = {}
        order: list[int] = []
        for r in rows:
            cid = int(float(r[0]) // 1000 // self.sec)
            qv, tb = float(r[7]), float(r[10])
            g = groups.get(cid)
            if g is None:
                g = groups[cid] = {"id": cid, "open": float(r[1]), "high": float(r[2]), "low": float(r[3]),
                                   "close": float(r[4]), "buy": 0.0, "sell": 0.0, "partial": False}
            order.append(cid)
            g["high"] = max(g["high"], float(r[2])); g["low"] = min(g["low"], float(r[3]))
            g["close"] = float(r[4]); g["buy"] += tb; g["sell"] += qv - tb
        self.reset()
        now_cid = int(time.time() // self.sec)
        for cid in order:
            g = groups[cid]
            if cid >= now_cid: self.cur = g
            else: self.prev_closed, self.last_closed = self.last_closed, g


class Candle:
    __slots__ = ("id", "open", "high", "low", "close", "cvd_open", "cvd_high", "cvd_low", "cvd_close",
                 "cvd_at_low", "cvd_at_high", "buy", "sell", "high_ts", "low_ts", "high_live",
                 "low_live", "partial", "row", "fp")

    def __init__(self, cid: int, price: float, cvd: float, ts: float, row: float,
                 live: bool, partial: bool = False) -> None:
        self.id = cid
        self.open = self.high = self.low = self.close = price
        self.cvd_open = self.cvd_high = self.cvd_low = self.cvd_close = cvd
        self.cvd_at_low = self.cvd_at_high = cvd
        self.buy = self.sell = 0.0
        self.high_ts = self.low_ts = ts
        self.high_live = self.low_live = live
        self.partial = partial
        self.row = row
        self.fp: dict[int, list[float]] | None = {} if live else None


class CandleSeries:
    def __init__(self, candle_sec: float, history_candles: int, tick: float, fp_cfg: dict[str, Any]) -> None:
        self.candle = float(candle_sec)
        self.max_candles = int(history_candles) + 2
        self.tick = tick if tick > 0 else 1e-8
        self.fp_cfg = fp_cfg
        self.div_cfg = CONFIG.get("divergence", {})
        self.reset()

    def reset(self) -> None:
        self.candles: deque[Candle] = deque(maxlen=self.max_candles)
        self.cum = 0.0
        self.seen = {"low": -1, "high": -1}

    def _row(self, price: float) -> float:
        raw = price * self.fp_cfg["row_bps"] / 10_000.0
        return round(max(self.tick, round(raw / self.tick) * self.tick), 12)

    def load_history(self, rows: list[list[Any]]) -> None:
        """v7: the still-forming kline is INCLUDED (otherwise the live candle misses its first part
        and prints a wrong low/high). CVD at an extreme = CVD at the end of the base bar that
        printed that extreme (much closer to truth than min/max of the bar)."""
        self.reset()
        cum = 0.0
        cur: Candle | None = None
        for r in rows:
            ts = float(r[6]) / 1000.0
            cid = int(float(r[0]) // 1000 // self.candle)
            qv, tb = float(r[7]), float(r[10])
            before, after = cum, cum + 2.0 * tb - qv
            if cur is None or cur.id != cid:
                cur = Candle(cid, float(r[1]), before, ts, 0.0, live=False)
                self.candles.append(cur)
            hi, lo = float(r[2]), float(r[3])
            if hi > cur.high: cur.high, cur.high_ts, cur.cvd_at_high = hi, ts, after
            if lo < cur.low: cur.low, cur.low_ts, cur.cvd_at_low = lo, ts, after
            cur.close = float(r[4])
            cur.cvd_high = max(cur.cvd_high, before, after)
            cur.cvd_low = min(cur.cvd_low, before, after)
            cur.cvd_close = after
            cur.buy += tb; cur.sell += qv - tb
            cum = after
        self.cum = cum
        for c in self.candles: c.row = self._row(c.close)

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        signed = quote if is_buy else -quote
        before = self.cum
        self.cum += signed
        cid = int(now // self.candle)
        c = self.candles[-1] if self.candles else None
        if c is None or c.id != cid:
            partial = c is None and (now - cid * self.candle) > 5.0
            c = Candle(cid, price, before, now, self._row(price), live=True, partial=partial)
            self.candles.append(c)
        keep = int(self.fp_cfg["keep_candles"])
        if len(self.candles) > keep:
            self.candles[-keep - 1].fp = None
        if price > c.high:
            c.high, c.high_ts, c.high_live = price, now, True
            c.cvd_at_high = self.cum
        if price < c.low:
            c.low, c.low_ts, c.low_live = price, now, True
            c.cvd_at_low = self.cum
        c.close = price
        c.cvd_close = self.cum
        if self.cum > c.cvd_high: c.cvd_high = self.cum
        if self.cum < c.cvd_low: c.cvd_low = self.cum
        if is_buy: c.buy += quote
        else: c.sell += quote
        if c.fp is None: c.fp = {}
        lvl = int(price // c.row)
        cell = c.fp.get(lvl)
        if cell is None: cell = c.fp[lvl] = [0.0, 0.0]
        cell[1 if is_buy else 0] += quote

    @staticmethod
    def _is_fractal_low(cs: list[Candle], i: int, left: int, right: int) -> bool:
        if i - left < 0 or i + right >= len(cs): return False
        cur = cs[i]
        for k in range(1, left + 1):
            if cur.low > cs[i - k].low: return False
        for k in range(1, right + 1):
            if cur.low >= cs[i + k].low: return False
        return True

    @staticmethod
    def _is_fractal_high(cs: list[Candle], i: int, left: int, right: int) -> bool:
        if i - left < 0 or i + right >= len(cs): return False
        cur = cs[i]
        for k in range(1, left + 1):
            if cur.high < cs[i - k].high: return False
        for k in range(1, right + 1):
            if cur.high <= cs[i + k].high: return False
        return True

    def _confirmed_swings(self, cs: list[Candle], pivot: str, left: int, right: int) -> list[int]:
        out: list[int] = []
        for i in range(left, len(cs) - right):
            if any(cs[j].partial for j in range(i - left, i + right + 1)): continue
            ok = (self._is_fractal_low(cs, i, left, right) if pivot == "low"
                  else self._is_fractal_high(cs, i, left, right))
            if ok: out.append(i)
        return out

    def find_divergences(self) -> list[dict[str, Any]]:
        """v7: divergence between TWO SWINGS (A older, B newest). B is the live/forming extreme if it
        is a local extreme, otherwise the last confirmed fractal. No fixed candle distance."""
        cs = list(self.candles)
        n = len(cs)
        left = int(self.div_cfg.get("fractal_left", 2))
        right = int(self.div_cfg.get("fractal_right", 1))
        min_gap = int(self.div_cfg.get("min_swing_gap_candles", 2))
        max_gap = int(self.div_cfg.get("max_swing_gap_candles", 40))
        use_forming = bool(self.div_cfg.get("use_forming_swing", True))
        out: list[dict[str, Any]] = []
        if n < left + right + 2: return out

        for pivot in ("low", "high"):
            conf = self._confirmed_swings(cs, pivot, left, right)
            last_conf = conf[-1] if conf else -1
            b_idx: int | None = None
            if use_forming:
                start = max(last_conf + 1, left)
                tail = [j for j in range(start, n) if not cs[j].partial]
                if tail:
                    if pivot == "low": j = min(tail, key=lambda k: (cs[k].low, -k))
                    else: j = max(tail, key=lambda k: (cs[k].high, k))
                    if pivot == "low":
                        local = all(cs[j].low <= cs[j - k].low for k in range(1, left + 1) if j - k >= 0)
                    else:
                        local = all(cs[j].high >= cs[j - k].high for k in range(1, left + 1) if j - k >= 0)
                    if local and j > last_conf: b_idx = j

            pairs: list[tuple[int, int]] = []
            if len(conf) >= 2: pairs.append((conf[-2], conf[-1]))     # confirmed A -> confirmed B
            if b_idx is not None and conf: pairs.append((conf[-1], b_idx))  # confirmed A -> forming B

            for a_idx, bi in pairs:
                gap = bi - a_idx
                if gap < min_gap or gap > max_gap: continue
                ref, cand = cs[a_idx], cs[bi]
                if ref.partial or cand.partial or cand.id <= self.seen[pivot]: continue

                if pivot == "low":
                    p1, p2, v1, v2, ts = ref.low, cand.low, ref.cvd_at_low, cand.cvd_at_low, cand.low_ts
                    clean = all(cs[k].low >= min(p1, p2) for k in range(a_idx + 1, bi))
                else:
                    p1, p2, v1, v2, ts = ref.high, cand.high, ref.cvd_at_high, cand.cvd_at_high, cand.high_ts
                    clean = all(cs[k].high <= max(p1, p2) for k in range(a_idx + 1, bi))
                if not clean: continue

                found: tuple[str, str] | None = None
                if pivot == "low":
                    if p2 < p1 and v2 > v1: found = ("BUY", "regular")
                    elif p2 > p1 and v2 < v1: found = ("BUY", "hidden")
                else:
                    if p2 > p1 and v2 < v1: found = ("SELL", "regular")
                    elif p2 < p1 and v2 > v1: found = ("SELL", "hidden")

                if found:
                    out.append({"pivot": pivot, "side": found[0], "kind": found[1], "p1": p1, "p2": p2,
                                "v1": v1, "v2": v2, "ts": ts, "cand": cand, "ref": ref, "gap": gap,
                                "forming": bi > last_conf})
        return out

    def mark(self, d: dict[str, Any]) -> None:
        self.seen[d["pivot"]] = max(self.seen[d["pivot"]], d["cand"].id)

    def flow_footprint_check(self, side: str, last_price: float, f: dict[str, Any]) -> dict[str, Any]:
        res: dict[str, Any] = {"available": False, "ok": False, "ratio": 0.0, "stacked": 0, "row": 0.0}
        c = self.candles[-1] if self.candles else None
        if c is None or not c.fp or c.row <= 0 or c.partial: return res
        fp, z = c.fp, int(f["flow_zone_rows"])
        total = sum(a + b for a, b in fp.values())
        cur = int(last_price // c.row)
        levels = range(cur - z + 1, cur + 1) if side == "BUY" else range(cur, cur + z)

        def vol(level: int, i: int) -> float:
            cell = fp.get(level); return cell[i] if cell else 0.0

        buy = sum(vol(lv, 1) for lv in levels); sell = sum(vol(lv, 0) for lv in levels)
        if total <= 0 or buy + sell < f["imbalance_min_frac"] * total: return res
        res["available"], res["row"] = True, c.row
        mine, other = (buy, sell) if side == "BUY" else (sell, buy)
        res["ratio"] = mine / other if other > 0 else float("inf")
        min_vol, ratio = f["imbalance_min_frac"] * total, f["imbalance_ratio"]
        best = run = 0
        seq = levels if side == "BUY" else reversed(levels)
        for lv in seq:
            if side == "BUY":
                hit = vol(lv, 1) >= min_vol and vol(lv, 1) >= ratio * vol(lv - 1, 0)
            else:
                hit = vol(lv, 0) >= min_vol and vol(lv, 0) >= ratio * vol(lv + 1, 1)
            run = run + 1 if hit else 0
            best = max(best, run)
        res["stacked"] = best
        res["ok"] = res["ratio"] >= f["flow_min_ratio"] and best >= f["min_stacked"]
        return res

    @staticmethod
    def footprint_check(d: dict[str, Any], last_price: float, f: dict[str, Any]) -> dict[str, Any]:
        cand: Candle = d["cand"]
        low = d["pivot"] == "low"
        res: dict[str, Any] = {"available": False, "absorb": False, "stacked": 0, "frac": 0.0,
                               "confirms": 0, "row": cand.row, "span": 0}
        fp = cand.fp
        live = cand.low_live if low else cand.high_live
        if not fp or not live or cand.row <= 0: return res
        lo, hi = min(fp), max(fp)
        res["span"] = hi - lo + 1
        total = sum(s + b for s, b in fp.values())
        if total <= 0 or res["span"] < f["min_rows"]: return res
        res["available"] = True
        z = int(f["zone_rows"]); min_vol = f["imbalance_min_frac"] * total; ratio = f["imbalance_ratio"]

        def vol(level: int, side: int) -> float:
            cell = fp.get(level); return cell[side] if cell else 0.0

        best = run = 0
        if low:
            heavy = sum(vol(lv, 0) for lv in range(lo, lo + z))
            res["frac"] = heavy / total
            res["absorb"] = res["frac"] >= f["absorb_min_frac"] and last_price >= (lo + z) * cand.row
            for lv in range(lo, lo + z + 3):
                ask, bid_below = vol(lv, 1), vol(lv - 1, 0)
                run = run + 1 if (ask >= min_vol and ask >= ratio * bid_below) else 0
                best = max(best, run)
        else:
            heavy = sum(vol(lv, 1) for lv in range(hi - z + 1, hi + 1))
            res["frac"] = heavy / total
            res["absorb"] = res["frac"] >= f["absorb_min_frac"] and last_price < (hi - z + 1) * cand.row
            for lv in range(hi, hi - z - 3, -1):
                bid, ask_above = vol(lv, 0), vol(lv + 1, 1)
                run = run + 1 if (bid >= min_vol and bid >= ratio * ask_above) else 0
                best = max(best, run)
        res["stacked"] = best
        res["confirms"] = int(res["absorb"]) + int(best >= f["min_stacked"])
        return res


class OrderFlowState:
    def __init__(self, symbol: str, timeframes: dict[str, dict[str, Any]], bucket_sec: float, levels: int,
                 div_history: int = 48, short_sec: float = 900.0, tick: float = 0.0,
                 fp_cfg: dict[str, Any] | None = None) -> None:
        self.symbol, self.levels = symbol, levels
        self.windows = {name: RollingWindow(tf["seconds"], bucket_sec) for name, tf in timeframes.items()}
        self.short = RollingWindow(short_sec, bucket_sec)
        self.mtf_windows = {"30m": RollingWindow(1800.0, bucket_sec)}
        fp_cfg = fp_cfg if fp_cfg is not None else CONFIG["footprint"]
        self.candles = {name: {int(sec): CandleSeries(sec, div_history, tick, fp_cfg)
                               for sec in tf["div_candle_seconds"]}
                        for name, tf in timeframes.items()}
        self.trig = {name: TriggerTracker(tf["confirm_candle_seconds"]) for name, tf in timeframes.items()}
        self.reset()

    def reset(self) -> None:
        for w in self.windows.values(): w.reset()
        self.short.reset()
        for w in self.mtf_windows.values(): w.reset()
        for group in self.candles.values():
            for s in group.values(): s.reset()
        for t in self.trig.values(): t.reset()
        self.last_price = 0.0; self.last_trade_ts = 0.0
        self.bid_vol = self.ask_vol = 0.0
        self.best_bid = self.best_ask = 0.0
        self.has_depth = False; self.last_update = 0.0
        self.cside: dict[str, str | None] = {}
        self.cross_ts: dict[str, float] = {}

    def add_trade(self, price: float, qty: float, buyer_is_maker: bool, now: float) -> None:
        quote = price * qty
        is_buy = not buyer_is_maker
        for w in self.windows.values(): w.add_trade(price, quote, is_buy, now)
        self.short.add_trade(price, quote, is_buy, now)
        for w in self.mtf_windows.values(): w.add_trade(price, quote, is_buy, now)
        for group in self.candles.values():
            for s in group.values(): s.add_trade(price, quote, is_buy, now)
        for t in self.trig.values(): t.add_trade(price, quote, is_buy, now)
        self.last_price = price; self.last_trade_ts = now; self.last_update = now

    def update_depth(self, bids: list[list[str]], asks: list[list[str]], now: float) -> None:
        bids, asks = bids[: self.levels], asks[: self.levels]
        bb, ba = float(bids[0][0]), float(asks[0][0])
        if bb <= 0 or ba < bb: raise ValueError("crossed or invalid book")
        self.bid_vol = sum(float(q) for _, q in bids)
        self.ask_vol = sum(float(q) for _, q in asks)
        self.best_bid, self.best_ask, self.has_depth, self.last_update = bb, ba, True, now
        obi = self.obi_now
        for w in self.windows.values(): w.add_obi(obi, now)
        self.short.add_obi(obi, now)
        for w in self.mtf_windows.values(): w.add_obi(obi, now)

    def evict(self, now: float) -> None:
        for w in self.windows.values(): w.evict(now)
        self.short.evict(now)
        for w in self.mtf_windows.values(): w.evict(now)

    def is_stale(self, now: float, limit: float) -> bool:
        return self.last_update == 0.0 or now - self.last_update > limit

    @property
    def obi_now(self) -> float:
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0

    def obi_for(self, name: str, use_average: bool) -> float:
        if use_average:
            avg = self.windows[name].avg_obi
            if avg is not None: return avg
        return self.obi_now

    @property
    def spread_bps(self) -> float:
        if not self.has_depth: return float("inf")
        mid = (self.best_ask + self.best_bid) / 2.0
        return (self.best_ask - self.best_bid) / mid * 10_000.0

    def recent_move_pct(self, name: str, min_fill: float) -> float:
        return self.windows[name].recent_move_pct(self.last_price, self.last_trade_ts, min_fill)

    def frame_window(self, frame: str) -> RollingWindow | None:
        if frame in self.windows: return self.windows[frame]
        if frame in self.mtf_windows: return self.mtf_windows[frame]
        if frame == "15m": return self.short
        return None


class NtfyAlerter:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.url = os.getenv("NTFY_URL", "").strip()
        self.token = os.getenv("NTFY_TOKEN", "").strip()
        self.session: aiohttp.ClientSession | None = None
        if not self.url:
            log.warning("[ENGINE] ntfy disabled (NTFY_URL not set); alerts are logged only")

    async def start(self) -> None:
        timeout = aiohttp.ClientTimeout(total=self.cfg["ntfy_timeout_sec"])
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        if self.session and not self.session.closed: await self.session.close()

    async def send(self, title: str, body: str, priority: str = "high", tags: str = "") -> bool:
        if not self.url or self.session is None: return False
        headers = {"Title": title.encode("ascii", "ignore").decode(), "Priority": priority}
        if tags: headers["Tags"] = tags
        if self.token: headers["Authorization"] = f"Bearer {self.token}"
        try:
            async with self.session.post(self.url, data=body.encode("utf-8"), headers=headers) as resp:
                if resp.status >= 300:
                    log.warning("[ENGINE] ntfy HTTP %s for '%s'", resp.status, title); return False
                return True
        except Exception as exc:
            log.warning("[ENGINE] ntfy send failed for '%s': %s", title, exc); return False


class HealthMonitor:
    def __init__(self, cfg: dict[str, Any], alerter: NtfyAlerter) -> None:
        h = cfg.get("health", {})
        self.silent_limit = float(h.get("silent_limit_sec", 120))
        self.check_interval = float(h.get("check_interval_sec", 60))
        self.alert_on_silent = bool(h.get("alert_on_silent", True))
        self.alerter = alerter
        self.spawn: Any = None
        self.start_ts = time.time()
        self.last_msg_ts = 0.0
        self.window_msgs = 0; self.window_trades = 0
        self.total_signals = 0; self.eval_cycles = 0; self.eval_errors = 0
        self.last_check_ts = 0.0; self.last_rate = 0.0
        self.silent_alerted = False

    def note_message(self, kind: str, now: float) -> None:
        self.window_msgs += 1
        if kind == "trade": self.window_trades += 1
        self.last_msg_ts = now; self.silent_alerted = False

    def note_signal(self) -> None: self.total_signals += 1

    def note_eval(self, error: bool = False) -> None:
        self.eval_cycles += 1
        if error: self.eval_errors += 1

    def check(self, now: float, connected: bool) -> None:
        if self.last_check_ts == 0.0: self.last_check_ts = now; return
        elapsed = now - self.last_check_ts
        if elapsed < self.check_interval: return
        self.last_rate = self.window_msgs / max(elapsed, 1.0)
        self.window_msgs = 0; self.last_check_ts = now
        if connected and self.last_msg_ts > 0 and (now - self.last_msg_ts) > self.silent_limit:
            if not self.silent_alerted and self.alert_on_silent and self.spawn is not None:
                self.silent_alerted = True
                gap = now - self.last_msg_ts
                self.spawn(self.alerter.send(
                    "Orderflow bot: data stream silent",
                    f"No WebSocket message for {gap:.0f}s while connected. Bot may be stuck.",
                    "high", "warning"))

    def summary(self, now: float) -> str:
        up = (now - self.start_ts) / 3600.0
        return (f"uptime={up:.1f}h msg_rate={self.last_rate:.1f}/s signals={self.total_signals} "
                f"eval_cycles={self.eval_cycles} eval_errors={self.eval_errors}")


class AlertBook:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg; self.day = utc_day(); self.alerts_today = 0
        self.last_alert: dict[str, float] = {}
        self.today_symbols: dict[str, int] = {}
        self.today_counts: dict[str, int] = {}

    def load(self) -> None:
        if not os.path.exists(STATE_FILE):
            log.info("[ENGINE] No saved state; starting fresh"); return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f: d = json.load(f)
            self.day = str(d.get("day", self.day))
            self.alerts_today = int(d.get("alerts_today", 0))
            self.last_alert = {k: float(v) for k, v in d.get("last_alert", {}).items()}
            self.today_symbols = {k: int(v) for k, v in d.get("today_symbols", {}).items()}
            self.today_counts = {k: int(v) for k, v in d.get("today_counts", {}).items()}
            log.info("[ENGINE] Loaded state: day=%s alerts_today=%d", self.day, self.alerts_today)
        except Exception as exc:
            log.error("[ENGINE] Corrupt state file (%s); starting fresh", exc)

    def snapshot(self) -> dict[str, Any]:
        return {"day": self.day, "alerts_today": self.alerts_today, "last_alert": self.last_alert,
                "today_symbols": self.today_symbols, "today_counts": self.today_counts,
                "saved_at": datetime.now(timezone.utc).isoformat()}

    @staticmethod
    def _write_atomic(data: dict[str, Any]) -> None:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f: json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)

    async def save(self) -> None:
        try: await asyncio.to_thread(self._write_atomic, self.snapshot())
        except Exception as exc: log.error("[ENGINE] State save failed: %s", exc)

    def on_cooldown(self, key: str, now: float, minutes: float) -> bool:
        return now - self.last_alert.get(key, 0.0) < minutes * 60

    def cap_reached(self) -> bool: return self.alerts_today >= self.cfg["max_alerts_per_day"]

    def register(self, key: str, symbol: str, tf: str, side: str, now: float, label: str | None = None) -> None:
        self.alerts_today += 1; self.last_alert[key] = now
        self.today_symbols[symbol] = self.today_symbols.get(symbol, 0) + 1
        label = label or f"{tf} {side}"
        self.today_counts[label] = self.today_counts.get(label, 0) + 1

    def build_summary(self, label: str) -> str:
        ranked = sorted(self.today_symbols.items(), key=lambda kv: kv[1], reverse=True)
        top = ", ".join(f"{s} x{n}" for s, n in ranked[:5]) or "n/a"
        lines = [f"Signal Summary {label}", f"Alerts sent: {self.alerts_today}"]
        lines += [f"{k}: {v}" for k, v in sorted(self.today_counts.items())]
        lines.append(f"Most active: {top}")
        return "\n".join(lines)

    def reset_day(self, new_day: str) -> None:
        self.day, self.alerts_today = new_day, 0
        self.today_symbols, self.today_counts = {}, {}
        cutoff = time.time() - 86400
        self.last_alert = {k: v for k, v in self.last_alert.items() if v > cutoff}


class Engine:
    def __init__(self) -> None:
        self.cfg = CONFIG
        self.book = AlertBook(self.cfg)
        self.alerter = NtfyAlerter(self.cfg)
        self.adapt = AdaptiveThresholds(self.cfg)
        self.health = HealthMonitor(self.cfg, self.alerter)
        self.health.spawn = self._spawn
        self.states: dict[str, OrderFlowState] = {}
        self.stop = asyncio.Event()
        self.connected = False
        self.stream_start = time.time()
        self.last_msg_ts = 0.0
        self._wiped_since_msg = False
        self.block_counts: dict[str, int] = {}
        self._bg: set[asyncio.Task[Any]] = set()

    def _spawn(self, coro: Any) -> None:
        task = asyncio.create_task(coro); self._bg.add(task); task.add_done_callback(self._bg.discard)

    def request_stop(self, why: str) -> None:
        if not self.stop.is_set():
            log.info("[ENGINE] Stop requested (%s)", why); self.stop.set()

    async def _sleep_or_stop(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=max(seconds, 0.0)); return True
        except asyncio.TimeoutError:
            return False

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try: loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda s, f, n=sig.name: loop.call_soon_threadsafe(self.request_stop, n))

        try: validate_config(self.cfg)
        except ValueError as exc:
            log.error("[CONFIG] Startup aborted, fix config first: %s", exc); return 1

        lvl = int(self.cfg["depth_levels"])
        await self.alerter.start()
        await asyncio.to_thread(self.book.load)

        try:
            timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
            async with aiohttp.ClientSession(timeout=timeout) as session:
                picked = await SymbolSelector(self.cfg).select(session)
        except Exception as exc:
            log.error("[ENGINE] Symbol selection failed: %s", exc)
            if "451" in str(exc):
                log.error("[ENGINE] HTTP 451 = Binance blocks this server's region. Run from a "
                          "non-US/non-restricted host (VPS), or set BINANCE_REST_BASE / BINANCE_WS_BASE.")
            await self.alerter.close(); return 1

        symbols = [p["symbol"] for p in picked]
        if not symbols:
            log.error("[ENGINE] No symbols passed the filters; exiting")
            await self.alerter.close(); return 1

        ticks = {p["symbol"]: float(p.get("tick", 0.0)) for p in picked}
        for s in symbols:
            self.states[s] = OrderFlowState(s, self.cfg["timeframes"], self.cfg["bucket_seconds"], lvl,
                                            int(self.cfg["div_history_candles"]),
                                            float(self.cfg["short_window_seconds"]),
                                            ticks.get(s, 0.0), self.cfg["footprint"])

        if self.book.day != utc_day(): await self._rollover()

        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@aggTrade/{s.lower()}@depth{lvl}@100ms" for s in symbols)
        log.info("[ENGINE] Starting with %d symbols, timeframes=%s (alerts only, no trading)",
                 len(symbols), ",".join(self.cfg["timeframes"]))

        tasks = [asyncio.create_task(c) for c in (self._ws_loop(url), self._eval_loop(),
                                                  self._save_loop(), self._daily_loop(), self._diag_loop())]
        await self.stop.wait()
        for t in tasks: t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._shutdown()
        return 0

    async def _shutdown(self) -> None:
        await self.book.save()
        if self._bg: await asyncio.wait(self._bg, timeout=self.cfg["ntfy_timeout_sec"])
        await self.alerter.close()
        log.info("[ENGINE] Shutdown complete | alerts_today=%d", self.book.alerts_today)

    def _handle_message(self, raw: str | bytes) -> None:
        msg = json.loads(raw); stream, data = msg["stream"], msg["data"]
        now = time.time(); self.last_msg_ts = now; self._wiped_since_msg = False
        self.health.note_message("trade" if "@aggTrade" in stream else "depth", now)
        sym = stream.split("@", 1)[0].upper()
        st = self.states.get(sym)
        if st is None: return
        try:
            if "@aggTrade" in stream:
                st.add_trade(float(data["p"]), float(data["q"]), bool(data["m"]), now)
            elif "@depth" in stream:
                st.update_depth(data["bids"], data["asks"], now)
        except (KeyError, ValueError, TypeError, IndexError) as exc:
            log.warning("[%s] Bad %s message skipped: %s", sym, stream.split("@", 1)[1], exc)

    def _on_connect(self) -> None: self.stream_start = time.time()

    async def _prepare_state(self) -> None:
        gap = time.time() - self.last_msg_ts if self.last_msg_ts else float("inf")
        if gap <= self.cfg["reconnect_wipe_after_sec"] or self._wiped_since_msg: return
        for st in self.states.values(): st.reset()
        self._wiped_since_msg = True
        log.info("[ENGINE] Windows wiped (%s)", "start" if not self.last_msg_ts else f"gap {gap:.0f}s")
        try: await self._backfill_all()
        except Exception as exc:
            log.warning("[ENGINE] Candle backfill failed (%s); divergence history will build live", exc)

    async def _backfill_all(self) -> None:
        hist = int(self.cfg["div_history_candles"])
        selector = SymbolSelector(self.cfg)
        sem = asyncio.Semaphore(8)
        loaded = 0
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def one(sym: str, st: OrderFlowState, tf_name: str, sec: int, series: CandleSeries) -> None:
                nonlocal loaded
                base = int(self.cfg["timeframes"][tf_name]["div_base_seconds"])
                iv = KLINE_INTERVALS.get(base)
                if iv is None or sec % base:
                    log.warning("[%s] Bad div_base_seconds=%d for %ds candles; no backfill", sym, base, sec); return
                start_ms = int((time.time() // sec - hist) * sec * 1000)
                async with sem:
                    try:
                        rows: list[list[Any]] = []
                        while True:
                            data = await selector._get(
                                session, f"{REST_BASE}/api/v3/klines?symbol={sym}&interval={iv}"
                                         f"&startTime={start_ms}&limit=1000")
                            rows.extend(data)
                            if len(data) < 1000: break
                            start_ms = int(data[-1][0]) + base * 1000
                        series.load_history(rows)
                        st.trig[tf_name].seed(rows)          # v7: trigger candles ready at start
                        loaded += 1
                    except Exception as exc:
                        log.warning("[%s] Kline backfill %s failed: %s", sym, iv, exc)

            await asyncio.gather(*(one(sym, st, name, sec, series) for sym, st in self.states.items()
                                   for name, group in st.candles.items() for sec, series in group.items()))
        total = sum(len(g) for st in self.states.values() for g in st.candles.values())
        log.info("[ENGINE] Candle history loaded for %d/%d series", loaded, total)

    async def _ws_loop(self, url: str) -> None:
        fails, alerted = 0, False
        while not self.stop.is_set():
            got_data = False
            await self._prepare_state()
            try:
                log.info("[ENGINE] Connecting WebSocket (%d chars URL)", len(url))
                async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                              max_size=None, close_timeout=5) as ws:
                    self._on_connect()
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg["ws_recv_timeout_sec"])
                        if not got_data:
                            got_data, fails, alerted, self.connected = True, 0, False, True
                            log.info("[ENGINE] WebSocket connected, receiving data")
                        try: self._handle_message(raw)
                        except Exception as exc:
                            log.warning("[ENGINE] Malformed message skipped: %s", exc)
            except asyncio.CancelledError: raise
            except Exception as exc:
                log.warning("[ENGINE] WebSocket closed/error: %s: %s", type(exc).__name__, exc)
                self.connected = False
                if self.stop.is_set(): break
                if got_data:
                    delay = 1.0
                    log.info("[ENGINE] Reconnecting after session end (e.g. 24h limit) in %.0fs", delay)
                else:
                    fails += 1
                    delay = min(2.0 ** (fails - 1), self.cfg["reconnect_backoff_max_sec"])
                    log.warning("[ENGINE] Reconnect failure #%d, retry in %.0fs", fails, delay)
                    if fails >= self.cfg["reconnect_fail_alert_after"] and not alerted:
                        alerted = True
                        self._spawn(self.alerter.send("WebSocket reconnect failing",
                                                      f"{fails} consecutive reconnect failures.", "high", "warning"))
                if await self._sleep_or_stop(delay): break

    async def _eval_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["eval_interval_sec"]):
            if not self.connected: continue
            now = time.time()
            self.health.note_eval(); self.health.check(now, self.connected)
            for sym, st in list(self.states.items()):
                try: self._evaluate(sym, st, now)
                except Exception as exc:
                    self.health.note_eval(error=True)
                    log.exception("[%s] Evaluation error: %s", sym, exc)

    def _count(self, key: str) -> None:
        self.block_counts[key] = self.block_counts.get(key, 0) + 1

    def _global_block(self, st: OrderFlowState, now: float) -> str | None:
        c = self.cfg
        if st.is_stale(now, c["stale_seconds"]) or st.last_price <= 0: return "stale"
        if not st.has_depth: return "no_book"
        if now - self.stream_start < c["warmup_seconds"]: return "warmup"
        if st.spread_bps >= c["spread_max_bps"]: return "spread"
        return None

    def _tf_status(self, st: OrderFlowState, name: str, tf: dict[str, Any]) -> tuple[str, str | None, dict[str, float]]:
        c = self.cfg
        w = st.windows[name]
        move = st.recent_move_pct(name, c["min_window_fill"])
        smove = st.short.recent_move_pct(st.last_price, st.last_trade_ts, c["min_window_fill"])
        cvd, obi = w.cvd_pct, st.obi_for(name, c["obi_window_average"])
        scvd = st.short.cvd_pct
        m = {"cvd": cvd, "obi": obi,
             "move": move, "scvd": scvd, "smove": smove}
        if move >= 999.0 or smove >= 999.0: return "window_filling", None, m
        if move >= tf["max_recent_move_pct"]: return "move", None, m
        if smove >= c["short_max_move_pct"]: return "short_move", None, m
        cvd_th, scvd_th, obi_th = self.adapt.cvd, self.adapt.short_cvd, self.adapt.obi
        if cvd > cvd_th and scvd > scvd_th and obi > obi_th: return "ok", "BUY", m
        if cvd < -cvd_th and scvd < -scvd_th and obi < -obi_th: return "ok", "SELL", m
        return "cvd_obi", None, m

    def _mtf_confirmation(self, st: OrderFlowState, side: str, frames: list[str]) -> str | None:
        c = self.cfg
        sign = 1.0 if side == "BUY" else -1.0
        for frame in frames:
            window = st.frame_window(frame)
            if window is None: continue
            filled_move = window.recent_move_pct(st.last_price, st.last_trade_ts, c["min_window_fill"])
            if filled_move >= 999.0: return f"mtf_{frame}_warmup"
            cvd = window.cvd_pct; obi = window.avg_obi
            if sign * cvd < c["mtf_cvd_min_abs_pct"]: return f"mtf_{frame}_cvd"
            if obi is None or sign * obi < c["mtf_obi_min_abs"]: return f"mtf_{frame}_obi"
        return None

    def _cvd_side(self, m: dict[str, float]) -> str | None:
        cvd_th, scvd_th = self.adapt.cvd, self.adapt.short_cvd
        if m["cvd"] > cvd_th and m["scvd"] > scvd_th: return "BUY"
        if m["cvd"] < -cvd_th and m["scvd"] < -scvd_th: return "SELL"
        return None

    def _update_cross(self, st: OrderFlowState, name: str, m: dict[str, float], now: float) -> None:
        side = self._cvd_side(m); prev = st.cside.get(name, "?")
        if prev == "?": st.cross_ts[name] = 0.0
        elif side != prev: st.cross_ts[name] = now if side else 0.0
        st.cside[name] = side

    def _regime_aligns(self, st: OrderFlowState, side: str, kind: str) -> bool:
        """v7: soft regime filter. Regular (reversal) divergences are NOT blocked by default,
        only hidden (continuation) ones must agree with the higher-TF flow."""
        dv = self.cfg.get("divergence", {})
        mode = dv.get("regime_mode", "hidden_only")
        if mode == "off": return True
        if mode == "hidden_only" and kind != "hidden": return True
        w = st.windows.get(dv.get("regime_tf", "4h"))
        if w is None: return True
        oppose = float(dv.get("regime_oppose_cvd_pct", 8.0))
        cvd = w.cvd_pct
        if side == "BUY" and cvd <= -oppose: return False
        if side == "SELL" and cvd >= oppose: return False
        return True

    def _evaluate(self, sym: str, st: OrderFlowState, now: float) -> None:
        c = self.cfg
        st.evict(now)
        gb = self._global_block(st, now)
        if gb: self._count(f"all:{gb}"); return
        smove = st.short.recent_move_pct(st.last_price, st.last_trade_ts, c["min_window_fill"])
        if smove < 999.0: self.adapt.observe(smove, now)

        for name, tf in c["timeframes"].items():
            reason, side, m = self._tf_status(st, name, tf)
            self._update_cross(st, name, m, now)
            if not c["flow_signals"].get("enabled", True): continue
            if side is None: self._count(f"{name}:{reason}"); continue
            mtf_block = self._mtf_confirmation(st, side, tf.get("mtf_frames", []))
            if mtf_block: self._count(f"{name}:{mtf_block}"); continue
            age = now - st.cross_ts.get(name, 0.0)
            if age > c["fresh_max_age_sec"]: self._count(f"{name}:not_fresh"); continue
            series = next(iter(st.candles[name].values()))
            fpf = series.flow_footprint_check(side, st.last_price, c["footprint"])
            if not fpf["available"]:
                if c["footprint"]["required"]: self._count(f"{name}:fp_unavailable"); continue
            elif not fpf["ok"]:
                if c["footprint"]["required"]: self._count(f"{name}:fp_weak"); continue
            key = f"{sym}|{name}|{side}"
            if self.book.on_cooldown(key, now, tf["cooldown_min"]): self._count(f"{name}:cooldown"); continue
            self._count(f"{name}:signal")
            self._alert(sym, name, side, st, m, age, now, key, fpf)

        for name, tf in c["timeframes"].items():
            self._check_divergence(sym, name, tf, st, now)

    def _trigger_check(self, side: str, closed: dict[str, Any], prev: dict[str, Any] | None) -> str | None:
        dv = self.cfg["divergence"]
        buy = side == "BUY"
        if closed.get("partial"): return "trigger_partial"
        if dv.get("trigger_require_color", True):
            if buy and not closed["close"] > closed["open"]: return "trigger_color"
            if not buy and not closed["close"] < closed["open"]: return "trigger_color"
        if dv.get("trigger_require_delta", True):
            if buy and not closed["buy"] > closed["sell"]: return "trigger_delta"
            if not buy and not closed["sell"] > closed["buy"]: return "trigger_delta"
        mode = dv.get("trigger_break", "prev_close")
        if mode != "off" and prev is not None and prev["id"] == closed["id"] - 1:
            if buy:
                ref = prev["close"] if mode == "prev_close" else prev["high"]
                if not closed["close"] > ref: return "trigger_break"
            else:
                ref = prev["close"] if mode == "prev_close" else prev["low"]
                if not closed["close"] < ref: return "trigger_break"
        return None

    def _check_divergence(self, sym: str, name: str, tf: dict[str, Any],
                          st: OrderFlowState, now: float) -> None:
        """v7: runs ONLY when a trigger candle (30m for 4h, 15m for 1h) has just closed."""
        tr = st.trig[name]
        closed = tr.poll(now)
        if closed is None: return
        trig_sec = int(tf["confirm_candle_seconds"])
        if now - (closed["id"] + 1) * trig_sec > self.cfg["divergence"]["trigger_max_delay_sec"]:
            self._count(f"{name}/div:trigger_stale"); return
        prev = tr.prev_closed
        w = st.windows[name]
        for sec, series in st.candles[name].items():
            cl = candle_label(sec)
            for d in series.find_divergences():
                tag = f"{name}/{cl}-div"
                if not self._regime_aligns(st, d["side"], d["kind"]):
                    self._count(f"{tag}:regime_oppose"); continue
                reason = self._div_gate(d, series, st, w, tf, now)
                if reason: self._count(f"{tag}:{reason}"); continue
                treason = self._trigger_check(d["side"], closed, prev)
                if treason: self._count(f"{tag}:{treason}"); continue
                key = f"{sym}|{name}|{cl}|{d['side']}|{d['kind']}-div"
                if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                    self._count(f"{tag}:cooldown"); continue
                self._count(f"{tag}:signal")
                series.mark(d)
                self._alert_div(sym, name, cl, d, st, now, key, closed)

    def _div_gate(self, d: dict[str, Any], series: CandleSeries, st: OrderFlowState,
                  w: RollingWindow, tf: dict[str, Any], now: float) -> str | None:
        f = self.cfg["footprint"]
        p1, p2 = d["p1"], d["p2"]
        if p1 <= 0 or p2 <= 0: return "weak_price"
        move = (st.last_price - p2) if d["side"] == "BUY" else (p2 - st.last_price)
        d["since"] = move / p2 * 100.0
        d["fp"] = series.footprint_check(d, st.last_price, f)
        if now - d["ts"] > tf["max_swing_age_sec"]: return "old_swing"
        if d["since"] > tf["div_max_move_since_swing_pct"]: return "late"
        if d["since"] < -tf["div_max_move_since_swing_pct"]: return "failed"   # price broke past swing B
        if abs(p2 - p1) / p1 * 100.0 < tf["div_min_price_diff_pct"]: return "weak_price"
        if not d["fp"]["available"]:
            if f["required"]: return "fp_unavailable"
        elif d["fp"]["confirms"] < f["min_confirms"]:
            if f["required"]: return "fp_weak"
        return None

    def _alert_div(self, sym: str, tf: str, cl: str, d: dict[str, Any], st: OrderFlowState,
                   now: float, key: str, closed: dict[str, Any]) -> None:
        side, kind, fp = d["side"], d["kind"], d["fp"]
        w = st.windows[tf]
        trig_label = candle_label(int(self.cfg["timeframes"][tf]["confirm_candle_seconds"]))
        extreme = "low" if side == "BUY" else "high"
        if fp["available"]:
            heavy = "sell" if side == "BUY" else "buy"
            imb = "buy" if side == "BUY" else "sell"
            fp_line = (f"Footprint (row {fp['row']:.8g}): absorption {'YES' if fp['absorb'] else 'no'} "
                       f"({heavy} vol at {extreme} rows {fp['frac'] * 100:.1f}% of candle), "
                       f"stacked {imb} imbalances x{fp['stacked']}")
        else:
            fp_line = "Footprint: n/a"
        t_delta = closed["buy"] - closed["sell"]
        body = (
            f"Symbol: {sym}\n"
            f"Timeframe: {tf} (CVD swing divergence on {cl} candles)\n"
            f"Trigger: {trig_label} candle CLOSED ({'green' if closed['close'] > closed['open'] else 'red'}, "
            f"delta {t_delta:+,.0f} USDT)\n"
            f"Side: {side}\n"
            f"Signal: {DIV_TEXT[(side, kind)]}\n"
            f"Price: {st.last_price:.8g}\n"
            f"Swing price A->B: {d['p1']:.8g} -> {d['p2']:.8g} "
            f"({d['gap']} candles apart{', B forming' if d['forming'] else ''})\n"
            f"Move since swing: {d['since']:+.2f}% (swing B {max(now - d['ts'], 0.0) / 60:.0f} min ago)\n"
            f"CVD swing value (USDT): {d['v1']:+,.0f} -> {d['v2']:+,.0f}\n"
            f"{fp_line}\n"
            f"CVD% ({tf}): {w.cvd_pct:+.1f}\n"
            f"Regime 4h CVD%: {st.windows['4h'].cvd_pct:+.1f}\n"
            f"Spread bps: {st.spread_bps:.2f}"
        )
        if self.book.cap_reached():
            log.info("[%s] %s %s %s-DIV (daily cap %d reached, log only) | %s", sym, tf, side, kind,
                     self.cfg["max_alerts_per_day"], body.replace("\n", " | "))
            self.book.last_alert[key] = now; return
        self.book.register(key, sym, tf, side, now, label=f"{tf}/{cl} {side} {kind}-div")
        self.health.note_signal()
        log.info("[%s] %s/%s %s %s-DIV | %s", sym, tf, cl, side, kind, body.replace("\n", " | "))
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._spawn(self.alerter.send(f"{sym} {side} Divergence [{tf}] {trig_label} close", body, "high", tags))

    def _alert(self, sym: str, tf: str, side: str, st: OrderFlowState, m: dict[str, float],
               age: float, now: float, key: str, fpf: dict[str, Any] | None = None) -> None:
        c = self.cfg
        sl = f"{int(c['short_window_seconds'] // 60)}m"
        obi_label = f"OBI ({tf} avg)" if c["obi_window_average"] else "OBI"
        body = (
            f"Symbol: {sym}\n"
            f"Timeframe: {tf}\n"
            f"Side: {side}\n"
            f"Price: {st.last_price:.8g}\n"
            f"CVD% ({tf}): {m['cvd']:+.1f}\n"
            f"CVD% ({sl}): {m['scvd']:+.1f}\n"
            f"{obi_label}: {m['obi']:+.2f}\n"
            f"Spread bps: {st.spread_bps:.2f}\n"
            f"Move ({tf}): {m['move']:.2f}%\n"
            f"Move ({sl}): {m['smove']:.2f}%\n"
            f"Regime 4h CVD%: {st.windows['4h'].cvd_pct:+.1f}\n"
            f"CVD cross age: {age / 60:.1f} min"
        )
        if fpf and fpf["available"]:
            body += (f"\nFootprint (row {fpf['row']:.8g}): {side.lower()} volume {fpf['ratio']:.1f}x the other side "
                     f"at current price, stacked imbalances x{fpf['stacked']}")
        else:
            body += "\nFootprint: n/a"
        if self.book.cap_reached():
            log.info("[%s] %s %s SIGNAL (daily cap %d reached, log only) | %s", sym, tf, side,
                     c["max_alerts_per_day"], body.replace("\n", " | "))
            self.book.last_alert[key] = now; return
        self.book.register(key, sym, tf, side, now)
        self.health.note_signal()
        log.info("[%s] %s %s SIGNAL | %s", sym, tf, side, body.replace("\n", " | "))
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._spawn(self.alerter.send(f"{sym} {side} Signal [{tf}]", body, "high", tags))

    async def _diag_loop(self) -> None:
        minutes = self.cfg["diag_interval_min"]
        while not await self._sleep_or_stop(minutes * 60):
            if not self.connected: continue
            now = time.time()
            log.info("[DIAG] health: %s | adaptive regime=%s mult=%.2f (cvd=%.1f short_cvd=%.1f obi=%.2f)",
                     self.health.summary(now), self.adapt.regime, self.adapt.mult,
                     self.adapt.cvd, self.adapt.short_cvd, self.adapt.obi)
            counts, self.block_counts = self.block_counts, {}
            groups: dict[str, dict[str, int]] = {}
            for k, n in counts.items():
                grp, reason = k.split(":", 1); groups.setdefault(grp, {})[reason] = n
            for grp, d in sorted(groups.items()):
                tot = sum(d.values())
                parts = " ".join(f"{r}={n / tot * 100:.0f}%" for r, n in sorted(d.items(), key=lambda kv: -kv[1]))
                log.info("[DIAG] %s gates over last %dmin (share of checks): %s", grp, minutes, parts)
            for sym, st in self.states.items():
                if self._global_block(st, now): continue
                for name, tf in self.cfg["timeframes"].items():
                    reason, _, m = self._tf_status(st, name, tf)
                    move = "filling" if m["move"] >= 999.0 else f"{m['move']:.2f}%"
                    smove = "filling" if m["smove"] >= 999.0 else f"{m['smove']:.2f}%"
                    log.info("[DIAG] %s %s gate=%s cvd=%+.1f cvd15=%+.1f obi=%+.2f "
                             "move=%s(max %.1f%%) move15=%s(max %.1f%%) 4hcvd=%+.1f",
                             sym, name, reason, m["cvd"], m["scvd"], m["obi"],
                             move, tf["max_recent_move_pct"], smove, self.cfg["short_max_move_pct"],
                             st.windows["4h"].cvd_pct)

    async def _save_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["state_save_interval_sec"]):
            await self.book.save()
            log.info("[ENGINE] State saved | alerts_today=%d connected=%s", self.book.alerts_today, self.connected)

    async def _rollover(self) -> None:
        label = self.book.day
        summary = self.book.build_summary(label)
        log.info("[ENGINE] Daily rollover:\n%s", summary)
        self._spawn(self.alerter.send(f"Signal Summary {label}", summary, "default", "chart_with_upwards_trend"))
        self.book.reset_day(utc_day())
        await self.book.save()

    async def _daily_loop(self) -> None:
        while True:
            now = datetime.now(timezone.utc)
            nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=1, microsecond=0)
            if await self._sleep_or_stop((nxt - now).total_seconds()): return
            await self._rollover()


def main() -> None:
    load_dotenv(); setup_logging()
    code = 0
    try: code = asyncio.run(_amain())
    except KeyboardInterrupt: log.info("[ENGINE] Interrupted")
    sys.exit(code)


async def _amain() -> int:
    return await Engine().run()


if __name__ == "__main__":
    main()