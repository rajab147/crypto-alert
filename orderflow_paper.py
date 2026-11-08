#!/usr/bin/env python3
"""
orderflow_paper.py
Early Reversal + Breakout alert engine.
Alerts only. No trading. No order execution.
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
    "min_quote_volume_24h": 10_000_000,
    "min_trades_24h": 10_000,
    "max_price_change_24h_pct": 25.0,
    "spread_max_bps": 15.0,
    "eval_interval_sec": 3,
    "warmup_seconds": 5,
    "bucket_seconds": 5,
    "min_window_fill": 0.75,
    "obi_window_average": True,
    "diag_interval_min": 30,
    "history_candles": 60,
    "short_window_seconds": 900,
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
    "health": {
        "silent_limit_sec": 120,
        "check_interval_sec": 60,
        "alert_on_silent": True,
    },
    "footprint": {
        "row_bps": 3.0,
        "keep_candles": 12,
        "zone_rows": 3,
        "imbalance_ratio": 3.0,
        "imbalance_min_frac": 0.004,
    },
    "reversal_breakout": {
        "enabled": True,
        "reversal_enabled": True,
        "breakout_enabled": True,
        "early_enabled": True,
        "confirmed_enabled": True,
        "fractal_left": 2,
        "fractal_right": 1,
        "swing_lookback_candles": 40,
        "sweep_min_pct": 0.05,
        "sweep_max_pct": 2.0,
        "reclaim_buffer_pct": 0.03,
        "recovery_min_pct": 0.08,
        "absorption_min_frac": 0.06,
        "absorption_zone_rows": 3,
        "delta_confirm_frac_pct": 1.0,
        "cvd_confirm_pct": 1.0,
        "obi_confirm": 0.01,
        "range_lookback_candles": 30,
        "range_min_height_pct": 0.35,
        "range_max_height_pct": 6.0,
        "range_tolerance": 0.30,
        "range_max_age_sec": 172_800,
        "boundary_proximity_frac": 0.30,
        "breakout_confirmed_buffer_pct": 0.05,
        "setup_max_age_sec": 21_600,
        "invalidation_buffer_pct": 0.20,
        "early_cooldown_sec": 900,
        "confirmed_cooldown_sec": 1_800,
        "confirm_max_delay_sec": 300,
    },
    "timeframes": {
        "1h": {
            "seconds": 3600,
            "cooldown_min": 45,
            "candle_seconds": [3600],
            "base_seconds": 60,
            "confirm_candle_seconds": 900,
            "max_swing_age_sec": 28_800,
            "max_recent_move_pct": 1.5,
        },
        "4h": {
            "seconds": 14_400,
            "cooldown_min": 180,
            "candle_seconds": [14_400],
            "base_seconds": 300,
            "confirm_candle_seconds": 1_800,
            "max_swing_age_sec": 144_000,
            "max_recent_move_pct": 2.5,
        },
    },
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
KLINE_INTERVALS = {
    60: "1m", 180: "3m", 300: "5m", 900: "15m", 1800: "30m",
    3600: "1h", 7200: "2h", 14400: "4h",
}


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


def validate_config(cfg: dict[str, Any]) -> None:
    errs: list[str] = []

    def num(d: dict[str, Any], key: str, lo: float, hi: float | None = None) -> None:
        v = d.get(key)
        if not isinstance(v, (int, float)) or isinstance(v, bool):
            errs.append(f"'{key}' must be number, got {v!r}")
            return
        if v < lo or (hi is not None and v > hi):
            errs.append(f"'{key}' must be in [{lo}, {hi}], got {v}")

    num(cfg, "max_symbols", 1, 200)
    num(cfg, "min_quote_volume_24h", 0)
    num(cfg, "min_trades_24h", 0)
    num(cfg, "max_price_change_24h_pct", 0, 100)
    num(cfg, "spread_max_bps", 0.1, 200)
    num(cfg, "eval_interval_sec", 1, 60)
    num(cfg, "bucket_seconds", 1, 60)
    num(cfg, "min_window_fill", 0.1, 1.0)
    num(cfg, "history_candles", 10)
    num(cfg, "short_window_seconds", 60)
    num(cfg, "max_alerts_per_day", 1)
    num(cfg, "stale_seconds", 1)

    if cfg.get("depth_levels") not in (5, 10, 20):
        errs.append(f"depth_levels invalid: {cfg.get('depth_levels')!r}")

    rb = cfg.get("reversal_breakout")
    if not isinstance(rb, dict):
        errs.append("reversal_breakout must be dict")
    else:
        for k in (
            "fractal_left", "fractal_right", "swing_lookback_candles",
            "sweep_min_pct", "sweep_max_pct", "reclaim_buffer_pct",
            "recovery_min_pct", "absorption_min_frac", "absorption_zone_rows",
            "delta_confirm_frac_pct", "cvd_confirm_pct", "obi_confirm",
            "range_lookback_candles", "range_min_height_pct", "range_max_height_pct",
            "range_tolerance", "range_max_age_sec", "boundary_proximity_frac",
            "breakout_confirmed_buffer_pct", "setup_max_age_sec",
            "invalidation_buffer_pct", "early_cooldown_sec",
            "confirmed_cooldown_sec", "confirm_max_delay_sec",
        ):
            num(rb, k, 0)

    tfs = cfg.get("timeframes")
    if not isinstance(tfs, dict) or not tfs:
        errs.append("timeframes must be non-empty dict")
    else:
        for name, tf in tfs.items():
            if not isinstance(tf, dict):
                errs.append(f"timeframes['{name}'] must be dict")
                continue
            for k in ("seconds", "cooldown_min", "confirm_candle_seconds", "max_swing_age_sec", "max_recent_move_pct"):
                num(tf, k, 0)
            base = tf.get("base_seconds")
            if base not in KLINE_INTERVALS:
                errs.append(f"timeframes['{name}']['base_seconds'] invalid")
            cs = tf.get("candle_seconds")
            if not isinstance(cs, list) or not cs:
                errs.append(f"timeframes['{name}']['candle_seconds'] invalid")
            else:
                for sec in cs:
                    if not isinstance(sec, int) or sec <= 0 or (isinstance(base, int) and base and sec % base):
                        errs.append(f"timeframes['{name}'] candle {sec} invalid vs base {base}")
            ct = tf.get("confirm_candle_seconds")
            if not isinstance(ct, int) or ct <= 0 or (isinstance(base, int) and base and ct % base):
                errs.append(f"timeframes['{name}']['confirm_candle_seconds'] invalid")
            elif ct not in KLINE_INTERVALS:
                errs.append(f"timeframes['{name}']['confirm_candle_seconds'] must be kline size")

    if errs:
        raise ValueError(" | ".join(errs))


class SymbolSelector:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg

    @staticmethod
    def is_excluded_base(base: str, trading_bases: set[str]) -> bool:
        if base in STABLE_BASES or base.startswith("USD"):
            return True
        if base.endswith("USD") and len(base) > 3:
            return True
        for suf in LEVERAGED_SUFFIXES:
            if base.endswith(suf) and base[: -len(suf)] in trading_bases:
                return True
        return False

    async def _get(self, session: aiohttp.ClientSession, url: str) -> Any:
        last: Exception | None = None
        for attempt in range(1, self.cfg["http_retries"] + 1):
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as exc:
                last = exc
                log.warning("[ENGINE] REST %s failed (attempt %d): %s", url, attempt, exc)
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"REST request failed: {url}: {last}")

    def _check(self, t: dict[str, Any], trading: set[str], trading_bases: set[str]) -> dict[str, Any] | None:
        sym = str(t["symbol"])
        if not sym.endswith("USDT") or sym not in trading:
            return None
        base = sym[: -len("USDT")]
        if not base or self.is_excluded_base(base, trading_bases):
            return None
        qv = float(t["quoteVolume"])
        cnt = int(t["count"])
        chg = float(t["priceChangePercent"])
        if qv < self.cfg["min_quote_volume_24h"] or cnt < self.cfg["min_trades_24h"]:
            return None
        if abs(chg) > self.cfg["max_price_change_24h_pct"]:
            return None
        return {"symbol": sym, "quote_volume": qv, "count": cnt, "change_pct": chg}

    async def select(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        tickers, info = await asyncio.gather(self._get(session, TICKER_URL), self._get(session, INFO_URL))
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
        usdt = [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")]
        picked: list[dict[str, Any]] = []
        for t in usdt:
            try:
                item = self._check(t, trading, trading_bases)
            except (KeyError, ValueError, TypeError):
                continue
            if item:
                item["tick"] = ticks.get(item["symbol"], 0.0)
                picked.append(item)
        picked.sort(key=lambda x: x["quote_volume"], reverse=True)
        picked = picked[: self.cfg["max_symbols"]]
        log.info("[ENGINE] Selected %d symbols after filtering %d total USDT pairs", len(picked), len(usdt))
        for i, p in enumerate(picked, 1):
            log.info(
                "[%s] #%d rank=$%s trades24h=%s change24h=%+.2f%%",
                p["symbol"], i, f"{p['quote_volume']:,.0f}", f"{p['count']:,}", p["change_pct"],
            )
        return picked


class RollingWindow:
    def __init__(self, seconds: float, bucket_sec: float) -> None:
        self.window = float(seconds)
        self.bucket = float(bucket_sec)
        self.reset()

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
        if is_buy:
            b[1] += quote
            self.buy_vol += quote
        else:
            b[2] += quote
            self.sell_vol += quote
        b[3] += 1
        self.count += 1
        if b[4] == 0.0:
            b[4] = price
        self.evict(now)

    def add_obi(self, obi: float, now: float) -> None:
        b = self._current(now)
        b[5] += obi
        b[6] += 1
        self.obi_sum += obi
        self.obi_n += 1

    def evict(self, now: float) -> None:
        limit = now - self.window
        while self.buckets and (self.buckets[0][0] + 1) * self.bucket <= limit:
            _, buy, sell, cnt, _, osum, on = self.buckets.popleft()
            self.buy_vol -= buy
            self.sell_vol -= sell
            self.count -= int(cnt)
            self.obi_sum -= osum
            self.obi_n -= int(on)
            if self.count <= 0:
                self.count, self.buy_vol, self.sell_vol = 0, 0.0, 0.0
            if self.obi_n <= 0:
                self.obi_n, self.obi_sum = 0.0, 0.0

    @property
    def window_volume(self) -> float:
        return max(self.buy_vol + self.sell_vol, 0.0)

    @property
    def trade_count(self) -> int:
        return self.count

    @property
    def cvd_pct(self) -> float:
        total = self.buy_vol + self.sell_vol
        return (self.buy_vol - self.sell_vol) / total * 100.0 if total > 0 else 0.0

    @property
    def avg_obi(self) -> float | None:
        return self.obi_sum / self.obi_n if self.obi_n > 0 else None

    def recent_move_pct(self, last_price: float, last_trade_ts: float, min_fill: float) -> float:
        for b in self.buckets:
            if b[4] > 0:
                if last_trade_ts - b[0] * self.bucket < self.window * min_fill:
                    return 999.0
                return abs(last_price - b[4]) / b[4] * 100.0
        return 999.0


class TriggerTracker:
    def __init__(self, sec: float) -> None:
        self.sec = float(sec)
        self.reset()

    def reset(self) -> None:
        self.cur: dict[str, Any] | None = None
        self.last_closed: dict[str, Any] | None = None
        self.prev_closed: dict[str, Any] | None = None
        self.pending: dict[str, Any] | None = None

    def _close(self) -> None:
        if self.cur is None:
            return
        self.prev_closed, self.last_closed = self.last_closed, self.cur
        self.pending, self.cur = self.cur, None

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        cid = int(now // self.sec)
        if self.cur is not None and self.cur["id"] < cid:
            self._close()
        if self.cur is None:
            consecutive = self.last_closed is not None and self.last_closed["id"] == cid - 1
            partial = (now - cid * self.sec) > 5.0 and not consecutive
            self.cur = {
                "id": cid,
                "open": price,
                "high": price,
                "low": price,
                "close": price,
                "buy": 0.0,
                "sell": 0.0,
                "partial": partial,
            }
        c = self.cur
        c["high"] = max(c["high"], price)
        c["low"] = min(c["low"], price)
        c["close"] = price
        if is_buy:
            c["buy"] += quote
        else:
            c["sell"] += quote

    def poll(self, now: float) -> dict[str, Any] | None:
        cid = int(now // self.sec)
        if self.cur is not None and self.cur["id"] < cid:
            self._close()
        p, self.pending = self.pending, None
        if p is None or p["id"] != cid - 1:
            return None
        return p

    def seed(self, rows: list[list[Any]]) -> None:
        groups: dict[int, dict[str, Any]] = {}
        order: list[int] = []
        for r in rows:
            cid = int(float(r[0]) // 1000 // self.sec)
            qv = float(r[7])
            tb = float(r[10])
            g = groups.get(cid)
            if g is None:
                g = groups[cid] = {
                    "id": cid,
                    "open": float(r[1]),
                    "high": float(r[2]),
                    "low": float(r[3]),
                    "close": float(r[4]),
                    "buy": 0.0,
                    "sell": 0.0,
                    "partial": False,
                }
            order.append(cid)
            g["high"] = max(g["high"], float(r[2]))
            g["low"] = min(g["low"], float(r[3]))
            g["close"] = float(r[4])
            g["buy"] += tb
            g["sell"] += qv - tb
        self.reset()
        now_cid = int(time.time() // self.sec)
        for cid in order:
            g = groups[cid]
            if cid >= now_cid:
                self.cur = g
            else:
                self.prev_closed, self.last_closed = self.last_closed, g


class Candle:
    __slots__ = (
        "id", "open", "high", "low", "close", "buy", "sell",
        "high_ts", "low_ts", "high_live", "low_live", "partial", "row", "fp",
    )

    def __init__(self, cid: int, price: float, ts: float, row: float, live: bool, partial: bool = False) -> None:
        self.id = cid
        self.open = self.high = self.low = self.close = price
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
        self.reset()

    def reset(self) -> None:
        self.candles: deque[Candle] = deque(maxlen=self.max_candles)

    def _row(self, price: float) -> float:
        raw = price * self.fp_cfg["row_bps"] / 10_000.0
        return round(max(self.tick, round(raw / self.tick) * self.tick), 12)

    def load_history(self, rows: list[list[Any]]) -> None:
        self.reset()
        cur: Candle | None = None
        for r in rows:
            ts = float(r[6]) / 1000.0
            cid = int(float(r[0]) // 1000 // self.candle)
            if cur is None or cur.id != cid:
                cur = Candle(cid, float(r[1]), ts, 0.0, live=False)
                self.candles.append(cur)
            hi, lo = float(r[2]), float(r[3])
            if hi > cur.high:
                cur.high, cur.high_ts = hi, ts
            if lo < cur.low:
                cur.low, cur.low_ts = lo, ts
            cur.close = float(r[4])
            tb = float(r[10])
            qv = float(r[7])
            cur.buy += tb
            cur.sell += qv - tb
        for c in self.candles:
            c.row = self._row(c.close)

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        cid = int(now // self.candle)
        c = self.candles[-1] if self.candles else None
        if c is None or c.id != cid:
            if c is not None:
                c.partial = False
            partial = c is None and (now - cid * self.candle) > 5.0
            c = Candle(cid, price, now, self._row(price), live=True, partial=partial)
            self.candles.append(c)
        keep = int(self.fp_cfg["keep_candles"])
        if len(self.candles) > keep:
            self.candles[-keep - 1].fp = None
        if price > c.high:
            c.high, c.high_ts, c.high_live = price, now, True
        if price < c.low:
            c.low, c.low_ts, c.low_live = price, now, True
        c.close = price
        if is_buy:
            c.buy += quote
        else:
            c.sell += quote
        if c.fp is None:
            c.fp = {}
        lvl = int(price // c.row)
        cell = c.fp.get(lvl)
        if cell is None:
            cell = c.fp[lvl] = [0.0, 0.0]
        cell[1 if is_buy else 0] += quote

    def pivots(self, pivot: str, left: int, right: int) -> list[int]:
        cs = list(self.candles)
        out: list[int] = []
        if len(cs) < left + right + 1:
            return out
        for i in range(left, len(cs) - right):
            if any(cs[j].partial for j in range(i - left, i + right + 1)):
                continue
            c = cs[i]
            if pivot == "low":
                ok = all(c.low <= cs[i - k].low for k in range(1, left + 1)) and all(c.low < cs[i + k].low for k in range(1, right + 1))
            else:
                ok = all(c.high >= cs[i - k].high for k in range(1, left + 1)) and all(c.high > cs[i + k].high for k in range(1, right + 1))
            if ok:
                out.append(i)
        return out

    @staticmethod
    def absorption_stats(c: Candle | None, side: str, last_price: float, zone_rows: int, min_frac: float) -> dict[str, Any]:
        res = {"available": False, "frac": 0.0, "recovery": 0.0}
        if c is None:
            return res
        if side == "BUY" and c.low > 0:
            res["recovery"] = (last_price - c.low) / c.low * 100.0
        elif side == "SELL" and c.high > 0:
            res["recovery"] = (c.high - last_price) / c.high * 100.0
        if not c.fp or c.row <= 0:
            return res
        total = sum(a + b for a, b in c.fp.values())
        if total <= 0:
            return res
        levels = sorted(c.fp.keys())
        z = max(1, int(zone_rows))
        if side == "BUY":
            zone = levels[:z]
            frac = sum(c.fp[lv][0] for lv in zone) / total
        else:
            zone = levels[-z:]
            frac = sum(c.fp[lv][1] for lv in zone) / total
        res["available"] = True
        res["frac"] = frac
        return res


class OrderFlowState:
    def __init__(
        self,
        symbol: str,
        timeframes: dict[str, dict[str, Any]],
        bucket_sec: float,
        levels: int,
        history_candles: int = 60,
        short_sec: float = 900.0,
        tick: float = 0.0,
        fp_cfg: dict[str, Any] | None = None,
    ) -> None:
        self.symbol = symbol
        self.levels = levels
        self.windows = {name: RollingWindow(tf["seconds"], bucket_sec) for name, tf in timeframes.items()}
        self.short = RollingWindow(short_sec, bucket_sec)
        self.mtf_windows = {"30m": RollingWindow(1800.0, bucket_sec)}
        fp_cfg = fp_cfg if fp_cfg is not None else CONFIG["footprint"]
        self.candles = {
            name: {int(sec): CandleSeries(sec, history_candles, tick, fp_cfg) for sec in tf["candle_seconds"]}
            for name, tf in timeframes.items()
        }
        self.trig = {name: TriggerTracker(tf["confirm_candle_seconds"]) for name, tf in timeframes.items()}
        self.rb: dict[str, dict[str, Any]] = {}
        self.reset()

    def reset(self) -> None:
        for w in self.windows.values():
            w.reset()
        self.short.reset()
        for w in self.mtf_windows.values():
            w.reset()
        for group in self.candles.values():
            for s in group.values():
                s.reset()
        for t in self.trig.values():
            t.reset()
        self.rb = {}
        self.last_price = 0.0
        self.last_trade_ts = 0.0
        self.bid_vol = self.ask_vol = 0.0
        self.best_bid = self.best_ask = 0.0
        self.has_depth = False
        self.last_update = 0.0

    def add_trade(self, price: float, qty: float, buyer_is_maker: bool, now: float) -> None:
        quote = price * qty
        is_buy = not buyer_is_maker
        for w in self.windows.values():
            w.add_trade(price, quote, is_buy, now)
        self.short.add_trade(price, quote, is_buy, now)
        for w in self.mtf_windows.values():
            w.add_trade(price, quote, is_buy, now)
        for group in self.candles.values():
            for s in group.values():
                s.add_trade(price, quote, is_buy, now)
        for t in self.trig.values():
            t.add_trade(price, quote, is_buy, now)
        self.last_price = price
        self.last_trade_ts = now
        self.last_update = now

    def update_depth(self, bids: list[list[str]], asks: list[list[str]], now: float) -> None:
        bids, asks = bids[: self.levels], asks[: self.levels]
        bb, ba = float(bids[0][0]), float(asks[0][0])
        if bb <= 0 or ba < bb:
            raise ValueError("crossed or invalid book")
        self.bid_vol = sum(float(q) for _, q in bids)
        self.ask_vol = sum(float(q) for _, q in asks)
        self.best_bid, self.best_ask, self.has_depth, self.last_update = bb, ba, True, now
        obi = self.obi_now
        for w in self.windows.values():
            w.add_obi(obi, now)
        self.short.add_obi(obi, now)
        for w in self.mtf_windows.values():
            w.add_obi(obi, now)

    def evict(self, now: float) -> None:
        for w in self.windows.values():
            w.evict(now)
        self.short.evict(now)
        for w in self.mtf_windows.values():
            w.evict(now)

    def is_stale(self, now: float, limit: float) -> bool:
        return self.last_update == 0.0 or now - self.last_update > limit

    @property
    def obi_now(self) -> float:
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0

    def obi_for(self, name: str, use_average: bool = True) -> float:
        if use_average:
            avg = self.windows[name].avg_obi
            if avg is not None:
                return avg
        return self.obi_now

    @property
    def spread_bps(self) -> float:
        if not self.has_depth:
            return float("inf")
        mid = (self.best_ask + self.best_bid) / 2.0
        return (self.best_ask - self.best_bid) / mid * 10_000.0

    def recent_move_pct(self, name: str, min_fill: float) -> float:
        return self.windows[name].recent_move_pct(self.last_price, self.last_trade_ts, min_fill)

    def frame_window(self, frame: str) -> RollingWindow | None:
        if frame in self.windows:
            return self.windows[frame]
        if frame in self.mtf_windows:
            return self.mtf_windows[frame]
        if frame == "15m":
            return self.short
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
        if self.session and not self.session.closed:
            await self.session.close()

    async def send(self, title: str, body: str, priority: str = "high", tags: str = "") -> bool:
        if not self.url or self.session is None:
            return False
        headers = {"Title": title.encode("ascii", "ignore").decode(), "Priority": priority}
        if tags:
            headers["Tags"] = tags
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        try:
            async with self.session.post(self.url, data=body.encode("utf-8"), headers=headers) as resp:
                if resp.status >= 300:
                    log.warning("[ENGINE] ntfy HTTP %s for '%s'", resp.status, title)
                    return False
                return True
        except Exception as exc:
            log.warning("[ENGINE] ntfy send failed for '%s': %s", title, exc)
            return False


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
        self.window_msgs = 0
        self.total_signals = 0
        self.eval_cycles = 0
        self.eval_errors = 0
        self.last_check_ts = 0.0
        self.last_rate = 0.0
        self.silent_alerted = False

    def note_message(self, kind: str, now: float) -> None:
        self.window_msgs += 1
        self.last_msg_ts = now
        self.silent_alerted = False

    def note_signal(self) -> None:
        self.total_signals += 1

    def note_eval(self, error: bool = False) -> None:
        self.eval_cycles += 1
        if error:
            self.eval_errors += 1

    def check(self, now: float, connected: bool) -> None:
        if self.last_check_ts == 0.0:
            self.last_check_ts = now
            return
        elapsed = now - self.last_check_ts
        if elapsed < self.check_interval:
            return
        self.last_rate = self.window_msgs / max(elapsed, 1.0)
        self.window_msgs = 0
        self.last_check_ts = now
        if connected and self.last_msg_ts > 0 and (now - self.last_msg_ts) > self.silent_limit:
            if not self.silent_alerted and self.alert_on_silent and self.spawn is not None:
                self.silent_alerted = True
                gap = now - self.last_msg_ts
                self.spawn(self.alerter.send(
                    "Orderflow bot: data stream silent",
                    f"No WebSocket message for {gap:.0f}s while connected. Bot may be stuck.",
                    "high",
                    "warning",
                ))

    def summary(self, now: float) -> str:
        up = (now - self.start_ts) / 3600.0
        return (
            f"uptime={up:.1f}h msg_rate={self.last_rate:.1f}/s signals={self.total_signals} "
            f"eval_cycles={self.eval_cycles} eval_errors={self.eval_errors}"
        )


class AlertBook:
    def __init__(self, cfg: dict[str, Any]) -> None:
        self.cfg = cfg
        self.day = utc_day()
        self.alerts_today = 0
        self.last_alert: dict[str, float] = {}
        self.today_symbols: dict[str, int] = {}
        self.today_counts: dict[str, int] = {}

    def load(self) -> None:
        if not os.path.exists(STATE_FILE):
            log.info("[ENGINE] No saved state; starting fresh")
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.day = str(d.get("day", self.day))
            self.alerts_today = int(d.get("alerts_today", 0))
            self.last_alert = {k: float(v) for k, v in d.get("last_alert", {}).items()}
            self.today_symbols = {k: int(v) for k, v in d.get("today_symbols", {}).items()}
            self.today_counts = {k: int(v) for k, v in d.get("today_counts", {}).items()}
            log.info("[ENGINE] Loaded state: day=%s alerts_today=%d", self.day, self.alerts_today)
        except Exception as exc:
            log.error("[ENGINE] Corrupt state file (%s); starting fresh", exc)

    def snapshot(self) -> dict[str, Any]:
        return {
            "day": self.day,
            "alerts_today": self.alerts_today,
            "last_alert": self.last_alert,
            "today_symbols": self.today_symbols,
            "today_counts": self.today_counts,
            "saved_at": datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _write_atomic(data: dict[str, Any]) -> None:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)

    async def save(self) -> None:
        try:
            await asyncio.to_thread(self._write_atomic, self.snapshot())
        except Exception as exc:
            log.error("[ENGINE] State save failed: %s", exc)

    def on_cooldown(self, key: str, now: float, minutes: float) -> bool:
        return now - self.last_alert.get(key, 0.0) < minutes * 60

    def cap_reached(self) -> bool:
        return self.alerts_today >= self.cfg["max_alerts_per_day"]

    def register(self, key: str, symbol: str, tf: str, side: str, now: float, label: str | None = None) -> None:
        self.alerts_today += 1
        self.last_alert[key] = now
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
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    def request_stop(self, why: str) -> None:
        if not self.stop.is_set():
            log.info("[ENGINE] Stop requested (%s)", why)
            self.stop.set()

    async def _sleep_or_stop(self, seconds: float) -> bool:
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=max(seconds, 0.0))
            return True
        except asyncio.TimeoutError:
            return False

    def _count(self, key: str) -> None:
        self.block_counts[key] = self.block_counts.get(key, 0) + 1

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda s, f, n=sig.name: loop.call_soon_threadsafe(self.request_stop, n))

        try:
            validate_config(self.cfg)
        except ValueError as exc:
            log.error("[CONFIG] Startup aborted, fix config first: %s", exc)
            return 1

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
                log.error(
                    "[ENGINE] HTTP 451 = Binance blocks this server's region. Run from a "
                    "non-US/non-restricted host (VPS), or set BINANCE_REST_BASE / BINANCE_WS_BASE."
                )
            await self.alerter.close()
            return 1

        symbols = [p["symbol"] for p in picked]
        if not symbols:
            log.error("[ENGINE] No symbols passed the filters; exiting")
            await self.alerter.close()
            return 1

        ticks = {p["symbol"]: float(p.get("tick", 0.0)) for p in picked}
        for s in symbols:
            self.states[s] = OrderFlowState(
                s,
                self.cfg["timeframes"],
                self.cfg["bucket_seconds"],
                lvl,
                int(self.cfg["history_candles"]),
                float(self.cfg["short_window_seconds"]),
                ticks.get(s, 0.0),
                self.cfg["footprint"],
            )

        if self.book.day != utc_day():
            await self._rollover()

        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@aggTrade/{s.lower()}@depth{lvl}@100ms" for s in symbols
        )
        log.info(
            "[ENGINE] Starting with %d symbols, timeframes=%s (alerts only, no trading)",
            len(symbols),
            ",".join(self.cfg["timeframes"]),
        )

        tasks = [
            asyncio.create_task(c)
            for c in (self._ws_loop(url), self._eval_loop(), self._save_loop(), self._daily_loop(), self._diag_loop())
        ]
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._shutdown()
        return 0

    async def _shutdown(self) -> None:
        await self.book.save()
        if self._bg:
            await asyncio.wait(self._bg, timeout=self.cfg["ntfy_timeout_sec"])
        await self.alerter.close()
        log.info("[ENGINE] Shutdown complete | alerts_today=%d", self.book.alerts_today)

    def _handle_message(self, raw: str | bytes) -> None:
        msg = json.loads(raw)
        stream, data = msg["stream"], msg["data"]
        now = time.time()
        self.last_msg_ts = now
        self._wiped_since_msg = False
        self.health.note_message("trade" if "@aggTrade" in stream else "depth", now)
        sym = stream.split("@", 1)[0].upper()
        st = self.states.get(sym)
        if st is None:
            return
        try:
            if "@aggTrade" in stream:
                st.add_trade(float(data["p"]), float(data["q"]), bool(data["m"]), now)
            elif "@depth" in stream:
                st.update_depth(data["bids"], data["asks"], now)
        except (KeyError, ValueError, TypeError, IndexError) as exc:
            log.warning("[%s] Bad %s message skipped: %s", sym, stream.split("@", 1)[1], exc)

    def _on_connect(self) -> None:
        self.stream_start = time.time()

    async def _prepare_state(self) -> None:
        gap = time.time() - self.last_msg_ts if self.last_msg_ts else float("inf")
        if gap <= self.cfg["reconnect_wipe_after_sec"] or self._wiped_since_msg:
            return
        for st in self.states.values():
            st.reset()
        self._wiped_since_msg = True
        log.info("[ENGINE] Windows wiped (%s)", "start" if not self.last_msg_ts else f"gap {gap:.0f}s")
        try:
            await self._backfill_all()
        except Exception as exc:
            log.warning("[ENGINE] Candle backfill failed (%s); history will build live", exc)

    async def _backfill_all(self) -> None:
        hist = int(self.cfg["history_candles"])
        selector = SymbolSelector(self.cfg)
        sem = asyncio.Semaphore(8)
        loaded = 0
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def one(sym: str, st: OrderFlowState, tf_name: str, sec: int, series: CandleSeries) -> None:
                nonlocal loaded
                tf = self.cfg["timeframes"][tf_name]
                base = int(tf["base_seconds"])
                iv = KLINE_INTERVALS.get(base)
                if iv is None or sec % base:
                    log.warning("[%s] Bad base_seconds=%d for %ds candles; no backfill", sym, base, sec)
                    return
                start_ms = int((time.time() // sec - hist) * sec * 1000)
                async with sem:
                    try:
                        rows: list[list[Any]] = []
                        while True:
                            data = await selector._get(
                                session,
                                f"{REST_BASE}/api/v3/klines?symbol={sym}&interval={iv}&startTime={start_ms}&limit=1000",
                            )
                            rows.extend(data)
                            if len(data) < 1000:
                                break
                            start_ms = int(data[-1][0]) + base * 1000
                        series.load_history(rows)
                        st.trig[tf_name].seed(rows)
                        loaded += 1
                    except Exception as exc:
                        log.warning("[%s] Kline backfill %s failed: %s", sym, iv, exc)

            await asyncio.gather(
                *(
                    one(sym, st, name, sec, series)
                    for sym, st in self.states.items()
                    for name, group in st.candles.items()
                    for sec, series in group.items()
                )
            )
        total = sum(len(g) for st in self.states.values() for g in st.candles.values())
        log.info("[ENGINE] Candle history loaded for %d/%d series", loaded, total)

    async def _ws_loop(self, url: str) -> None:
        fails, alerted = 0, False
        while not self.stop.is_set():
            got_data = False
            await self._prepare_state()
            try:
                log.info("[ENGINE] Connecting WebSocket (%d chars URL)", len(url))
                async with websockets.connect(
                    url, ping_interval=20, ping_timeout=20, max_size=None, close_timeout=5
                ) as ws:
                    self._on_connect()
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg["ws_recv_timeout_sec"])
                        if not got_data:
                            got_data, fails, alerted, self.connected = True, 0, False, True
                            log.info("[ENGINE] WebSocket connected, receiving data")
                        try:
                            self._handle_message(raw)
                        except Exception as exc:
                            log.warning("[ENGINE] Malformed message skipped: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                log.warning("[ENGINE] WebSocket closed/error: %s: %s", type(exc).__name__, exc)
                self.connected = False
                if self.stop.is_set():
                    break
                if got_data:
                    delay = 1.0
                    log.info("[ENGINE] Reconnecting after session end in %.0fs", delay)
                else:
                    fails += 1
                    delay = min(2.0 ** (fails - 1), self.cfg["reconnect_backoff_max_sec"])
                    log.warning("[ENGINE] Reconnect failure #%d, retry in %.0fs", fails, delay)
                    if fails >= self.cfg["reconnect_fail_alert_after"] and not alerted:
                        alerted = True
                        self._spawn(self.alerter.send(
                            "WebSocket reconnect failing",
                            f"{fails} consecutive reconnect failures.",
                            "high",
                            "warning",
                        ))
                if await self._sleep_or_stop(delay):
                    break

    async def _eval_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["eval_interval_sec"]):
            if not self.connected:
                continue
            now = time.time()
            self.health.note_eval()
            self.health.check(now, self.connected)
            for sym, st in list(self.states.items()):
                try:
                    self._evaluate(sym, st, now)
                except Exception as exc:
                    self.health.note_eval(error=True)
                    log.exception("[%s] Evaluation error: %s", sym, exc)

    def _global_block(self, st: OrderFlowState, now: float) -> str | None:
        c = self.cfg
        if st.is_stale(now, c["stale_seconds"]) or st.last_price <= 0:
            return "stale"
        if not st.has_depth:
            return "no_book"
        if now - self.stream_start < c["warmup_seconds"]:
            return "warmup"
        if st.spread_bps >= c["spread_max_bps"]:
            return "spread"
        return None

    def _evaluate(self, sym: str, st: OrderFlowState, now: float) -> None:
        st.evict(now)
        gb = self._global_block(st, now)
        if gb:
            self._count(f"all:{gb}")
            return
        self._evaluate_rb(sym, st, now)

    def _evaluate_rb(self, sym: str, st: OrderFlowState, now: float) -> None:
        cfg = self.cfg.get("reversal_breakout", {})
        if not cfg.get("enabled", True):
            return
        self._cleanup_rb(st, now, cfg)
        for tf_name, tf in self.cfg["timeframes"].items():
            if cfg.get("reversal_enabled", True):
                self._check_reversal(sym, tf_name, tf, st, now, cfg)
            if cfg.get("breakout_enabled", True):
                self._check_breakout(sym, tf_name, tf, st, now, cfg)

    def _cleanup_rb(self, st: OrderFlowState, now: float, cfg: dict[str, Any]) -> None:
        max_age = float(cfg.get("setup_max_age_sec", 21_600))
        conf_cool = float(cfg.get("confirmed_cooldown_sec", 1_800))
        for k, s in list(st.rb.items()):
            created = float(s.get("created", 0.0))
            if now - created > max_age * 2:
                st.rb.pop(k, None)
                continue
            if s.get("confirmed_sent") and now - float(s.get("confirmed_ts", created)) > conf_cool * 2:
                st.rb.pop(k, None)

    def _series_for(self, st: OrderFlowState, tf_name: str) -> CandleSeries | None:
        group = st.candles.get(tf_name)
        if not group:
            return None
        return next(iter(group.values()))

    def _rb_state(self, st: OrderFlowState, key: str, defaults: dict[str, Any]) -> dict[str, Any]:
        s = st.rb.get(key)
        if s is None:
            s = dict(defaults)
            s["id"] = key
            s["created"] = time.time()
            s["early_sent"] = False
            s["confirmed_sent"] = False
            s["confirmed_ts"] = 0.0
            st.rb[key] = s
        s["last_seen"] = time.time()
        return s

    def _live_delta_frac(self, st: OrderFlowState, tf_name: str) -> float:
        tr = st.trig.get(tf_name)
        cur = tr.cur if tr else None
        if not cur:
            return 0.0
        total = cur["buy"] + cur["sell"]
        return (cur["buy"] - cur["sell"]) / total * 100.0 if total > 0 else 0.0

    @staticmethod
    def _closed_delta_frac(closed: dict[str, Any] | None) -> float | None:
        if closed is None:
            return None
        total = closed["buy"] + closed["sell"]
        return (closed["buy"] - closed["sell"]) / total * 100.0 if total > 0 else 0.0

    def _flow_metrics(
        self,
        st: OrderFlowState,
        tf_name: str,
        side: str,
        closed: dict[str, Any] | None,
        cfg: dict[str, Any],
    ) -> tuple[bool, bool, str]:
        sign = 1.0 if side == "BUY" else -1.0
        cvd = st.windows[tf_name].cvd_pct
        scvd = st.short.cvd_pct
        obi = st.obi_for(tf_name, self.cfg.get("obi_window_average", True))
        ld = self._live_delta_frac(st, tf_name)
        cd = self._closed_delta_frac(closed)

        cvd_th = float(cfg.get("cvd_confirm_pct", 1.0))
        obi_th = float(cfg.get("obi_confirm", 0.01))
        delta_th = float(cfg.get("delta_confirm_frac_pct", 1.0))

        early_ok = (
            sign * ld >= delta_th
            or sign * cvd >= cvd_th
            or sign * scvd >= cvd_th
            or (obi is not None and sign * obi >= obi_th)
        )

        confirmed_ok = False
        if cd is not None:
            confirmed_ok = (
                sign * cd >= delta_th
                and (
                    sign * cvd >= cvd_th
                    or sign * scvd >= cvd_th
                    or (obi is not None and sign * obi >= obi_th)
                    or sign * ld >= delta_th
                )
            )

        obi_s = f"{obi:+.3f}" if obi is not None else "n/a"
        cd_s = f"{cd:+.2f}%" if cd is not None else "n/a"
        text = f"CVD {cvd:+.2f}% | 15m CVD {scvd:+.2f}% | OBI {obi_s} | liveDelta {ld:+.2f}% | closedDelta {cd_s}"
        return early_ok, confirmed_ok, text

    def _last_swing(
        self,
        series: CandleSeries,
        pivot: str,
        now: float,
        tf: dict[str, Any],
        cfg: dict[str, Any],
    ) -> dict[str, Any] | None:
        left = int(cfg.get("fractal_left", 2))
        right = int(cfg.get("fractal_right", 1))
        idxs = series.pivots(pivot, left, right)
        cs = list(series.candles)
        max_age = float(tf.get("max_swing_age_sec", 86_400))
        for i in reversed(idxs):
            c = cs[i]
            ts = c.low_ts if pivot == "low" else c.high_ts
            if now - ts <= max_age:
                return {
                    "price": c.low if pivot == "low" else c.high,
                    "ts": ts,
                    "idx": i,
                    "candle_id": c.id,
                    "candle": c,
                }
        return None

    def _find_sweep(
        self,
        series: CandleSeries,
        side: str,
        level: float,
        cfg: dict[str, Any],
    ) -> dict[str, Any] | None:
        look = int(cfg.get("swing_lookback_candles", 40))
        cs = list(series.candles)[-look:]
        min_pct = float(cfg.get("sweep_min_pct", 0.05)) / 100.0
        max_pct = float(cfg.get("sweep_max_pct", 2.0)) / 100.0
        for c in reversed(cs):
            if side == "BUY":
                px = c.low
                if px > 0 and px < level * (1.0 - min_pct) and px > level * (1.0 - max_pct):
                    return {"candle": c, "price": px, "id": c.id}
            else:
                px = c.high
                if px > 0 and px > level * (1.0 + min_pct) and px < level * (1.0 + max_pct):
                    return {"candle": c, "price": px, "id": c.id}
        return None

    def _closed_recent(self, closed: dict[str, Any] | None, tf: dict[str, Any], now: float, cfg: dict[str, Any]) -> bool:
        if closed is None:
            return False
        trig_sec = int(tf["confirm_candle_seconds"])
        max_delay = float(cfg.get("confirm_max_delay_sec", 300))
        return now - (closed["id"] + 1) * trig_sec <= max_delay

    def _check_reversal(
        self,
        sym: str,
        tf_name: str,
        tf: dict[str, Any],
        st: OrderFlowState,
        now: float,
        cfg: dict[str, Any],
    ) -> None:
        series = self._series_for(st, tf_name)
        if series is None:
            return
        move = st.recent_move_pct(tf_name, self.cfg.get("min_window_fill", 0.75))
        if move >= 999.0:
            self._count(f"{tf_name}/rb:filling")
            return

        for side in ("BUY", "SELL"):
            pivot = "low" if side == "BUY" else "high"
            swing = self._last_swing(series, pivot, now, tf, cfg)
            if not swing:
                continue
            sweep = self._find_sweep(series, side, swing["price"], cfg)
            if not sweep:
                continue

            level = float(swing["price"])
            price = st.last_price
            if side == "BUY":
                reclaim = price > level * (1.0 + float(cfg.get("reclaim_buffer_pct", 0.03)) / 100.0)
                recovery = (price - sweep["price"]) / sweep["price"] * 100.0 if sweep["price"] > 0 else 0.0
                invalid = price < level * (1.0 - float(cfg.get("invalidation_buffer_pct", 0.20)) / 100.0)
            else:
                reclaim = price < level * (1.0 - float(cfg.get("reclaim_buffer_pct", 0.03)) / 100.0)
                recovery = (sweep["price"] - price) / sweep["price"] * 100.0 if sweep["price"] > 0 else 0.0
                invalid = price > level * (1.0 + float(cfg.get("invalidation_buffer_pct", 0.20)) / 100.0)

            key = f"rev|{tf_name}|{side}|{level:.8g}|{int(swing['ts'])}"
            if invalid:
                st.rb.pop(key, None)
                continue

            absorb = CandleSeries.absorption_stats(
                sweep["candle"],
                side,
                price,
                int(cfg.get("absorption_zone_rows", 3)),
                float(cfg.get("absorption_min_frac", 0.06)),
            )
            absorb_ok = (
                absorb["available"] and absorb["frac"] >= float(cfg.get("absorption_min_frac", 0.06))
            ) or recovery >= float(cfg.get("recovery_min_pct", 0.08))

            if not reclaim or not absorb_ok:
                continue

            early_ok, _, flow_text = self._flow_metrics(st, tf_name, side, None, cfg)
            if not early_ok:
                continue

            state = self._rb_state(st, key, {
                "type": "REVERSAL",
                "side": side,
                "tf": tf_name,
                "swing_price": level,
                "swing_ts": swing["ts"],
                "sweep_price": sweep["price"],
                "sweep_id": sweep["id"],
            })

            absorb_text = (
                f"contra_frac={absorb['frac'] * 100:.1f}% "
                f"recovery={recovery:.2f}% available={absorb['available']}"
            )
            delta_text = f"live {self._live_delta_frac(st, tf_name):+.2f}%"

            if cfg.get("early_enabled", True) and not state["early_sent"]:
                reason = (
                    "Liquidity sweep below swing low + reclaim + selling absorption/order-flow buy pressure"
                    if side == "BUY"
                    else "Liquidity sweep above swing high + reclaim + buying absorption/order-flow sell pressure"
                )
                self._send_rb_alert(
                    sym, tf_name, st, now, state, "REVERSAL", "EARLY", side,
                    reason, {"delta": delta_text, "flow": flow_text, "absorption": absorb_text},
                )
                state["early_sent"] = True

            if cfg.get("confirmed_enabled", True) and not state["confirmed_sent"]:
                closed = st.trig[tf_name].last_closed
                if self._closed_recent(closed, tf, now, cfg):
                    _, confirmed_ok, flow_text2 = self._flow_metrics(st, tf_name, side, closed, cfg)
                    cd = self._closed_delta_frac(closed)
                    sign = 1.0 if side == "BUY" else -1.0
                    delta_th = float(cfg.get("delta_confirm_frac_pct", 1.0))
                    if side == "BUY":
                        price_condition = closed is not None and closed["close"] > level * (1.0 + float(cfg.get("reclaim_buffer_pct", 0.03)) / 100.0)
                    else:
                        price_condition = closed is not None and closed["close"] < level * (1.0 - float(cfg.get("reclaim_buffer_pct", 0.03)) / 100.0)
                    if price_condition and confirmed_ok and cd is not None and sign * cd >= delta_th:
                        reason = (
                            "Closed trigger candle reclaimed swing low with positive delta/order-flow confirmation"
                            if side == "BUY"
                            else "Closed trigger candle reclaimed swing high with negative delta/order-flow confirmation"
                        )
                        delta_text2 = f"live {self._live_delta_frac(st, tf_name):+.2f}%, closed {cd:+.2f}%"
                        self._send_rb_alert(
                            sym, tf_name, st, now, state, "REVERSAL", "CONFIRMED", side,
                            reason, {"delta": delta_text2, "flow": flow_text2, "absorption": absorb_text},
                        )
                        state["confirmed_sent"] = True
                        state["confirmed_ts"] = now

    def _range_box(
        self,
        series: CandleSeries,
        now: float,
        tf: dict[str, Any],
        cfg: dict[str, Any],
        price: float,
    ) -> dict[str, Any] | None:
        left = int(cfg.get("fractal_left", 2))
        right = int(cfg.get("fractal_right", 1))
        cs = list(series.candles)
        max_age = float(cfg.get("range_max_age_sec", 172_800))

        highs = []
        for i in series.pivots("high", left, right):
            c = cs[i]
            if now - c.high_ts <= max_age:
                highs.append({"price": c.high, "ts": c.high_ts})
        lows = []
        for i in series.pivots("low", left, right):
            c = cs[i]
            if now - c.low_ts <= max_age:
                lows.append({"price": c.low, "ts": c.low_ts})

        if len(highs) < 2 or len(lows) < 2:
            return None

        h1, h2 = highs[-2], highs[-1]
        l1, l2 = lows[-2], lows[-1]
        top = max(h1["price"], h2["price"])
        bottom = min(l1["price"], l2["price"])
        height = top - bottom
        if bottom <= 0 or height <= 0:
            return None

        height_pct = height / bottom * 100.0
        min_h = float(cfg.get("range_min_height_pct", 0.35))
        max_h = float(cfg.get("range_max_height_pct", 6.0))
        if not (min_h <= height_pct <= max_h):
            return None

        tol = float(cfg.get("range_tolerance", 0.30)) * height
        if abs(h1["price"] - h2["price"]) > tol or abs(l1["price"] - l2["price"]) > tol:
            return None

        buf = float(cfg.get("breakout_confirmed_buffer_pct", 0.05)) / 100.0
        if price > top * (1.0 + 3.0 * buf) or price < bottom * (1.0 - 3.0 * buf):
            return None

        newest_ts = max(h1["ts"], h2["ts"], l1["ts"], l2["ts"])
        return {
            "top": top,
            "bottom": bottom,
            "height": height,
            "height_pct": height_pct,
            "newest_ts": newest_ts,
        }

    def _check_breakout(
        self,
        sym: str,
        tf_name: str,
        tf: dict[str, Any],
        st: OrderFlowState,
        now: float,
        cfg: dict[str, Any],
    ) -> None:
        series = self._series_for(st, tf_name)
        if series is None:
            return
        price = st.last_price
        rng = self._range_box(series, now, tf, cfg, price)
        if not rng:
            return

        prox = float(cfg.get("boundary_proximity_frac", 0.30))
        height = float(rng["height"])
        top = float(rng["top"])
        bottom = float(rng["bottom"])

        for side in ("BUY", "SELL"):
            if side == "BUY":
                near = price >= top - height * prox or price > top
                invalid = price < top - height * prox * 1.5
            else:
                near = price <= bottom + height * prox or price < bottom
                invalid = price > bottom + height * prox * 1.5

            if invalid:
                key = f"bo|{tf_name}|{side}|{top:.8g}|{bottom:.8g}|{int(rng['newest_ts'])}"
                st.rb.pop(key, None)
                continue
            if not near:
                continue

            early_ok, _, flow_text = self._flow_metrics(st, tf_name, side, None, cfg)
            if not early_ok:
                continue

            key = f"bo|{tf_name}|{side}|{top:.8g}|{bottom:.8g}|{int(rng['newest_ts'])}"
            state = self._rb_state(st, key, {
                "type": "BREAKOUT",
                "side": side,
                "tf": tf_name,
                "range_top": top,
                "range_bottom": bottom,
                "range_height_pct": rng["height_pct"],
            })

            delta_text = f"live {self._live_delta_frac(st, tf_name):+.2f}%"

            if cfg.get("early_enabled", True) and not state["early_sent"]:
                reason = (
                    "Price at/above range high with buying pressure"
                    if side == "BUY"
                    else "Price at/below range low with selling pressure"
                )
                self._send_rb_alert(
                    sym, tf_name, st, now, state, "BREAKOUT", "EARLY", side,
                    reason, {"delta": delta_text, "flow": flow_text, "absorption": "n/a"},
                )
                state["early_sent"] = True

            if cfg.get("confirmed_enabled", True) and not state["confirmed_sent"]:
                closed = st.trig[tf_name].last_closed
                if self._closed_recent(closed, tf, now, cfg):
                    _, confirmed_ok, flow_text2 = self._flow_metrics(st, tf_name, side, closed, cfg)
                    cd = self._closed_delta_frac(closed)
                    sign = 1.0 if side == "BUY" else -1.0
                    delta_th = float(cfg.get("delta_confirm_frac_pct", 1.0))
                    buf = float(cfg.get("breakout_confirmed_buffer_pct", 0.05)) / 100.0
                    if side == "BUY":
                        price_condition = closed is not None and closed["close"] > top * (1.0 + buf)
                    else:
                        price_condition = closed is not None and closed["close"] < bottom * (1.0 - buf)
                    if price_condition and confirmed_ok and cd is not None and sign * cd >= delta_th:
                        reason = (
                            "Candle closed above range high with delta/order-flow confirmation"
                            if side == "BUY"
                            else "Candle closed below range low with delta/order-flow confirmation"
                        )
                        delta_text2 = f"live {self._live_delta_frac(st, tf_name):+.2f}%, closed {cd:+.2f}%"
                        self._send_rb_alert(
                            sym, tf_name, st, now, state, "BREAKOUT", "CONFIRMED", side,
                            reason, {"delta": delta_text2, "flow": flow_text2, "absorption": "n/a"},
                        )
                        state["confirmed_sent"] = True
                        state["confirmed_ts"] = now

    def _rb_extra_lines(
        self,
        sym: str,
        tf_name: str,
        side: str,
        signal_type: str,
        stage: str,
        now: float,
    ) -> list[str]:
        return []

    def _send_rb_alert(
        self,
        sym: str,
        tf_name: str,
        st: OrderFlowState,
        now: float,
        state: dict[str, Any],
        signal_type: str,
        stage: str,
        side: str,
        reason: str,
        metrics: dict[str, str],
    ) -> bool:
        cfg = self.cfg.get("reversal_breakout", {})
        cool_sec = float(cfg.get("early_cooldown_sec" if stage == "EARLY" else "confirmed_cooldown_sec", 900))
        key = f"{sym}|rb|{tf_name}|{signal_type}|{side}|{stage}|{state['id']}"
        if self.book.on_cooldown(key, now, cool_sec / 60.0):
            self._count(f"{tf_name}/rb:{stage.lower()}_cooldown")
            return False

        lines = [
            f"Symbol: {sym}",
            f"Timeframe: {tf_name}",
            f"Signal Type: {signal_type}",
            f"Direction: {side}",
            f"Alert Stage: {stage}",
            f"Current Price: {st.last_price:.8g}",
        ]

        if state.get("swing_price") is not None:
            swing_name = "Swing Low" if side == "BUY" else "Swing High"
            lines.append(f"{swing_name}: {float(state['swing_price']):.8g}")
        if state.get("sweep_price") is not None:
            lines.append(f"Sweep Price: {float(state['sweep_price']):.8g}")
        if state.get("range_top") is not None:
            lines.append(f"Range High: {float(state['range_top']):.8g}")
        if state.get("range_bottom") is not None:
            lines.append(f"Range Low: {float(state['range_bottom']):.8g}")
        if state.get("range_height_pct") is not None:
            lines.append(f"Range Height %: {float(state['range_height_pct']):.2f}%")

        lines.append(f"Delta: {metrics.get('delta', 'n/a')}")
        lines.append(f"Order Flow: {metrics.get('flow', 'n/a')}")
        lines.append(f"Absorption: {metrics.get('absorption', 'n/a')}")
        lines.append(f"Alert Timestamp: {datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"Signal Reason: {reason}")
        lines.extend(self._rb_extra_lines(sym, tf_name, side, signal_type, stage, now))

        body = "\n".join(lines)
        title = f"{sym} {stage} {signal_type} {side} [{tf_name}]"
        tags = "green_circle" if side == "BUY" else "red_circle"

        if self.book.cap_reached():
            log.info(
                "[%s] %s (daily cap %d reached, log only) | %s",
                sym, title, self.cfg["max_alerts_per_day"], body.replace("\n", " | "),
            )
            self.book.last_alert[key] = now
            return True

        self.book.register(key, sym, tf_name, side, now, label=f"{tf_name} {signal_type} {side} {stage}")
        self.health.note_signal()
        log.info("[%s] %s | %s", sym, title, body.replace("\n", " | "))
        self._spawn(self.alerter.send(title, body, "high", tags))
        return True

    async def _diag_loop(self) -> None:
        minutes = self.cfg["diag_interval_min"]
        while not await self._sleep_or_stop(minutes * 60):
            if not self.connected:
                continue
            now = time.time()
            log.info("[DIAG] health: %s", self.health.summary(now))
            counts, self.block_counts = self.block_counts, {}
            groups: dict[str, dict[str, int]] = {}
            for k, n in counts.items():
                grp, reason = k.split(":", 1)
                groups.setdefault(grp, {})[reason] = n
            for grp, d in sorted(groups.items()):
                tot = sum(d.values())
                parts = " ".join(f"{r}={n / tot * 100:.0f}%" for r, n in sorted(d.items(), key=lambda kv: -kv[1]))
                log.info("[DIAG] %s gates over last %dmin: %s", grp, minutes, parts)
            for sym, st in self.states.items():
                log.info("[DIAG] %s active_rb_setups=%d", sym, len(st.rb))

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
            if await self._sleep_or_stop((nxt - now).total_seconds()):
                return
            await self._rollover()


def main() -> None:
    load_dotenv()
    setup_logging()
    code = 0
    try:
        code = asyncio.run(Engine().run())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")
    sys.exit(code)


if __name__ == "__main__":
    main()
