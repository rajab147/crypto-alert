#!/usr/bin/env python3
"""
orderflow_paper.py
Near-accurate footprint + completed reversal/breakout alert engine.
Alerts only. No trading. No order execution.
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import signal
import sqlite3
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
    "eval_interval_sec": 2,
    "warmup_seconds": 10,
    "bucket_seconds": 5,
    "min_window_fill": 0.75,
    "obi_window_average": True,
    "short_window_seconds": 900,
    "diag_interval_min": 30,
    "depth_levels": 10,
    "stale_seconds": 15,
    "ws_recv_timeout_sec": 30,
    "reconnect_wipe_after_sec": 120,
    "reconnect_backoff_max_sec": 60,
    "reconnect_fail_alert_after": 3,
    "state_save_interval_sec": 60,
    "http_timeout_sec": 60,
    "http_retries": 3,
    "ntfy_timeout_sec": 10,

    # Unlimited practical cap. Duplicate control is per setup, not global fake cap.
    "max_alerts_per_day": 1_000_000,

    "footprint": {
        "db_path": "footprint.db",
        "base_candle_seconds": 60,
        "signal_candle_seconds": 900,
        "backfill_minutes": 180,
        "keep_bars": 3000,
        "gap_fill": True,
        "validate_klines": True,

        "zone_rows": 5,
        "absorption_min_frac": 0.18,
        "reclaim_ticks": 5,
        "sweep_min_ticks": 10,
        "breakout_buffer_ticks": 5,

        "delta_min_quote": 10_000,
        "imbalance_ratio": 2.5,
        "imbalance_min_frac": 0.005,
        "stacked_min": 2,
        "counter_pressure_max": 0.65,

        "cvd_confirm_pct": 2.0,
        "obi_confirm": 0.02,

        "swing_left": 2,
        "swing_right": 1,
        "swing_lookback_bars": 120,
        "max_swing_age_sec": 21_600,

        "range_lookback_bars": 80,
        "range_min_height_pct": 0.8,
        "range_max_height_pct": 6.0,
        "range_tolerance": 0.18,
        "boundary_proximity_frac": 0.25,

        "setup_cooldown_sec": 86_400,
    },

    "sessions": {
        "enabled": True,
        "alert_on_change": True,
        "definitions": {
            "Asia": [0, 8],
            "London": [7, 16],
            "New York": [12, 21],
        },
    },

    "timeframes": {
        "1h": {
            "seconds": 3600,
            "cooldown_min": 45,
            "div_candle_seconds": [3600],
            "div_base_seconds": 60,
            "confirm_candle_seconds": 900,
            "max_recent_move_pct": 1.5,
        },
        "4h": {
            "seconds": 14400,
            "cooldown_min": 180,
            "div_candle_seconds": [14400],
            "div_base_seconds": 300,
            "confirm_candle_seconds": 1800,
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


def candle_label(sec: float) -> str:
    return KLINE_INTERVALS.get(int(sec), f"{int(sec)}s")


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
    num(cfg, "short_window_seconds", 60)
    num(cfg, "max_alerts_per_day", 1)
    num(cfg, "stale_seconds", 1)

    if cfg.get("depth_levels") not in (5, 10, 20):
        errs.append(f"depth_levels invalid: {cfg.get('depth_levels')!r}")

    fp = cfg.get("footprint")
    if not isinstance(fp, dict):
        errs.append("footprint must be dict")
    else:
        num(fp, "base_candle_seconds", 10)
        num(fp, "signal_candle_seconds", 60)
        num(fp, "backfill_minutes", 0)
        num(fp, "keep_bars", 50)
        num(fp, "zone_rows", 1)
        num(fp, "absorption_min_frac", 0.01, 1.0)
        num(fp, "reclaim_ticks", 0)
        num(fp, "sweep_min_ticks", 0)
        num(fp, "breakout_buffer_ticks", 0)
        num(fp, "delta_min_quote", 0)
        num(fp, "imbalance_ratio", 1.01)
        num(fp, "imbalance_min_frac", 0.0)
        num(fp, "stacked_min", 0)
        num(fp, "counter_pressure_max", 0.0, 1.0)
        num(fp, "cvd_confirm_pct", 0.0)
        num(fp, "obi_confirm", 0.0)
        num(fp, "swing_left", 1)
        num(fp, "swing_right", 1)
        num(fp, "swing_lookback_bars", 5)
        num(fp, "max_swing_age_sec", 60)
        num(fp, "range_lookback_bars", 5)
        num(fp, "range_min_height_pct", 0.0)
        num(fp, "range_max_height_pct", 0.0)
        num(fp, "range_tolerance", 0.0, 1.0)
        num(fp, "boundary_proximity_frac", 0.0, 1.0)
        num(fp, "setup_cooldown_sec", 0)

    sess = cfg.get("sessions")
    if not isinstance(sess, dict):
        errs.append("sessions must be dict")
    else:
        defs = sess.get("definitions")
        if not isinstance(defs, dict):
            errs.append("sessions.definitions must be dict")
        else:
            for name, rng in defs.items():
                if not isinstance(rng, list) or len(rng) != 2:
                    errs.append(f"session '{name}' must be [start_hour, end_hour]")
                    continue
                if not (isinstance(rng[0], int) and isinstance(rng[1], int)):
                    errs.append(f"session '{name}' hours must be int")

    tfs = cfg.get("timeframes")
    if not isinstance(tfs, dict) or not tfs:
        errs.append("timeframes must be non-empty dict")
    else:
        for name, tf in tfs.items():
            if not isinstance(tf, dict):
                errs.append(f"timeframes['{name}'] must be dict")
                continue
            num(tf, "seconds", 1)
            num(tf, "cooldown_min", 0)
            num(tf, "confirm_candle_seconds", 1)
            num(tf, "max_recent_move_pct", 0)

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
        tickers, info = await asyncio.gather(
            self._get(session, TICKER_URL),
            self._get(session, INFO_URL),
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


class FootprintBar:
    __slots__ = (
        "id", "open_ts", "open", "high", "low", "close",
        "buy", "sell", "levels", "finalized", "last_ts",
    )

    def __init__(
        self,
        cid: int,
        open_ts: float,
        price: float,
        levels: dict[int, list[float]] | None = None,
    ) -> None:
        self.id = cid
        self.open_ts = float(open_ts)
        self.open = self.high = self.low = self.close = float(price)
        self.buy = 0.0
        self.sell = 0.0
        self.levels: dict[int, list[float]] = levels or {}
        self.finalized = False
        self.last_ts = float(open_ts)

    def add(self, level: int, price: float, quote: float, is_buy: bool, ts: float) -> None:
        self.high = max(self.high, price)
        self.low = min(self.low, price)
        self.close = price
        self.last_ts = max(self.last_ts, ts)
        if is_buy:
            self.buy += quote
        else:
            self.sell += quote
        cell = self.levels.get(level)
        if cell is None:
            cell = self.levels[level] = [0.0, 0.0]
        cell[1 if is_buy else 0] += quote

    @property
    def total(self) -> float:
        return self.buy + self.sell

    @property
    def delta(self) -> float:
        return self.buy - self.sell


class TickFootprint:
    def __init__(
        self,
        symbol: str,
        tick: float,
        base_sec: int,
        db_path: str,
        keep_bars: int,
    ) -> None:
        self.symbol = symbol
        self.tick = float(tick if tick > 0 else 1e-8)
        self.base_sec = int(base_sec)
        self.db_path = db_path
        self.keep_bars = int(keep_bars)
        self.bars: deque[FootprintBar] = deque(maxlen=self.keep_bars)
        self.current: FootprintBar | None = None
        self._init_db()
        self.load_recent()

    def _init_db(self) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                """
                CREATE TABLE IF NOT EXISTS fp_bars (
                    symbol TEXT NOT NULL,
                    candle_id INTEGER NOT NULL,
                    open_ts REAL NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    buy REAL NOT NULL,
                    sell REAL NOT NULL,
                    levels TEXT NOT NULL,
                    PRIMARY KEY (symbol, candle_id)
                )
                """
            )
            con.commit()
        finally:
            con.close()

    def load_recent(self) -> None:
        now_cid = int(time.time() // self.base_sec)
        start_cid = now_cid - self.keep_bars
        con = sqlite3.connect(self.db_path)
        try:
            cur = con.execute(
                "SELECT candle_id, open_ts, open, high, low, close, buy, sell, levels "
                "FROM fp_bars WHERE symbol=? AND candle_id>=? ORDER BY candle_id ASC",
                (self.symbol, start_cid),
            )
            for row in cur.fetchall():
                cid, open_ts, o, h, l, c, buy, sell, levels_json = row
                levels = {int(k): [float(v[0]), float(v[1])] for k, v in json.loads(levels_json).items()}
                bar = FootprintBar(int(cid), float(open_ts), float(o), levels)
                bar.high = float(h)
                bar.low = float(l)
                bar.close = float(c)
                bar.buy = float(buy)
                bar.sell = float(sell)
                bar.finalized = True
                bar.last_ts = float(open_ts) + self.base_sec
                self.bars.append(bar)
        finally:
            con.close()

    def _save(self, bar: FootprintBar) -> None:
        con = sqlite3.connect(self.db_path)
        try:
            con.execute(
                "INSERT OR REPLACE INTO fp_bars VALUES (?,?,?,?,?,?,?,?,?,?)",
                (
                    self.symbol,
                    bar.id,
                    bar.open_ts,
                    bar.open,
                    bar.high,
                    bar.low,
                    bar.close,
                    bar.buy,
                    bar.sell,
                    json.dumps({str(k): v for k, v in bar.levels.items()}),
                ),
            )
            con.commit()
        finally:
            con.close()

    def level(self, price: float) -> int:
        return int(math.floor(price / self.tick + 1e-9))

    def add_trade(self, ts: float, price: float, quote: float, is_buy: bool) -> None:
        cid = int(ts // self.base_sec)
        if self.current is None or self.current.id != cid:
            if self.current is not None:
                self._finalize(self.current)
            self.current = FootprintBar(cid, cid * self.base_sec, price)
        self.current.add(self.level(price), price, quote, is_buy, ts)

    def _finalize(self, bar: FootprintBar) -> None:
        bar.finalized = True
        self._save(bar)
        if not self.bars or self.bars[-1].id != bar.id:
            self.bars.append(bar)

    def poll(self, now: float) -> FootprintBar | None:
        cid = int(now // self.base_sec)
        if self.current is not None and self.current.id < cid:
            bar = self.current
            self._finalize(bar)
            self.current = None
            return bar
        return None

    def recent_bars(self, include_current: bool = True) -> list[FootprintBar]:
        out = list(self.bars)
        if include_current and self.current is not None:
            out.append(self.current)
        return out

    def aggregate_bars(self, bars: list[FootprintBar], seconds: int) -> list[FootprintBar]:
        seconds = max(60, int(seconds))
        out: dict[int, FootprintBar] = {}
        for b in bars:
            start = b.id * self.base_sec
            sid = int(start // seconds)
            ob = out.get(sid)
            if ob is None:
                ob = FootprintBar(sid, sid * seconds, b.open, {k: list(v) for k, v in b.levels.items()})
                ob.high = b.high
                ob.low = b.low
                ob.close = b.close
                ob.buy = b.buy
                ob.sell = b.sell
                ob.finalized = b.finalized
                ob.last_ts = b.last_ts
                out[sid] = ob
            else:
                ob.high = max(ob.high, b.high)
                ob.low = min(ob.low, b.low)
                ob.close = b.close
                ob.buy += b.buy
                ob.sell += b.sell
                ob.finalized = ob.finalized and b.finalized
                ob.last_ts = max(ob.last_ts, b.last_ts)
                for k, v in b.levels.items():
                    cell = ob.levels.get(k)
                    if cell is None:
                        ob.levels[k] = list(v)
                    else:
                        cell[0] += v[0]
                        cell[1] += v[1]
        return [out[k] for k in sorted(out)]


def zone_fraction(bar: FootprintBar, rows: int, vol_side: str, zone: str) -> float:
    total = bar.total
    if total <= 0 or not bar.levels:
        return 0.0
    levels = sorted(bar.levels)
    rows = max(1, min(int(rows), len(levels)))
    if zone == "low":
        sel = levels[:rows]
    else:
        sel = levels[-rows:]
    idx = 0 if vol_side == "sell" else 1
    vol = sum(bar.levels[lv][idx] for lv in sel)
    return vol / total


def stacked_imbalances(bar: FootprintBar, side: str, ratio: float, min_frac: float) -> int:
    total = bar.total
    if total <= 0 or not bar.levels:
        return 0
    min_vol = max(1.0, min_frac * total)
    best = run = 0
    for lv in sorted(bar.levels):
        sell, buy = bar.levels[lv]
        if side == "BUY":
            hit = buy >= min_vol and buy >= ratio * sell
        else:
            hit = sell >= min_vol and sell >= ratio * buy
        run = run + 1 if hit else 0
        best = max(best, run)
    return best


def counter_fraction(bar: FootprintBar, side: str) -> float:
    total = bar.total
    if total <= 0:
        return 1.0
    return (bar.sell / total) if side == "BUY" else (bar.buy / total)


def find_pivots(bars: list[FootprintBar], pivot: str, left: int, right: int) -> list[int]:
    out: list[int] = []
    if len(bars) < left + right + 1:
        return out
    for i in range(left, len(bars) - right):
        b = bars[i]
        if pivot == "low":
            ok = all(b.low <= bars[i - k].low for k in range(1, left + 1)) and all(
                b.low < bars[i + k].low for k in range(1, right + 1)
            )
        else:
            ok = all(b.high >= bars[i - k].high for k in range(1, left + 1)) and all(
                b.high > bars[i + k].high for k in range(1, right + 1)
            )
        if ok:
            out.append(i)
    return out


def range_box(bars: list[FootprintBar], cfg: dict[str, Any]) -> dict[str, Any] | None:
    left = int(cfg.get("swing_left", 2))
    right = int(cfg.get("swing_right", 1))
    look = int(cfg.get("range_lookback_bars", 80))
    use = bars[-look:] if len(bars) > look else bars
    highs = [use[i] for i in find_pivots(use, "high", left, right)]
    lows = [use[i] for i in find_pivots(use, "low", left, right)]
    if len(highs) < 2 or len(lows) < 2:
        return None
    h1, h2 = highs[-2], highs[-1]
    l1, l2 = lows[-2], lows[-1]
    top = max(h1.high, h2.high)
    bottom = min(l1.low, l2.low)
    height = top - bottom
    if bottom <= 0 or height <= 0:
        return None
    height_pct = height / bottom * 100.0
    min_h = float(cfg.get("range_min_height_pct", 0.8))
    max_h = float(cfg.get("range_max_height_pct", 6.0))
    if not (min_h <= height_pct <= max_h):
        return None
    tol = float(cfg.get("range_tolerance", 0.18)) * height
    if abs(h1.high - h2.high) > tol or abs(l1.low - l2.low) > tol:
        return None
    return {
        "top": top,
        "bottom": bottom,
        "height": height,
        "height_pct": height_pct,
        "top_ts": max(h1.open_ts, h2.open_ts),
        "bottom_ts": max(l1.open_ts, l2.open_ts),
    }


def current_sessions(now: float, cfg: dict[str, Any]) -> list[str]:
    defs = cfg.get("definitions", {})
    hour = datetime.fromtimestamp(now, timezone.utc).hour
    out = []
    for name, rng in defs.items():
        try:
            s, e = int(rng[0]), int(rng[1])
        except Exception:
            continue
        if s <= e:
            if s <= hour < e:
                out.append(name)
        else:
            if hour >= s or hour < e:
                out.append(name)
    return sorted(out)


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


class OrderFlowState:
    def __init__(
        self,
        symbol: str,
        timeframes: dict[str, dict[str, Any]],
        bucket_sec: float,
        levels: int,
        tick: float,
        fp_cfg: dict[str, Any],
    ) -> None:
        self.symbol = symbol
        self.levels = levels
        self.windows = {name: RollingWindow(tf["seconds"], bucket_sec) for name, tf in timeframes.items()}
        self.short = RollingWindow(float(CONFIG["short_window_seconds"]), bucket_sec)
        self.fp = TickFootprint(
            symbol,
            tick,
            int(fp_cfg["base_candle_seconds"]),
            str(fp_cfg["db_path"]),
            int(fp_cfg["keep_bars"]),
        )
        self.last_price = 0.0
        self.last_trade_ts = 0.0
        self.bid_vol = self.ask_vol = 0.0
        self.best_bid = self.best_ask = 0.0
        self.has_depth = False
        self.last_update = 0.0

    def reset_windows(self) -> None:
        for w in self.windows.values():
            w.reset()
        self.short.reset()

    def add_trade(self, price: float, quote: float, is_buy: bool, trade_ts: float, now: float) -> None:
        for w in self.windows.values():
            w.add_trade(price, quote, is_buy, now)
        self.short.add_trade(price, quote, is_buy, now)
        self.fp.add_trade(trade_ts, price, quote, is_buy)
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

    def evict(self, now: float) -> None:
        for w in self.windows.values():
            w.evict(now)
        self.short.evict(now)

    def is_stale(self, now: float, limit: float) -> bool:
        return self.last_update == 0.0 or now - self.last_update > limit

    @property
    def obi_now(self) -> float:
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0

    def obi_for(self, name: str, use_average: bool = True) -> float:
        if use_average and name in self.windows:
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
        self.last_agg_id: dict[str, int] = {}
        self._session_key: tuple[str, ...] | None = None

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
                ticks.get(s, 0.0),
                self.cfg["footprint"],
            )

        if self.book.day != utc_day():
            await self._rollover()

        await self._backfill_footprint_all()

        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@aggTrade/{s.lower()}@depth{lvl}@100ms" for s in symbols
        )
        log.info(
            "[ENGINE] Starting with %d symbols, footprint base=%ss signal=%ss (alerts only, no trading)",
            len(symbols),
            self.cfg["footprint"]["base_candle_seconds"],
            self.cfg["footprint"]["signal_candle_seconds"],
        )

        tasks = [
            asyncio.create_task(c)
            for c in (
                self._ws_loop(url),
                self._eval_loop(),
                self._save_loop(),
                self._daily_loop(),
                self._diag_loop(),
            )
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
                a = int(data["a"])
                price = float(data["p"])
                qty = float(data["q"])
                trade_ts = float(data["T"]) / 1000.0
                is_buyer_maker = bool(data["m"])
                is_buy = not is_buyer_maker
                quote = price * qty

                expected = self.last_agg_id.get(sym)
                if expected is not None and a > expected + 1 and self.cfg["footprint"].get("gap_fill", True):
                    self._spawn(self._fill_gap(sym, expected + 1, a - 1))
                if expected is None or a >= expected:
                    self.last_agg_id[sym] = a

                st.add_trade(price, quote, is_buy, trade_ts, now)
            elif "@depth" in stream:
                st.update_depth(data["bids"], data["asks"], now)
        except (KeyError, ValueError, TypeError, IndexError) as exc:
            log.warning("[%s] Bad %s message skipped: %s", sym, stream.split("@", 1)[1], exc)

    async def _fill_gap(self, sym: str, from_id: int, to_id: int) -> None:
        st = self.states.get(sym)
        if st is None:
            return
        selector = SymbolSelector(self.cfg)
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                cur_id = from_id
                while cur_id <= to_id:
                    url = (
                        f"{REST_BASE}/api/v3/aggTrades?symbol={sym}"
                        f"&fromId={cur_id}&limit=1000"
                    )
                    rows = await selector._get(session, url)
                    if not rows:
                        break
                    now = time.time()
                    for r in rows:
                        a = int(r[0])
                        if a < cur_id:
                            continue
                        if a > to_id:
                            break
                        price = float(r[1])
                        qty = float(r[2])
                        trade_ts = float(r[5]) / 1000.0
                        is_buy = not bool(r[6])
                        quote = price * qty
                        if now - trade_ts <= self.cfg["short_window_seconds"] * 3:
                            st.add_trade(price, quote, is_buy, trade_ts, now)
                        else:
                            st.fp.add_trade(trade_ts, price, quote, is_buy)
                    cur_id = int(rows[-1][0]) + 1
                    if len(rows) < 1000:
                        break
        except Exception as exc:
            log.warning("[%s] aggTrade gap fill failed %s-%s: %s", sym, from_id, to_id, exc)

    async def _backfill_footprint_all(self) -> None:
        minutes = int(self.cfg["footprint"].get("backfill_minutes", 0))
        if minutes <= 0:
            return
        selector = SymbolSelector(self.cfg)
        sem = asyncio.Semaphore(4)
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        end_ms = int(time.time() * 1000)
        start_ms = end_ms - minutes * 60_000
        loaded = 0
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def one(sym: str, st: OrderFlowState) -> None:
                nonlocal loaded
                async with sem:
                    try:
                        from_id: int | None = None
                        while True:
                            if from_id is None:
                                url = (
                                    f"{REST_BASE}/api/v3/aggTrades?symbol={sym}"
                                    f"&startTime={start_ms}&endTime={end_ms}&limit=1000"
                                )
                            else:
                                url = (
                                    f"{REST_BASE}/api/v3/aggTrades?symbol={sym}"
                                    f"&fromId={from_id}&limit=1000"
                                )
                            rows = await selector._get(session, url)
                            if not rows:
                                break
                            now = time.time()
                            for r in rows:
                                ts = float(r[5]) / 1000.0
                                if ts * 1000 > end_ms:
                                    break
                                price = float(r[1])
                                qty = float(r[2])
                                is_buy = not bool(r[6])
                                quote = price * qty
                                if now - ts <= self.cfg["short_window_seconds"] * 3:
                                    st.add_trade(price, quote, is_buy, ts, now)
                                else:
                                    st.fp.add_trade(ts, price, quote, is_buy)
                            from_id = int(rows[-1][0]) + 1
                            if len(rows) < 1000:
                                break
                        loaded += 1
                    except Exception as exc:
                        log.warning("[%s] footprint backfill failed: %s", sym, exc)

            await asyncio.gather(*(one(sym, st) for sym, st in self.states.items()))
        log.info("[ENGINE] Footprint backfill loaded for %d/%d symbols", loaded, len(self.states))

    def _on_connect(self) -> None:
        self.stream_start = time.time()

    async def _prepare_state(self) -> None:
        gap = time.time() - self.last_msg_ts if self.last_msg_ts else float("inf")
        if gap <= self.cfg["reconnect_wipe_after_sec"] or self._wiped_since_msg:
            return
        for st in self.states.values():
            st.reset_windows()
        self._wiped_since_msg = True
        log.info("[ENGINE] Rolling windows wiped (%s); footprint DB preserved",
                 "start" if not self.last_msg_ts else f"gap {gap:.0f}s")

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
            self._check_session_alert(now)
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
        st.fp.poll(now)
        self._check_footprint_signals(sym, st, now)

    def _signal_extra_lines(self, sym: str, signal_type: str, side: str, now: float) -> list[str]:
        return []

    def _session_extra_lines(self, now: float, sessions: list[str]) -> list[str]:
        return []

    def _check_footprint_signals(self, sym: str, st: OrderFlowState, now: float) -> None:
        cfg = self.cfg["footprint"]
        bars = st.fp.recent_bars(include_current=True)
        if len(bars) < 20:
            return
        sig_bars = st.fp.aggregate_bars(bars, int(cfg["signal_candle_seconds"]))
        if len(sig_bars) < 12:
            return
        live = sig_bars[-1]
        final = sig_bars[:-1]
        if not final:
            return

        tick = st.fp.tick
        cvd = st.short.cvd_pct
        obi = st.obi_for("1h", bool(self.cfg.get("obi_window_average", True)))
        sessions = current_sessions(now, self.cfg.get("sessions", {}))

        rng = range_box(final, cfg)

        # ---------- Completed Reversal ----------
        for side in ("BUY", "SELL"):
            pivot = "low" if side == "BUY" else "high"
            pivots = find_pivots(final, pivot, int(cfg["swing_left"]), int(cfg["swing_right"]))
            if not pivots:
                continue
            sw = final[pivots[-1]]
            level = sw.low if side == "BUY" else sw.high
            age = now - sw.open_ts
            if age > float(cfg["max_swing_age_sec"]):
                continue

            if side == "BUY":
                swept = live.low <= level - float(cfg["sweep_min_ticks"]) * tick
                reclaimed = live.close >= level + float(cfg["reclaim_ticks"]) * tick
                zone_frac = zone_fraction(live, int(cfg["zone_rows"]), "sell", "low")
                delta_ok = live.delta >= float(cfg["delta_min_quote"])
                flow_ok = cvd >= float(cfg["cvd_confirm_pct"]) and obi >= float(cfg["obi_confirm"])
                near_range = rng is None or live.low <= rng["bottom"] + rng["height"] * float(cfg["boundary_proximity_frac"])
            else:
                swept = live.high >= level + float(cfg["sweep_min_ticks"]) * tick
                reclaimed = live.close <= level - float(cfg["reclaim_ticks"]) * tick
                zone_frac = zone_fraction(live, int(cfg["zone_rows"]), "buy", "high")
                delta_ok = live.delta <= -float(cfg["delta_min_quote"])
                flow_ok = cvd <= -float(cfg["cvd_confirm_pct"]) and obi <= -float(cfg["obi_confirm"])
                near_range = rng is None or live.high >= rng["top"] - rng["height"] * float(cfg["boundary_proximity_frac"])

            stacked = stacked_imbalances(live, side, float(cfg["imbalance_ratio"]), float(cfg["imbalance_min_frac"]))
            counter = counter_fraction(live, side)

            if not (swept and reclaimed and zone_frac >= float(cfg["absorption_min_frac"])):
                continue
            if not (delta_ok and flow_ok and stacked >= int(cfg["stacked_min"])):
                continue
            if counter > float(cfg["counter_pressure_max"]):
                continue
            if not near_range:
                continue

            setup_id = f"rev|{side}|{level:.8f}|{sw.id}"
            reason = (
                "Footprint sweep + reclaim + absorption + stacked imbalance + delta/CVD/OBI confirmation"
                if side == "BUY"
                else "Footprint sweep + reclaim + absorption + stacked imbalance + delta/CVD/OBI confirmation"
            )
            metrics = {
                "level": level,
                "sweep_extreme": live.low if side == "BUY" else live.high,
                "zone_frac": zone_frac,
                "stacked": stacked,
                "counter": counter,
                "delta": live.delta,
                "cvd": cvd,
                "obi": obi,
                "sessions": sessions,
                "range": rng,
            }
            self._send_completed_signal(
                sym=sym,
                st=st,
                now=now,
                signal_type="REVERSAL",
                side=side,
                setup_id=setup_id,
                reason=reason,
                metrics=metrics,
            )

        # ---------- Completed Breakout ----------
        if rng is None:
            return
        top = float(rng["top"])
        bottom = float(rng["bottom"])
        height = float(rng["height"])

        for side in ("BUY", "SELL"):
            if side == "BUY":
                crossed = live.close >= top + float(cfg["breakout_buffer_ticks"]) * tick
                zone_frac = zone_fraction(live, int(cfg["zone_rows"]), "buy", "high")
                delta_ok = live.delta >= float(cfg["delta_min_quote"])
                flow_ok = cvd >= float(cfg["cvd_confirm_pct"]) and obi >= float(cfg["obi_confirm"])
            else:
                crossed = live.close <= bottom - float(cfg["breakout_buffer_ticks"]) * tick
                zone_frac = zone_fraction(live, int(cfg["zone_rows"]), "sell", "low")
                delta_ok = live.delta <= -float(cfg["delta_min_quote"])
                flow_ok = cvd <= -float(cfg["cvd_confirm_pct"]) and obi <= -float(cfg["obi_confirm"])

            stacked = stacked_imbalances(live, side, float(cfg["imbalance_ratio"]), float(cfg["imbalance_min_frac"]))
            counter = counter_fraction(live, side)

            if not crossed:
                continue
            if zone_frac < float(cfg["absorption_min_frac"]):
                continue
            if not (delta_ok and flow_ok and stacked >= int(cfg["stacked_min"])):
                continue
            if counter > float(cfg["counter_pressure_max"]):
                continue

            setup_id = f"bo|{side}|{top:.8f}|{bottom:.8f}|{int(rng['top_ts'])}|{int(rng['bottom_ts'])}"
            reason = (
                "Range breakout completed with footprint aggression + stacked imbalance + delta/CVD/OBI confirmation"
                if side == "BUY"
                else "Range breakdown completed with footprint aggression + stacked imbalance + delta/CVD/OBI confirmation"
            )
            metrics = {
                "range_top": top,
                "range_bottom": bottom,
                "range_height_pct": rng["height_pct"],
                "zone_frac": zone_frac,
                "stacked": stacked,
                "counter": counter,
                "delta": live.delta,
                "cvd": cvd,
                "obi": obi,
                "sessions": sessions,
            }
            self._send_completed_signal(
                sym=sym,
                st=st,
                now=now,
                signal_type="BREAKOUT",
                side=side,
                setup_id=setup_id,
                reason=reason,
                metrics=metrics,
            )

    def _send_completed_signal(
        self,
        sym: str,
        st: OrderFlowState,
        now: float,
        signal_type: str,
        side: str,
        setup_id: str,
        reason: str,
        metrics: dict[str, Any],
    ) -> None:
        cfg = self.cfg["footprint"]
        cool_min = float(cfg.get("setup_cooldown_sec", 86_400)) / 60.0
        key = f"{sym}|fp|{signal_type}|{side}|{setup_id}"
        if self.book.on_cooldown(key, now, cool_min):
            self._count(f"fp:{signal_type.lower()}_{side.lower()}_cooldown")
            return

        lines = [
            f"Symbol: {sym}",
            f"Signal Type: {signal_type}",
            f"Direction: {side}",
            f"Status: COMPLETED",
            f"Current Price: {st.last_price:.8g}",
        ]

        if "level" in metrics:
            lines.append(f"Swing Level: {float(metrics['level']):.8g}")
        if "sweep_extreme" in metrics:
            lines.append(f"Sweep Extreme: {float(metrics['sweep_extreme']):.8g}")
        if "range_top" in metrics:
            lines.append(f"Range High: {float(metrics['range_top']):.8g}")
        if "range_bottom" in metrics:
            lines.append(f"Range Low: {float(metrics['range_bottom']):.8g}")
        if "range_height_pct" in metrics:
            lines.append(f"Range Height %: {float(metrics['range_height_pct']):.2f}%")

        lines.append(f"Footprint Zone Fraction: {float(metrics.get('zone_frac', 0)) * 100:.2f}%")
        lines.append(f"Stacked Imbalances: {int(metrics.get('stacked', 0))}")
        lines.append(f"Counter Pressure: {float(metrics.get('counter', 0)) * 100:.2f}%")
        lines.append(f"Bar Delta USDT: {float(metrics.get('delta', 0)):+,.0f}")
        lines.append(f"15m CVD%: {float(metrics.get('cvd', 0)):+.2f}%")
        obi = metrics.get("obi")
        lines.append(f"OBI: {obi:+.4f}" if obi is not None else "OBI: n/a")
        lines.append(f"Sessions: {', '.join(metrics.get('sessions', [])) or 'none'}")
        lines.append(f"Alert Timestamp: {datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}")
        lines.append(f"Reason: {reason}")
        lines.extend(self._signal_extra_lines(sym, signal_type, side, now))

        body = "\n".join(lines)
        title = f"{sym} {signal_type} {side} COMPLETED"
        tags = "green_circle" if side == "BUY" else "red_circle"

        if self.book.cap_reached():
            log.info(
                "[%s] %s (daily cap %d reached, log only) | %s",
                sym, title, self.cfg["max_alerts_per_day"], body.replace("\n", " | "),
            )
            self.book.last_alert[key] = now
            return

        self.book.register(key, sym, "fp", side, now, label=f"{signal_type} {side} COMPLETED")
        self.health.note_signal()
        log.info("[%s] %s | %s", sym, title, body.replace("\n", " | "))
        self._spawn(self.alerter.send(title, body, "high", tags))

    def _check_session_alert(self, now: float) -> None:
        sess_cfg = self.cfg.get("sessions", {})
        if not sess_cfg.get("enabled", True):
            return
        sessions = current_sessions(now, sess_cfg)
        key = tuple(sessions)
        if key == self._session_key:
            return
        self._session_key = key
        if not sess_cfg.get("alert_on_change", True):
            return

        top = sorted(
            self.states.items(),
            key=lambda kv: abs(kv[1].short.cvd_pct),
            reverse=True,
        )[:5]
        lines = [
            f"UTC Time: {datetime.fromtimestamp(now, timezone.utc).strftime('%Y-%m-%d %H:%M:%S')}",
            f"Active Sessions: {', '.join(sessions) or 'none'}",
            f"Symbols: {len(self.states)}",
            "Top 15m CVD:",
        ]
        for sym, st in top:
            lines.append(f"  {sym}: CVD {st.short.cvd_pct:+.2f}% | OBI {st.obi_now:+.4f} | Price {st.last_price:.8g}")
        lines.extend(self._session_extra_lines(now, sessions))
        body = "\n".join(lines)
        title = f"Session Alert: {', '.join(sessions) or 'Off-session'}"
        self._spawn(self.alerter.send(title, body, "default", "clock"))

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