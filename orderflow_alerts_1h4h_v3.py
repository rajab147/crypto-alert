#!/usr/bin/env python3
"""
orderflow_alerts_1h4h_balanced.py - LIVE order-flow BUY / SELL SIGNAL ALERTS on Binance Spot,
measured over 1-HOUR and 4-HOUR windows.
No trading of any kind: the script only watches the market and pushes ntfy alerts.

BALANCED VERSION: Proven v3 logic + smart improvements for GitHub Actions
* OBI window average (anti-spoofing)
* Relaxed spread threshold (GitHub Actions depth data is stale)
* Optional absorption (informational, not blocking)
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
    # --- symbol selection ---
    "max_symbols": 50,
    "min_quote_volume_24h": 10_000_000,
    "min_trades_24h": 10_000,
    "max_price_change_24h_pct": 25.0,
    
    # --- order-flow thresholds (BALANCED for GitHub Actions) ---
    "cvd_threshold": 25.0,
    "obi_threshold": 0.20,
    "obi_window_average": True,       # ✅ anti-spoofing (v3 improvement)
    "spread_max_bps": 15.0,           # ✅ 5.0 سے 15.0 (GitHub Actions پر stale data)
    "eval_interval_sec": 3,
    "warmup_seconds": 5,
    "bucket_seconds": 5,
    "min_window_fill": 0.75,
    "diag_interval_min": 30,
    "div_history_candles": 48,
    
    # --- early-entry rules (BALANCED) ---
    "short_window_seconds": 900,
    "short_cvd_threshold": 20.0,
    "short_max_move_pct": 0.5,        # ✅ v3 کی original value
    "fresh_max_age_sec": 900,         # ✅ v3 کی original value (15 min)
    
    # --- multi-timeframe confirmation ---
    "mtf_cvd_min_abs_pct": 5.0,
    "mtf_obi_min_abs": 0.05,
    
    # --- footprint (v3 proven values) ---
    "footprint": {
        "row_bps": 3.0,
        "keep_candles": 3,
        "min_rows": 5,
        "zone_rows": 3,
        "absorb_min_frac": 0.10,
        "imbalance_ratio": 3.0,
        "imbalance_min_frac": 0.004,
        "min_stacked": 2,
        "min_confirms": 1,
        "flow_zone_rows": 6,
        "flow_min_ratio": 1.5,
        "required": True,
    },
    
    # --- timeframes (BALANCED) ---
    "timeframes": {
        "1h": {
            "seconds": 3600,
            "cooldown_min": 60,
            "div_candle_seconds": [3600],
            "div_base_seconds": 60,
            "confirm_candle_seconds": 900,
            "min_window_volume_usdt": 1_000_000,  # ✅ 1.5M سے 1.0M
            "min_trade_count": 800,               # ✅ 1000 سے 800
            "max_recent_move_pct": 1.0,           # ✅ v3 کی original value
            "div_max_move_since_swing_pct": 0.35,
            "div_min_price_diff_pct": 0.10,
            "div_min_cvd_diff_frac": 0.02,
        },
        "4h": {
            "seconds": 14400,
            "cooldown_min": 240,
            "div_candle_seconds": [14400],
            "div_base_seconds": 300,
            "confirm_candle_seconds": 1800,
            "min_window_volume_usdt": 3_000_000,  # ✅ 5.0M سے 3.0M
            "min_trade_count": 3000,              # ✅ 4000 سے 3000
            "max_recent_move_pct": 1.5,           # ✅ v3 کی original value
            "div_max_move_since_swing_pct": 0.80,
            "div_min_price_diff_pct": 0.15,
            "div_min_cvd_diff_frac": 0.02,
        },
    },
    
    # --- alerts ---
    "max_alerts_per_day": 100,
    
    # --- supporting settings ---
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
        qv, cnt, chg = float(t["quoteVolume"]), int(t["count"]), float(t["priceChangePercent"])
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
            log.info("[%s] #%d quoteVolume=$%s trades24h=%s change24h=%+.2f%%", p["symbol"], i,
                     f"{p['quote_volume']:,.0f}", f"{p['count']:,}", p["change_pct"])
        return picked

class RollingWindow:
    def __init__(self, seconds: float, bucket_sec: float) -> None:
        self.window, self.bucket = float(seconds), float(bucket_sec)
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
            self.obi_n, self.obi_sum = 0, 0.0

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

DIV_TEXT = {
    ("BUY", "regular"): "Regular Bullish Divergence (price lower low, CVD higher low)",
    ("BUY", "hidden"): "Hidden Bullish Divergence (price higher low, CVD lower low)",
    ("SELL", "regular"): "Regular Bearish Divergence (price higher high, CVD lower high)",
    ("SELL", "hidden"): "Hidden Bearish Divergence (price lower high, CVD higher high)",
}

class Candle:
    __slots__ = ("id", "open", "high", "low", "close", "cvd_open", "cvd_high", "cvd_low", "cvd_close",
                 "buy", "sell", "high_ts", "low_ts", "high_live", "low_live", "partial", "row", "fp")

    def __init__(self, cid: int, price: float, cvd: float, ts: float, row: float,
                 live: bool, partial: bool = False) -> None:
        self.id = cid
        self.open = self.high = self.low = self.close = price
        self.cvd_open = self.cvd_high = self.cvd_low = self.cvd_close = cvd
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
        self.cum = 0.0
        self.seen = {"low": -1, "high": -1}

    def _row(self, price: float) -> float:
        raw = price * self.fp_cfg["row_bps"] / 10_000.0
        return round(max(self.tick, round(raw / self.tick) * self.tick), 12)

    def load_history(self, rows: list[list[Any]]) -> None:
        self.reset()
        now_ms = time.time() * 1000.0
        cum = 0.0
        cur: Candle | None = None
        for r in rows:
            if float(r[6]) >= now_ms:
                continue
            ts = float(r[6]) / 1000.0
            cid = int(float(r[0]) // 1000 // self.candle)
            qv, tb = float(r[7]), float(r[10])
            before, after = cum, cum + 2.0 * tb - qv
            if cur is None or cur.id != cid:
                cur = Candle(cid, float(r[1]), before, ts, 0.0, live=False)
                self.candles.append(cur)
            hi, lo = float(r[2]), float(r[3])
            if hi > cur.high:
                cur.high, cur.high_ts = hi, ts
            if lo < cur.low:
                cur.low, cur.low_ts = lo, ts
            cur.close = float(r[4])
            cur.cvd_high = max(cur.cvd_high, before, after)
            cur.cvd_low = min(cur.cvd_low, before, after)
            cur.cvd_close = after
            cur.buy += tb
            cur.sell += qv - tb
            cum = after
        self.cum = cum
        for c in self.candles:
            c.row = self._row(c.close)

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
        if price < c.low:
            c.low, c.low_ts, c.low_live = price, now, True
        c.close = price
        c.cvd_close = self.cum
        if self.cum > c.cvd_high:
            c.cvd_high = self.cum
        if self.cum < c.cvd_low:
            c.cvd_low = self.cum
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

    def early_candidates(self) -> list[dict[str, Any]]:
        cs = list(self.candles)
        n = len(cs)
        if n < 4:
            return []
        out: list[dict[str, Any]] = []
        for pivot in ("low", "high"):
            ref_i: int | None = None
            for i in range(n - 3, 0, -1):
                prev, cur, nxt = cs[i - 1], cs[i], cs[i + 1]
                if cur.partial or cur.id != prev.id + 1 or nxt.id != cur.id + 1:
                    continue
                if pivot == "low":
                    hit = cur.low < prev.low and cur.low <= nxt.low
                else:
                    hit = cur.high > prev.high and cur.high >= nxt.high
                if hit:
                    ref_i = i
                    break
            if ref_i is None:
                continue
            tail = [(j, cs[j]) for j in range(ref_i + 1, n) if not cs[j].partial]
            if not tail:
                continue
            if pivot == "low":
                j, cand = min(tail, key=lambda t: (t[1].low, -t[0]))
            else:
                j, cand = max(tail, key=lambda t: (t[1].high, t[0]))
            if j < n - 2 or cand.id <= self.seen[pivot]:
                continue
            ref = cs[ref_i]
            if pivot == "low":
                p1, p2, v1, v2, ts = ref.low, cand.low, ref.cvd_low, cand.cvd_low, cand.low_ts
            else:
                p1, p2, v1, v2, ts = ref.high, cand.high, ref.cvd_high, cand.cvd_high, cand.high_ts
            found: tuple[str, str] | None = None
            if pivot == "low":
                if p2 < p1 and v2 > v1:
                    found = ("BUY", "regular")
                elif p2 > p1 and v2 < v1:
                    found = ("BUY", "hidden")
            else:
                if p2 > p1 and v2 < v1:
                    found = ("SELL", "regular")
                elif p2 < p1 and v2 > v1:
                    found = ("SELL", "hidden")
            if found:
                out.append({"pivot": pivot, "side": found[0], "kind": found[1], "p1": p1, "p2": p2,
                            "v1": v1, "v2": v2, "ts": ts, "cand": cand, "ref": ref})
        return out

    def mark(self, d: dict[str, Any]) -> None:
        self.seen[d["pivot"]] = max(self.seen[d["pivot"]], d["cand"].id)

    def flow_footprint_check(self, side: str, last_price: float, f: dict[str, Any]) -> dict[str, Any]:
        res: dict[str, Any] = {"available": False, "ok": False, "ratio": 0.0, "stacked": 0, "row": 0.0}
        c = self.candles[-1] if self.candles else None
        if c is None or not c.fp or c.row <= 0 or c.partial:
            return res
        fp, z = c.fp, int(f["flow_zone_rows"])
        total = sum(a + b for a, b in fp.values())
        cur = int(last_price // c.row)
        levels = range(cur - z + 1, cur + 1) if side == "BUY" else range(cur, cur + z)
        def vol(level: int, i: int) -> float:
            cell = fp.get(level)
            return cell[i] if cell else 0.0
        buy = sum(vol(lv, 1) for lv in levels)
        sell = sum(vol(lv, 0) for lv in levels)
        if total <= 0 or buy + sell < f["imbalance_min_frac"] * total:
            return res
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
        if not fp or not live or cand.row <= 0:
            return res
        lo, hi = min(fp), max(fp)
        res["span"] = hi - lo + 1
        total = sum(s + b for s, b in fp.values())
        if total <= 0 or res["span"] < f["min_rows"]:
            return res
        res["available"] = True
        z = int(f["zone_rows"])
        min_vol = f["imbalance_min_frac"] * total
        ratio = f["imbalance_ratio"]
        def vol(level: int, side: int) -> float:
            cell = fp.get(level)
            return cell[side] if cell else 0.0
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
        self.last_price = 0.0
        self.last_trade_ts = 0.0
        self.bid_vol = self.ask_vol = 0.0
        self.best_bid = self.best_ask = 0.0
        self.has_depth = False
        self.last_update = 0.0
        self.last_confirm: dict[str, int] = {}
        self.cside: dict[str, str | None] = {}
        self.cross_ts: dict[str, float] = {}

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

    def obi_for(self, name: str, use_average: bool) -> float:
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
        return {"day": self.day, "alerts_today": self.alerts_today, "last_alert": self.last_alert,
                "today_symbols": self.today_symbols, "today_counts": self.today_counts,
                "saved_at": datetime.now(timezone.utc).isoformat()}

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

    async def run(self) -> int:
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:
                signal.signal(sig, lambda s, f, n=sig.name: loop.call_soon_threadsafe(self.request_stop, n))
        lvl = int(self.cfg["depth_levels"])
        if lvl not in (5, 10, 20):
            raise ValueError(f"depth_levels must be 5, 10 or 20 (Binance limit), got {lvl}")
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
            await self.alerter.close()
            return 1
        symbols = [p["symbol"] for p in picked]
        if not symbols:
            log.error("[ENGINE] No symbols passed the filters; exiting")
            await self.alerter.close()
            return 1
        ticks = {p["symbol"]: float(p.get("tick", 0.0)) for p in picked}
        for s in symbols:
            self.states[s] = OrderFlowState(s, self.cfg["timeframes"], self.cfg["bucket_seconds"], lvl,
                                            int(self.cfg["div_history_candles"]),
                                            float(self.cfg["short_window_seconds"]),
                                            ticks.get(s, 0.0), self.cfg["footprint"])
        if self.book.day != utc_day():
            await self._rollover()
        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@aggTrade/{s.lower()}@depth{lvl}@100ms" for s in symbols)
        log.info("[ENGINE] Starting with %d symbols, timeframes=%s (alerts only, no trading)",
                 len(symbols), ",".join(self.cfg["timeframes"]))
        tasks = [asyncio.create_task(c) for c in (self._ws_loop(url), self._eval_loop(),
                                                   self._save_loop(), self._daily_loop(),
                                                   self._diag_loop())]
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
            log.warning("[ENGINE] Candle backfill failed (%s); divergence history will build live", exc)

    async def _backfill_all(self) -> None:
        hist = int(self.cfg["div_history_candles"])
        selector = SymbolSelector(self.cfg)
        sem = asyncio.Semaphore(8)
        loaded = 0
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def one(sym: str, tf_name: str, sec: int, series: CandleSeries) -> None:
                nonlocal loaded
                base = int(self.cfg["timeframes"][tf_name]["div_base_seconds"])
                iv = KLINE_INTERVALS.get(base)
                if iv is None or sec % base:
                    log.warning("[%s] Bad div_base_seconds=%d for %ds candles; no backfill", sym, base, sec)
                    return
                start_ms = int((time.time() // sec - hist) * sec * 1000)
                async with sem:
                    try:
                        rows: list[list[Any]] = []
                        while True:
                            data = await selector._get(
                                session, f"{REST_BASE}/api/v3/klines?symbol={sym}&interval={iv}"
                                         f"&startTime={start_ms}&limit=1000")
                            rows.extend(data)
                            if len(data) < 1000:
                                break
                            start_ms = int(data[-1][0]) + base * 1000
                        series.load_history(rows)
                        loaded += 1
                    except Exception as exc:
                        log.warning("[%s] Kline backfill %s failed: %s", sym, iv, exc)
            await asyncio.gather(*(one(sym, name, sec, series) for sym, st in self.states.items()
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
                    log.info("[ENGINE] Reconnecting after session end (e.g. 24h limit) in %.0fs", delay)
                else:
                    fails += 1
                    delay = min(2.0 ** (fails - 1), self.cfg["reconnect_backoff_max_sec"])
                    log.warning("[ENGINE] Reconnect failure #%d, retry in %.0fs", fails, delay)
                if fails >= self.cfg["reconnect_fail_alert_after"] and not alerted:
                    alerted = True
                    self._spawn(self.alerter.send("WebSocket reconnect failing",
                                                  f"{fails} consecutive reconnect failures.", "high", "warning"))
                if await self._sleep_or_stop(delay):
                    break

    async def _eval_loop(self) -> None:
        while not await self._sleep_or_stop(self.cfg["eval_interval_sec"]):
            if not self.connected:
                continue
            now = time.time()
            for sym, st in list(self.states.items()):
                try:
                    self._evaluate(sym, st, now)
                except Exception as exc:
                    log.exception("[%s] Evaluation error: %s", sym, exc)

    def _count(self, key: str) -> None:
        self.block_counts[key] = self.block_counts.get(key, 0) + 1

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

    def _tf_status(self, st: OrderFlowState, name: str, tf: dict[str, Any]
                   ) -> tuple[str, str | None, dict[str, float]]:
        c = self.cfg
        w = st.windows[name]
        move = st.recent_move_pct(name, c["min_window_fill"])
        smove = st.short.recent_move_pct(st.last_price, st.last_trade_ts, c["min_window_fill"])
        cvd, obi = w.cvd_pct, st.obi_for(name, c["obi_window_average"])
        scvd = st.short.cvd_pct
        m = {"cvd": cvd, "obi": obi, "vol": w.window_volume, "trades": float(w.trade_count), "move": move,
             "scvd": scvd, "smove": smove}
        if w.window_volume <= tf["min_window_volume_usdt"]:
            return "volume", None, m
        if w.trade_count < tf["min_trade_count"]:
            return "trades", None, m
        if move >= 999.0 or smove >= 999.0:
            return "window_filling", None, m
        if move >= tf["max_recent_move_pct"]:
            return "move", None, m
        if smove >= c["short_max_move_pct"]:
            return "short_move", None, m
        if cvd > c["cvd_threshold"] and scvd > c["short_cvd_threshold"] and obi > c["obi_threshold"]:
            return "ok", "BUY", m
        if cvd < -c["cvd_threshold"] and scvd < -c["short_cvd_threshold"] and obi < -c["obi_threshold"]:
            return "ok", "SELL", m
        return "cvd_obi", None, m

    def _mtf_confirmation(self, st: OrderFlowState, side: str) -> str | None:
        c = self.cfg
        sign = 1.0 if side == "BUY" else -1.0
        frames: dict[str, RollingWindow] = {
            "4h": st.windows["4h"],
            "1h": st.windows["1h"],
            "30m": st.mtf_windows["30m"],
            "15m": st.short,
        }
        for frame, window in frames.items():
            filled_move = window.recent_move_pct(
                st.last_price, st.last_trade_ts, c["min_window_fill"]
            )
            if filled_move >= 999.0:
                return f"mtf_{frame}_warmup"
            cvd = window.cvd_pct
            obi = window.avg_obi
            if sign * cvd < c["mtf_cvd_min_abs_pct"]:
                return f"mtf_{frame}_cvd"
            if obi is None or sign * obi < c["mtf_obi_min_abs"]:
                return f"mtf_{frame}_obi"
        return None

    def _cvd_side(self, m: dict[str, float]) -> str | None:
        c = self.cfg
        if m["cvd"] > c["cvd_threshold"] and m["scvd"] > c["short_cvd_threshold"]:
            return "BUY"
        if m["cvd"] < -c["cvd_threshold"] and m["scvd"] < -c["short_cvd_threshold"]:
            return "SELL"
        return None

    def _update_cross(self, st: OrderFlowState, name: str, m: dict[str, float], now: float) -> None:
        side = self._cvd_side(m)
        prev = st.cside.get(name, "?")
        if prev == "?":
            st.cross_ts[name] = 0.0
        elif side != prev:
            st.cross_ts[name] = now if side else 0.0
        st.cside[name] = side

    def _evaluate(self, sym: str, st: OrderFlowState, now: float) -> None:
        c = self.cfg
        st.evict(now)
        gb = self._global_block(st, now)
        if gb:
            self._count(f"all:{gb}")
            return
        for name, tf in c["timeframes"].items():
            reason, side, m = self._tf_status(st, name, tf)
            self._update_cross(st, name, m, now)
            if side is None:
                self._count(f"{name}:{reason}")
                continue
            mtf_block = self._mtf_confirmation(st, side)
            if mtf_block:
                self._count(f"{name}:{mtf_block}")
                continue
            age = now - st.cross_ts.get(name, 0.0)
            if age > c["fresh_max_age_sec"]:
                self._count(f"{name}:not_fresh")
                continue
            series = next(iter(st.candles[name].values()))
            fpf = series.flow_footprint_check(side, st.last_price, c["footprint"])
            if not fpf["available"]:
                if c["footprint"]["required"]:
                    self._count(f"{name}:fp_unavailable")
                    continue
            elif not fpf["ok"]:
                self._count(f"{name}:fp_weak")
                continue
            key = f"{sym}|{name}|{side}"
            if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                self._count(f"{name}:cooldown")
                continue
            self._count(f"{name}:signal")
            self._alert(sym, name, side, st, m, age, now, key, fpf)
        for name, tf in c["timeframes"].items():
            self._check_divergence(sym, name, tf, st, now)

    def _check_divergence(self, sym: str, name: str, tf: dict[str, Any],
                          st: OrderFlowState, now: float) -> None:
        confirm_sec = int(tf.get("confirm_candle_seconds", 0))
        if confirm_sec <= 0:
            return
        cid = int(now // confirm_sec)
        previous_cid = st.last_confirm.get(name)
        if previous_cid is None:
            st.last_confirm[name] = cid
            return
        if previous_cid == cid:
            return
        st.last_confirm[name] = cid
        w = st.windows[name]
        if not (w.window_volume > tf["min_window_volume_usdt"] and w.trade_count >= tf["min_trade_count"]):
            self._count(f"{name}/div:volume")
            return
        for sec, series in st.candles[name].items():
            cl = candle_label(sec)
            for d in series.early_candidates():
                reason = self._div_gate(d, series, st, w, tf)
                if reason:
                    self._count(f"{name}/{cl}-div:{reason}")
                    continue
                key = f"{sym}|{name}|{cl}|{d['side']}|{d['kind']}-div"
                if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                    self._count(f"{name}/{cl}-div:cooldown")
                    continue
                self._count(f"{name}/{cl}-div:signal")
                series.mark(d)
                self._alert_div(sym, name, cl, d, st, now, key)

    def _div_gate(self, d: dict[str, Any], series: CandleSeries, st: OrderFlowState,
                  w: RollingWindow, tf: dict[str, Any]) -> str | None:
        f = self.cfg["footprint"]
        p1, p2 = d["p1"], d["p2"]
        if p1 <= 0 or p2 <= 0:
            return "weak_price"
        move = (st.last_price - p2) if d["side"] == "BUY" else (p2 - st.last_price)
        d["since"] = move / p2 * 100.0
        d["fp"] = series.footprint_check(d, st.last_price, f)
        if d["since"] > tf["div_max_move_since_swing_pct"]:
            return "late"
        if abs(p2 - p1) / p1 * 100.0 < tf["div_min_price_diff_pct"]:
            return "weak_price"
        if abs(d["v2"] - d["v1"]) < tf["div_min_cvd_diff_frac"] * w.window_volume:
            return "weak_cvd"
        if not d["fp"]["available"]:
            if f["required"]:
                return "fp_unavailable"
        elif d["fp"]["confirms"] < f["min_confirms"]:
            return "fp_weak"
        return None

    def _alert_div(self, sym: str, tf: str, cl: str, d: dict[str, Any], st: OrderFlowState,
                   now: float, key: str) -> None:
        side, kind, fp = d["side"], d["kind"], d["fp"]
        w = st.windows[tf]
        confirm_label = candle_label(int(self.cfg["timeframes"][tf]["confirm_candle_seconds"]))
        extreme = "low" if side == "BUY" else "high"
        if fp["available"]:
            heavy = "sell" if side == "BUY" else "buy"
            imb = "buy" if side == "BUY" else "sell"
            fp_line = (f"Footprint (row {fp['row']:.8g}): absorption {'YES' if fp['absorb'] else 'no'} "
                       f"({heavy} vol at {extreme} rows {fp['frac'] * 100:.1f}% of candle), "
                       f"stacked {imb} imbalances x{fp['stacked']}")
        else:
            fp_line = "Footprint: n/a (swing extreme printed before start / candle too narrow)"
        body = (f"Symbol: {sym}\nTimeframe: {tf} (CVD candle divergence on {cl} candles)\n"
                f"Early signal, entry check: {confirm_label} candle close\nSide: {side}\n"
                f"Signal: {DIV_TEXT[(side, kind)]}\n"
                f"Price: {st.last_price:.8g}\nSwing price: {d['p1']:.8g} -> {d['p2']:.8g}\n"
                f"Move since swing: {d['since']:+.2f}% (swing {max(now - d['ts'], 0.0) / 60:.0f} min ago)\n"
                f"CVD candle {extreme} (USDT): {d['v1']:+,.0f} -> {d['v2']:+,.0f}\n"
                f"{fp_line}\n"
                f"CVD% ({tf}): {w.cvd_pct:+.1f}\n"
                f"Spread bps: {st.spread_bps:.2f}\nVolume USDT ({tf}): {w.window_volume:,.0f}\n"
                f"Trades ({tf}): {w.trade_count}")
        if self.book.cap_reached():
            log.info("[%s] %s %s %s-DIV (daily cap %d reached, log only) | %s", sym, tf, side, kind,
                     self.cfg["max_alerts_per_day"], body.replace("\n", " | "))
            self.book.last_alert[key] = now
            return
        self.book.register(key, sym, tf, side, now, label=f"{tf}/{cl} {side} {kind}-div")
        log.info("[%s] %s/%s %s %s-DIV | %s", sym, tf, cl, side, kind, body.replace("\n", " | "))
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._spawn(self.alerter.send(f"{sym} {side} Early Divergence [{tf}]", body, "high", tags))

    def _alert(self, sym: str, tf: str, side: str, st: OrderFlowState, m: dict[str, float],
               age: float, now: float, key: str, fpf: dict[str, Any] | None = None) -> None:
        c = self.cfg
        w = st.windows[tf]
        sl = f"{int(c['short_window_seconds'] // 60)}m"
        obi_label = f"OBI ({tf} avg)" if c["obi_window_average"] else "OBI"
        body = (f"Symbol: {sym}\nTimeframe: {tf}\nSide: {side}\nPrice: {st.last_price:.8g}\n"
                f"CVD% ({tf}): {m['cvd']:+.1f}\nCVD% ({sl}): {m['scvd']:+.1f}\n"
                f"{obi_label}: {m['obi']:+.2f}\nSpread bps: {st.spread_bps:.2f}\n"
                f"Volume USDT ({tf}): {w.window_volume:,.0f}\nTrades ({tf}): {w.trade_count}\n"
                f"Move ({tf}): {m['move']:.2f}%\nMove ({sl}): {m['smove']:.2f}%\n"
                f"CVD cross age: {age / 60:.1f} min")
        if fpf and fpf["available"]:
            body += (f"\nFootprint (row {fpf['row']:.8g}): {side.lower()} volume {fpf['ratio']:.1f}x the other side "
                     f"at current price, stacked imbalances x{fpf['stacked']}")
        else:
            body += "\nFootprint: n/a"
        if self.book.cap_reached():
            log.info("[%s] %s %s SIGNAL (daily cap %d reached, log only) | %s", sym, tf, side,
                     c["max_alerts_per_day"], body.replace("\n", " | "))
            self.book.last_alert[key] = now
            return
        self.book.register(key, sym, tf, side, now)
        log.info("[%s] %s %s SIGNAL | %s", sym, tf, side, body.replace("\n", " | "))
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._spawn(self.alerter.send(f"{sym} {side} Signal [{tf}]", body, "high", tags))

    async def _diag_loop(self) -> None:
        minutes = self.cfg["diag_interval_min"]
        while not await self._sleep_or_stop(minutes * 60):
            if not self.connected:
                continue
            now = time.time()
            counts, self.block_counts = self.block_counts, {}
            groups: dict[str, dict[str, int]] = {}
            for k, n in counts.items():
                grp, reason = k.split(":", 1)
                groups.setdefault(grp, {})[reason] = n
            for grp, d in sorted(groups.items()):
                tot = sum(d.values())
                parts = " ".join(f"{r}={n / tot * 100:.0f}%" for r, n in sorted(d.items(), key=lambda kv: -kv[1]))
                log.info("[DIAG] %s gates over last %dmin (share of checks): %s", grp, minutes, parts)
            for sym, st in self.states.items():
                if self._global_block(st, now):
                    continue
                for name, tf in self.cfg["timeframes"].items():
                    reason, _, m = self._tf_status(st, name, tf)
                    move = "filling" if m["move"] >= 999.0 else f"{m['move']:.2f}%"
                    smove = "filling" if m["smove"] >= 999.0 else f"{m['smove']:.2f}%"
                    log.info("[DIAG] %s %s gate=%s cvd=%+.1f cvd15=%+.1f obi=%+.2f vol=%.1fM(min %.1fM) "
                             "trades=%d(min %d) move=%s(max %.1f%%) move15=%s(max %.1f%%)", sym, name, reason,
                             m["cvd"], m["scvd"], m["obi"], m["vol"] / 1e6,
                             tf["min_window_volume_usdt"] / 1e6, int(m["trades"]), tf["min_trade_count"],
                             move, tf["max_recent_move_pct"], smove, self.cfg["short_max_move_pct"])

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
        code = asyncio.run(_amain())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")
        sys.exit(code)

async def _amain() -> int:
    return await Engine().run()

if __name__ == "__main__":
    main()
