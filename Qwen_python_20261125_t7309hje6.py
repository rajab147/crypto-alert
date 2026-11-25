#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
CHoCH-gated order-flow. LuxAlgo Fractal, CLOSED candles only.
  1h CHoCH -> 30m + 1h   (base "30m+1h" values, unchanged)
  4h CHoCH -> 30m + 1h   (base "30m+1h" values, unchanged)
  1d CHoCH -> 1h + 4h    (1h from base "1h_daily", 4h from base "4h", unchanged)
"""
from __future__ import annotations

import asyncio
import multiprocessing
import os
import sys
import time
import warnings
from concurrent.futures import ProcessPoolExecutor
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
from dotenv import load_dotenv

import orderflow_paper as of

load_dotenv()


def _ensure_mp_start_method() -> None:
    try:
        if multiprocessing.get_start_method(allow_none=True) is None:
            multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        pass


def _worker_init() -> None:
    import signal as _s
    _s.signal(_s.SIGINT, _s.SIG_IGN)


CHOCH_WORKERS = int(os.getenv("CHOCH_WORKERS", "2"))
_req = os.getenv("CHOCH_ENGINE", "lux").strip().lower()
CHOCH_ENGINE = "lux"
if _req != "lux":
    warnings.warn(f"CHOCH_ENGINE={_req!r} ignored; LuxAlgo Fractal only", RuntimeWarning)

LUX_LENGTH = int(os.getenv("LUX_LENGTH", "5"))
if LUX_LENGTH < 3:
    raise ValueError("LUX_LENGTH must be >= 3")

CHOCH_FRAMES: Dict[str, Dict[str, Any]] = {
    "1h": {"seconds": 3600, "interval": "1h", "initial_candles": 1000},
    "4h": {"seconds": 14400, "interval": "4h", "initial_candles": 1000},
    "1d": {"seconds": 86400, "interval": "1d", "initial_candles": 1000},
    "1w": {"seconds": 604800, "interval": "1w", "initial_candles": 1000},
}
CONTEXT_FRAMES = ["1h", "4h", "1d"]

# ===== RULES: every value copied UNCHANGED; only legs + gate mapped =====
_b = of.CONFIG["orderflow_signals"]["rules"]

_r1 = dict(_b["30m+1h"])
_r1.update({"enabled": True, "legs": ["30m", "1h"], "trend_frame": "1h"})

_r4 = dict(_b["30m+1h"])
_r4.update({"enabled": True, "legs": ["30m", "1h"], "trend_frame": "4h"})

_rd = dict(_b["4h"])
_rd.update({
    "enabled": True, "legs": ["1h", "4h"], "trend_frame": "1d",
    "cvd_pct": {"1h": float(_b["1h_daily"]["cvd_pct"]["1h"]),
                "4h": float(_b["4h"]["cvd_pct"]["4h"])},
})

of.CONFIG["orderflow_signals"]["rules"] = {"1h_choch": _r1, "4h_choch": _r4, "1d_choch": _rd}


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


def _sgn(x: float) -> int:
    return (x > 0) - (x < 0)


def detect_lux(candles: List[List[float]], length: int = 5) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    if not isinstance(length, int) or isinstance(length, bool) or length < 3:
        raise ValueError("LUX_LENGTH must be an integer >= 3")
    n_bars = len(candles)
    p = int(length / 2)
    hi = [c[2] for c in candles]
    lo = [c[3] for c in candles]
    cl = [c[4] for c in candles]

    def dsum(arr: List[float], i: int) -> int:
        return sum(_sgn(arr[i - k] - arr[i - k - 1]) for k in range(p))

    up_val: Optional[float] = None
    up_crossed = False
    lo_val: Optional[float] = None
    lo_crossed = False
    os_ = 0
    events: List[Dict[str, Any]] = []
    start = max(length - 1, 2 * p)

    for i in range(start, n_bars):
        prev_up, prev_lo = up_val, lo_val
        bullf = (dsum(hi, i) == -p and dsum(hi, i - p) == p
                 and hi[i - p] == max(hi[i - length + 1: i + 1]))
        bearf = (dsum(lo, i) == p and dsum(lo, i - p) == -p
                 and lo[i - p] == min(lo[i - length + 1: i + 1]))
        if bullf:
            up_val, up_crossed = hi[i - p], False
        if bearf:
            lo_val, lo_crossed = lo[i - p], False
        c, c1 = cl[i], cl[i - 1]
        if up_val is not None and prev_up is not None and c > up_val and c1 <= prev_up and not up_crossed:
            if os_ == -1:
                events.append({"ts": candles[i][0], "type": "bullish", "level": up_val, "close": c,
                               "strength": "strong", "mode": "lux"})
            up_crossed = True
            os_ = 1
        if lo_val is not None and prev_lo is not None and c < lo_val and c1 >= prev_lo and not lo_crossed:
            if os_ == 1:
                events.append({"ts": candles[i][0], "type": "bearish", "level": lo_val, "close": c,
                               "strength": "strong", "mode": "lux"})
            lo_crossed = True
            os_ = -1

    trend = "bullish" if os_ == 1 else "bearish" if os_ == -1 else None
    return events, trend


def _sync_analyze(candles: List[List[float]]) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    return detect_lux(candles, LUX_LENGTH)


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
            if not sym.endswith("USDT"):
                continue
            base = sym[:-4]
            oi: Optional[float] = None
            try:
                data = await _get_json(
                    self._session,
                    f"https://www.okx.com/api/v5/public/open-interest?instId={base}-USDT-SWAP", 2)
                oi = float(data["data"][0]["oi"])
            except Exception:  # noqa: BLE001
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
            self.last[sym] = {"oi15": self._chg(sym, 900), "oi60": self._chg(sym, 3600)}
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
        return (now_oi - ref[1]) / ref[1] * 100.0 if ref[1] > 0 else 0.0


class ChochOrderflowEngine(of.Engine):
    def __init__(self) -> None:
        super().__init__()
        self.choch: Dict[str, Dict[str, Dict[str, Any]]] = {}
        self._choch_executor = ProcessPoolExecutor(max_workers=CHOCH_WORKERS, initializer=_worker_init)
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
            self._choch_executor.shutdown(wait=False, cancel_futures=True)
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
            "latest": None, "active": None, "next_due_ms": 0})

    async def _choch_fetch(self, session: aiohttp.ClientSession, sym: str, frame: str,
                           st: Dict[str, Any]) -> bool:
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
            st["next_due_ms"] = candles[-1][0] + 2 * tfc["seconds"] * 1000 + 5_000
            return
        if len(candles) < max(30, LUX_LENGTH * 4 + 10):
            st["next_due_ms"] = now_ms + 60_000
            return
        loop = asyncio.get_running_loop()
        events, trend = await loop.run_in_executor(self._choch_executor, _sync_analyze, candles)
        st["events"] = events
        st["trend"] = trend
        st["latest"] = events[-1] if events else None
        st["active"] = st["latest"]
        st["next_due_ms"] = candles[-1][0] + 2 * tfc["seconds"] * 1000 + 5_000
        if events:
            ev = events[-1]
            of.log.info("[CHOCH] %s %s %s CHoCH | level %.8f close %.8f",
                        sym, frame, ev["type"].upper(), ev["level"], ev["close"])

    async def _choch_refresh(self, sym: str) -> None:
        try:
            async with self._choch_sem:
                session = self._get_session()
                for frame in CHOCH_FRAMES:
                    try:
                        await self._choch_update(session, sym, frame)
                    except Exception as exc:  # noqa: BLE001
                        self._choch_state(sym, frame)["next_due_ms"] = int(time.time() * 1000) + 60_000
                        of.log.debug("[CHOCH] %s %s fetch failed: %s", sym, frame, exc)
                    await asyncio.sleep(0.05)
        finally:
            self._choch_busy[sym] = False

    def _evaluate(self, sym: str, st: Any, now: float) -> None:
        now_ms = int(now * 1000)
        if not self._choch_busy.get(sym, False):
            if any(now_ms >= self._choch_state(sym, fr)["next_due_ms"] for fr in CHOCH_FRAMES):
                self._choch_busy[sym] = True
                self._spawn(self._choch_refresh(sym))
        super()._evaluate(sym, st, now)

    def _trend_allows(self, sym: str, rname: str, side: str, rule: Dict[str, Any]) -> Tuple[bool, str]:
        frame = rule.get("trend_frame")
        if not frame:
            return True, ""
        cst = self._choch_state(sym, frame)
        if int(time.time() * 1000) >= cst["next_due_ms"]:
            return False, f"{frame}_refresh_pending"
        trend = cst.get("trend")
        if trend is None:
            return False, f"no_{frame}_trend"
        want = "bullish" if side == "BUY" else "bearish"
        if trend != want:
            return False, f"{side.lower()}_against_{frame}_{trend}"
        return True, ""

    def _format_choch_event(self, ev: Dict[str, Any], frame: str, now: float) -> str:
        age_h = (now - float(ev["ts"]) / 1000.0) / 3600.0
        return (f"{frame} {ev['type'].capitalize()} CHoCH ({age_h:.1f}h ago, "
                f"level {ev['level']:g}, close {ev['close']:g})")

    def _gate_line(self, sym: str, side: str, now: float) -> str:
        hits: List[str] = []
        want = "bullish" if side == "BUY" else "bearish"
        for fr in CONTEXT_FRAMES:
            ev = self._choch_state(sym, fr).get("active")
            if not ev:
                continue
            tag = "" if ev["type"] == want else "[opposite] "
            hits.append(tag + self._format_choch_event(ev, fr, now))
        return "; ".join(hits) if hits else "none"

    @staticmethod
    def _oi_tag(oi60: float, move: float) -> str:
        thr = 0.3
        if oi60 > thr:
            return "NEW LONGS (strong)" if move >= 0 else "NEW SHORTS (strong)"
        if oi60 < -thr:
            return "SHORT COVERING (weak)" if move >= 0 else "LONG CLOSING (weak)"
        return "flat"

    def _signal_context_lines(self, sym: str, rname: str, side: str, now: float) -> List[str]:
        lines: List[str] = []
        st = self.states.get(sym)
        oi = self.oi.last.get(sym)
        if oi and st is not None:
            w = st.windows.get("1h")
            move = w.move_pct(st.last_price) if w is not None else 0.0
            lines.append(f"OI (Futures): 15m {oi['oi15']:+.2f}% | 1h {oi['oi60']:+.2f}% | "
                         f"{self._oi_tag(oi['oi60'], move)}")
        for fr in ("1h", "4h", "1d"):
            tr = self._choch_state(sym, fr).get("trend")
            if tr:
                lines.append(f"Trend ({fr.upper()}): {tr.capitalize()}")
        lines.append(f"CHoCH context: {self._gate_line(sym, side, now)}")
        tr = self._choch_state(sym, "1w").get("trend")
        if tr:
            lines.append(f"HTF context: 1w: {tr.capitalize()}")
        return lines


def main() -> None:
    load_dotenv()
    _ensure_mp_start_method()
    of.setup_logging()
    of.log.info("[BOOT] CHoCH gates: 1h->30m+1h | 4h->30m+1h | 1d->1h+4h | lux=%d | rest=%s",
                LUX_LENGTH, of.CONFIG["rest_schedule"]["enabled"])
    for rn, r in of.CONFIG["orderflow_signals"]["rules"].items():
        of.log.info("[RULE] %s legs=%s gate=%s cooldown=%s fill=%s cvd=%s",
                    rn, r.get("legs"), r.get("trend_frame"),
                    r.get("cooldown_min"), r.get("min_window_fill"), r.get("cvd_pct"))
    code = 0
    try:
        code = asyncio.run(ChochOrderflowEngine().run())
    except KeyboardInterrupt:
        of.log.info("[ENGINE] Interrupted (graceful stop)")
    sys.exit(code)


if __name__ == "__main__":
    main()