#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combined: Spot order-flow + CHoCH gates + Futures order-flow/OI/funding.
Order-flow rules unchanged (inherited). No exchange names in alerts.
"""
from __future__ import annotations
import asyncio, json, os, sys, time
from typing import Any, Dict, List, Optional, Tuple
import aiohttp, websockets
from dotenv import load_dotenv
import orderflow_paper as of
import choch_live as cl

load_dotenv()

FUT_WS = os.getenv("OKX_WS", "wss://ws.okx.com:8443/ws/v5/public").strip()
FUT_REST = os.getenv("OKX_REST", "https://www.okx.com/api/v5").rstrip("/")
FUT_ENABLED = os.getenv("FUT_ENABLED", "1") == "1"
FUT_REQUIRED = os.getenv("FUT_REQUIRED", "0") == "1"
FUT_DEPTH_LEVELS = int(os.getenv("FUT_DEPTH_LEVELS", "5"))
FUT_BATCH = int(os.getenv("FUT_BATCH", "60"))
FUT_STALE_SEC = float(os.getenv("FUT_STALE_SEC", "30"))
FUT_CVD_30M = float(os.getenv("FUT_CVD_30M", "3.0"))
FUT_CVD_1H = float(os.getenv("FUT_CVD_1H", "2.5"))
FUT_CVD_4H = float(os.getenv("FUT_CVD_4H", "6.0"))
FUT_SHORT_CVD = float(os.getenv("FUT_SHORT_CVD", "2.0"))
FUT_OBI = float(os.getenv("FUT_OBI", "0.02"))
FUT_MIN_VOLUME_USDT = float(os.getenv("FUT_MIN_VOLUME_USDT", "20000"))
FUT_MIN_TRADES = int(os.getenv("FUT_MIN_TRADES", "30"))
FUT_MIN_FILL = float(os.getenv("FUT_MIN_FILL", "0.50"))


class FuturesState:
    __slots__ = ("symbol", "ct_val", "levels", "windows", "short", "fast",
                 "bid_vol", "ask_vol", "has_depth", "last_price", "last_update")
    def __init__(self, symbol, ct_val, timeframes, bucket_sec, short_sec, fast_sec, whale_mult, retail_mult, levels):
        self.symbol = symbol; self.ct_val = float(ct_val) if float(ct_val) > 0 else 1.0; self.levels = int(levels)
        self.windows = {n: of.RollingWindow(float(tf["seconds"]), float(bucket_sec), float(whale_mult), float(retail_mult))
                        for n, tf in timeframes.items() if n in ("30m", "1h", "4h")}
        self.short = of.RollingWindow(float(short_sec), float(bucket_sec), float(whale_mult), float(retail_mult))
        self.fast = of.RollingWindow(float(fast_sec), float(bucket_sec), float(whale_mult), float(retail_mult))
        self.reset()
    def reset(self):
        for w in self.windows.values(): w.reset()
        self.short.reset(); self.fast.reset()
        self.bid_vol = self.ask_vol = 0.0; self.has_depth = False; self.last_price = 0.0; self.last_update = 0.0
    def add_trade(self, price, size, side, now):
        price, size = float(price), float(size); quote = price * size * self.ct_val; is_buy = str(side).lower() == "buy"
        for w in self.windows.values(): w.add_trade(price, quote, is_buy, now)
        self.short.add_trade(price, quote, is_buy, now); self.fast.add_trade(price, quote, is_buy, now)
        self.last_price = price; self.last_update = now
    def update_depth(self, bids, asks, now):
        if not bids or not asks: return
        bids, asks = bids[: self.levels], asks[: self.levels]
        bb, ba = float(bids[0][0]), float(asks[0][0])
        if bb <= 0 or ba < bb: return
        self.bid_vol = sum(float(p) * float(q) * self.ct_val for p, q in bids)
        self.ask_vol = sum(float(p) * float(q) * self.ct_val for p, q in asks)
        self.has_depth = True; self.last_update = now; obi = self.obi_now
        for w in self.windows.values(): w.add_obi(obi, now)
        self.short.add_obi(obi, now); self.fast.add_obi(obi, now)
    @property
    def obi_now(self):
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0


class FuturesContextPoller:
    def __init__(self):
        self.engine = None; self.last: Dict[str, Dict[str, Any]] = {}
        self._hist: Dict[str, List[Tuple[float, float]]] = {}; self._task = None; self._session = None
    async def start(self):
        self._session = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=15)); self._task = asyncio.create_task(self._loop())
    async def stop(self):
        if self._task:
            self._task.cancel()
            try: await self._task
            except asyncio.CancelledError: pass
        if self._session and not self._session.closed: await self._session.close()
    async def _loop(self):
        while True:
            try: await self._poll()
            except asyncio.CancelledError: raise
            except Exception as exc: of.log.debug("[FUTCTX] poll error: %s", exc)
            await asyncio.sleep(60.0)
    async def _poll(self):
        if self.engine is None or not self.engine.states or self._session is None: return
        now = time.time()
        for sym in list(self.engine.states.keys()):
            if not sym.endswith("USDT"): continue
            inst = f"{sym[:-4]}-USDT-SWAP"
            try:
                d = await cl._get_json(self._session, f"{FUT_REST}/public/open-interest?instId={inst}", 2)
                if str(d.get("code", "0")) != "0": continue
                oi = float(d["data"][0]["oi"])
            except Exception as exc: of.log.debug("[FUTCTX] OI failed %s: %s", sym, exc); continue
            funding = None
            try:
                fr = await cl._get_json(self._session, f"{FUT_REST}/public/funding-rate?instId={inst}", 2)
                if str(fr.get("code", "0")) == "0": funding = float(fr["data"][0]["fundingRate"]) * 100.0
            except Exception: funding = None
            hist = self._hist.setdefault(sym, []); hist.append((now, oi)); del hist[:-130]
            self.last[sym] = {"oi15": self._chg(sym, 900), "oi60": self._chg(sym, 3600), "funding": funding}
            await asyncio.sleep(0.05)
    def _chg(self, sym, secs):
        hist = self._hist.get(sym, [])
        if len(hist) < 2: return 0.0
        now_ts, now_oi = hist[-1]; target = now_ts - secs; ref = hist[0]
        for ts, oi in reversed(hist):
            if ts <= target: ref = (ts, oi); break
        return (now_oi - ref[1]) / ref[1] * 100.0 if ref[1] > 0 else 0.0


class CombinedOrderflowEngine(cl.ChochOrderflowEngine):
    def __init__(self):
        super().__init__()
        self.oi = FuturesContextPoller(); self.oi.engine = self
        self.futures_states: Dict[str, FuturesState] = {}; self._fut_books: Dict[str, Dict[str, Dict[float, float]]] = {}
        self._fut_meta: Dict[str, float] = {}; self._fut_inst_to_sym: Dict[str, str] = {}
        self._fut_task = None; self._fut_warned = False
    async def run(self):
        if FUT_ENABLED: self._fut_task = asyncio.create_task(self._fut_ws_loop())
        try: return await super().run()
        finally:
            if self._fut_task is not None:
                self._fut_task.cancel()
                try: await self._fut_task
                except asyncio.CancelledError: pass
    async def _load_meta(self, session):
        if self._fut_meta: return
        try: data = await cl._get_json(session, f"{FUT_REST}/public/instruments?instType=SWAP")
        except Exception as exc: of.log.warning("[FUT] instruments fetch failed: %s", exc); return
        if str(data.get("code", "0")) != "0": of.log.warning("[FUT] instruments API error: %s", data.get("msg")); return
        raw = data.get("data", []); kept = 0
        for x in raw:
            inst = str(x.get("instId", ""))
            if not inst.endswith("-USDT-SWAP"): continue
            if str(x.get("state", "")).lower() not in ("", "live"): continue
            try: ct = float(x.get("ctVal", "0"))
            except Exception: ct = 0.0
            if ct <= 0: continue
            self._fut_meta[inst] = ct; kept += 1
        of.log.info("[FUT] instruments rows=%d usable=%d", len(raw), kept)
    def _build_states(self):
        if self.futures_states: return
        tf_cfg = of.CONFIG["orderflow_signals"]["timeframes"]
        for sym in self.states:
            if not sym.endswith("USDT"): continue
            inst = f"{sym[:-4]}-USDT-SWAP"; ct = self._fut_meta.get(inst)
            if ct is None: continue
            self.futures_states[sym] = FuturesState(sym, ct, tf_cfg, float(of.CONFIG["bucket_seconds"]),
                float(of.CONFIG["short_window_seconds"]), float(of.CONFIG["fast_window_seconds"]),
                float(of.CONFIG["whale_mult"]), float(of.CONFIG["retail_mult"]), FUT_DEPTH_LEVELS)
            self._fut_inst_to_sym[inst] = sym; self._fut_books[inst] = {"bids": {}, "asks": {}}
        of.log.info("[FUT] flow states ready for %d/%d symbols", len(self.futures_states), len(self.states))
    async def _fut_ws_loop(self):
        fails = 0
        while not self.stop.is_set():
            try:
                while not self.states and not self.stop.is_set(): await asyncio.sleep(1.0)
                await self._load_meta(self._get_session()); self._build_states()
                inst_ids = list(self._fut_inst_to_sym)
                if not inst_ids:
                    if not self._fut_warned: of.log.warning("[FUT] no futures instruments mapped"); self._fut_warned = True
                    if await self._sleep_or_stop(30.0): break
                    continue
                self._fut_warned = False
                of.log.info("[FUT] connecting futures feed for %d instruments", len(inst_ids))
                async with websockets.connect(FUT_WS, ping_interval=None, max_size=None, close_timeout=5) as ws:
                    fails = 0; ping = asyncio.create_task(self._fut_ping(ws))
                    try:
                        args = []
                        for inst in inst_ids: args.append({"channel": "trades", "instId": inst})
                        for inst in inst_ids: args.append({"channel": "books5", "instId": inst})
                        for i in range(0, len(args), FUT_BATCH):
                            await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + FUT_BATCH]})); await asyncio.sleep(0.10)
                        of.log.info("[FUT] subscribed trades + books5 for %d instruments", len(inst_ids))
                        async for raw in ws:
                            if self.stop.is_set(): break
                            try: self._fut_handle(raw)
                            except Exception as exc: of.log.debug("[FUT] handle error: %s", exc)
                    finally:
                        ping.cancel()
                        try: await ping
                        except asyncio.CancelledError: pass
            except asyncio.CancelledError: raise
            except Exception as exc:
                fails += 1; of.log.warning("[FUT] feed error #%d: %s: %s", fails, type(exc).__name__, exc)
                if await self._sleep_or_stop(min(2.0 ** (fails - 1), 60.0)): break
    async def _fut_ping(self, ws):
        while True:
            await asyncio.sleep(20)
            try: await ws.send("ping")
            except Exception: return
    def _fut_handle(self, raw):
        try: msg = json.loads(raw)
        except Exception: return
        if not isinstance(msg, dict) or msg.get("event"): return
        arg = msg.get("arg", {}); ch = str(arg.get("channel", "")); inst = str(arg.get("instId", ""))
        sym = self._fut_inst_to_sym.get(inst); fs = self.futures_states.get(sym) if sym else None
        if fs is None: return
        if ch == "trades":
            now = time.time()
            for d in msg.get("data", []):
                try: fs.add_trade(float(d["px"]), float(d["sz"]), str(d.get("side", "buy")), now)
                except Exception: continue
        elif ch.startswith("books"):
            book = self._fut_books.get(inst); data = msg.get("data")
            if book is None or not data: return
            d = data[0]
            if str(msg.get("action", "snapshot")).lower() == "snapshot": book["bids"], book["asks"] = {}, {}
            for key in ("bids", "asks"):
                for row in d.get(key, []) or []:
                    if len(row) < 2: continue
                    p, s = float(row[0]), float(row[1])
                    if s == 0: book[key].pop(p, None)
                    else: book[key][p] = s
            tb = sorted(book["bids"].items(), key=lambda kv: kv[0], reverse=True)[:FUT_DEPTH_LEVELS]
            ta = sorted(book["asks"].items(), key=lambda kv: kv[0])[:FUT_DEPTH_LEVELS]
            if tb and ta: fs.update_depth([[p, s] for p, s in tb], [[p, s] for p, s in ta], time.time())
    def _futures_gate(self, sym, side, legs):
        if not FUT_ENABLED: return True, "disabled"
        fs = self.futures_states.get(sym)
        if fs is None: return False, "no_futures"
        now = time.time()
        if fs.last_update <= 0 or now - fs.last_update > FUT_STALE_SEC: return False, "stale"
        if not fs.has_depth: return False, "no_book"
        thr = {"30m": FUT_CVD_30M, "1h": FUT_CVD_1H, "4h": FUT_CVD_4H}; sgn = 1.0 if side == "BUY" else -1.0
        for leg in legs:
            w = fs.windows.get(leg)
            if w is None: return False, f"no_{leg}"
            if w.fill(now) < FUT_MIN_FILL: return False, "underfill"
            if w.window_volume < FUT_MIN_VOLUME_USDT: return False, "low_volume"
            if w.trade_count < FUT_MIN_TRADES: return False, "low_trades"
            if w.cvd_pct * sgn < thr.get(leg, 2.0): return False, f"weak_{leg}_cvd"
        if fs.short.cvd_pct * sgn < FUT_SHORT_CVD: return False, "weak_15m_cvd"
        if fs.obi_now * sgn < FUT_OBI: return False, "weak_obi"
        return True, "aligned"
    def _trend_allows(self, sym, rname, side, rule):
        ok, reason = super()._trend_allows(sym, rname, side, rule)
        if not ok: return ok, reason
        if FUT_ENABLED and FUT_REQUIRED:
            ok2, r2 = self._futures_gate(sym, side, list(rule.get("legs", [])))
            if not ok2: return False, f"futures_{r2}"
        return True, ""
    def _signal_context_lines(self, sym, rname, side, now):
        lines = []; st = self.states.get(sym); oi = self.oi.last.get(sym)
        if oi and st is not None:
            w = st.windows.get("1h"); move = w.move_pct(st.last_price) if w is not None else 0.0
            ftxt = f"{oi['funding']:+.4f}%" if oi.get("funding") is not None else "n/a"
            lines.append(f"OI (Futures): 15m {oi['oi15']:+.2f}% | 1h {oi['oi60']:+.2f}% | funding {ftxt} | {self._oi_tag(oi['oi60'], move)}")
        for fr in ("1h", "4h", "1d"):
            tr = self._choch_state(sym, fr).get("trend")
            if tr: lines.append(f"Trend ({fr.upper()}): {tr.capitalize()}")
        lines.append(f"CHoCH context: {self._gate_line(sym, side, now)}")
        tr = self._choch_state(sym, "1w").get("trend")
        if tr: lines.append(f"HTF context: 1w: {tr.capitalize()}")
        if FUT_ENABLED:
            fs = self.futures_states.get(sym)
            legs = list(of.CONFIG["orderflow_signals"]["rules"].get(rname, {}).get("legs", []))
            if fs is not None:
                parts = [f"{l} CVD {fs.windows[l].cvd_pct:+.2f}%" for l in legs if l in fs.windows]
                parts.append(f"15m CVD {fs.short.cvd_pct:+.2f}%"); parts.append(f"OBI {fs.obi_now:+.4f}")
                lines.append(f"Futures ({sym}): " + " | ".join(parts))
                vol_parts = [f"{l} ${fs.windows[l].window_volume:,.0f}/{fs.windows[l].trade_count}t" for l in legs if l in fs.windows]
                lines.append("Futures volume: " + " | ".join(vol_parts))
                sgn = 1.0 if side == "BUY" else -1.0
                lead = legs[0] if legs and legs[0] in fs.windows else "1h"; w = fs.windows.get(lead)
                if w is not None:
                    lines.append(f"Futures size ({lead}): whale ${w.whale_net * sgn:+,.0f} | mid ${w.mid_net * sgn:+,.0f} | retail ${w.retail_net * sgn:+,.0f} | source {w.flow_source(side)}")
                gok, gr = self._futures_gate(sym, side, legs)
                lines.append(f"Futures gate: {'ALIGNED' if gok else 'BLOCKED'} ({gr})")
            else: lines.append("Futures: no data")
        return lines
    def _session_extra_lines(self, now, sessions):
        if not FUT_ENABLED or not self.futures_states: return []
        top = sorted(self.futures_states.items(), key=lambda kv: abs(kv[1].short.cvd_pct), reverse=True)[:5]
        lines = ["Top 15m Futures CVD:"]
        for sym, fs in top:
            lines.append(f"  {sym}: CVD {fs.short.cvd_pct:+.2f}% | OBI {fs.obi_now:+.4f} | price {fs.last_price:.8g}")
        return lines


def main():
    load_dotenv(); cl._ensure_mp_start_method(); of.setup_logging()
    of.log.info("[BOOT] combined | CHoCH 1h->30m+1h, 4h->30m+1h, 1d->1h+4h | rest=%s | FUT_REQUIRED=%s",
                of.CONFIG["rest_schedule"]["enabled"], FUT_REQUIRED)
    code = 0
    try: code = asyncio.run(CombinedOrderflowEngine().run())
    except KeyboardInterrupt: of.log.info("[ENGINE] Interrupted (graceful stop)")
    sys.exit(code)


if __name__ == "__main__":
    main()