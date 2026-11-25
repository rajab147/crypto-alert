#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Combined: Spot order-flow + CHoCH gates + REAL futures order-flow.
Futures = actual executed trades + live order book, windows 30m/1h/4h + 15m.
Futures windows persisted. Order-flow rules inherited UNCHANGED.
No exchange names in alerts. No trading.
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import aiohttp
import websockets
from dotenv import load_dotenv

import orderflow_paper as of
import choch_live as cl

load_dotenv()

FUT_WS = os.getenv("OKX_WS", "wss://ws.okx.com:8443/ws/v5/public").strip()
FUT_REST = os.getenv("OKX_REST", "https://www.okx.com/api/v5").rstrip("/")
FUT_ENABLED = os.getenv("FUT_ENABLED", "1") == "1"
FUT_REQUIRED = os.getenv("FUT_REQUIRED", "0") == "1"
FUT_PAIR_ONLY = os.getenv("FUT_PAIR_ONLY", "1") == "1"
FUT_DEPTH_LEVELS = int(os.getenv("FUT_DEPTH_LEVELS", "5"))
FUT_BATCH = int(os.getenv("FUT_BATCH", "60"))
FUT_STALE_SEC = float(os.getenv("FUT_STALE_SEC", "30"))
FUT_STATE_FILE = os.getenv("FUT_STATE_FILE", "futures_state.json")

FUT_CVD = {"30m": float(os.getenv("FUT_CVD_30M", "3.0")),
           "1h": float(os.getenv("FUT_CVD_1H", "2.5")),
           "4h": float(os.getenv("FUT_CVD_4H", "6.0"))}
FUT_SHORT_CVD = float(os.getenv("FUT_SHORT_CVD", "2.0"))
FUT_OBI = float(os.getenv("FUT_OBI", "0.02"))
FUT_MIN_VOLUME_USDT = float(os.getenv("FUT_MIN_VOLUME_USDT", "20000"))
FUT_MIN_TRADES = int(os.getenv("FUT_MIN_TRADES", "30"))
FUT_MIN_FILL = float(os.getenv("FUT_MIN_FILL", "0.50"))


class FuturesState:
    __slots__ = ("symbol", "ct_val", "levels", "windows", "short", "fast",
                 "bid_vol", "ask_vol", "best_bid", "best_ask", "has_depth",
                 "last_price", "last_update", "last_trade_ts", "last_book_ts",
                 "trade_msgs", "book_msgs")

    def __init__(self, symbol: str, ct_val: float, timeframes: Dict[str, Dict[str, Any]],
                 bucket_sec: float, long_bucket: float, long_from: float,
                 short_sec: float, fast_sec: float,
                 whale_mult: float, retail_mult: float, levels: int) -> None:
        self.symbol = symbol
        self.ct_val = float(ct_val) if float(ct_val) > 0 else 1.0
        self.levels = int(levels)

        def bs(sec: float) -> float:
            return long_bucket if float(sec) >= long_from else float(bucket_sec)

        self.windows: Dict[str, Any] = {
            n: of.RollingWindow(float(tf["seconds"]), bs(float(tf["seconds"])),
                                float(whale_mult), float(retail_mult))
            for n, tf in timeframes.items() if n in ("30m", "1h", "4h")}
        self.short = of.RollingWindow(float(short_sec), float(bucket_sec),
                                      float(whale_mult), float(retail_mult))
        self.fast = of.RollingWindow(float(fast_sec), float(bucket_sec),
                                     float(whale_mult), float(retail_mult))
        self.reset()

    def reset(self) -> None:
        for w in self.windows.values():
            w.reset()
        self.short.reset()
        self.fast.reset()
        self.bid_vol = self.ask_vol = 0.0
        self.best_bid = self.best_ask = 0.0
        self.has_depth = False
        self.last_price = 0.0
        self.last_update = 0.0
        self.last_trade_ts = 0.0
        self.last_book_ts = 0.0
        self.trade_msgs = 0
        self.book_msgs = 0

    def add_trade(self, price: float, size: float, side: str, now: float) -> None:
        price, size = float(price), float(size)
        if price <= 0 or size <= 0:
            return
        quote = price * size * self.ct_val
        is_buy = str(side).lower() == "buy"
        for w in self.windows.values():
            w.add_trade(price, quote, is_buy, now)
        self.short.add_trade(price, quote, is_buy, now)
        self.fast.add_trade(price, quote, is_buy, now)
        self.last_price = price
        self.last_trade_ts = now
        self.last_update = now
        self.trade_msgs += 1

    def update_depth(self, bids: List[List[Any]], asks: List[List[Any]], now: float) -> None:
        if not bids or not asks:
            return
        bids, asks = bids[: self.levels], asks[: self.levels]
        if not bids or not asks:
            return
        bb, ba = float(bids[0][0]), float(asks[0][0])
        if bb <= 0 or ba < bb:
            return
        self.bid_vol = sum(float(p) * float(q) * self.ct_val for p, q in bids)
        self.ask_vol = sum(float(p) * float(q) * self.ct_val for p, q in asks)
        self.best_bid, self.best_ask = bb, ba
        self.has_depth = True
        self.last_book_ts = now
        self.last_update = now
        self.book_msgs += 1
        obi = self.obi_now
        for w in self.windows.values():
            w.add_obi(obi, now)
        self.short.add_obi(obi, now)
        self.fast.add_obi(obi, now)

    def evict(self, now: float) -> None:
        for w in self.windows.values():
            w.evict(now)
        self.short.evict(now)
        self.fast.evict(now)

    @property
    def obi_now(self) -> float:
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0

    @property
    def spread_bps(self) -> float:
        if not self.has_depth or self.best_bid <= 0:
            return float("inf")
        mid = (self.best_ask + self.best_bid) / 2.0
        return (self.best_ask - self.best_bid) / mid * 10_000.0

    def to_dict(self) -> Dict[str, Any]:
        return {"ct_val": self.ct_val,
                "windows": {n: w.to_dict() for n, w in self.windows.items()},
                "short": self.short.to_dict(), "fast": self.fast.to_dict(),
                "last_price": self.last_price}

    def load_dict(self, d: Dict[str, Any]) -> None:
        for n, wd in d.get("windows", {}).items():
            if n in self.windows:
                self.windows[n].load_dict(wd)
        if "short" in d:
            self.short.load_dict(d["short"])
        if "fast" in d:
            self.fast.load_dict(d["fast"])
        self.last_price = float(d.get("last_price", 0.0))


class FuturesContextPoller:
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
                of.log.debug("[FUTCTX] poll error: %s", exc)
            await asyncio.sleep(60.0)

    async def _poll(self) -> None:
        if self.engine is None or not self.engine.states or self._session is None:
            return
        now = time.time()
        for sym in list(self.engine.states.keys()):
            if not sym.endswith("USDT"):
                continue
            inst = f"{sym[:-4]}-USDT-SWAP"
            try:
                d = await cl._get_json(self._session, f"{FUT_REST}/public/open-interest?instId={inst}", 2)
                if str(d.get("code", "0")) != "0":
                    continue
                oi = float(d["data"][0]["oi"])
            except Exception as exc:  # noqa: BLE001
                of.log.debug("[FUTCTX] OI failed %s: %s", sym, exc)
                continue
            funding = None
            try:
                fr = await cl._get_json(self._session, f"{FUT_REST}/public/funding-rate?instId={inst}", 2)
                if str(fr.get("code", "0")) == "0":
                    funding = float(fr["data"][0]["fundingRate"]) * 100.0
            except Exception:  # noqa: BLE001
                funding = None
            hist = self._hist.setdefault(sym, [])
            hist.append((now, oi))
            del hist[:-130]
            self.last[sym] = {"oi15": self._chg(sym, 900), "oi60": self._chg(sym, 3600), "funding": funding}
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


class CombinedOrderflowEngine(cl.ChochOrderflowEngine):
    def __init__(self) -> None:
        super().__init__()
        self.oi = FuturesContextPoller()
        self.oi.engine = self
        self.futures_states: Dict[str, FuturesState] = {}
        self._fut_books: Dict[str, Dict[str, Dict[float, float]]] = {}
        self._fut_meta: Dict[str, float] = {}
        self._fut_inst_to_sym: Dict[str, str] = {}
        self._fut_task: Optional[asyncio.Task] = None
        self._fut_warned = False
        self._fut_ready_logged = False

    # ---------- pair universe ----------
    async def _allowed_bases(self, session: aiohttp.ClientSession) -> set[str] | None:
        if not (FUT_ENABLED and FUT_PAIR_ONLY):
            return None
        await self._load_meta(session)
        if not self._fut_meta:
            of.log.warning("[FUT] pair-universe unavailable; keeping all spot symbols")
            return None
        bases = {inst.split("-")[0].upper() for inst in self._fut_meta}
        of.log.info("[FUT] pair-universe ready: %d coins listed on both venues", len(bases))
        return bases

    async def _load_meta(self, session: aiohttp.ClientSession) -> None:
        if self._fut_meta:
            return
        try:
            data = await cl._get_json(session, f"{FUT_REST}/public/instruments?instType=SWAP")
        except Exception as exc:  # noqa: BLE001
            of.log.warning("[FUT] instruments fetch failed: %s", exc)
            return
        if str(data.get("code", "0")) != "0":
            of.log.warning("[FUT] instruments API error: %s", data.get("msg"))
            return

        raw = data.get("data", [])
        kept = 0
        sk_not_swap = sk_quote = sk_state = sk_ctval = 0

        for x in raw:
            inst = str(x.get("instId", ""))
            if not inst.endswith("-USDT-SWAP"):
                sk_not_swap += 1
                continue
            # Derivatives have EMPTY quoteCcy; settleCcy is the real field.
            qc = str(x.get("quoteCcy", "") or "")
            sc = str(x.get("settleCcy", "") or "")
            if qc and qc != "USDT":
                sk_quote += 1
                continue
            if sc and sc != "USDT":
                sk_quote += 1
                continue
            state = str(x.get("state", "") or "").lower()
            if state and state != "live":
                sk_state += 1
                continue
            try:
                ct = float(x.get("ctVal", "0") or 0)
            except Exception:
                ct = 0.0
            if ct <= 0:
                sk_ctval += 1
                continue
            self._fut_meta[inst] = ct
            kept += 1

        if kept == 0:
            of.log.warning("[FUT] instruments rows=%d usable=0 | not_swap=%d quote=%d state=%d ctval=%d",
                           len(raw), sk_not_swap, sk_quote, sk_state, sk_ctval)
            return
        of.log.info("[FUT] instruments rows=%d usable=%d", len(raw), kept)

    # ---------- futures persistence ----------
    def _save_futures(self) -> None:
        try:
            data = {"saved_at": time.time(),
                    "states": {s: fs.to_dict() for s, fs in self.futures_states.items()}}
            tmp = FUT_STATE_FILE + ".tmp"
            with open(tmp, "w", encoding="utf-8") as f:
                json.dump(data, f)
            os.replace(tmp, FUT_STATE_FILE)
        except Exception as exc:  # noqa: BLE001
            of.log.debug("[FUT] state save failed: %s", exc)

    def _load_futures(self) -> None:
        if not os.path.exists(FUT_STATE_FILE):
            return
        try:
            with open(FUT_STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            age = time.time() - float(d.get("saved_at", 0))
            if age > 4 * 3600:
                of.log.info("[FUT] saved windows too old; starting fresh")
                return
            now = time.time()
            n = 0
            for s, sd in d.get("states", {}).items():
                fs = self.futures_states.get(s)
                if fs is None:
                    continue
                fs.load_dict(sd)
                fs.evict(now)
                n += 1
            if n:
                of.log.info("[FUT] Restored warm futures windows for %d symbols (age %.1fmin)", n, age / 60)
        except Exception as exc:  # noqa: BLE001
            of.log.debug("[FUT] state load failed: %s", exc)

    def _save_windows(self) -> None:
        super()._save_windows()
        self._save_futures()

    def _load_windows(self) -> None:
        super()._load_windows()
        self._load_futures()

    # ---------- lifecycle ----------
    async def run(self) -> int:
        if FUT_ENABLED:
            self._fut_task = asyncio.create_task(self._fut_ws_loop())
        try:
            return await super().run()
        finally:
            if self._fut_task is not None:
                self._fut_task.cancel()
                try:
                    await self._fut_task
                except asyncio.CancelledError:
                    pass

    def _build_states(self) -> None:
        if self.futures_states:
            return
        cfg = of.CONFIG
        tf_cfg = cfg["orderflow_signals"]["timeframes"]
        for sym in self.states:
            if not sym.endswith("USDT"):
                continue
            inst = f"{sym[:-4]}-USDT-SWAP"
            ct = self._fut_meta.get(inst)
            if ct is None:
                continue
            self.futures_states[sym] = FuturesState(
                sym, ct, tf_cfg, float(cfg["bucket_seconds"]),
                float(cfg["long_bucket_seconds"]), float(cfg["long_bucket_from_sec"]),
                float(cfg["short_window_seconds"]), float(cfg["fast_window_seconds"]),
                float(cfg["whale_mult"]), float(cfg["retail_mult"]), FUT_DEPTH_LEVELS)
            self._fut_inst_to_sym[inst] = sym
            self._fut_books[inst] = {"bids": {}, "asks": {}}
        of.log.info("[FUT] order-flow states ready for %d/%d symbols",
                    len(self.futures_states), len(self.states))
        self._load_futures()

    async def _fut_ws_loop(self) -> None:
        fails = 0
        while not self.stop.is_set():
            try:
                while not self.states and not self.stop.is_set():
                    await asyncio.sleep(1.0)
                await self._load_meta(self._get_session())
                self._build_states()
                inst_ids = list(self._fut_inst_to_sym)
                if not inst_ids:
                    if not self._fut_warned:
                        of.log.warning("[FUT] no futures instruments mapped")
                        self._fut_warned = True
                    if await self._sleep_or_stop(30.0):
                        break
                    continue
                self._fut_warned = False
                of.log.info("[FUT] connecting futures feed for %d instruments", len(inst_ids))
                async with websockets.connect(FUT_WS, ping_interval=None,
                                              max_size=None, close_timeout=5) as ws:
                    fails = 0
                    ping = asyncio.create_task(self._fut_ping(ws))
                    try:
                        args: List[Dict[str, str]] = []
                        for inst in inst_ids:
                            args.append({"channel": "trades", "instId": inst})
                        for inst in inst_ids:
                            args.append({"channel": "books5", "instId": inst})
                        for i in range(0, len(args), FUT_BATCH):
                            await ws.send(json.dumps({"op": "subscribe", "args": args[i:i + FUT_BATCH]}))
                            await asyncio.sleep(0.10)
                        of.log.info("[FUT] subscribed trades + books5 for %d instruments", len(inst_ids))
                        async for raw in ws:
                            if self.stop.is_set():
                                break
                            try:
                                self._fut_handle(raw)
                            except Exception as exc:
                                of.log.debug("[FUT] handle error: %s", exc)
                            if not self._fut_ready_logged:
                                live = sum(1 for f in self.futures_states.values() if f.trade_msgs > 0)
                                if live >= max(1, len(self.futures_states) // 2):
                                    self._fut_ready_logged = True
                                    of.log.info("[FUT] REAL order-flow receiving: %d/%d symbols have trades",
                                                live, len(self.futures_states))
                    finally:
                        ping.cancel()
                        try:
                            await ping
                        except asyncio.CancelledError:
                            pass
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fails += 1
                of.log.warning("[FUT] feed error #%d: %s: %s", fails, type(exc).__name__, exc)
                if await self._sleep_or_stop(min(2.0 ** (fails - 1), 60.0)):
                    break

    async def _fut_ping(self, ws) -> None:
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send("ping")
            except Exception:
                return

    def _fut_handle(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return
        if not isinstance(msg, dict) or msg.get("event"):
            return
        arg = msg.get("arg", {})
        ch = str(arg.get("channel", ""))
        inst = str(arg.get("instId", ""))
        sym = self._fut_inst_to_sym.get(inst)
        fs = self.futures_states.get(sym) if sym else None
        if fs is None:
            return
        if ch == "trades":
            now = time.time()
            for d in msg.get("data", []):
                try:
                    fs.add_trade(float(d["px"]), float(d["sz"]), str(d.get("side", "buy")), now)
                except Exception:
                    continue
        elif ch.startswith("books"):
            book = self._fut_books.get(inst)
            data = msg.get("data")
            if book is None or not data:
                return
            d = data[0]
            if str(msg.get("action", "snapshot")).lower() == "snapshot":
                book["bids"], book["asks"] = {}, {}
            for key in ("bids", "asks"):
                for row in d.get(key, []) or []:
                    if len(row) < 2:
                        continue
                    try:
                        p, s = float(row[0]), float(row[1])
                    except Exception:
                        continue
                    if s == 0:
                        book[key].pop(p, None)
                    else:
                        book[key][p] = s
            tb = sorted(book["bids"].items(), key=lambda kv: kv[0], reverse=True)[:FUT_DEPTH_LEVELS]
            ta = sorted(book["asks"].items(), key=lambda kv: kv[0])[:FUT_DEPTH_LEVELS]
            if tb and ta:
                fs.update_depth([[p, s] for p, s in tb], [[p, s] for p, s in ta], time.time())

    def _futures_gate(self, sym: str, side: str, legs: List[str]) -> Tuple[bool, str]:
        if not FUT_ENABLED:
            return True, "disabled"
        fs = self.futures_states.get(sym)
        if fs is None:
            return False, "no_futures"
        now = time.time()
        if fs.last_trade_ts <= 0 or now - fs.last_trade_ts > FUT_STALE_SEC:
            return False, "stale_trades"
        if not fs.has_depth or now - fs.last_book_ts > FUT_STALE_SEC:
            return False, "stale_book"
        sgn = 1.0 if side == "BUY" else -1.0
        for leg in legs:
            w = fs.windows.get(leg)
            if w is None:
                return False, f"no_{leg}"
            if w.fill(now) < FUT_MIN_FILL:
                return False, f"underfill_{leg}"
            if w.window_volume < FUT_MIN_VOLUME_USDT:
                return False, "low_volume"
            if w.trade_count < FUT_MIN_TRADES:
                return False, "low_trades"
            if w.cvd_pct * sgn < FUT_CVD.get(leg, 2.0):
                return False, f"weak_{leg}_cvd"
        if fs.short.cvd_pct * sgn < FUT_SHORT_CVD:
            return False, "weak_15m_cvd"
        if fs.obi_now * sgn < FUT_OBI:
            return False, "weak_obi"
        return True, "aligned"

    def _trend_allows(self, sym: str, rname: str, side: str, rule: Dict[str, Any]) -> Tuple[bool, str]:
        ok, reason = super()._trend_allows(sym, rname, side, rule)
        if not ok:
            return ok, reason
        if FUT_ENABLED and FUT_REQUIRED:
            ok2, r2 = self._futures_gate(sym, side, list(rule.get("legs", [])))
            if not ok2:
                return False, f"futures_{r2}"
        return True, ""

    def _signal_context_lines(self, sym: str, rname: str, side: str, now: float) -> List[str]:
        lines: List[str] = []
        st = self.states.get(sym)
        legs: List[str] = list(of.CONFIG["orderflow_signals"]["rules"].get(rname, {}).get("legs", []))
        sgn = 1.0 if side == "BUY" else -1.0

        oi = self.oi.last.get(sym)
        if oi and st is not None:
            w = st.windows.get("1h")
            move = w.move_pct(st.last_price) if w is not None else 0.0
            ftxt = f"{oi['funding']:+.4f}%" if oi.get("funding") is not None else "n/a"
            lines.append(f"OI (Futures): 15m {oi['oi15']:+.2f}% | 1h {oi['oi60']:+.2f}% | "
                         f"funding {ftxt} | {self._oi_tag(oi['oi60'], move)}")

        for fr in ("1h", "4h", "1d"):
            tr = self._choch_state(sym, fr).get("trend")
            if tr:
                lines.append(f"Trend ({fr.upper()}): {tr.capitalize()}")
        lines.append(f"CHoCH context: {self._gate_line(sym, side, now)}")
        tr = self._choch_state(sym, "1w").get("trend")
        if tr:
            lines.append(f"HTF context: 1w: {tr.capitalize()}")

        if not FUT_ENABLED:
            return [of.clean_alert_text(x) for x in lines]

        fs = self.futures_states.get(sym)
        if fs is None:
            lines.append("Futures: no data")
            return [of.clean_alert_text(x) for x in lines]

        for leg in legs:
            w = fs.windows.get(leg)
            if w is None:
                continue
            lines.append(f"Futures CVD% ({leg}): {w.cvd_pct:+.2f}% | vol ${w.window_volume:,.0f} | "
                         f"trades {w.trade_count} | fill {w.fill(now) * 100:.0f}%")
            lines.append(f"Futures Flow ({leg}): INFLOW ${w.inflow:,.0f} | OUTFLOW ${w.outflow:,.0f} | "
                         f"NET ${w.delta:+,.0f}")
            lines.append(f"Futures Size ({leg}): whale ${w.whale_net * sgn:+,.0f} | "
                         f"mid ${w.mid_net * sgn:+,.0f} | retail ${w.retail_net * sgn:+,.0f} | "
                         f"avg trade ${w.avg_trade_size:,.0f} | source {w.flow_source(side)}")
        lines.append(f"Futures CVD% (15m): {fs.short.cvd_pct:+.2f}% | NET ${fs.short.delta:+,.0f}")
        lines.append(f"Futures OBI: now {fs.obi_now:+.4f} | spread {fs.spread_bps:.2f} bps | "
                     f"price {fs.last_price:.8g}")
        gok, gr = self._futures_gate(sym, side, legs)
        lines.append(f"Futures gate: {'ALIGNED' if gok else 'BLOCKED'} ({gr})")
        t_age = now - fs.last_trade_ts if fs.last_trade_ts > 0 else -1
        b_age = now - fs.last_book_ts if fs.last_book_ts > 0 else -1
        lines.append(f"Futures feed: trades {fs.trade_msgs} msgs (last {t_age:.1f}s) | "
                     f"book {fs.book_msgs} msgs (last {b_age:.1f}s)")
        return [of.clean_alert_text(x) for x in lines]

    def _session_extra_lines(self, now: float, sessions: List[str]) -> List[str]:
        if not FUT_ENABLED or not self.futures_states:
            return []
        live = [f for f in self.futures_states.values() if f.trade_msgs > 0]
        top = sorted(self.futures_states.items(),
                     key=lambda kv: abs(kv[1].short.cvd_pct), reverse=True)[:5]
        lines = [f"Futures feed live: {len(live)}/{len(self.futures_states)} symbols",
                 "Top 15m Futures CVD:"]
        for sym, fs in top:
            lines.append(f"  {sym}: CVD {fs.short.cvd_pct:+.2f}% | OBI {fs.obi_now:+.4f} | "
                         f"trades {fs.short.trade_count} | price {fs.last_price:.8g}")
        return [of.clean_alert_text(x) for x in lines]


def main() -> None:
    load_dotenv()
    cl._ensure_mp_start_method()
    of.setup_logging()
    rules = of.CONFIG["orderflow_signals"]["rules"]
    of.log.info("[BOOT] combined | rules=%s | rest=%s | FUT_ENABLED=%s FUT_REQUIRED=%s FUT_PAIR_ONLY=%s",
                ",".join(rules.keys()), of.CONFIG["rest_schedule"]["enabled"],
                FUT_ENABLED, FUT_REQUIRED, FUT_PAIR_ONLY)
    for rn, r in rules.items():
        of.log.info("[RULE] %s legs=%s gate=%s cooldown=%s fill=%s cvd=%s",
                    rn, r.get("legs"), r.get("trend_frame"),
                    r.get("cooldown_min"), r.get("min_window_fill"), r.get("cvd_pct"))
    code = 0
    try:
        code = asyncio.run(CombinedOrderflowEngine().run())
    except KeyboardInterrupt:
        of.log.info("[ENGINE] Interrupted (graceful stop)")
    sys.exit(code)


if __name__ == "__main__":
    main()