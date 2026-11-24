#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
ORDERFLOW + LuxAlgo CHoCH + OKX FUTURES COMBINED LIVE.
Spot: Binance Spot order-flow from orderflow_paper.py
CHoCH: LuxAlgo Fractal from choch_live.py
Futures: OKX USDT-M Swap trades + books5 + OI + funding
One runner combines spot flow, futures flow, OI/funding, and CHoCH trend gate.
No trading. Public data + alerts only.
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

# ============================ OKX FUTURES CONFIG ============================
OKX_WS = os.getenv("OKX_WS", "wss://ws.okx.com:8443/ws/v5/public").strip()
OKX_REST = os.getenv("OKX_REST", "https://www.okx.com/api/v5").rstrip("/")

FUT_ENABLED = os.getenv("FUT_ENABLED", "1") == "1"
# Default is observe mode. Set FUT_REQUIRED=1 only after confirming OKX data is stable.
FUT_REQUIRED = os.getenv("FUT_REQUIRED", "0") == "1"

FUT_DEPTH_LEVELS = int(os.getenv("FUT_DEPTH_LEVELS", "5"))
FUT_BATCH = int(os.getenv("FUT_BATCH", "60"))
FUT_STALE_SEC = float(os.getenv("FUT_STALE_SEC", "30"))

FUT_CVD_30M = float(os.getenv("FUT_CVD_30M", "3.0"))
FUT_CVD_1H = float(os.getenv("FUT_CVD_1H", "2.5"))
FUT_SHORT_CVD = float(os.getenv("FUT_SHORT_CVD", "2.0"))
FUT_OBI = float(os.getenv("FUT_OBI", "0.02"))

FUT_MIN_VOLUME_USDT = float(os.getenv("FUT_MIN_VOLUME_USDT", "20000"))
FUT_MIN_TRADES = int(os.getenv("FUT_MIN_TRADES", "30"))
FUT_MIN_FILL = float(os.getenv("FUT_MIN_FILL", "0.50"))


class FuturesState:
    """
    OKX USDT-M Futures order-flow state.
    Uses same RollingWindow logic as Binance spot engine.
    Volume is converted to USD using OKX contract value.
    """
    __slots__ = (
        "symbol", "inst_id", "ct_val", "levels",
        "windows", "short", "fast",
        "bid_vol", "ask_vol", "best_bid", "best_ask",
        "has_depth", "last_price", "last_update",
    )

    def __init__(
        self,
        symbol: str,
        inst_id: str,
        ct_val: float,
        timeframes: Dict[str, Dict[str, Any]],
        bucket_sec: float,
        short_sec: float,
        fast_sec: float,
        whale_mult: float,
        retail_mult: float,
        levels: int,
    ) -> None:
        self.symbol = symbol
        self.inst_id = inst_id
        self.ct_val = float(ct_val) if float(ct_val) > 0 else 1.0
        self.levels = int(levels)

        self.windows: Dict[str, Any] = {
            name: of.RollingWindow(
                float(tf["seconds"]),
                float(bucket_sec),
                float(whale_mult),
                float(retail_mult),
            )
            for name, tf in timeframes.items()
            if name in ("30m", "1h")
        }

        self.short = of.RollingWindow(
            float(short_sec),
            float(bucket_sec),
            float(whale_mult),
            float(retail_mult),
        )

        self.fast = of.RollingWindow(
            float(fast_sec),
            float(bucket_sec),
            float(whale_mult),
            float(retail_mult),
        )

        self.reset()

    def reset(self) -> None:
        for w in self.windows.values():
            w.reset()
        self.short.reset()
        self.fast.reset()

        self.bid_vol = 0.0
        self.ask_vol = 0.0
        self.best_bid = 0.0
        self.best_ask = 0.0
        self.has_depth = False
        self.last_price = 0.0
        self.last_update = 0.0

    def add_trade(self, price: float, size: float, side: str, now: float) -> None:
        price = float(price)
        size = float(size)
        quote = price * size * self.ct_val
        is_buy = str(side).lower() == "buy"

        for w in self.windows.values():
            w.add_trade(price, quote, is_buy, now)

        self.short.add_trade(price, quote, is_buy, now)
        self.fast.add_trade(price, quote, is_buy, now)

        self.last_price = price
        self.last_update = now

    def update_depth(
        self,
        bids: List[List[Any]],
        asks: List[List[Any]],
        now: float,
    ) -> None:
        if not bids or not asks:
            return

        bids = bids[: self.levels]
        asks = asks[: self.levels]

        if not bids or not asks:
            return

        bb = float(bids[0][0])
        ba = float(asks[0][0])

        if bb <= 0 or ba < bb:
            return

        self.bid_vol = sum(float(p) * float(q) * self.ct_val for p, q in bids)
        self.ask_vol = sum(float(p) * float(q) * self.ct_val for p, q in asks)

        self.best_bid = bb
        self.best_ask = ba
        self.has_depth = True
        self.last_update = now

        obi = self.obi_now
        for w in self.windows.values():
            w.add_obi(obi, now)

        self.short.add_obi(obi, now)
        self.fast.add_obi(obi, now)

    @property
    def obi_now(self) -> float:
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0


class OkxContextPoller:
    """
    OKX-only futures context:
    - Open Interest
    - Funding Rate
    """
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
                of.log.debug("[OKXCTX] poll error: %s", exc)
            await asyncio.sleep(60.0)

    async def _poll(self) -> None:
        if self.engine is None or not self.engine.states or self._session is None:
            return

        now = time.time()

        for sym in list(self.engine.states.keys()):
            if not sym.endswith("USDT"):
                continue

            base = sym[:-4]
            inst = f"{base}-USDT-SWAP"

            oi: Optional[float] = None
            funding: Optional[float] = None

            try:
                data = await cl._get_json(
                    self._session,
                    f"{OKX_REST}/public/open-interest?instId={inst}",
                    2,
                )
                if str(data.get("code", "0")) != "0":
                    of.log.debug("[OKXCTX] OKX OI API error for %s: %s", sym, data.get("msg"))
                    continue
                oi = float(data["data"][0]["oi"])
            except Exception as exc:  # noqa: BLE001
                of.log.debug("[OKXCTX] open-interest failed for %s: %s", sym, exc)
                continue

            try:
                fr = await cl._get_json(
                    self._session,
                    f"{OKX_REST}/public/funding-rate?instId={inst}",
                    2,
                )
                if str(fr.get("code", "0")) == "0":
                    funding = float(fr["data"][0]["fundingRate"]) * 100.0
            except Exception as exc:  # noqa: BLE001
                of.log.debug("[OKXCTX] funding failed for %s: %s", sym, exc)
                funding = None

            hist = self._hist.setdefault(sym, [])
            hist.append((now, oi))
            del hist[:-130]

            self.last[sym] = {
                "oi15": self._chg(sym, 900),
                "oi60": self._chg(sym, 3600),
                "src": "okx",
                "funding": funding,
            }

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


class CombinedOrderflowEngine(cl.ChochOrderflowEngine):
    def __init__(self) -> None:
        super().__init__()
        self.oi = OkxContextPoller()
        self.oi.engine = self

        self.futures_states: Dict[str, FuturesState] = {}
        self._fut_books: Dict[str, Dict[str, Dict[float, float]]] = {}
        self._fut_meta: Dict[str, Dict[str, float]] = {}
        self._fut_inst_to_sym: Dict[str, str] = {}
        self._fut_meta_loaded = False
        self._fut_task: Optional[asyncio.Task] = None
        self._fut_warned_empty = False
        self._fut_empty_warned = False
        self._fut_map_warned = False

    async def run(self) -> int:
        if FUT_ENABLED:
            self._fut_task = asyncio.create_task(self._okx_ws_loop())

        try:
            return await super().run()
        finally:
            if self._fut_task is not None:
                self._fut_task.cancel()
                try:
                    await self._fut_task
                except asyncio.CancelledError:
                    pass

    async def _okx_ws_loop(self) -> None:
        fails = 0

        while not self.stop.is_set():
            try:
                while not self.states and not self.stop.is_set():
                    await asyncio.sleep(1.0)

                await self._ensure_futures_states()

                inst_ids = list(self._fut_inst_to_sym.keys())
                if not inst_ids:
                    if not self._fut_warned_empty:
                        of.log.warning("[FUT] No OKX USDT-M swaps mapped for selected Binance symbols")
                        self._fut_warned_empty = True
                    if await self._sleep_or_stop(30.0):
                        break
                    continue

                self._fut_warned_empty = False
                of.log.info("[FUT] Connecting OKX Futures WebSocket for %d swaps", len(inst_ids))

                async with websockets.connect(
                    OKX_WS,
                    ping_interval=None,
                    max_size=None,
                    close_timeout=5,
                ) as ws:
                    fails = 0
                    ping_task = asyncio.create_task(self._okx_ping(ws))

                    try:
                        await self._okx_subscribe(ws, inst_ids)

                        async for raw in ws:
                            if self.stop.is_set():
                                break
                            try:
                                self._okx_handle(raw)
                            except Exception as exc:
                                of.log.debug("[FUT] Message handle error: %s", exc)

                    finally:
                        ping_task.cancel()
                        try:
                            await ping_task
                        except asyncio.CancelledError:
                            pass

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                fails += 1
                of.log.warning("[FUT] OKX WebSocket error #%d: %s: %s", fails, type(exc).__name__, exc)
                delay = min(2.0 ** (fails - 1), 60.0)
                if await self._sleep_or_stop(delay):
                    break

    async def _okx_ping(self, ws) -> None:
        while True:
            await asyncio.sleep(20)
            try:
                await ws.send("ping")
            except Exception:
                return

    async def _okx_load_meta(self) -> None:
        if self._fut_meta_loaded and self._fut_meta:
            return

        session = self._get_session()

        try:
            data = await cl._get_json(session, f"{OKX_REST}/public/instruments?instType=SWAP")
        except Exception as exc:  # noqa: BLE001
            of.log.warning("[FUT] OKX instruments fetch failed: %s", exc)
            return

        if str(data.get("code", "0")) != "0":
            of.log.warning("[FUT] OKX instruments API error: %s", data.get("msg"))
            return

        raw = data.get("data", [])
        of.log.info("[FUT] OKX instruments returned %d rows", len(raw))

        kept = 0
        skipped_not_swap = 0
        skipped_quote = 0
        skipped_state = 0
        skipped_ctval = 0

        for x in raw:
            inst = str(x.get("instId", ""))

            if not inst.endswith("-USDT-SWAP"):
                skipped_not_swap += 1
                continue

            qc = str(x.get("quoteCcy", ""))
            if qc and qc != "USDT":
                skipped_quote += 1
                continue

            state = str(x.get("state", "")).lower()
            if state and state != "live":
                skipped_state += 1
                continue

            try:
                ct_val = float(x.get("ctVal", "0"))
            except Exception:
                ct_val = 0.0

            if ct_val <= 0:
                skipped_ctval += 1
                continue

            self._fut_meta[inst] = {"ct_val": ct_val}
            kept += 1

        if kept == 0:
            of.log.warning(
                "[FUT] OKX instruments parsed but no USDT-M swaps kept | "
                "not_swap=%d quote=%d state=%d ctval=%d | will retry next cycle",
                skipped_not_swap, skipped_quote, skipped_state, skipped_ctval,
            )
            return

        self._fut_meta_loaded = True
        of.log.info("[FUT] Loaded OKX swap metadata for %d USDT-M instruments", kept)

    async def _ensure_futures_states(self) -> None:
        if self.futures_states:
            return

        await self._okx_load_meta()

        if not self._fut_meta:
            if not self._fut_empty_warned:
                of.log.warning("[FUT] OKX meta still empty; futures states not built")
                self._fut_empty_warned = True
            return

        tf_cfg = of.CONFIG["orderflow_signals"]["timeframes"]
        bucket = float(of.CONFIG["bucket_seconds"])
        short = float(of.CONFIG["short_window_seconds"])
        fast = float(of.CONFIG["fast_window_seconds"])
        whale_mult = float(of.CONFIG["whale_mult"])
        retail_mult = float(of.CONFIG["retail_mult"])

        mapped = 0
        unmapped_examples: List[str] = []

        for sym in self.states.keys():
            if not sym.endswith("USDT"):
                continue

            base = sym[:-4]
            inst = f"{base}-USDT-SWAP"

            meta = self._fut_meta.get(inst)
            if not meta:
                if len(unmapped_examples) < 10:
                    unmapped_examples.append(f"{sym}->{inst}")
                continue

            fs = FuturesState(
                symbol=sym,
                inst_id=inst,
                ct_val=float(meta["ct_val"]),
                timeframes=tf_cfg,
                bucket_sec=bucket,
                short_sec=short,
                fast_sec=fast,
                whale_mult=whale_mult,
                retail_mult=retail_mult,
                levels=FUT_DEPTH_LEVELS,
            )

            self.futures_states[sym] = fs
            self._fut_inst_to_sym[inst] = sym
            self._fut_books[inst] = {"bids": {}, "asks": {}}
            mapped += 1

        of.log.info(
            "[FUT] Prepared OKX futures order-flow state for %d symbols out of %d spot symbols",
            mapped,
            len(self.states),
        )

        if unmapped_examples:
            of.log.info(
                "[FUT] Sample unmapped spot symbols: %s",
                ", ".join(unmapped_examples),
            )

        if mapped == 0 and not self._fut_map_warned:
            of.log.warning("[FUT] OKX meta loaded but no Binance spot symbols mapped to OKX swaps")
            self._fut_map_warned = True

    async def _okx_subscribe(self, ws, inst_ids: List[str]) -> None:
        args: List[Dict[str, str]] = []

        for inst in inst_ids:
            args.append({"channel": "trades", "instId": inst})

        for inst in inst_ids:
            args.append({"channel": "books5", "instId": inst})

        for i in range(0, len(args), FUT_BATCH):
            batch = args[i:i + FUT_BATCH]
            await ws.send(json.dumps({"op": "subscribe", "args": batch}))
            await asyncio.sleep(0.10)

        of.log.info("[FUT] Subscribed OKX trades + books5 for %d swaps", len(inst_ids))

    def _okx_handle(self, raw: str | bytes) -> None:
        try:
            msg = json.loads(raw)
        except Exception:
            return

        if not isinstance(msg, dict):
            return

        if msg.get("event"):
            return

        arg = msg.get("arg", {})
        channel = str(arg.get("channel", ""))
        inst = str(arg.get("instId", ""))

        if not inst:
            return

        if channel == "trades":
            sym = self._fut_inst_to_sym.get(inst)
            fs = self.futures_states.get(sym) if sym else None
            if fs is None:
                return

            now = time.time()
            for d in msg.get("data", []):
                try:
                    px = float(d["px"])
                    sz = float(d["sz"])
                    side = str(d.get("side", "buy"))
                    fs.add_trade(px, sz, side, now)
                except Exception:
                    continue

        elif channel in ("books5", "books", "books-l2-tbt"):
            self._okx_book(inst, msg)

    def _okx_book(self, inst: str, msg: Dict[str, Any]) -> None:
        book = self._fut_books.get(inst)
        if book is None:
            return

        data = msg.get("data")
        if not data:
            return

        d = data[0]
        action = str(msg.get("action", "snapshot")).lower()

        if action == "snapshot":
            bids: Dict[float, float] = {}
            asks: Dict[float, float] = {}

            for row in d.get("bids", []) or []:
                if len(row) < 2:
                    continue
                p = float(row[0])
                s = float(row[1])
                if s > 0:
                    bids[p] = s

            for row in d.get("asks", []) or []:
                if len(row) < 2:
                    continue
                p = float(row[0])
                s = float(row[1])
                if s > 0:
                    asks[p] = s

            book["bids"] = bids
            book["asks"] = asks

        else:
            for row in d.get("bids", []) or []:
                if len(row) < 2:
                    continue
                p = float(row[0])
                s = float(row[1])
                if s == 0:
                    book["bids"].pop(p, None)
                else:
                    book["bids"][p] = s

            for row in d.get("asks", []) or []:
                if len(row) < 2:
                    continue
                p = float(row[0])
                s = float(row[1])
                if s == 0:
                    book["asks"].pop(p, None)
                else:
                    book["asks"][p] = s

        top_bids = sorted(book["bids"].items(), key=lambda kv: kv[0], reverse=True)[:FUT_DEPTH_LEVELS]
        top_asks = sorted(book["asks"].items(), key=lambda kv: kv[0])[:FUT_DEPTH_LEVELS]

        sym = self._fut_inst_to_sym.get(inst)
        fs = self.futures_states.get(sym) if sym else None
        if fs is None:
            return

        if top_bids and top_asks:
            fs.update_depth(
                [[p, s] for p, s in top_bids],
                [[p, s] for p, s in top_asks],
                time.time(),
            )

    def _futures_gate(self, sym: str, side: str) -> Tuple[bool, str]:
        if not FUT_ENABLED:
            return True, "disabled"

        fs = self.futures_states.get(sym)
        if fs is None:
            return False, "no_okx_swap"

        now = time.time()

        if fs.last_update <= 0 or now - fs.last_update > FUT_STALE_SEC:
            return False, "stale"

        if not fs.has_depth:
            return False, "no_book"

        w30 = fs.windows.get("30m")
        w1h = fs.windows.get("1h")

        if w30 is None or w1h is None:
            return False, "missing_windows"

        for w in (w30, w1h):
            if w.fill(now) < FUT_MIN_FILL:
                return False, "underfill"
            if w.window_volume < FUT_MIN_VOLUME_USDT:
                return False, "low_volume"
            if w.trade_count < FUT_MIN_TRADES:
                return False, "low_trades"

        cvd30 = w30.cvd_pct
        cvd1h = w1h.cvd_pct
        scvd = fs.short.cvd_pct
        obi = fs.obi_now

        if side == "BUY":
            if cvd30 < FUT_CVD_30M:
                return False, "weak_30m_cvd"
            if cvd1h < FUT_CVD_1H:
                return False, "weak_1h_cvd"
            if scvd < FUT_SHORT_CVD:
                return False, "weak_15m_cvd"
            if obi < FUT_OBI:
                return False, "weak_obi"
        else:
            if cvd30 > -FUT_CVD_30M:
                return False, "weak_30m_cvd"
            if cvd1h > -FUT_CVD_1H:
                return False, "weak_1h_cvd"
            if scvd > -FUT_SHORT_CVD:
                return False, "weak_15m_cvd"
            if obi > -FUT_OBI:
                return False, "weak_obi"

        return True, "aligned"

    def _futures_snapshot(self, sym: str, side: str) -> Optional[Dict[str, Any]]:
        fs = self.futures_states.get(sym)
        if fs is None:
            return None

        w30 = fs.windows.get("30m")
        w1h = fs.windows.get("1h")

        if w30 is None or w1h is None:
            return None

        sgn = 1.0 if side == "BUY" else -1.0

        return {
            "inst": fs.inst_id,
            "cvd30": w30.cvd_pct,
            "cvd1h": w1h.cvd_pct,
            "scvd": fs.short.cvd_pct,
            "obi": fs.obi_now,
            "vol30": w30.window_volume,
            "vol1h": w1h.window_volume,
            "trades30": w30.trade_count,
            "trades1h": w1h.trade_count,
            "whale30_signed": w30.whale_net * sgn,
            "mid30_signed": w30.mid_net * sgn,
            "retail30_signed": w30.retail_net * sgn,
            "source30": w30.flow_source(side),
            "last_price": fs.last_price,
            "age_sec": time.time() - fs.last_update if fs.last_update > 0 else 999999,
        }

    def _trend_allows(self, sym: str, rname: str, side: str, rule: Dict[str, Any]) -> Tuple[bool, str]:
        ok, reason = super()._trend_allows(sym, rname, side, rule)
        if not ok:
            return ok, reason

        if FUT_ENABLED and FUT_REQUIRED:
            ok2, reason2 = self._futures_gate(sym, side)
            if not ok2:
                return False, f"future_{reason2}"

        return True, ""

    def _signal_context_lines(self, sym: str, rname: str, side: str, now: float) -> List[str]:
        lines = super()._signal_context_lines(sym, rname, side, now)

        oi = self.oi.last.get(sym)
        if oi:
            st = self.states.get(sym)
            w = st.windows.get("1h") if st else None
            move = w.move_pct(st.last_price) if w is not None and st is not None else 0.0
            funding_txt = (
                f"{oi.get('funding'):+.4f}%"
                if oi.get("funding") is not None
                else "n/a"
            )
            new_oi_line = (
                f"OI (OKX): 15m {oi['oi15']:+.2f}% | 1h {oi['oi60']:+.2f}% | "
                f"funding {funding_txt} | {self._oi_tag(oi['oi60'], move)}"
            )

            replaced = False
            for i, line in enumerate(lines):
                if line.startswith("OI ("):
                    lines[i] = new_oi_line
                    replaced = True
                    break
            if not replaced:
                lines.insert(0, new_oi_line)

        if FUT_ENABLED:
            snap = self._futures_snapshot(sym, side)
            if snap:
                lines.append(
                    f"Futures OKX ({snap['inst']}): "
                    f"30m CVD {snap['cvd30']:+.2f}% | "
                    f"1h CVD {snap['cvd1h']:+.2f}% | "
                    f"15m CVD {snap['scvd']:+.2f}% | "
                    f"OBI {snap['obi']:+.4f}"
                )
                lines.append(
                    f"Futures volume: 30m ${snap['vol30']:,.0f} / {snap['trades30']} trades | "
                    f"1h ${snap['vol1h']:,.0f} / {snap['trades1h']} trades"
                )
                lines.append(
                    f"Futures size: whale {side} ${snap['whale30_signed']:+,.0f} | "
                    f"mid ${snap['mid30_signed']:+,.0f} | "
                    f"retail ${snap['retail30_signed']:+,.0f} | "
                    f"source {snap['source30']}"
                )
                gate_ok, gate_reason = self._futures_gate(sym, side)
                lines.append(
                    f"Futures gate: {'ALIGNED' if gate_ok else 'BLOCKED'} ({gate_reason})"
                )
            else:
                lines.append("Futures OKX: no mapped USDT-M swap data")

        return lines

    def _session_extra_lines(self, now: float, sessions: List[str]) -> List[str]:
        lines = super()._session_extra_lines(now, sessions)
        if not FUT_ENABLED:
            return lines

        top = sorted(
            self.futures_states.items(),
            key=lambda kv: abs(kv[1].short.cvd_pct),
            reverse=True,
        )[:5]

        if top:
            lines.append("Top 15m OKX Futures CVD:")
            for sym, fs in top:
                lines.append(
                    f"  {sym}: {fs.inst_id} CVD {fs.short.cvd_pct:+.2f}% | "
                    f"OBI {fs.obi_now:+.4f} | price {fs.last_price:.8g}"
                )
        return lines


def main() -> None:
    load_dotenv()
    cl._ensure_mp_start_method()
    of.setup_logging()
    of.log.info(
        "[BOOT] combined Binance Spot + OKX Futures order-flow | LuxAlgo CHoCH length=%d | FUT_REQUIRED=%s",
        cl.LUX_LENGTH,
        FUT_REQUIRED,
    )
    code = 0
    try:
        code = asyncio.run(CombinedOrderflowEngine().run())
    except KeyboardInterrupt:
        of.log.info("[ENGINE] Interrupted (graceful stop)")
    sys.exit(code)


if __name__ == "__main__":
    main()
