#!/usr/bin/env python3
"""
orderflow_paper.py - LIVE order-flow PAPER trading on Binance Spot (no real orders).

WHAT IT DOES
  * At startup, one-time REST calls (api.binance.com ticker/24hr + exchangeInfo) select the
    TOP 50 USDT pairs by 24h quote volume, excluding stablecoins, leveraged tokens and
    pump/dump coins (|24h change| > 25%).
  * One combined LIVE WebSocket (wss://stream.binance.com:9443/stream) carries @aggTrade and
    @depth10@100ms for all symbols.
  * Per symbol it keeps a 60s rolling window: CVD%, OBI (top 10 levels), spread bps,
    window volume (USDT), trade count, recent 60s move %, last-update timestamp.
  * Every 3s it evaluates entry/exit rules and paper-trades ($2000 balance, $100 per
    trade, max 5 concurrent positions).
  * Alerts go ONLY to ntfy (max 1 entry alert/symbol/20min, max 15 alerts/day, then log only).

ENV VARS
  NTFY_URL    e.g. https://ntfy.sh/your-secret-topic  (if missing: ntfy disabled, trading continues)
  NTFY_TOKEN  optional bearer token for protected topics
  (a .env file is loaded automatically via python-dotenv)

FILES CREATED (current directory)
  paper_state.json   balance, open positions, alert counters (saved every 60s + on SIGINT)
  paper_trades.jsonl one closed trade per line
  orderflow.log      rotating log (10MB x 5 backups), also printed to console

CHANGING SYMBOL COUNT / THRESHOLDS
  Edit the CONFIG dict below (e.g. "max_symbols": 30, "cvd_entry_threshold": 30.0).

ANTI-CHASE
  Entry is skipped if price already moved >= max_recent_move_pct (1.5%) versus the oldest
  trade in the 60s window, during the 5s warm-up after (re)connect, or while the window is
  stale. Windows are wiped on every reconnect so stale data is never traded.

EXECUTION MODEL (paper)
  Entries fill at best ask, exits at best bid (conservative). Exit triggers (TP/SL) use that
  gross price PnL; recorded PnL is net of CONFIG["fee_bps_per_side"] (set 0 to disable fees).

RUN
  pip install websockets aiohttp python-dotenv
  export NTFY_URL="https://ntfy.sh/your-topic"
  python orderflow_paper.py
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
    "window_seconds": 60,
    "eval_interval_sec": 3,
    "cvd_entry_threshold": 25.0,
    "obi_entry_threshold": 0.20,
    "spread_max_bps": 5.0,
    "min_window_volume_usdt": 100_000,
    "min_trade_count": 30,
    "max_recent_move_pct": 1.5,
    "cvd_exit_threshold": -10.0,
    "obi_exit_threshold": -0.20,
    "take_profit_pct": 0.6,
    "stop_loss_pct": -0.35,
    "max_hold_minutes": 45,
    "starting_balance": 2000.0,
    "position_size_usdt": 100.0,
    "max_concurrent_positions": 5,
    "alert_cooldown_min": 20,
    "max_alerts_per_day": 15,
    "warmup_seconds": 5,
    # --- supporting settings (no magic numbers in the code below) ---
    "spread_exit_bps": 15.0,
    "depth_levels": 10,   # MUST be 5, 10 or 20 — Binance partial depth stream limit
    "stale_seconds": 10,
    "ws_recv_timeout_sec": 30,
    "reconnect_backoff_max_sec": 60,
    "reconnect_fail_alert_after": 3,
    "state_save_interval_sec": 60,
    "http_timeout_sec": 60,
    "http_retries": 3,
    "ntfy_timeout_sec": 10,
    "fee_bps_per_side": 10.0,
}

TICKER_URL =  "https://data-api.binance.vision/api/v3/ticker/24hr"
INFO_URL = "https://data-api.binance.vision/api/v3/exchangeInfo"
WS_BASE =  "wss://data-stream.binance.vision/stream"
STATE_FILE = "paper_state.json"
TRADES_FILE = "paper_trades.jsonl"
LOG_FILE = "orderflow.log"

STABLE_BASES = {
    "USDC", "BUSD", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "AEUR", "EURI", "USDP", "USDE",
    "USDS", "USDG", "USD1", "PYUSD", "RLUSD", "RWUSD", "BFUSD", "XUSD", "U", "TRY", "BIDR",
    "UST", "USTC", "USDD", "FRAX", "LUSD", "GUSD", "USDB", "CRVUSD", "GHO", "USDJ",
}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")
LEVERAGED_EXCEPTIONS = {"JUP"}

log = logging.getLogger("orderflow")


def setup_logging() -> None:
    """Configure console + rotating file logging (UTC timestamps)."""
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
    """Return the UTC date string (YYYY-MM-DD) for a timestamp (default now)."""
    return datetime.fromtimestamp(ts if ts is not None else time.time(), timezone.utc).strftime("%Y-%m-%d")


# ----------------------------------------------------------------------------------------
class SymbolSelector:
    """One-time REST selection of the top USDT symbols after strict filtering."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        """Store configuration."""
        self.cfg = cfg

    @staticmethod
    def is_excluded_base(base: str) -> bool:
        """Return True for stablecoin or leveraged-token base assets."""
        if base in STABLE_BASES or base.startswith("USD"):
            return True
        if base.endswith("USD") and len(base) > 3:
            return True
        if base not in LEVERAGED_EXCEPTIONS and base.endswith(LEVERAGED_SUFFIXES):
            return True
        return False

    async def _get(self, session: aiohttp.ClientSession, url: str) -> Any:
        """GET a JSON document with retries and simple backoff."""
        last: Exception | None = None
        for attempt in range(1, self.cfg["http_retries"] + 1):
            try:
                async with session.get(url) as resp:
                    resp.raise_for_status()
                    return await resp.json()
            except Exception as exc:  # noqa: BLE001
                last = exc
                log.warning("[ENGINE] REST %s failed (attempt %d): %s", url, attempt, exc)
                await asyncio.sleep(2 ** attempt)
        raise RuntimeError(f"REST request failed: {url}: {last}")

    def _check(self, t: dict[str, Any], trading: set[str]) -> dict[str, Any] | None:
        """Apply all filter criteria to one ticker; return a summary dict or None."""
        sym = str(t["symbol"])
        if not sym.endswith("USDT") or sym not in trading:
            return None
        base = sym[: -len("USDT")]
        if not base or self.is_excluded_base(base):
            return None
        qv, cnt, chg = float(t["quoteVolume"]), int(t["count"]), float(t["priceChangePercent"])
        if qv < self.cfg["min_quote_volume_24h"] or cnt < self.cfg["min_trades_24h"]:
            return None
        if abs(chg) > self.cfg["max_price_change_24h_pct"]:
            return None
        return {"symbol": sym, "quote_volume": qv, "count": cnt, "change_pct": chg}

    async def select(self, session: aiohttp.ClientSession) -> list[dict[str, Any]]:
        """Fetch ticker + exchangeInfo once and return the top symbols by quote volume."""
        tickers, info = await asyncio.gather(self._get(session, TICKER_URL), self._get(session, INFO_URL))
        trading = {s["symbol"] for s in info.get("symbols", []) if s.get("status") == "TRADING"}
        usdt = [t for t in tickers if str(t.get("symbol", "")).endswith("USDT")]
        picked: list[dict[str, Any]] = []
        for t in usdt:
            try:
                item = self._check(t, trading)
            except (KeyError, ValueError, TypeError):
                continue
            if item:
                picked.append(item)
        picked.sort(key=lambda x: x["quote_volume"], reverse=True)
        picked = picked[: self.cfg["max_symbols"]]
        log.info("[ENGINE] Selected %d symbols after filtering %d total USDT pairs", len(picked), len(usdt))
        for i, p in enumerate(picked, 1):
            log.info("[%s] #%d quoteVolume=$%s trades24h=%s change24h=%+.2f%%", p["symbol"], i,
                     f"{p['quote_volume']:,.0f}", f"{p['count']:,}", p["change_pct"])
        return picked


# ----------------------------------------------------------------------------------------
class OrderFlowState:
    """Rolling-window order flow metrics for one symbol."""

    def __init__(self, symbol: str, window: float, levels: int) -> None:
        """Create empty state."""
        self.symbol, self.window, self.levels = symbol, float(window), levels
        self.reset()

    def reset(self) -> None:
        """Wipe all window and book data (used on start and every reconnect)."""
        self.trades: deque[tuple[float, float, float, bool]] = deque()  # ts, price, quote, is_buy
        self.buy_vol = self.sell_vol = 0.0
        self.last_price = 0.0
        self.bid_vol = self.ask_vol = 0.0
        self.best_bid = self.best_ask = 0.0
        self.has_depth = False
        self.last_update = 0.0

    def add_trade(self, price: float, qty: float, buyer_is_maker: bool, now: float) -> None:
        """Record an aggTrade. buyer_is_maker=False means aggressive BUY."""
        quote = price * qty
        is_buy = not buyer_is_maker
        self.trades.append((now, price, quote, is_buy))
        if is_buy:
            self.buy_vol += quote
        else:
            self.sell_vol += quote
        self.last_price = price
        self.last_update = now
        self.evict(now)

    def update_depth(self, bids: list[list[str]], asks: list[list[str]], now: float) -> None:
        """Update top-N book aggregates from a partial depth snapshot."""
        bids, asks = bids[: self.levels], asks[: self.levels]
        bb, ba = float(bids[0][0]), float(asks[0][0])
        if bb <= 0 or ba < bb:
            raise ValueError("crossed or invalid book")
        self.bid_vol = sum(float(q) for _, q in bids)
        self.ask_vol = sum(float(q) for _, q in asks)
        self.best_bid, self.best_ask, self.has_depth, self.last_update = bb, ba, True, now

    def evict(self, now: float) -> None:
        """Drop trades older than the window and keep running sums consistent."""
        while self.trades and now - self.trades[0][0] > self.window:
            _, _, quote, is_buy = self.trades.popleft()
            if is_buy:
                self.buy_vol -= quote
            else:
                self.sell_vol -= quote
        if not self.trades:
            self.buy_vol = self.sell_vol = 0.0

    def is_stale(self, now: float, limit: float) -> bool:
        """True if no update arrived within `limit` seconds."""
        return self.last_update == 0.0 or now - self.last_update > limit

    @property
    def window_volume(self) -> float:
        """Sum of price*qty over the window (USDT)."""
        return max(self.buy_vol + self.sell_vol, 0.0)

    @property
    def trade_count(self) -> int:
        """Number of trades in the window."""
        return len(self.trades)

    @property
    def cvd_pct(self) -> float:
        """(buy - sell) / total * 100 over the window."""
        total = self.buy_vol + self.sell_vol
        return (self.buy_vol - self.sell_vol) / total * 100.0 if total > 0 else 0.0

    @property
    def obi(self) -> float:
        """(bid_vol - ask_vol) / (bid_vol + ask_vol) over top N levels, in [-1, 1]."""
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0

    @property
    def spread_bps(self) -> float:
        """Spread in basis points of mid price (inf if no book)."""
        if not self.has_depth:
            return float("inf")
        mid = (self.best_ask + self.best_bid) / 2.0
        return (self.best_ask - self.best_bid) / mid * 10_000.0

    @property
    def recent_move_pct(self) -> float:
        """abs(current price - oldest price in window) / oldest * 100.
        Returns 999.0 if the window is younger than 75% of window_seconds,
        which blocks entry — we need a full window to trust the move reading."""
        if not self.trades or self.trades[0][1] <= 0:
            return 999.0
        span = self.trades[-1][0] - self.trades[0][0]
        if span < self.window * 0.75:
            return 999.0
        return abs(self.last_price - self.trades[0][1]) / self.trades[0][1] * 100.0


# ----------------------------------------------------------------------------------------
class NtfyAlerter:
    """Sends alerts to ntfy; failures are logged and never raised."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        """Read NTFY_URL / NTFY_TOKEN from the environment."""
        self.cfg = cfg
        self.url = os.getenv("NTFY_URL", "").strip()
        self.token = os.getenv("NTFY_TOKEN", "").strip()
        self.session: aiohttp.ClientSession | None = None
        if not self.url:
            log.warning("[ENGINE] ntfy disabled (NTFY_URL not set)")

    async def start(self) -> None:
        """Create the shared HTTP session."""
        timeout = aiohttp.ClientTimeout(total=self.cfg["ntfy_timeout_sec"])
        self.session = aiohttp.ClientSession(timeout=timeout)

    async def close(self) -> None:
        """Close the HTTP session."""
        if self.session and not self.session.closed:
            await self.session.close()

    async def send(self, title: str, body: str, priority: str = "high", tags: str = "") -> bool:
        """POST one message to ntfy. Returns True on success."""
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
        except Exception as exc:  # noqa: BLE001
            log.warning("[ENGINE] ntfy send failed for '%s': %s", title, exc)
            return False


# ----------------------------------------------------------------------------------------
class PaperTrader:
    """Paper account: positions, balance, alert counters, persistence."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        """Initialise a fresh account (call load() to restore saved state)."""
        self.cfg = cfg
        self.balance = float(cfg["starting_balance"])  # realised balance (start + realised PnL)
        self.positions: dict[str, dict[str, float]] = {}
        self.day = utc_day()
        self.alerts_today = 0
        self.last_entry_alert: dict[str, float] = {}
        self.today_trades: list[dict[str, Any]] = []

    def load(self) -> None:
        """Load saved state from disk if present (blocking; call via to_thread)."""
        if not os.path.exists(STATE_FILE):
            log.info("[ENGINE] No saved state; starting with $%.2f", self.balance)
            return
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                d = json.load(f)
            self.balance = float(d["balance"])
            self.positions = {k: {kk: float(vv) for kk, vv in v.items()} for k, v in d.get("positions", {}).items()}
            self.day = str(d.get("day", self.day))
            self.alerts_today = int(d.get("alerts_today", 0))
            self.last_entry_alert = {k: float(v) for k, v in d.get("last_entry_alert", {}).items()}
            self.today_trades = list(d.get("today_trades", []))
            log.info("[ENGINE] Loaded state: balance=$%.2f open=%d day=%s", self.balance, len(self.positions), self.day)
        except Exception as exc:  # noqa: BLE001
            log.error("[ENGINE] Corrupt state file (%s); starting fresh", exc)

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-serialisable copy of the state."""
        return {"balance": self.balance, "positions": self.positions, "day": self.day,
                "alerts_today": self.alerts_today, "last_entry_alert": self.last_entry_alert,
                "today_trades": self.today_trades, "saved_at": datetime.now(timezone.utc).isoformat()}

    @staticmethod
    def _write_atomic(data: dict[str, Any]) -> None:
        """Write state atomically (blocking; run in a thread)."""
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2)
        os.replace(tmp, STATE_FILE)

    async def save(self) -> None:
        """Persist state without blocking the event loop."""
        try:
            await asyncio.to_thread(self._write_atomic, self.snapshot())
        except Exception as exc:  # noqa: BLE001
            log.error("[ENGINE] State save failed: %s", exc)

    @staticmethod
    def _append_line(line: str) -> None:
        """Append one line to the trades file (blocking; run in a thread)."""
        with open(TRADES_FILE, "a", encoding="utf-8") as f:
            f.write(line + "\n")

    @property
    def invested(self) -> float:
        """Total notional currently in open positions."""
        return sum(p["size"] for p in self.positions.values())

    def open_position(self, symbol: str, price: float, now: float) -> bool:
        """Open a fixed-size long. Returns False if limits/cash forbid it."""
        size = float(self.cfg["position_size_usdt"])
        if symbol in self.positions or len(self.positions) >= self.cfg["max_concurrent_positions"]:
            return False
        if self.balance - self.invested < size or price <= 0:
            return False
        self.positions[symbol] = {"entry_price": price, "entry_time": now, "size": size,
                                  "unrealized_pnl": 0.0}
        return True

    def update_unrealized(self, symbol: str, price: float) -> float:
        """Update and return unrealised PnL % for a position at `price`."""
        pos = self.positions[symbol]
        pct = (price / pos["entry_price"] - 1.0) * 100.0
        pos["unrealized_pnl"] = pos["size"] * pct / 100.0
        return pct

    def close_position(self, symbol: str, price: float, now: float, reason: str) -> dict[str, Any]:
        """Close a position at `price`; returns the trade record (net of fees)."""
        pos = self.positions.pop(symbol)
        ret = price / pos["entry_price"] - 1.0
        fee = self.cfg["fee_bps_per_side"] / 10_000.0
        pnl = pos["size"] * ret - pos["size"] * fee - pos["size"] * (1.0 + ret) * fee
        self.balance += pnl
        return {"timestamp": datetime.fromtimestamp(now, timezone.utc).isoformat(), "symbol": symbol,
                "side": "LONG", "entry_price": pos["entry_price"], "exit_price": price, "size": pos["size"],
                "pnl": round(pnl, 6), "pnl_pct": round(pnl / pos["size"] * 100.0, 4), "reason": reason,
                "held_seconds": round(now - pos["entry_time"], 1)}

    async def record_trade(self, rec: dict[str, Any]) -> None:
        """Append a closed trade to jsonl and today's list."""
        self.today_trades.append({"symbol": rec["symbol"], "pnl": rec["pnl"]})
        try:
            await asyncio.to_thread(self._append_line, json.dumps(rec))
        except Exception as exc:  # noqa: BLE001
            log.error("[%s] Failed to write trade: %s", rec["symbol"], exc)

    def alert_block_reason(self, kind: str, symbol: str, now: float) -> str | None:
        """Return why an alert must not be pushed, or None if allowed."""
        if self.alerts_today >= self.cfg["max_alerts_per_day"]:
            return f"daily cap {self.cfg['max_alerts_per_day']} reached (log only)"
        if kind == "entry":
            last = self.last_entry_alert.get(symbol, 0.0)
            if now - last < self.cfg["alert_cooldown_min"] * 60:
                return f"entry alert cooldown {self.cfg['alert_cooldown_min']}min active"
        return None

    def register_alert(self, kind: str, symbol: str, now: float) -> None:
        """Count a pushed alert."""
        self.alerts_today += 1
        if kind == "entry":
            self.last_entry_alert[symbol] = now

    def build_summary(self, label: str) -> str:
        """Build the daily summary text for `label`."""
        trades = self.today_trades
        n = len(trades)
        wins = sum(1 for t in trades if t["pnl"] > 0)
        total = sum(t["pnl"] for t in trades)
        per: dict[str, float] = {}
        for t in trades:
            per[t["symbol"]] = per.get(t["symbol"], 0.0) + t["pnl"]
        ranked = sorted(per.items(), key=lambda kv: kv[1], reverse=True)
        top = ", ".join(f"{s} {v:+.2f}" for s, v in ranked[:3]) or "n/a"
        worst = ", ".join(f"{s} {v:+.2f}" for s, v in sorted(ranked, key=lambda kv: kv[1])[:3]) or "n/a"
        return (f"Portfolio Summary {label}\nTotal trades: {n}\nWin rate: {wins / n * 100:.1f}%\n"
                if n else f"Portfolio Summary {label}\nTotal trades: 0\nWin rate: n/a\n") + (
            f"Total PnL: {total:+.2f} USDT\nTop 3: {top}\nWorst 3: {worst}\n"
            f"Balance: ${self.balance:.2f}\nOpen positions: {len(self.positions)}")

    def reset_day(self, new_day: str) -> None:
        """Reset daily counters for a new UTC day."""
        self.day, self.alerts_today, self.today_trades = new_day, 0, []
        self.last_entry_alert = {}


# ----------------------------------------------------------------------------------------
class Engine:
    """Wires WebSocket ingestion, signal evaluation, paper trading and alerts together."""

    def __init__(self) -> None:
        """Create components and shared state."""
        self.cfg = CONFIG
        self.trader = PaperTrader(self.cfg)
        self.alerter = NtfyAlerter(self.cfg)
        self.states: dict[str, OrderFlowState] = {}
        self.stop = asyncio.Event()
        self.connected = False
        self.stream_start = time.time()
        self._bg: set[asyncio.Task[Any]] = set()

    def _spawn(self, coro: Any) -> None:
        """Run a coroutine in the background and keep a reference."""
        task = asyncio.create_task(coro)
        self._bg.add(task)
        task.add_done_callback(self._bg.discard)

    def request_stop(self, why: str) -> None:
        """Signal the engine to shut down gracefully."""
        if not self.stop.is_set():
            log.info("[ENGINE] Stop requested (%s)", why)
            self.stop.set()

    async def _sleep_or_stop(self, seconds: float) -> bool:
        """Sleep up to `seconds`; return True if stop was requested meanwhile."""
        try:
            await asyncio.wait_for(self.stop.wait(), timeout=max(seconds, 0.0))
            return True
        except asyncio.TimeoutError:
            return False

    # ---- startup -------------------------------------------------------------------------
    async def run(self) -> int:
        """Main entry: select symbols, run all loops, shut down cleanly."""
        loop = asyncio.get_running_loop()
        for sig in (signal.SIGINT, signal.SIGTERM):
            try:
                loop.add_signal_handler(sig, self.request_stop, sig.name)
            except NotImplementedError:  # Windows
                signal.signal(sig, lambda s, f, n=sig.name: loop.call_soon_threadsafe(self.request_stop, n))
        await self.alerter.start()
        await asyncio.to_thread(self.trader.load)
        try:
            timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
            async with aiohttp.ClientSession(timeout=timeout) as session:
                picked = await SymbolSelector(self.cfg).select(session)
        except Exception as exc:  # noqa: BLE001
            log.error("[ENGINE] Symbol selection failed: %s", exc)
            await self.alerter.close()
            return 1
        symbols = [p["symbol"] for p in picked]
        for extra in self.trader.positions:  # keep streaming symbols with restored positions
            if extra not in symbols:
                symbols.append(extra)
        if not symbols:
            log.error("[ENGINE] No symbols passed the filters; exiting")
            await self.alerter.close()
            return 1
        for s in symbols:
            self.states[s] = OrderFlowState(s, self.cfg["window_seconds"], self.cfg["depth_levels"])
        if self.trader.day != utc_day():
            await self._rollover()
        lvl = int(self.cfg["depth_levels"])
        if lvl not in (5, 10, 20):
            raise ValueError(f"depth_levels must be 5, 10 or 20 (Binance limit), got {lvl}")
        url = WS_BASE + "?streams=" + "/".join(
            f"{s.lower()}@aggTrade/{s.lower()}@depth{lvl}@100ms" for s in symbols)
        log.info("[ENGINE] Starting with %d symbols, balance=$%.2f", len(symbols), self.trader.balance)
        tasks = [asyncio.create_task(c) for c in (self._ws_loop(url), self._eval_loop(),
                                                  self._save_loop(), self._daily_loop())]
        await self.stop.wait()
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        await self._shutdown()
        return 0

    async def _shutdown(self) -> None:
        """Close all paper positions, save state, send final summary."""
        now = time.time()
        for sym in list(self.trader.positions):
            try:
                await self._close(sym, now, "shutdown (SIGINT)", alert=False)
            except Exception as exc:  # noqa: BLE001
                log.error("[%s] Shutdown close failed: %s", sym, exc)
        await self.trader.save()
        summary = self.trader.build_summary(self.trader.day)
        log.info("[ENGINE] Final summary:\n%s", summary)
        await self.alerter.send(f"Final Portfolio Summary {self.trader.day}", summary, "default",
                                "chart_with_upwards_trend")
        if self._bg:
            await asyncio.wait(self._bg, timeout=self.cfg["ntfy_timeout_sec"])
        await self.alerter.close()
        log.info("[ENGINE] Shutdown complete")

    # ---- websocket -----------------------------------------------------------------------
    def _handle_message(self, raw: str | bytes) -> None:
        """Parse one combined-stream message and update the symbol state."""
        msg = json.loads(raw)
        stream, data = msg["stream"], msg["data"]
        now = time.time()
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

    async def _ws_loop(self, url: str) -> None:
        """Maintain the single combined WebSocket with exponential-backoff reconnects."""
        fails, alerted = 0, False
        while not self.stop.is_set():
            got_data = False
            try:
                log.info("[ENGINE] Connecting WebSocket (%d chars URL)", len(url))
                async with websockets.connect(url, ping_interval=20, ping_timeout=20,
                                              max_size=None, close_timeout=5) as ws:
                    for st in self.states.values():
                        st.reset()
                    self.stream_start = time.time()
                    while True:
                        raw = await asyncio.wait_for(ws.recv(), timeout=self.cfg["ws_recv_timeout_sec"])
                        if not got_data:
                            got_data, fails, alerted, self.connected = True, 0, False, True
                            log.info("[ENGINE] WebSocket connected, receiving data")
                        try:
                            self._handle_message(raw)
                        except Exception as exc:  # noqa: BLE001
                            log.warning("[ENGINE] Malformed message skipped: %s", exc)
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # noqa: BLE001
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

    # ---- evaluation ----------------------------------------------------------------------
    async def _eval_loop(self) -> None:
        """Evaluate every symbol each eval_interval_sec."""
        while not await self._sleep_or_stop(self.cfg["eval_interval_sec"]):
            if not self.connected:
                continue
            now = time.time()
            for sym, st in list(self.states.items()):
                try:
                    await self._evaluate(sym, st, now)
                except Exception as exc:  # noqa: BLE001
                    log.exception("[%s] Evaluation error: %s", sym, exc)

    def _exit_price(self, st: OrderFlowState) -> float:
        """Conservative exit price: best bid, falling back to last trade."""
        return st.best_bid if st.has_depth and st.best_bid > 0 else st.last_price

    def _exit_reason(self, sym: str, st: OrderFlowState, now: float, stale: bool) -> str | None:
        """Return the first triggered exit reason for an open position, else None."""
        c, pos = self.cfg, self.trader.positions[sym]
        held = now - pos["entry_time"]
        if not stale and self._exit_price(st) > 0:
            pnl = self.trader.update_unrealized(sym, self._exit_price(st))
            if pnl <= c["stop_loss_pct"]:
                return f"stop loss ({pnl:+.2f}%)"
            fees_pct = (2 * c["fee_bps_per_side"]) / 100.0     # round-trip fee in %
            net_tp = c["take_profit_pct"] + fees_pct
            if pnl >= net_tp:
                return f"take profit (gross {pnl:+.2f}% / net {pnl - fees_pct:+.2f}%)"
            if st.spread_bps > c["spread_exit_bps"]:
                return f"liquidity vanished (spread {st.spread_bps:.1f}bps)"
            if st.cvd_pct < c["cvd_exit_threshold"]:
                return f"CVD flipped ({st.cvd_pct:.1f}%)"
            if st.obi < c["obi_exit_threshold"]:
                return f"OBI flipped ({st.obi:+.2f})"
        if held > c["max_hold_minutes"] * 60:
            return f"max hold {c['max_hold_minutes']}min"
        return None

    async def _evaluate(self, sym: str, st: OrderFlowState, now: float) -> None:
        """Run exit checks for open positions or entry checks otherwise."""
        c = self.cfg
        st.evict(now)
        stale = st.is_stale(now, c["stale_seconds"])
        if sym in self.trader.positions:
            reason = self._exit_reason(sym, st, now, stale)
            if reason:
                await self._close(sym, now, reason)
            return
        if stale or not st.has_depth or st.last_price <= 0:
            return
        if now - self.stream_start < c["warmup_seconds"]:
            return
        if len(self.trader.positions) >= c["max_concurrent_positions"]:
            return
        if not (st.cvd_pct > c["cvd_entry_threshold"] and st.obi > c["obi_entry_threshold"]
                and st.spread_bps < c["spread_max_bps"]
                and st.window_volume > c["min_window_volume_usdt"]
                and st.trade_count >= c["min_trade_count"]
                and st.recent_move_pct < c["max_recent_move_pct"]):
            return
        price = st.best_ask
        if not self.trader.open_position(sym, price, now):
            return
        reason = (f"CVD {st.cvd_pct:+.1f}% > {c['cvd_entry_threshold']}, OBI {st.obi:+.2f}, "
                  f"move60s {st.recent_move_pct:.2f}%")
        log.info("[%s] ENTRY BUY @ %.8g | %s", sym, price, reason)
        await self._alert("entry", sym, "BUY", price, st, reason, now)

    async def _close(self, sym: str, now: float, reason: str, alert: bool = True) -> None:
        """Close a paper position at the conservative market price and alert."""
        st = self.states.get(sym)
        price = self._exit_price(st) if st else 0.0
        if price <= 0:
            price = self.trader.positions[sym]["entry_price"]
            log.warning("[%s] No market price; closing at entry price", sym)
        gross_pct = (price / self.trader.positions[sym]["entry_price"] - 1.0) * 100.0  # before pop
        rec = self.trader.close_position(sym, price, now, reason)
        await self.trader.record_trade(rec)
        log.info("[%s] EXIT SELL @ %.8g | gross=%+.3f%% net=%+.3f%% pnl=%+.4f USDT held=%.0fs | %s", sym,
                 price, gross_pct, rec["pnl_pct"], rec["pnl"], rec["held_seconds"], reason)
        if alert and st:
            extra = (f"Gross PnL: {gross_pct:+.3f}%\nNet PnL: {rec['pnl_pct']:+.3f}% ({rec['pnl']:+.4f} USDT)\n"
                     f"Held: {rec['held_seconds']:.0f}s")
            await self._alert("exit", sym, "SELL", price, st, reason, now, extra)

    async def _alert(self, kind: str, sym: str, side: str, price: float, st: OrderFlowState,
                     reason: str, now: float, extra: str = "") -> None:
        """Build the alert body, apply anti-spam rules, push via ntfy if allowed."""
        spread_str = f"{st.spread_bps:.2f}" if st.has_depth else "n/a (no book)"
        body = (f"Symbol: {sym}\nSide: {side}\nPrice: {price:.8g}\nCVD%: {st.cvd_pct:+.1f}\n"
                f"OBI: {st.obi:+.2f}\nSpread bps: {spread_str}\nVolume USDT (60s): {st.window_volume:,.0f}\n"
                f"Reason: {reason}\nCurrent balance: ${self.trader.balance:.2f}\n"
                f"Open positions: {len(self.trader.positions)}")
        if extra:
            body += "\n" + extra
        blocked = self.trader.alert_block_reason(kind, sym, now)
        if blocked:
            log.info("[%s] ntfy push skipped: %s | %s", sym, blocked, body.replace("\n", " | "))
            return
        self.trader.register_alert(kind, sym, now)
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._spawn(self.alerter.send(f"{sym} {side} Signal", body, "high", tags))

    # ---- housekeeping --------------------------------------------------------------------
    async def _save_loop(self) -> None:
        """Persist state periodically."""
        while not await self._sleep_or_stop(self.cfg["state_save_interval_sec"]):
            await self.trader.save()
            log.info("[ENGINE] State saved | balance=$%.2f open=%d alerts_today=%d connected=%s",
                     self.trader.balance, len(self.trader.positions), self.trader.alerts_today, self.connected)

    async def _rollover(self) -> None:
        """Send the daily summary for the finished day and reset daily counters."""
        label = self.trader.day
        summary = self.trader.build_summary(label)
        log.info("[ENGINE] Daily rollover:\n%s", summary)
        self._spawn(self.alerter.send(f"Portfolio Summary {label}", summary, "default",
                                      "chart_with_upwards_trend"))
        self.trader.reset_day(utc_day())
        await self.trader.save()

    async def _daily_loop(self) -> None:
        """Fire the summary and counter reset at every 00:00 UTC."""
        while True:
            now = datetime.now(timezone.utc)
            nxt = (now + timedelta(days=1)).replace(hour=0, minute=0, second=1, microsecond=0)
            if await self._sleep_or_stop((nxt - now).total_seconds()):
                return
            await self._rollover()


def main() -> None:
    """Program entry point."""
    load_dotenv()
    setup_logging()
    code = 0
    try:
        code = asyncio.run(_amain())
    except KeyboardInterrupt:
        log.info("[ENGINE] Interrupted")
    sys.exit(code)


async def _amain() -> int:
    """Create and run the engine inside the event loop."""
    return await Engine().run()


if __name__ == "__main__":
    main()
