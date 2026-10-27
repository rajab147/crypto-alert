#!/usr/bin/env python3
"""
orderflow_alerts_1h4h.py - LIVE order-flow BUY / SELL SIGNAL ALERTS on Binance Spot,
measured over 1-HOUR and 4-HOUR windows.

No trading of any kind: the script only watches the market and pushes ntfy alerts.

WHAT IT DOES
  * At startup, one-time REST calls (ticker/24hr + exchangeInfo) pick the top USDT pairs by
    24h quote volume, excluding stablecoins, leveraged tokens and pump/dump coins.
  * One combined WebSocket carries @aggTrade and @depth10@100ms for all symbols.
  * Per symbol and per timeframe (1h, 4h) it keeps a rolling window with: CVD%, OBI (top 10
    levels), window volume (USDT), trade count and price move. Spread is the live spread.
  * Every 3s each symbol is checked on both timeframes. BUY or SELL alerts go to ntfy
    (per symbol+timeframe+side cooldown, daily cap; beyond the cap it is logged only).

ORDER-FLOW LOGIC (unchanged, only the window length differs)
  CVD% = (buy - sell) / (buy + sell) * 100 over the window.
  OBI  = (bid_vol - ask_vol) / (bid_vol + ask_vol) over the top 10 book levels.
  BUY : CVD% > +cvd_threshold  AND OBI > +obi_threshold
  SELL: CVD% < -cvd_threshold  AND OBI < -obi_threshold
  plus: spread < spread_max_bps, window volume and trade count above the minimum, and
  |price move over the window| < max_recent_move_pct (anti-chase).

  OBI over a long window: a single book snapshot is meaningless for 1h/4h, so OBI is the
  AVERAGE of the book imbalance over the window (same formula per snapshot).
  Set "obi_window_average": False to use the instantaneous OBI instead.

  EARLY-ENTRY RULES (added so signals come near the START of a move, not after it):
    1. 15-minute trigger: besides the 1h/4h CVD (background filter), the CVD% of the last 15
       minutes must also agree (|cvd15| > short_cvd_threshold, same direction).
    2. Absorption / anti-chase: the price move over the last 15 minutes must be small
       (< short_max_move_pct) and the 1h/4h move limits are tighter (1% / 1.5%).
    3. Fresh cross: the CVD condition (1h/4h CVD + 15m CVD) must have turned true NO MORE than
       fresh_max_age_sec ago (default 15 min). A trend that has been "on" for a long time is
       never alerted late. After a restart the first observation counts as old.

DIVERGENCE SIGNALS (in addition to the signals above)
  Price and CVD are tracked in candles. 1h timeframe: divergence on 1-hour candles, checked at
  15-minute confirmation boundaries. 4h timeframe: divergence on 4-hour candles, checked at
  30-minute confirmation boundaries (see "div_candle_seconds" and "confirm_candle_seconds").
  At start, and after a long disconnect, the candles are filled from Binance
  klines (taker-buy volume gives the CVD), so divergences work immediately.
  A swing low/high is a candle whose low/high is beyond the previous candle and not beaten by
  the next one. The last two swing lows (or highs) are compared:
    Regular bullish: price lower low,  CVD higher low  -> BUY
    Hidden  bullish: price higher low, CVD lower low   -> BUY
    Regular bearish: price higher high, CVD lower high -> SELL
    Hidden  bearish: price lower high,  CVD higher high -> SELL
  There is NO candle-count rule: no minimum/maximum number of candles between the two swings
  (only the last "div_history_candles" candles are kept in memory).
  The 1h/4h candle that is still forming ("moment candle") is INCLUDED: no waiting for the
  1H/4H candle to close. Once a divergence exists it is confirmed; it is evaluated and sent ONLY
  at a 15-minute candle close (1h) or 30-minute candle close (4h), never in between, and once
  per swing. Swings older than the previous candle are ignored (no stale divergences).
  Divergence alerts use the same liquidity gates (volume, trade count, spread, stale data) and
  the same per-timeframe cooldown, but not the CVD%/OBI thresholds or the anti-chase limit.

STORAGE
  Trades are kept in 5-second buckets (running sums) so a 4h window stays cheap in memory.

WARM-UP
  The move check needs the window to be at least 75% full: ~45 min for 1h and ~3h for 4h
  after a fresh start (or after a long disconnect, see reconnect_wipe_after_sec).
  Short reconnects (e.g. Binance 24h limit) keep the windows; a gap longer than
  reconnect_wipe_after_sec wipes them so stale data is never traded.

ENV VARS
  NTFY_URL    e.g. https://ntfy.sh/your-secret-topic  (missing: alerts are only logged)
  NTFY_TOKEN  optional bearer token for protected topics
  (a .env file is loaded automatically)

FILES (current directory)
  alerts_state.json  alert counters and cooldowns (saved every 60s and on exit)
  orderflow.log      rotating log (10MB x 5), also printed to console

RUN
  pip install websockets aiohttp python-dotenv
  export NTFY_URL="https://ntfy.sh/your-topic"
  python orderflow_alerts_1h4h.py
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
    # --- order-flow thresholds (same logic as before; BUY uses +, SELL uses -) ---
    "cvd_threshold": 25.0,
    "obi_threshold": 0.20,
    "obi_window_average": False,    # False = instantaneous book OBI (original logic); True = window average
    "spread_max_bps": 5.0,
    "eval_interval_sec": 3,
    "warmup_seconds": 5,
    "bucket_seconds": 5,            # storage resolution of the rolling windows
    "min_window_fill": 0.75,        # window must be this full before the move check passes
    "diag_interval_min": 30,        # log "why no signal" diagnostics every N minutes (log only)
    "div_history_candles": 48,      # candles kept/backfilled per divergence series (memory only, not a rule)
    # --- early-entry rules for the CVD + OBI signal ---
    "short_window_seconds": 900,    # 15-minute trigger window
    "short_cvd_threshold": 20.0,    # |CVD% over last 15m| must exceed this, same direction as the signal
    "short_max_move_pct": 0.5,      # absorption: price may move at most this much in the last 15m
    "fresh_max_age_sec": 900,       # CVD condition must have turned true within this many seconds
    # --- timeframes: divergence candle size and entry-confirmation boundary ---
    "timeframes": {
        "1h": {
            "seconds": 3600,
            "cooldown_min": 60,
            "div_candle_seconds": [3600],       # Divergence on 1H candles only
            "confirm_candle_seconds": 900,      # Check at each 15m boundary
            "min_window_volume_usdt": 1_500_000,
            "min_trade_count": 1_000,
            "max_recent_move_pct": 1.0,
        },
        "4h": {
            "seconds": 14400,
            "cooldown_min": 240,
            "div_candle_seconds": [14400],      # Divergence on 4H candles only
            "confirm_candle_seconds": 1800,     # Check at each 30m boundary
            "min_window_volume_usdt": 5_000_000,
            "min_trade_count": 4_000,
            "max_recent_move_pct": 1.5,
        },
    },
    # --- alerts ---
    "max_alerts_per_day": 100,
    # --- supporting settings ---
    "depth_levels": 10,   # MUST be 5, 10 or 20 (Binance partial depth limit)
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

# Binance "market data only" hosts (public data, no keys). api.binance.com / stream.binance.com
# answer HTTP 451 from US datacenter IPs such as GitHub-hosted runners. Override via env if needed:
#   BINANCE_REST_BASE=https://api.binance.com   BINANCE_WS_BASE=wss://stream.binance.com:9443/stream
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
    """Human label for a candle size in seconds (900 -> '15m')."""
    return KLINE_INTERVALS.get(int(sec), f"{int(sec)}s")

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
    def is_excluded_base(base: str, trading_bases: set[str]) -> bool:
        """True for stablecoin or leveraged-token base assets.

        A UP/DOWN/BULL/BEAR suffix only counts as a leveraged token when the stem is itself
        a trading asset (BTCUP -> BTC), so coins like JUP or SYRUP are kept."""
        if base in STABLE_BASES or base.startswith("USD"):
            return True
        if base.endswith("USD") and len(base) > 3:
            return True
        for suf in LEVERAGED_SUFFIXES:
            if base.endswith(suf) and base[: -len(suf)] in trading_bases:
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

    def _check(self, t: dict[str, Any], trading: set[str], trading_bases: set[str]) -> dict[str, Any] | None:
        """Apply all filter criteria to one ticker; return a summary dict or None."""
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
        """Fetch ticker + exchangeInfo once and return the top symbols by quote volume."""
        tickers, info = await asyncio.gather(self._get(session, TICKER_URL), self._get(session, INFO_URL))
        symbols = [s for s in info.get("symbols", []) if s.get("status") == "TRADING"]
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
                picked.append(item)
        picked.sort(key=lambda x: x["quote_volume"], reverse=True)
        picked = picked[: self.cfg["max_symbols"]]
        log.info("[ENGINE] Selected %d symbols after filtering %d total USDT pairs", len(picked), len(usdt))
        for i, p in enumerate(picked, 1):
            log.info("[%s] #%d quoteVolume=$%s trades24h=%s change24h=%+.2f%%", p["symbol"], i,
                     f"{p['quote_volume']:,.0f}", f"{p['count']:,}", p["change_pct"])
        return picked


# ----------------------------------------------------------------------------------------
class RollingWindow:
    """Rolling window kept in fixed-size time buckets with running sums.

    Bucket layout: [bucket_id, buy_quote, sell_quote, trade_count, first_price, obi_sum, obi_n]
    """

    def __init__(self, seconds: float, bucket_sec: float) -> None:
        """Create an empty window of `seconds` length."""
        self.window, self.bucket = float(seconds), float(bucket_sec)
        self.reset()

    def reset(self) -> None:
        """Drop all data."""
        self.buckets: deque[list[float]] = deque()
        self.buy_vol = self.sell_vol = self.obi_sum = 0.0
        self.count = self.obi_n = 0

    def _current(self, now: float) -> list[float]:
        """Return the bucket for `now`, creating it if needed."""
        bid = int(now // self.bucket)
        if not self.buckets or self.buckets[-1][0] != bid:
            self.buckets.append([bid, 0.0, 0.0, 0, 0.0, 0.0, 0])
        return self.buckets[-1]

    def add_trade(self, price: float, quote: float, is_buy: bool, now: float) -> None:
        """Add one aggressive trade (quote = price * qty)."""
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
        """Add one order-book imbalance sample."""
        b = self._current(now)
        b[5] += obi
        b[6] += 1
        self.obi_sum += obi
        self.obi_n += 1

    def evict(self, now: float) -> None:
        """Drop buckets that are entirely older than the window."""
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
        """Sum of price*qty over the window (USDT)."""
        return max(self.buy_vol + self.sell_vol, 0.0)

    @property
    def trade_count(self) -> int:
        """Number of trades in the window."""
        return self.count

    @property
    def cvd_pct(self) -> float:
        """(buy - sell) / total * 100 over the window."""
        total = self.buy_vol + self.sell_vol
        return (self.buy_vol - self.sell_vol) / total * 100.0 if total > 0 else 0.0

    @property
    def avg_obi(self) -> float | None:
        """Average book imbalance over the window (None if no samples)."""
        return self.obi_sum / self.obi_n if self.obi_n > 0 else None

    def recent_move_pct(self, last_price: float, last_trade_ts: float, min_fill: float) -> float:
        """abs(current price - oldest price in window) / oldest * 100.
        Returns 999.0 until the window is at least `min_fill` full, which blocks signals."""
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


class CandleSeries:
    """Track price swings and their associated cumulative delta for divergence checks.

    Candle layout:
      [candle_id, high, low, close, cvd_at_close, cvd_at_high, cvd_at_low]

    Divergence is calculated from the latest TWO confirmed swing lows or swing highs.
    A swing is confirmed only after the following candle exists. The Engine decides when
    to call check(): every 15m boundary for the 1H series and every 30m boundary for 4H.
    """

    def __init__(self, candle_sec: float, history_candles: int) -> None:
        self.candle = float(candle_sec)
        self.max_candles = int(history_candles) + 2
        self.reset()

    def load_history(self, rows: list[list[Any]]) -> None:
        """Backfill candles from Binance klines; historical intrabar CVD is unavailable."""
        self.reset()
        cum = 0.0
        for r in rows:
            cum += 2.0 * float(r[10]) - float(r[7])
            high, low = float(r[2]), float(r[3])
            # Klines do not expose CVD at the exact high/low timestamp, so use candle-close
            # CVD for historical pivots. Live candles track CVD at the actual price extremes.
            self.candles.append([
                int(float(r[0]) // 1000 // self.candle), high, low, float(r[4]), cum, cum, cum
            ])
        self.cum = cum
        # Existing historical swings seed the detector; they must not trigger startup alerts.
        self.check(mark_only=True)

    def reset(self) -> None:
        self.candles: deque[list[float]] = deque(maxlen=self.max_candles)
        self.cum = 0.0
        self.seen = {"low": -1, "high": -1}

    def add_trade(self, price: float, signed_quote: float, now: float) -> None:
        """Update candle CVD and the CVD value at each new price extreme."""
        cid = int(now // self.candle)
        self.cum += signed_quote
        if not self.candles or self.candles[-1][0] != cid:
            self.candles.append([cid, price, price, price, self.cum, self.cum, self.cum])
            return

        c = self.candles[-1]
        if price > c[1]:
            c[1] = price
            c[5] = self.cum
        if price < c[2]:
            c[2] = price
            c[6] = self.cum
        c[3] = price
        c[4] = self.cum

    def check(self, mark_only: bool = False) -> list[tuple[str, str, float, float, float, float]]:
        """Return divergence only when a NEW confirmed swing is available.

        Compares exactly the latest two confirmed lows/highs. A forming candle cannot be a
        confirmed swing by itself. Missing candle IDs break a pivot sequence so non-adjacent
        candles are not incorrectly treated as neighbouring candles.
        """
        cs = list(self.candles)
        if len(cs) < 3:
            return []

        out: list[tuple[str, str, float, float, float, float]] = []
        for pivot in ("low", "high"):
            swings: list[tuple[float, float, float]] = []  # candle id, price extreme, CVD at extreme
            # i must have both a previous and a following candle; this confirms the pivot.
            for i in range(1, len(cs) - 1):
                prev, cur, nxt = cs[i - 1], cs[i], cs[i + 1]
                if cur[0] != prev[0] + 1 or nxt[0] != cur[0] + 1:
                    continue
                if pivot == "low":
                    if cur[2] < prev[2] and cur[2] <= nxt[2]:
                        swings.append((cur[0], cur[2], cur[6]))
                else:
                    if cur[1] > prev[1] and cur[1] >= nxt[1]:
                        swings.append((cur[0], cur[1], cur[5]))

            if not swings:
                continue
            newest_id = swings[-1][0]
            if newest_id <= self.seen[pivot]:
                continue

            # Mark the newest confirmed swing examined even if it does not form divergence.
            self.seen[pivot] = newest_id
            if len(swings) < 2 or mark_only:
                continue

            _, p1, v1 = swings[-2]
            _, p2, v2 = swings[-1]
            found: tuple[str, str] | None = None
            if pivot == "low":
                if p2 < p1 and v2 > v1:
                    found = ("BUY", "regular")       # lower price low, higher CVD low
                elif p2 > p1 and v2 < v1:
                    found = ("BUY", "hidden")        # higher price low, lower CVD low
            else:
                if p2 > p1 and v2 < v1:
                    found = ("SELL", "regular")      # higher price high, lower CVD high
                elif p2 < p1 and v2 > v1:
                    found = ("SELL", "hidden")       # lower price high, higher CVD high

            if found:
                out.append((found[0], found[1], p1, p2, v1, v2))
        return out


class OrderFlowState:
    """Order-flow metrics for one symbol across all timeframes."""

    def __init__(self, symbol: str, timeframes: dict[str, dict[str, Any]], bucket_sec: float, levels: int,
                 div_history: int = 48, short_sec: float = 900.0) -> None:
        """Create empty state with one rolling window per timeframe plus a short (15m) window."""
        self.symbol, self.levels = symbol, levels
        self.windows = {name: RollingWindow(tf["seconds"], bucket_sec) for name, tf in timeframes.items()}
        self.short = RollingWindow(short_sec, bucket_sec)
        self.candles = {name: {int(sec): CandleSeries(sec, div_history) for sec in tf["div_candle_seconds"]}
                        for name, tf in timeframes.items()}
        self.reset()

    def reset(self) -> None:
        """Wipe all window, book, and divergence confirmation state."""
        for w in self.windows.values():
            w.reset()
        self.short.reset()
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
        # Fresh-cross tracking per timeframe: current CVD-condition side and when it turned true.
        self.cside: dict[str, str | None] = {}
        self.cross_ts: dict[str, float] = {}

    def add_trade(self, price: float, qty: float, buyer_is_maker: bool, now: float) -> None:
        """Record an aggTrade. buyer_is_maker=False means aggressive BUY."""
        quote = price * qty
        is_buy = not buyer_is_maker
        for w in self.windows.values():
            w.add_trade(price, quote, is_buy, now)
        self.short.add_trade(price, quote, is_buy, now)
        signed = quote if is_buy else -quote
        for group in self.candles.values():
            for s in group.values():
                s.add_trade(price, signed, now)
        self.last_price = price
        self.last_trade_ts = now
        self.last_update = now

    def update_depth(self, bids: list[list[str]], asks: list[list[str]], now: float) -> None:
        """Update top-N book aggregates from a partial depth snapshot."""
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

    def evict(self, now: float) -> None:
        """Evict old data from every window."""
        for w in self.windows.values():
            w.evict(now)
        self.short.evict(now)

    def is_stale(self, now: float, limit: float) -> bool:
        """True if no update arrived within `limit` seconds."""
        return self.last_update == 0.0 or now - self.last_update > limit

    @property
    def obi_now(self) -> float:
        """(bid_vol - ask_vol) / (bid_vol + ask_vol) over top N levels, in [-1, 1]."""
        tot = self.bid_vol + self.ask_vol
        return (self.bid_vol - self.ask_vol) / tot if tot > 0 else 0.0

    def obi_for(self, name: str, use_average: bool) -> float:
        """OBI for a timeframe: window average (default) or the instantaneous value."""
        if use_average:
            avg = self.windows[name].avg_obi
            if avg is not None:
                return avg
        return self.obi_now

    @property
    def spread_bps(self) -> float:
        """Live spread in basis points of mid price (inf if no book)."""
        if not self.has_depth:
            return float("inf")
        mid = (self.best_ask + self.best_bid) / 2.0
        return (self.best_ask - self.best_bid) / mid * 10_000.0

    def recent_move_pct(self, name: str, min_fill: float) -> float:
        """Price move over the named window (999.0 until the window is full enough)."""
        return self.windows[name].recent_move_pct(self.last_price, self.last_trade_ts, min_fill)


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
            log.warning("[ENGINE] ntfy disabled (NTFY_URL not set); alerts are logged only")

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
class AlertBook:
    """Alert counters (daily cap, cooldowns) with JSON persistence."""

    def __init__(self, cfg: dict[str, Any]) -> None:
        """Start with empty counters."""
        self.cfg = cfg
        self.day = utc_day()
        self.alerts_today = 0
        self.last_alert: dict[str, float] = {}      # key: "SYMBOL|tf|SIDE"
        self.today_symbols: dict[str, int] = {}
        self.today_counts: dict[str, int] = {}      # key: "1h BUY"

    def load(self) -> None:
        """Load saved state if present (blocking; call via to_thread)."""
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
        except Exception as exc:  # noqa: BLE001
            log.error("[ENGINE] Corrupt state file (%s); starting fresh", exc)

    def snapshot(self) -> dict[str, Any]:
        """Return a JSON-serialisable copy of the state."""
        return {"day": self.day, "alerts_today": self.alerts_today, "last_alert": self.last_alert,
                "today_symbols": self.today_symbols, "today_counts": self.today_counts,
                "saved_at": datetime.now(timezone.utc).isoformat()}

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

    def on_cooldown(self, key: str, now: float, minutes: float) -> bool:
        """True if this key alerted within its cooldown window."""
        return now - self.last_alert.get(key, 0.0) < minutes * 60

    def cap_reached(self) -> bool:
        """True once the daily alert cap is used up."""
        return self.alerts_today >= self.cfg["max_alerts_per_day"]

    def register(self, key: str, symbol: str, tf: str, side: str, now: float, label: str | None = None) -> None:
        """Count a signal that was pushed."""
        self.alerts_today += 1
        self.last_alert[key] = now
        self.today_symbols[symbol] = self.today_symbols.get(symbol, 0) + 1
        label = label or f"{tf} {side}"
        self.today_counts[label] = self.today_counts.get(label, 0) + 1

    def build_summary(self, label: str) -> str:
        """Build the daily summary text."""
        ranked = sorted(self.today_symbols.items(), key=lambda kv: kv[1], reverse=True)
        top = ", ".join(f"{s} x{n}" for s, n in ranked[:5]) or "n/a"
        lines = [f"Signal Summary {label}", f"Alerts sent: {self.alerts_today}"]
        lines += [f"{k}: {v}" for k, v in sorted(self.today_counts.items())]
        lines.append(f"Most active: {top}")
        return "\n".join(lines)

    def reset_day(self, new_day: str) -> None:
        """Reset daily counters; keep cooldowns that are still younger than 24h."""
        self.day, self.alerts_today = new_day, 0
        self.today_symbols, self.today_counts = {}, {}
        cutoff = time.time() - 86400
        self.last_alert = {k: v for k, v in self.last_alert.items() if v > cutoff}


# ----------------------------------------------------------------------------------------
class Engine:
    """Wires WebSocket ingestion, signal evaluation and alerts together."""

    def __init__(self) -> None:
        """Create components and shared state."""
        self.cfg = CONFIG
        self.book = AlertBook(self.cfg)
        self.alerter = NtfyAlerter(self.cfg)
        self.states: dict[str, OrderFlowState] = {}
        self.stop = asyncio.Event()
        self.connected = False
        self.stream_start = time.time()
        self.last_msg_ts = 0.0
        self._wiped_since_msg = False            # windows already wiped/backfilled since the last message
        self.block_counts: dict[str, int] = {}   # diagnostics: which gate blocked how often
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
        lvl = int(self.cfg["depth_levels"])
        if lvl not in (5, 10, 20):
            raise ValueError(f"depth_levels must be 5, 10 or 20 (Binance limit), got {lvl}")
        await self.alerter.start()
        await asyncio.to_thread(self.book.load)
        try:
            timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
            async with aiohttp.ClientSession(timeout=timeout) as session:
                picked = await SymbolSelector(self.cfg).select(session)
        except Exception as exc:  # noqa: BLE001
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
        for s in symbols:
            self.states[s] = OrderFlowState(s, self.cfg["timeframes"], self.cfg["bucket_seconds"], lvl,
                                            int(self.cfg["div_history_candles"]),
                                            float(self.cfg["short_window_seconds"]))
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
        """Save state and let pending alerts finish."""
        await self.book.save()
        if self._bg:
            await asyncio.wait(self._bg, timeout=self.cfg["ntfy_timeout_sec"])
        await self.alerter.close()
        log.info("[ENGINE] Shutdown complete | alerts_today=%d", self.book.alerts_today)

    # ---- websocket -----------------------------------------------------------------------
    def _handle_message(self, raw: str | bytes) -> None:
        """Parse one combined-stream message and update the symbol state."""
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
        """Restart the warm-up timer for a freshly opened connection."""
        self.stream_start = time.time()

    async def _prepare_state(self) -> None:
        """Before (re)connecting: at start or after a long gap, wipe the windows and refill the
        candle series from klines. Short gaps keep hours of history untouched."""
        gap = time.time() - self.last_msg_ts if self.last_msg_ts else float("inf")
        if gap <= self.cfg["reconnect_wipe_after_sec"] or self._wiped_since_msg:
            return
        for st in self.states.values():
            st.reset()
        self._wiped_since_msg = True
        log.info("[ENGINE] Windows wiped (%s)", "start" if not self.last_msg_ts else f"gap {gap:.0f}s")
        try:
            await self._backfill_all()
        except Exception as exc:  # noqa: BLE001
            log.warning("[ENGINE] Candle backfill failed (%s); divergence history will build live", exc)

    async def _backfill_all(self) -> None:
        """Fill every divergence candle series from recent klines so divergences work immediately."""
        n = int(self.cfg["div_history_candles"]) + 1
        selector = SymbolSelector(self.cfg)
        sem = asyncio.Semaphore(8)
        loaded = 0
        timeout = aiohttp.ClientTimeout(total=self.cfg["http_timeout_sec"])
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async def one(sym: str, sec: int, series: CandleSeries) -> None:
                nonlocal loaded
                iv = KLINE_INTERVALS.get(sec)
                if iv is None:
                    log.warning("[%s] No kline interval for %ds candles; no backfill", sym, sec)
                    return
                async with sem:
                    try:
                        rows = await selector._get(
                            session, f"{REST_BASE}/api/v3/klines?symbol={sym}&interval={iv}&limit={n}")
                        series.load_history(rows)
                        loaded += 1
                    except Exception as exc:  # noqa: BLE001
                        log.warning("[%s] Kline backfill %s failed: %s", sym, iv, exc)
            await asyncio.gather(*(one(sym, sec, series) for sym, st in self.states.items()
                                   for group in st.candles.values() for sec, series in group.items()))
        total = sum(len(g) for st in self.states.values() for g in st.candles.values())
        log.info("[ENGINE] Candle history loaded for %d/%d series", loaded, total)

    async def _ws_loop(self, url: str) -> None:
        """Maintain the single combined WebSocket with exponential-backoff reconnects."""
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
        """Check every symbol each eval_interval_sec."""
        while not await self._sleep_or_stop(self.cfg["eval_interval_sec"]):
            if not self.connected:
                continue
            now = time.time()
            for sym, st in list(self.states.items()):
                try:
                    self._evaluate(sym, st, now)
                except Exception as exc:  # noqa: BLE001
                    log.exception("[%s] Evaluation error: %s", sym, exc)

    def _count(self, key: str) -> None:
        """Diagnostics: count one blocked/passed check."""
        self.block_counts[key] = self.block_counts.get(key, 0) + 1

    def _global_block(self, st: OrderFlowState, now: float) -> str | None:
        """Reason this symbol cannot signal on any timeframe right now, else None."""
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
        """Run the gates for one timeframe in the original order.

        Returns (reason, side, metrics); reason is 'ok' (and side set) when every gate passes."""
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
            return "short_move", None, m        # absorption rule: price already ran in the last 15m
        if cvd > c["cvd_threshold"] and scvd > c["short_cvd_threshold"] and obi > c["obi_threshold"]:
            return "ok", "BUY", m
        if cvd < -c["cvd_threshold"] and scvd < -c["short_cvd_threshold"] and obi < -c["obi_threshold"]:
            return "ok", "SELL", m
        return "cvd_obi", None, m

    def _cvd_side(self, m: dict[str, float]) -> str | None:
        """Side of the CVD condition alone (1h/4h CVD and 15m CVD agree), ignoring OBI and other gates."""
        c = self.cfg
        if m["cvd"] > c["cvd_threshold"] and m["scvd"] > c["short_cvd_threshold"]:
            return "BUY"
        if m["cvd"] < -c["cvd_threshold"] and m["scvd"] < -c["short_cvd_threshold"]:
            return "SELL"
        return None

    def _update_cross(self, st: OrderFlowState, name: str, m: dict[str, float], now: float) -> None:
        """Track when the CVD condition turned true. The first observation counts as OLD (ts = 0)."""
        side = self._cvd_side(m)
        prev = st.cside.get(name, "?")
        if prev == "?":
            st.cross_ts[name] = 0.0
        elif side != prev:
            st.cross_ts[name] = now if side else 0.0
        st.cside[name] = side

    def _evaluate(self, sym: str, st: OrderFlowState, now: float) -> None:
        """Check the BUY/SELL rules for one symbol on every timeframe."""
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
            age = now - st.cross_ts.get(name, 0.0)
            if age > c["fresh_max_age_sec"]:
                self._count(f"{name}:not_fresh")      # CVD condition has been on for too long: late signal
                continue
            key = f"{sym}|{name}|{side}"
            if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                self._count(f"{name}:cooldown")
                continue
            self._count(f"{name}:signal")
            self._alert(sym, name, side, st, m, age, now, key)
        for name, tf in c["timeframes"].items():
            self._check_divergence(sym, name, tf, st, now)

    def _check_divergence(
        self,
        sym: str,
        name: str,
        tf: dict[str, Any],
        st: OrderFlowState,
        now: float,
    ) -> None:
        """Divergence on the 1H (or 4H) candles, evaluated ONLY at a 15m (or 30m) candle close.

        Nothing is checked between boundaries. At each boundary the divergence series (which
        includes the still-forming 1H/4H candle) is examined once and any divergence found is
        alerted immediately, so detection, confirmation and entry check all happen at that close."""
        confirm_sec = int(tf.get("confirm_candle_seconds", 0))
        if confirm_sec <= 0:
            return

        # A new bucket means the prior 15m/30m candle has just closed. Ignore the first
        # observation after startup; it is not necessarily a close event observed by us.
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
            for side, kind, p1, p2, v1, v2 in series.check():
                # Keep divergence-candle size in the key for separate cooldowns.
                key = f"{sym}|{name}|{cl}|{side}|{kind}-div"
                if self.book.on_cooldown(key, now, tf["cooldown_min"]):
                    self._count(f"{name}/{cl}-div:cooldown")
                    continue
                self._count(f"{name}/{cl}-div:signal")
                self._alert_div(sym, name, cl, side, kind, (p1, p2, v1, v2), st, now, key)

    def _alert_div(self, sym: str, tf: str, cl: str, side: str, kind: str,
                   pts: tuple[float, float, float, float], st: OrderFlowState, now: float, key: str) -> None:
        """Build and push a divergence alert (logged only once the daily cap is reached)."""
        p1, p2, v1, v2 = pts
        w = st.windows[tf]
        confirm_label = candle_label(int(self.cfg["timeframes"][tf]["confirm_candle_seconds"]))
        since = (st.last_price - p2) / p2 * 100.0 if p2 > 0 else 0.0
        body = (f"Symbol: {sym}\nTimeframe: {tf} (divergence on {cl} candles)\n"
                f"Entry check: {confirm_label} candle close\nSide: {side}\n"
                f"Signal: {DIV_TEXT[(side, kind)]}\n"
                f"Price: {st.last_price:.8g}\nSwing price: {p1:.8g} -> {p2:.8g}\n"
                f"Move since swing: {since:+.2f}%\n"
                f"Swing CVD (USDT): {v1:+,.0f} -> {v2:+,.0f}\nCVD% ({tf}): {w.cvd_pct:+.1f}\n"
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
        self._spawn(self.alerter.send(f"{sym} {side} Divergence [{tf}]", body, "high", tags))

    async def _diag_loop(self) -> None:
        """Periodically log why symbols are not signalling. Log only; no effect on the rules."""
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

    def _alert(self, sym: str, tf: str, side: str, st: OrderFlowState, m: dict[str, float],
               age: float, now: float, key: str) -> None:
        """Build the alert, log it, and push via ntfy unless the daily cap is reached."""
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
        if self.book.cap_reached():
            log.info("[%s] %s %s SIGNAL (daily cap %d reached, log only) | %s", sym, tf, side,
                     c["max_alerts_per_day"], body.replace("\n", " | "))
            self.book.last_alert[key] = now  # keep the log quiet during the cooldown
            return
        self.book.register(key, sym, tf, side, now)
        log.info("[%s] %s %s SIGNAL | %s", sym, tf, side, body.replace("\n", " | "))
        tags = "green_circle" if side == "BUY" else "red_circle"
        self._spawn(self.alerter.send(f"{sym} {side} Signal [{tf}]", body, "high", tags))

    # ---- housekeeping --------------------------------------------------------------------
    async def _save_loop(self) -> None:
        """Persist state periodically."""
        while not await self._sleep_or_stop(self.cfg["state_save_interval_sec"]):
            await self.book.save()
            log.info("[ENGINE] State saved | alerts_today=%d connected=%s", self.book.alerts_today, self.connected)

    async def _rollover(self) -> None:
        """Send the daily summary for the finished day and reset daily counters."""
        label = self.book.day
        summary = self.book.build_summary(label)
        log.info("[ENGINE] Daily rollover:\n%s", summary)
        self._spawn(self.alerter.send(f"Signal Summary {label}", summary, "default", "chart_with_upwards_trend"))
        self.book.reset_day(utc_day())
        await self.book.save()

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
