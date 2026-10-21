#!/usr/bin/env python3
"""
Binance Spot: 4h Footprint (Order Flow) + CVD Zero-Line touch.

- Timeframe : 4 ساعت (UTC-aligned buckets)
- Window    : config میں window_hours (default 8 => پچھلا مکمل 4h + موجودہ بنتا ہوا 4h)
- Data      : /api/v3/aggTrades (m=True => seller aggressor => BID volume,
                                  m=False => buyer aggressor => ASK volume)

Footprint میں معیاری آرڈر فلو لاجک:
  * ہر price level پر Bid x Ask volume، Delta، Total، Trades
  * Bar Delta، Bar CVD (ہر بار صفر سے شروع)، Delta Max / Min
  * POC، Value Area (70%) => VAH / VAL
  * Diagonal Imbalance (ask[P] vs bid[P-1]، bid[P] vs ask[P+1]) ratio 3:1 (300%)
  * Stacked Imbalances (لگاتار levels)
  * Unfinished Auction (high / low پر دونوں طرف volume)
  * CVD Zero-Line touch (ہر 4h بار کی حد میں)
"""
import argparse
import json
import logging
import time
from array import array
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import requests
import yaml

BASE_URL = "https://api.binance.com"
INTERVAL_MS = 4 * 60 * 60 * 1000
WEIGHT_SOFT_LIMIT = 4800  # Binance حد 6000/min ہے

session = requests.Session()
session.headers.update({"User-Agent": "Footprint-CVD-Zero/1.0"})


# --------------------------------------------------------------------------
# Helpers / API
# --------------------------------------------------------------------------
def utc_iso(ts_ms):
    return datetime.fromtimestamp(ts_ms / 1000, tz=timezone.utc).isoformat()


def fmt(d):
    return format(d, "f")


def _throttle_on_weight(response):
    used = response.headers.get("X-MBX-USED-WEIGHT-1M")
    try:
        if used and int(used) > WEIGHT_SOFT_LIMIT:
            time.sleep(60 - (time.time() % 60) + 1)
    except ValueError:
        pass


def api_get(endpoint, params=None):
    """صرف 418/429، 5xx، timeout اور connection errors پر retry."""
    for attempt in range(7):
        try:
            response = session.get(BASE_URL + endpoint, params=params, timeout=30)
        except (requests.Timeout, requests.ConnectionError):
            if attempt == 6:
                raise
            time.sleep(min(2 ** attempt, 30))
            continue

        _throttle_on_weight(response)

        if response.status_code in (418, 429):
            try:
                wait = int(response.headers.get("Retry-After", ""))
            except ValueError:
                wait = min(2 ** attempt, 60)
            time.sleep(max(wait, 1))
            continue

        if response.status_code >= 500:
            time.sleep(min(2 ** attempt, 30))
            continue

        response.raise_for_status()
        return response.json()

    raise RuntimeError("Binance API retries exhausted")


def load_config(filename):
    with open(filename, "r", encoding="utf-8") as f:
        config = yaml.safe_load(f)
    if not isinstance(config, dict):
        raise ValueError("Invalid YAML configuration")
    return config


def get_tick_size(symbol):
    info = api_get("/api/v3/exchangeInfo", {"symbol": symbol})
    for flt in info["symbols"][0]["filters"]:
        if flt["filterType"] == "PRICE_FILTER":
            return Decimal(flt["tickSize"])
    raise RuntimeError(f"{symbol}: tickSize نہیں ملا")


def iter_trades(symbol, start_ms, end_ms):
    """
    aggTrades کو وقت کی ترتیب میں stream کرتا ہے۔
    نوٹ: startTime+endTime ایک ساتھ دیں تو Binance صرف 1 گھنٹے تک کی اجازت دیتا ہے،
    اس لیے پہلا صفحہ startTime سے اور آگے fromId سے لیا جاتا ہے۔
    """
    params = {"symbol": symbol, "startTime": start_ms, "limit": 1000}
    last_id = None

    while True:
        page = api_get("/api/v3/aggTrades", params)
        if not page:
            return

        page_last = int(page[-1]["a"])
        if last_id is not None and page_last <= last_id:
            return  # آگے نہیں بڑھ رہا

        for trade in page:
            trade_id = int(trade["a"])
            if last_id is not None and trade_id <= last_id:
                continue
            if int(trade["T"]) > end_ms:
                return
            last_id = trade_id
            yield trade

        if len(page) < 1000:
            return

        params = {"symbol": symbol, "fromId": page_last + 1, "limit": 1000}
        time.sleep(0.05)


# --------------------------------------------------------------------------
# Bucket (ایک 4h footprint بار)
# --------------------------------------------------------------------------
class Bucket:
    def __init__(self, start_ms):
        self.start = start_ms
        self.levels = {}  # level_index -> [bid, ask, trades]
        self.times = array("q")  # CVD کے لیے compact (ts)
        self.deltas = array("d")  # CVD کے لیے compact (delta)
        self.open = self.high = self.low = self.close = None
        self.bid = 0.0
        self.ask = 0.0
        self.count = 0

    def add(self, ts, price, level_idx, qty, seller_aggressor):
        if self.open is None:
            self.open = self.high = self.low = price
        self.close = price
        if price > self.high:
            self.high = price
        if price < self.low:
            self.low = price

        lvl = self.levels.get(level_idx)
        if lvl is None:
            lvl = self.levels[level_idx] = [0.0, 0.0, 0]

        if seller_aggressor:  # m=True => سیلر نے bid کو hit کیا
            lvl[0] += qty
            self.bid += qty
            delta = -qty
        else:  # buyer نے ask کو lift کیا
            lvl[1] += qty
            self.ask += qty
            delta = qty
        lvl[2] += 1
        self.count += 1
        self.times.append(ts)
        self.deltas.append(delta)


# --------------------------------------------------------------------------
# CVD Zero-Line لاجک
# --------------------------------------------------------------------------
def cvd_zero_line(bucket, config):
    """
    CVD ہر 4h بار کی شروعات پر 0 ہوتا ہے۔
    Touch = |CVD| <= tolerance، یا CVD کا صفر عبور کرنا (sign change)۔
    tolerance = max(abs, pct * بار کا کل volume).
    require_departure=True ہو تو touch تب گنا جاتا ہے جب CVD پہلے tolerance سے
    باہر نکلا ہو اور پھر واپس آئے (ورنہ بار کے شروع میں ہی touch بن جاتا ہے)۔
    """
    tol_abs = float(config.get("zero_tolerance_abs", 0))
    tol_pct = float(config.get("zero_tolerance_pct_of_4h_volume", 0.01))
    require_departure = bool(config.get("require_departure", True))

    total = bucket.bid + bucket.ask
    tolerance = max(tol_abs, total * tol_pct)

    cvd = 0.0
    prev = 0.0
    departed = not require_departure
    in_zone = False
    touches = 0
    first_touch = None
    cvd_max = 0.0
    cvd_min = 0.0

    for ts, delta in zip(bucket.times, bucket.deltas):
        cvd += delta
        cvd_max = max(cvd_max, cvd)
        cvd_min = min(cvd_min, cvd)

        near = abs(cvd) <= tolerance
        crossed = prev * cvd < 0
        prev = cvd

        if not near:
            departed = True

        if departed and (near or crossed):
            if not in_zone:
                touches += 1
                if first_touch is None:
                    first_touch = ts
            in_zone = True
        else:
            in_zone = False

    return {
        "cvd_final": round(cvd, 8),
        "cvd_max": round(cvd_max, 8),
        "cvd_min": round(cvd_min, 8),
        "zero_tolerance_qty": round(tolerance, 8),
        "cvd_zero_line_touch": touches > 0,
        "zero_touch_count": touches,
        "first_zero_touch_utc": utc_iso(first_touch) if first_touch else None,
    }


# --------------------------------------------------------------------------
# Footprint لاجک (POC / Value Area / Imbalance / Stacked / Unfinished)
# --------------------------------------------------------------------------
def value_area(rows, poc_i, pct):
    """معیاری 70% Value Area: POC سے شروع، ہر قدم پر اوپر/نیچے کے 2-2 levels کا موازنہ."""
    total = sum(r["total"] for r in rows)
    target = total * pct
    low_i = high_i = poc_i
    acc = rows[poc_i]["total"]

    while acc < target and (low_i > 0 or high_i < len(rows) - 1):
        up = sum(r["total"] for r in rows[high_i + 1: high_i + 3])
        dn = sum(r["total"] for r in rows[max(low_i - 2, 0): low_i])
        can_up = high_i < len(rows) - 1
        can_dn = low_i > 0

        if can_up and (up >= dn or not can_dn):
            take = rows[high_i + 1: high_i + 3]
            high_i += len(take)
            acc += up
        elif can_dn:
            take = rows[max(low_i - 2, 0): low_i]
            low_i -= len(take)
            acc += dn
        else:
            break
    return low_i, high_i


def footprint_analysis(bucket, level_size, config):
    ratio = float(config.get("imbalance_ratio", 3.0))  # 300%
    min_vol = float(config.get("imbalance_min_volume", 0))
    stacked_min = int(config.get("stacked_min_levels", 3))
    va_pct = float(config.get("value_area_pct", 0.70))

    idxs = sorted(bucket.levels)  # کم سے زیادہ قیمت
    rows = []
    for idx in idxs:
        bid, ask, trades = bucket.levels[idx]
        rows.append({
            "idx": idx,
            "price": Decimal(idx) * level_size,
            "bid": bid,
            "ask": ask,
            "delta": ask - bid,
            "total": ask + bid,
            "trades": trades,
            "buy_imb": False,
            "sell_imb": False,
        })

    by_idx = {r["idx"]: r for r in rows}

    # Diagonal imbalance
    for r in rows:
        below = by_idx.get(r["idx"] - 1)  # ایک level نیچے کا bid
        above = by_idx.get(r["idx"] + 1)  # ایک level اوپر کا ask

        if below is not None:
            if r["ask"] >= min_vol and r["ask"] > 0 and r["ask"] >= ratio * below["bid"]:
                r["buy_imb"] = True
        if above is not None:
            if r["bid"] >= min_vol and r["bid"] > 0 and r["bid"] >= ratio * above["ask"]:
                r["sell_imb"] = True

    # Stacked imbalances (لگاتار levels)
    stacked = []
    for side, key in (("buy", "buy_imb"), ("sell", "sell_imb")):
        run = []
        for r in rows + [None]:
            if r is not None and r[key] and (not run or r["idx"] == run[-1]["idx"] + 1):
                run.append(r)
                continue
            if len(run) >= stacked_min:
                stacked.append({
                    "side": side,
                    "levels": len(run),
                    "from_price": fmt(run[0]["price"]),
                    "to_price": fmt(run[-1]["price"]),
                })
            run = [r] if (r is not None and r[key]) else []

    poc_i = max(range(len(rows)), key=lambda i: rows[i]["total"])
    va_lo, va_hi = value_area(rows, poc_i, va_pct)

    top, bottom = rows[-1], rows[0]
    out_rows = [{
        "price": fmt(r["price"]),
        "bid_vol": round(r["bid"], 8),
        "ask_vol": round(r["ask"], 8),
        "delta": round(r["delta"], 8),
        "total": round(r["total"], 8),
        "trades": r["trades"],
        "buy_imbalance": r["buy_imb"],
        "sell_imbalance": r["sell_imb"],
        "is_poc": i == poc_i,
        "in_value_area": va_lo <= i <= va_hi,
    } for i, r in enumerate(rows)]
    out_rows.reverse()  # اوپر سے نیچے (چارٹ کی طرح)

    return {
        "poc": fmt(rows[poc_i]["price"]),
        "vah": fmt(rows[va_hi]["price"]),
        "val": fmt(rows[va_lo]["price"]),
        "unfinished_high": top["bid"] > 0 and top["ask"] > 0,
        "unfinished_low": bottom["bid"] > 0 and bottom["ask"] > 0,
        "stacked_imbalances": stacked,
        "levels": out_rows,
    }


# --------------------------------------------------------------------------
# ایک symbol کا مکمل تجزیہ
# --------------------------------------------------------------------------
def analyze_symbol(symbol, start_ms, end_ms, config):
    tick = get_tick_size(symbol)
    per_symbol = config.get("ticks_per_level_by_symbol", {}) or {}
    ticks = int(per_symbol.get(symbol, config.get("ticks_per_level", 10)))
    level_size = tick * ticks

    buckets = {}
    for trade in iter_trades(symbol, start_ms, end_ms):
        ts = int(trade["T"])
        b_start = ts // INTERVAL_MS * INTERVAL_MS
        bucket = buckets.get(b_start)
        if bucket is None:
            bucket = buckets[b_start] = Bucket(b_start)

        p_str = trade["p"]
        level_idx = int(Decimal(p_str) // level_size)
        bucket.add(ts, float(p_str), level_idx, float(trade["q"]), bool(trade["m"]))

    if not buckets:
        return None

    out = []
    for b_start in sorted(buckets):
        bucket = buckets[b_start]
        fp = footprint_analysis(bucket, level_size, config)
        cvd = cvd_zero_line(bucket, config)
        total = bucket.bid + bucket.ask

        out.append({
            "start_utc": utc_iso(b_start),
            "end_utc": utc_iso(b_start + INTERVAL_MS),
            "complete": b_start + INTERVAL_MS <= end_ms,
            "open": bucket.open,
            "high": bucket.high,
            "low": bucket.low,
            "close": bucket.close,
            "trade_count": bucket.count,
            "bid_vol": round(bucket.bid, 8),
            "ask_vol": round(bucket.ask, 8),
            "delta": round(bucket.ask - bucket.bid, 8),
            "total_vol": round(total, 8),
            **cvd,
            **fp,
        })

    return {
        "symbol": symbol,
        "timeframe": "4h",
        "tick_size": fmt(tick),
        "price_level_size": fmt(level_size),
        "window_start_utc": utc_iso(start_ms),
        "window_end_utc": utc_iso(end_ms),
        "buckets": out,
    }


# --------------------------------------------------------------------------
# main
# --------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config", default="config.yaml")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    config = load_config(args.config)

    symbols = [s.upper() for s in config.get("symbols", ["BTCUSDT"])]
    window_hours = int(config.get("window_hours", 8))
    if window_hours % 4 != 0 or window_hours <= 0:
        raise ValueError("window_hours کو 4 کا multiple ہونا چاہیے")
    n_buckets = window_hours // 4

    end_ms = int(time.time() * 1000)
    # بار کی حد سے aligned: پہلا بار مکمل ہوتا ہے، آخری (موجودہ) جزوی
    start_ms = (end_ms // INTERVAL_MS) * INTERVAL_MS - (n_buckets - 1) * INTERVAL_MS

    workers = max(1, int(config.get("workers", 1)))
    logging.info("%d symbols | window %s -> %s", len(symbols), utc_iso(start_ms), utc_iso(end_ms))

    results = []
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(analyze_symbol, s, start_ms, end_ms, config): s for s in symbols}
        for future in as_completed(futures):
            symbol = futures[future]
            try:
                result = future.result()
                if result:
                    results.append(result)
                    for b in result["buckets"]:
                        logging.info(
                            "%s %s | delta=%.4f POC=%s VAH=%s VAL=%s | CVD zero touch=%s",
                            symbol, b["start_utc"], b["delta"], b["poc"], b["vah"], b["val"],
                            b["cvd_zero_line_touch"],
                        )
            except Exception as exc:
                logging.warning("%s failed: %s", symbol, exc)

    results.sort(key=lambda r: r["symbol"])
    output = Path(config.get("output_file", "footprint_cvd.json"))
    output.write_text(json.dumps({
        "generated_utc": utc_iso(end_ms),
        "timeframe": "4h",
        "window_hours": window_hours,
        "symbols": results,
    }, indent=2), encoding="utf-8")
    logging.info("Saved: %s", output)


if __name__ == "__main__":
    main()
