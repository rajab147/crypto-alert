import os
import time
import statistics
import requests
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز (اسٹریٹجی وہی)
# ============================================================
BASE_URL = "https://data-api.binance.vision"
WINDOW_HOURS = 12            # 12 گھنٹے کی رولنگ ونڈو (آخری مکمل گھنٹے تک)

# POC
POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.30

# Buy/Sell (Contrarian)
BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

# Risk
TARGET_PCT = 0.7
STOP_PCT = 0.6
MAX_HOLD_HOURS = 12          # اس کے بعد ٹریڈ بند کرنے کی ہدایت
FEE_ROUNDTRIP_PCT = 0.2      # صرف نوٹیفکیشن میں فیس کی یاد دہانی کے لیے

# ATR کے مطابق TP/SL (TARGET_PCT اور STOP_PCT اب کم از کم حد ہیں)
SL_ATR_MULT = 1.5            # SL = 1.5 × اوسط 1h رینج
TP_ATR_MULT = 1.5            # TP = 1.5 × اوسط 1h رینج
MAX_DRIFT_FRAC = 0.5         # اگر قیمت سگنل سے SL فاصلے کے 50% سے زیادہ ہل چکی ہو تو سگنل رد

# کوئنز
MAX_SYMBOLS = 200
THREADS_KLINES = 10
THREADS_AGG = 6
MAX_TRADE_PAGES = 80         # ایک کوئن کے لیے زیادہ سے زیادہ 80,000 ٹریڈز
MIN_TRADES = 500
MOVE_FILTER_PCT = 0.5        # مرحلہ 1 فلٹر

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    raise SystemExit(1)

session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0, "too_few": 0}


# ============================================================
# 🔧 Safe GET (retry + backoff)
# ============================================================
def safe_get(url, params=None, timeout=15, retries=4):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 418):
                stats["rate_limited"] += 1
                wait = int(r.headers.get("Retry-After", 2 ** attempt))
                time.sleep(min(wait, 30))
                continue
            if r.status_code != 200:
                time.sleep(1 + attempt)
                continue
            return r.json()
        except Exception:
            time.sleep(1 + attempt)
    stats["failed"] += 1
    return None


def utc_naive(ms):
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).replace(tzinfo=None)


def window_bounds():
    """آخری مکمل گھنٹے پر ختم ہونے والی 12 گھنٹے کی ونڈو (UTC)۔"""
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=WINDOW_HOURS)
    return start, end


# ============================================================
# 📋 کوئنز + tick size
# ============================================================
def get_all_markets():
    print("Binance مارکیٹس...")
    data = safe_get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=30)
    if not data or "symbols" not in data:
        return [], {}

    skip = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
            "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY", "USDTBIDR"}

    ticks = {}
    for s in data["symbols"]:
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        sym = s.get("symbol", "")
        if sym in skip:
            continue
        base = s.get("baseAsset", "")
        if base.endswith(("UP", "DOWN", "BULL", "BEAR")) and base not in ("JUP",):
            continue
        tick = None
        for f in s.get("filters", []):
            if f.get("filterType") == "PRICE_FILTER":
                tick = float(f["tickSize"])
        if tick and tick > 0:
            ticks[sym] = tick

    print(f"کل USDT مارکیٹس: {len(ticks)}")

    ticker = safe_get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=30)
    if not isinstance(ticker, list):
        return list(ticks)[:MAX_SYMBOLS], ticks

    volumes = []
    for t in ticker:
        sym = t.get("symbol")
        if sym in ticks:
            try:
                volumes.append((sym, float(t.get("quoteVolume", 0))))
            except (TypeError, ValueError):
                continue
    volumes.sort(key=lambda x: x[1], reverse=True)
    result = [s for s, _ in volumes[:MAX_SYMBOLS]]
    print(f"{len(result)} کوئنز منتخب")
    return result, ticks


# ============================================================
# ⚡ مرحلہ 1: Klines سے تیز فلٹر (صرف مکمل کینڈلز)
# ============================================================
def quick_scan(symbol, win_end):
    data = safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": "1h",
                            "endTime": int(win_end.timestamp() * 1000) - 1,
                            "limit": WINDOW_HOURS})
    if not isinstance(data, list) or len(data) < 6:
        return None
    for k in data[-6:]:
        o = float(k[1]); c = float(k[4])
        if o > 0 and abs(c - o) / o * 100 > MOVE_FILTER_PCT:
            return {"symbol": symbol}
    return None


# ============================================================
# 📥 aggTrades — پوری ونڈو، fromId سے (کوئی ٹریڈ مس/ڈبل نہیں)
# ============================================================
def load_aggtrades(symbol, win_start, win_end):
    start_ms = int(win_start.timestamp() * 1000)
    end_ms = int(win_end.timestamp() * 1000)

    trades = []
    params = {"symbol": symbol, "startTime": start_ms, "limit": 1000}
    for _ in range(MAX_TRADE_PAGES):
        data = safe_get(f"{BASE_URL}/api/v3/aggTrades", params=params)
        if not isinstance(data, list):
            return None
        if not data:
            return trades
        for t in data:
            if t["T"] >= end_ms:
                return trades
            trades.append(t)
        if len(data) < 1000:
            return trades
        params = {"symbol": symbol, "fromId": data[-1]["a"] + 1, "limit": 1000}

    stats["too_heavy"] += 1      # ونڈو پوری نہیں ملی — ادھورے ڈیٹا سے سگنل نہیں دیں گے
    return None


# ============================================================
# 🎯 Order Flow + Contrarian
# ============================================================
def analyze_symbol(symbol, tick_size, win_start, win_end):
    trades = load_aggtrades(symbol, win_start, win_end)
    if trades is None:
        return None
    if len(trades) < MIN_TRADES:
        stats["too_few"] += 1
        return None

    # ہر کوائن کے لیے والیوم اسکیل: درمیانی ٹریڈ ≈ 100 یونٹ
    med_q = statistics.median(float(t["q"]) for t in trades[:5000])
    vol_scale = 100.0 / med_q if med_q > 0 else 1.0

    config = FootprintEngineConfig(
        tick_size=tick_size,
        aggregation_type=AggregationType.TIME,
        aggregation_value="1h",
        value_area_percentage=0.7,
    )
    engine = FootprintEngine(config=config)

    for t in trades:
        try:
            engine.process_tick(
                timestamp=utc_naive(t["T"]),
                price=float(t["p"]),
                volume=max(1, int(round(float(t["q"]) * vol_scale))),
                is_bid_trade=not t["m"],
            )
        except Exception:
            continue

    # آخری بار بند کرنے کے لیے ونڈو کے بعد ایک علامتی ٹک (بار ڈیٹا میں شامل نہیں ہوگی)
    try:
        engine.process_tick(
            timestamp=(win_end + timedelta(seconds=1)).replace(tzinfo=None),
            price=float(trades[-1]["p"]), volume=1, is_bid_trade=True)
    except Exception:
        pass

    win_start_n = win_start.replace(tzinfo=None)
    win_end_n = win_end.replace(tzinfo=None)

    rows = []
    for bar in engine.get_all_completed_bars():
        if not (win_start_n <= bar.start_time < win_end_n):
            continue
        rows.append({
            "start_time": bar.start_time,
            "high": bar.high_price,
            "low": bar.low_price,
            "close": bar.close_price,
            "poc": bar.poc_price,
            "delta": bar.bar_delta,
            "bid_volume": bar.total_bar_bid_volume,
            "ask_volume": bar.total_bar_ask_volume,
        })

    if len(rows) < 6:
        return None

    df = pd.DataFrame(rows).sort_values("start_time")
    rng = (df["high"] - df["low"])
    df["poc_position"] = ((df["poc"] - df["low"]) / rng.where(rng > 0)).fillna(0.5)
    df["ratio"] = df["ask_volume"] / df["bid_volume"].replace(0, 1)

    df["of_bull"] = (df["delta"] > 0) & (df["poc_position"] > POC_BULL_MIN) & (df["ratio"] > 1.0)
    df["of_bear"] = (df["delta"] < 0) & (df["poc_position"] < POC_BEAR_MAX) & (df["ratio"] < 1.0)

    bull = int(df["of_bull"].sum())
    bear = int(df["of_bear"].sum())
    total = bull + bear
    if total < MIN_SIGNALS:
        return None

    bull_pct = bull / total * 100
    if bull_pct <= BUY_THRESHOLD:
        signal = "BUY"
    elif bull_pct >= SELL_THRESHOLD:
        signal = "SELL"
    else:
        return None

    atr_pct = float(((df["high"] - df["low"]) / df["close"]).mean() * 100)

    return {
        "symbol": symbol,
        "signal": signal,
        "atr_pct": atr_pct,
        "price": float(df.iloc[-1]["close"]),
        "bull_count": bull,
        "bear_count": bear,
        "bull_pct": bull_pct,
        "bars": len(df),
    }


# ============================================================
# 💰 TP / SL
# ============================================================
def trade_levels(signal, entry, atr_pct):
    tp_pct = max(TARGET_PCT, TP_ATR_MULT * atr_pct)
    sl_pct = max(STOP_PCT, SL_ATR_MULT * atr_pct)
    if signal == "BUY":
        tp = entry * (1 + tp_pct / 100)
        sl = entry * (1 - sl_pct / 100)
    else:
        tp = entry * (1 - tp_pct / 100)
        sl = entry * (1 + sl_pct / 100)
    return tp, sl, tp_pct, sl_pct


def apply_live_prices(signals):
    """انٹری کے لیے موجودہ قیمت لیں؛ جو سگنل پہلے ہی بہت ہل چکا ہو اسے رد کریں۔"""
    data = safe_get(f"{BASE_URL}/api/v3/ticker/price")
    if not isinstance(data, list):
        print("⚠️ live قیمتیں نہیں ملیں — پرانی close استعمال ہو رہی ہے")
        return signals
    live = {d["symbol"]: float(d["price"]) for d in data}

    fresh = []
    for s in signals:
        p = live.get(s["symbol"])
        if not p:
            fresh.append(s)
            continue
        _, _, _, sl_pct = trade_levels(s["signal"], s["price"], s["atr_pct"])
        drift = (p - s["price"]) / s["price"] * 100
        if abs(drift) >= MAX_DRIFT_FRAC * sl_pct:
            print(f"   {s['symbol']}: پرانا سگنل (قیمت {drift:+.2f}% ہل چکی) — رد")
            continue
        s["price"] = p
        fresh.append(s)
    return fresh


def fmt_price(p):
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"


# ============================================================
# 📲 Ntfy (4096 بائٹ حد کے لیے ٹکڑوں میں)
# ============================================================
def post_ntfy(title, body):
    try:
        r = requests.post(NFTY_URL, data=body.encode("utf-8"),
                          headers={"Title": title, "Priority": "high",
                                   "Tags": "rotating_light,moneybag"},
                          timeout=15)
        print(f"Ntfy: {r.status_code}")
    except Exception as e:
        print(f"Ntfy Error: {e}")


def send_nfty(signals):
    if not signals:
        return

    blocks = []
    for s in sorted(signals, key=lambda x: (x["signal"], x["symbol"])):
        sym = s["symbol"].replace("USDT", "")
        tp, sl, tp_pct, sl_pct = trade_levels(s["signal"], s["price"], s["atr_pct"])
        blocks.append(
            f"{s['signal']} {sym} | {fmt_price(s['price'])}\n"
            f"TP {fmt_price(tp)} ({tp_pct:.1f}%) | SL {fmt_price(sl)} ({sl_pct:.1f}%)\n"
            f"Bull% {s['bull_pct']:.0f} | {s['bull_count']}B/{s['bear_count']}S"
        )

    footer = f"\nMax hold {MAX_HOLD_HOURS}h | fees ~{FEE_ROUNDTRIP_PCT}% roundtrip"

    chunks, cur = [], ""
    for b in blocks:
        if len(cur.encode("utf-8")) + len(b.encode("utf-8")) > 3500:
            chunks.append(cur)
            cur = ""
        cur += b + "\n---\n"
    if cur:
        chunks.append(cur)

    for i, c in enumerate(chunks, 1):
        title = f"{len(signals)} Signals (1h)" + (f" [{i}/{len(chunks)}]" if len(chunks) > 1 else "")
        post_ntfy(title, c + (footer if i == len(chunks) else ""))


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    win_start, win_end = window_bounds()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | 1h | {WINDOW_HOURS}h ونڈو "
          f"{win_start:%m-%d %H:%M} → {win_end:%m-%d %H:%M} UTC")
    print("=" * 60)

    symbols, ticks = get_all_markets()
    if not symbols:
        print("مارکیٹس نہیں ملیں")
        return

    print(f"\nمرحلہ 1: {len(symbols)} کوئنز Klines فلٹر...")
    candidates = []
    with ThreadPoolExecutor(max_workers=THREADS_KLINES) as ex:
        futures = [ex.submit(quick_scan, s, win_end) for s in symbols]
        for i, f in enumerate(as_completed(futures), 1):
            if i % 100 == 0:
                print(f"   {i}/{len(symbols)}")
            try:
                r = f.result()
                if r:
                    candidates.append(r)
            except Exception as e:
                print(f"   scan error: {e}")
    print(f"{len(candidates)} کوئنز پاس")
    if not candidates:
        print("کوئی کوئن نہیں")
        return

    print(f"\nمرحلہ 2: {len(candidates)} کوئنز aggTrades...")
    signals = []
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(analyze_symbol, c["symbol"], ticks[c["symbol"]],
                             win_start, win_end): c["symbol"] for c in candidates}
        for i, f in enumerate(as_completed(futures), 1):
            if i % 10 == 0:
                print(f"   {i}/{len(candidates)}")
            try:
                r = f.result()
                if r:
                    signals.append(r)
                    print(f"   {r['symbol']}: {r['signal']} ({r['bull_pct']:.0f}%)")
            except Exception as e:
                print(f"   {futures[f]} error: {e}")

    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{len(symbols)} کوئنز | {len(signals)} سگنلز | {elapsed:.0f}s")
    print(f"اعداد: {stats}")

    signals = apply_live_prices(signals) if signals else signals

    if signals:
        send_nfty(signals)
    else:
        print("کوئی سگنل نہیں — اگلی بار")


if __name__ == "__main__":
    main()
            
