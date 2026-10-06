import os
import sys
import time
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from requests.adapters import HTTPAdapter

# ============================================================
# سیٹنگز
# ============================================================
BAR_HOURS = 4                  # 4h بار (UTC حدود: 00,04,08,12,16,20)
MIN_1M_CANDLES = 200           # 240 میں سے کم از کم (کم لیکوئڈ کوئنز میں منٹ غائب ہو سکتے ہیں)
NUM_BINS = 40                  # POC کے لیے قیمت کے bins

# رولز
POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.25
DELTA_MIN_USDT = 1_000_000     # 4h بار کا net taker delta (USDT میں) — اپنے حساب سے ٹیون کریں
RATIO_BULL_MIN = 1.1           # taker buy / taker sell
RATIO_BEAR_MAX = 0.9
EMA_TREND = 20

TARGET_PCT = 2.0
STOP_PCT = 1.0

MIN_VOLUME_USDT = 10_000_000   # 24h quote volume
MAX_SYMBOLS = 200
THREADS = 10

# بار بند ہونے کے بعد کتنے منٹ تک سگنل تازہ مانا جائے
ENFORCE_FRESH = True
MAX_BAR_AGE_MIN = 60

STABLES = {"USDC", "BUSD", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "AEUR",
           "USDP", "USTC", "PAXG", "XUSD", "USD1"}
LEVERAGED_SUFFIXES = ("UP", "DOWN", "BULL", "BEAR")

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    sys.exit(1)

BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
]

session = requests.Session()
session.mount("https://", HTTPAdapter(pool_connections=THREADS, pool_maxsize=THREADS * 2))


# ============================================================
# نیٹ ورک (retry + rate limit کا خیال)
# ============================================================
def get_json(url, params=None, retries=3, timeout=10):
    for attempt in range(retries):
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 200:
                return r.json()
            if r.status_code in (429, 418):
                wait = int(r.headers.get("Retry-After", 5))
                print(f"   rate limit ({r.status_code}) — {wait}s انتظار")
                time.sleep(min(wait, 60))
                continue
            time.sleep(1)
        except Exception:
            time.sleep(1)
    return None


def get_working_endpoint():
    for url in BINANCE_ENDPOINTS:
        try:
            if session.get(f"{url}/api/v3/ping", timeout=5).status_code == 200:
                return url
        except Exception:
            continue
    return None


# ============================================================
# کوئنز کی فہرست (آخری قیمت کے ساتھ)
# ============================================================
def get_top_symbols(base_url):
    print("USDT کوئنز...")
    data = get_json(f"{base_url}/api/v3/ticker/24hr", timeout=30)
    if not isinstance(data, list):
        return []

    all_syms = {t.get("symbol", "") for t in data}
    rows = []
    for t in data:
        sym = t.get("symbol", "")
        if not sym.endswith("USDT"):
            continue
        base = sym[:-4]
        if base in STABLES:
            continue
        # leveraged ٹوکن: صرف تب خارج جب اصل کوئن بھی موجود ہو (JUP, SUPER وغیرہ بچ جائیں)
        skip = False
        for suf in LEVERAGED_SUFFIXES:
            if base.endswith(suf) and (base[:-len(suf)] + "USDT") in all_syms:
                skip = True
                break
        if skip:
            continue
        try:
            vol = float(t["quoteVolume"])
            price = float(t["lastPrice"])
        except Exception:
            continue
        if vol >= MIN_VOLUME_USDT and price > 0:
            rows.append((sym, vol, price))

    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:MAX_SYMBOLS]
    print(f"{len(rows)} کوئنز")
    return [(s, p) for s, v, p in rows]


# ============================================================
# بار کی UTC حدود (آخری مکمل 4h بار)
# ============================================================
def last_closed_bar_window():
    now = datetime.now(timezone.utc)
    cur_start = now.replace(hour=(now.hour // BAR_HOURS) * BAR_HOURS,
                            minute=0, second=0, microsecond=0)
    bar_start = cur_start - timedelta(hours=BAR_HOURS)
    bar_end = cur_start
    return now, bar_start, bar_end


# ============================================================
# تجزیہ: 1m klines سے delta, ratio, POC
# ============================================================
def analyze_symbol(base_url, symbol, cur_price, bar_start, bar_end):
    start_ms = int(bar_start.timestamp() * 1000)
    end_ms = int(bar_end.timestamp() * 1000) - 1

    k1 = get_json(f"{base_url}/api/v3/klines", {
        "symbol": symbol, "interval": "1m",
        "startTime": start_ms, "endTime": end_ms,
        "limit": BAR_HOURS * 60,
    })
    if not isinstance(k1, list) or len(k1) < MIN_1M_CANDLES:
        return None

    arr = np.array([[float(k[2]), float(k[3]), float(k[4]), float(k[7]), float(k[10])]
                    for k in k1])
    high, low, close, qv, tbq = arr[:, 0], arr[:, 1], arr[:, 2], arr[:, 3], arr[:, 4]

    total_qv = qv.sum()
    buy_q = tbq.sum()
    sell_q = total_qv - buy_q
    if total_qv <= 0 or sell_q <= 0 or buy_q <= 0:
        return None

    delta = buy_q - sell_q                  # USDT میں
    ratio = buy_q / sell_q

    bar_high, bar_low = high.max(), low.min()
    rng = bar_high - bar_low
    if rng <= 0:
        return None

    # POC کا تخمینہ: ہر منٹ کی typical price کو quote volume کے وزن سے histogram میں ڈالیں
    typical = (high + low + close) / 3.0
    hist, edges = np.histogram(typical, bins=NUM_BINS, range=(bar_low, bar_high), weights=qv)
    idx = int(np.argmax(hist))
    poc_price = (edges[idx] + edges[idx + 1]) / 2.0
    poc_position = (poc_price - bar_low) / rng

    # EMA20 (صرف بند کینڈلز)
    k4 = get_json(f"{base_url}/api/v3/klines", {
        "symbol": symbol, "interval": f"{BAR_HOURS}h", "limit": EMA_TREND + 15,
    })
    if not isinstance(k4, list) or len(k4) < EMA_TREND + 2:
        return None
    now_ms = int(datetime.now(timezone.utc).timestamp() * 1000)
    closed = [k for k in k4 if int(k[6]) < now_ms]
    if len(closed) < EMA_TREND + 1:
        return None
    closes = pd.Series([float(k[4]) for k in closed])
    ema = closes.ewm(span=EMA_TREND, adjust=False).mean().iloc[-1]
    last_close = closes.iloc[-1]
    is_uptrend = last_close > ema

    signal = None
    if (poc_position > POC_BULL_MIN and delta > DELTA_MIN_USDT
            and ratio > RATIO_BULL_MIN and is_uptrend):
        signal = "BUY"
    elif (poc_position < POC_BEAR_MAX and delta < -DELTA_MIN_USDT
          and ratio < RATIO_BEAR_MAX and not is_uptrend):
        signal = "SELL"

    if signal is None:
        return None

    return {
        "symbol": symbol,
        "signal": signal,
        "price": cur_price,           # موجودہ قیمت
        "bar_close": float(close[-1]),
        "poc_position": poc_position,
        "delta": delta,
        "ratio": ratio,
    }


# ============================================================
# Nfty
# ============================================================
def send_nfty(signals, bar_start):
    buys = [s for s in signals if s["signal"] == "BUY"]
    sells = [s for s in signals if s["signal"] == "SELL"]
    label = bar_start.strftime("%d %b %H:%M UTC")

    lines = [f"{len(signals)} سگنلز | {BAR_HOURS}h بار: {label}\n"]

    if buys:
        lines.append(f"BUY ({len(buys)}):")
        for s in buys:
            e = s["price"]
            lines.append("-" * 15)
            lines.append(f"{s['symbol'].replace('USDT', '')} | ${e:,.6g}")
            lines.append(f"TP: ${e * (1 + TARGET_PCT / 100):,.6g} | SL: ${e * (1 - STOP_PCT / 100):,.6g}")
            lines.append(f"POC {s['poc_position']:.2f} | Δ {s['delta'] / 1e6:+.1f}M | R {s['ratio']:.2f}")

    if sells:
        lines.append(f"\nSELL ({len(sells)}):")
        for s in sells:
            e = s["price"]
            lines.append("-" * 15)
            lines.append(f"{s['symbol'].replace('USDT', '')} | ${e:,.6g}")
            lines.append(f"TP: ${e * (1 - TARGET_PCT / 100):,.6g} | SL: ${e * (1 + STOP_PCT / 100):,.6g}")
            lines.append(f"POC {s['poc_position']:.2f} | Δ {s['delta'] / 1e6:+.1f}M | R {s['ratio']:.2f}")

    try:
        r = session.post(
            NFTY_URL,
            data="\n".join(lines).encode("utf-8"),
            headers={
                "Title": f"{len(signals)} signals ({BAR_HOURS}h)",
                "Priority": "high",
                "Tags": "rotating_light,moneybag",
            },
            timeout=15,
        )
        print("Nfty بھیجا" if r.status_code == 200 else f"Nfty status {r.status_code}")
    except Exception as e:
        print(f"Nfty خرابی: {e}")


# ============================================================
# MAIN
# ============================================================
def main():
    t0 = time.time()
    now, bar_start, bar_end = last_closed_bar_window()
    age_min = (now - bar_end).total_seconds() / 60

    print("=" * 60)
    print(f"{now.strftime('%H:%M:%S')} UTC | بار: {bar_start.strftime('%m-%d %H:%M')} -> {bar_end.strftime('%H:%M')}")
    print(f"POC>{POC_BULL_MIN}/<{POC_BEAR_MAX} | Δ>{DELTA_MIN_USDT / 1e6:.1f}M USDT | R>{RATIO_BULL_MIN}/<{RATIO_BEAR_MAX}")
    print(f"بار بند ہوئے {age_min:.0f} منٹ ہوئے")
    print("=" * 60)

    if ENFORCE_FRESH and age_min > MAX_BAR_AGE_MIN:
        print(f"بار {MAX_BAR_AGE_MIN} منٹ سے پرانا ہے — سگنل رد۔ بار بند ہونے کے فوراً بعد چلائیں۔")
        return

    base_url = get_working_endpoint()
    if not base_url:
        print("کوئی endpoint کام نہیں کر رہا")
        return
    print(f"{base_url}\n")

    symbols = get_top_symbols(base_url)
    if not symbols:
        return

    signals = []
    done = 0
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(analyze_symbol, base_url, s, p, bar_start, bar_end): s
                   for s, p in symbols}
        for f in as_completed(futures):
            done += 1
            if done % 50 == 0:
                print(f"   {done}/{len(symbols)}")
            try:
                r = f.result(timeout=60)
            except Exception:
                continue
            if r:
                signals.append(r)
                print(f"   {r['symbol']}: {r['signal']} (POC {r['poc_position']:.2f}, "
                      f"Δ {r['delta'] / 1e6:+.1f}M, R {r['ratio']:.2f})")

    print(f"\n{len(symbols)} کوئنز | {len(signals)} سگنلز | {time.time() - t0:.0f}s")

    if signals:
        send_nfty(signals, bar_start)
    else:
        print("کوئی سگنل نہیں")


if __name__ == "__main__":
    main()
    
