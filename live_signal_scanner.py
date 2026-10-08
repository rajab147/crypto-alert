import os
import json
import time
import statistics
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز — سٹریٹجی (لاجک وہی، صرف SL/سگنل تعداد کنٹرول)
# ============================================================
BASE_URL = "https://data-api.binance.vision"
WINDOW_HOURS = 12

POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.30

BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

TARGET_PCT = 0.7
STOP_PCT = 0.6
MAX_HOLD_HOURS = 12
FEE_ROUNDTRIP_PCT = 0.2

SL_ATR_MULT = 1.0            # پہلے 1.5 تھا — SL چھوٹا
TP_ATR_MULT = 1.5
MAX_SL_PCT = 2.0             # اس سے چوڑے SL والا سگنل رد
MAX_DRIFT_FRAC = 0.5

MAX_SIGNALS_PER_SCAN = 5     # ہر سکین پر صرف بہترین 5
COOLDOWN_HOURS = 4           # ایک کوائن دوبارہ 4 گھنٹے تک نہیں
STATE_FILE = "signal_state.json"

MAX_SYMBOLS = 200
THREADS_KLINES = 10
THREADS_AGG = 6
MAX_TRADE_PAGES = 80
MIN_TRADES = 500
MOVE_FILTER_PCT = 0.5

# ============================================================
# ⚙️ سیٹنگز — بڑے کوائنز کا آرڈر فلو الرٹ (صرف قیمت)
# ============================================================
MAJORS = ["BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
          "DOGEUSDT", "ADAUSDT", "AVAXUSDT", "LINKUSDT", "TRXUSDT"]

OF_TF = {
    "1h": {"minutes": 60,  "min_delta_pct": 6.0},   # delta ≥ 6% of volume
    "4h": {"minutes": 240, "min_delta_pct": 5.0},
}
OF_POC_BULL = 0.60           # POC بار کے اوپری 40% میں
OF_POC_BEAR = 0.40           # POC بار کے نچلے 40% میں
OF_CLOSE_BULL = 0.65         # close رینج کے اوپری حصے میں
OF_CLOSE_BEAR = 0.35
OF_VOL_MULT = 1.0            # والیوم پچھلے 20 بارز کی اوسط سے کم نہ ہو

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    raise SystemExit(1)
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL   # چاہیں تو آرڈر فلو کا الگ ٹاپک

session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0, "too_few": 0, "wide_sl": 0}


# ============================================================
# 🔧 Safe GET
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
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(hours=WINDOW_HOURS)
    return start, end


def fmt_price(p):
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"


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
# 📲 Ntfy
# ============================================================
def post_ntfy(title, body, tags="rotating_light", url=None):
    # Title ہیڈر میں صرف ASCII رکھیں؛ ایموجی Tags سے آتے ہیں
    try:
        r = requests.post(url or NFTY_URL, data=body.encode("utf-8"),
                          headers={"Title": title, "Priority": "high", "Tags": tags},
                          timeout=15)
        print(f"Ntfy [{title}]: {r.status_code}")
    except Exception as e:
        print(f"Ntfy Error: {e}")


# ============================================================
# 🧭 حصہ A: بڑے کوائنز کا آرڈر فلو (1h اور 4h، بلش/بیئریش، صرف قیمت)
#    ڈیٹا: 1m + native klines (taker buy volume سے اصل delta)
#    POC: 1m کینڈلز کے والیوم پروفائل سے تخمینہ
# ============================================================
def klines(symbol, interval, end_ms, limit):
    return safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": interval,
                            "endTime": end_ms - 1, "limit": limit})


def approx_poc(m1_candles, lo, hi, tick):
    """1m کینڈلز کا والیوم ان کی [low, high] رینج میں برابر بانٹ کر POC نکالیں۔"""
    if hi <= lo:
        return lo
    step = max(tick, (hi - lo) / 60)
    n = int((hi - lo) / step) + 1
    prof = np.zeros(n)
    for k in m1_candles:
        l, h, v = float(k[3]), float(k[2]), float(k[5])
        i0 = min(max(int((l - lo) / step), 0), n - 1)
        i1 = min(max(int((h - lo) / step), 0), n - 1)
        if i1 < i0:
            i1 = i0
        prof[i0:i1 + 1] += v / (i1 - i0 + 1)
    return lo + (int(prof.argmax()) + 0.5) * step


def check_orderflow(symbol, tfs, end_ms, tick):
    m1 = klines(symbol, "1m", end_ms, 240)
    if not isinstance(m1, list) or not m1:
        return []
    out = []
    for tf in tfs:
        cfg = OF_TF[tf]
        mins = cfg["minutes"]
        start_ms = end_ms - mins * 60_000
        hist = klines(symbol, tf, end_ms, 21)
        if not isinstance(hist, list) or len(hist) < 3:
            continue
        bar = hist[-1]
        if int(bar[0]) != start_ms:
            continue
        prev = hist[:-1]
        avg_vol = statistics.mean(float(k[5]) for k in prev)

        h, l, c = float(bar[2]), float(bar[3]), float(bar[4])
        v, tb = float(bar[5]), float(bar[9])
        if v <= 0 or h <= l:
            continue
        delta_pct = (2 * tb - v) / v * 100
        ratio = tb / max(v - tb, 1e-12)
        close_pos = (c - l) / (h - l)

        sub = [k for k in m1 if int(k[0]) >= start_ms]
        if len(sub) < mins * 0.9:
            continue
        poc = approx_poc(sub, l, h, tick)
        poc_pos = (poc - l) / (h - l)
        vol_ok = v >= OF_VOL_MULT * avg_vol

        side = None
        if (delta_pct >= cfg["min_delta_pct"] and ratio > 1.0 and poc_pos >= OF_POC_BULL
                and close_pos >= OF_CLOSE_BULL and vol_ok):
            side = "BULL"
        elif (delta_pct <= -cfg["min_delta_pct"] and ratio < 1.0 and poc_pos <= OF_POC_BEAR
                and close_pos <= OF_CLOSE_BEAR and vol_ok):
            side = "BEAR"

        print(f"   OF {symbol} {tf}: delta {delta_pct:+.1f}% | poc {poc_pos:.2f} | "
              f"close {close_pos:.2f} | vol×{v / avg_vol:.2f} → {side or '-'}")
        if side:
            out.append((tf, side, c))
    return out


def run_orderflow(win_end, ticks):
    tfs = ["1h"]
    if win_end.hour % 4 == 0:        # 4h بار UTC 00,04,08,12,16,20 پر بند ہوتی ہے
        tfs.append("4h")
    end_ms = int(win_end.timestamp() * 1000)
    print(f"\nآرڈر فلو: {len(MAJORS)} بڑے کوائنز | ٹائم فریم {tfs}")

    def work(sym):
        try:
            return sym, check_orderflow(sym, tfs, end_ms, ticks.get(sym, 0.0))
        except Exception as e:
            print(f"   OF {sym} error: {e}")
            return sym, []

    alerts = {}
    with ThreadPoolExecutor(max_workers=4) as ex:
        for sym, res in ex.map(work, MAJORS):
            for tf, side, price in res:
                alerts.setdefault((tf, side), []).append((sym, price))

    if not alerts:
        print("آرڈر فلو: کوئی مضبوط الرٹ نہیں")
        return
    for (tf, side), items in sorted(alerts.items()):
        lines = [f"{s.replace('USDT', '')} {fmt_price(p)}" for s, p in items]
        name = "Bullish" if side == "BULL" else "Bearish"
        post_ntfy(f"{tf.upper()} {name} Order Flow", "\n".join(lines),
                  tags="green_circle" if side == "BULL" else "red_circle",
                  url=NFTY_OF_URL)


# ============================================================
# ⚡ حصہ B: سٹریٹجی — مرحلہ 1 فلٹر
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

    stats["too_heavy"] += 1
    return None


def analyze_symbol(symbol, tick_size, win_start, win_end):
    trades = load_aggtrades(symbol, win_start, win_end)
    if trades is None:
        return None
    if len(trades) < MIN_TRADES:
        stats["too_few"] += 1
        return None

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

    # SL بہت چوڑا ہو تو سگنل رد
    if max(STOP_PCT, SL_ATR_MULT * atr_pct) > MAX_SL_PCT:
        stats["wide_sl"] += 1
        return None

    score = min(total, 8) + abs(bull_pct - 50) / 50 * 5
    return {
        "symbol": symbol,
        "signal": signal,
        "atr_pct": atr_pct,
        "price": float(df.iloc[-1]["close"]),
        "bull_count": bull,
        "bear_count": bear,
        "bull_pct": bull_pct,
        "bars": len(df),
        "score": score,
    }


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


# ---------- cooldown (state فائل؛ محفوظ نہ ہو تو بھی کوڈ چلتا ہے) ----------
def load_state():
    try:
        with open(STATE_FILE) as f:
            return json.load(f)
    except Exception:
        return {}


def save_state(st):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(st, f)
    except Exception as e:
        print(f"state save error: {e}")


def in_cooldown(st, sym, now):
    t = st.get(sym)
    if not t:
        return False
    try:
        return now - datetime.fromisoformat(t) < timedelta(hours=COOLDOWN_HOURS)
    except Exception:
        return False


def send_signals(signals):
    """ہر سگنل الگ نوٹیفکیشن، صاف فارمیٹ میں۔"""
    for s in signals:
        sym = s["symbol"].replace("USDT", "")
        tp, sl, tp_pct, sl_pct = trade_levels(s["signal"], s["price"], s["atr_pct"])
        buy = s["signal"] == "BUY"
        sign_tp, sign_sl = ("+", "-") if buy else ("-", "+")
        body = (
            f"Entry: {fmt_price(s['price'])}\n"
            f"TP: {fmt_price(tp)} ({sign_tp}{tp_pct:.1f}%)\n"
            f"SL: {fmt_price(sl)} ({sign_sl}{sl_pct:.1f}%)\n"
            f"Bull {s['bull_pct']:.0f}% | {s['bull_count']}B/{s['bear_count']}S\n"
            f"Max hold {MAX_HOLD_HOURS}h | fees ~{FEE_ROUNDTRIP_PCT}%"
        )
        post_ntfy(f"{s['signal']} {sym} @ {fmt_price(s['price'])}", body,
                  tags="green_circle,moneybag" if buy else "red_circle,moneybag")
        time.sleep(0.3)


def run_strategy(symbols, ticks, win_start, win_end):
    symbols = [s for s in symbols if s not in MAJORS]   # بڑے کوائنز صرف آرڈر فلو الرٹ
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

    print(f"{len(signals)} سگنلز (فلٹر سے پہلے) | اعداد: {stats}")
    signals = apply_live_prices(signals) if signals else signals

    # cooldown + بہترین N
    now = datetime.now(timezone.utc)
    state = load_state()
    signals = [s for s in signals if not in_cooldown(state, s["symbol"], now)]
    signals.sort(key=lambda s: s["score"], reverse=True)
    signals = signals[:MAX_SIGNALS_PER_SCAN]

    if not signals:
        print("کوئی سگنل نہیں — اگلی بار")
        return
    send_signals(signals)
    for s in signals:
        state[s["symbol"]] = now.isoformat()
    save_state(state)


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    win_start, win_end = window_bounds()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | {WINDOW_HOURS}h ونڈو "
          f"{win_start:%m-%d %H:%M} → {win_end:%m-%d %H:%M} UTC")
    print("=" * 60)

    symbols, ticks = get_all_markets()
    if not symbols:
        print("مارکیٹس نہیں ملیں")
        return

    # 1) آرڈر فلو الرٹس پہلے (بار بند ہوتے ہی فوراً)
    try:
        run_orderflow(win_end, ticks)
    except Exception as e:
        print(f"آرڈر فلو error: {e}")

    # 2) باقی کوائنز کی سٹریٹجی
    run_strategy(symbols, ticks, win_start, win_end)
    print(f"\nمکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
    
