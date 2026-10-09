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
# ⚙️ سیٹنگز — حصہ B: TP/SL والی سٹریٹجی (1h بارز، 12h ونڈو)
#    نیا: سکین ہر 15 منٹ کی کینڈل بند ہونے پر (شیڈیولر ہر 15 منٹ پر چلائیں)
#    ونڈو کا اختتام = آخری بند 15m کینڈل؛ موجودہ بنتی ہوئی 1h بار بھی شامل
# ============================================================
BASE_URL = "https://data-api.binance.vision"
WINDOW_HOURS = 12
SCAN_MINUTES = 15

POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.30

BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

TARGET_PCT = 0.7
STOP_PCT = 0.6
MAX_HOLD_HOURS = 12
FEE_ROUNDTRIP_PCT = 0.2

SL_ATR_MULT = 2.0
TP_ATR_MULT = 2.5
MAX_SL_PCT = 4.0
MAX_DRIFT_FRAC = 0.5

MAX_SIGNALS_PER_SCAN = 5
COOLDOWN_HOURS = 4
STATE_FILE = "signal_state.json"

MAX_SYMBOLS = 200
THREADS_KLINES = 10
THREADS_AGG = 6
MAX_TRADE_PAGES = 80
MIN_TRADES = 500
MOVE_FILTER_PCT = 0.5

# جو کوائنز آرڈر فلو میں آ گئے وہ حصہ B سے نکال دیں (False کریں تو دونوں میں سکین ہوں گے)
B_EXCLUDE_OF_COINS = True

# ============================================================
# ⚙️ سیٹنگز — حصہ A: آرڈر فلو الرٹ (ٹاپ والیوم کوائنز، 1h اور 4h)
#    کینڈل کلوزنگ کی شرط نہیں: جاری بار میں سیٹ اپ بنتے ہی الرٹ
# ============================================================
OF_COIN_COUNT = 60
OF_TF = {
    "1h": {"minutes": 60,  "min_delta_pct": 6.0},
    "4h": {"minutes": 240, "min_delta_pct": 5.0},
}
OF_MIN_PROGRESS = 0.25      # بار کا کم از کم 25% وقت گزر چکا ہو (ورنہ جھوٹے الرٹ)
OF_BUCKETS = 40             # footprint تخمینے کے لیے قیمت کے خانے
OF_LOOKBACK = 12            # sweep / divergence کے لیے پچھلی بارز

OF_POC_BULL = 0.60
OF_POC_BEAR = 0.40
OF_CLOSE_BULL = 0.65
OF_CLOSE_BEAR = 0.35
OF_VOL_MULT = 1.0           # والیوم (وقت کے تناسب سے) اوسط سے کم نہ ہو

OF_ABS_DELTA = 4.0          # Absorption: |delta| ≥ 4% اور قیمت مخالف سمت میں بند
OF_ABS_VOL = 1.2
OF_STACK_RATIO = 3.0        # Stacked Imbalance: 3 گنا
OF_STACK_N = 3              # مسلسل 3 خانے
OF_STACK_MIN_DELTA = 3.0
OF_EXH_THIN = 0.05          # Exhaustion: انتہا کے 20% حصے میں کل والیوم ≤ 5%
OF_DIV_MIN = 0.5            # Divergence: CVD کا فرق اوسط |delta| کا کم از کم 50%
OF_STATE_FILE = "orderflow_state.json"

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    raise SystemExit(1)
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL

session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0, "too_few": 0, "wide_sl": 0}


# ============================================================
# 🔧 عمومی فنکشنز
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
    """آخری بند 15m کینڈل تک؛ 12 بارز (11 مکمل + موجودہ بنتی ہوئی، یا گھنٹے پر 12 مکمل)۔"""
    now = datetime.now(timezone.utc)
    end = now.replace(minute=(now.minute // SCAN_MINUTES) * SCAN_MINUTES,
                      second=0, microsecond=0)
    if end.minute == 0:
        start = end - timedelta(hours=WINDOW_HOURS)
    else:
        start = end.replace(minute=0) - timedelta(hours=WINDOW_HOURS - 1)
    return start, end


def fmt_price(p):
    if p >= 100: return f"{p:.2f}"
    if p >= 1: return f"{p:.4f}"
    if p >= 0.01: return f"{p:.5f}"
    return f"{p:.8f}"


def load_json(path):
    try:
        with open(path) as f:
            return json.load(f)
    except Exception:
        return {}


def save_json(path, obj):
    try:
        with open(path, "w") as f:
            json.dump(obj, f)
    except Exception as e:
        print(f"state save error ({path}): {e}")


def get_klines(symbol, interval, limit, end_ms=None):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if end_ms:
        params["endTime"] = end_ms
    return safe_get(f"{BASE_URL}/api/v3/klines", params=params)


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
    # Title ہیڈر میں صرف ASCII؛ ایموجی Tags سے آتے ہیں
    try:
        r = requests.post(url or NFTY_URL, data=body.encode("utf-8"),
                          headers={"Title": title, "Priority": "high", "Tags": tags},
                          timeout=15)
        print(f"Ntfy [{title}]: {r.status_code}")
        return r.ok
    except Exception as e:
        print(f"Ntfy Error: {e}")
        return False


# ============================================================
# 🧭 حصہ A: آرڈر فلو (جاری 1h اور 4h بار، کلوزنگ کی شرط کے بغیر)
#    ڈیٹا: 1m klines (taker buy volume سے اصل delta) + tf klines
#    footprint تخمینہ: ہر 1m کینڈل کا buy/sell والیوم اس کی [low, high] میں برابر بانٹا جاتا ہے
#    سیٹ اپس: Delta+POC، Absorption، Sweep، Exhaustion، Divergence (CVD)، Stacked Imbalance
# ============================================================
def build_profile(sub, lo, hi, nb):
    step = (hi - lo) / nb
    buy = np.zeros(nb)
    sell = np.zeros(nb)
    for k in sub:
        l, h, v, tb = float(k[3]), float(k[2]), float(k[5]), float(k[9])
        i0 = min(max(int((l - lo) / step), 0), nb - 1)
        i1 = min(max(int((h - lo) / step), 0), nb - 1)
        if i1 < i0:
            i1 = i0
        n = i1 - i0 + 1
        buy[i0:i1 + 1] += tb / n
        sell[i0:i1 + 1] += (v - tb) / n
    return buy, sell


def has_run(mask, n):
    run = 0
    for m in mask:
        run = run + 1 if m else 0
        if run >= n:
            return True
    return False


def eval_setups(hist, sub, tf, progress):
    """جاری بار (hist[-1]) پر تمام سیٹ اپس چیک کریں۔ واپسی: {"BULL": [...], "BEAR": [...]}"""
    cfg = OF_TF[tf]
    bar, prev = hist[-1], hist[:-1]
    o, h, l, c = float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4])
    v, tb = float(bar[5]), float(bar[9])
    if v <= 0 or h <= l:
        return None

    avg_vol = statistics.mean(float(k[5]) for k in prev)
    vol_ratio = v / (avg_vol * progress) if avg_vol > 0 else 0.0   # وقت کے تناسب سے
    delta_pct = (2 * tb - v) / v * 100
    ratio = tb / max(v - tb, 1e-12)
    close_pos = (c - l) / (h - l)

    buy, sell = build_profile(sub, l, h, OF_BUCKETS)
    tot = buy + sell
    if tot.sum() <= 0:
        return None
    poc_pos = (int(tot.argmax()) + 0.5) / OF_BUCKETS
    vol_ok = vol_ratio >= OF_VOL_MULT

    look = prev[-OF_LOOKBACK:]
    ph = max(float(k[2]) for k in look)
    pl = min(float(k[3]) for k in look)
    found = {"BULL": [], "BEAR": []}

    # 1) Delta + POC + Close تصدیق
    if (delta_pct >= cfg["min_delta_pct"] and ratio > 1.0 and poc_pos >= OF_POC_BULL
            and close_pos >= OF_CLOSE_BULL and vol_ok):
        found["BULL"].append("Delta+POC")
    if (delta_pct <= -cfg["min_delta_pct"] and ratio < 1.0 and poc_pos <= OF_POC_BEAR
            and close_pos <= OF_CLOSE_BEAR and vol_ok):
        found["BEAR"].append("Delta+POC")

    # 2) Absorption: جارحانہ فروخت/خریداری مگر قیمت نے مخالف سمت میں قبول کیا
    if (vol_ratio >= OF_ABS_VOL and delta_pct <= -OF_ABS_DELTA
            and c >= o and close_pos >= 0.55):
        found["BULL"].append("Absorption")
    if (vol_ratio >= OF_ABS_VOL and delta_pct >= OF_ABS_DELTA
            and c <= o and close_pos <= 0.45):
        found["BEAR"].append("Absorption")

    # 3) Liquidity Sweep: پچھلی پستی/بلندی توڑ کر واپسی، delta تصدیق کرے
    if l < pl and c > pl and close_pos >= 0.6 and delta_pct > 0 and vol_ok:
        found["BULL"].append("Sweep")
    if h > ph and c < ph and close_pos <= 0.4 and delta_pct < 0 and vol_ok:
        found["BEAR"].append("Sweep")

    # 4) Exhaustion: نئی انتہا پر والیوم سوکھ گیا (پتلی انتہا)، بار واپس لوٹی
    nb5 = OF_BUCKETS // 5
    low_zone = tot[:nb5].sum() / tot.sum()
    high_zone = tot[-nb5:].sum() / tot.sum()
    if l <= pl and low_zone <= OF_EXH_THIN and close_pos >= 0.5 and vol_ok:
        found["BULL"].append("Exhaustion")
    if h >= ph and high_zone <= OF_EXH_THIN and close_pos <= 0.5 and vol_ok:
        found["BEAR"].append("Exhaustion")

    # 5) CVD Divergence: قیمت نئی انتہا، مگر CVD پچھلی انتہا والی سطح سے کمزور
    deltas = np.array([2 * float(k[9]) - float(k[5]) for k in hist])
    cvd = np.cumsum(deltas)
    n = len(look)
    s = len(hist) - 1 - n
    i_hi = s + int(np.argmax([float(k[2]) for k in look]))
    i_lo = s + int(np.argmin([float(k[3]) for k in look]))
    margin = OF_DIV_MIN * float(np.mean(np.abs(deltas[:-1])))
    if h >= ph and cvd[i_hi] - cvd[-1] >= margin and vol_ok:
        found["BEAR"].append("Divergence")
    if l <= pl and cvd[-1] - cvd[i_lo] >= margin and vol_ok:
        found["BULL"].append("Divergence")

    # 6) Stacked Imbalance (3 گنا، مسلسل 3 قیمتیں) — delta کی سمت میں
    if abs(delta_pct) >= OF_STACK_MIN_DELTA:
        ok = tot >= 0.3 * tot.sum() / OF_BUCKETS
        if delta_pct > 0 and has_run(ok & (buy >= OF_STACK_RATIO * sell), OF_STACK_N):
            found["BULL"].append("Stacked Imbalance")
        if delta_pct < 0 and has_run(ok & (sell >= OF_STACK_RATIO * buy), OF_STACK_N):
            found["BEAR"].append("Stacked Imbalance")

    info = (f"delta {delta_pct:+.1f}% | poc {poc_pos:.2f} | close {close_pos:.2f} | "
            f"vol×{vol_ratio:.2f}")
    return found, info


def check_orderflow(symbol, now_ms):
    m1 = get_klines(symbol, "1m", 250)
    if not isinstance(m1, list) or not m1:
        return []
    out = []
    for tf, cfg in OF_TF.items():
        mins = cfg["minutes"]
        ms = mins * 60_000
        start_ms = now_ms // ms * ms
        progress = (now_ms - start_ms) / ms
        if progress < OF_MIN_PROGRESS:
            continue
        hist = get_klines(symbol, tf, 21)
        if not isinstance(hist, list) or len(hist) < 4:
            continue
        if int(hist[-1][0]) != start_ms:
            continue
        sub = [k for k in m1 if int(k[0]) >= start_ms]
        if len(sub) < max(5, int(progress * mins * 0.9)):
            continue
        res = eval_setups(hist, sub, tf, progress)
        if not res:
            continue
        found, info = res
        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        print(f"   OF {symbol} {tf} ({progress:.0%}): {info} → {', '.join(tags) or '-'}")
        for side, names in found.items():
            if names:
                out.append((tf, side, float(hist[-1][4]), names, start_ms))
    return out


def run_orderflow(of_symbols):
    now_ms = int(time.time() * 1000)
    print(f"\nآرڈر فلو: {len(of_symbols)} کوائنز | ٹائم فریم {list(OF_TF)}")

    cutoff = now_ms - 2 * 24 * 3600 * 1000
    state = {k: v for k, v in load_json(OF_STATE_FILE).items()
             if isinstance(v, (int, float)) and v >= cutoff}

    def work(sym):
        try:
            return sym, check_orderflow(sym, now_ms)
        except Exception as e:
            print(f"   OF {sym} error: {e}")
            return sym, []

    alerts = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, res in ex.map(work, of_symbols):
            for tf, side, price, names, start_ms in res:
                new = [n for n in names if f"{sym}|{tf}|{side}|{start_ms}|{n}" not in state]
                if new:
                    alerts.setdefault((tf, side), []).append((sym, price, new, start_ms))

    if not alerts:
        print("آرڈر فلو: کوئی نیا الرٹ نہیں")
        save_json(OF_STATE_FILE, state)
        return

    for (tf, side), items in sorted(alerts.items()):
        lines = [f"{s.replace('USDT', '')} {fmt_price(p)} | {' + '.join(new)}"
                 for s, p, new, _ in items]
        name = "Bullish" if side == "BULL" else "Bearish"
        sent = post_ntfy(f"{tf.upper()} {name} Order Flow", "\n".join(lines),
                         tags="green_circle" if side == "BULL" else "red_circle",
                         url=NFTY_OF_URL)
        if sent:
            for s, _, new, start_ms in items:
                for n in new:
                    state[f"{s}|{tf}|{side}|{start_ms}|{n}"] = start_ms
    save_json(OF_STATE_FILE, state)


# ============================================================
# ⚡ حصہ B: TP/SL سٹریٹجی — مرحلہ 1 فلٹر
# ============================================================
def quick_scan(symbol, win_end):
    data = get_klines(symbol, "1h", WINDOW_HOURS, end_ms=int(win_end.timestamp() * 1000) - 1)
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

    # موجودہ بنتی ہوئی 1h بار کو بند کرنے کے لیے اگلے گھنٹے کی پہلی ٹک بھیجیں
    flush = win_end if win_end.minute == 0 else win_end.replace(minute=0) + timedelta(hours=1)
    try:
        engine.process_tick(
            timestamp=(flush + timedelta(seconds=1)).replace(tzinfo=None),
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


def in_cooldown(st, sym, now):
    t = st.get(sym)
    if not t:
        return False
    try:
        return now - datetime.fromisoformat(t) < timedelta(hours=COOLDOWN_HOURS)
    except Exception:
        return False


def send_signals(signals):
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


def run_strategy(symbols, ticks, of_set, win_start, win_end):
    if B_EXCLUDE_OF_COINS:
        symbols = [s for s in symbols if s not in of_set]
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

    now = datetime.now(timezone.utc)
    state = load_json(STATE_FILE)
    signals = [s for s in signals if not in_cooldown(state, s["symbol"], now)]
    signals.sort(key=lambda s: s["score"], reverse=True)
    signals = signals[:MAX_SIGNALS_PER_SCAN]

    if not signals:
        print("کوئی سگنل نہیں — اگلی بار")
        return
    send_signals(signals)
    for s in signals:
        state[s["symbol"]] = now.isoformat()
    save_json(STATE_FILE, state)


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    win_start, win_end = window_bounds()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | {WINDOW_HOURS}h ونڈو "
          f"{win_start:%m-%d %H:%M} → {win_end:%m-%d %H:%M} UTC (آخری بند 15m)")
    print("=" * 60)

    symbols, ticks = get_all_markets()
    if not symbols:
        print("مارکیٹس نہیں ملیں")
        return

    of_symbols = symbols[:OF_COIN_COUNT]

    # 1) آرڈر فلو الرٹس (جاری 1h/4h بار، کلوزنگ کی شرط نہیں)
    try:
        run_orderflow(of_symbols)
    except Exception as e:
        print(f"آرڈر فلو error: {e}")

    # 2) TP/SL سٹریٹجی (15m کینڈل بند ہونے پر)
    run_strategy(symbols, ticks, set(of_symbols), win_start, win_end)
    print(f"\nمکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
