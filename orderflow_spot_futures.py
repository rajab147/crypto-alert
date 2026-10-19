import os
import json
import time
import statistics
import requests
import numpy as np
import pandas as pd
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# ⚙️ سیٹنگز — حصہ B: TP/SL والی سٹریٹجی (1h بارز، 12h ونڈو)
#    نیا: سکین ہر 15 منٹ کی کینڈل بند ہونے پر (شیڈیولر ہر 15 منٹ پر چلائیں)
#    ونڈو کا اختتام = آخری بند 15m کینڈل؛ موجودہ بنتی ہوئی 1h بار بھی شامل
# ============================================================
BASE_URL = "https://data-api.binance.vision"
WINDOW_HOURS = 10
SCAN_MINUTES = 15

POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.30

BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

TARGET_PCT = 0.7
STOP_PCT = 0.6
MAX_HOLD_HOURS = 10
FEE_ROUNDTRIP_PCT = 0.2

SL_ATR_MULT = 2.0
TP_ATR_MULT = 2.5
MAX_SL_PCT = 4.0
MAX_DRIFT_FRAC = 0.5

MAX_SIGNALS_PER_SCAN = 5
COOLDOWN_HOURS = 4
STATE_FILE = "signal_state.json"

# ٹریڈ ٹریکر: ہر بھیجے گئے سگنل کو کھلی ٹریڈ کے طور پر محفوظ کریں اور TP/SL/Timeout کا نتیجہ بتائیں
TRADES_FILE = "trades_state.json"
MAX_CLOSED_KEEP = 500       # فائل میں زیادہ سے زیادہ اتنی بند ٹریڈز رکھیں
SUMMARY_LAST_N = 50         # win rate کے خلاصے میں آخری اتنی ٹریڈز
BOTH_HIT_RESULT = "SL"      # ایک ہی 1m کینڈل میں TP اور SL دونوں چھو جائیں تو محتاط فرض: SL

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
OF_MIN_PROGRESS = 0.10      # جاری بار میں جلدی scan؛ صرف ابتدائی حرکت کے لیے
OF_MAX_MOVE_FROM_OPEN_PCT = 0.45  # موجودہ 1h/4h بار میں اتنا move ہو چکا ہو تو chase نہ کریں
OF_MAX_RANGE_AVG_MULT = 0.85      # بار کی range پچھلی بارز کی اوسط range کے قریب پہنچے تو late سمجھیں
OF_MAX_LOOKBACK_MOVE_PCT = 1.20   # حالیہ 3 بند 1h بارز میں بڑا pump/dump ہو تو نئی entry روکیں
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

# ============================================================
# مستقل CVD Zero-Line Touch ماڈیول (صرف 1h؛ footprint/orderflow سے الگ)
# CVD صرف Binance 1h kline کے taker-buy base volume سے بنتا ہے۔
# ہر UTC دن پر CVD reset؛ صرف zero کے قریب TOUCH پر الرٹ، crossover لازم نہیں۔
# ============================================================
CVD_TF = "1h"
CVD_LOOKBACK_HOURS = 48
CVD_ZERO_TOUCH_PCT = 0.02   # tolerance: اوسط hourly base volume کا 2%
CVD_STATE_FILE = "cvd_zero_touch_state.json"
CVD_ALERT_MAX_SYMBOLS = 200

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
    """آخری بند 15m کینڈل تک؛ 10 گھنٹے کی ونڈو؛ آخری دستیاب 15m حد تک۔"""
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
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(obj, f, ensure_ascii=False)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    except Exception as e:
        print(f"state save error ({path}): {e}")
        try:
            if os.path.exists(tmp):
                os.remove(tmp)
        except OSError:
            pass


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
def build_profile(trades, lo, hi, nb):
    """حقیقی aggTrades سے price-bucket footprint؛ کوئی candle-range volume تخمینہ نہیں۔
    Binance aggTrade m=True کا مطلب buyer maker ہے، یعنی aggressive seller۔
    """
    buy = np.zeros(nb, dtype=float)
    sell = np.zeros(nb, dtype=float)
    if hi <= lo:
        return buy, sell
    step = (hi - lo) / nb
    for t in trades:
        try:
            price = float(t["p"])
            qty = float(t["q"])
            idx = min(max(int((price - lo) / step), 0), nb - 1)
            if bool(t["m"]):
                sell[idx] += qty
            else:
                buy[idx] += qty
        except (KeyError, TypeError, ValueError):
            continue
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
    if h <= l or not prev:
        return None
    # اصل executed aggTrades سے current-bar buy/sell حجم اور delta نکالیں۔
    # m=True => buyer maker => aggressive seller؛ m=False => aggressive buyer۔
    buy_qty = sum(float(t["q"]) for t in sub if not bool(t["m"]))
    sell_qty = sum(float(t["q"]) for t in sub if bool(t["m"]))
    v = buy_qty + sell_qty
    if v <= 0:
        return None
    avg_vol = statistics.mean(float(k[5]) for k in prev)
    elapsed = min(1.0, max(float(progress), 1.0 / max(cfg["minutes"], 1)))
    vol_ratio = v / (avg_vol * elapsed) if avg_vol > 0 else 0.0
    delta_pct = (buy_qty - sell_qty) / v * 100
    ratio = buy_qty / max(sell_qty, 1e-12)
    close_pos = (c - l) / (h - l)

    # ANTI-CHASE: پہلے ہی چل چکے pump/dump کے پیچھے entry signal نہ دیں.
    # موجودہ candle کی open سے قیمت کا فاصلہ، candle range بمقابلہ پچھلی اوسط range،
    # اور حالیہ 3 بند candles کا مجموعی move چیک کریں۔ یہ filter OF setups پر لاگو ہے۔
    move_from_open_pct = abs(c - o) / max(o, 1e-12) * 100.0
    prev_ranges_pct = [
        (float(k[2]) - float(k[3])) / max(float(k[4]), 1e-12) * 100.0
        for k in prev[-OF_LOOKBACK:]
        if float(k[2]) >= float(k[3]) and float(k[4]) > 0
    ]
    avg_range_pct = float(np.mean(prev_ranges_pct)) if prev_ranges_pct else 0.0
    current_range_pct = (h - l) / max(o, 1e-12) * 100.0
    recent = prev[-3:]
    recent_move_pct = 0.0
    if len(recent) >= 2:
        recent_start = float(recent[0][1])
        recent_end = float(recent[-1][4])
        recent_move_pct = abs(recent_end - recent_start) / max(recent_start, 1e-12) * 100.0

    if (move_from_open_pct >= OF_MAX_MOVE_FROM_OPEN_PCT
            or (avg_range_pct > 0 and current_range_pct >= avg_range_pct * OF_MAX_RANGE_AVG_MULT)
            or recent_move_pct >= OF_MAX_LOOKBACK_MOVE_PCT):
        return None

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

    # ایک ہی کینڈل میں متضاد bullish اور bearish سیٹ اپس ہوں تو اسے غیر واضح سمجھیں۔
    if found["BULL"] and found["BEAR"]:
        found = {"BULL": [], "BEAR": []}

    info = (f"delta {delta_pct:+.1f}% | poc {poc_pos:.2f} | close {close_pos:.2f} | "
            f"vol×{vol_ratio:.2f}")
    return found, info


def check_orderflow(symbol, now_ms):
    out = []
    for tf, cfg in OF_TF.items():
        mins = cfg["minutes"]
        interval_ms = mins * 60_000
        start_ms = now_ms // interval_ms * interval_ms
        progress = (now_ms - start_ms) / interval_ms
        if progress < OF_MIN_PROGRESS:
            continue
        hist = get_klines(symbol, tf, 21)
        if not isinstance(hist, list) or len(hist) < 4 or int(hist[-1][0]) != start_ms:
            continue
        # موجودہ جاری بار کے اصل executed trades؛ 1m candle سے footprint اخذ نہیں کرتے۔
        sub = load_aggtrades(symbol,
            datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc) + timedelta(milliseconds=1))
        if not isinstance(sub, list) or len(sub) < 20:
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
# 🟣 مستقل CVD Zero-Line Touch — 1h only، دوسرے ماڈیولز سے آزاد
# ============================================================
def check_cvd_zero_touch(symbol, now_ms):
    """Independent 1h CVD zero-line TOUCH.
    CVD resets at UTC midnight and is accumulated from 1h taker-buy base volume.
    Trigger only when the current cumulative CVD is within the zero tolerance.
    No footprint profile or order-flow setup is used here.
    """
    kl = get_klines(symbol, CVD_TF, CVD_LOOKBACK_HOURS)
    if not isinstance(kl, list) or len(kl) < 3:
        return None

    now_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    day_start_ms = int(now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    today = [k for k in kl if day_start_ms <= int(k[0]) <= now_ms]
    if len(today) < 2:
        return None

    deltas, hourly_volumes = [], []
    for k in today:
        try:
            volume, taker_buy = float(k[5]), float(k[9])
            deltas.append(2.0 * taker_buy - volume)
            hourly_volumes.append(volume)
        except (TypeError, ValueError, IndexError):
            continue
    if len(deltas) < 2 or len(hourly_volumes) != len(deltas):
        return None

    # Cumulative series is reset at UTC day start; last value is the live 1h zero-touch check.
    cvd_path = np.cumsum(np.asarray(deltas, dtype=float))
    cvd = float(cvd_path[-1])
    avg_hourly_volume = float(np.mean(hourly_volumes))
    tolerance = max(avg_hourly_volume * CVD_ZERO_TOUCH_PCT, 1e-12)
    if abs(cvd) > tolerance:
        return None

    last = today[-1]
    try:
        price = float(last[4])
        candle_start = int(last[0])
    except (TypeError, ValueError, IndexError):
        return None
    return {"symbol": symbol, "price": price, "cvd": cvd,
            "tolerance": tolerance, "candle_start": candle_start,
            "delta": float(deltas[-1])}


def run_cvd_zero_touch(symbols):
    """آزاد اور علیحدہ 1h CVD zero-touch scan؛ اس کا نتیجہ OF/TP-SL signals کو نہیں بدلتا۔"""
    now_ms = int(time.time() * 1000)
    print(f"\nCVD Zero Touch: {len(symbols)} symbols | timeframe 1h | independent module")
    state = load_json(CVD_STATE_FILE)
    if not isinstance(state, dict):
        state = {}

    found = []
    workers = min(THREADS_KLINES, max(1, len(symbols)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(check_cvd_zero_touch, sym, now_ms): sym for sym in symbols[:CVD_ALERT_MAX_SYMBOLS]}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                result = future.result()
            except Exception as e:
                print(f"   CVD {sym} error: {e}")
                continue
            if not result:
                continue
            key = f"{sym}|1h|{result['candle_start']}"
            if key in state:
                continue
            found.append((key, result))

    if not found:
        print("CVD Zero Touch: no new touch")
        return

    lines = []
    for key, r in found:
        lines.append(
            f"{r['symbol'].replace('USDT', '')} @ {fmt_price(r['price'])} | "
            f"CVD {r['cvd']:+.4g} (zero tolerance ±{r['tolerance']:.4g}) | "
            f"1h delta {r['delta']:+.4g}"
        )
    sent = post_ntfy("1H CVD ZERO-LINE TOUCH", "\n".join(lines),
                     tags="large_purple_circle", url=NFTY_URL)
    if sent:
        for key, _ in found:
            state[key] = now_ms
        # state کو محدود رکھیں تاکہ فائل غیر ضروری طور پر نہ بڑھے
        cutoff = now_ms - 7 * 24 * 3600 * 1000
        state = {k: v for k, v in state.items() if isinstance(v, (int, float)) and v >= cutoff}
        save_json(CVD_STATE_FILE, state)
    else:
        print("CVD alert send failed; state not marked, will retry next run")


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
    """Fetch executed aggregate trades over [start, end), paging by aggregate trade ID.
    Returns None on API failure or if pagination cap prevents proving full coverage.
    """
    start_ms = int(win_start.timestamp() * 1000)
    end_ms = int(win_end.timestamp() * 1000)
    if end_ms <= start_ms:
        return []
    trades = []
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms - 1, "limit": 1000}
    for page in range(MAX_TRADE_PAGES):
        data = safe_get(f"{BASE_URL}/api/v3/aggTrades", params=params)
        if not isinstance(data, list):
            return None
        if not data:
            return trades
        batch = []
        for t in data:
            try:
                ts = int(t["T"])
                if start_ms <= ts < end_ms:
                    batch.append(t)
            except (KeyError, TypeError, ValueError):
                return None
        trades.extend(batch)
        if len(data) < 1000:
            return trades
        last_id = int(data[-1]["a"])
        # startTime/endTime اور fromId ایک ہی درخواست میں نہ بھیجیں۔
        params = {"symbol": symbol, "fromId": last_id + 1, "limit": 1000}
        # fromId paging وقت کی حد سے آگے بھی جا سکتی ہے؛ اگلے batch میں فلٹر ہوگا۔
        if int(data[-1]["T"]) >= end_ms - 1:
            return trades
    stats["too_heavy"] += 1
    print(f"⚠️ {symbol}: aggTrades pagination limit reached; data may be incomplete, skipping")
    return None


def analyze_symbol(symbol, tick_size, win_start, win_end):
    """Build hourly footprint bars directly from executed Binance aggTrades.
    Price-level volume is bucketed at the market tick size; buy/sell aggressor side
    comes from aggTrade.m. This is real trade-print footprint, not full order-book data.
    """
    trades = load_aggtrades(symbol, win_start, win_end)
    if trades is None:
        return None
    if len(trades) < MIN_TRADES:
        stats["too_few"] += 1
        return None

    grouped = {}
    for t in trades:
        try:
            ts = int(t["T"])
            if not (int(win_start.timestamp() * 1000) <= ts < int(win_end.timestamp() * 1000)):
                continue
            hour = ts // 3_600_000 * 3_600_000
            price, qty = float(t["p"]), float(t["q"])
            row = grouped.setdefault(hour, {"trades": [], "high": price, "low": price,
                                            "close": price, "last_ts": -1,
                                            "buy": 0.0, "sell": 0.0})
            row["trades"].append(t)
            row["high"] = max(row["high"], price)
            row["low"] = min(row["low"], price)
            if ts >= row["last_ts"]:
                row["last_ts"], row["close"] = ts, price
            if bool(t["m"]):
                row["sell"] += qty
            else:
                row["buy"] += qty
        except (KeyError, TypeError, ValueError):
            continue

    rows = []
    for hour, bar in sorted(grouped.items()):
        if not bar["trades"]:
            continue
        # True executed volume at each observed price; POC is highest-volume price level.
        profile = {}
        for t in bar["trades"]:
            price = float(t["p"])
            qty = float(t["q"])
            level = round(round(price / tick_size) * tick_size, 12) if tick_size > 0 else price
            profile[level] = profile.get(level, 0.0) + qty
        poc = max(profile.items(), key=lambda x: x[1])[0]
        rng = bar["high"] - bar["low"]
        poc_pos = (poc - bar["low"]) / rng if rng > 0 else 0.5
        buy_vol, sell_vol = bar["buy"], bar["sell"]
        delta = buy_vol - sell_vol
        ratio = buy_vol / sell_vol if sell_vol > 0 else (999.0 if buy_vol > 0 else 1.0)
        rows.append({"start_time": datetime.fromtimestamp(hour / 1000, tz=timezone.utc).replace(tzinfo=None),
                     "high": bar["high"], "low": bar["low"], "close": bar["close"],
                     "poc": poc, "poc_position": poc_pos, "delta": delta,
                     "bid_volume": sell_vol, "ask_volume": buy_vol, "ratio": ratio,
                     "trade_count": len(bar["trades"])})

    if len(rows) < 6:
        return None
    df = pd.DataFrame(rows).sort_values("start_time")
    df["of_bull"] = (df["delta"] > 0) & (df["poc_position"] > POC_BULL_MIN) & (df["ratio"] > 1.0)
    df["of_bear"] = (df["delta"] < 0) & (df["poc_position"] < POC_BEAR_MAX) & (df["ratio"] < 1.0)
    bull, bear = int(df["of_bull"].sum()), int(df["of_bear"].sum())
    total = bull + bear
    if total < MIN_SIGNALS:
        return None
    bull_pct = bull / total * 100
    if bull_pct >= SELL_THRESHOLD:
        signal = "BUY"
    elif bull_pct <= BUY_THRESHOLD:
        signal = "SELL"
    else:
        return None

    ranges = ((df["high"] - df["low"]) / df["close"] * 100).replace([np.inf, -np.inf], np.nan).dropna()
    atr_pct = float(ranges.mean()) if not ranges.empty else 0.0
    if atr_pct <= 0 or max(STOP_PCT, SL_ATR_MULT * atr_pct) > MAX_SL_PCT:
        stats["wide_sl"] += 1
        return None
    # Strategy anti-chase: do not issue a fresh entry after a sharp last-hour candle
    # or after the last few hourly closes have already travelled too far.
    last_row = df.iloc[-1]
    last_open = float(last_row["high"] + last_row["low"]) / 2.0
    last_range_pct = (float(last_row["high"]) - float(last_row["low"])) / max(float(last_row["close"]), 1e-12) * 100.0
    if last_range_pct >= max(OF_MAX_MOVE_FROM_OPEN_PCT * 2.0, atr_pct * 1.25):
        return None
    score = min(total, 8) + abs(bull_pct - 50) / 50 * 5
    return {"symbol": symbol, "signal": signal, "atr_pct": atr_pct,
            "price": float(df.iloc[-1]["close"]), "bull_count": bull, "bear_count": bear,
            "bull_pct": bull_pct, "bars": len(df), "score": score,
            "footprint_source": "Binance aggTrades", "trade_count": int(df["trade_count"].sum())}


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
    sent_signals = []
    for s in signals:
        sym = s["symbol"].replace("USDT", "")
        tp, sl, tp_pct, sl_pct = trade_levels(s["signal"], s["price"], s["atr_pct"])
        buy = s["signal"] == "BUY"
        sign_tp, sign_sl = ("+", "-") if buy else ("-", "+")
        body = (f"Entry: {fmt_price(s['price'])}\n"
                f"TP: {fmt_price(tp)} ({sign_tp}{tp_pct:.1f}%)\n"
                f"SL: {fmt_price(sl)} ({sign_sl}{sl_pct:.1f}%)\n"
                f"Bull {s['bull_pct']:.0f}% | {s['bull_count']}B/{s['bear_count']}S\n"
                f"Max hold {MAX_HOLD_HOURS}h | fees ~{FEE_ROUNDTRIP_PCT}%\n"
                f"Footprint: executed aggTrades ({s.get('trade_count', 0)} trades)")
        ok = post_ntfy(f"{s['signal']} {sym} @ {fmt_price(s['price'])}", body,
                       tags="green_circle,moneybag" if buy else "red_circle,moneybag")
        if ok:
            sent_signals.append(s)
        time.sleep(0.3)
    return sent_signals


# ============================================================
# 📈 ٹریڈ ٹریکر: TP / SL / Timeout کا نتیجہ
#    - ہر سگنل بھیجتے وقت کھلی ٹریڈ محفوظ ہوتی ہے
#    - ہر رن پر 1m کینڈلز سے دیکھا جاتا ہے کہ پہلے TP لگا یا SL
#    - ایک ہی کینڈل میں دونوں چھو جائیں تو BOTH_HIT_RESULT (محتاط: SL)
#    - MAX_HOLD_HOURS گزرنے پر Timeout (اس وقت کی قیمت پر بند)
#    - نتیجے کا Ntfy الرٹ + win rate کا خلاصہ
# ============================================================
def load_trades():
    d = load_json(TRADES_FILE)
    return {"open": d.get("open", []), "closed": d.get("closed", [])}


def register_trades(signals, now):
    """نئے بھیجے گئے سگنلز کو کھلی ٹریڈز میں شامل کریں۔"""
    book = load_trades()
    now_ms = int(now.timestamp() * 1000)
    for s in signals:
        tp, sl, tp_pct, sl_pct = trade_levels(s["signal"], s["price"], s["atr_pct"])
        book["open"].append({
            "id": f"{s['symbol']}|{now_ms}",
            "symbol": s["symbol"],
            "side": s["signal"],
            "entry": s["price"],
            "tp": tp,
            "sl": sl,
            "tp_pct": tp_pct,
            "sl_pct": sl_pct,
            "opened_ms": now_ms,
            "expires_ms": now_ms + MAX_HOLD_HOURS * 3600 * 1000,
            "checked_ms": (now_ms // 60_000 + 1) * 60_000,  # اگلی مکمل 1m کینڈل سے
        })
    save_json(TRADES_FILE, book)


def evaluate_trade(tr, now_ms):
    """ایک کھلی ٹریڈ چیک کریں۔ بند ہو تو (result, exit_price, exit_ms) ورنہ None۔
    ڈیٹا نہ ملے تو بھی None (ٹریڈ کھلی رہتی ہے)۔"""
    buy = tr["side"] == "BUY"
    start = int(tr["checked_ms"])
    kl = get_klines_from(tr["symbol"], "1m", start, 1000)
    if not isinstance(kl, list):
        return None

    last_close = None
    last_ms = start
    for k in kl:
        k_open, k_close_time = int(k[0]), int(k[6])
        if k_close_time >= now_ms:      # ابھی بنتی ہوئی کینڈل چھوڑ دیں
            break
        if k_open >= tr["expires_ms"]:
            break
        h, l, c = float(k[2]), float(k[3]), float(k[4])
        hit_tp = h >= tr["tp"] if buy else l <= tr["tp"]
        hit_sl = l <= tr["sl"] if buy else h >= tr["sl"]
        if hit_tp and hit_sl:
            res = BOTH_HIT_RESULT
            return res, (tr["tp"] if res == "TP" else tr["sl"]), k_open
        if hit_tp:
            return "TP", tr["tp"], k_open
        if hit_sl:
            return "SL", tr["sl"], k_open
        last_close, last_ms = c, k_open + 60_000

    if now_ms >= tr["expires_ms"]:
        if last_close is None:
            last_close = live_price(tr["symbol"]) or tr["entry"]
        return "TIMEOUT", last_close, tr["expires_ms"]

    tr["checked_ms"] = last_ms          # اگلی بار یہیں سے آگے
    return None


def get_klines_from(symbol, interval, start_ms, limit):
    return safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": interval,
                            "startTime": start_ms, "limit": limit})


def live_price(symbol):
    d = safe_get(f"{BASE_URL}/api/v3/ticker/price", params={"symbol": symbol})
    try:
        return float(d["price"])
    except Exception:
        return None


def pnl_pct(side, entry, exit_price):
    gross = (exit_price - entry) / entry * 100
    if side == "SELL":
        gross = -gross
    return gross - FEE_ROUNDTRIP_PCT


def summary_text(closed):
    last = closed[-SUMMARY_LAST_N:]
    if not last:
        return "No closed trades yet"
    wins = sum(1 for t in last if t["pnl_pct"] > 0)
    tp = sum(1 for t in last if t["result"] == "TP")
    sl = sum(1 for t in last if t["result"] == "SL")
    to = sum(1 for t in last if t["result"] == "TIMEOUT")
    total = sum(t["pnl_pct"] for t in last)
    return (f"Last {len(last)}: win {wins / len(last) * 100:.0f}% | "
            f"TP {tp} / SL {sl} / TO {to} | net {total:+.1f}% (after fees)")


def track_trades():
    """کھلی ٹریڈز چیک کریں، بند ہونے والی کی اطلاع دیں، خلاصہ بھیجیں۔"""
    book = load_trades()
    if not book["open"]:
        print("ٹریکر: کوئی کھلی ٹریڈ نہیں")
        return
    now_ms = int(time.time() * 1000)
    print(f"\nٹریکر: {len(book['open'])} کھلی ٹریڈز چیک ہو رہی ہیں")

    still_open, newly_closed = [], []
    for tr in book["open"]:
        try:
            res = evaluate_trade(tr, now_ms)
        except Exception as e:
            print(f"   {tr['symbol']} ٹریکر error: {e}")
            res = None
        if res is None:
            still_open.append(tr)
            continue
        result, exit_price, exit_ms = res
        tr = dict(tr, result=result, exit=exit_price, exit_ms=exit_ms,
                  pnl_pct=pnl_pct(tr["side"], tr["entry"], exit_price))
        newly_closed.append(tr)
        print(f"   {tr['symbol']} {tr['side']}: {result} ({tr['pnl_pct']:+.2f}%)")

    book["open"] = still_open
    book["closed"] = (book["closed"] + newly_closed)[-MAX_CLOSED_KEEP:]
    save_json(TRADES_FILE, book)

    if not newly_closed:
        print(f"ٹریکر: کوئی ٹریڈ بند نہیں ہوئی | {len(still_open)} کھلی")
        return

    summary = summary_text(book["closed"])
    for tr in newly_closed:
        sym = tr["symbol"].replace("USDT", "")
        held_h = (tr["exit_ms"] - tr["opened_ms"]) / 3_600_000
        body = (
            f"{tr['side']} {sym} | {tr['result']} | {tr['pnl_pct']:+.2f}% net\n"
            f"Entry {fmt_price(tr['entry'])} -> Exit {fmt_price(tr['exit'])}\n"
            f"Held {held_h:.1f}h | open now: {len(still_open)}\n"
            f"{summary}"
        )
        tag = {"TP": "white_check_mark", "SL": "x", "TIMEOUT": "hourglass"}[tr["result"]]
        post_ntfy(f"{tr['result']} {sym} {tr['pnl_pct']:+.1f}%", body, tags=tag)
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
    sent_signals = send_signals(signals)
    if not sent_signals:
        print("⚠️ کوئی سگنل Ntfy پر کامیابی سے نہیں بھیجا گیا؛ ٹریڈز رجسٹر نہیں کی گئیں")
        return
    register_trades(sent_signals, now)
    for s in sent_signals:
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

    # 1b) علیحدہ CVD Zero-Line Touch: صرف 1h؛ orderflow/footprint signals سے آزاد
    try:
        run_cvd_zero_touch(symbols)
    except Exception as e:
        print(f"CVD Zero Touch error: {e}")

    # 2) کھلی ٹریڈز کا نتیجہ (TP/SL/Timeout) — نئے سگنلز سے پہلے چیک کریں
    try:
        track_trades()
    except Exception as e:
        print(f"ٹریکر error: {e}")

    # 3) TP/SL سٹریٹجی (15m کینڈل بند ہونے پر)
    run_strategy(symbols, ticks, set(of_symbols), win_start, win_end)
    print(f"\nمکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
