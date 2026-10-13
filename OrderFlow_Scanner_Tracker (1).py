import os
import json
import time
import statistics
import requests
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor

# ============================================================
# ⚙️ سیٹنگز — آرڈر فلو الرٹ (ٹاپ 100 والیوم کوائنز: 1h، 4h، ڈیلی)
#    کینڈل کلوزنگ کی شرط نہیں: جاری بار میں سیٹ اپ بنتے ہی الرٹ
#    ڈیٹا: Binance سپاٹ (data-api.binance.vision)
# ============================================================
BASE_URL = "https://data-api.binance.vision"

OF_COIN_COUNT = 100                 # 24h والیوم کی رینک میں پہلے اتنے کوائنز
DAILY_MIN_QUOTE_VOL = 5_000_000     # ڈیلی کے لیے کم از کم 24h والیوم (USDT)؛ اس سے کم والے ڈیلی سے باہر

# min_progress: بار کا کم از کم کتنا وقت گزرنے کے بعد الرٹ (ورنہ جھوٹے الرٹ)
# sub: footprint تخمینے کے لیے چھوٹی کینڈل؛ verify: aggTrades سے اصل تصدیق
OF_TF = {
    "1h": {"minutes": 60,   "min_delta_pct": 6.0, "min_progress": 0.50, "sub": "1m", "verify": True},
    "4h": {"minutes": 240,  "min_delta_pct": 5.0, "min_progress": 0.25, "sub": "1m", "verify": True},
    "1d": {"minutes": 1440, "min_delta_pct": 4.0, "min_progress": 0.25, "sub": "5m", "verify": False},
}
OF_BUCKETS = 40             # footprint کے لیے قیمت کے خانے
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

# aggTrades سے اصل تصدیق (صرف 1h اور 4h، اور صرف ان کوائنز پر جن پر تخمینے سے سیٹ اپ بنا)
OF_MAX_TRADE_PAGES = 50     # زیادہ سے زیادہ 50 صفحے × 1000 ٹریڈز؛ اس سے بھاری ہو تو "est" ہی رہے
OF_MIN_REAL_TRADES = 200    # اس سے کم ٹریڈز پر اصل footprint بے معنی
OF_VOL_MATCH = (0.85, 1.25)  # ٹریڈز کا کل والیوم kline والیوم کے قریب ہو، ورنہ ڈیٹا ادھورا

# ============================================================
# ⚙️ سیٹنگز — ٹریکر (پیمائش: MFE / MAE / مقررہ وقت کے بعد سمت)
#    TP/SL نہیں؛ مقصد یہ ناپنا ہے کہ ہر سیٹ اپ اور ٹائم فریم کتنا کام کرتا ہے
# ============================================================
TRADES_FILE = "trades_state.json"
OF_TRACK = {
    "1h": {"hours": 6,  "step": "1m",  "step_ms": 60_000},
    "4h": {"hours": 24, "step": "5m",  "step_ms": 300_000},
    "1d": {"hours": 72, "step": "15m", "step_ms": 900_000},
}
MAX_CLOSED_KEEP = 500       # فائل میں زیادہ سے زیادہ اتنی بند ٹریڈز
SUMMARY_LAST_N = 30         # خلاصے میں ہر گروپ کی آخری اتنی ٹریڈز
SMALL_SAMPLE = 10           # اس سے کم نمونے پر "*" (کم بھروسہ)
NTFY_CHUNK = 30             # ایک پیغام میں زیادہ سے زیادہ اتنے کوائنز

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    raise SystemExit(1)
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL

session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0, "mismatch": 0,
         "confirmed": 0, "rejected": 0, "unverified": 0}


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


def get_klines(symbol, interval, limit, start_ms=None, end_ms=None):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        params["startTime"] = int(start_ms)
    if end_ms is not None:
        params["endTime"] = int(end_ms)
    return safe_get(f"{BASE_URL}/api/v3/klines", params=params)


# ============================================================
# 📋 ٹاپ کوائنز (24h والیوم کی رینک)
# ============================================================
def get_top_markets():
    """واپسی: (سمبلز رینک کے مطابق, {سمبل: 24h quote والیوم}, {سمبل: 24h ٹریڈز کی تعداد})"""
    print("Binance مارکیٹس...")
    data = safe_get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=30)
    if not data or "symbols" not in data:
        return [], {}, {}

    skip = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
            "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY", "USDTBIDR",
            "USD1USDT", "USDPUSDT", "USDEUSDT", "RLUSDUSDT", "PYUSDUSDT"}  # stablecoin جوڑے

    valid = set()
    for s in data["symbols"]:
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        sym = s.get("symbol", "")
        if sym in skip:
            continue
        base = s.get("baseAsset", "")
        if base.endswith(("UP", "DOWN", "BULL", "BEAR")) and base not in ("JUP",):
            continue
        valid.add(sym)
    print(f"کل USDT مارکیٹس: {len(valid)}")

    ticker = safe_get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=30)
    if not isinstance(ticker, list):
        print("ticker نہیں ملا — رینکنگ کے بغیر آگے نہیں بڑھ سکتے")
        return [], {}, {}

    rows = []
    for t in ticker:
        sym = t.get("symbol")
        if sym in valid:
            try:
                rows.append((sym, float(t.get("quoteVolume", 0)), int(t.get("count", 0))))
            except (TypeError, ValueError):
                continue
    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:OF_COIN_COUNT]
    print(f"{len(rows)} کوئنز منتخب")
    return ([r[0] for r in rows],
            {r[0]: r[1] for r in rows},
            {r[0]: r[2] for r in rows})


# ============================================================
# 📲 Ntfy
# ============================================================
def post_ntfy(title, body, tags="rotating_light", url=None, priority="high"):
    # Title ہیڈر میں صرف ASCII؛ ایموجی Tags سے آتے ہیں
    try:
        r = requests.post(url or NFTY_URL, data=body.encode("utf-8"),
                          headers={"Title": title, "Priority": priority, "Tags": tags},
                          timeout=15)
        print(f"Ntfy [{title}]: {r.status_code}")
        return r.ok
    except Exception as e:
        print(f"Ntfy Error: {e}")
        return False


# ============================================================
# 🧭 آرڈر فلو (جاری 1h، 4h اور ڈیلی بار، کلوزنگ کی شرط کے بغیر)
#    ڈیٹا: چھوٹی klines (taker buy volume سے اصل delta) + ٹائم فریم klines
#    footprint: پہلے تخمینہ (ہر چھوٹی کینڈل کا والیوم اس کی [low, high] میں برابر)
#    1h/4h: تخمینے سے سیٹ اپ بنے تو aggTrades سے اصل footprint بنا کر دوبارہ جانچ
#    سیٹ اپس: Delta+POC، Absorption، Sweep، Exhaustion، Divergence (CVD)، Stacked Imbalance
# ============================================================
def build_profile(sub, lo, hi, nb):
    """تخمینی footprint (klines سے)۔"""
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


def build_profile_real(trades, lo, hi, nb):
    """اصل footprint (aggTrades سے): m=True کا مطلب خریدار maker، یعنی جارح فروخت۔"""
    step = (hi - lo) / nb
    n = len(trades)
    p = np.fromiter((float(t["p"]) for t in trades), float, n)
    q = np.fromiter((float(t["q"]) for t in trades), float, n)
    is_buy = np.fromiter((not t["m"] for t in trades), bool, n)
    idx = np.clip(((p - lo) / step).astype(int), 0, nb - 1)
    buy = np.bincount(idx[is_buy], weights=q[is_buy], minlength=nb)
    sell = np.bincount(idx[~is_buy], weights=q[~is_buy], minlength=nb)
    return buy, sell


def has_run(mask, n):
    run = 0
    for m in mask:
        run = run + 1 if m else 0
        if run >= n:
            return True
    return False


def eval_setups(hist, profile, tf, progress):
    """جاری بار (hist[-1]) پر تمام سیٹ اپس چیک کریں۔ profile = (buy, sell) خانوں کے لحاظ سے۔
    واپسی: ({"BULL": [...], "BEAR": [...]}, info) یا None"""
    cfg = OF_TF[tf]
    bar, prev = hist[-1], hist[:-1]
    o, h, l, c = float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4])
    v, tb = float(bar[5]), float(bar[9])
    if v <= 0 or h <= l:
        return None

    if not prev:
        return None
    avg_vol = statistics.mean(float(k[5]) for k in prev)
    # progress کو 0 سے اوپر اور 1 تک محدود رکھیں؛ جزوی بار کے حجم کا تقابل
    # اسی گزرے ہوئے وقت کے تناسب سے ہو، مکمل بار کے حجم سے براہِ راست نہیں۔
    elapsed = min(1.0, max(float(progress), 1.0 / max(cfg["minutes"], 1)))
    vol_ratio = v / (avg_vol * elapsed) if avg_vol > 0 else 0.0
    delta_pct = (2 * tb - v) / v * 100
    ratio = tb / max(v - tb, 1e-12)
    close_pos = (c - l) / (h - l)

    buy, sell = profile
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


def load_real_trades(symbol, start_ms, end_ms):
    """بار کے آغاز سے اب تک کے aggTrades۔ بہت بھاری ہو تو None۔"""
    trades = []
    params = {"symbol": symbol, "startTime": int(start_ms), "limit": 1000}
    for _ in range(OF_MAX_TRADE_PAGES):
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


def verify_real(symbol, hist, start_ms, now_ms, tf, progress, tcount):
    """اصل footprint سے سیٹ اپس دوبارہ جانچیں۔ واپسی: eval_setups کا نتیجہ، یا None (تصدیق ممکن نہیں)۔"""
    bar = hist[-1]
    hi, lo = float(bar[2]), float(bar[3])
    v = float(bar[5])
    if hi <= lo or v <= 0:
        return None

    # 24h اوسط سے اندازہ: ٹریڈز حد سے بہت زیادہ ہوں تو کالیں ضائع نہ کریں
    est_trades = tcount * (now_ms - start_ms) / 86_400_000 if tcount else 0
    if est_trades > OF_MAX_TRADE_PAGES * 1000 * 1.2:
        stats["too_heavy"] += 1
        return None

    trades = load_real_trades(symbol, start_ms, int(time.time() * 1000))
    if trades is None or len(trades) < OF_MIN_REAL_TRADES:
        return None

    qty = sum(float(t["q"]) for t in trades)
    if not (OF_VOL_MATCH[0] <= qty / v <= OF_VOL_MATCH[1]):
        stats["mismatch"] += 1
        return None

    return eval_setups(hist, build_profile_real(trades, lo, hi, OF_BUCKETS), tf, progress)


def all_sent(sent, sym, tf, start_ms, found):
    """اس بار کے تمام سیٹ اپس پہلے ہی الرٹ ہو چکے ہوں تو تصدیق کی ضرورت نہیں۔"""
    keys = [f"{sym}|{tf}|{side}|{start_ms}|{n}"
            for side, names in found.items() for n in names]
    return bool(keys) and all(k in sent for k in keys)


def check_orderflow(symbol, now_ms, sent, qvol, tcount):
    out = []
    m1 = None
    for tf, cfg in OF_TF.items():
        mins = cfg["minutes"]
        ms = mins * 60_000
        start_ms = now_ms // ms * ms
        progress = (now_ms - start_ms) / ms
        if progress < cfg["min_progress"]:
            continue
        if tf == "1d" and qvol < DAILY_MIN_QUOTE_VOL:
            continue

        hist = get_klines(symbol, tf, 21)
        if not isinstance(hist, list) or len(hist) < 4:
            continue
        if int(hist[-1][0]) != start_ms:
            continue

        if cfg["sub"] == "1m":
            if m1 is None:
                m1 = get_klines(symbol, "1m", 250)
            allsub, sub_min = m1, 1
        else:
            allsub = get_klines(symbol, cfg["sub"], 300, start_ms=start_ms)
            sub_min = int(cfg["sub"][:-1])
        if not isinstance(allsub, list) or not allsub:
            continue
        sub = [k for k in allsub if int(k[0]) >= start_ms]
        if len(sub) < max(5, int(progress * mins / sub_min * 0.9)):
            continue

        bar = hist[-1]
        hi, lo = float(bar[2]), float(bar[3])
        if hi <= lo:
            continue
        est = eval_setups(hist, build_profile(sub, lo, hi, OF_BUCKETS), tf, progress)
        if not est:
            continue
        found, info = est
        if not (found["BULL"] or found["BEAR"]):
            continue

        mode = "est"
        if cfg["verify"]:
            if all_sent(sent, symbol, tf, start_ms, found):
                continue
            real = verify_real(symbol, hist, start_ms, now_ms, tf, progress, tcount.get(symbol, 0))
            if real is None:
                stats["unverified"] += 1          # تصدیق ممکن نہیں: تخمینی الرٹ ("est")
            else:
                found, info = real
                if not (found["BULL"] or found["BEAR"]):
                    stats["rejected"] += 1
                    print(f"   OF {symbol} {tf}: تخمینے سے بنا، اصل ڈیٹا پر رد")
                    continue
                mode = "real"
                stats["confirmed"] += 1

        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        print(f"   OF {symbol} {tf} ({progress:.0%}) [{mode}]: {info} → {', '.join(tags)}")
        for side, names in found.items():
            if names:
                out.append((tf, side, float(bar[4]), names, start_ms, mode))
    return out


def run_orderflow(symbols, qvol, tcount):
    now_ms = int(time.time() * 1000)
    print(f"\nآرڈر فلو: {len(symbols)} کوائنز | ٹائم فریم {list(OF_TF)}")

    cutoff = now_ms - 2 * 24 * 3600 * 1000
    state = {k: v for k, v in load_json(OF_STATE_FILE).items()
             if isinstance(v, (int, float)) and v >= cutoff}

    def work(sym):
        try:
            return sym, check_orderflow(sym, now_ms, state, qvol.get(sym, 0), tcount)
        except Exception as e:
            print(f"   OF {sym} error: {e}")
            return sym, []

    alerts = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for sym, res in ex.map(work, symbols):
            for tf, side, price, names, start_ms, mode in res:
                new = [n for n in names if f"{sym}|{tf}|{side}|{start_ms}|{n}" not in state]
                if new:
                    alerts.setdefault((tf, side), []).append((sym, price, new, start_ms, mode))

    if not alerts:
        print("آرڈر فلو: کوئی نیا الرٹ نہیں")
        save_json(OF_STATE_FILE, state)
        return

    to_track = []
    for (tf, side), items in sorted(alerts.items()):
        name = "Bullish" if side == "BULL" else "Bearish"
        for i in range(0, len(items), NTFY_CHUNK):
            chunk = items[i:i + NTFY_CHUNK]
            lines = [f"{s.replace('USDT', '')} {fmt_price(p)} | {' + '.join(new)} [{mode}]"
                     for s, p, new, _, mode in chunk]
            sent = post_ntfy(f"{tf.upper()} {name} Order Flow", "\n".join(lines),
                             tags="green_circle" if side == "BULL" else "red_circle",
                             url=NFTY_OF_URL)
            if sent:
                for s, p, new, start_ms, mode in chunk:
                    for n in new:
                        state[f"{s}|{tf}|{side}|{start_ms}|{n}"] = start_ms
                    to_track.append((tf, side, s, p, new, start_ms, mode))
    save_json(OF_STATE_FILE, state)
    try:
        register_of_trades(to_track, now_ms)
    except Exception as e:
        print(f"ٹریکر رجسٹریشن error: {e}")


# ============================================================
# 📈 ٹریکر: ہر بھیجے گئے الرٹ کی پیمائش
#    - الرٹ کے وقت کی قیمت entry مانی جاتی ہے
#    - مقررہ وقت (1h→6 گھنٹے، 4h→24 گھنٹے، ڈیلی→72 گھنٹے) گزرنے پر نتیجہ نکلتا ہے:
#        MFE = سازگار سمت میں زیادہ سے 
#        MAE = مخالف سمت میں زیادہ سے زیادہ حرکت (fakeout یہاں نظر آتا ہے)
#        end = مدت کے اختتام پر سمت کے لحاظ سے منافع/نقصان
#    - SL/TP نہیں، اس لیے fakeout سے ٹریڈ "ماری" نہیں جاتی
#    - ایک بار (sym, tf, side) کا ایک ہی اندراج؛ بعد میں آنے والے سیٹ اپس اسی میں جمع ہوتے ہیں
# ============================================================
def load_book():
    d = load_json(TRADES_FILE)
    # پرانی سٹریٹجی کی ٹریڈز (جن میں "tf" نہیں) چھوڑ دی جاتی ہیں
    return {"open": [t for t in d.get("open", []) if "tf" in t],
            "closed": [t for t in d.get("closed", []) if "tf" in t],
            "last_report_day": d.get("last_report_day")}


def register_of_trades(entries, now_ms):
    if not entries:
        return
    book = load_book()
    idx = {t["id"]: t for t in book["open"]}
    for tf, side, sym, price, names, start_ms, mode in entries:
        tid = f"{sym}|{tf}|{side}|{start_ms}"
        if tid in idx:
            for n in names:
                if n not in idx[tid]["setups"]:
                    idx[tid]["setups"].append(n)
            continue
        tr = {
            "id": tid, "symbol": sym, "tf": tf, "side": side,
            "setups": list(names), "mode": mode, "entry": price,
            "bar_start_ms": start_ms, "opened_ms": now_ms,
            "expires_ms": now_ms + OF_TRACK[tf]["hours"] * 3600 * 1000,
        }
        book["open"].append(tr)
        idx[tid] = tr
    save_json(TRADES_FILE, book)


def evaluate_of_trade(tr, now_ms):
    """مدت پوری ہو تو {mfe_pct, mae_pct, ret_pct, exit, closed_ms}؛ ورنہ None؛ ڈیٹا کبھی نہ ملے تو "DROP"۔"""
    if now_ms < tr["expires_ms"]:
        return None
    cfg = OF_TRACK[tr["tf"]]
    step_ms = cfg["step_ms"]
    first = (tr["opened_ms"] // step_ms + 1) * step_ms      # الرٹ کے بعد پہلی مکمل کینڈل
    kl = get_klines(tr["symbol"], cfg["step"], 1000, start_ms=first, end_ms=tr["expires_ms"])
    if not isinstance(kl, list):
        return None
    kl = [k for k in kl if int(k[6]) <= tr["expires_ms"]]
    expected = (tr["expires_ms"] - first) // step_ms
    if len(kl) < max(1, int(expected * 0.8)):
        return "DROP" if now_ms > tr["expires_ms"] + 2 * 86_400_000 else None

    entry = tr["entry"]
    top = max(float(k[2]) for k in kl)
    bottom = min(float(k[3]) for k in kl)
    last = float(kl[-1][4])
    if tr["side"] == "BULL":
        mfe, mae, ret = (top - entry) / entry * 100, (entry - bottom) / entry * 100, (last - entry) / entry * 100
    else:
        mfe, mae, ret = (entry - bottom) / entry * 100, (top - entry) / entry * 100, (entry - last) / entry * 100
    return {"mfe_pct": max(mfe, 0.0), "mae_pct": max(mae, 0.0), "ret_pct": ret,
            "exit": last, "closed_ms": now_ms}


def group_line(label, rows):
    n = len(rows)
    end_ok = sum(1 for r in rows if r["ret_pct"] > 0)
    edge = sum(1 for r in rows if r["mfe_pct"] > r["mae_pct"])
    mfe = statistics.mean(r["mfe_pct"] for r in rows)
    mae = statistics.mean(r["mae_pct"] for r in rows)
    star = "*" if n < SMALL_SAMPLE else ""
    return (f"{label} n={n}{star}: end+ {end_ok / n * 100:.0f}% | MFE>MAE {edge / n * 100:.0f}% | "
            f"MFE +{mfe:.1f}% / MAE -{mae:.1f}%")


def summary_text(closed):
    lines = []
    for tf in OF_TF:
        for mode in ("real", "est"):
            rows = [t for t in closed if t["tf"] == tf and t.get("mode") == mode][-SUMMARY_LAST_N:]
            if rows:
                lines.append(group_line(f"{tf} {mode}", rows))
    return "\n".join(lines) or "No closed trades yet"


def setup_report(closed):
    names = sorted({n for t in closed for n in t["setups"]})
    lines = ["By setup (all kept results, * = small sample)"]
    for tf in OF_TF:
        for n in names:
            rows = [t for t in closed if t["tf"] == tf and n in t["setups"]]
            if rows:
                lines.append(group_line(f"{tf} {n}", rows))
    lines += ["", "By timeframe / mode (last results)", summary_text(closed)]
    return "\n".join(lines)


def result_line(t):
    side = "Bull" if t["side"] == "BULL" else "Bear"
    return (f"{t['symbol'].replace('USDT', '')} {t['tf']} {side} [{t['mode']}] "
            f"{'+'.join(t['setups'])} | MFE +{t['mfe_pct']:.1f}% MAE -{t['mae_pct']:.1f}% | "
            f"end {t['ret_pct']:+.1f}%")


def track_of_trades():
    """مدت پوری کرنے والی ٹریڈز کا نتیجہ نکالیں، اطلاع دیں، روزانہ سیٹ اپ رپورٹ بھیجیں۔"""
    book = load_book()
    now_ms = int(time.time() * 1000)
    print(f"\nٹریکر: {len(book['open'])} کھلی | {len(book['closed'])} بند")

    still, newly = [], []
    for tr in book["open"]:
        try:
            res = evaluate_of_trade(tr, now_ms)
        except Exception as e:
            print(f"   {tr['symbol']} ٹریکر error: {e}")
            res = None
        if res is None:
            still.append(tr)
        elif res == "DROP":
            print(f"   {tr['symbol']} {tr['tf']}: ڈیٹا نہیں ملا، اندراج ہٹا دیا")
        else:
            newly.append(dict(tr, **res))
            print(f"   {result_line(newly[-1])}")

    book["open"] = still
    book["closed"] = (book["closed"] + newly)[-MAX_CLOSED_KEEP:]

    if newly:
        lines = [result_line(t) for t in newly[:15]]
        if len(newly) > 15:
            lines.append(f"+{len(newly) - 15} more")
        body = "\n".join(lines) + f"\nopen now: {len(still)}\n" + summary_text(book["closed"])
        post_ntfy(f"OF results: {len(newly)} closed", body, tags="bar_chart",
                  url=NFTY_OF_URL, priority="default")

    today = now_ms // 86_400_000
    if book["closed"] and book.get("last_report_day") != today:
        if post_ntfy("OF daily report", setup_report(book["closed"]), tags="chart_with_upwards_trend",
                     url=NFTY_OF_URL, priority="default"):
            book["last_report_day"] = today

    save_json(TRADES_FILE, book)


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | آرڈر فلو سکینر | ٹاپ {OF_COIN_COUNT} کوائنز")
    print("=" * 60)

    symbols, qvol, tcount = get_top_markets()
    if not symbols:
        print("مارکیٹس نہیں ملیں")
        return

    # 1) آرڈر فلو الرٹس (جاری 1h/4h/ڈیلی بار، کلوزنگ کی شرط نہیں)
    try:
        run_orderflow(symbols, qvol, tcount)
    except Exception as e:
        print(f"آرڈر فلو error: {e}")

    # 2) ٹریکر: مدت پوری کرنے والے الرٹس کا نتیجہ
    try:
        track_of_trades()
    except Exception as e:
        print(f"ٹریکر error: {e}")

    print(f"\nاعداد: {stats}")
    print(f"مکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
    
