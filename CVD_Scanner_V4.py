import os
import json
import time
import bisect
import requests
import numpy as np
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CVD اسکینر — دو ماڈیول، دونوں TradingView کے
# "CVD - Cumulative Volume Delta Candles" انڈیکیٹر کے طریقے پر:
#
#   1) CVD زیرو لائن: 1h CVD صفر کو CROSS یا TOUCH کرے (+ بھاگتی مارکیٹ کا فلٹر)
#   2) CVD کینڈلز (30m اور 1h) پر SMC:
#        - Fair Value Gap بنے اور CVD دوبارہ اس خلا میں داخل ہو  -> الرٹ
#        - Liquidity Sweep: CVD پچھلی swing انتہا توڑ کر واپس اندر بند ہو -> الرٹ
#      یہ پرائس ایکشن سے بالکل آزاد ہے؛ صرف CVD کینڈلز کی قدریں۔
#
# CVD کا طریقہ (انڈیکیٹر کی نقل):
#   - 1m intrabars: close>open اوپر | close<open نیچے | برابر ہو تو close پچھلی close سے
#     موازنہ | پھر پچھلی سمت | آخر میں اوپر؛ کینڈل کا پورا والیوم اس سمت میں
#   - ہر UTC دن کے آغاز پر CVD ری سیٹ (انڈیکیٹر کا ڈیفالٹ: 1D)
#   - CVD کینڈل: open = پچھلی CVD، close = open + delta،
#                high = open + سب سے بڑا اوپر 1m والیوم، low = open + سب سے بڑا نیچے 1m والیوم
# ============================================================
BASE_URL = "https://data-api.binance.vision"

# ---------- مشترک ----------
CVD_LTF = "1m"                # intrabar ٹائم فریم
CVD_COIN_COUNT = 300          # کم از کم 300 کوائنز (ٹاپ والیوم USDT مارکیٹس)
CVD_FRESH_MIN = 20            # ایونٹ آخری اتنے منٹ میں ہوا ہو (سکین 15 منٹ پر ہو تو 20 رکھیں)
CVD_SKIP_FIRST_MIN = 60       # UTC دن کا پہلا گھنٹہ چھوڑ دیں (CVD صفر سے شروع ہوتی ہے)
CVD_THREADS = 12
CVD_NTFY_MAX_BYTES = 3500     # ایک ntfy پیغام کی حد (4096 سے کم)
CVD_STATE_FILE = "cvd_zero_touch_state.json"

# ---------- 1) زیرو لائن (پرانی منطق، بغیر تبدیلی) ----------
CVD_TF = "1h"                 # اوسط والیوم/رینج کے لیے
CVD_HISTORY_HOURS = 24        # اوسط 1h والیوم/رینج کے لیے پچھلے اتنے بند گھنٹے
CVD_ZERO_TOUCH_TOL = 0.03     # zero پٹی = اوسط hourly والیوم کا 3%
CVD_RUN_WINDOW_HOURS = 4      # "مارکیٹ پہلے ہی چل چکی" جانچنے کی ونڈو
CVD_MOVED_RANGE_MULT = 2.5    # 4h رینج ≥ اوسط 1h رینج کا 2.5 گنا ہو تو مارکیٹ "چل چکی" سمجھیں
CVD_MIN_RETRACE = 0.35        # چل چکی مارکیٹ میں انتہا سے کم از کم 35% واپسی ہو تو ریٹریسمنٹ، ورنہ بھاگتی مارکیٹ

# ---------- 2) FVG ری انٹری + Liquidity Sweep (CVD کینڈلز پر) ----------
CVD_SMC_TFS = {"30m": 30 * 60_000, "1h": 60 * 60_000}
CVD_FVG_MIN_GAP = 0.03        # خلا کم از کم اس TF کی اوسط کینڈل والیوم کا 3% ہو (چھوٹے خلا نظرانداز)
CVD_SWING_N = 2               # swing: دونوں طرف کی اتنی کینڈلز سے اونچی/نیچی
CVD_SWEEP_MIN_PIERCE = 0.01   # انتہا توڑنے کی کم از کم گہرائی: اوسط کینڈل والیوم کا 1%

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    raise SystemExit(1)

session = requests.Session()
stats = {"rate_limited": 0, "failed": 0}


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


def get_klines_from(symbol, interval, start_ms, limit):
    return safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": interval,
                            "startTime": start_ms, "limit": limit})


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
# 📋 کوائنز: ٹاپ والیوم USDT مارکیٹس
# ============================================================
def get_cvd_symbols(limit):
    data = safe_get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=30)
    if not data or "symbols" not in data:
        return []
    skip = {"USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
            "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY", "USDTBIDR"}
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

    ticker = safe_get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=30)
    if not isinstance(ticker, list):
        return sorted(valid)[:limit]
    volumes = []
    for t in ticker:
        sym = t.get("symbol")
        if sym in valid:
            try:
                volumes.append((sym, float(t.get("quoteVolume", 0))))
            except (TypeError, ValueError):
                continue
    volumes.sort(key=lambda x: x[1], reverse=True)
    return [s for s, _ in volumes[:limit]]


# ============================================================
# 🟣 CVD بنیاد: TradingView کی نقل
# ============================================================
def fetch_1m_since(symbol, start_ms, now_ms):
    """start_ms سے اب تک کی 1m klines (1000 فی درخواست، زیادہ سے زیادہ 3 صفحے = 3000 منٹ)۔"""
    out, cur = [], start_ms
    for _ in range(3):
        kl = get_klines_from(symbol, CVD_LTF, cur, 1000)
        if not isinstance(kl, list):
            return None if not out else out
        if not kl:
            break
        out.extend(kl)
        if len(kl) < 1000:
            break
        cur = int(kl[-1][0]) + 60_000
        if cur > now_ms:
            break
    return out


def tv_cvd_series(bars):
    """TradingView CVD کی نقل: bars = [(t, o, h, l, c, v)]۔
    واپسی: (path, signed) — path: ہر 1m بار کے بعد CVD، signed: ہر بار کا سمت والا والیوم (+اوپر / -نیچے)۔
    upDnIntrabarVolumes() کی ترتیب: close>open | close<open | close>prev close | close<prev close |
    پچھلی سمت | close==open (اوپر)۔"""
    path, signed, cvd, prev_dir, prev_close = [], [], 0.0, 0, None
    for t, o, h, l, c, v in bars:
        if c > o:
            d = 1
        elif c < o:
            d = -1
        elif prev_close is not None and c > prev_close:
            d = 1
        elif prev_close is not None and c < prev_close:
            d = -1
        elif prev_dir != 0:
            d = prev_dir
        else:
            d = 1
        cvd += d * v
        path.append(cvd)
        signed.append(d * v)
        prev_dir, prev_close = d, c
    return path, signed


def tv_cvd_path(bars):
    return tv_cvd_series(bars)[0]


def build_cvd_candles(bars, signed, path, tf_ms, now_ms):
    """1m بارز سے TF کی CVD کینڈلز (انڈیکیٹر کے فارمولے سے)۔ بنتی ہوئی کینڈل بھی شامل، closed کا جھنڈا الگ۔
    hi/lo: wick کو open/close سے باہر رکھنے کے لیے max/min لیا جاتا ہے۔"""
    candles, cur, start = [], None, None
    for i, b in enumerate(bars):
        t = b[0]
        bs = t // tf_ms * tf_ms
        if cur is None or bs != start:
            if cur is not None:
                candles.append(cur)
            start = bs
            o = path[i - 1] if i > 0 else 0.0
            cur = {"start": bs, "end": bs + tf_ms, "o": o, "c": o, "max_up": 0.0,
                   "max_dn": 0.0, "vol": 0.0, "first_i": i, "last_i": i}
        s = signed[i]
        cur["c"] = path[i]
        cur["vol"] += b[5]
        cur["last_i"] = i
        if s > 0:
            cur["max_up"] = max(cur["max_up"], s)
        elif s < 0:
            cur["max_dn"] = min(cur["max_dn"], s)
    if cur is not None:
        candles.append(cur)
    for c in candles:
        c["h"] = max(c["o"] + c["max_up"], c["o"], c["c"])
        c["l"] = min(c["o"] + c["max_dn"], c["o"], c["c"])
        c["closed"] = c["end"] <= now_ms
    return candles


# ============================================================
# 1) CVD زیرو لائن — V3 کی منطق (بغیر تبدیلی)
# ============================================================
def cvd_zero_line_event(symbol, bars, path, hist, now_ms):
    """CROSS یا TOUCH — واپسی: (result_dict | None, reason)
      CROSS : CVD صفر پار کر کے دوسری طرف چلی گئی (مثبت↔منفی)
      TOUCH : CVD صفر کی پٹی (±tol) میں داخل ہوئی (پچھلا منٹ پٹی سے باہر تھا)
    ایونٹ آخری CVD_FRESH_MIN منٹ کے اندر ہونا لازم ہے، اور دن کے پہلے گھنٹے میں نہیں۔

    مارکیٹ فلٹر: پچھلے 4h میں رینج اوسط 1h رینج کے CVD_MOVED_RANGE_MULT گنا سے بڑی ہو
    (مارکیٹ چل چکی) اور قیمت اپنی انتہا کے قریب ہو (واپسی < CVD_MIN_RETRACE) تو سگنل نہیں۔
    چل چکی مگر انتہا سے کافی واپس آ چکی ہو (ریٹریسمنٹ) تو سگنل ملے گا۔
    """
    minute_ms = 60_000
    hour_ms = 3_600_000
    day_start_ms = now_ms // 86_400_000 * 86_400_000

    if not isinstance(hist, list) or len(hist) < 7:
        return None, "no_hist"
    hist = hist[:-1]            # بنتا ہوا گھنٹہ نکال دیں
    try:
        avg_vol = float(np.mean([float(k[5]) for k in hist]))
        ranges = [(float(k[2]) - float(k[3])) / float(k[4]) * 100.0 for k in hist if float(k[4]) > 0]
    except (TypeError, ValueError, IndexError):
        return None, "bad_hist"
    avg_range_pct = float(np.mean(ranges)) if ranges else 0.0
    if avg_vol <= 0 or avg_range_pct <= 0:
        return None, "no_scale"
    tol = max(avg_vol * CVD_ZERO_TOUCH_TOL, 1e-12)

    times = [b[0] for b in bars]
    min_t = max(now_ms - CVD_FRESH_MIN * minute_ms, day_start_ms + CVD_SKIP_FIRST_MIN * minute_ms)

    event, ev_i = None, None
    for i in range(len(path) - 1, 0, -1):
        if times[i] < min_t:
            break
        prev, cur = path[i - 1], path[i]
        if prev * cur < 0:
            event = "CROSS UP" if cur > 0 else "CROSS DOWN"
            ev_i = i
            break
        if abs(cur) <= tol and abs(prev) > tol:
            event = "TOUCH FROM ABOVE" if prev > 0 else "TOUCH FROM BELOW"
            ev_i = i
            break
    if not event:
        return None, "no_event"

    # ---- مارکیٹ پہلے ہی بھاگ چکی ہو تو سگنل نہیں؛ ریٹریسمنٹ میں ٹھیک ہے ----
    price = bars[-1][4]
    win = [b for b in bars if b[0] >= now_ms - CVD_RUN_WINDOW_HOURS * hour_ms]
    if len(win) < 30:
        return None, "few_window"
    hi = max(b[2] for b in win)
    lo = min(b[3] for b in win)
    window_range_pct = (hi - lo) / lo * 100.0 if lo > 0 else 0.0
    pullback = None
    if hi > lo and window_range_pct >= CVD_MOVED_RANGE_MULT * avg_range_pct:
        idx_hi = max(range(len(win)), key=lambda i: win[i][2])
        idx_lo = min(range(len(win)), key=lambda i: win[i][3])
        if idx_hi > idx_lo:     # اوپر کی چال: واپسی = high سے نیچے آنا
            pullback = (hi - price) / (hi - lo)
        else:                   # نیچے کی چال: واپسی = low سے اوپر آنا
            pullback = (price - lo) / (hi - lo)
        if pullback < CVD_MIN_RETRACE:
            return None, "running"

    ev_ms = times[ev_i]
    hour_open_ms = ev_ms // hour_ms * hour_ms
    ref = 0.0
    for t, v in zip(times, path):
        if t < hour_open_ms:
            ref = v
        else:
            break
    return {"symbol": symbol, "price": price, "event": event, "cvd": float(path[-1]),
            "ref": float(ref), "tolerance": tol, "hour_start": hour_open_ms,
            "event_ms": ev_ms, "ago_min": (now_ms - ev_ms) / minute_ms,
            "hour_delta": float(path[-1] - ref), "range_pct": window_range_pct,
            "pullback": pullback}, "ok"


# ============================================================
# 2) CVD کینڈلز پر SMC: FVG ری انٹری + Liquidity Sweep
# ============================================================
def _zone_sign(lo, hi):
    return "positive" if (lo + hi) / 2.0 > 0 else "negative"


def find_fvg_reentry(candles, times, path, now_ms, min_gap):
    """بند کینڈلز میں 3 کینڈل FVG ڈھونڈیں؛ بننے کے بعد CVD پہلی بار خلا میں داخل ہو
    اور وہ داخلہ آخری CVD_FRESH_MIN منٹ میں ہوا ہو تو ایونٹ۔
      Bullish FVG : کینڈل1.high < کینڈل3.low   -> خلا [c1.high, c3.low]؛ CVD اوپر سے نیچے آ کر c3.low چھوئے
      Bearish FVG : کینڈل1.low  > کینڈل3.high  -> خلا [c3.high, c1.low]؛ CVD نیچے سے اوپر جا کر c3.high چھوئے
    ہر FVG کا صرف پہلا داخلہ گنا جاتا ہے۔ واپسی: ایونٹس کی فہرست۔"""
    closed = [c for c in candles if c["closed"]]
    fresh_t = now_ms - CVD_FRESH_MIN * 60_000
    out = []
    for i in range(len(closed) - 2):
        c1, c3 = closed[i], closed[i + 2]
        if c3["l"] - c1["h"] >= min_gap:
            direction, lo, hi = "BULLISH", c1["h"], c3["l"]
        elif c1["l"] - c3["h"] >= min_gap:
            direction, lo, hi = "BEARISH", c3["h"], c1["l"]
        else:
            continue
        j0 = bisect.bisect_left(times, c3["end"])
        entry = None
        for j in range(j0, len(path)):
            if (direction == "BULLISH" and path[j] <= hi) or (direction == "BEARISH" and path[j] >= lo):
                entry = j
                break
        if entry is None or times[entry] < fresh_t:
            continue
        out.append({"dir": direction, "lo": lo, "hi": hi, "zone": _zone_sign(lo, hi),
                    "c3_start": c3["start"], "entry_ms": times[entry],
                    "ago_min": (now_ms - times[entry]) / 60_000, "cvd": float(path[-1])})
    return out


def find_liquidity_sweep(candles, now_ms, min_pierce, n=CVD_SWING_N):
    """آخری بند CVD کینڈل کے لیے sweep:
      Bearish sweep: پہلے سے موجود swing high توڑا (high > level) مگر close واپس نیچے (close < level)
      Bullish sweep: swing low توڑا (low < level) مگر close واپس اوپر (close > level)
    swing: دونوں طرف n کینڈلز سے اونچی/نیچی؛ اور level پہلے کسی کینڈل نے نہ توڑا ہو۔
    الرٹ تب جب کینڈل آخری CVD_FRESH_MIN منٹ میں بند ہوئی ہو۔ واپسی: dict | None"""
    closed = [c for c in candles if c["closed"]]
    k = len(closed) - 1
    if k < 2 * n + 1:
        return None
    last = closed[k]
    if not (now_ms - CVD_FRESH_MIN * 60_000 <= last["end"] <= now_ms):
        return None
    hi = [c["h"] for c in closed]
    lo = [c["l"] for c in closed]

    for p in range(k - n - 1, n - 1, -1):
        # Bearish: swing high
        lvl = hi[p]
        if (lvl > max(hi[p - n:p]) and lvl > max(hi[p + 1:p + n + 1])
                and max(hi[p + 1:k]) <= lvl
                and last["h"] > lvl + min_pierce and last["c"] < lvl):
            return {"dir": "BEARISH", "level": lvl, "close": last["c"], "extreme": last["h"],
                    "zone": "positive" if lvl > 0 else "negative", "candle_start": last["start"]}
        # Bullish: swing low
        lvl = lo[p]
        if (lvl < min(lo[p - n:p]) and lvl < min(lo[p + 1:p + n + 1])
                and min(lo[p + 1:k]) >= lvl
                and last["l"] < lvl - min_pierce and last["c"] > lvl):
            return {"dir": "BULLISH", "level": lvl, "close": last["c"], "extreme": last["l"],
                    "zone": "positive" if lvl > 0 else "negative", "candle_start": last["start"]}
    return None


# ============================================================
# 🔎 ایک کوائن: ایک بار ڈیٹا، تینوں چیک
# ============================================================
def analyze_cvd_symbol(symbol, now_ms):
    """واپسی: (items, reasons)۔ items = [(key, title, tags, line)]"""
    minute_ms = 60_000
    day_start_ms = now_ms // 86_400_000 * 86_400_000
    if now_ms - day_start_ms < CVD_SKIP_FIRST_MIN * minute_ms:
        return [], ["first_hour"]

    kl = fetch_1m_since(symbol, day_start_ms, now_ms)
    if not isinstance(kl, list) or not kl:
        return [], ["no_data"]
    bars = []
    for k in kl:
        try:
            t = int(k[0])
            if t > now_ms:
                continue
            bars.append((t, float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[5])))
        except (TypeError, ValueError, IndexError):
            continue
    if len(bars) < 30:
        return [], ["few_bars"]
    # ڈیٹا میں خلا ہو (آخری بار بہت پرانی) تو سگنل نہیں
    if now_ms - bars[-1][0] > 3 * minute_ms:
        return [], ["stale_data"]

    path, signed = tv_cvd_series(bars)
    times = [b[0] for b in bars]
    price = bars[-1][4]
    base = symbol.replace("USDT", "")
    items, reasons = [], []

    # ---- 1) زیرو لائن ----
    hist = get_klines(symbol, CVD_TF, CVD_HISTORY_HOURS + 1)
    zr, reason = cvd_zero_line_event(symbol, bars, path, hist, now_ms)
    reasons.append(reason)
    if zr:
        ctx = (f"4h range {zr['range_pct']:.1f}% pullback {zr['pullback']:.0%}"
               if zr["pullback"] is not None else "quiet market")
        line = (f"{base} @ {fmt_price(zr['price'])} | "
                f"{zr['ago_min']:.0f}m ago | CVD {zr['cvd']:+.4g} (1h open {zr['ref']:+.4g}) | "
                f"tol ±{zr['tolerance']:.3g} | 1h delta {zr['hour_delta']:+.4g} | {ctx}")
        tag = ("green_circle" if zr["event"] == "CROSS UP" else
               "red_circle" if zr["event"] == "CROSS DOWN" else "large_purple_circle")
        items.append((f"{symbol}|1h|{zr['hour_start']}|{zr['event']}", f"1H CVD {zr['event']}", tag, line))

    # ---- 2) SMC: FVG ری انٹری + Sweep ----
    for tf, tf_ms in CVD_SMC_TFS.items():
        candles = build_cvd_candles(bars, signed, path, tf_ms, now_ms)
        closed = [c for c in candles if c["closed"]]
        if len(closed) < 3:
            continue
        mean_vol = float(np.mean([c["vol"] for c in closed]))
        if mean_vol <= 0:
            continue
        tfu = tf.upper()

        # CVD کئی پرانے خلاؤں سے گزرے تو spam نہ ہو: ہر سمت کا صرف تازہ ترین بنا خلا،
        # اور اسی TF کینڈل میں اس سمت کا الرٹ دوبارہ نہیں
        latest = {}
        for f in find_fvg_reentry(candles, times, path, now_ms, mean_vol * CVD_FVG_MIN_GAP):
            if f["dir"] not in latest or f["c3_start"] > latest[f["dir"]]["c3_start"]:
                latest[f["dir"]] = f
        for f in latest.values():
            line = (f"{base} @ {fmt_price(price)} | {tf} | gap {f['lo']:+.4g} to {f['hi']:+.4g} "
                    f"({f['zone']} CVD) | CVD now {f['cvd']:+.4g} | {f['ago_min']:.0f}m ago")
            items.append((f"{symbol}|{tf}|fvg|{f['dir']}|{f['entry_ms'] // tf_ms}",
                          f"{tfu} {f['dir']} CVD FVG RE-ENTRY",
                          "green_circle" if f["dir"] == "BULLISH" else "red_circle", line))

        s = find_liquidity_sweep(candles, now_ms, mean_vol * CVD_SWEEP_MIN_PIERCE)
        if s:
            line = (f"{base} @ {fmt_price(price)} | {tf} | swept {s['level']:+.4g} ({s['zone']} CVD) | "
                    f"extreme {s['extreme']:+.4g} | candle close {s['close']:+.4g}")
            items.append((f"{symbol}|{tf}|sweep|{s['candle_start']}|{s['dir']}",
                          f"{tfu} {s['dir']} CVD LIQUIDITY SWEEP",
                          "green_circle" if s["dir"] == "BULLISH" else "red_circle", line))
    return items, reasons


# ============================================================
# 📲 الرٹس بھیجنا (قسم کے حساب سے گروپ، ntfy کی حد کے مطابق ٹکڑے)
# ============================================================
def send_alerts(items, state, now_ms):
    groups = {}
    for key, title, tags, line in items:
        groups.setdefault((title, tags), []).append((key, line))

    for (title, tags), lst in sorted(groups.items()):
        chunk, chunk_keys, size = [], [], 0

        def flush():
            nonlocal chunk, chunk_keys, size
            if not chunk:
                return
            if post_ntfy(title, "\n".join(chunk), tags=tags, url=NFTY_URL):
                for k in chunk_keys:
                    state[k] = now_ms
            else:
                print(f"{title}: alert send failed; state not marked, will retry next run")
            chunk, chunk_keys, size = [], [], 0

        for key, line in lst:
            n = len(line.encode("utf-8")) + 1
            if size + n > CVD_NTFY_MAX_BYTES:
                flush()
            chunk.append(line)
            chunk_keys.append(key)
            size += n
        flush()
        time.sleep(0.3)


def run_cvd(symbols):
    now_ms = int(time.time() * 1000)
    cvd_symbols = get_cvd_symbols(CVD_COIN_COUNT)
    if not cvd_symbols:
        cvd_symbols = list(symbols or [])
    if len(cvd_symbols) < CVD_COIN_COUNT:
        print(f"⚠️ CVD: صرف {len(cvd_symbols)} کوائنز دستیاب (ہدف {CVD_COIN_COUNT})")
    print(f"\nCVD: {len(cvd_symbols)} symbols | زیرو لائن + FVG ری انٹری + Sweep ({list(CVD_SMC_TFS)})")

    state = load_json(CVD_STATE_FILE)
    if not isinstance(state, dict):
        state = {}
    cutoff = now_ms - 7 * 24 * 3600 * 1000
    state = {k: v for k, v in state.items() if isinstance(v, (int, float)) and v >= cutoff}

    found, reasons = [], {}
    workers = min(CVD_THREADS, max(1, len(cvd_symbols)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(analyze_cvd_symbol, sym, now_ms): sym for sym in cvd_symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                items, rs = future.result()
            except Exception as e:
                print(f"   CVD {sym} error: {e}")
                continue
            for r in rs:
                reasons[r] = reasons.get(r, 0) + 1
            for it in items:
                if it[0] not in state:
                    found.append(it)

    print(f"CVD زیرو لائن نتائج: {reasons} | اعداد: {stats}")
    if not found:
        print("CVD: کوئی نیا سگنل نہیں")
        save_json(CVD_STATE_FILE, state)
        return
    print(f"CVD: {len(found)} نئے سگنل")
    send_alerts(found, state, now_ms)
    save_json(CVD_STATE_FILE, state)


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | {datetime.now(timezone.utc):%m-%d %H:%M} UTC | CVD اسکینر")
    print("=" * 60)
    try:
        run_cvd([])
    except Exception as e:
        print(f"CVD error: {e}")
    print(f"\nمکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
