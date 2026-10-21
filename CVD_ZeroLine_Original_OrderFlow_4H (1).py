#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""صرف TradingView-style CVD Zero-Line اور اصل executed aggTrades Order Flow (صرف 4h)."""
import os, json, time, statistics, requests, numpy as np
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "https://data-api.binance.vision"
NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    raise SystemExit("NFTY_URL environment variable سیٹ کریں")
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL
session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0}

# CVD کی TradingView-style اصل سیٹنگز/منطق برقرار
CVD_TF = "1h"
CVD_LTF = "1m"
CVD_COIN_COUNT = 300
CVD_HISTORY_HOURS = 24
CVD_ZERO_TOUCH_TOL = 0.03
CVD_FRESH_MIN = 20
CVD_SKIP_FIRST_MIN = 60
CVD_RUN_WINDOW_HOURS = 4
CVD_MOVED_RANGE_MULT = 2.5
CVD_MIN_RETRACE = 0.35
CVD_THREADS = 12
CVD_NTFY_MAX_BYTES = 3500
CVD_STATE_FILE = "cvd_zero_touch_state.json"

# صرف 4h original order-flow scan؛ setup thresholds اصل فائل جیسے ہی
OF_TF = {"4h": {"minutes": 240, "min_delta_pct": 5.0}}
OF_MIN_PROGRESS = 0.10
OF_MAX_MOVE_FROM_OPEN_PCT = 0.45
OF_MAX_RANGE_AVG_MULT = 0.85
OF_MAX_LOOKBACK_MOVE_PCT = 1.20
OF_BUCKETS = 40
OF_LOOKBACK = 12
OF_POC_BULL = 0.60
OF_POC_BEAR = 0.40
OF_CLOSE_BULL = 0.65
OF_CLOSE_BEAR = 0.35
OF_VOL_MULT = 1.0
OF_ABS_DELTA = 4.0
OF_ABS_VOL = 1.2
OF_STACK_RATIO = 3.0
OF_STACK_N = 3
OF_STACK_MIN_DELTA = 3.0
OF_EXH_THIN = 0.05
OF_DIV_MIN = 0.5
OF_STATE_FILE = "orderflow_4h_state.json"
MAX_TRADE_PAGES = 80
OF_COIN_COUNT = 60


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


def get_cvd_symbols(limit):
    """ٹاپ والیوم USDT کوائنز (کم از کم limit)؛ حصہ A/B کی MAX_SYMBOLS سے آزاد۔"""
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


def tv_cvd_path(bars):
    """TradingView CVD کی نقل: bars = [(t, o, h, l, c, v)]۔ واپسی: ہر 1m بار کے بعد CVD کی فہرست۔
    upDnIntrabarVolumes() کی ترتیب: close>open | close<open | close>prev close | close<prev close |
    پچھلی سمت | close==open (اوپر)۔"""
    path, cvd, prev_dir, prev_close = [], 0.0, 0, None
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
        prev_dir, prev_close = d, c
    return path


def check_cvd_zero_touch(symbol, now_ms):
    """1h CVD zero-line CROSS یا TOUCH — واپسی: (result_dict | None, reason)

    CVD TradingView کے طریقے سے (1m intrabar کینڈل رنگ)، ہر UTC دن reset۔
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
    if now_ms - day_start_ms < CVD_SKIP_FIRST_MIN * minute_ms:
        return None, "first_hour"

    kl = fetch_1m_since(symbol, day_start_ms, now_ms)
    if not isinstance(kl, list) or not kl:
        return None, "no_data"
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
        return None, "few_bars"
    # ڈیٹا میں خلا ہو (آخری بار بہت پرانی) تو سگنل نہیں
    if now_ms - bars[-1][0] > 3 * minute_ms:
        return None, "stale_data"

    # پیمانہ: پچھلے بند گھنٹوں سے اوسط والیوم اور اوسط 1h رینج
    hist = get_klines(symbol, CVD_TF, CVD_HISTORY_HOURS + 1)
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

    path = tv_cvd_path(bars)
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


def run_cvd_zero_touch(symbols):
    """آزاد اور علیحدہ 1h CVD zero-touch/cross scan؛ اس کا نتیجہ OF/TP-SL signals کو نہیں بدلتا۔"""
    now_ms = int(time.time() * 1000)
    cvd_symbols = get_cvd_symbols(CVD_COIN_COUNT)
    if not cvd_symbols:
        cvd_symbols = list(symbols)
    if len(cvd_symbols) < CVD_COIN_COUNT:
        print(f"⚠️ CVD: صرف {len(cvd_symbols)} کوائنز دستیاب (ہدف {CVD_COIN_COUNT})")
    print(f"\nCVD Zero Touch/Cross: {len(cvd_symbols)} symbols | 1h CVD | independent module")

    state = load_json(CVD_STATE_FILE)
    if not isinstance(state, dict):
        state = {}
    cutoff = now_ms - 7 * 24 * 3600 * 1000
    state = {k: v for k, v in state.items() if isinstance(v, (int, float)) and v >= cutoff}

    found, reasons = [], {}
    workers = min(CVD_THREADS, max(1, len(cvd_symbols)))
    with ThreadPoolExecutor(max_workers=workers) as ex:
        futures = {ex.submit(check_cvd_zero_touch, sym, now_ms): sym for sym in cvd_symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                result, reason = future.result()
            except Exception as e:
                print(f"   CVD {sym} error: {e}")
                continue
            reasons[reason] = reasons.get(reason, 0) + 1
            if not result:
                continue
            key = f"{sym}|1h|{result['hour_start']}|{result['event']}"
            if key in state:
                continue
            found.append((key, result))

    print(f"CVD نتائج: {reasons}")
    if not found:
        print("CVD Zero Touch/Cross: no new signal")
        save_json(CVD_STATE_FILE, state)
        return

    groups = {}
    for key, r in found:
        groups.setdefault(r["event"], []).append((key, r))

    for event, items in sorted(groups.items()):
        tag = ("green_circle" if event == "CROSS UP" else
               "red_circle" if event == "CROSS DOWN" else "large_purple_circle")
        chunk, chunk_keys, size = [], [], 0

        def flush():
            nonlocal chunk, chunk_keys, size
            if not chunk:
                return
            if post_ntfy(f"1H CVD {event}", "\n".join(chunk), tags=tag, url=NFTY_URL):
                for k in chunk_keys:
                    state[k] = now_ms
            else:
                print("CVD alert send failed; state not marked, will retry next run")
            chunk, chunk_keys, size = [], [], 0

        for key, r in items:
            ctx = (f"4h range {r['range_pct']:.1f}% pullback {r['pullback']:.0%}"
                   if r["pullback"] is not None else "quiet market")
            line = (f"{r['symbol'].replace('USDT', '')} @ {fmt_price(r['price'])} | "
                    f"{r['ago_min']:.0f}m ago | CVD {r['cvd']:+.4g} (1h open {r['ref']:+.4g}) | "
                    f"tol ±{r['tolerance']:.3g} | 1h delta {r['hour_delta']:+.4g} | {ctx}")
            if size + len(line.encode("utf-8")) + 1 > CVD_NTFY_MAX_BYTES:
                flush()
            chunk.append(line)
            chunk_keys.append(key)
            size += len(line.encode("utf-8")) + 1
        flush()
        time.sleep(0.3)

    save_json(CVD_STATE_FILE, state)



def main():
    now_ms = int(time.time() * 1000)
    print("=" * 58)
    print("CVD Zero-Line + Original Order Flow | Order Flow timeframe: 4H only")
    print("CVD TradingView-style logic unchanged; other scanner/strategy/tracker removed")
    print("=" * 58)
    try:
        run_cvd_zero_touch([])
    except Exception as e:
        print(f"CVD error: {e}")
    try:
        # Top-volume USDT markets; CVD scanner still independently uses its original 300-symbol selection.
        syms = get_cvd_symbols(OF_COIN_COUNT)
        run_orderflow(syms)
    except Exception as e:
        print(f"4H Order Flow error: {e}")
    print(f"Finished. API failures={stats['failed']} rate-limits={stats['rate_limited']}")

if __name__ == "__main__":
    main()
