#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""اصل executed aggTrades Order Flow (صرف 4h) - ٹاپ 300 USDT کوائنز، stablecoins کے بغیر۔

v2 تبدیلیاں:
  1) بڑے کوائنز (OF_MAJORS) کا aggTrades 30 منٹ کے ٹکڑوں میں متوازی ڈاؤنلوڈ ہوتا ہے تاکہ ڈیٹا ادھورا نہ رہے۔
  2) بڑے کوائنز کی anti-chase حدیں ہلکی سی نرم (صرف انہی کی؛ باقی سب کچھ پہلے جیسا)۔
  3) بڑے کوائنز کے لیے تشخیصی لاگ، اور skip ہونے والے symbols کی فہرست۔
"""
import os, json, time, statistics, requests, numpy as np
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://data-api.binance.vision"
NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    raise SystemExit("NFTY_URL environment variable سیٹ کریں")
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL
session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0}
skipped_majors = []   # وہ بڑے کوائنز جن کا ڈیٹا مکمل نہ مل سکا

# ---- Order Flow سیٹنگز (عام کوائنز کے لیے اصل ویلیوز، بغیر تبدیلی) ----
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
OF_COIN_COUNT = 300
OF_THREADS = 8

# ---- بڑے کوائنز: صرف انہی کے anti-chase فلٹر اپنی رینج کے تناسب سے ----
OF_MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"}
OF_MAJOR_MIN_DELTA = 2.0
OF_MAJOR_ABS_DELTA = 2.0
OF_MAJOR_STACK_MIN_DELTA = 1.5
OF_MAJOR_MOVE_MULT = 0.7       # open سے move <= 0.7 x اوسط رینج   (پہلے 0.5)
OF_MAJOR_RANGE_MULT = 1.3      # موجودہ رینج <= 1.3 x اوسط رینج    (پہلے 1.0)
OF_MAJOR_LOOKBACK_MULT = 2.5   # آخری 3 کینڈل move <= 2.5 x اوسط   (پہلے 2.0)

# ---- بڑے کوائنز کا aggTrades ٹکڑوں میں ڈاؤنلوڈ ----
OF_MAJOR_CHUNK_MIN = 30        # ہر ٹکڑا کتنے منٹ کا
OF_MAJOR_CHUNK_THREADS = 4     # ٹکڑوں کے لیے متوازی تھریڈز
OF_DEBUG_MAJORS = True         # بڑے کوائنز کا تشخیصی لاگ

# ---- Stablecoin فلٹر ----
STABLE_BASES = {"USDC", "BUSD", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "AEUR", "EURI",
                "USDP", "USDE", "USDS", "USDG", "USD1", "PYUSD", "RLUSD", "RWUSD",
                "BFUSD", "XUSD", "U", "TRY", "BIDR", "UST", "USTC"}
STABLE_MAX_AVG_RANGE_PCT = 0.15   # 4h اوسط رینج اس سے کم ہو تو stable سمجھ کر skip


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


def get_klines(symbol, interval, limit):
    return safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit})


def post_ntfy(title, body, tags="rotating_light", url=None):
    try:
        r = requests.post(url or NFTY_URL, data=body.encode("utf-8"),
                          headers={"Title": title, "Priority": "high", "Tags": tags},
                          timeout=15)
        print(f"Ntfy [{title}]: {r.status_code}")
        return r.ok
    except Exception as e:
        print(f"Ntfy Error: {e}")
        return False


def is_stable_base(base):
    b = base.upper()
    return b in STABLE_BASES or b.startswith("USD") or (b.endswith("USD") and len(b) > 3)


def get_of_symbols(limit):
    """ٹاپ والیوم USDT کوائنز (stablecoins اور leveraged ٹوکنز کے بغیر)، بڑے کوائنز لازماً شامل۔"""
    data = safe_get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=30)
    if not data or "symbols" not in data:
        return []
    valid = set()
    for s in data["symbols"]:
        if s.get("quoteAsset") != "USDT" or s.get("status") != "TRADING":
            continue
        sym = s.get("symbol", "")
        base = s.get("baseAsset", "")
        if is_stable_base(base):
            continue
        if base.endswith(("UP", "DOWN", "BULL", "BEAR")) and base not in ("JUP",):
            continue
        valid.add(sym)

    ticker = safe_get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=30)
    if not isinstance(ticker, list):
        syms = sorted(valid)[:limit]
    else:
        volumes = []
        for t in ticker:
            sym = t.get("symbol")
            if sym in valid:
                try:
                    volumes.append((sym, float(t.get("quoteVolume", 0))))
                except (TypeError, ValueError):
                    continue
        volumes.sort(key=lambda x: x[1], reverse=True)
        syms = [s for s, _ in volumes[:limit]]
    for m in OF_MAJORS:
        if m in valid and m not in syms:
            syms.append(m)
    return syms


def build_profile(trades, lo, hi, nb):
    """حقیقی aggTrades سے price-bucket footprint۔ m=True => aggressive seller۔"""
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


def anti_chase(hist, symbol):
    """صرف klines سے anti-chase فلٹر۔ واپسی: (blocked, avg_range_pct)۔
    یہ aggTrades سے پہلے چلتا ہے تاکہ بھاری ڈاؤنلوڈ صرف پاس ہونے والے کوائنز کا ہو؛ نتیجہ وہی رہتا ہے۔"""
    bar, prev = hist[-1], hist[:-1]
    o, h, l, c = float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4])
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

    if symbol in OF_MAJORS:
        blocked = avg_range_pct <= 0 or (
            move_from_open_pct >= avg_range_pct * OF_MAJOR_MOVE_MULT
            or current_range_pct >= avg_range_pct * OF_MAJOR_RANGE_MULT
            or recent_move_pct >= avg_range_pct * OF_MAJOR_LOOKBACK_MULT)
    else:
        blocked = (move_from_open_pct >= OF_MAX_MOVE_FROM_OPEN_PCT
                   or (avg_range_pct > 0 and current_range_pct >= avg_range_pct * OF_MAX_RANGE_AVG_MULT)
                   or recent_move_pct >= OF_MAX_LOOKBACK_MOVE_PCT)
    return blocked, avg_range_pct


def eval_setups(hist, sub, tf, progress, symbol=""):
    """جاری بار (hist[-1]) پر تمام سیٹ اپس چیک کریں۔ anti-chase پہلے ہو چکا ہوتا ہے۔"""
    cfg = OF_TF[tf]
    is_major = symbol in OF_MAJORS
    min_delta = OF_MAJOR_MIN_DELTA if is_major else cfg["min_delta_pct"]
    abs_delta = OF_MAJOR_ABS_DELTA if is_major else OF_ABS_DELTA
    stack_min = OF_MAJOR_STACK_MIN_DELTA if is_major else OF_STACK_MIN_DELTA

    bar, prev = hist[-1], hist[:-1]
    o, h, l, c = float(bar[1]), float(bar[2]), float(bar[3]), float(bar[4])
    if h <= l or not prev:
        return None
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

    # 1) Delta + POC + Close
    if (delta_pct >= min_delta and ratio > 1.0 and poc_pos >= OF_POC_BULL
            and close_pos >= OF_CLOSE_BULL and vol_ok):
        found["BULL"].append("Delta+POC")
    if (delta_pct <= -min_delta and ratio < 1.0 and poc_pos <= OF_POC_BEAR
            and close_pos <= OF_CLOSE_BEAR and vol_ok):
        found["BEAR"].append("Delta+POC")

    # 2) Absorption
    if (vol_ratio >= OF_ABS_VOL and delta_pct <= -abs_delta
            and c >= o and close_pos >= 0.55):
        found["BULL"].append("Absorption")
    if (vol_ratio >= OF_ABS_VOL and delta_pct >= abs_delta
            and c <= o and close_pos <= 0.45):
        found["BEAR"].append("Absorption")

    # 3) Liquidity Sweep
    if l < pl and c > pl and close_pos >= 0.6 and delta_pct > 0 and vol_ok:
        found["BULL"].append("Sweep")
    if h > ph and c < ph and close_pos <= 0.4 and delta_pct < 0 and vol_ok:
        found["BEAR"].append("Sweep")

    # 4) Exhaustion
    nb5 = OF_BUCKETS // 5
    low_zone = tot[:nb5].sum() / tot.sum()
    high_zone = tot[-nb5:].sum() / tot.sum()
    if l <= pl and low_zone <= OF_EXH_THIN and close_pos >= 0.5 and vol_ok:
        found["BULL"].append("Exhaustion")
    if h >= ph and high_zone <= OF_EXH_THIN and close_pos <= 0.5 and vol_ok:
        found["BEAR"].append("Exhaustion")

    # 5) CVD Divergence (kline taker volume سے)
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

    # 6) Stacked Imbalance
    if abs(delta_pct) >= stack_min:
        ok = tot >= 0.3 * tot.sum() / OF_BUCKETS
        if delta_pct > 0 and has_run(ok & (buy >= OF_STACK_RATIO * sell), OF_STACK_N):
            found["BULL"].append("Stacked Imbalance")
        if delta_pct < 0 and has_run(ok & (sell >= OF_STACK_RATIO * buy), OF_STACK_N):
            found["BEAR"].append("Stacked Imbalance")

    if found["BULL"] and found["BEAR"]:
        found = {"BULL": [], "BEAR": []}

    info = (f"delta {delta_pct:+.1f}% | poc {poc_pos:.2f} | close {close_pos:.2f} | "
            f"vol×{vol_ratio:.2f}")
    return found, info


def load_aggtrades(symbol, win_start, win_end):
    """[start, end) کے executed aggTrades؛ ناکامی یا ادھورے ڈیٹا پر None۔"""
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
        params = {"symbol": symbol, "fromId": last_id + 1, "limit": 1000}
        if int(data[-1]["T"]) >= end_ms - 1:
            return trades
    stats["too_heavy"] += 1
    print(f"⚠️ {symbol}: aggTrades pagination limit reached; skipping")
    return None


def load_aggtrades_split(symbol, win_start, win_end, chunk_min=OF_MAJOR_CHUNK_MIN):
    """بڑے کوائنز کے لیے: ونڈو کو نصف کھلے ٹکڑوں [t, e) میں بانٹ کر متوازی ڈاؤنلوڈ۔
    ٹکڑے آپس میں overlap نہیں کرتے، لہٰذا نتیجہ ایک ہی بڑی درخواست جیسا ہے مگر مکمل آتا ہے۔
    کوئی ایک ٹکڑا ناکام ہو تو پورا None (ادھورا ڈیٹا کبھی استعمال نہیں ہوتا)۔"""
    wins, t = [], win_start
    step = timedelta(minutes=chunk_min)
    while t < win_end:
        e = min(t + step, win_end)
        wins.append((t, e))
        t = e
    if not wins:
        return []
    with ThreadPoolExecutor(max_workers=OF_MAJOR_CHUNK_THREADS) as ex:
        parts = list(ex.map(lambda w: load_aggtrades(symbol, w[0], w[1]), wins))
    if any(p is None for p in parts):
        return None
    return [x for p in parts for x in p]


def check_orderflow(symbol, now_ms):
    out = []
    is_major = symbol in OF_MAJORS
    for tf, cfg in OF_TF.items():
        interval_ms = cfg["minutes"] * 60_000
        start_ms = now_ms // interval_ms * interval_ms
        progress = (now_ms - start_ms) / interval_ms
        if progress < OF_MIN_PROGRESS:
            continue
        hist = get_klines(symbol, tf, 21)
        if not isinstance(hist, list) or len(hist) < 4 or int(hist[-1][0]) != start_ms:
            if is_major and OF_DEBUG_MAJORS:
                print(f"   MAJOR {symbol}: klines ناکام یا کینڈل بدل چکی (skip)")
            continue
        try:
            if float(hist[-1][2]) <= float(hist[-1][3]):
                continue
            blocked, avg_range_pct = anti_chase(hist, symbol)
        except (TypeError, ValueError, IndexError):
            continue
        # stable جیسے (تقریباً ساکن) کوائنز skip
        if avg_range_pct < STABLE_MAX_AVG_RANGE_PCT:
            continue
        if is_major and OF_DEBUG_MAJORS:
            print(f"   MAJOR {symbol}: blocked={blocked} avg_range={avg_range_pct:.2f}% "
                  f"progress={progress:.0%}")
        if blocked:
            continue
        loader = load_aggtrades_split if is_major else load_aggtrades
        sub = loader(symbol,
            datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc) + timedelta(milliseconds=1))
        if not isinstance(sub, list):
            if is_major:
                skipped_majors.append(symbol)
                print(f"   MAJOR {symbol}: aggTrades مکمل نہیں ملے (skip)")
            continue
        if len(sub) < 20:
            continue
        if is_major and OF_DEBUG_MAJORS:
            print(f"   MAJOR {symbol}: {len(sub)} aggTrades لوڈ ہوئے")
        res = eval_setups(hist, sub, tf, progress, symbol)
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
    with ThreadPoolExecutor(max_workers=OF_THREADS) as ex:
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


def main():
    print("=" * 58)
    print("Order Flow v2 | 4H only | top 300 USDT | no stablecoins")
    print("=" * 58)
    try:
        syms = get_of_symbols(OF_COIN_COUNT)
        if not syms:
            print("symbols نہیں ملے")
        else:
            run_orderflow(syms)
    except Exception as e:
        print(f"4H Order Flow error: {e}")
    if skipped_majors:
        print(f"⚠️ ڈیٹا ادھورا ہونے پر skip ہونے والے بڑے کوائنز: {sorted(set(skipped_majors))}")
    print(f"Finished. API failures={stats['failed']} rate-limits={stats['rate_limited']} "
          f"too_heavy={stats['too_heavy']}")


if __name__ == "__main__":
    main()
