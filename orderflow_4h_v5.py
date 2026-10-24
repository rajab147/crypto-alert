#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Binance اسپاٹ Order Flow اسکینر (صرف 4h) - ٹاپ 150 USDT کوائنز، stablecoins کے بغیر۔

ڈیٹا (سب Binance اسپاٹ سے، data-api.binance.vision):
  * aggTrades: ہر ٹریڈ کی اصل جارح سمت (m فلیگ) => delta، footprint، intrabar فلو
  * klines taker-buy والیم => CVD
  * depth snapshot: صرف امیدوار سگنل پر، آرڈر بک کی تصدیق (ایک تصویر، تاریخ نہیں)

v5 تبدیلیاں (v4 کے مقابلے میں):
  1) "موو کے بعد سگنل نہیں": anti-chase سخت (0.5x / 1.0x / 2.0x اوسط رینج)،
     آخری 30 منٹ کی اصل ٹریڈ حرکت پر پابندی، سویپ کے بعد ریباؤنڈ کی حد،
     اور سگنل صرف کینڈل کے 10% سے 60% کے درمیان۔
  2) نیا لیکوئیڈیٹی سویپ: کئی سطحیں (Extreme / Recent / Equal / Swing)، سویپ کی گہرائی کی حد،
     سطح کے نیچے کی ٹریڈز میں stop-run کی جانچ، اور low/high کے بعد کی اصل ٹریڈ delta (reclaim)۔
  3) Vol اب اسکور کا مفت پوائنٹ نہیں، صرف گیٹ ہے۔ لیٹ اشارے (POC / Close) اسکور سے نکال دیے۔
  4) الٹ پھیر والے سیٹ اپس (Sweep / Absorption / Exhaustion / Divergence) کو delta کی سمت
     کی شرط سے آزاد کیا؛ Absorption اب مخالف سمت سے منسوخ نہیں ہوتا۔
  5) Whale (بڑی ٹریڈز کا delta) اور Book (آرڈر بک عدم توازن) کے اضافی پوائنٹس۔
  6) بھاری کوائنز کا aggTrades چھوٹی کھڑکیوں میں بانٹ کر مکمل ڈاؤنلوڈ (خاموشی سے skip نہیں)۔
  7) ہر کوائن/کینڈل پر ایک ہی سمت، لیبل صرف اوپر کی طرف (EARLY -> STRONG)۔
  8) BULL/BEAR گنتی کا لاگ اور orderflow_signals_log.jsonl (بعد میں نتائج جانچنے کے لیے)۔
  9) 418 پر اسکرپٹ فوراً رکتا ہے، 400/404 پر retry نہیں۔
"""
import os, json, time, threading, requests, numpy as np
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor

BASE_URL = "https://data-api.binance.vision"
NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    raise SystemExit("NFTY_URL environment variable سیٹ کریں")
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL
session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "too_heavy": 0, "heavy_failed": 0, "banned": 0}
ABORT = threading.Event()          # 418 ملنے پر سب تھریڈز کو روکنے کے لیے
skipped_majors = []

diag_lock = threading.Lock()
diag = {k: 0 for k in (
    "scanned", "no_kline", "stable_skip", "blocked_antichase", "no_trades", "late_move",
    "vol_gate", "evaluated", "delta_pos", "delta_neg", "conflict", "bull_sig", "bear_sig")}


def bump(key, n=1):
    with diag_lock:
        diag[key] += n


# ---------------- سیٹنگز ----------------
OF_TF = {"4h": {"minutes": 240, "min_delta_pct": 5.0}}
OF_MIN_PROGRESS = 0.10
OF_MAX_PROGRESS = 0.60          # کینڈل کے 60% کے بعد کوئی سگنل نہیں
OF_MIN_TRADES = 100
OF_VOL_GATE = 0.8               # والیم صرف گیٹ ہے، اسکور میں پوائنٹ نہیں

# anti-chase: اوسط رینج کے تناسب میں (سب کوائنز کے لیے سخت)
OF_MOVE_MULT = 0.5              # open سے move
OF_RANGE_MULT = 1.0             # موجودہ رینج
OF_LOOKBACK_MULT = 2.0          # آخری 3 کینڈل کا move
OF_RECENT_MIN = 30              # اصل ٹریڈز سے آخری N منٹ کی حرکت
OF_RECENT_MOVE_MULT = 0.4

OF_BUCKETS = 40
OF_LOOKBACK = 12
OF_ABS_DELTA = 4.0
OF_ABS_VOL = 1.2
OF_NEAR_LEVEL_MULT = 0.3        # Absorption: قیمت 12-کینڈل low/high کے اتنے قریب (x اوسط رینج)
OF_STACK_RATIO = 3.0
OF_STACK_N = 3
OF_STACK_MIN_DELTA = 3.0
OF_EXH_THIN = 0.05
OF_DIV_MIN = 0.5
OF_STATE_FILE = "orderflow_4h_state.json"
OF_LOG_FILE = "orderflow_signals_log.jsonl"
MAX_TRADE_PAGES = 80
OF_COIN_COUNT = 150
OF_THREADS = 8

# ---- لیکوئیڈیٹی سویپ ----
OF_EQUAL_TOL_MULT = 0.12        # برابر highs/lows: فرق <= 0.12 x اوسط رینج
OF_SWEEP_MIN_DEPTH_MULT = 0.03  # سطح کے پار گہرائی کم از کم (x اوسط رینج)
OF_SWEEP_MAX_DEPTH_MULT = 0.8   # اس سے گہرا ہو تو بریک آؤٹ ہے، سویپ نہیں
OF_SWEEP_MAX_AWAY_MULT = 0.35   # واپسی کے بعد قیمت سطح سے زیادہ دور نہ ہو
OF_MAX_REBOUND_MULT = 0.6       # extreme سے ریباؤنڈ کی حد (دیر سے انٹری روکنے کے لیے)
OF_SWEEP_MAX_AGE_MIN = 75       # extreme کو زیادہ سے زیادہ اتنے منٹ ہوئے ہوں
OF_SWEEP_MIN_AFTER = 20         # extreme کے بعد کم از کم اتنی ٹریڈز
OF_SWEEP_RECLAIM_DELTA = 2.0    # extreme کے بعد کی ٹریڈز کا delta (%) درست سمت میں
OF_SWEEP_MIN_VOL_SHARE = 0.01   # سطح کے پار کا والیم کل کا کم از کم 1%
OF_STOPRUN_FRAC = 0.55          # سطح کے پار جارح بیچنے/خریدنے والوں کا حصہ

# ---- اسکور ----
SCORE_PTS = {"Sweep": 3, "Absorption": 2, "Exhaustion": 2, "Divergence": 2, "Stacked": 2,
             "StopRun": 1, "Flow": 1, "Whale": 1, "Book": 1}
CORE_SETUPS = {"Sweep", "Absorption", "Exhaustion", "Divergence"}
OF_SCORE_EARLY = 4              # کم از کم ایک core سیٹ اپ لازمی
OF_SCORE_STRONG = 7             # اور کم از کم دو core سیٹ اپس
LABEL_RANK = {"EARLY": 1, "STRONG": 2}
OF_WHALE_PCT = 95
OF_WHALE_DELTA = 10.0

# ---- آرڈر بک (صرف امیدواروں پر) ----
OF_BOOK_LIMIT = 100
OF_BOOK_BAND_MULT = 0.25        # قیمت کے ارد گرد اوسط رینج کا 0.25x
OF_BOOK_MIN_IMB = 0.15

# ---- بڑے کوائنز ----
OF_MAJORS = {"BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT"}
OF_MAJOR_MIN_DELTA = 2.0
OF_MAJOR_ABS_DELTA = 2.0
OF_MAJOR_STACK_MIN_DELTA = 1.5
OF_MAJOR_MOVE_MULT = 0.7
OF_MAJOR_RANGE_MULT = 1.3
OF_MAJOR_LOOKBACK_MULT = 2.5
OF_MAJOR_RECENT_MULT = 0.5

# ---- aggTrades ٹکڑوں میں ڈاؤنلوڈ ----
OF_MAJOR_CHUNK_MIN = 30
OF_MAJOR_CHUNK_THREADS = 4
OF_HEAVY_CHUNK_MIN = 15          # عام کوائن بھاری نکلے تو fallback
OF_HEAVY_CHUNK_THREADS = 2
OF_DEBUG_MAJORS = True
HEAVY = "HEAVY"

# ---- Stablecoin فلٹر ----
STABLE_BASES = {"USDC", "BUSD", "TUSD", "FDUSD", "DAI", "EUR", "GBP", "AEUR", "EURI",
                "USDP", "USDE", "USDS", "USDG", "USD1", "PYUSD", "RLUSD", "RWUSD",
                "BFUSD", "XUSD", "U", "TRY", "BIDR", "UST", "USTC"}
STABLE_MAX_AVG_RANGE_PCT = 0.15


# ---------------- نیٹ ورک ----------------
def safe_get(url, params=None, timeout=15, retries=4):
    for attempt in range(retries):
        if ABORT.is_set():
            return None
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code == 418:
                # IP بین: مزید درخواستیں حالت بگاڑتی ہیں، سب روک دیں
                stats["banned"] += 1
                ABORT.set()
                print(f"⛔ 418 (IP ban) ملا، اسکرپٹ روکا جا رہا ہے۔ Retry-After={r.headers.get('Retry-After')}")
                return None
            if r.status_code == 429:
                stats["rate_limited"] += 1
                try:
                    wait = int(r.headers.get("Retry-After", 2 ** attempt))
                except ValueError:
                    wait = 2 ** attempt
                time.sleep(min(wait, 30))
                continue
            if r.status_code in (400, 404):
                stats["failed"] += 1
                return None            # غلط symbol/پیرامیٹر: retry بیکار ہے
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


def append_log(rows):
    if not rows:
        return
    try:
        with open(OF_LOG_FILE, "a", encoding="utf-8") as f:
            for r in rows:
                f.write(json.dumps(r, ensure_ascii=False) + "\n")
    except Exception as e:
        print(f"log error: {e}")


def get_klines(symbol, interval, limit):
    return safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": interval, "limit": limit})


def get_depth(symbol, limit=OF_BOOK_LIMIT):
    return safe_get(f"{BASE_URL}/api/v3/depth", params={"symbol": symbol, "limit": limit})


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


# ---------------- aggTrades ----------------
def load_aggtrades(symbol, win_start, win_end):
    """[start, end) کے executed aggTrades۔ ناکامی پر None، pagination حد پر HEAVY۔"""
    start_ms = int(win_start.timestamp() * 1000)
    end_ms = int(win_end.timestamp() * 1000)
    if end_ms <= start_ms:
        return []
    trades = []
    params = {"symbol": symbol, "startTime": start_ms, "endTime": end_ms - 1, "limit": 1000}
    for _ in range(MAX_TRADE_PAGES):
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
    return HEAVY


def load_aggtrades_split(symbol, win_start, win_end, chunk_min, threads):
    """ونڈو کو نصف کھلے ٹکڑوں [t, e) میں بانٹ کر متوازی ڈاؤنلوڈ۔ کوئی ٹکڑا ناکام یا بھاری ہو تو None۔"""
    wins, t = [], win_start
    step = timedelta(minutes=chunk_min)
    while t < win_end:
        e = min(t + step, win_end)
        wins.append((t, e))
        t = e
    if not wins:
        return []
    with ThreadPoolExecutor(max_workers=threads) as ex:
        parts = list(ex.map(lambda w: load_aggtrades(symbol, w[0], w[1]), wins))
    if any(not isinstance(p, list) for p in parts):
        return None
    return [x for p in parts for x in p]


def load_trades(symbol, win_start, win_end, is_major):
    if is_major:
        return load_aggtrades_split(symbol, win_start, win_end,
                                    OF_MAJOR_CHUNK_MIN, OF_MAJOR_CHUNK_THREADS)
    r = load_aggtrades(symbol, win_start, win_end)
    if isinstance(r, str):   # HEAVY: چھوٹے ٹکڑوں میں دوبارہ
        stats["too_heavy"] += 1
        r = load_aggtrades_split(symbol, win_start, win_end,
                                 OF_HEAVY_CHUNK_MIN, OF_HEAVY_CHUNK_THREADS)
        if r is None:
            stats["heavy_failed"] += 1
            print(f"⚠️ {symbol}: aggTrades بھاری، ٹکڑوں میں بھی مکمل نہ ملے (skip)")
    return r if isinstance(r, list) else None


def to_arrays(trades):
    n = len(trades)
    p = np.fromiter((float(t["p"]) for t in trades), dtype=float, count=n)
    q = np.fromiter((float(t["q"]) for t in trades), dtype=float, count=n)
    m = np.fromiter((bool(t["m"]) for t in trades), dtype=bool, count=n)   # True => aggressive seller
    T = np.fromiter((int(t["T"]) for t in trades), dtype=np.int64, count=n)
    order = np.argsort(T, kind="stable")
    return {"p": p[order], "q": q[order], "m": m[order], "T": T[order]}


# ---------------- فلٹرز ----------------
def anti_chase(hist, symbol):
    """klines سے anti-chase۔ واپسی: (blocked, avg_range_pct)۔ سب حدیں اوسط رینج کے تناسب میں۔"""
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
        rs, re = float(recent[0][1]), float(recent[-1][4])
        recent_move_pct = abs(re - rs) / max(rs, 1e-12) * 100.0

    if symbol in OF_MAJORS:
        mv, rg, lb = OF_MAJOR_MOVE_MULT, OF_MAJOR_RANGE_MULT, OF_MAJOR_LOOKBACK_MULT
    else:
        mv, rg, lb = OF_MOVE_MULT, OF_RANGE_MULT, OF_LOOKBACK_MULT
    blocked = avg_range_pct <= 0 or (
        move_from_open_pct >= avg_range_pct * mv
        or current_range_pct >= avg_range_pct * rg
        or recent_move_pct >= avg_range_pct * lb)
    return blocked, avg_range_pct


def recent_move_blocked(tr, now_ms, avg_range_pct, symbol):
    """اصل ٹریڈز سے: آخری OF_RECENT_MIN منٹ میں قیمت پہلے ہی بہت چل چکی ہو تو بلاک۔"""
    mult = OF_MAJOR_RECENT_MULT if symbol in OF_MAJORS else OF_RECENT_MOVE_MULT
    T, p = tr["T"], tr["p"]
    cutoff = now_ms - OF_RECENT_MIN * 60_000
    i = min(int(np.searchsorted(T, cutoff)), len(p) - 1)
    ref = float(p[i])
    if ref <= 0:
        return False
    move = abs(float(p[-1]) - ref) / ref * 100.0
    return move >= avg_range_pct * mult


# ---------------- مددگار ----------------
def find_run(mask, n):
    """پہلی مسلسل True رن (start, end_exclusive) جس کی لمبائی >= n؛ ورنہ None۔"""
    run = 0
    for i, m in enumerate(mask):
        run = run + 1 if m else 0
        if run >= n:
            return (i - run + 1, i + 1)
    return None


def liquidity_levels(vals, side):
    """سطحیں جہاں stops/لیکوئیڈیٹی جمع ہوتی ہے۔ side='low' یا 'high'۔
    واپسی: [(قیمت، قسم، وزن)]۔ Extreme=3، Recent/Equal=2، Swing=1۔"""
    low = side == "low"
    pick = min if low else max
    n = len(vals)
    out = [(pick(vals), "Extreme", 3)]
    r6 = pick(vals[-6:])
    out.append((r6, "Recent", 2))
    sw = []
    for i in range(1, n - 1):
        if low and vals[i] < vals[i - 1] and vals[i] <= vals[i + 1]:
            sw.append(i)
        if (not low) and vals[i] > vals[i - 1] and vals[i] >= vals[i + 1]:
            sw.append(i)
    for i in sw:
        out.append((vals[i], "Swing", 1))
    return out, sw


def book_imbalance(depth, price, band_pct):
    """آرڈر بک عدم توازن (bid-ask)/(bid+ask) قیمت کے ارد گرد band میں؛ [-1, 1] یا None۔"""
    try:
        bids = np.array([[float(x[0]), float(x[1])] for x in depth["bids"]])
        asks = np.array([[float(x[0]), float(x[1])] for x in depth["asks"]])
        if len(bids) < 5 or len(asks) < 5 or price <= 0:
            return None
        cover = min((price - bids[:, 0].min()) / price * 100.0,
                    (asks[:, 0].max() - price) / price * 100.0)
        band = min(band_pct, cover)
        if band <= 0:
            return None
        bq = float(bids[bids[:, 0] >= price * (1 - band / 100.0), 1].sum())
        aq = float(asks[asks[:, 0] <= price * (1 + band / 100.0), 1].sum())
        tot = bq + aq
        return (bq - aq) / tot if tot > 0 else None
    except (KeyError, TypeError, ValueError, IndexError):
        return None


def score_of(hits):
    return sum(SCORE_PTS[x] for x in set(hits))


def label_for(score, core):
    if core >= 1 and score >= OF_SCORE_STRONG and core >= 2:
        return "STRONG"
    if core >= 1 and score >= OF_SCORE_EARLY:
        return "EARLY"
    return None


def display_names(d):
    out = []
    for n in sorted(set(d["hits"]), key=lambda x: -SCORE_PTS[x]):
        tag = d["tags"].get(n)
        out.append(f"{n}[{tag}]" if tag else n)
    return out


# ---------------- سیٹ اپس ----------------
def eval_setups(hist, tr, tf, progress, symbol, now_ms, avg_range_pct):
    """جاری کینڈل کے اصل ٹریڈز سے ہر سمت کے سیٹ اپس۔
    واپسی: ({"BULL": {...}, "BEAR": {...}}, info) یا None۔
    high/low/close ٹریڈز سے لیے جاتے ہیں تاکہ klines کے وقت کا فرق نہ پڑے۔"""
    cfg = OF_TF[tf]
    is_major = symbol in OF_MAJORS
    min_delta = OF_MAJOR_MIN_DELTA if is_major else cfg["min_delta_pct"]
    abs_delta = OF_MAJOR_ABS_DELTA if is_major else OF_ABS_DELTA
    stack_min = OF_MAJOR_STACK_MIN_DELTA if is_major else OF_STACK_MIN_DELTA
    avg = avg_range_pct

    prev = hist[:-1]
    if not prev:
        return None
    p, q, isell, T = tr["p"], tr["q"], tr["m"], tr["T"]
    o = float(hist[-1][1])
    h, l, c = float(p.max()), float(p.min()), float(p[-1])
    if h <= l:
        return None
    buy_qty = float(q[~isell].sum())
    sell_qty = float(q[isell].sum())
    v = buy_qty + sell_qty
    if v <= 0:
        return None
    avg_vol = float(np.mean([float(k[5]) for k in prev]))
    elapsed = min(1.0, max(float(progress), 1.0 / max(cfg["minutes"], 1)))
    vol_ratio = v / (avg_vol * elapsed) if avg_vol > 0 else 0.0
    delta_pct = (buy_qty - sell_qty) / v * 100.0
    close_pos = (c - l) / (h - l)
    info = {"delta": delta_pct, "close_pos": close_pos, "vol_ratio": vol_ratio, "price": c}

    sides = {s: {"hits": [], "core": 0, "tags": {}} for s in ("BULL", "BEAR")}

    def add(side, name, tag=None):
        sides[side]["hits"].append(name)
        if name in CORE_SETUPS:
            sides[side]["core"] += 1
        if tag:
            sides[side]["tags"][name] = tag

    if vol_ratio < OF_VOL_GATE:
        info["gated"] = True
        return sides, info

    # --- footprint ---
    nb = OF_BUCKETS
    step = (h - l) / nb
    idx = np.clip(((p - l) / step).astype(int), 0, nb - 1)
    bvol = np.bincount(idx[~isell], weights=q[~isell], minlength=nb)
    svol = np.bincount(idx[isell], weights=q[isell], minlength=nb)
    tot = bvol + svol
    if tot.sum() <= 0:
        return None

    look = prev[-OF_LOOKBACK:]
    lows = [float(k[3]) for k in look]
    highs = [float(k[2]) for k in look]
    pl, ph = min(lows), max(highs)
    near_low_pct = (l - pl) / pl * 100.0        # منفی => پچھلے low کے نیچے
    near_high_pct = (ph - h) / ph * 100.0

    # --- extreme کے بعد کی اصل فلو (reclaim) ---
    def delta_after(i):
        qa, sa = q[i + 1:], isell[i + 1:]
        t = float(qa.sum())
        if len(qa) < OF_SWEEP_MIN_AFTER or t <= 0:
            return None
        return (float(qa[~sa].sum()) - float(qa[sa].sum())) / t * 100.0

    i_low = len(p) - 1 - int(np.argmin(p[::-1]))
    i_high = len(p) - 1 - int(np.argmax(p[::-1]))
    low_after, high_after = delta_after(i_low), delta_after(i_high)
    age_low = (now_ms - int(T[i_low])) / 60000.0
    age_high = (now_ms - int(T[i_high])) / 60000.0

    # --- 1) Liquidity Sweep ---
    lv_low, sw_low = liquidity_levels(lows, "low")
    lv_high, sw_high = liquidity_levels(highs, "high")
    tol = avg * OF_EQUAL_TOL_MULT
    # برابر lows/highs: دو swing پوائنٹس کی قیمتیں قریب ہوں
    for i in range(len(sw_low)):
        for j in range(i + 1, len(sw_low)):
            a, b = lows[sw_low[i]], lows[sw_low[j]]
            if abs(a - b) / max(a, 1e-12) * 100.0 <= tol:
                lv_low.append((min(a, b), "Equal", 2))
    for i in range(len(sw_high)):
        for j in range(i + 1, len(sw_high)):
            a, b = highs[sw_high[i]], highs[sw_high[j]]
            if abs(a - b) / max(a, 1e-12) * 100.0 <= tol:
                lv_high.append((max(a, b), "Equal", 2))

    def sweep_for(side):
        best = None
        for lvl, kind, w in (lv_low if side == "BULL" else lv_high):
            if side == "BULL":
                if not (l < lvl < c):
                    continue
                depth = (lvl - l) / lvl * 100.0
                away = (c - lvl) / lvl * 100.0
                rebound = (c - l) / l * 100.0
                after, age = low_after, age_low
                zone = p < lvl
                push = float(q[zone & isell].sum())
            else:
                if not (c < lvl < h):
                    continue
                depth = (h - lvl) / lvl * 100.0
                away = (lvl - c) / lvl * 100.0
                rebound = (h - c) / h * 100.0
                after, age = high_after, age_high
                zone = p > lvl
                push = float(q[zone & ~isell].sum())
            if not (avg * OF_SWEEP_MIN_DEPTH_MULT <= depth <= avg * OF_SWEEP_MAX_DEPTH_MULT):
                continue
            if away > avg * OF_SWEEP_MAX_AWAY_MULT or rebound > avg * OF_MAX_REBOUND_MULT:
                continue
            if age > OF_SWEEP_MAX_AGE_MIN or after is None:
                continue
            if side == "BULL" and after < OF_SWEEP_RECLAIM_DELTA:
                continue
            if side == "BEAR" and after > -OF_SWEEP_RECLAIM_DELTA:
                continue
            vz = float(q[zone].sum())
            if vz <= 0 or vz / v < OF_SWEEP_MIN_VOL_SHARE:
                continue
            cand = (w, depth, push / vz >= OF_STOPRUN_FRAC, kind)
            if best is None or (cand[0], cand[1]) > (best[0], best[1]):
                best = cand
        return best

    for side in ("BULL", "BEAR"):
        sw = sweep_for(side)
        if sw:
            add(side, "Sweep", sw[3])
            if sw[2]:
                add(side, "StopRun")

    # --- 2) Absorption (قیمت پچھلے low/high کے قریب) ---
    if vol_ratio >= OF_ABS_VOL:
        if (delta_pct <= -abs_delta and c >= o and close_pos >= 0.5
                and near_low_pct <= avg * OF_NEAR_LEVEL_MULT):
            add("BULL", "Absorption")
        if (delta_pct >= abs_delta and c <= o and close_pos <= 0.5
                and near_high_pct <= avg * OF_NEAR_LEVEL_MULT):
            add("BEAR", "Absorption")

    # --- 3) Exhaustion (سرے پر پتلا والیم، پھر الٹ سمت کا فلو) ---
    nb5 = nb // 5
    low_zone = float(tot[:nb5].sum() / tot.sum())
    high_zone = float(tot[-nb5:].sum() / tot.sum())
    if l <= pl and low_zone <= OF_EXH_THIN and close_pos >= 0.5 and low_after is not None and low_after >= 0:
        add("BULL", "Exhaustion")
    if h >= ph and high_zone <= OF_EXH_THIN and close_pos <= 0.5 and high_after is not None and high_after <= 0:
        add("BEAR", "Exhaustion")

    # --- 4) CVD Divergence (جاری کینڈل کا delta ٹریڈز سے) ---
    deltas = np.array([2 * float(k[9]) - float(k[5]) for k in prev] + [buy_qty - sell_qty])
    cvd = np.cumsum(deltas)
    n = len(look)
    s = len(deltas) - 1 - n
    i_hi = s + int(np.argmax(highs))
    i_lo = s + int(np.argmin(lows))
    margin = OF_DIV_MIN * float(np.mean(np.abs(deltas[:-1])))
    if l <= pl and cvd[-1] - cvd[i_lo] >= margin:
        add("BULL", "Divergence")
    if h >= ph and cvd[i_hi] - cvd[-1] >= margin:
        add("BEAR", "Divergence")

    # --- 5) Stacked Imbalance (رن کنارے کی طرف: BULL نچلے آدھے میں، BEAR اوپری آدھے میں) ---
    if abs(delta_pct) >= stack_min:
        ok = tot >= 0.3 * tot.sum() / nb
        if delta_pct > 0:
            run = find_run(ok & (bvol >= OF_STACK_RATIO * svol), OF_STACK_N)
            if run and (run[0] + run[1]) / 2.0 / nb <= 0.5:
                add("BULL", "Stacked")
        else:
            run = find_run(ok & (svol >= OF_STACK_RATIO * bvol), OF_STACK_N)
            if run and (run[0] + run[1]) / 2.0 / nb >= 0.5:
                add("BEAR", "Stacked")

    # --- 6) Flow (delta کی سمت) ---
    if delta_pct >= min_delta * 0.5:
        add("BULL", "Flow")
    if delta_pct <= -min_delta * 0.5:
        add("BEAR", "Flow")

    # --- 7) Whale (سب سے بڑی 5% ٹریڈز کا delta) ---
    if len(q) >= 200:
        big = q >= float(np.percentile(q, OF_WHALE_PCT))
        bq = float(q[big & ~isell].sum())
        sq = float(q[big & isell].sum())
        if bq + sq > 0:
            ld = (bq - sq) / (bq + sq) * 100.0
            info["whale"] = ld
            if ld >= OF_WHALE_DELTA:
                add("BULL", "Whale")
            if ld <= -OF_WHALE_DELTA:
                add("BEAR", "Whale")

    return sides, info


# ---------------- ہر کوائن ----------------
def check_orderflow(symbol, now_ms):
    """واپسی: [(tf, side, price, label, score, names, start_ms, progress, extra)]"""
    out = []
    is_major = symbol in OF_MAJORS
    for tf, cfg in OF_TF.items():
        interval_ms = cfg["minutes"] * 60_000
        start_ms = now_ms // interval_ms * interval_ms
        progress = (now_ms - start_ms) / interval_ms
        if progress < OF_MIN_PROGRESS or progress > OF_MAX_PROGRESS:
            continue
        bump("scanned")
        hist = get_klines(symbol, tf, 21)
        if not isinstance(hist, list) or len(hist) < 4 or int(hist[-1][0]) != start_ms:
            bump("no_kline")
            if is_major and OF_DEBUG_MAJORS:
                print(f"   MAJOR {symbol}: klines ناکام یا کینڈل بدل چکی (skip)")
            continue
        try:
            if float(hist[-1][2]) <= float(hist[-1][3]):
                continue
            blocked, avg_range_pct = anti_chase(hist, symbol)
        except (TypeError, ValueError, IndexError):
            continue
        if avg_range_pct < STABLE_MAX_AVG_RANGE_PCT:
            bump("stable_skip")
            continue
        if is_major and OF_DEBUG_MAJORS:
            print(f"   MAJOR {symbol}: blocked={blocked} avg_range={avg_range_pct:.2f}% "
                  f"progress={progress:.0%}")
        if blocked:
            bump("blocked_antichase")
            continue

        trades = load_trades(
            symbol,
            datetime.fromtimestamp(start_ms / 1000, tz=timezone.utc),
            datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc) + timedelta(milliseconds=1),
            is_major)
        if trades is None:
            bump("no_trades")
            if is_major:
                skipped_majors.append(symbol)
                print(f"   MAJOR {symbol}: aggTrades مکمل نہیں ملے (skip)")
            continue
        if len(trades) < OF_MIN_TRADES:
            bump("no_trades")
            continue
        tr = to_arrays(trades)

        if recent_move_blocked(tr, now_ms, avg_range_pct, symbol):
            bump("late_move")
            continue

        res = eval_setups(hist, tr, tf, progress, symbol, now_ms, avg_range_pct)
        if not res:
            continue
        sides, info = res
        if info.get("gated"):
            bump("vol_gate")
            continue
        bump("evaluated")
        bump("delta_pos" if info["delta"] > 0 else "delta_neg")

        # آرڈر بک کی تصدیق: صرف امیدواروں پر، ایک ہی snapshot
        depth_imb, depth_done = None, False
        cands = {}
        for side, d in sides.items():
            if d["core"] == 0:
                continue
            if score_of(d["hits"]) + SCORE_PTS["Book"] >= OF_SCORE_EARLY:
                if not depth_done:
                    depth_done = True
                    dp = get_depth(symbol)
                    if isinstance(dp, dict):
                        depth_imb = book_imbalance(dp, info["price"], avg_range_pct * OF_BOOK_BAND_MULT)
                if depth_imb is not None:
                    if (side == "BULL" and depth_imb >= OF_BOOK_MIN_IMB) or \
                       (side == "BEAR" and depth_imb <= -OF_BOOK_MIN_IMB):
                        d["hits"].append("Book")
            score = score_of(d["hits"])
            label = label_for(score, d["core"])
            if label:
                cands[side] = (label, score, display_names(d))

        if len(cands) == 2:       # دونوں سمتوں میں core سیٹ اپ: متضاد، الرٹ نہیں
            bump("conflict")
            cands = {}

        tags = [f"{sd}:{c[0]}({c[1]})" for sd, c in cands.items()]
        print(f"   OF {symbol} {tf} ({progress:.0%}): delta {info['delta']:+.1f}% | "
              f"vol×{info['vol_ratio']:.2f} | book {('%+.2f' % depth_imb) if depth_imb is not None else 'n/a'}"
              f" → {', '.join(tags) or '-'}")
        for side, (label, score, names) in cands.items():
            bump("bull_sig" if side == "BULL" else "bear_sig")
            extra = {"delta": round(info["delta"], 2), "vol_ratio": round(info["vol_ratio"], 2),
                     "avg_range_pct": round(avg_range_pct, 3),
                     "book_imb": None if depth_imb is None else round(depth_imb, 3)}
            out.append((tf, side, info["price"], label, score, names, start_ms, progress, extra))
    return out


# ---------------- مکمل رن ----------------
def run_orderflow(of_symbols, now_ms):
    print(f"\nآرڈر فلو: {len(of_symbols)} کوائنز | ٹائم فریم {list(OF_TF)}")

    cutoff = now_ms - 2 * 24 * 3600 * 1000
    state = {k: v for k, v in load_json(OF_STATE_FILE).items()
             if isinstance(v, (int, float)) and v >= cutoff}

    # ہر (کوائن، کینڈل) پر پہلے بھیجی گئی سمت/لیبل
    sent = {}
    for k in state:
        parts = k.split("|")
        if len(parts) == 5 and parts[4] in LABEL_RANK:
            d = sent.setdefault((parts[0], parts[1], parts[3]), {})
            d[parts[2]] = max(d.get(parts[2], 0), LABEL_RANK[parts[4]])

    def work(sym):
        try:
            return sym, check_orderflow(sym, now_ms)
        except Exception as e:
            print(f"   OF {sym} error: {e}")
            return sym, []

    alerts = {}
    log_rows = []
    with ThreadPoolExecutor(max_workers=OF_THREADS) as ex:
        for sym, res in ex.map(work, of_symbols):
            for tf, side, price, label, score, names, start_ms, progress, extra in res:
                if f"{sym}|{tf}|{side}|{start_ms}|{label}" in state:
                    continue
                prior = sent.get((sym, tf, str(start_ms)), {})
                other = "BEAR" if side == "BULL" else "BULL"
                if other in prior:                      # اسی کینڈل میں مخالف سمت کا الرٹ جا چکا
                    continue
                if prior.get(side, 0) >= LABEL_RANK[label]:   # لیبل صرف اوپر کی طرف
                    continue
                alerts.setdefault((tf, side, label), []).append(
                    (sym, price, score, names, start_ms, progress))
                log_rows.append({
                    "ts": datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc).isoformat(),
                    "symbol": sym, "tf": tf, "side": side, "label": label, "score": score,
                    "names": names, "price": price, "progress": round(progress, 3),
                    "candle_start": start_ms, **extra})

    append_log(log_rows)

    if not alerts:
        print("آرڈر فلو: کوئی نیا الرٹ نہیں")
        save_json(OF_STATE_FILE, state)
        return

    for (tf, side, label), items in sorted(alerts.items()):
        items.sort(key=lambda x: -x[2])
        lines = [f"{s.replace('USDT', '')} {fmt_price(p)} | {sc} | {' + '.join(nm)} | {pg:.0%}"
                 for s, p, sc, nm, _, pg in items]
        name = "Bullish" if side == "BULL" else "Bearish"
        ok = post_ntfy(f"{tf.upper()} {label} {name} Order Flow", "\n".join(lines),
                       tags="green_circle" if side == "BULL" else "red_circle",
                       url=NFTY_OF_URL)
        if ok:
            for s, _, _, _, start_ms, _ in items:
                state[f"{s}|{tf}|{side}|{start_ms}|{label}"] = start_ms
    save_json(OF_STATE_FILE, state)


def main():
    print("=" * 62)
    print("Order Flow v5 | 4H | Binance spot | top 150 | EARLY/STRONG | sweep + book")
    print("=" * 62)
    now_ms = int(time.time() * 1000)
    interval_ms = OF_TF["4h"]["minutes"] * 60_000
    progress = (now_ms - now_ms // interval_ms * interval_ms) / interval_ms
    if progress < OF_MIN_PROGRESS or progress > OF_MAX_PROGRESS:
        print(f"کینڈل کا {progress:.0%} گزرا ہے؛ سگنل صرف {OF_MIN_PROGRESS:.0%} سے "
              f"{OF_MAX_PROGRESS:.0%} کے بیچ دیے جاتے ہیں۔ اس رن میں اسکین نہیں۔")
        return
    try:
        syms = get_of_symbols(OF_COIN_COUNT)
        if not syms:
            print("symbols نہیں ملے")
        else:
            run_orderflow(syms, now_ms)
    except Exception as e:
        print(f"4H Order Flow error: {e}")
    if skipped_majors:
        print(f"⚠️ ڈیٹا ادھورا ہونے پر skip ہونے والے بڑے کوائنز: {sorted(set(skipped_majors))}")
    d = diag
    print(f"تشخیص: اسکین {d['scanned']} | stable {d['stable_skip']} | anti-chase بلاک "
          f"{d['blocked_antichase']} | late-move بلاک {d['late_move']} | ٹریڈز نہیں/ناکام {d['no_trades']} | "
          f"والیم گیٹ {d['vol_gate']} | جانچے گئے {d['evaluated']}")
    print(f"فلو: delta>0 = {d['delta_pos']} | delta<0 = {d['delta_neg']} | "
          f"BULL سگنل = {d['bull_sig']} | BEAR سگنل = {d['bear_sig']} | متضاد = {d['conflict']}")
    print(f"Finished. API failures={stats['failed']} rate-limits={stats['rate_limited']} "
          f"too_heavy={stats['too_heavy']} heavy_failed={stats['heavy_failed']} banned={stats['banned']}")


if __name__ == "__main__":
    main()
