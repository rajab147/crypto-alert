import os
import sys
import json
import gzip
import math
import time
import threading
import statistics
import requests
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

# ============================================================
# ⚙️ سیٹنگز — فیوچرز آرڈر فلو (OKX + Gate + Bitget، USDT perpetuals)
#    ڈیٹا: تینوں ایکسچینجز کے اصل ٹریڈز (taker side) — کوئی تخمینہ نہیں
#    ہر رن میں نئے ٹریڈز اٹھا کر 5 منٹ کے خانوں میں جمع ہوتے ہیں (state فائل)
#    الرٹ تب بنتا ہے جب بار کا ڈیٹا کم از کم MIN_COVERAGE تک مکمل ہو
# ============================================================
TIER1_COUNT = 200           # والیوم میں پہلے 200 کوائنز
TIER2_COUNT = 100           # اگلے 100 (رینک 201–300)، چھوٹے ٹوکنز
DAILY_MIN_QUOTE_VOL = 2_000_000
MIN_BAR_USD = 20_000        # بار میں اس سے کم کل ٹریڈ والیوم پر الرٹ نہیں

OF_TF = {
    "1h": {"minutes": 60,   "min_delta_pct": 6.0, "min_progress": 0.50, "div": True},
    "4h": {"minutes": 240,  "min_delta_pct": 5.0, "min_progress": 0.25, "div": True},
    "1d": {"minutes": 1440, "min_delta_pct": 4.0, "min_progress": 0.25, "div": False},
}
OF_BUCKETS = 40
OF_LOOKBACK = 12
OF_POC_BULL, OF_POC_BEAR = 0.60, 0.40
OF_CLOSE_BULL, OF_CLOSE_BEAR = 0.65, 0.35
OF_VOL_MULT = 1.0
OF_ABS_DELTA, OF_ABS_VOL = 4.0, 1.2
OF_STACK_RATIO, OF_STACK_N, OF_STACK_MIN_DELTA = 3.0, 3, 3.0
OF_EXH_THIN = 0.05
OF_DIV_MIN = 0.5

MIN_COVERAGE = 0.85         # ٹریڈز کا بار میں مکمل ہونے کا کم از کم تناسب (والیوم وزن سے)
BUCKET_MS = 300_000         # 5 منٹ کا خانہ
LOGSTEP = math.log(1.0005)  # footprint کی قیمت کا خانہ: 0.05%
RETAIN_MS = 50 * 3600_000   # خانوں کا ڈیٹا اتنا رکھیں
BINS_KEEP_MS = 26 * 3600_000  # قیمت کے خانے (footprint) اتنا رکھیں
PAGES = 4                   # ایک سمبل پر خلا بھرنے کے لیے زیادہ سے زیادہ صفحے
FETCH_BUDGET_S = 240        # ٹریڈز اٹھانے کا زیادہ سے زیادہ وقت

FLOW_FILE = "flow_state.json.gz"
OF_STATE_FILE = "orderflow_state.json"
TRADES_FILE = "trades_state.json"

# ٹریکر: (مدت گھنٹے، کینڈل، ms)
OF_TRACK = {
    "1h": {"hours": 6,  "step": "5m",  "step_ms": 300_000},
    "4h": {"hours": 24, "step": "15m", "step_ms": 900_000},
    "1d": {"hours": 72, "step": "1h",  "step_ms": 3_600_000},
}
MAX_CLOSED_KEEP = 500
SUMMARY_LAST_N = 30
SMALL_SAMPLE = 10
NTFY_CHUNK = 30

NFTY_URL = os.environ.get("NFTY_URL")
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL

# ============================================================
# 🌐 ریٹ لمٹ: ہر ایکسچینج کا الگ، تمام تھریڈز میں مشترکہ
# ============================================================
HOSTS = {"okx": "https://www.okx.com",
         "gate": "https://api.gateio.ws/api/v4",
         "bitget": "https://api.bitget.com"}


class Limiter:
    def __init__(self, per_sec):
        self.gap = 1.0 / per_sec
        self.lock = threading.Lock()
        self.next = 0.0

    def wait(self):
        with self.lock:
            now = time.monotonic()
            t = max(now, self.next)
            self.next = t + self.gap
        if t > now:
            time.sleep(t - now)

    def penalize(self, secs):
        with self.lock:
            self.next = max(self.next, time.monotonic() + secs)


# سرکاری حدوں سے نیچے رکھی گئی ہیں (OKX history 10/s، Bitget 10/s، Gate 20/s)
LIM = {"okx": Limiter(9), "gate": Limiter(12), "bitget": Limiter(8)}
session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "gaps": 0, "low_cov": 0, "no_candles": 0,
         "fetch_skipped": 0}


def safe_get(ex, path, params=None, timeout=15, retries=4):
    url = HOSTS[ex] + path
    for attempt in range(retries):
        LIM[ex].wait()
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 418, 403):
                stats["rate_limited"] += 1
                wait = min(int(r.headers.get("Retry-After", 2 ** (attempt + 1))), 30)
                LIM[ex].penalize(wait)
                continue
            if r.status_code != 200:
                time.sleep(1 + attempt)
                continue
            d = r.json()
            if isinstance(d, dict) and str(d.get("code")) in ("50011", "50061", "429", "43011"):
                stats["rate_limited"] += 1
                LIM[ex].penalize(2 ** (attempt + 1))
                continue
            return d
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


def load_gz(path):
    try:
        with gzip.open(path, "rt") as f:
            return json.load(f)
    except Exception:
        return {}


def save_gz(path, obj):
    try:
        with gzip.open(path, "wt", compresslevel=6) as f:
            json.dump(obj, f, separators=(",", ":"))
    except Exception as e:
        print(f"state save error ({path}): {e}")


# ============================================================
# 📋 یونیورس: تینوں ایکسچینجز کے USDT perpetuals، مجموعی والیوم سے رینک
# ============================================================
STABLES = {"USDC", "USDT", "DAI", "FDUSD", "TUSD", "USDE", "USD1", "BUSD", "PYUSD", "USDP"}


def _f(x, d=0.0):
    try:
        return float(x)
    except (TypeError, ValueError):
        return d


def load_universe():
    uni = {}

    def put(base, ex, cid, mult, qv):
        if base in STABLES or not base:
            return
        u = uni.setdefault(base, {"base": base, "ex": {}, "qvol": 0.0})
        u["ex"][ex] = {"id": cid, "mult": mult, "qv": qv}
        u["qvol"] += qv

    # OKX
    ins = safe_get("okx", "/api/v5/public/instruments", {"instType": "SWAP"})
    tk = safe_get("okx", "/api/v5/market/tickers", {"instType": "SWAP"})
    if ins and tk and ins.get("code") == "0" and tk.get("code") == "0":
        meta = {i["instId"]: i for i in ins["data"]
                if i.get("state") == "live" and i["instId"].endswith("-USDT-SWAP")}
        for t in tk["data"]:
            i = meta.get(t["instId"])
            if not i:
                continue
            mult = _f(i.get("ctVal")) * (_f(i.get("ctMult"), 1.0) or 1.0)
            qv = _f(t.get("volCcy24h")) * _f(t.get("last"))
            if mult > 0:
                put(t["instId"].split("-")[0], "okx", t["instId"], mult, qv)

    # Gate
    cs = safe_get("gate", "/futures/usdt/contracts")
    tk = safe_get("gate", "/futures/usdt/tickers")
    if isinstance(cs, list) and isinstance(tk, list):
        meta = {c["name"]: c for c in cs
                if not c.get("in_delisting") and c.get("status", "trading") == "trading"}
        for t in tk:
            c = meta.get(t.get("contract"))
            if not c:
                continue
            mult = _f(c.get("quanto_multiplier"))
            qv = _f(t.get("volume_24h_quote")) or _f(t.get("volume_24h_settle"))
            if mult > 0 and c["name"].endswith("_USDT"):
                put(c["name"][:-5], "gate", c["name"], mult, qv)

    # Bitget
    tk = safe_get("bitget", "/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    if tk and tk.get("code") == "00000":
        for t in tk["data"]:
            s = t.get("symbol", "")
            if s.endswith("USDT"):
                qv = _f(t.get("usdtVolume")) or _f(t.get("quoteVolume"))
                put(s[:-4], "bitget", s, 1.0, qv)

    ranked = sorted(uni.values(), key=lambda u: u["qvol"], reverse=True)
    ranked = ranked[:TIER1_COUNT + TIER2_COUNT]
    for i, u in enumerate(ranked):
        u["tier"] = 1 if i < TIER1_COUNT else 2
        u["ref"] = max(u["ex"], key=lambda e: u["ex"][e]["qv"])
    per_ex = {e: sum(1 for u in ranked if e in u["ex"]) for e in HOSTS}
    print(f"یونیورس: {len(ranked)} کوائنز (T1={min(TIER1_COUNT, len(ranked))}) | ایکسچینج کوریج {per_ex}")
    return ranked


# ============================================================
# 🧾 اصل ٹریڈز: ہر فنکشن (items, gap) دیتا ہے، یا None (ناکامی)
#    item = (id, ts_ms, price, base_qty, is_buy_taker)
#    gap  = [start_ms, end_ms] اگر نئے ٹریڈز پورے نہ اٹھ سکے
# ============================================================
def _finish(tr, cur, truncated):
    if not tr:
        return [], None
    items = sorted(tr.values())
    gap = None
    if cur:
        if truncated and items[0][0] > cur["id"]:
            gap = [cur["ts"], items[0][1]]
        items = [t for t in items if t[0] > cur["id"]]
    return items, gap


def fetch_okx(info, cur, now_ms):
    iid, mult = info["id"], info["mult"]
    d = safe_get("okx", "/api/v5/market/trades", {"instId": iid, "limit": 500})
    if not d or d.get("code") != "0":
        return None
    tr = {}

    def add(rows):
        for r in rows:
            tid = int(r["tradeId"])
            tr[tid] = (tid, int(r["ts"]), float(r["px"]), float(r["sz"]) * mult, r["side"] == "buy")
    add(d["data"])
    truncated = len(d["data"]) >= 500
    if cur and truncated and tr and min(tr) > cur["id"]:
        for _ in range(PAGES):
            d2 = safe_get("okx", "/api/v5/market/history-trades",
                          {"instId": iid, "type": 1, "after": min(tr), "limit": 100})
            if not d2 or d2.get("code") != "0" or not d2["data"]:
                break
            add(d2["data"])
            if min(tr) <= cur["id"]:
                break
    return _finish(tr, cur, truncated)


def _gate_ms(r):
    v = r.get("create_time_ms")
    if v is None:
        return int(float(r["create_time"]) * 1000)
    v = float(v)
    return int(v if v > 1e12 else v * 1000)


def fetch_gate(info, cur, now_ms):
    mult = info["mult"]
    p = {"contract": info["id"], "limit": 1000}
    if cur:
        p["from"] = max(0, cur["ts"] // 1000 - 1)
    tr = {}
    truncated = False
    prev_min = None
    for page in range(PAGES):
        d = safe_get("gate", "/futures/usdt/trades", p)
        if not isinstance(d, list):
            if page == 0:
                return None
            break
        for r in d:
            tid = int(r["id"])
            size = float(r["size"])
            tr[tid] = (tid, _gate_ms(r), float(r["price"]), abs(size) * mult, size > 0)
        if len(d) < 1000:
            break
        if page == 0:
            truncated = True
        if not cur:
            break
        mn = min(int(r["id"]) for r in d)
        if mn <= cur["id"] or (prev_min is not None and mn >= prev_min):
            break
        prev_min = mn
        p["to"] = min(_gate_ms(r) for r in d) // 1000
        if p["to"] <= p["from"]:
            break
    return _finish(tr, cur, truncated)


def _bg_rows(d):
    if not d or d.get("code") != "00000":
        return None
    data = d.get("data")
    if isinstance(data, dict):
        data = data.get("fillList") or data.get("list") or []
    return data if isinstance(data, list) else None


def fetch_bitget(info, cur, now_ms):
    sym = info["id"]
    base = {"symbol": sym, "productType": "USDT-FUTURES"}
    tr = {}

    def add(rows):
        for r in rows:
            tid = int(r["tradeId"])
            tr[tid] = (tid, int(r["ts"]), float(r["price"]), float(r["size"]), r["side"] == "buy")

    if cur:
        p = dict(base, limit=1000, startTime=cur["ts"], endTime=now_ms)
        truncated = False
        ok = False
        for page in range(PAGES):
            rows = _bg_rows(safe_get("bitget", "/api/v2/mix/market/fills-history", p))
            if rows is None:
                break
            ok = True
            add(rows)
            if len(rows) < 1000:
                break
            if page == 0:
                truncated = True
            mn = min(int(r["tradeId"]) for r in rows)
            if mn <= cur["id"]:
                break
            p["idLessThan"] = mn
        if ok:
            return _finish(tr, cur, truncated)
    rows = _bg_rows(safe_get("bitget", "/api/v2/mix/market/fills", dict(base, limit=100)))
    if rows is None:
        return None
    add(rows)
    return _finish(tr, cur, len(rows) >= 100)


FETCHERS = {"okx": fetch_okx, "gate": fetch_gate, "bitget": fetch_bitget}


# ============================================================
# 🕯️ کینڈلز (صرف OHLC اور ریفرنس والیوم کے لیے؛ delta ٹریڈز سے آتا ہے)
#    واپسی: [[t_ms, o, h, l, c, vol_usd], ...] پرانی سے نئی
# ============================================================
IV = {"5m": {"okx": "5m", "gate": "5m", "bitget": "5m"},
      "15m": {"okx": "15m", "gate": "15m", "bitget": "15m"},
      "1h": {"okx": "1H", "gate": "1h", "bitget": "1H"},
      "4h": {"okx": "4H", "gate": "4h", "bitget": "4H"},
      "1d": {"okx": "1Dutc", "gate": "1d", "bitget": "1Dutc"}}


def get_candles(ex, cid, mult, interval, limit):
    iv = IV[interval][ex]
    out = []
    try:
        if ex == "okx":
            d = safe_get("okx", "/api/v5/market/candles", {"instId": cid, "bar": iv, "limit": min(limit, 300)})
            if not d or d.get("code") != "0":
                return None
            for r in reversed(d["data"]):
                out.append([int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7])])
        elif ex == "gate":
            d = safe_get("gate", "/futures/usdt/candlesticks", {"contract": cid, "interval": iv, "limit": limit})
            if not isinstance(d, list):
                return None
            for r in d:
                c = float(r["c"])
                usd = float(r["sum"]) if r.get("sum") not in (None, "") else float(r["v"]) * mult * c
                out.append([int(r["t"]) * 1000, float(r["o"]), float(r["h"]), float(r["l"]), c, usd])
        else:
            d = safe_get("bitget", "/api/v2/mix/market/candles",
                         {"symbol": cid, "productType": "USDT-FUTURES", "granularity": iv, "limit": limit})
            if not d or d.get("code") != "00000":
                return None
            for r in d["data"]:
                out.append([int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[6])])
            out.sort()
    except Exception as e:
        print(f"candles parse error {ex} {cid}: {e}")
        return None
    return out


# ============================================================
# 🗄️ فلو state: 5 منٹ کے خانے + کوریج کا حساب
#    bk[sym][bucket_ms] = [buy_usd, sell_usd, hi, lo, {price_bin: [buy, sell]}]
#    cur["ex|sym"] = {id, ts, since, fetched}   gaps["ex|sym"] = [[start, end], ...]
# ============================================================
def ingest(state, sym, ex, items, gap, now_ms):
    key = f"{ex}|{sym}"
    cur = state["cur"].get(key)
    if gap:
        stats["gaps"] += 1
        state["gaps"].setdefault(key, []).append(gap)
    if items:
        bks = state["bk"].setdefault(sym, {})
        for tid, ts, px, q, buy in items:
            b = bks.setdefault(str(ts // BUCKET_MS * BUCKET_MS), [0.0, 0.0, 0.0, 0.0, {}])
            usd = px * q
            b[0 if buy else 1] += usd
            if b[2] == 0 or px > b[2]:
                b[2] = px
            if b[3] == 0 or px < b[3]:
                b[3] = px
            e = b[4].setdefault(str(int(round(math.log(px) / LOGSTEP))), [0.0, 0.0])
            e[0 if buy else 1] += usd
        last = items[-1]
        state["cur"][key] = {"id": last[0], "ts": last[1], "fetched": now_ms,
                             "since": cur["since"] if cur else items[0][1]}
    elif cur:
        cur["fetched"] = now_ms
    else:
        state["cur"][key] = {"id": 0, "ts": now_ms, "since": now_ms, "fetched": now_ms}


def prune_state(state, now_ms):
    old = now_ms - RETAIN_MS
    nobin = now_ms - BINS_KEEP_MS
    for sym in list(state["bk"]):
        bks = state["bk"][sym]
        for k in list(bks):
            kt = int(k)
            if kt < old:
                del bks[k]
                continue
            b = bks[k]
            b[0], b[1] = round(b[0], 1), round(b[1], 1)
            if kt < nobin:
                b[4] = {}
            else:
                for e in b[4].values():
                    e[0], e[1] = round(e[0], 1), round(e[1], 1)
        if not bks:
            del state["bk"][sym]
    for key in list(state["gaps"]):
        state["gaps"][key] = [g for g in state["gaps"][key] if g[1] >= old]
        if not state["gaps"][key]:
            del state["gaps"][key]


def coverage(state, sym, info, a, b):
    """[a, b) کے لیے ٹریڈز کا مکمل تناسب، ایکسچینجز کے والیوم کے وزن سے۔"""
    span = max(b - a, 1)
    num = den = 0.0
    for ex, d in info["ex"].items():
        w = d["qv"] or 1.0
        den += w
        key = f"{ex}|{sym}"
        cur = state["cur"].get(key)
        if not cur:
            continue
        end = min(b, cur["fetched"])
        start = max(a, cur["since"])
        if end <= start:
            continue
        covered = end - start
        for g0, g1 in state["gaps"].get(key, []):
            lo_, hi_ = max(g0, start), min(g1, end)
            if hi_ > lo_:
                covered -= hi_ - lo_
        num += w * max(covered, 0) / span
    return num / den if den else 0.0


def bar_sum(state, sym, a, b):
    """[a, b) کے خانوں کا مجموعہ: (buy, sell, bins{idx: [b, s]})"""
    buy = sell = 0.0
    bins = {}
    for k, v in state["bk"].get(sym, {}).items():
        kt = int(k)
        if a <= kt < b:
            buy += v[0]
            sell += v[1]
            for idx, (bb, ss) in v[4].items():
                e = bins.setdefault(idx, [0.0, 0.0])
                e[0] += bb
                e[1] += ss
    return buy, sell, bins


def bar_delta(state, sym, info, a, b):
    if coverage(state, sym, info, a, b) < MIN_COVERAGE:
        return None
    buy, sell, _ = bar_sum(state, sym, a, b)
    return buy - sell if buy + sell > 0 else None


def build_profile_real(bins, lo, hi, nb):
    step = (hi - lo) / nb
    buy = np.zeros(nb)
    sell = np.zeros(nb)
    for idx, (bb, ss) in bins.items():
        p = math.exp(int(idx) * LOGSTEP)
        i = min(max(int((p - lo) / step), 0), nb - 1)
        buy[i] += bb
        sell[i] += ss
    return buy, sell


# ============================================================
# 🧭 سیٹ اپس (منطق وہی، مگر delta/footprint اصل ٹریڈز سے)
#    hist = [[t, o, h, l, c, vol_usd], ...]  (آخری = جاری بار)
# ============================================================
def has_run(mask, n):
    run = 0
    for m in mask:
        run = run + 1 if m else 0
        if run >= n:
            return True
    return False


def eval_setups(hist, buy_usd, sell_usd, profile, tf, progress, prev_deltas):
    cfg = OF_TF[tf]
    bar, prev = hist[-1], hist[:-1]
    o, h, l, c, v = bar[1], bar[2], bar[3], bar[4], bar[5]
    tot_usd = buy_usd + sell_usd
    if tot_usd <= 0 or h <= l or not prev:
        return None

    avg_vol = statistics.mean(k[5] for k in prev)
    elapsed = min(1.0, max(float(progress), 1.0 / max(cfg["minutes"], 1)))
    vol_ratio = v / (avg_vol * elapsed) if avg_vol > 0 else 0.0
    delta = buy_usd - sell_usd
    delta_pct = delta / tot_usd * 100
    ratio = buy_usd / max(sell_usd, 1e-12)
    close_pos = (c - l) / (h - l)

    buy, sell = profile
    tot = buy + sell
    if tot.sum() <= 0:
        return None
    poc_pos = (int(tot.argmax()) + 0.5) / OF_BUCKETS
    vol_ok = vol_ratio >= OF_VOL_MULT

    look = prev[-OF_LOOKBACK:]
    ph = max(k[2] for k in look)
    pl = min(k[3] for k in look)
    found = {"BULL": [], "BEAR": []}

    if (delta_pct >= cfg["min_delta_pct"] and ratio > 1.0 and poc_pos >= OF_POC_BULL
            and close_pos >= OF_CLOSE_BULL and vol_ok):
        found["BULL"].append("Delta+POC")
    if (delta_pct <= -cfg["min_delta_pct"] and ratio < 1.0 and poc_pos <= OF_POC_BEAR
            and close_pos <= OF_CLOSE_BEAR and vol_ok):
        found["BEAR"].append("Delta+POC")

    if vol_ratio >= OF_ABS_VOL and delta_pct <= -OF_ABS_VOL and delta_pct <= -OF_ABS_DELTA and c >= o and close_pos >= 0.55:
        found["BULL"].append("Absorption")
    if vol_ratio >= OF_ABS_VOL and delta_pct >= OF_ABS_DELTA and c <= o and close_pos <= 0.45:
        found["BEAR"].append("Absorption")

    if l < pl and c > pl and close_pos >= 0.6 and delta_pct > 0 and vol_ok:
        found["BULL"].append("Sweep")
    if h > ph and c < ph and close_pos <= 0.4 and delta_pct < 0 and vol_ok:
        found["BEAR"].append("Sweep")

    nb5 = OF_BUCKETS // 5
    low_zone = tot[:nb5].sum() / tot.sum()
    high_zone = tot[-nb5:].sum() / tot.sum()
    if l <= pl and low_zone <= OF_EXH_THIN and close_pos >= 0.5 and vol_ok:
        found["BULL"].append("Exhaustion")
    if h >= ph and high_zone <= OF_EXH_THIN and close_pos <= 0.5 and vol_ok:
        found["BEAR"].append("Exhaustion")

    # CVD Divergence: صرف جب پچھلی تمام بارز کا اصل delta دستیاب ہو
    if (prev_deltas is not None and len(prev_deltas) == len(look)
            and all(d is not None for d in prev_deltas)):
        ds = np.array(list(prev_deltas) + [delta])
        cvd = np.cumsum(ds)
        i_hi = int(np.argmax([k[2] for k in look]))
        i_lo = int(np.argmin([k[3] for k in look]))
        margin = OF_DIV_MIN * float(np.mean(np.abs(ds[:-1])))
        if h >= ph and cvd[i_hi] - cvd[-1] >= margin and vol_ok:
            found["BEAR"].append("Divergence")
        if l <= pl and cvd[-1] - cvd[i_lo] >= margin and vol_ok:
            found["BULL"].append("Divergence")

    if abs(delta_pct) >= OF_STACK_MIN_DELTA:
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


def check_orderflow(info, now_ms, state):
    sym = info["base"]
    out = []
    ex_ref = info["ref"]
    ref = info["ex"][ex_ref]
    for tf, cfg in OF_TF.items():
        ms = cfg["minutes"] * 60_000
        start_ms = now_ms // ms * ms
        progress = (now_ms - start_ms) / ms
        if progress < cfg["min_progress"]:
            continue
        if tf == "1d" and info["qvol"] < DAILY_MIN_QUOTE_VOL:
            continue
        if coverage(state, sym, info, start_ms, now_ms) < MIN_COVERAGE:
            stats["low_cov"] += 1
            continue
        buy_usd, sell_usd, bins = bar_sum(state, sym, start_ms, now_ms)
        if buy_usd + sell_usd < MIN_BAR_USD:
            continue

        hist = get_candles(ex_ref, ref["id"], ref["mult"], tf, 21)
        if not hist or len(hist) < 4 or hist[-1][0] != start_ms:
            stats["no_candles"] += 1
            continue
        hi, lo = hist[-1][2], hist[-1][3]
        if hi <= lo:
            continue

        prev_deltas = None
        if cfg["div"]:
            look = hist[:-1][-OF_LOOKBACK:]
            prev_deltas = [bar_delta(state, sym, info, k[0], k[0] + ms) for k in look]

        res = eval_setups(hist, buy_usd, sell_usd, build_profile_real(bins, lo, hi, OF_BUCKETS),
                          tf, progress, prev_deltas)
        if not res:
            continue
        found, text = res
        if not (found["BULL"] or found["BEAR"]):
            continue
        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        print(f"   OF {sym} {tf} ({progress:.0%}) [real]: {text} → {', '.join(tags)}")
        refinfo = (ex_ref, ref["id"], ref["mult"])
        for side, names in found.items():
            if names:
                out.append((tf, side, hist[-1][4], names, start_ms, "real", refinfo))
    return out


# ============================================================
# 📲 Ntfy
# ============================================================
def post_ntfy(title, body, tags="rotating_light", url=None, priority="high"):
    try:
        r = requests.post(url or NFTY_URL, data=body.encode("utf-8"),
                          headers={"Title": title, "Priority": priority, "Tags": tags}, timeout=15)
        print(f"Ntfy [{title}]: {r.status_code}")
        return r.ok
    except Exception as e:
        print(f"Ntfy Error: {e}")
        return False


def run_orderflow(ranked, state):
    now_ms = int(time.time() * 1000)
    print(f"\nآرڈر فلو: {len(ranked)} کوائنز | ٹائم فریم {list(OF_TF)}")
    sent_state = {k: v for k, v in load_json(OF_STATE_FILE).items()
                  if isinstance(v, (int, float)) and v >= now_ms - 2 * 24 * 3600 * 1000}

    def work(info):
        try:
            return info, check_orderflow(info, now_ms, state)
        except Exception as e:
            print(f"   OF {info['base']} error: {e}")
            return info, []

    alerts = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for info, res in ex.map(work, ranked):
            sym = info["base"]
            for tf, side, price, names, start_ms, mode, refinfo in res:
                new = [n for n in names if f"{sym}|{tf}|{side}|{start_ms}|{n}" not in sent_state]
                if new:
                    alerts.setdefault((tf, side), []).append(
                        (sym, price, new, start_ms, mode, refinfo, info["tier"]))

    if not alerts:
        print("آرڈر فلو: کوئی نیا الرٹ نہیں")
        save_json(OF_STATE_FILE, sent_state)
        return

    to_track = []
    for (tf, side), items in sorted(alerts.items()):
        name = "Bullish" if side == "BULL" else "Bearish"
        for i in range(0, len(items), NTFY_CHUNK):
            chunk = items[i:i + NTFY_CHUNK]
            lines = [f"{s} {fmt_price(p)} | {' + '.join(new)}{' (small)' if tier == 2 else ''}"
                     for s, p, new, _, _, _, tier in chunk]
            if post_ntfy(f"{tf.upper()} {name} Futures Order Flow", "\n".join(lines),
                         tags="green_circle" if side == "BULL" else "red_circle", url=NFTY_OF_URL):
                for s, p, new, start_ms, mode, refinfo, _ in chunk:
                    for n in new:
                        sent_state[f"{s}|{tf}|{side}|{start_ms}|{n}"] = start_ms
                    to_track.append((tf, side, s, p, new, start_ms, mode, refinfo))
    save_json(OF_STATE_FILE, sent_state)
    try:
        register_of_trades(to_track, now_ms)
    except Exception as e:
        print(f"ٹریکر رجسٹریشن error: {e}")


# ============================================================
# 📈 ٹریکر: MFE / MAE / مدت کے بعد سمت (منطق وہی، کینڈلز ریفرنس ایکسچینج سے)
# ============================================================
def load_book():
    d = load_json(TRADES_FILE)
    return {"open": [t for t in d.get("open", []) if "tf" in t and "ref" in t],
            "closed": [t for t in d.get("closed", []) if "tf" in t],
            "last_report_day": d.get("last_report_day")}


def register_of_trades(entries, now_ms):
    if not entries:
        return
    book = load_book()
    idx = {t["id"]: t for t in book["open"]}
    for tf, side, sym, price, names, start_ms, mode, refinfo in entries:
        tid = f"{sym}|{tf}|{side}|{start_ms}"
        if tid in idx:
            for n in names:
                if n not in idx[tid]["setups"]:
                    idx[tid]["setups"].append(n)
            continue
        tr = {"id": tid, "symbol": sym, "tf": tf, "side": side, "setups": list(names),
              "mode": mode, "entry": price, "ref": list(refinfo),
              "bar_start_ms": start_ms, "opened_ms": now_ms,
              "expires_ms": now_ms + OF_TRACK[tf]["hours"] * 3600 * 1000}
        book["open"].append(tr)
        idx[tid] = tr
    save_json(TRADES_FILE, book)


def evaluate_of_trade(tr, now_ms):
    if now_ms < tr["expires_ms"]:
        return None
    cfg = OF_TRACK[tr["tf"]]
    step_ms = cfg["step_ms"]
    first = (tr["opened_ms"] // step_ms + 1) * step_ms
    ex, cid, mult = tr["ref"]
    kl = get_candles(ex, cid, mult, cfg["step"], 300)
    if kl is None:
        return None
    kl = [k for k in kl if first <= k[0] and k[0] + step_ms <= tr["expires_ms"]]
    expected = (tr["expires_ms"] - first) // step_ms
    if len(kl) < max(1, int(expected * 0.8)):
        return "DROP" if now_ms > tr["expires_ms"] + 2 * 86_400_000 else None

    entry = tr["entry"]
    top = max(k[2] for k in kl)
    bottom = min(k[3] for k in kl)
    last = kl[-1][4]
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
        rows = [t for t in closed if t["tf"] == tf][-SUMMARY_LAST_N:]
        if rows:
            lines.append(group_line(tf, rows))
    return "\n".join(lines) or "No closed trades yet"


def setup_report(closed):
    names = sorted({n for t in closed for n in t["setups"]})
    lines = ["By setup (all kept results, * = small sample)"]
    for tf in OF_TF:
        for n in names:
            rows = [t for t in closed if t["tf"] == tf and n in t["setups"]]
            if rows:
                lines.append(group_line(f"{tf} {n}", rows))
    lines += ["", "By timeframe (last results)", summary_text(closed)]
    return "\n".join(lines)


def result_line(t):
    side = "Bull" if t["side"] == "BULL" else "Bear"
    return (f"{t['symbol']} {t['tf']} {side} {'+'.join(t['setups'])} | "
            f"MFE +{t['mfe_pct']:.1f}% MAE -{t['mae_pct']:.1f}% | end {t['ret_pct']:+.1f}%")


def track_of_trades():
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
        if post_ntfy("OF daily report", setup_report(book["closed"]),
                     tags="chart_with_upwards_trend", url=NFTY_OF_URL, priority="default"):
            book["last_report_day"] = today
    save_json(TRADES_FILE, book)


# ============================================================
# 🧪 selftest: python orderflow_futures.py --selftest
#    ہر ایکسچینج سے BTC کے ٹریڈز اور کینڈلز اٹھا کر فیلڈز کی تصدیق
# ============================================================
def selftest():
    ranked = load_universe()
    btc = next((u for u in ranked if u["base"] == "BTC"), None)
    if not btc:
        print("BTC یونیورس میں نہیں ملا")
        return
    now_ms = int(time.time() * 1000)
    for ex, d in btc["ex"].items():
        r = FETCHERS[ex](d, None, now_ms)
        if r is None:
            print(f"[{ex}] ٹریڈز: ناکام")
        else:
            items, _ = r
            print(f"[{ex}] ٹریڈز: {len(items)} | آخری 2: {items[-2:]}")
        c = get_candles(ex, d["id"], d["mult"], "1h", 3)
        print(f"[{ex}] کینڈلز: {c}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    if not NFTY_URL:
        print("NFTY_URL set نہیں ہے")
        raise SystemExit(1)

    t0 = time.time()
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | فیوچرز آرڈر فلو | OKX+Gate+Bitget")
    print("=" * 60)

    ranked = load_universe()
    if not ranked:
        print("مارکیٹس نہیں ملیں")
        return

    state = load_gz(FLOW_FILE)
    for k in ("cur", "gaps", "bk"):
        state.setdefault(k, {})

    # 1) نئے ٹریڈز اٹھانا (ریٹ لمٹ مشترکہ limiter سے)
    now_ms = int(time.time() * 1000)
    jobs = [(u["base"], ex, d) for u in ranked for ex, d in u["ex"].items()]
    print(f"ٹریڈز: {len(jobs)} (ایکسچینج × کوئن) درخواستیں")

    def fetch(job):
        sym, ex, d = job
        if time.time() - t0 > FETCH_BUDGET_S:
            stats["fetch_skipped"] += 1
            return job, "SKIP"
        try:
            return job, FETCHERS[ex](d, state["cur"].get(f"{ex}|{sym}"), now_ms)
        except Exception as e:
            print(f"   {ex} {sym} fetch error: {e}")
            return job, None

    with ThreadPoolExecutor(max_workers=12) as pool:
        for (sym, ex, d), res in pool.map(fetch, jobs):
            if res is None or res == "SKIP":
                continue
            items, gap = res
            ingest(state, sym, ex, items, gap, now_ms)

    prune_state(state, now_ms)
    save_gz(FLOW_FILE, state)
    print(f"ٹریڈز مکمل: {time.time() - t0:.0f}s")

    # 2) الرٹس
    try:
        run_orderflow(ranked, state)
    except Exception as e:
        print(f"آرڈر فلو error: {e}")

    # 3) ٹریکر
    try:
        track_of_trades()
    except Exception as e:
        print(f"ٹریکر error: {e}")

    print(f"\nاعداد: {stats}")
    print(f"مکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
