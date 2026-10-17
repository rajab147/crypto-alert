import os
import sys
import json
import gzip
import math
import time
import itertools
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
TIER1_COUNT = 100           # پہلے 100 زیادہ لیکویڈ کوائنز
TIER2_COUNT = 20            # ان کے علاوہ زیادہ سے زیادہ 20 کموڈٹی سے متعلق ٹوکنز
MIN_EXCHANGE_QUOTE_VOL = 5_000_000  # کم از کم ایک ایکسچینج پر 24h والیوم $5M
MIN_COMMODITY_QUOTE_VOL = 1_000_000 # کموڈٹی ٹوکن کے لیے کم از کم ایک ایکسچینج پر $1M
DAILY_MIN_QUOTE_VOL = 30_000_000
MIN_BAR_USD = 150_000       # بار میں اس سے کم کل ٹریڈ والیوم پر الرٹ نہیں (پتلے ڈیٹا کا شور)

# صرف امیدوار نام: load_universe میں وہی شامل ہوں گے جو OKX/Gate/Bitget پر
# واقعی USDT perpetual کے طور پر ملیں اور والیوم شرط پوری کریں۔
COMMODITY_BASES = {
    "PAXG", "XAUT", "KAU", "DGLD", "PMGT", "GOLD", "XAU", "XAUM",
    "XAG", "SILVER", "SLV", "OIL", "WTI", "BRENT", "COPPER",
    "XPT", "PLATINUM", "XPD", "PALLADIUM", "URANIUM"
}

OF_TF = {
    "1h": {"minutes": 60,   "min_delta_pct": 6.0, "min_progress": 0.35, "div": True},
    "4h": {"minutes": 240,  "min_delta_pct": 5.0, "min_progress": 0.15, "div": True},
    "1d": {"minutes": 1440, "min_delta_pct": 4.0, "min_progress": 0.10, "div": False},
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
OF_MAX_DELTA_PCT = 95.0     # |delta| اس سے اوپر ہو تو ڈیٹا پتلا سمجھ کر رد (لاگ میں -100% والے الرٹ)
OPP_COOLDOWN_MS = 3 * 3600_000  # اسی کوائن پر مخالف سمت کا الرٹ حال ہی میں آیا ہو تو نیا الرٹ نہیں

# ---------- CVD صفر لائن + CVD Early (بار کے انتظار کے بغیر) ----------
# CVD Zero : CVD ایک anchor سے صفر پر ری سیٹ ہوتا ہے (1h/4h: UTC دن، 1d: UTC ہفتہ؛ فیوچرز 1d: پچھلا دن)
#            اور صفر لائن کو (کافی فاصلے سے) اوپر/نیچے کراس کرے تو سگنل۔
# CVD Early: پچھلی ونڈو میں CVD الٹ سمت میں تھا، اب پلٹ گیا، مگر قیمت ابھی زیادہ نہیں چلی
#            (یعنی فلو قیمت سے آگے ہے)۔ min_progress کی شرط نہیں، اس لیے بار کے شروع میں بھی چلتا ہے۔
CVD_ALERTS = True           # False: CVD Zero/Early الرٹ نہ بھیجیں، صرف دوسرے الرٹس پر CVD✓/✗ نشان
CVD_CFG = {
    "1h": {"win": 3, "step": "15m", "step_ms": 900_000,   "anchor": "day"},   # ونڈو 45 منٹ
    "4h": {"win": 8, "step": "15m", "step_ms": 900_000,   "anchor": "day"},   # ونڈو 2 گھنٹے
    "1d": {"win": 8, "step": "1h",  "step_ms": 3_600_000, "anchor": "week"},  # ونڈو 8 گھنٹے
}
CVD_ZERO_MARGIN = 0.6       # صفر سے اتنا دور (ایک سٹیپ کے اوسط فلو کا حصہ) ہو تو کراس مانا جائے
CVD_EARLY_MIN_PCT = 8.0     # موجودہ ونڈو میں net delta (کل فلو کا %)
CVD_EARLY_PREV_PCT = 3.0    # پچھلی ونڈو میں مخالف net delta کم از کم %
CVD_EARLY_LAG = 0.6         # قیمت کی ونڈو حرکت، عام ونڈو حرکت کے اس حصے سے کم ہو
# CVD Retest: رجحان کے ساتھ انٹری۔ CVD صفر سے واضح دور تھا، پلٹ کر صفر لائن کے قریب (±TOUCH) پہنچا،
#             اسے توڑا نہیں، اور واپس اپنی سمت میں مڑ گیا (صفر لائن سپورٹ/ریزسٹنس بنی)۔ Bull اور Bear دونوں۔
CVD_RETEST_TOUCH = 0.35     # صفر کے اتنے (ایک سٹیپ کے اوسط فلو کا حصہ) قریب پہنچے تو "ٹچ"
CVD_RETEST_BOUNCE = 0.5     # ٹچ کے بعد واپس اتنا مڑے تو رد ہونا مانا جائے
CVD_NAMES = ("CVD Zero", "CVD Early", "CVD Retest")

# ---------- Binance سپاٹ (data-api.binance.vision) ----------
ENABLE_FUTURES = True       # OKX + Gate + Bitget فیوچرز
ENABLE_SPOT = True          # Binance سپاٹ
SPOT_COUNT = 200            # 24h والیوم میں پہلے اتنے USDT سپاٹ کوائنز
SPOT_MIN_QUOTE_VOL = 5_000_000       # کم از کم 24h والیوم
SPOT_DAILY_MIN_QUOTE_VOL = 10_000_000
SPOT_MIN_BAR_USD = 150_000  # بار میں کم از کم ٹریڈ والیوم
OF_MAX_TRADE_PAGES = 60     # aggTrades کے زیادہ سے زیادہ صفحے (ہر صفحہ 1000 ٹریڈز)
OF_MIN_REAL_TRADES = 200
OF_VOL_MATCH = (0.85, 1.25)  # aggTrades کا کل والیوم kline والیوم کے قریب ہو
TOTAL_BUDGET_S = 400        # اس وقت کے بعد سپاٹ کی باقی جانچ چھوڑ دیں (ورک فلو 9-10 منٹ میں ختم ہو)
RUN_T0 = time.time()

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
         "bitget": "https://api.bitget.com",
         "binance": "https://data-api.binance.vision"}


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
LIM = {"okx": Limiter(9), "gate": Limiter(12), "bitget": Limiter(8), "binance": Limiter(15)}
session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "gaps": 0, "low_cov": 0, "no_candles": 0,
         "fetch_skipped": 0, "blocked": 0, "spot_skipped": 0, "too_heavy": 0,
         "mismatch": 0, "confirmed": 0, "rejected": 0, "unverified": 0}


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
            if r.status_code in (400, 451):
                # غلط درخواست یا جغرافیائی پابندی: دوبارہ کوشش بے کار، وقت ضائع نہ کریں
                stats["failed"] += 1
                if r.status_code == 451:
                    stats["blocked"] += 1
                return None
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

    all_ranked = sorted(uni.values(), key=lambda u: u["qvol"], reverse=True)

    # پہلے زیادہ لیکویڈ عام کوائنز؛ کم والیوم والے ٹوکن خودکار طور پر باہر۔
    liquid = [u for u in all_ranked
              if u["base"] not in COMMODITY_BASES
              and max((d["qv"] for d in u["ex"].values()), default=0) >= MIN_EXCHANGE_QUOTE_VOL]
    # اگر $5M فلٹر کے بعد 100 سے کم ہوں تو اگلے سب سے بہتر دستیاب کوائنز سے جگہ پُر کریں،
    # مگر انتہائی کم والیوم والے (ہر ایکسچینج پر $2M سے کم) شامل نہ ہوں۔
    if len(liquid) < TIER1_COUNT:
        fallback = [u for u in all_ranked
                    if u["base"] not in COMMODITY_BASES
                    and max((d["qv"] for d in u["ex"].values()), default=0) >= 2_000_000]
        seen = {u["base"] for u in liquid}
        liquid.extend(u for u in fallback if u["base"] not in seen)
    selected_coins = liquid[:TIER1_COUNT]

    # کموڈٹی امیدواروں میں سے صرف حقیقی listed USDT perpetuals شامل ہوں گے۔
    commodities = [u for u in all_ranked
                   if u["base"] in COMMODITY_BASES
                   and max((d["qv"] for d in u["ex"].values()), default=0) >= MIN_COMMODITY_QUOTE_VOL]
    commodities = commodities[:TIER2_COUNT]

    ranked = selected_coins + commodities
    for i, u in enumerate(ranked):
        u["tier"] = 1 if i < len(selected_coins) else 2
        u["ref"] = max(u["ex"], key=lambda e: u["ex"][e]["qv"])
    per_ex = {e: sum(1 for u in ranked if e in u["ex"]) for e in ("okx", "gate", "bitget")}
    commodity_count = sum(1 for u in ranked if u["base"] in COMMODITY_BASES)
    print(f"یونیورس: {len(selected_coins)} لیکویڈ کوائنز + {commodity_count} دستیاب کموڈٹی ٹوکنز "
          f"(زیادہ سے زیادہ {TIER2_COUNT}) | ایکسچینج کوریج {per_ex}")
    if commodity_count < TIER2_COUNT:
        print(f"نوٹ: 20 کموڈٹی ٹوکنز میں سے صرف {commodity_count} اس وقت supported exchanges پر "
              "USDT perpetual اور والیوم شرط کے ساتھ ملے؛ غیر موجود مارکیٹس فرضی طور پر شامل نہیں کی گئیں۔")
    if len(selected_coins) < TIER1_COUNT:
        print(f"نوٹ: فلٹر کے بعد {TIER1_COUNT} کے بجائے {len(selected_coins)} قابلِ قبول کوائنز ملے۔")
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
IV = {"5m": {"okx": "5m", "gate": "5m", "bitget": "5m", "binance": "5m"},
      "15m": {"okx": "15m", "gate": "15m", "bitget": "15m", "binance": "15m"},
      "1h": {"okx": "1H", "gate": "1h", "bitget": "1H", "binance": "1h"},
      "4h": {"okx": "4H", "gate": "4h", "bitget": "4H", "binance": "4h"},
      "1d": {"okx": "1Dutc", "gate": "1d", "bitget": "1Dutc", "binance": "1d"}}


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
        elif ex == "binance":
            d = safe_get("binance", "/api/v3/klines",
                         {"symbol": cid, "interval": iv, "limit": min(limit, 1000)})
            if not isinstance(d, list):
                return None
            for r in d:
                out.append([int(r[0]), float(r[1]), float(r[2]), float(r[3]), float(r[4]), float(r[7])])
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
    if abs(delta_pct) > OF_MAX_DELTA_PCT:
        return None
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

    if vol_ratio >= OF_ABS_VOL and delta_pct <= -OF_ABS_DELTA and c >= o and close_pos >= 0.55:
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

    if abs(delta_pct) >= OF_STACK_MIN_DELTA and vol_ok:
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


# ============================================================
# 🧲 CVD صفر لائن اور CVD Early (فیوچرز اور سپاٹ دونوں کے لیے مشترکہ منطق)
#    steps = [(t, delta_usd, flow_usd, price), ...] پرانا→نیا، آخری سٹیپ جاری ہے
# ============================================================
def cvd_anchor(tf, now_ms, fut=False):
    day = now_ms // 86_400_000 * 86_400_000
    if CVD_CFG[tf]["anchor"] == "week" and not fut:
        wd = (now_ms // 86_400_000 + 3) % 7        # 1970-01-01 جمعرات تھی، پیر = 0
        return day - wd * 86_400_000
    if tf == "1d":                                   # فیوچرز میں ہفتہ وار ڈیٹا نہیں رکھا جاتا
        return day - 86_400_000
    return day


def _cvd_cross(cv, ts, margin, win, bar_start, up):
    """cv صفر سے نیچے سے اوپر (up=True) یا اوپر سے نیچے گیا؟ کراس موجودہ ٹائم فریم بار کے اندر ہو۔"""
    sg = 1 if up else -1
    x = [sg * v for v in cv]
    if x[-1] < margin:
        return False
    z = None
    for i in range(len(x) - 1, -1, -1):
        if x[i] <= 0:
            z = i
            break
    if z is None or z + 1 >= len(ts):
        return False
    if ts[z + 1] < bar_start:
        return False
    return min(x[max(0, z - win):z + 1]) <= -margin


def _cvd_retest(cv, ts, typ, win, bar_start, up):
    """up=True: CVD اوپر کے رجحان میں تھا، صفر تک گر کر ٹچ ہوا اور واپس اوپر مڑا (Bull)۔
    up=False: آئینہ (Bear)۔ ٹچ موجودہ ٹائم فریم بار کے اندر ہو۔"""
    sg = 1 if up else -1
    x = [sg * v for v in cv]
    touch, bounce, margin = CVD_RETEST_TOUCH * typ, CVD_RETEST_BOUNCE * typ, CVD_ZERO_MARGIN * typ
    n = len(x)
    seg = x[-(win + 1):]
    lo = min(seg)
    if lo < -touch or lo > touch:           # صفر کے قریب نہیں پہنچا، یا اسے توڑ گیا
        return False
    i = n - len(seg) + seg.index(lo)
    if i < 1 or ts[i] < bar_start or x[i - 1] <= lo:
        return False
    if x[-1] < touch or x[-1] - lo < bounce:  # واپس مڑا نہیں
        return False
    before = x[max(0, i - 2 * win):i]
    return bool(before) and max(before) >= margin   # ٹچ سے پہلے واضح رجحان


def cvd_analyze(steps, anchor_ms, win, bar_start_ms, min_flow):
    """واپسی: {"sign": +1/-1/0/None, "cross": سمت/None, "early": سمت/None, "retest": سمت/None}"""
    out = {"sign": None, "cross": None, "early": None, "retest": None}
    anch = [s for s in steps if s[0] >= anchor_ms]
    if len(anch) >= win + 2:
        flows = [s[2] for s in anch]
        typ = sum(flows) / len(flows)
        if typ > 0:
            margin = CVD_ZERO_MARGIN * typ
            cv = list(itertools.accumulate(s[1] for s in anch))
            ts = [s[0] for s in anch]
            out["sign"] = 1 if cv[-1] >= margin else (-1 if cv[-1] <= -margin else 0)
            if sum(flows[-win:]) >= min_flow:
                up = _cvd_cross(cv, ts, margin, win, bar_start_ms, True)
                dn = _cvd_cross(cv, ts, margin, win, bar_start_ms, False)
                if up and not dn:
                    out["cross"] = "BULL"
                elif dn and not up:
                    out["cross"] = "BEAR"
                rb = _cvd_retest(cv, ts, typ, win, bar_start_ms, True)
                rs = _cvd_retest(cv, ts, typ, win, bar_start_ms, False)
                if rb and not rs:
                    out["retest"] = "BULL"
                elif rs and not rb:
                    out["retest"] = "BEAR"

    n = len(steps)
    if n >= 2 * win + 1:
        cur, prev = steps[-win:], steps[-2 * win:-win]
        d_now, f_now = sum(s[1] for s in cur), sum(s[2] for s in cur)
        d_prev, f_prev = sum(s[1] for s in prev), sum(s[2] for s in prev)
        px = [s[3] for s in steps]
        if f_now >= min_flow and f_prev > 0 and all(p > 0 for p in px):
            moves = [abs(px[i] / px[i - win] - 1) * 100 for i in range(win, n)]
            vol_win = sum(moves) / len(moves) if moves else 0.0
            mv = (px[-1] / px[-1 - win] - 1) * 100
            pct_now, pct_prev = d_now / f_now * 100, d_prev / f_prev * 100
            if vol_win > 0:
                if (pct_prev <= -CVD_EARLY_PREV_PCT and pct_now >= CVD_EARLY_MIN_PCT
                        and mv <= CVD_EARLY_LAG * vol_win):
                    out["early"] = "BULL"
                elif (pct_prev >= CVD_EARLY_PREV_PCT and pct_now <= -CVD_EARLY_MIN_PCT
                        and mv >= -CVD_EARLY_LAG * vol_win):
                    out["early"] = "BEAR"
    out["price"] = steps[-1][3] if steps else None
    return out


def cvd_tag(cv, side):
    """موجودہ الرٹ کی سمت کے مطابق CVD: '+' موافق، '-' مخالف، '?' نامعلوم/صفر کے قریب"""
    if not cv or cv.get("sign") in (None, 0):
        return "?"
    return "+" if cv["sign"] == (1 if side == "BULL" else -1) else "-"


def merge_cvd(found, cv):
    if cv and CVD_ALERTS:
        for key, name in (("cross", "CVD Zero"), ("early", "CVD Early"), ("retest", "CVD Retest")):
            side = cv.get(key)
            if side:
                found[side].append(name)
    if found["BULL"] and found["BEAR"]:
        return {"BULL": [], "BEAR": []}
    return found


def fut_cvd_steps(state, sym, step_ms, start_ms, now_ms):
    acc = {}
    for k, v in state["bk"].get(sym, {}).items():
        kt = int(k)
        if kt < start_ms or kt > now_ms:
            continue
        s = kt // step_ms * step_ms
        e = acc.setdefault(s, [0.0, 0.0, 0, 0.0])
        e[0] += v[0] - v[1]
        e[1] += v[0] + v[1]
        if v[2] > 0 and v[3] > 0 and kt >= e[2]:
            e[2] = kt
            e[3] = (v[2] + v[3]) / 2
    steps, px = [], 0.0
    t = start_ms // step_ms * step_ms
    end = now_ms // step_ms * step_ms
    while t <= end:
        e = acc.get(t)
        if e and e[3] > 0:
            px = e[3]
        steps.append((t, e[0] if e else 0.0, e[1] if e else 0.0, px))
        t += step_ms
    return [s for s in steps if s[3] > 0]


def fut_cvd(state, sym, info, tf, now_ms):
    cfg = CVD_CFG[tf]
    step_ms, win = cfg["step_ms"], cfg["win"]
    sinces = [state["cur"].get(f"{ex}|{sym}", {}).get("since") for ex in info["ex"]]
    sinces = [s for s in sinces if s]
    if not sinces:
        return None
    anchor = max(cvd_anchor(tf, now_ms, fut=True), min(sinces))
    series_start = min(anchor, now_ms - (2 * win + 3) * step_ms)
    if coverage(state, sym, info, series_start, now_ms) < MIN_COVERAGE:
        return None
    steps = fut_cvd_steps(state, sym, step_ms, series_start, now_ms)
    bar_start = now_ms // (OF_TF[tf]["minutes"] * 60_000) * (OF_TF[tf]["minutes"] * 60_000)
    return cvd_analyze(steps, anchor, win, bar_start, MIN_BAR_USD)


def fut_setups(info, state, tf, cfg, now_ms):
    """بار کے سیٹ اپس (Delta+POC وغیرہ)۔ شرط پوری نہ ہو تو None۔ واپسی: (found, text, close)"""
    sym = info["base"]
    ex_ref = info["ref"]
    ref = info["ex"][ex_ref]
    ms = cfg["minutes"] * 60_000
    start_ms = now_ms // ms * ms
    progress = (now_ms - start_ms) / ms
    if progress < cfg["min_progress"]:
        return None
    if coverage(state, sym, info, start_ms, now_ms) < MIN_COVERAGE:
        stats["low_cov"] += 1
        return None
    buy_usd, sell_usd, bins = bar_sum(state, sym, start_ms, now_ms)
    if buy_usd + sell_usd < MIN_BAR_USD:
        return None

    hist = get_candles(ex_ref, ref["id"], ref["mult"], tf, 21)
    if not hist or len(hist) < 4 or hist[-1][0] != start_ms:
        stats["no_candles"] += 1
        return None
    hi, lo = hist[-1][2], hist[-1][3]
    if hi <= lo:
        return None

    prev_deltas = None
    if cfg["div"]:
        look = hist[:-1][-OF_LOOKBACK:]
        prev_deltas = [bar_delta(state, sym, info, k[0], k[0] + ms) for k in look]

    res = eval_setups(hist, buy_usd, sell_usd, build_profile_real(bins, lo, hi, OF_BUCKETS),
                      tf, progress, prev_deltas)
    if not res:
        return None
    found, text = res
    return found, f"{text} | prog {progress:.0%}", hist[-1][4]


def check_orderflow(info, now_ms, state):
    sym = info["base"]
    out = []
    ex_ref = info["ref"]
    ref = info["ex"][ex_ref]
    refinfo = (ex_ref, ref["id"], ref["mult"])
    for tf, cfg in OF_TF.items():
        if tf == "1d" and info["qvol"] < DAILY_MIN_QUOTE_VOL:
            continue
        ms = cfg["minutes"] * 60_000
        start_ms = now_ms // ms * ms
        r = fut_setups(info, state, tf, cfg, now_ms)
        found, text, close = r if r else ({"BULL": [], "BEAR": []}, "", None)
        cv = fut_cvd(state, sym, info, tf, now_ms)
        found = merge_cvd(found, cv)
        if not (found["BULL"] or found["BEAR"]):
            continue
        if close is None:                       # صرف CVD سگنل: موجودہ قیمت کینڈل سے
            c = get_candles(ex_ref, ref["id"], ref["mult"], tf, 3)
            if not c or c[-1][0] != start_ms:
                stats["no_candles"] += 1
                continue
            close = c[-1][4]
        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        print(f"   OF {sym} {tf} [real]: {text or 'CVD'} | cvd_sign {cv.get('sign') if cv else None} → {', '.join(tags)}")
        for side, names in found.items():
            if names:
                has_base = any(n not in CVD_NAMES for n in names)
                tag = cvd_tag(cv, side) if has_base else "+"
                out.append((tf, side, close, names, start_ms, "real", refinfo, tag))
    return out


# ============================================================
# 🟡 Binance سپاٹ: delta اصل taker volume سے (klines)، footprint aggTrades سے
#    delta / CVD: ہر کوائن پر مکمل اور اصل (taker buy quote volume، کوئی خلا نہیں)
#    footprint: پہلے 1m/5m klines سے تخمینہ؛ 1h/4h میں سیٹ اپ بنے تو aggTrades سے اصل جانچ
#    بہت بھاری کوائنز (BTC، ETH...) میں aggTrades کے صفحے کم پڑیں تو footprint "est" رہتا ہے
# ============================================================
SPOT_SUB = {"1h": "1m", "4h": "1m", "1d": "5m"}
SPOT_VERIFY = {"1h": True, "4h": True, "1d": False}
SPOT_SKIP_BASES = {"EUR", "GBP", "AEUR", "TRY", "BRL", "RLUSD", "XUSD", "UST", "USTC", "WBTC", "WBETH",
                   "BFUSD", "USDS", "EURI"}


def spot_klines(symbol, interval, limit, start_ms=None):
    p = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_ms is not None:
        p["startTime"] = int(start_ms)
    return safe_get("binance", "/api/v3/klines", p)


def load_spot_universe():
    data = safe_get("binance", "/api/v3/exchangeInfo", timeout=30)
    if not data or "symbols" not in data:
        print("Binance سپاٹ: exchangeInfo نہیں ملا (بلاک یا ناکامی) — سپاٹ اس رن میں بند")
        return []
    valid = {}
    for sy in data["symbols"]:
        if sy.get("quoteAsset") != "USDT" or sy.get("status") != "TRADING":
            continue
        if not sy.get("isSpotTradingAllowed", True):
            continue
        base = sy.get("baseAsset", "")
        if not base or base in STABLES or base in SPOT_SKIP_BASES:
            continue
        if base.endswith(("UP", "DOWN", "BULL", "BEAR")) and base not in ("JUP",):
            continue
        valid[sy["symbol"]] = base
    tk = safe_get("binance", "/api/v3/ticker/24hr", timeout=30)
    if not isinstance(tk, list):
        print("Binance سپاٹ: ticker نہیں ملا — سپاٹ اس رن میں بند")
        return []
    rows = []
    for t in tk:
        sym = t.get("symbol")
        if sym in valid:
            qv = _f(t.get("quoteVolume"))
            if qv >= SPOT_MIN_QUOTE_VOL:
                rows.append((sym, qv, int(_f(t.get("count")))))
    rows.sort(key=lambda x: x[1], reverse=True)
    rows = rows[:SPOT_COUNT]
    out = [{"symbol": sym, "label": valid[sym] + "-SPOT", "qvol": qv, "tcount": cnt, "tier": 1}
           for sym, qv, cnt in rows]
    print(f"Binance سپاٹ یونیورس: {len(out)} کوائنز")
    return out


def spot_profile_est(sub, lo, hi, nb):
    step = (hi - lo) / nb
    buy = np.zeros(nb)
    sell = np.zeros(nb)
    for k in sub:
        l, h, qv, tb = float(k[3]), float(k[2]), float(k[7]), float(k[10])
        i0 = min(max(int((l - lo) / step), 0), nb - 1)
        i1 = min(max(int((h - lo) / step), 0), nb - 1)
        if i1 < i0:
            i1 = i0
        n = i1 - i0 + 1
        buy[i0:i1 + 1] += tb / n
        sell[i0:i1 + 1] += (qv - tb) / n
    return buy, sell


def spot_profile_real(trades, lo, hi, nb):
    # aggTrades: m=True کا مطلب خریدار maker، یعنی جارح فروخت
    step = (hi - lo) / nb
    n = len(trades)
    p = np.fromiter((float(t["p"]) for t in trades), float, n)
    q = np.fromiter((float(t["q"]) for t in trades), float, n)
    w = p * q
    is_buy = np.fromiter((not t["m"] for t in trades), bool, n)
    idx = np.clip(((p - lo) / step).astype(int), 0, nb - 1)
    buy = np.bincount(idx[is_buy], weights=w[is_buy], minlength=nb)
    sell = np.bincount(idx[~is_buy], weights=w[~is_buy], minlength=nb)
    return buy, sell


def load_spot_trades(symbol, start_ms, end_ms):
    trades = []
    params = {"symbol": symbol, "startTime": int(start_ms), "limit": 1000}
    for _ in range(OF_MAX_TRADE_PAGES):
        data = safe_get("binance", "/api/v3/aggTrades", params)
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


def verify_spot(info, hist, buy_usd, sell_usd, start_ms, now_ms, tf, progress, prev_deltas):
    tot = buy_usd + sell_usd
    hi, lo = hist[-1][2], hist[-1][3]
    if hi <= lo or tot <= 0:
        return None
    est_trades = info["tcount"] * (now_ms - start_ms) / 86_400_000 if info["tcount"] else 0
    if est_trades > OF_MAX_TRADE_PAGES * 1000 * 1.2:
        stats["too_heavy"] += 1
        return None
    trades = load_spot_trades(info["symbol"], start_ms, int(time.time() * 1000))
    if trades is None or len(trades) < OF_MIN_REAL_TRADES:
        return None
    qv = sum(float(t["p"]) * float(t["q"]) for t in trades)
    if not (OF_VOL_MATCH[0] <= qv / tot <= OF_VOL_MATCH[1]):
        stats["mismatch"] += 1
        return None
    return eval_setups(hist, buy_usd, sell_usd, spot_profile_real(trades, lo, hi, OF_BUCKETS),
                       tf, progress, prev_deltas)


def spot_all_sent(sent, label, tf, start_ms, found):
    keys = [f"{label}|{tf}|{side}|{start_ms}|{n}" for side, names in found.items() for n in names]
    return bool(keys) and all(k in sent for k in keys)


def spot_setups(info, tf, cfg, now_ms, sent, cache):
    """بار کے سیٹ اپس (سپاٹ)۔ واپسی: (found, text, mode, close) یا None"""
    sym, label = info["symbol"], info["label"]
    mins = cfg["minutes"]
    ms = mins * 60_000
    start_ms = now_ms // ms * ms
    progress = (now_ms - start_ms) / ms
    if progress < cfg["min_progress"]:
        return None

    raw = spot_klines(sym, tf, 21)
    if not isinstance(raw, list) or len(raw) < 4 or int(raw[-1][0]) != start_ms:
        stats["no_candles"] += 1
        return None
    tot = float(raw[-1][7])
    buy_usd = float(raw[-1][10])
    sell_usd = tot - buy_usd
    if tot < SPOT_MIN_BAR_USD:
        return None
    hist = [[int(k[0]), float(k[1]), float(k[2]), float(k[3]), float(k[4]), float(k[7])] for k in raw]
    hi, lo = hist[-1][2], hist[-1][3]
    if hi <= lo:
        return None

    sub_iv = SPOT_SUB[tf]
    sub_min = 1 if sub_iv == "1m" else 5
    if sub_iv == "1m":
        if "m1" not in cache:
            cache["m1"] = spot_klines(sym, "1m", 250)
        allsub = cache["m1"]
    else:
        allsub = spot_klines(sym, sub_iv, 300, start_ms=start_ms)
    if not isinstance(allsub, list) or not allsub:
        return None
    sub = [k for k in allsub if int(k[0]) >= start_ms]
    if len(sub) < max(5, int(progress * mins / sub_min * 0.9)):
        return None

    # CVD Divergence کے لیے پچھلی بارز کا اصل delta (taker buy - taker sell)
    prev_deltas = None
    if cfg["div"]:
        prev_deltas = [2 * float(k[10]) - float(k[7]) for k in raw[:-1][-OF_LOOKBACK:]]

    res = eval_setups(hist, buy_usd, sell_usd, spot_profile_est(sub, lo, hi, OF_BUCKETS),
                      tf, progress, prev_deltas)
    if not res:
        return None
    found, text = res
    if not (found["BULL"] or found["BEAR"]):
        return None

    mode = "est"
    if SPOT_VERIFY[tf]:
        if spot_all_sent(sent, label, tf, start_ms, found):
            return None
        real = verify_spot(info, hist, buy_usd, sell_usd, start_ms, now_ms, tf, progress, prev_deltas)
        if real is None:
            stats["unverified"] += 1
        else:
            found, text = real
            if not (found["BULL"] or found["BEAR"]):
                stats["rejected"] += 1
                print(f"   OF {label} {tf}: تخمینے سے بنا، اصل ٹریڈز پر رد")
                return None
            mode = "real"
            stats["confirmed"] += 1
    return found, f"{text} | prog {progress:.0%}", mode, hist[-1][4]


def spot_cvd(info, tf, now_ms, cache):
    """CVD (taker buy - sell) klines سے: 1h/4h کے لیے 15m سٹیپس، 1d کے لیے 1h سٹیپس۔ مکمل اور اصل ڈیٹا۔"""
    cfg = CVD_CFG[tf]
    step, step_ms, win = cfg["step"], cfg["step_ms"], cfg["win"]
    key = "cvd" + step
    if key not in cache:
        cache[key] = spot_klines(info["symbol"], step, 100 if step == "15m" else 170)
    raw = cache[key]
    if not isinstance(raw, list) or len(raw) < 2 * win + 2:
        return None
    if int(raw[-1][0]) != now_ms // step_ms * step_ms:
        return None
    steps = [(int(k[0]), 2 * float(k[10]) - float(k[7]), float(k[7]), float(k[4])) for k in raw]
    tfms = OF_TF[tf]["minutes"] * 60_000
    return cvd_analyze(steps, cvd_anchor(tf, now_ms), win, now_ms // tfms * tfms, SPOT_MIN_BAR_USD)


def check_spot(info, now_ms, sent):
    sym, label = info["symbol"], info["label"]
    out = []
    cache = {}
    refinfo = ("binance", sym, 1.0)
    for tf, cfg in OF_TF.items():
        if time.time() - RUN_T0 > TOTAL_BUDGET_S:
            stats["spot_skipped"] += 1
            break
        if tf == "1d" and info["qvol"] < SPOT_DAILY_MIN_QUOTE_VOL:
            continue
        ms = cfg["minutes"] * 60_000
        start_ms = now_ms // ms * ms

        r = spot_setups(info, tf, cfg, now_ms, sent, cache)
        found, text, mode, close = r if r else ({"BULL": [], "BEAR": []}, "", "real", None)
        cv = spot_cvd(info, tf, now_ms, cache)
        found = merge_cvd(found, cv)
        if not (found["BULL"] or found["BEAR"]):
            continue
        if close is None:
            close = cv["price"] if cv else None
            if close is None:
                continue

        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        print(f"   OF {label} {tf} [{mode}]: {text or 'CVD'} | cvd_sign {cv.get('sign') if cv else None} → {', '.join(tags)}")
        for side, names in found.items():
            if names:
                has_base = any(n not in CVD_NAMES for n in names)
                tag = cvd_tag(cv, side) if has_base else "+"
                out.append((tf, side, close, names, start_ms, mode if has_base else "real", refinfo, tag))
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


def run_orderflow(ranked, state, spot=()):
    now_ms = int(time.time() * 1000)
    print(f"\nآرڈر فلو: فیوچرز {len(ranked)} + سپاٹ {len(spot)} کوائنز | ٹائم فریم {list(OF_TF)}")
    sent_state = {k: v for k, v in load_json(OF_STATE_FILE).items()
                  if isinstance(v, (int, float)) and v >= now_ms - 2 * 24 * 3600 * 1000}

    def work(item):
        kind, info = item
        try:
            if kind == "SPOT":
                return item, check_spot(info, now_ms, sent_state)
            return item, check_orderflow(info, now_ms, state)
        except Exception as e:
            print(f"   OF {info.get('base') or info.get('label')} error: {e}")
            return item, []

    items_all = [("FUT", u) for u in ranked] + [("SPOT", u) for u in spot]
    alerts = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for (kind, info), res in ex.map(work, items_all):
            sym = info["label"] if kind == "SPOT" else info["base"]
            # مخالف سمت کا الرٹ حال ہی میں بھیجا جا چکا ہو تو نیا الرٹ روک دیں
            recent = {}
            for k, v in sent_state.items():
                p = k.split("|")
                if len(p) >= 5 and p[0] == sym and v >= now_ms - OPP_COOLDOWN_MS:
                    recent[p[2]] = True
            for tf, side, price, names, start_ms, mode, refinfo, tag in res:
                if recent.get("BEAR" if side == "BULL" else "BULL"):
                    continue
                new = [n for n in names if f"{sym}|{tf}|{side}|{start_ms}|{n}" not in sent_state]
                if new:
                    alerts.setdefault((tf, side, kind), []).append(
                        (sym, price, new, start_ms, mode, refinfo, info["tier"], tag))

    if not alerts:
        print("آرڈر فلو: کوئی نیا الرٹ نہیں")
        save_json(OF_STATE_FILE, sent_state)
        return

    btc = btc_regime()
    to_track = []
    for (tf, side, kind), items in sorted(alerts.items()):
        name = "Bullish" if side == "BULL" else "Bearish"
        venue = "Spot" if kind == "SPOT" else "Futures"
        for i in range(0, len(items), NTFY_CHUNK):
            chunk = items[i:i + NTFY_CHUNK]
            lines = []
            for s, p, new, _, md, _, tier, tag in chunk:
                mark = ""
                if any(n not in CVD_NAMES for n in new):
                    mark = {"+": " [CVD✓]", "-": " [CVD✗]"}.get(tag, "")
                lines.append(f"{s} {fmt_price(p)} | {' + '.join(new)}{mark}"
                             f"{' (small)' if tier == 2 else ''}{' [est]' if md == 'est' else ''}")
            if post_ntfy(f"{tf.upper()} {name} {venue} Order Flow", "\n".join(lines),
                         tags="green_circle" if side == "BULL" else "red_circle", url=NFTY_OF_URL):
                for s, p, new, start_ms, mode, refinfo, _, tag in chunk:
                    for n in new:
                        sent_state[f"{s}|{tf}|{side}|{start_ms}|{n}"] = start_ms
                    to_track.append((tf, side, s, p, new, start_ms, mode, refinfo, tag))
    save_json(OF_STATE_FILE, sent_state)
    try:
        register_of_trades(to_track, now_ms, btc)
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


_BTC_CACHE = {}


def btc_regime():
    """BTC کی پچھلے ~4 گھنٹے کی سمت: 'up' / 'down' / '?' (ہر رن میں ایک بار)"""
    if "v" in _BTC_CACHE:
        return _BTC_CACHE["v"]
    v = "?"
    try:
        k = spot_klines("BTCUSDT", "1h", 5)
        if isinstance(k, list) and len(k) >= 5:
            v = "up" if float(k[-1][4]) >= float(k[0][1]) else "down"
    except Exception as e:
        print(f"BTC سمت error: {e}")
    _BTC_CACHE["v"] = v
    return v


def register_of_trades(entries, now_ms, btc="?"):
    if not entries:
        return
    book = load_book()
    idx = {t["id"]: t for t in book["open"]}
    for tf, side, sym, price, names, start_ms, mode, refinfo, tag in entries:
        tid = f"{sym}|{tf}|{side}|{start_ms}"
        if tid in idx:
            for n in names:
                if n not in idx[tid]["setups"]:
                    idx[tid]["setups"].append(n)
            continue
        tr = {"id": tid, "symbol": sym, "tf": tf, "side": side, "setups": list(names),
              "kind": "SPOT" if sym.endswith("-SPOT") else "FUT",
              "cvd": tag, "btc": btc,
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
    for kind in ("FUT", "SPOT"):
        for tf in OF_TF:
            rows = [t for t in closed
                    if t["tf"] == tf and t.get("kind", "FUT") == kind][-SUMMARY_LAST_N:]
            if rows:
                lines.append(group_line(f"{kind} {tf}", rows))
    return "\n".join(lines) or "No closed trades yet"


def setup_report(closed):
    names = sorted({n for t in closed for n in t["setups"]})
    lines = ["By setup (all kept results, * = small sample)"]
    for kind in ("FUT", "SPOT"):
        for tf in OF_TF:
            for n in names:
                rows = [t for t in closed
                        if t["tf"] == tf and t.get("kind", "FUT") == kind and n in t["setups"]]
                if rows:
                    lines.append(group_line(f"{kind} {tf} {n}", rows))
    lines += ["", "By timeframe (last results)", summary_text(closed)]
    lines += ["", direction_report(closed), "", cvd_report(closed), "", regime_report(closed)]
    return "\n".join(lines)


def direction_report(closed):
    """Bull اور Bear الگ (FUT/SPOT × ٹائم فریم، آخری نتائج)"""
    lines = ["By direction (last results)"]
    for kind in ("FUT", "SPOT"):
        for tf in OF_TF:
            for side, nm in (("BULL", "Bull"), ("BEAR", "Bear")):
                rows = [t for t in closed if t["tf"] == tf and t.get("kind", "FUT") == kind
                        and t["side"] == side][-SUMMARY_LAST_N:]
                if rows:
                    lines.append(group_line(f"{kind} {tf} {nm}", rows))
    return "\n".join(lines)


def cvd_report(closed):
    """الرٹ کے وقت CVD سمت کے موافق (✓) تھا یا مخالف (✗)؟ (سب محفوظ نتائج)"""
    lines = ["By CVD at alert (all kept results; ✓ agree, ✗ against, ? unknown)"]
    for kind in ("FUT", "SPOT"):
        for tf in OF_TF:
            for tg, nm in (("+", "CVD✓"), ("-", "CVD✗"), ("?", "CVD?")):
                rows = [t for t in closed if t["tf"] == tf and t.get("kind", "FUT") == kind
                        and t.get("cvd", "?") == tg
                        and any(n not in CVD_NAMES for n in t["setups"])]
                if rows:
                    lines.append(group_line(f"{kind} {tf} {nm}", rows))
    return "\n".join(lines)


def regime_report(closed):
    """الرٹ کے وقت BTC کی 4h سمت کے ساتھ نتائج (سب محفوظ نتائج)"""
    lines = ["By BTC trend at alert (~4h; all kept results)"]
    for tf in OF_TF:
        for side, nm in (("BULL", "Bull"), ("BEAR", "Bear")):
            for rg, arrow in (("up", "BTC↑"), ("down", "BTC↓")):
                rows = [t for t in closed if t["tf"] == tf and t["side"] == side and t.get("btc") == rg]
                if rows:
                    lines.append(group_line(f"{tf} {nm} {arrow}", rows))
    return "\n".join(lines)


def post_long(title, text, **kw):
    """ntfy کی حد (~4KB) سے بچنے کے لیے لمبی رپورٹ کئی پیغامات میں"""
    chunks, cur = [], ""
    for line in text.split("\n"):
        if len(cur) + len(line) + 1 > 3300 and cur:
            chunks.append(cur)
            cur = ""
        cur += line + "\n"
    if cur.strip():
        chunks.append(cur)
    ok = True
    for i, c in enumerate(chunks, 1):
        t = title if len(chunks) == 1 else f"{title} ({i}/{len(chunks)})"
        ok = post_ntfy(t, c.rstrip("\n"), **kw) and ok
    return ok


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
        if post_long("OF daily report", setup_report(book["closed"]),
                     tags="chart_with_upwards_trend", url=NFTY_OF_URL, priority="default"):
            book["last_report_day"] = today
    save_json(TRADES_FILE, book)


def print_coverage(ranked, state, now_ms):
    """سب سے مصروف 12 کوائنز کی موجودہ 1h بار کی کوریج، ایکسچینج کے لحاظ سے۔"""
    a = now_ms // 3_600_000 * 3_600_000
    print("\nکوریج (موجودہ 1h بار، سب سے مصروف کوائنز):")
    for u in ranked[:12]:
        sym = u["base"]
        parts = [f"{ex} {coverage(state, sym, {'ex': {ex: d}}, a, now_ms):.0%}"
                 for ex, d in u["ex"].items()]
        print(f"   {sym}: کل {coverage(state, sym, u, a, now_ms):.0%} | " + " ".join(parts))


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

    k = safe_get("binance", "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 2})
    print(f"[binance سپاٹ] BTCUSDT klines (اندراج 7=quote vol، 10=taker buy quote): {k}")


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
    print(f"{start.strftime('%H:%M:%S')} | فیوچرز آرڈر فلو | فیوچرز OKX+Gate+Bitget + سپاٹ Binance")
    print("=" * 60)

    ranked = load_universe() if ENABLE_FUTURES else []
    spot = load_spot_universe() if ENABLE_SPOT else []
    if not ranked and not spot:
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
        run_orderflow(ranked, state, spot)
    except Exception as e:
        print(f"آرڈر فلو error: {e}")

    print_coverage(ranked, state, int(time.time() * 1000))

    # 3) ٹریکر
    try:
        track_of_trades()
    except Exception as e:
        print(f"ٹریکر error: {e}")

    print(f"\nاعداد: {stats}")
    print(f"مکمل: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
