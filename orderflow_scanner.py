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
# SETTINGS - Futures order flow (OKX + Gate + Bitget, USDT perpetuals)
# ============================================================
TIER1_COUNT = 100
TIER2_COUNT = 20
MIN_EXCHANGE_QUOTE_VOL = 5_000_000
MIN_COMMODITY_QUOTE_VOL = 1_000_000
DAILY_MIN_QUOTE_VOL = 30_000_000
MIN_BAR_USD = 150_000
MIN_BAR_USD_PER_TF = {"1h": 150_000, "4h": 400_000, "1d": 1_500_000}

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
OF_MAX_DELTA_PCT = 95.0
OPP_COOLDOWN_MS = 3 * 3600_000

# CVD zero line - real-time touch (1h, 4h)
CVD_SIGNALS = {"retest": False, "zero": False, "early": False}

CVD_RT_ENABLE = True
CVD_RT_TFS = {"1h": 60, "4h": 240}
CVD_RT_ZONE_PER_TF = {"1h": 0.50, "4h": 0.60}
CVD_RT_EXIT_MULT = 2.0
CVD_RT_MIN_GAP_MIN = 30
CVD_RT_MIN_STEPS = 24
CVD_RT_MIN_BARS = 4
CVD_RT_S_LOOKBACK_H = 30
CVD_RT_RECENT_FLOW_MS = 30 * 60_000

# Legacy CVD (disabled by default)
CVD_BAR_TFS = {"30m": 30, "1h": 60, "4h": 240}
CVD_TOUCH = 1.0
CVD_OVER = 0.5
CVD_BOUNCE = 1.0
CVD_MAX_AGE_MS = 25 * 60_000
CVD_CFG = {
    "1h": {"win": 3, "step": "15m", "step_ms": 900_000},
    "4h": {"win": 8, "step": "15m", "step_ms": 900_000},
}
CVD_ZERO_MARGIN = 0.6
CVD_EARLY_MIN_PCT = 8.0
CVD_EARLY_PREV_PCT = 3.0
CVD_EARLY_LAG = 0.6
# FIX: "CVD Zero Touch" added so it is not treated as a base setup
CVD_NAMES = ("CVD Zero", "CVD Early", "CVD Retest", "CVD Zero Touch")

# Binance spot
ENABLE_FUTURES = True
ENABLE_SPOT = True
SPOT_COUNT = 200
SPOT_MIN_QUOTE_VOL = 5_000_000
SPOT_DAILY_MIN_QUOTE_VOL = 10_000_000
SPOT_MIN_BAR_USD = 150_000
SPOT_MIN_BAR_USD_PER_TF = {"1h": 150_000, "4h": 400_000, "1d": 1_500_000}
OF_MAX_TRADE_PAGES = 60
OF_MIN_REAL_TRADES = 200
OF_VOL_MATCH = (0.85, 1.25)
TOTAL_BUDGET_S = 400
RUN_T0 = time.time()

MIN_COVERAGE = 0.85
BUCKET_MS = 300_000
LOGSTEP = math.log(1.0005)
RETAIN_MS = 50 * 3600_000
BINS_KEEP_MS = 26 * 3600_000
PAGES = 4
FETCH_BUDGET_S = 240

FLOW_FILE = "flow_state.json.gz"
OF_STATE_FILE = "orderflow_state.json"
TRADES_FILE = "trades_state.json"

OF_TRACK = {
    "30m": {"hours": 3, "step": "5m",  "step_ms": 300_000},
    "1h": {"hours": 6,  "step": "5m",  "step_ms": 300_000},
    "4h": {"hours": 24, "step": "15m", "step_ms": 900_000},
    "1d": {"hours": 72, "step": "1h",  "step_ms": 3_600_000},
}
REPORT_TFS = ("30m", "1h", "4h", "1d")
MAX_CLOSED_KEEP = 500
SUMMARY_LAST_N = 30
SMALL_SAMPLE = 10
NTFY_CHUNK = 30

NFTY_URL = os.environ.get("NFTY_URL")
NFTY_OF_URL = os.environ.get("NFTY_OF_URL") or NFTY_URL

# ============================================================
# Rate limit
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


LIM = {"okx": Limiter(9), "gate": Limiter(12), "bitget": Limiter(8), "binance": Limiter(15)}
session = requests.Session()
stats = {"rate_limited": 0, "failed": 0, "gaps": 0, "low_cov": 0, "no_candles": 0,
         "fetch_skipped": 0, "blocked": 0, "spot_skipped": 0, "too_heavy": 0,
         "mismatch": 0, "confirmed": 0, "rejected": 0, "unverified": 0,
         "cvd_bars": 0, "cvd_touch": 0, "cvd_sig": 0, "cvd_rt": 0, "cvd_rt_alerts": 0}


def safe_get(ex, path, params=None, timeout=15, retries=4):
    url = HOSTS[ex] + path
    for attempt in range(retries):
        LIM[ex].wait()
        try:
            r = session.get(url, params=params, timeout=timeout)
            if r.status_code in (429, 418, 403):
                stats["rate_limited"] += 1
                try:
                    wait = min(int(r.headers.get("Retry-After", 2 ** (attempt + 1))), 30)
                except ValueError:
                    wait = 2 ** (attempt + 1)
                LIM[ex].penalize(wait)
                continue
            if r.status_code in (400, 451):
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
    if p >= 100:
        return f"{p:.2f}"
    if p >= 1:
        return f"{p:.4f}"
    if p >= 0.01:
        return f"{p:.5f}"
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
# Universe
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

    tk = safe_get("bitget", "/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"})
    if tk and tk.get("code") == "00000":
        for t in tk["data"]:
            s = t.get("symbol", "")
            if s.endswith("USDT"):
                qv = _f(t.get("usdtVolume")) or _f(t.get("quoteVolume"))
                put(s[:-4], "bitget", s, 1.0, qv)

    all_ranked = sorted(uni.values(), key=lambda u: u["qvol"], reverse=True)

    liquid = [u for u in all_ranked
              if u["base"] not in COMMODITY_BASES
              and max((d["qv"] for d in u["ex"].values()), default=0) >= MIN_EXCHANGE_QUOTE_VOL]
    if len(liquid) < TIER1_COUNT:
        fallback = [u for u in all_ranked
                    if u["base"] not in COMMODITY_BASES
                    and max((d["qv"] for d in u["ex"].values()), default=0) >= 2_000_000]
        seen = {u["base"] for u in liquid}
        liquid.extend(u for u in fallback if u["base"] not in seen)
    selected_coins = liquid[:TIER1_COUNT]

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
    print(f"Universe: {len(selected_coins)} liquid coins + {commodity_count} commodity tokens "
          f"(max {TIER2_COUNT}) | exchange coverage {per_ex}")
    if commodity_count < TIER2_COUNT:
        print(f"Note: only {commodity_count} of {TIER2_COUNT} commodity tokens matched.")
    if len(selected_coins) < TIER1_COUNT:
        print(f"Note: {len(selected_coins)} acceptable coins found instead of {TIER1_COUNT}.")
    return ranked


# ============================================================
# Trade fetchers
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
# Candles
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
# Flow state: 5-minute buckets + coverage
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
            if px <= 0:
                continue
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


def combined_hi_lo(state, sym, a, b):
    hi = lo = 0.0
    for k, v in state["bk"].get(sym, {}).items():
        kt = int(k)
        if a <= kt < b:
            if v[2] > 0 and v[2] > hi:
                hi = v[2]
            if v[3] > 0 and (lo == 0 or v[3] < lo):
                lo = v[3]
    return hi, lo


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
# Setups
# ============================================================
def has_run(mask, n):
    run = 0
    for m in mask:
        run = run + 1 if m else 0
        if run >= n:
            return True
    return False


def eval_setups(hist, buy_usd, sell_usd, profile, tf, progress, prev_deltas, vol_ratio=None):
    cfg = OF_TF[tf]
    bar, prev = hist[-1], hist[:-1]
    o, h, l, c, v = bar[1], bar[2], bar[3], bar[4], bar[5]
    tot_usd = buy_usd + sell_usd
    if tot_usd <= 0 or h <= l or not prev:
        return None

    if vol_ratio is None:
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
            f"volx{vol_ratio:.2f}")
    return found, info


# ============================================================
# CVD anchor + legacy analysis
# ============================================================
def cvd_anchor(now_ms):
    return now_ms // 86_400_000 * 86_400_000


def _cvd_cross(cv, ts, margin, win, bar_start, up):
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


def cvd_analyze(steps, anchor_ms, win, bar_start_ms, min_flow):
    out = {"sign": None, "cross": None, "early": None}
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
    if not cv or cv.get("sign") in (None, 0):
        return "?"
    return "+" if cv["sign"] == (1 if side == "BULL" else -1) else "-"


def merge_cvd(found, cv):
    if cv:
        for key, sig, name in (("cross", "zero", "CVD Zero"), ("early", "early", "CVD Early")):
            side = cv.get(key)
            if side and CVD_SIGNALS.get(sig):
                found[side].append(name)
    if found["BULL"] and found["BEAR"]:
        return {"BULL": [], "BEAR": []}
    return found


def agg_steps(steps, step_ms):
    out = {}
    for t, d, f, p in steps:
        k = t // step_ms * step_ms
        e = out.get(k)
        if e is None:
            out[k] = [d, f, p]
        else:
            e[0] += d
            e[1] += f
            e[2] = p
    return [(k, v[0], v[1], v[2]) for k, v in sorted(out.items())]


def cvd_bar_signals(steps5, anchor_ms, now_ms, min_flow):
    out = []
    if not CVD_SIGNALS.get("retest") or not steps5:
        return out
    anch = [s for s in steps5 if s[0] >= anchor_ms]
    if len(anch) < 12:
        return out
    cv = list(itertools.accumulate(s[1] for s in anch))
    ts = [s[0] for s in anch]
    flows = [s[2] for s in anch]
    price = steps5[-1][3]
    for tf, mins in CVD_BAR_TFS.items():
        ms = mins * 60_000
        end = now_ms // ms * ms
        start = end - ms
        if now_ms - end > CVD_MAX_AGE_MS or start < anchor_ms:
            continue
        need = ms // 300_000
        idx = [i for i, t in enumerate(ts) if start <= t < end]
        if len(idx) < int(need * 0.8):
            continue
        i0, i1 = idx[0], idx[-1]
        if sum(flows[i] for i in idx) < min_flow:
            continue
        per = {}
        cnt = {}
        for t, d, f, p in steps5:
            if t < end:
                k = t // ms * ms
                per[k] = per.get(k, 0.0) + d
                cnt[k] = cnt.get(k, 0) + 1
        vals = [abs(per[k]) for k in per if cnt[k] >= int(need * 0.9)]
        if len(vals) < 4:
            continue
        S = sum(vals) / len(vals)
        if S <= 0:
            continue
        stats["cvd_bars"] += 1
        touch, over, bounce = CVD_TOUCH * S, CVD_OVER * S, CVD_BOUNCE * S
        c_open = cv[i0 - 1] if i0 > 0 else 0.0
        seg = cv[i0:i1 + 1]
        hi, lo, cl = max(seg), min(seg), seg[-1]
        if c_open <= -touch and -touch <= hi <= over:
            stats["cvd_touch"] += 1
            if cl < 0 and hi - cl >= bounce:
                stats["cvd_sig"] += 1
                out.append((tf, "BEAR", start, price))
        elif c_open >= touch and -over <= lo <= touch:
            stats["cvd_touch"] += 1
            if cl > 0 and cl - lo >= bounce:
                stats["cvd_sig"] += 1
                out.append((tf, "BULL", start, price))
    return out


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
    cfg = CVD_CFG.get(tf)
    if not cfg:
        return None
    step_ms, win = cfg["step_ms"], cfg["win"]
    sinces = [state["cur"].get(f"{ex}|{sym}", {}).get("since") for ex in info["ex"]]
    sinces = [s for s in sinces if s]
    if not sinces:
        return None
    anchor = max(cvd_anchor(now_ms), min(sinces))
    series_start = min(anchor, now_ms - (2 * win + 3) * step_ms)
    if coverage(state, sym, info, series_start, now_ms) < MIN_COVERAGE:
        return None
    steps = fut_cvd_steps(state, sym, step_ms, series_start, now_ms)
    bar_start = now_ms // (OF_TF[tf]["minutes"] * 60_000) * (OF_TF[tf]["minutes"] * 60_000)
    return cvd_analyze(steps, anchor, win, bar_start, MIN_BAR_USD)


def fut_cvd_bars(state, sym, info, now_ms):
    if not CVD_SIGNALS.get("retest"):
        return []
    sinces = [state["cur"].get(f"{ex}|{sym}", {}).get("since") for ex in info["ex"]]
    sinces = [s for s in sinces if s]
    if not sinces:
        return []
    anchor = max(cvd_anchor(now_ms), min(sinces))
    if coverage(state, sym, info, anchor, now_ms) < MIN_COVERAGE:
        return []
    first = max(min(anchor, now_ms - 26 * 3600_000), min(sinces))
    steps = fut_cvd_steps(state, sym, 300_000, first, now_ms)
    return cvd_bar_signals(steps, anchor, now_ms, MIN_BAR_USD)


# ============================================================
# CVD zero line - real-time touch (1h, 4h)
# ============================================================
def _cvd_rt_S(steps5, tf_ms, now_ms, min_bars):
    bar = {}
    for t, d, f, p in steps5:
        k = t // tf_ms * tf_ms
        e = bar.setdefault(k, [0.0, 0])
        e[0] += d
        e[1] += 1
    need = int(tf_ms // 300_000 * 0.9)
    vals = [abs(v[0]) for k, v in bar.items()
            if k + tf_ms <= now_ms and v[1] >= need]
    if len(vals) < min_bars:
        return None
    return sum(vals) / len(vals)


def cvd_rt_check(sym, steps5, rt_state, now_ms, min_flow, tag=""):
    if not CVD_RT_ENABLE or not steps5:
        return []
    anchor = cvd_anchor(now_ms)
    post = [s for s in steps5 if s[0] >= anchor]
    if len(post) < CVD_RT_MIN_STEPS:
        return []
    price = steps5[-1][3]
    if price <= 0:
        return []
    cvd_now = sum(s[1] for s in post)

    recent_flow = sum(s[2] for s in steps5 if s[0] >= now_ms - CVD_RT_RECENT_FLOW_MS)
    if recent_flow < min_flow:
        return []

    alerts = []
    for tf, mins in CVD_RT_TFS.items():
        S = _cvd_rt_S(steps5, mins * 60_000, now_ms, CVD_RT_MIN_BARS)
        if not S or S <= 0:
            continue
        zone = CVD_RT_ZONE_PER_TF[tf]
        in_zone = abs(cvd_now) < zone * S
        outside = abs(cvd_now) > CVD_RT_EXIT_MULT * S

        key = f"{tag}{sym}|{tf}"
        st = rt_state.setdefault(key, {"anchor_ms": anchor, "last_side": 0,
                                        "in_zone": False, "last_alert_ms": 0})
        if st["anchor_ms"] != anchor:
            st["anchor_ms"] = anchor
            st["last_side"] = 0
            st["in_zone"] = False

        if outside:
            st["last_side"] = 1 if cvd_now > 0 else -1

        stats["cvd_rt"] += 1
        if in_zone and not st["in_zone"] and st["last_side"] != 0:
            if now_ms - st["last_alert_ms"] >= CVD_RT_MIN_GAP_MIN * 60_000:
                side = "BULL" if st["last_side"] > 0 else "BEAR"
                alerts.append((tf, side, price))
                st["last_alert_ms"] = now_ms
                st["in_zone"] = True
                stats["cvd_rt_alerts"] += 1
        elif not in_zone:
            st["in_zone"] = False
    return alerts


# ============================================================
# Futures setups and check_orderflow
# ============================================================
def fut_setups(info, state, tf, cfg, now_ms):
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
    min_bar = MIN_BAR_USD_PER_TF.get(tf, MIN_BAR_USD)
    if buy_usd + sell_usd < min_bar:
        return None

    hist = get_candles(ex_ref, ref["id"], ref["mult"], tf, 21)
    if not hist or len(hist) < 4 or hist[-1][0] != start_ms:
        stats["no_candles"] += 1
        return None

    s_hi, s_lo = combined_hi_lo(state, sym, start_ms, now_ms)
    if s_hi > 0 and s_lo > 0 and s_hi > s_lo:
        p_hi, p_lo = s_hi, s_lo
    else:
        p_hi, p_lo = hist[-1][2], hist[-1][3]
    if p_hi <= p_lo:
        return None

    prev_deltas = None
    prev_vols = []
    vol_ratio = None
    look = hist[:-1][-OF_LOOKBACK:]
    if look:
        prev_deltas = []
        for k in look:
            a = k[0]
            b = a + ms
            if coverage(state, sym, info, a, b) >= MIN_COVERAGE:
                pb, ps, _ = bar_sum(state, sym, a, b)
                tot_pb = pb + ps
                prev_vols.append(tot_pb if tot_pb > 0 else 0.0)
                prev_deltas.append(pb - ps if tot_pb > 0 else None)
            else:
                prev_vols.append(0.0)
                prev_deltas.append(None)
        valid_vols = [x for x in prev_vols if x > 0]
        if valid_vols:
            avg_vol = statistics.median(valid_vols)
            cur_vol = buy_usd + sell_usd
            elapsed = min(1.0, max(float(progress), 1.0 / max(cfg["minutes"], 1)))
            vol_ratio = cur_vol / (avg_vol * elapsed) if avg_vol > 0 else 0.0
        else:
            ref_prev = [k[5] for k in look if k[5] > 0]
            if ref_prev:
                avg_vol = statistics.median(ref_prev)
                elapsed = min(1.0, max(float(progress), 1.0 / max(cfg["minutes"], 1)))
                vol_ratio = hist[-1][5] / (avg_vol * elapsed) if avg_vol > 0 else 0.0
    if not cfg["div"]:
        prev_deltas = None

    res = eval_setups(hist, buy_usd, sell_usd,
                      build_profile_real(bins, p_lo, p_hi, OF_BUCKETS),
                      tf, progress, prev_deltas, vol_ratio=vol_ratio)
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

        cv = fut_cvd(state, sym, info, tf, now_ms) if any(CVD_SIGNALS.values()) else None
        found = merge_cvd(found, cv)

        if not (found["BULL"] or found["BEAR"]):
            continue

        if close is None:
            c = get_candles(ex_ref, ref["id"], ref["mult"], tf, 3)
            if not c or c[-1][0] != start_ms:
                stats["no_candles"] += 1
                continue
            close = c[-1][4]

        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        sign = cv.get("sign") if cv else None
        print(f"   OF {sym} {tf} [real]: {text or 'CVD'} | cvd_sign {sign} -> {', '.join(tags)}")

        for side, names in found.items():
            if names:
                has_base = any(n not in CVD_NAMES for n in names)
                tag = cvd_tag(cv, side) if has_base else "+"
                out.append((tf, side, close, names, start_ms, "real", refinfo, tag))

    bars = fut_cvd_bars(state, sym, info, now_ms)
    if bars:
        c5 = get_candles(ex_ref, ref["id"], ref["mult"], "5m", 2)
        for tf, side, start_ms, px in bars:
            price = c5[-1][4] if c5 else px
            print(f"   OF {sym} {tf} [real]: CVD zero line rejection (closed bar) -> {side}:CVD Retest")
            out.append((tf, side, price, ["CVD Retest"], start_ms, "real", refinfo, "+"))

    if CVD_RT_ENABLE:
        sinces = [state["cur"].get(f"{ex}|{sym}", {}).get("since") for ex in info["ex"]]
        sinces = [s for s in sinces if s]
        if sinces:
            hist_start = max(now_ms - CVD_RT_S_LOOKBACK_H * 3600_000, min(sinces))
            steps5 = fut_cvd_steps(state, sym, 300_000, hist_start, now_ms)
            rt_state = state.setdefault("cvd_rt", {})
            alerts = cvd_rt_check(sym, steps5, rt_state, now_ms, MIN_BAR_USD)
            for tf, side, price in alerts:
                bucket = now_ms // 300_000 * 300_000
                print(f"   OF {sym} {tf} [RT] CVD zero touch -> {side} @ {fmt_price(price)}")
                out.append((tf, side, price, ["CVD Zero Touch"], bucket, "real", refinfo, "+"))

    return out


# ============================================================
# Binance spot
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
        print("Binance spot: exchangeInfo not received - spot disabled this run")
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
        print("Binance spot: ticker not received - spot disabled this run")
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
    print(f"Binance spot universe: {len(out)} coins")
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
    min_bar = SPOT_MIN_BAR_USD_PER_TF.get(tf, SPOT_MIN_BAR_USD)
    if tot < min_bar:
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
    if SPOT_VERIFY.get(tf, False):
        if spot_all_sent(sent, label, tf, start_ms, found):
            return None
        real = verify_spot(info, hist, buy_usd, sell_usd, start_ms, now_ms, tf, progress, prev_deltas)
        if real is None:
            stats["unverified"] += 1
        else:
            found, text = real
            if not (found["BULL"] or found["BEAR"]):
                stats["rejected"] += 1
                print(f"   OF {label} {tf}: est-based setup rejected on real trades")
                return None
            mode = "real"
            stats["confirmed"] += 1
    return found, f"{text} | prog {progress:.0%}", mode, hist[-1][4]


def spot_steps5(info, now_ms, cache):
    if "s5" not in cache:
        raw = spot_klines(info["symbol"], "5m", 300)
        ok = isinstance(raw, list) and len(raw) >= 20 and int(raw[-1][0]) == now_ms // 300_000 * 300_000
        cache["s5"] = ([(int(k[0]), 2 * float(k[10]) - float(k[7]), float(k[7]), float(k[4])) for k in raw]
                       if ok else None)
    return cache["s5"]


def spot_cvd(info, tf, now_ms, cache):
    cfg = CVD_CFG.get(tf)
    if not cfg:
        return None
    s5 = spot_steps5(info, now_ms, cache)
    if not s5:
        return None
    win = cfg["win"]
    steps = agg_steps(s5, cfg["step_ms"])
    if len(steps) < 2 * win + 2:
        return None
    tfms = OF_TF[tf]["minutes"] * 60_000
    return cvd_analyze(steps, cvd_anchor(now_ms), win, now_ms // tfms * tfms, SPOT_MIN_BAR_USD)


def check_spot(info, now_ms, sent, state):
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
        cv = spot_cvd(info, tf, now_ms, cache) if any(CVD_SIGNALS.values()) else None
        found = merge_cvd(found, cv)
        if not (found["BULL"] or found["BEAR"]):
            continue
        if close is None:
            close = cv["price"] if cv else None
            if close is None:
                continue

        tags = [f"{sd}:{'+'.join(nm)}" for sd, nm in found.items() if nm]
        print(f"   OF {label} {tf} [{mode}]: {text or 'CVD'} | cvd_sign {cv.get('sign') if cv else None} -> {', '.join(tags)}")
        for side, names in found.items():
            if names:
                has_base = any(n not in CVD_NAMES for n in names)
                tag = cvd_tag(cv, side) if has_base else "+"
                out.append((tf, side, close, names, start_ms, mode if has_base else "real", refinfo, tag))

    if CVD_SIGNALS.get("retest") and time.time() - RUN_T0 <= TOTAL_BUDGET_S:
        s5 = spot_steps5(info, now_ms, cache)
        if s5:
            for tf, side, start_ms, px in cvd_bar_signals(s5, cvd_anchor(now_ms), now_ms, SPOT_MIN_BAR_USD):
                print(f"   OF {label} {tf} [real]: CVD zero line rejection (closed bar) -> {side}:CVD Retest")
                out.append((tf, side, px, ["CVD Retest"], start_ms, "real", refinfo, "+"))

    if CVD_RT_ENABLE:
        s5 = spot_steps5(info, now_ms, cache)
        if s5:
            rt_state = state.setdefault("cvd_rt", {})
            for tf, side, price in cvd_rt_check(label, s5, rt_state, now_ms,
                                                SPOT_MIN_BAR_USD, tag="SPOT:"):
                bucket = now_ms // 300_000 * 300_000
                print(f"   OF {label} {tf} [RT] CVD zero touch -> {side} @ {fmt_price(price)}")
                out.append((tf, side, price, ["CVD Zero Touch"],
                            bucket, "real", refinfo, "+"))
    return out


# ============================================================
# Ntfy
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
    print(f"\nOrder flow: futures {len(ranked)} + spot {len(spot)} coins | TFs {list(OF_TF)}")
    sent_state = {k: v for k, v in load_json(OF_STATE_FILE).items()
                  if isinstance(v, (int, float)) and v >= now_ms - 2 * 24 * 3600 * 1000}

    def work(item):
        kind, info = item
        try:
            if kind == "SPOT":
                return item, check_spot(info, now_ms, sent_state, state)
            return item, check_orderflow(info, now_ms, state)
        except Exception as e:
            print(f"   OF {info.get('base') or info.get('label')} error: {e}")
            return item, []

    items_all = [("FUT", u) for u in ranked] + [("SPOT", u) for u in spot]
    alerts = {}
    with ThreadPoolExecutor(max_workers=8) as ex:
        for (kind, info), res in ex.map(work, items_all):
            sym = info["label"] if kind == "SPOT" else info["base"]
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
        print("Order flow: no new alerts")
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
                    mark = {"+": " [CVD+]", "-": " [CVD-]"}.get(tag, "")
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
        print(f"Tracker registration error: {e}")


# ============================================================
# Tracker: MFE / MAE / end direction
# ============================================================
def load_book():
    d = load_json(TRADES_FILE)
    return {"open": [t for t in d.get("open", []) if "tf" in t and "ref" in t],
            "closed": [t for t in d.get("closed", []) if "tf" in t],
            "last_report_day": d.get("last_report_day")}


_BTC_CACHE = {}


def btc_regime():
    if "v" in _BTC_CACHE:
        return _BTC_CACHE["v"]
    v = "?"
    try:
        k = spot_klines("BTCUSDT", "1h", 5)
        if isinstance(k, list) and len(k) >= 5:
            v = "up" if float(k[-1][4]) >= float(k[0][1]) else "down"
    except Exception as e:
        print(f"BTC direction error: {e}")
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
        for tf in REPORT_TFS:
            rows = [t for t in closed
                    if t["tf"] == tf and t.get("kind", "FUT") == kind][-SUMMARY_LAST_N:]
            if rows:
                lines.append(group_line(f"{kind} {tf}", rows))
    return "\n".join(lines) or "No closed trades yet"


def setup_report(closed):
    names = sorted({n for t in closed for n in t["setups"]})
    lines = ["By setup (all kept results, * = small sample)"]
    for kind in ("FUT", "SPOT"):
        for tf in REPORT_TFS:
            for n in names:
                rows = [t for t in closed
                        if t["tf"] == tf and t.get("kind", "FUT") == kind and n in t["setups"]]
                if rows:
                    lines.append(group_line(f"{kind} {tf} {n}", rows))
    lines += ["", "By timeframe (last results)", summary_text(closed)]
    lines += ["", direction_report(closed), "", cvd_report(closed), "", regime_report(closed)]
    return "\n".join(lines)


def direction_report(closed):
    lines = ["By direction (last results)"]
    for kind in ("FUT", "SPOT"):
        for tf in REPORT_TFS:
            for side, nm in (("BULL", "Bull"), ("BEAR", "Bear")):
                rows = [t for t in closed if t["tf"] == tf and t.get("kind", "FUT") == kind
                        and t["side"] == side][-SUMMARY_LAST_N:]
                if rows:
                    lines.append(group_line(f"{kind} {tf} {nm}", rows))
    return "\n".join(lines)


def cvd_report(closed):
    lines = ["By CVD at alert (all kept results; + agree, - against, ? unknown)"]
    for kind in ("FUT", "SPOT"):
        for tf in REPORT_TFS:
            for tg, nm in (("+", "CVD+"), ("-", "CVD-"), ("?", "CVD?")):
                rows = [t for t in closed if t["tf"] == tf and t.get("kind", "FUT") == kind
                        and t.get("cvd", "?") == tg
                        and any(n not in CVD_NAMES for n in t["setups"])]
                if rows:
                    lines.append(group_line(f"{kind} {tf} {nm}", rows))
    return "\n".join(lines)


def regime_report(closed):
    lines = ["By BTC trend at alert (last ~5h; all kept results)"]
    for tf in REPORT_TFS:
        for side, nm in (("BULL", "Bull"), ("BEAR", "Bear")):
            for rg, arrow in (("up", "BTC^"), ("down", "BTCv")):
                rows = [t for t in closed if t["tf"] == tf and t["side"] == side and t.get("btc") == rg]
                if rows:
                    lines.append(group_line(f"{tf} {nm} {arrow}", rows))
    return "\n".join(lines)


def post_long(title, text, **kw):
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
    print(f"\nTracker: {len(book['open'])} open | {len(book['closed'])} closed")
    still, newly = [], []
    for tr in book["open"]:
        try:
            res = evaluate_of_trade(tr, now_ms)
        except Exception as e:
            print(f"   {tr['symbol']} tracker error: {e}")
            res = None
        if res is None:
            still.append(tr)
        elif res == "DROP":
            print(f"   {tr['symbol']} {tr['tf']}: no data, entry dropped")
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
    a = now_ms // 3_600_000 * 3_600_000
    print("\nCoverage (current 1h bar, busiest coins):")
    for u in ranked[:12]:
        sym = u["base"]
        parts = [f"{ex} {coverage(state, sym, {'ex': {ex: d}}, a, now_ms):.0%}"
                 for ex, d in u["ex"].items()]
        print(f"   {sym}: total {coverage(state, sym, u, a, now_ms):.0%} | " + " ".join(parts))


# ============================================================
# selftest
# ============================================================
def selftest():
    ranked = load_universe()
    btc = next((u for u in ranked if u["base"] == "BTC"), None)
    if not btc:
        print("BTC not found in universe")
        return
    now_ms = int(time.time() * 1000)
    for ex, d in btc["ex"].items():
        r = FETCHERS[ex](d, None, now_ms)
        if r is None:
            print(f"[{ex}] trades: failed")
        else:
            items, _ = r
            print(f"[{ex}] trades: {len(items)} | last 2: {items[-2:]}")
        c = get_candles(ex, d["id"], d["mult"], "1h", 3)
        print(f"[{ex}] candles: {c}")

    k = safe_get("binance", "/api/v3/klines", {"symbol": "BTCUSDT", "interval": "1h", "limit": 2})
    print(f"[binance spot] BTCUSDT klines (idx 7=quote vol, 10=taker buy quote): {k}")


# ============================================================
# MAIN
# ============================================================
def main():
    if "--selftest" in sys.argv:
        selftest()
        return
    if not NFTY_URL:
        print("NFTY_URL not set")
        raise SystemExit(1)

    t0 = time.time()
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | Futures order flow | OKX+Gate+Bitget futures + Binance spot")
    print("=" * 60)

    ranked = load_universe() if ENABLE_FUTURES else []
    spot = load_spot_universe() if ENABLE_SPOT else []
    if not ranked and not spot:
        print("No markets found")
        return

    state = load_gz(FLOW_FILE)
    for k in ("cur", "gaps", "bk"):
        state.setdefault(k, {})
    state.setdefault("cvd_rt", {})

    now_ms = int(time.time() * 1000)
    jobs = [(u["base"], ex, d) for u in ranked for ex, d in u["ex"].items()]
    print(f"Trades: {len(jobs)} (exchange x coin) requests")

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
    print(f"Trades done: {time.time() - t0:.0f}s")

    try:
        run_orderflow(ranked, state, spot)
    except Exception as e:
        print(f"Order flow error: {e}")

    # FIX: save again so cvd_rt state (cooldown / zone) persists between runs
    save_gz(FLOW_FILE, state)

    print_coverage(ranked, state, int(time.time() * 1000))

    try:
        track_of_trades()
    except Exception as e:
        print(f"Tracker error: {e}")

    print(f"\nStats: {stats}")
    print(f"Done: {(datetime.now() - start).total_seconds():.0f}s")


if __name__ == "__main__":
    main()
