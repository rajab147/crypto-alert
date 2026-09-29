import os
import sys
import json
import time
import argparse

import requests
import numpy as np
import pandas as pd

# --- Configuration ---
BUFFER = 0.001
RSI_PERIOD = 9
RSI_OB = 75
RSI_OS = 25
VOL_PERIOD = 20
VOL_MULT = 1.5

DAILY_LOOKBACK = 10
H4_LOOKBACK = 20
DAILY_HOURS = 48
H4_HOURS = 24

TOP_N = 500
REQUEST_DELAY = 0.12          # ریٹ لمٹ سے بچاؤ (سیکنڈ)
MAX_SIGNAL_AGE_CANDLES = 1    # سگنل صرف تازہ بند کینڈل پر (کینڈلز میں)

# --- OKX ---
OKX_BASE_URL = "https://www.okx.com"
INTERVAL_MAP = {"1d": "1Dutc", "4h": "4H", "1h": "1H", "30m": "30m"}  # 1Dutc = UTC ڈیلی کینڈل
TF_DELTA = {
    "1D": pd.Timedelta(days=1),
    "4H": pd.Timedelta(hours=4),
    "1H": pd.Timedelta(hours=1),
    "30M": pd.Timedelta(minutes=30),
}

# --- NTFY (ٹاپک ماحولیاتی متغیر سے) ---
# --- NTFY ---
NTFY_TOPIC = "ChartMaster786x7k2p9"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"
# --- ڈپلیکیٹ الرٹ سے بچاؤ (فائل میں محفوظ) ---
STATE_FILE = os.environ.get("STATE_FILE", "signaled.json")
SIGNALED = {}  # key -> expiry (ISO string)


def load_state():
    global SIGNALED
    try:
        with open(STATE_FILE, "r") as f:
            SIGNALED = json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        SIGNALED = {}
    prune_state()


def prune_state():
    """ایکسپائر ہو چکی انٹریز ہٹائیں"""
    now = pd.Timestamp.now(tz="UTC")
    keep = {}
    for k, v in SIGNALED.items():
        try:
            if pd.Timestamp(v) >= now:
                keep[k] = v
        except Exception:
            pass
    SIGNALED.clear()
    SIGNALED.update(keep)


def save_state():
    try:
        tmp = STATE_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(SIGNALED, f)
        os.replace(tmp, STATE_FILE)
    except Exception as e:
        print(f"State save error: {e}")


def okx_get(path, params=None, retries=3):
    """ریٹ لمٹ اور 429 ہینڈلنگ کے ساتھ GET"""
    url = f"{OKX_BASE_URL}{path}"
    for i in range(retries):
        time.sleep(REQUEST_DELAY)
        try:
            r = requests.get(url, params=params, timeout=15)
        except requests.RequestException:
            time.sleep(1 + i)
            continue
        if r.status_code == 429:
            time.sleep(1 + i)
            continue
        return r
    return None


def get_default_symbols():
    return ["SOLUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT",
            "OPUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT"]


def get_top_symbols():
    """OKX سے USDT پرپیچوئل سکے (ڈالر والیوم کے لحاظ سے)"""
    print(f"Fetching top {TOP_N} USDT perpetual symbols from OKX...")
    r = okx_get("/api/v5/market/tickers", {"instType": "SWAP"})
    if r is None or r.status_code != 200:
        print("OKX request failed. Using default symbols.")
        return get_default_symbols()
    try:
        data = r.json()
        if data.get("code") != "0":
            print(f"OKX API Error: {data.get('msg')}")
            return get_default_symbols()

        tickers = [t for t in data["data"] if t["instId"].endswith("-USDT-SWAP")]

        def usd_vol(t):
            try:
                return float(t.get("volCcy24h") or 0) * float(t.get("last") or 0)
            except ValueError:
                return 0.0

        tickers.sort(key=usd_vol, reverse=True)
        symbols = [t["instId"].replace("-USDT-SWAP", "USDT") for t in tickers[:TOP_N]]
        print(f"Fetched {len(symbols)} symbols.")
        return symbols
    except Exception as e:
        print(f"Error parsing symbols: {e}. Using default symbols.")
        return get_default_symbols()


def send_ntfy_notification(title, message, tags="chart"):
    if not NTFY_URL:
        print("   -> NTFY_TOPIC not set, notification skipped.")
        return
    try:
        headers = {"Title": title.encode("utf-8"), "Tags": tags, "Priority": "high"}
        r = requests.post(NTFY_URL, data=message.encode("utf-8"),
                          headers=headers, timeout=10)
        if r.status_code == 200:
            print("   -> NTFY notification sent.")
        else:
            print(f"   -> NTFY HTTP {r.status_code}")
    except Exception as e:
        print(f"   -> NTFY Error: {e}")


def get_data(symbol, interval, limit=200):
    """OKX سے مکمل شدہ کینڈلز"""
    params = {
        "instId": symbol[:-4] + "-USDT-SWAP",
        "bar": INTERVAL_MAP.get(interval, "30m"),
        "limit": limit,
    }
    r = okx_get("/api/v5/market/candles", params)
    if r is None or r.status_code != 200:
        return pd.DataFrame()
    try:
        data = r.json()
        if data.get("code") != "0" or not data.get("data"):
            return pd.DataFrame()

        klines = data["data"][::-1]  # پرانے سے نئے
        df = pd.DataFrame(klines, columns=[
            "time", "open", "high", "low", "close",
            "volume", "volCcy", "volCcyQuote", "confirm"])
        df["time"] = pd.to_datetime(df["time"].astype(float), unit="ms", utc=True)
        for c in ["open", "high", "low", "close", "volume"]:
            df[c] = pd.to_numeric(df[c])
        df = df[df["confirm"] == "1"]
        df = df[["time", "open", "high", "low", "close", "volume"]]
        return df.drop_duplicates("time").sort_values("time").reset_index(drop=True)
    except Exception:
        return pd.DataFrame()


def add_indicators(df):
    df = df.copy()
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    # موجودہ کینڈل کو اوسط سے باہر رکھا گیا ہے
    df["vol_avg"] = df["volume"].rolling(VOL_PERIOD).mean().shift(1)
    df["vol_ok"] = df["volume"] >= df["vol_avg"] * VOL_MULT
    return df


def find_sweeps(df, lookback, hours, tf):
    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    times = df["time"]

    old_high = df["high"].rolling(lookback, min_periods=lookback).max().shift(1).values
    old_low = df["low"].rolling(lookback, min_periods=lookback).min().shift(1).values

    with np.errstate(invalid="ignore"):
        high_mask = (high > old_high) & (close < old_high * (1 - BUFFER))
        low_mask = (low < old_low) & (close > old_low * (1 + BUFFER))

    sweeps = []
    delta = pd.Timedelta(hours=hours)

    for i in np.where(high_mask)[0]:
        t = times.iloc[i]
        sweeps.append({"time": t, "type": "HIGH", "extreme": high[i],
                       "until": t + delta, "tf": tf})
    for i in np.where(low_mask)[0]:
        t = times.iloc[i]
        sweeps.append({"time": t, "type": "LOW", "extreme": low[i],
                       "until": t + delta, "tf": tf})

    sweeps.sort(key=lambda x: x["time"])
    return sweeps


def check_live_signal(df, active_sweeps, symbol, tf, now):
    if len(df) < 2:
        return

    last = df.iloc[-1]
    t = last["time"]
    r = last["rsi"]
    close = last["close"]

    if pd.isna(r) or pd.isna(last["vol_avg"]) or not last["vol_ok"]:
        return

    # صرف تازہ بند ہوئی کینڈل پر سگنل
    candle_close = t + TF_DELTA[tf]
    if now - candle_close > TF_DELTA[tf] * MAX_SIGNAL_AGE_CANDLES:
        return

    for sweep in active_sweeps:
        # سگنل کینڈل سویپ کینڈل کے بند ہونے کے بعد کی ہو
        if t < sweep["time"] + TF_DELTA[sweep["tf"]]:
            continue

        key = f"{symbol}|{tf}|{sweep['tf']}|{sweep['time'].isoformat()}|{sweep['type']}"
        if key in SIGNALED:
            continue

        if sweep["type"] == "LOW":
            if r > RSI_OS:
                continue
            entry = close
            sl = sweep["extreme"] * (1 - BUFFER)
            risk = entry - sl
            if risk <= 0:
                continue
            tps = [entry + risk * m for m in (1, 2, 3)]
            title = f"🚀 LONG: {symbol} ({tf})"
            tags = "rocket,chart_with_upwards_trend"
        else:
            if r < RSI_OB:
                continue
            entry = close
            sl = sweep["extreme"] * (1 + BUFFER)
            risk = sl - entry
            if risk <= 0:
                continue
            tps = [entry - risk * m for m in (1, 2, 3)]
            title = f"🔻 SHORT: {symbol} ({tf})"
            tags = "arrow_down,chart_with_downwards_trend"

        SIGNALED[key] = sweep["until"].isoformat()
        save_state()

        msg = (f"Sweep: {sweep['tf']} {sweep['type']}\n"
               f"Entry: {entry:.4f}\nSL: {sl:.4f}\n"
               f"TP1: {tps[0]:.4f}\nTP2: {tps[1]:.4f}\nTP3: {tps[2]:.4f}")
        print(f"\n{title}\n{msg}")
        send_ntfy_notification(title, msg, tags=tags)


def scan_symbol(symbol, now):
    daily = get_data(symbol, "1d")
    h4 = get_data(symbol, "4h")
    if daily.empty or h4.empty:
        return 0

    all_sweeps = (find_sweeps(daily, DAILY_LOOKBACK, DAILY_HOURS, "1D")
                  + find_sweeps(h4, H4_LOOKBACK, H4_HOURS, "4H"))
    active = [s for s in all_sweeps if s["until"] >= now]
    if not active:
        return 0

    # 1H اور 30M صرف تب جب ایکٹو سویپ ہو
    h1 = get_data(symbol, "1h")
    if not h1.empty:
        check_live_signal(add_indicators(h1), active, symbol, "1H", now)

    m30 = get_data(symbol, "30m")
    if not m30.empty:
        check_live_signal(add_indicators(m30), active, symbol, "30M", now)

    return len(active)


def main():
    parser = argparse.ArgumentParser(description="Liquidity Sweep Live Scanner (OKX)")
    parser.add_argument("--once", action="store_true",
                        help="ایک اسکین کر کے بند (GitHub Actions کے لیے)")
    parser.add_argument("--test-notify", action="store_true",
                        help="ٹیسٹ نوٹیفکیشن بھیج کر بند")
    args = parser.parse_args()

    if args.test_notify:
        send_ntfy_notification("Test Notification", "Test message from the OKX scanner.")
        sys.exit(0)

    print("=" * 65)
    print("LIQUIDITY SWEEP LIVE SCANNER (OKX)")
    print("=" * 65)

    if not NTFY_TOPIC:
        print("WARNING: NTFY_TOPIC environment variable is not set.")

    load_state()

    if not args.once:
        send_ntfy_notification("Scanner Started", "OKX Scanner is running continuously.",
                               tags="white_check_mark")

    symbols = get_top_symbols()
    print(f"Scanning {len(symbols)} symbols...\n")

    start_time = time.time()
    MAX_RUNTIME = 5.5 * 3600

    while True:
        if not args.once and time.time() - start_time > MAX_RUNTIME:
            print("\nApproaching time limit. Exiting gracefully...")
            sys.exit(0)

        try:
            now = pd.Timestamp.now(tz="UTC")
            print(f"\n[{now.strftime('%Y-%m-%d %H:%M:%S')} UTC] Scanning {len(symbols)} coins...")
            prune_state()

            total_active = 0
            for symbol in symbols:
                try:
                    total_active += scan_symbol(symbol, now)
                except Exception as e:
                    print(f"   {symbol} error: {e}")

            print(f"   Status: Scanned {len(symbols)} coins. Active sweeps: {total_active}")

            if args.once:
                print("Single scan complete. Exiting...")
                sys.exit(0)

            print("   Waiting 5 minutes for next scan...")
            time.sleep(300)

        except Exception as e:
            print(f"\n⚠️ Error in main loop: {e}")
            time.sleep(60)


if __name__ == "__main__":
    main()
        
