import requests
import pandas as pd
import numpy as np
import time
import json
import os
import sys

# ============================================================
# Configuration
# ============================================================

BUFFER = 0.001
RSI_PERIOD = 9
RSI_OB = 75
RSI_OS = 25
VOL_PERIOD = 20
VOL_MULT = 1.5

DAILY_LOOKBACK = 1          # صرف پچھلے دن کی کینڈل
H4_LOOKBACK = 5             # 4H کی 5 کینڈلز کا سویپ

DAILY_WINDOW_HOURS = 12
H4_WINDOW_HOURS = 6
RECENT_CANDLES = 8

ENTRY_TIMEFRAME = "30m"
SCAN_INTERVAL_MINUTES = 30
REQUEST_DELAY = 0.20
MAX_SYMBOLS = 500

# GitHub Actions کی 6 گھنٹے کی حد سے پہلے خود بند ہو جاؤ
MAX_RUN_HOURS = 5.5
START_TIME = time.time()

OKX_BASE_URL = "https://www.okx.com"

# Topic اب GitHub Secret سے آئے گا (کوڈ میں نہیں)
NTFY_TOPIC = os.getenv("NTFY_TOPIC", "").strip()
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_state.json")


# ============================================================
# Symbol Loading
# ============================================================

def get_top_symbols():
    print(f"Fetching Top {MAX_SYMBOLS} USDT Perpetual Symbols from OKX...")
    try:
        url = f"{OKX_BASE_URL}/api/v5/market/tickers?instType=SWAP"
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return get_default_symbols()
        data = r.json()
        if data.get('code') != '0':
            return get_default_symbols()

        tickers = [t for t in data['data'] if t['instId'].endswith('-USDT-SWAP')]

        # volCcy24h کوائن کی تعداد میں ہوتا ہے، اس لیے قیمت سے ضرب دے کر USDT ویلیو نکالی
        def usdt_volume(t):
            try:
                return float(t.get('volCcy24h') or 0) * float(t.get('last') or 0)
            except ValueError:
                return 0.0

        tickers.sort(key=usdt_volume, reverse=True)
        top = [t['instId'].replace('-USDT-SWAP', 'USDT') for t in tickers[:MAX_SYMBOLS]]
        print(f"Successfully fetched {len(top)} symbols.")
        return top
    except Exception as e:
        print(f"Error: {e}")
        return get_default_symbols()


def get_default_symbols():
    return ["SOLUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT",
            "OPUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT"]


# ============================================================
# Data Fetching
# ============================================================

def get_data(symbol, interval, limit=200):
    # "1Dutc" = UTC پر کھلنے والی daily کینڈل
    interval_map = {"1d": "1Dutc", "4h": "4H", "1h": "1H", "30m": "30m"}
    okx_interval = interval_map.get(interval, "30m")
    okx_symbol = symbol.replace('USDT', '-USDT-SWAP')

    params = {"instId": okx_symbol, "bar": okx_interval, "limit": limit}

    try:
        r = requests.get(f"{OKX_BASE_URL}/api/v5/market/candles", params=params, timeout=15)
        if r.status_code != 200:
            return pd.DataFrame()
        data = r.json()
    except Exception:
        return pd.DataFrame()

    time.sleep(REQUEST_DELAY)

    if data.get('code') != '0':
        return pd.DataFrame()

    klines = data['data']
    if not klines:
        return pd.DataFrame()

    klines = klines[::-1]
    df = pd.DataFrame(klines, columns=["time", "open", "high", "low", "close", "volume",
                                        "volCcy", "volCcyQuote", "confirm"])
    df["time"] = pd.to_datetime(df["time"].astype(float), unit="ms", utc=True).dt.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c])

    # صرف بند (confirmed) کینڈلز رکھو، کھلی کینڈل خود ہٹ جاتی ہے
    df = df[df["confirm"] == '1']
    df = df[["time", "open", "high", "low", "close", "volume"]]
    df = df.drop_duplicates("time").sort_values("time").reset_index(drop=True)

    return df


def add_indicators(df):
    df = df.copy()
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1 / RSI_PERIOD, min_periods=RSI_PERIOD, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))

    df["vol_avg"] = df["volume"].rolling(VOL_PERIOD).mean()
    df["vol_ok"] = df["volume"] >= df["vol_avg"] * VOL_MULT

    return df


# ============================================================
# Sweep Detection
# ============================================================

def find_sweeps(df, lookback, hours, tf, bar_hours):
    if df.empty or len(df) <= lookback:
        return []

    high = df["high"].values
    low = df["low"].values
    close = df["close"].values
    time_series = df["time"]

    old_high = df["high"].rolling(lookback, min_periods=lookback).max().shift(1).values
    old_low = df["low"].rolling(lookback, min_periods=lookback).min().shift(1).values

    with np.errstate(invalid="ignore"):
        high_mask = (high > old_high) & (close < old_high * (1 - BUFFER))
        low_mask = (low < old_low) & (close > old_low * (1 + BUFFER))
    high_mask = np.nan_to_num(high_mask, nan=False).astype(bool)
    low_mask = np.nan_to_num(low_mask, nan=False).astype(bool)

    sweeps = []
    delta = pd.Timedelta(hours=hours)
    bar = pd.Timedelta(hours=bar_hours)

    for i in np.where(high_mask)[0]:
        t = time_series.iloc[i] + bar
        sweeps.append({"time": t, "type": "HIGH", "extreme": high[i], "until": t + delta, "tf": tf})

    for i in np.where(low_mask)[0]:
        t = time_series.iloc[i] + bar
        sweeps.append({"time": t, "type": "LOW", "extreme": low[i], "until": t + delta, "tf": tf})

    sweeps.sort(key=lambda x: x["time"])
    return sweeps


# ============================================================
# Signal Generation
# ============================================================

def scan_recent(symbol, entry_df, sweeps, state, n_recent=RECENT_CANDLES):
    signals = []
    if entry_df.empty or len(entry_df) < 2:
        return signals

    used = set(state.get("used", []))
    recent = entry_df.iloc[-n_recent:]

    for _, row in recent.iterrows():
        t = row["time"]
        r = row["rsi"]

        if pd.isna(r) or pd.isna(row["vol_avg"]) or not row["vol_ok"]:
            continue

        active = [s for s in sweeps if s["time"] <= t <= s["until"]]
        active.sort(key=lambda s: s["time"], reverse=True)

        for sweep in active:
            skey = f"{symbol}|{sweep['tf']}|{sweep['type']}|{sweep['time'].isoformat()}"
            if skey in used:
                continue

            if sweep["type"] == "LOW":
                if r > RSI_OS:
                    continue
                entry = row["close"]
                sl = sweep["extreme"] * (1 - BUFFER)
                risk = entry - sl
                if risk <= 0:
                    continue
                tps = {rr: entry + risk * rr for rr in [1, 2, 3]}
                side = "LONG"
            else:
                if r < RSI_OB:
                    continue
                entry = row["close"]
                sl = sweep["extreme"] * (1 + BUFFER)
                risk = sl - entry
                if risk <= 0:
                    continue
                tps = {rr: entry - risk * rr for rr in [1, 2, 3]}
                side = "SHORT"

            used.add(skey)
            signals.append({"time": t, "side": side, "entry": entry, "sl": sl, "tps": tps,
                            "sweep_tf": sweep["tf"], "sweep_type": sweep["type"], "sweep_key": skey})
            break

    return signals


# ============================================================
# State Management
# ============================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("State save error:", e)


# ============================================================
# NTFY Notification
# ============================================================

def send_ntfy(symbol, signal):
    side = signal["side"]
    entry = signal["entry"]
    sl = signal["sl"]
    tps = signal["tps"]

    tp_lines = "\n".join(f"TP (1:{rr}): {tp:.6f}" for rr, tp in sorted(tps.items()))
    title = f"{'LONG' if side == 'LONG' else 'SHORT'}: {symbol} (30M)"

    message = (
        f"Coin: {symbol}\n"
        f"Side: {side}\n"
        f"Sweep: {signal['sweep_type']} ({signal['sweep_tf']})\n"
        f"Entry: {entry:.6f}\n"
        f"SL: {sl:.6f}\n"
        f"{tp_lines}"
    )

    try:
        resp = requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={
                "Title": title,
                "Priority": "high",
                "Tags": "rocket" if side == "LONG" else "arrow_down",
            },
            timeout=10,
        )
        resp.raise_for_status()
        print(f"  -> NTFY Sent: {symbol} {side}")
        return True
    except Exception as e:
        print(f"  -> NTFY Error: {e}")
        return False


# ============================================================
# Scan Cycle
# ============================================================

def run_scan_cycle(symbols, state):
    new_count = 0
    for idx, symbol in enumerate(symbols):
        print(f"[{idx+1}/{len(symbols)}] {symbol}", end="\r")

        daily = get_data(symbol, "1d", limit=30)
        h4 = get_data(symbol, "4h", limit=60)
        entry_df = get_data(symbol, "30m", limit=60)

        if daily.empty or h4.empty or entry_df.empty:
            continue

        daily_sweeps = find_sweeps(daily, DAILY_LOOKBACK, DAILY_WINDOW_HOURS, "1D", bar_hours=24)
        h4_sweeps = find_sweeps(h4, H4_LOOKBACK, H4_WINDOW_HOURS, "4H", bar_hours=4)
        sweeps = daily_sweeps + h4_sweeps

        if not sweeps:
            continue

        entry_df = add_indicators(entry_df)
        signals = scan_recent(symbol, entry_df, sweeps, state)

        for signal in signals:
            if send_ntfy(symbol, signal):
                state.setdefault("used", []).append(signal["sweep_key"])
                new_count += 1

    state["used"] = state.get("used", [])[-5000:]
    print(" " * 80, end="\r")
    return new_count


# ============================================================
# Main
# ============================================================

def main():
    if not NTFY_TOPIC:
        print("ERROR: NTFY_TOPIC environment variable is not set.")
        sys.exit(1)

    print("=" * 65)
    print("LIQUIDITY SWEEP LIVE SCANNER")
    print(f"Daily Lookback: {DAILY_LOOKBACK} candle (previous day)")
    print(f"4H Lookback: {H4_LOOKBACK} candles")
    print("=" * 65)

    symbols = get_top_symbols()
    if not symbols:
        print("No symbols. Exiting.")
        sys.exit(1)

    print(f"Scanning {len(symbols)} symbols...\n")
    state = load_state()
    max_run_seconds = MAX_RUN_HOURS * 3600

    while True:
        current_utc = pd.Timestamp.now(tz='UTC').tz_localize(None)
        print(f"\n[{current_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] Scan starting...")

        start = time.time()
        count = run_scan_cycle(symbols, state)
        save_state(state)
        scan_seconds = time.time() - start

        print(f"Scan complete in {scan_seconds/60:.1f} min. New signals: {count}")

        now = pd.Timestamp.now(tz='UTC').tz_localize(None)
        next_scan = now.ceil(f'{SCAN_INTERVAL_MINUTES}min')
        sleep_seconds = (next_scan - now).total_seconds()

        if sleep_seconds < 30:
            next_scan = next_scan + pd.Timedelta(minutes=SCAN_INTERVAL_MINUTES)
            sleep_seconds = (next_scan - now).total_seconds()

        # اگلا سکین پورا ہونے سے پہلے وقت ختم ہو جائے گا تو صاف طریقے سے بند ہو جاؤ
        elapsed_total = time.time() - START_TIME
        if elapsed_total + sleep_seconds + scan_seconds > max_run_seconds:
            print("Time limit reached. State saved. Exiting cleanly.")
            save_state(state)
            break

        print(f"Next scan at {next_scan.strftime('%H:%M')} UTC (sleeping {sleep_seconds/60:.1f} min)")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
        
