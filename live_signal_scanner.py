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

# --- Aroon ---
AROON_PERIOD = 14
PIVOT_LEFT = 2
PIVOT_RIGHT = 2

# --- 4H Sweep (5-6 candles) ---
H4_LOOKBACK = 6
H4_WINDOW_HOURS = 8

# --- Entry ---
ENTRY_TIMEFRAMES = ["15m", "30m"]
RECENT_CANDLES = 30

SCAN_INTERVAL_MINUTES = 30
REQUEST_DELAY = 0.20
MAX_SYMBOLS = 500

OKX_BASE_URL = "https://www.okx.com"
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "ChartMaster786x7k2p9")
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"
STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_state.json")


# ============================================================
# Symbols
# ============================================================

def get_top_500_symbols():
    print("Fetching Top 500 USDT Perpetual Symbols from OKX...")
    try:
        url = f"{OKX_BASE_URL}/api/v5/market/tickers?instType=SWAP"
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return get_default_symbols()
        data = r.json()
        if data.get('code') != '0':
            return get_default_symbols()

        tickers = data['data']
        usdt_tickers = [t for t in tickers if t['instId'].endswith('-USDT-SWAP')]
        usdt_tickers.sort(key=lambda x: float(x.get('volCcy24h', 0)), reverse=True)
        top = [t['instId'].replace('-USDT-SWAP', 'USDT') for t in usdt_tickers[:MAX_SYMBOLS]]
        print(f"Successfully fetched {len(top)} symbols.")
        return top
    except Exception as e:
        print(f"Error: {e}")
        return get_default_symbols()


def get_default_symbols():
    return ["SOLUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT",
            "OPUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT"]


# ============================================================
# Data
# ============================================================

def get_data(symbol, interval, limit=200):
    interval_map = {"1d": "1D", "4h": "4H", "1h": "1H", "30m": "30m", "15m": "15m"}
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

    df = df[df["confirm"] == '1']
    df = df[["time", "open", "high", "low", "close", "volume"]]
    df = df.drop_duplicates("time").sort_values("time").reset_index(drop=True)

    if len(df) > 1:
        df = df.iloc[:-1].copy()

    return df


# ============================================================
# Aroon Indicator
# ============================================================

def add_aroon(df, period=AROON_PERIOD):
    df = df.copy()
    window = period + 1

    df["aroon_up"] = df["high"].rolling(window).apply(
        lambda x: (x.argmax() / period) * 100, raw=True
    )
    df["aroon_down"] = df["low"].rolling(window).apply(
        lambda x: (x.argmin() / period) * 100, raw=True
    )

    return df


# ============================================================
# Pivot Detection
# ============================================================

def find_pivot_lows(df, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    lows = df["low"].values
    indices = []
    for i in range(left, len(lows) - right):
        if lows[i] == lows[i-left:i+right+1].min():
            indices.append(i)
    return indices


def find_pivot_highs(df, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    highs = df["high"].values
    indices = []
    for i in range(left, len(highs) - right):
        if highs[i] == highs[i-left:i+right+1].max():
            indices.append(i)
    return indices


# ============================================================
# Divergence Detection
# ============================================================

def check_bullish_divergence(df):
    """
    Bullish Divergence:
    - Price makes a LOWER LOW
    - Aroon Down makes a LOWER LOW (downtrend weakening)
    OR
    - Aroon Up makes a HIGHER LOW (buying pressure increasing)
    """
    pivot_lows = find_pivot_lows(df)
    if len(pivot_lows) < 2:
        return False

    last, prev = pivot_lows[-1], pivot_lows[-2]

    price_lower_low = df["low"].iloc[last] < df["low"].iloc[prev]
    if not price_lower_low:
        return False

    aroon_down_lower = df["aroon_down"].iloc[last] < df["aroon_down"].iloc[prev]
    aroon_up_higher = df["aroon_up"].iloc[last] > df["aroon_up"].iloc[prev]

    return aroon_down_lower or aroon_up_higher


def check_bearish_divergence(df):
    """
    Bearish Divergence:
    - Price makes a HIGHER HIGH
    - Aroon Up makes a LOWER HIGH (uptrend weakening)
    OR
    - Aroon Down makes a HIGHER LOW (selling pressure increasing)
    """
    pivot_highs = find_pivot_highs(df)
    if len(pivot_highs) < 2:
        return False

    last, prev = pivot_highs[-1], pivot_highs[-2]

    price_higher_high = df["high"].iloc[last] > df["high"].iloc[prev]
    if not price_higher_high:
        return False

    aroon_up_lower = df["aroon_up"].iloc[last] < df["aroon_up"].iloc[prev]
    aroon_down_higher = df["aroon_down"].iloc[last] > df["aroon_down"].iloc[prev]

    return aroon_up_lower or aroon_down_higher


# ============================================================
# 4H Sweep Detection
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

def scan_recent(symbol, entry_df, entry_tf, sweeps, state, n_recent=RECENT_CANDLES):
    signals = []
    if entry_df.empty or len(entry_df) < AROON_PERIOD + PIVOT_LEFT + PIVOT_RIGHT + 5:
        return signals

    used = set(state.get("used", []))
    recent = entry_df.iloc[-n_recent:]

    for _, row in recent.iterrows():
        t = row["time"]

        active = [s for s in sweeps if s["time"] <= t <= s["until"]]
        active.sort(key=lambda s: s["time"], reverse=True)

        for sweep in active:
            skey = f"{symbol}|{entry_tf}|{sweep['tf']}|{sweep['type']}|{sweep['time'].isoformat()}"
            if skey in used:
                continue

            if sweep["type"] == "LOW":
                if not check_bullish_divergence(entry_df):
                    continue
                entry = row["close"]
                sl = sweep["extreme"] * (1 - BUFFER)
                risk = entry - sl
                if risk <= 0:
                    continue
                tps = {rr: entry + risk * rr for rr in [1, 2, 3]}
                side = "LONG"
            else:
                if not check_bearish_divergence(entry_df):
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
                            "sweep_tf": sweep["tf"], "sweep_type": sweep["type"],
                            "entry_tf": entry_tf, "sweep_key": skey})
            break

    return signals


# ============================================================
# State
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
# NTFY (Fixed: No emojis in Title to avoid latin-1 encoding error)
# ============================================================

def send_ntfy(symbol, signal):
    side = signal["side"]
    entry = signal["entry"]
    sl = signal["sl"]
    tps = signal["tps"]

    tp_lines = "\n".join(f"TP (1:{rr}): {tp:.6f}" for rr, tp in sorted(tps.items()))

    # IMPORTANT: Title میں صرف سادہ ASCII ٹیکسٹ (کوئی ایموجی نہیں)
    title = f"{side}: {symbol} ({signal['entry_tf']})"

    message = (
        f"Coin: {symbol}\n"
        f"Side: {side}\n"
        f"Entry TF: {signal['entry_tf']}\n"
        f"Sweep: {signal['sweep_type']} ({signal['sweep_tf']})\n"
        f"Confirmed: Aroon Divergence\n"
        f"Entry: {entry:.6f}\n"
        f"SL: {sl:.6f}\n"
        f"{tp_lines}"
    )

    try:
        resp = requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={
                "Title": title,  # No emojis here
                "Priority": "high",
                "Tags": "rocket" if side == "LONG" else "arrow_down",
            },
            timeout=10,
        )
        resp.raise_for_status()
        print(f"  -> NTFY Sent: {symbol} {side} ({signal['entry_tf']})")
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

        h4 = get_data(symbol, "4h", limit=60)
        if h4.empty:
            continue

        h4_sweeps = find_sweeps(h4, H4_LOOKBACK, H4_WINDOW_HOURS, "4H", bar_hours=4)
        if not h4_sweeps:
            continue

        for entry_tf in ENTRY_TIMEFRAMES:
            entry_df = get_data(symbol, entry_tf, limit=100)
            if entry_df.empty:
                continue

            entry_df = add_aroon(entry_df)
            signals = scan_recent(symbol, entry_df, entry_tf, h4_sweeps, state)

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
    print("=" * 65)
    print("LIQUIDITY SWEEP + AROON DIVERGENCE SCANNER")
    print(f"4H Sweep Lookback: {H4_LOOKBACK} candles")
    print(f"Entry TFs: {ENTRY_TIMEFRAMES}")
    print(f"Aroon Period: {AROON_PERIOD}")
    print(f"NTFY Topic: {NTFY_TOPIC}")
    print("=" * 65)

    symbols = get_top_500_symbols()
    if not symbols:
        print("No symbols. Exiting.")
        sys.exit(1)

    print(f"Scanning {len(symbols)} symbols...\n")
    state = load_state()

    while True:
        current_utc = pd.Timestamp.now(tz='UTC').tz_localize(None)
        print(f"\n[{current_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] Scan starting...")

        start = time.time()
        count = run_scan_cycle(symbols, state)
        save_state(state)
        elapsed = time.time() - start

        print(f"Scan complete in {elapsed/60:.1f} min. New signals: {count}")

        now = pd.Timestamp.now(tz='UTC').tz_localize(None)
        next_scan = now.ceil(f'{SCAN_INTERVAL_MINUTES}min')
        sleep_seconds = (next_scan - now).total_seconds()

        if sleep_seconds < 30:
            next_scan = next_scan + pd.Timedelta(minutes=SCAN_INTERVAL_MINUTES)
            sleep_seconds = (next_scan - now).total_seconds()

        print(f"Next scan at {next_scan.strftime('%H:%M')} UTC (sleeping {sleep_seconds/60:.1f} min)")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
