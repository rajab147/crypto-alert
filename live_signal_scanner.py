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

# --- RSI ---
RSI_PERIOD = 11
RSI_OVERBOUGHT = 70
RSI_OVERSOLD = 30

# --- Volume ---
VOLUME_LOOKBACK = 5          # پچھلی 5 کینڈلز سے موازنہ
CLIMAX_VOLUME_MULT = 2.0     # اوسط سے 2 گنا (Climax کے لیے)
CLIMAX_BODY_MULT = 1.5       # اوسط باڈی سے 1.5 گنا

# --- Sweep ---
DAILY_LOOKBACK = 1
H4_LOOKBACK = 5
DAILY_WINDOW_HOURS = 24
H4_WINDOW_HOURS = 8

# --- Pivot ---
PIVOT_LEFT = 2
PIVOT_RIGHT = 2

# --- Entry ---
ENTRY_TIMEFRAMES = ["15m", "30m"]   # دونوں لازمی
MAX_ENTRY_DRIFT_PERCENT = 1.0

# --- Symbols ---
MAX_SYMBOLS = 200                    # صرف Top 100

# --- Global ---
MAX_SIGNALS_PER_SCAN = 5
SCAN_INTERVAL_MINUTES = 30
REQUEST_DELAY = 0.20

MEXC_BASE_URL = "https://contract.mexc.com"

# --- NTFY (GitHub Secret) ---
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
if not NTFY_TOPIC:
    print("ERROR: NTFY_TOPIC not set in GitHub Secrets!")
    sys.exit(1)
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_state.json")
COOLDOWN_HOURS = 4
MIN_RISK_PERCENT = 0.15


# ============================================================
# Symbols
# ============================================================

def get_top_200_symbols():
    print(f"Fetching Top {MAX_SYMBOLS} USDT Perpetual Symbols from MEXC...")
    try:
        url = f"{MEXC_BASE_URL}/api/v1/contract/ticker"
        r = requests.get(url, timeout=20)
        if r.status_code != 200:
            return get_default_symbols()
        data = r.json()
        if not data.get('success'):
            return get_default_symbols()
        tickers = data['data']
        usdt = [t for t in tickers if t['symbol'].endswith('_USDT')]
        usdt.sort(key=lambda x: float(x.get('amount24', 0)), reverse=True)
        top = [t['symbol'].replace('_USDT', 'USDT') for t in usdt[:MAX_SYMBOLS]]
        print(f"Fetched {len(top)} symbols.")
        return top
    except Exception as e:
        print(f"Error: {e}")
        return get_default_symbols()


def get_default_symbols():
    return ["SOLUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT",
            "OPUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT"]


# ============================================================
# Live Prices
# ============================================================

def get_all_live_prices():
    try:
        url = f"{MEXC_BASE_URL}/api/v1/contract/ticker"
        r = requests.get(url, timeout=15)
        if r.status_code != 200:
            return {}
        data = r.json()
        if not data.get('success'):
            return {}
        prices = {}
        for t in data['data']:
            if t['symbol'].endswith('_USDT'):
                s = t['symbol'].replace('_USDT', 'USDT')
                try:
                    prices[s] = float(t['lastPrice'])
                except:
                    continue
        return prices
    except:
        return {}


# ============================================================
# Data
# ============================================================

def get_data(symbol, interval, limit=200):
    imap = {"1d": "Day1", "4h": "Hour4", "1h": "Hour1", "30m": "Min30", "15m": "Min15"}
    mexc_interval = imap.get(interval, "Min30")
    mexc_symbol = symbol.replace('USDT', '_USDT')
    url = f"{MEXC_BASE_URL}/api/v1/contract/kline/{mexc_symbol}"
    params = {"interval": mexc_interval}
    try:
        r = requests.get(url, params=params, timeout=15)
        if r.status_code != 200:
            return pd.DataFrame()
        data = r.json()
    except:
        return pd.DataFrame()
    time.sleep(REQUEST_DELAY)
    if not data.get('success'):
        return pd.DataFrame()
    kd = data.get('data', {})
    times = kd.get('time', [])
    if not times:
        return pd.DataFrame()
    df = pd.DataFrame({
        "time": times, "open": kd.get('open', []), "high": kd.get('high', []),
        "low": kd.get('low', []), "close": kd.get('close', []), "volume": kd.get('vol', [])
    })
    df["time"] = pd.to_datetime(df["time"].astype(float), unit="s", utc=True).dt.tz_localize(None)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c], errors='coerce')
    df = df.dropna().drop_duplicates("time").sort_values("time").reset_index(drop=True)
    if len(df) > limit:
        df = df.iloc[-limit:].copy()
    if len(df) > 1:
        df = df.iloc[:-1].copy()
    return df


# ============================================================
# RSI
# ============================================================

def add_rsi(df, period=RSI_PERIOD):
    df = df.copy()
    delta = df["close"].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    avg_loss = loss.ewm(alpha=1/period, min_periods=period, adjust=False).mean()
    rs = avg_gain / avg_loss
    df["rsi"] = 100 - (100 / (1 + rs))
    df["body"] = (df["close"] - df["open"]).abs()
    return df


# ============================================================
# Engulfing Detection
# ============================================================

def is_bullish_engulfing(df):
    """Buy Engulfing: موجودہ ہری کینڈل پچھلی سرخ کو نگل لے"""
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    prev_bearish = prev["close"] < prev["open"]
    curr_bullish = curr["close"] > curr["open"]
    engulfs = curr["open"] <= prev["close"] and curr["close"] >= prev["open"]
    return prev_bearish and curr_bullish and engulfs


def is_bearish_engulfing(df):
    """Sell Engulfing: موجودہ سرخ کینڈل پچھلی ہری کو نگل لے"""
    if len(df) < 2:
        return False
    prev = df.iloc[-2]
    curr = df.iloc[-1]
    prev_bullish = prev["close"] > prev["open"]
    curr_bearish = curr["close"] < curr["open"]
    engulfs = curr["open"] >= prev["close"] and curr["close"] <= prev["open"]
    return prev_bullish and curr_bearish and engulfs


def has_small_volume(df, lookback=VOLUME_LOOKBACK):
    """انگلوفنگ کینڈل کا والیوم چھوٹا ہو"""
    if len(df) < lookback + 1:
        return False
    curr_vol = df["volume"].iloc[-1]
    prev_avg = df["volume"].iloc[-(lookback + 1):-1].mean()
    return curr_vol < prev_avg


# ============================================================
# Volume Climax
# ============================================================

def is_buy_climax(df, lookback=VOLUME_LOOKBACK):
    """بڑی والیوم + بڑی ہری باڈی والی کینڈل (خریداری کا کلائمیکس)"""
    if len(df) < lookback + 1:
        return False
    curr = df.iloc[-1]
    curr_vol = curr["volume"]
    curr_body = curr["body"]
    avg_vol = df["volume"].iloc[-(lookback+1):-1].mean()
    avg_body = df["body"].iloc[-(lookback+1):-1].mean()
    # ہری کینڈل + بڑی والیوم + بڑی باڈی
    return (curr["close"] > curr["open"] 
            and curr_vol > avg_vol * CLIMAX_VOLUME_MULT
            and curr_body > avg_body * CLIMAX_BODY_MULT)


def is_sell_climax(df, lookback=VOLUME_LOOKBACK):
    """بڑی والیوم + بڑی سرخ باڈی والی کینڈل (فروخت کا کلائمیکس)"""
    if len(df) < lookback + 1:
        return False
    curr = df.iloc[-1]
    curr_vol = curr["volume"]
    curr_body = curr["body"]
    avg_vol = df["volume"].iloc[-(lookback+1):-1].mean()
    avg_body = df["body"].iloc[-(lookback+1):-1].mean()
    return (curr["close"] < curr["open"]
            and curr_vol > avg_vol * CLIMAX_VOLUME_MULT
            and curr_body > avg_body * CLIMAX_BODY_MULT)


# ============================================================
# Pivot & RSI Divergence
# ============================================================

def find_pivot_lows(df, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    lows = df["low"].values
    return [i for i in range(left, len(lows) - right)
            if lows[i] == lows[i-left:i+right+1].min()]


def find_pivot_highs(df, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    highs = df["high"].values
    return [i for i in range(left, len(highs) - right)
            if highs[i] == highs[i-left:i+right+1].max()]


def check_bullish_rsi_divergence(df):
    pivots = find_pivot_lows(df)
    if len(pivots) < 2:
        return False
    last, prev = pivots[-1], pivots[-2]
    price_ll = df["low"].iloc[last] < df["low"].iloc[prev]
    rsi_hl = df["rsi"].iloc[last] > df["rsi"].iloc[prev]
    return price_ll and rsi_hl


def check_bearish_rsi_divergence(df):
    pivots = find_pivot_highs(df)
    if len(pivots) < 2:
        return False
    last, prev = pivots[-1], pivots[-2]
    price_hh = df["high"].iloc[last] > df["high"].iloc[prev]
    rsi_lh = df["rsi"].iloc[last] < df["rsi"].iloc[prev]
    return price_hh and rsi_lh


# ============================================================
# Sweep Detection
# ============================================================

def get_previous_day_levels(daily_df):
    if daily_df.empty or len(daily_df) < 1:
        return None, None
    prev = daily_df.iloc[-1]
    return prev["high"], prev["low"]


def get_h4_sweep_levels(h4_df, lookback=H4_LOOKBACK):
    if h4_df.empty or len(h4_df) < lookback:
        return None, None
    recent = h4_df.iloc[-lookback:]
    return recent["high"].max(), recent["low"].min()


def check_daily_sweep(live_price, prev_high, prev_low):
    if prev_high is None or prev_low is None:
        return None
    if live_price > prev_high * (1 + BUFFER):
        return "HIGH"
    if live_price < prev_low * (1 - BUFFER):
        return "LOW"
    return None


def check_h4_sweep(live_price, h4_high, h4_low):
    if h4_high is None or h4_low is None:
        return None
    if live_price > h4_high * (1 + BUFFER):
        return "HIGH"
    if live_price < h4_low * (1 - BUFFER):
        return "LOW"
    return None


# ============================================================
# Entry Confirmation (دونوں ٹائم فریمز پر)
# ============================================================

def check_entry_conditions(df):
    """
    ایک ٹائم فریم پر تمام شرائط چیک کریں:
    - Engulfing
    - Small Volume
    - RSI Divergence OR Overbought/Oversold
    - Climax (Buy or Sell)
    Return: (is_long, is_short, details)
    """
    if df.empty or len(df) < RSI_PERIOD + VOLUME_LOOKBACK + 5:
        return False, False, {}

    details = {}
    last_rsi = df["rsi"].iloc[-1]

    # --- LONG کی شرائط ---
    bull_engulf = is_bullish_engulfing(df)
    small_vol = has_small_volume(df)
    bull_div = check_bullish_rsi_divergence(df)
    oversold = pd.notna(last_rsi) and last_rsi < RSI_OVERSOLD
    sell_climax = is_sell_climax(df)   # نیچے کا کلائمیکس (LONG کے لیے)

    long_ok = (bull_engulf and small_vol 
               and (bull_div or oversold) 
               and sell_climax)

    details["long"] = {
        "engulf": bull_engulf, "small_vol": small_vol,
        "div": bull_div, "oversold": oversold, "climax": sell_climax
    }

    # --- SHORT کی شرائط ---
    bear_engulf = is_bearish_engulfing(df)
    bear_div = check_bearish_rsi_divergence(df)
    overbought = pd.notna(last_rsi) and last_rsi > RSI_OVERBOUGHT
    buy_climax = is_buy_climax(df)   # اوپر کا کلائمیکس (SHORT کے لیے)

    short_ok = (bear_engulf and small_vol 
                and (bear_div or overbought) 
                and buy_climax)

    details["short"] = {
        "engulf": bear_engulf, "small_vol": small_vol,
        "div": bear_div, "overbought": overbought, "climax": buy_climax
    }

    return long_ok, short_ok, details


def check_both_timeframes(symbol, state):
    """
    15m اور 30m دونوں پر شرائط چیک کریں
    """
    results = {}
    for tf in ENTRY_TIMEFRAMES:
        df = get_data(symbol, tf, limit=100)
        if df.empty:
            return None
        df = add_rsi(df)
        long_ok, short_ok, details = check_entry_conditions(df)
        results[tf] = {"long": long_ok, "short": short_ok, "details": details}
        time.sleep(REQUEST_DELAY)

    # دونوں ٹائم فریمز پر ایک ہی سمت کی تصدیق
    both_long = all(results[tf]["long"] for tf in ENTRY_TIMEFRAMES if tf in results)
    both_short = all(results[tf]["short"] for tf in ENTRY_TIMEFRAMES if tf in results)

    return {"both_long": both_long, "both_short": both_short, "per_tf": results}


# ============================================================
# Signal Generation
# ============================================================

def scan_symbol(symbol, live_price, daily_df, h4_df, state):
    if live_price is None or live_price <= 0:
        return None

    now_utc = pd.Timestamp.now(tz='UTC').tz_localize(None)
    cooldown = state.get("cooldown", {})
    if symbol in cooldown:
        last = pd.Timestamp(cooldown[symbol])
        if (now_utc - last).total_seconds() < COOLDOWN_HOURS * 3600:
            return None

    # سوئپ چیک
    prev_high, prev_low = get_previous_day_levels(daily_df)
    h4_high, h4_low = get_h4_sweep_levels(h4_df)

    daily_sweep = check_daily_sweep(live_price, prev_high, prev_low)
    h4_sweep = check_h4_sweep(live_price, h4_high, h4_low)

    if not daily_sweep and not h4_sweep:
        return None

    # سمت کا تعین
    if daily_sweep == "HIGH" or h4_sweep == "HIGH":
        expected_side = "SHORT"
        sweep_extreme = max([x for x in [prev_high, h4_high] if x is not None])
    else:
        expected_side = "LONG"
        sweep_extreme = min([x for x in [prev_low, h4_low] if x is not None])

    # دونوں ٹائم فریمز پر انٹری کنفرمیشن
    tf_result = check_both_timeframes(symbol, state)
    if tf_result is None:
        return None

    if expected_side == "LONG" and not tf_result["both_long"]:
        return None
    if expected_side == "SHORT" and not tf_result["both_short"]:
        return None

    # Entry, SL, TPs
    entry = live_price
    sl = sweep_extreme * ((1 - BUFFER) if expected_side == "LONG" else (1 + BUFFER))
    risk = (entry - sl) if expected_side == "LONG" else (sl - entry)

    if risk <= 0:
        return None

    risk_pct = (risk / entry) * 100
    if risk_pct < MIN_RISK_PERCENT:
        return None

    direction = 1 if expected_side == "LONG" else -1
    tps = {rr: entry + direction * risk * rr for rr in [1, 2, 3, 4, 5]}

    cooldown[symbol] = now_utc.isoformat()
    state["cooldown"] = cooldown

    return {
        "symbol": symbol,
        "side": expected_side,
        "entry": entry,
        "sl": sl,
        "tps": tps,
        "entry_tf": "15m+30m",
        "risk_pct": round(risk_pct, 3)
    }


# ============================================================
# State
# ============================================================

def load_state():
    if os.path.exists(STATE_FILE):
        try:
            with open(STATE_FILE, "r") as f:
                return json.load(f)
        except:
            return {}
    return {}


def save_state(state):
    try:
        with open(STATE_FILE, "w") as f:
            json.dump(state, f)
    except Exception as e:
        print("State save error:", e)


# ============================================================
# NTFY
# ============================================================

def send_ntfy(signal):
    symbol = signal["symbol"]
    side = signal["side"]
    entry = signal["entry"]
    sl = signal["sl"]
    tps = signal["tps"]

    tp_lines = "\n".join(f"🎯 TP{i}: {tps[i]:.6f}" for i in sorted(tps.keys()))

    if side == "LONG":
        position_text = "Long Buy"
        arrow = "🚀"
        direction_emoji = "📈"
    else:
        position_text = "Short Sell"
        arrow = "🔻"
        direction_emoji = "📉"

    title = f"{symbol} SIGNAL"

    message = (
        f"🚨 {symbol} SIGNAL 🚨\n\n"
        f"{arrow} Position: {position_text} {direction_emoji}\n"
        f"⚡ Leverage: 10x–20x\n"
        f"🎯 Entry: {entry:.6f}\n\n"
        f"{tp_lines}\n\n"
        f"🛑 SL: {sl:.6f}"
    )

    try:
        resp = requests.post(
            NTFY_URL,
            data=message.encode("utf-8"),
            headers={"Title": title, "Priority": "high", "Tags": "rotating_light"},
            timeout=10,
        )
        resp.raise_for_status()
        print(f"  -> SENT: {symbol} {side} | Entry: {entry:.6f}")
        time.sleep(2)
        return True
    except Exception as e:
        print(f"  -> NTFY Error: {e}")
        return False


# ============================================================
# Scan Cycle
# ============================================================

def run_scan_cycle(symbols, state):
    new_count = 0

    print("\nFetching live prices...")
    live_prices = get_all_live_prices()
    print(f"Got prices for {len(live_prices)} symbols.\n")

    for idx, symbol in enumerate(symbols):
        if new_count >= MAX_SIGNALS_PER_SCAN:
            print(f"\nMax signals ({MAX_SIGNALS_PER_SCAN}) reached.")
            break

        print(f"[{idx+1}/{len(symbols)}] {symbol}", end="\r")

        live_price = live_prices.get(symbol)
        if live_price is None:
            continue

        daily = get_data(symbol, "1d", limit=10)
        if daily.empty:
            continue

        h4 = get_data(symbol, "4h", limit=30)
        if h4.empty:
            continue

        # فوری فلٹر
        prev_high, prev_low = get_previous_day_levels(daily)
        h4_high, h4_low = get_h4_sweep_levels(h4)
        daily_sweep = check_daily_sweep(live_price, prev_high, prev_low)
        h4_sweep = check_h4_sweep(live_price, h4_high, h4_low)

        if not daily_sweep and not h4_sweep:
            continue

        # مکمل چیک (دونوں ٹائم فریمز)
        signal = scan_symbol(symbol, live_price, daily, h4, state)
        if signal:
            if send_ntfy(signal):
                new_count += 1

    print(" " * 80, end="\r")
    return new_count


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 65)
    print("RSI DIVERGENCE + ENGULFING + CLIMAX SCANNER (Top 200)")
    print(f"RSI: {RSI_PERIOD} | OB: {RSI_OVERBOUGHT} | OS: {RSI_OVERSOLD}")
    print(f"Symbols: Top {MAX_SYMBOLS}")
    print(f"Entry TFs: {ENTRY_TIMEFRAMES} (both must confirm)")
    print(f"NTFY: [HIDDEN - from GitHub Secret]")
    print("=" * 65)

    symbols = get_top_200_symbols()
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
            next_scan += pd.Timedelta(minutes=SCAN_INTERVAL_MINUTES)
            sleep_seconds = (next_scan - now).total_seconds()

        print(f"Next scan at {next_scan.strftime('%H:%M')} UTC")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
