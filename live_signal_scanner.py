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
RSI_OVERBOUGHT = 60
RSI_OVERSOLD = 40

# --- Volume ---
VOLUME_LOOKBACK = 3
CLIMAX_VOLUME_MULT = 1.3
CLIMAX_BODY_MULT = 1.0

# --- Rejection Candle ---
REJECTION_WICK_RATIO = 2.0
REJECTION_BODY_MAX = 0.4

# --- Sweep ---
H4_LOOKBACK = 5
DAILY_LOOKBACK = 1

# --- Pivot ---
PIVOT_LEFT = 2
PIVOT_RIGHT = 2

# --- Entry ---
ENTRY_TIMEFRAMES = ["15m", "30m"]
RECENT_CANDLES = 2          # صرف تازہ ترین 2 بند کینڈلز

# --- Risk ---
MIN_RISK_PERCENT = 0.1
MAX_ENTRY_DRIFT_PERCENT = 0.8   # Live price اور candle close میں زیادہ فرق نہ ہو

# --- Global ---
MAX_SYMBOLS = 500
MAX_SIGNALS_PER_SCAN = 10
SCAN_INTERVAL_MINUTES = 30
REQUEST_DELAY = 0.15

# --- MEXC API ---
MEXC_BASE_URL = "https://contract.mexc.com"

# --- NTFY (GitHub Secret سے - پرائیویٹ) ---
NTFY_TOPIC = os.environ.get("NTFY_TOPIC", "").strip()
if not NTFY_TOPIC:
    print("=" * 65)
    print("ERROR: NTFY_TOPIC environment variable is not set!")
    print("Please set it in GitHub Secrets: Settings -> Secrets -> NTFY_TOPIC")
    print("=" * 65)
    sys.exit(1)
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

STATE_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "scanner_state.json")
COOLDOWN_HOURS = 4


# ============================================================
# Symbols (Top 500 from MEXC)
# ============================================================

def get_top_500_symbols():
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
        usdt_tickers = [t for t in tickers if t['symbol'].endswith('_USDT')]
        usdt_tickers.sort(key=lambda x: float(x.get('amount24', 0)), reverse=True)
        top = [t['symbol'].replace('_USDT', 'USDT') for t in usdt_tickers[:MAX_SYMBOLS]]
        print(f"Successfully fetched {len(top)} symbols.")
        return top
    except Exception as e:
        print(f"Error: {e}")
        return get_default_symbols()


def get_default_symbols():
    return ["BTCUSDT", "ETHUSDT", "SOLUSDT", "XRPUSDT", "DOGEUSDT"]


# ============================================================
# Live Prices (Bulk)
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
# Data Fetching
# ============================================================

def get_data(symbol, interval, limit=200):
    imap = {"1d": "Day1", "4h": "Hour4", "1h": "Hour1",
            "30m": "Min30", "15m": "Min15"}
    mexc_interval = imap.get(interval, "Min30")
    mexc_symbol = symbol.replace('USDT', '_USDT')

    url = f"{MEXC_BASE_URL}/api/v1/contract/kline/{mexc_symbol}"
    params = {"interval": mexc_interval}

    data = None
    for attempt in range(3):
        try:
            r = requests.get(url, params=params, timeout=20)
            if r.status_code != 200:
                time.sleep(2)
                continue
            data = r.json()
            break
        except Exception:
            time.sleep(2)

    if data is None:
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

    # ✅ آخری نامکمل کینڈل نکال دیں (Look-ahead Bias سے بچنے کے لیے)
    if len(df) > 1:
        df = df.iloc[:-1].copy()

    if len(df) > limit:
        df = df.iloc[-limit:].copy()

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
# Rejection Candle
# ============================================================

def is_bullish_rejection(df, i):
    if i < 1:
        return False
    curr = df.iloc[i]
    body = abs(curr["close"] - curr["open"])
    total_range = curr["high"] - curr["low"]
    if total_range == 0:
        return False
    lower_wick = min(curr["close"], curr["open"]) - curr["low"]
    upper_wick = curr["high"] - max(curr["close"], curr["open"])
    return (lower_wick > body * REJECTION_WICK_RATIO
            and lower_wick > upper_wick
            and body < total_range * REJECTION_BODY_MAX)


def is_bearish_rejection(df, i):
    if i < 1:
        return False
    curr = df.iloc[i]
    body = abs(curr["close"] - curr["open"])
    total_range = curr["high"] - curr["low"]
    if total_range == 0:
        return False
    lower_wick = min(curr["close"], curr["open"]) - curr["low"]
    upper_wick = curr["high"] - max(curr["close"], curr["open"])
    return (upper_wick > body * REJECTION_WICK_RATIO
            and upper_wick > lower_wick
            and body < total_range * REJECTION_BODY_MAX)


# ============================================================
# Climax
# ============================================================

def is_buy_climax(df, i, lookback=VOLUME_LOOKBACK):
    if i < lookback:
        return False
    curr = df.iloc[i]
    avg_vol = df["volume"].iloc[i-lookback:i].mean()
    avg_body = df["body"].iloc[i-lookback:i].mean()
    if pd.isna(avg_vol) or pd.isna(avg_body) or avg_vol == 0 or avg_body == 0:
        return False
    return (curr["close"] > curr["open"]
            and curr["volume"] > avg_vol * CLIMAX_VOLUME_MULT
            and curr["body"] >= avg_body * CLIMAX_BODY_MULT)


def is_sell_climax(df, i, lookback=VOLUME_LOOKBACK):
    if i < lookback:
        return False
    curr = df.iloc[i]
    avg_vol = df["volume"].iloc[i-lookback:i].mean()
    avg_body = df["body"].iloc[i-lookback:i].mean()
    if pd.isna(avg_vol) or pd.isna(avg_body) or avg_vol == 0 or avg_body == 0:
        return False
    return (curr["close"] < curr["open"]
            and curr["volume"] > avg_vol * CLIMAX_VOLUME_MULT
            and curr["body"] >= avg_body * CLIMAX_BODY_MULT)


# ============================================================
# Pivot & Divergence
# ============================================================

def find_pivot_lows(df, end_idx, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    lows = df["low"].values
    start = max(0, end_idx - 60)
    result = []
    for k in range(start + left, end_idx - right + 1):
        if k < len(lows) - right:
            window = lows[k-left:k+right+1]
            if len(window) == left + right + 1 and lows[k] == window.min():
                result.append(k)
    return result


def find_pivot_highs(df, end_idx, left=PIVOT_LEFT, right=PIVOT_RIGHT):
    highs = df["high"].values
    start = max(0, end_idx - 60)
    result = []
    for k in range(start + left, end_idx - right + 1):
        if k < len(highs) - right:
            window = highs[k-left:k+right+1]
            if len(window) == left + right + 1 and highs[k] == window.max():
                result.append(k)
    return result


def check_bullish_rsi_divergence(df, end_idx):
    pivots = find_pivot_lows(df, end_idx)
    if len(pivots) < 2:
        return False
    last, prev = pivots[-1], pivots[-2]
    return (df["low"].iloc[last] < df["low"].iloc[prev]
            and df["rsi"].iloc[last] > df["rsi"].iloc[prev])


def check_bearish_rsi_divergence(df, end_idx):
    pivots = find_pivot_highs(df, end_idx)
    if len(pivots) < 2:
        return False
    last, prev = pivots[-1], pivots[-2]
    return (df["high"].iloc[last] > df["high"].iloc[prev]
            and df["rsi"].iloc[last] < df["rsi"].iloc[prev])


# ============================================================
# Entry Conditions
# ============================================================

def check_entry_conditions(df, i):
    min_needed = max(RSI_PERIOD, VOLUME_LOOKBACK, PIVOT_LEFT + PIVOT_RIGHT) + 5
    if i < min_needed or i >= len(df):
        return False, False

    last_rsi = df["rsi"].iloc[i]
    if pd.isna(last_rsi):
        return False, False

    # LONG
    if is_bullish_rejection(df, i):
        bull_div = check_bullish_rsi_divergence(df, i)
        oversold = last_rsi < RSI_OVERSOLD
        sell_climax = is_sell_climax(df, i)
        if bull_div or oversold or sell_climax:
            return True, False

    # SHORT
    if is_bearish_rejection(df, i):
        bear_div = check_bearish_rsi_divergence(df, i)
        overbought = last_rsi > RSI_OVERBOUGHT
        buy_climax = is_buy_climax(df, i)
        if bear_div or overbought or buy_climax:
            return False, True

    return False, False


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
# Signal Scan (Live)
# ============================================================

def scan_symbol(symbol, live_price, state):
    """ایک کوئن پر مکمل اسکین"""
    if live_price is None or live_price <= 0:
        return None

    now_utc = pd.Timestamp.now(tz='UTC').tz_localize(None)
    cooldown = state.get("cooldown", {})
    if symbol in cooldown:
        last = pd.Timestamp(cooldown[symbol])
        if (now_utc - last).total_seconds() < COOLDOWN_HOURS * 3600:
            return None

    # 1D, 4H لوڈ کریں
    daily = get_data(symbol, "1d", limit=10)
    h4 = get_data(symbol, "4h", limit=30)

    if daily.empty or len(daily) < 1:
        return None
    if h4.empty or len(h4) < H4_LOOKBACK:
        return None

    # 1D Sweep: کل والی کینڈل
    prev_day = daily.iloc[-1]
    prev_high = prev_day["high"]
    prev_low = prev_day["low"]

    # 4H Sweep: پچھلی 5 کینڈلز
    h4_recent = h4.iloc[-H4_LOOKBACK:]
    h4_high = h4_recent["high"].max()
    h4_low = h4_recent["low"].min()

    # --- Sweep Detection (Live Price) ---
    daily_sweep = None
    if live_price > prev_high * (1 + BUFFER):
        daily_sweep = "HIGH"
    elif live_price < prev_low * (1 - BUFFER):
        daily_sweep = "LOW"

    h4_sweep = None
    if live_price > h4_high * (1 + BUFFER):
        h4_sweep = "HIGH"
    elif live_price < h4_low * (1 - BUFFER):
        h4_sweep = "LOW"

    if not daily_sweep and not h4_sweep:
        return None

    # سمت
    if daily_sweep == "HIGH" or h4_sweep == "HIGH":
        side = "SHORT"
        sweep_extreme = max(prev_high, h4_high)
    else:
        side = "LONG"
        sweep_extreme = min(prev_low, h4_low)

    # 15m اور 30m پر انٹری کنفرمیشن
    confirm_15 = False
    confirm_30 = False

    df15 = get_data(symbol, "15m", limit=100)
    if not df15.empty:
        df15 = add_rsi(df15)
        for i in range(len(df15) - RECENT_CANDLES, len(df15)):
            if i < 0:
                continue
            long_15, short_15 = check_entry_conditions(df15, i)
            if side == "LONG" and long_15:
                confirm_15 = True
            if side == "SHORT" and short_15:
                confirm_15 = True

    df30 = get_data(symbol, "30m", limit=100)
    if not df30.empty:
        df30 = add_rsi(df30)
        for i in range(len(df30) - RECENT_CANDLES, len(df30)):
            if i < 0:
                continue
            long_30, short_30 = check_entry_conditions(df30, i)
            if side == "LONG" and long_30:
                confirm_30 = True
            if side == "SHORT" and short_30:
                confirm_30 = True

    # کسی ایک TF پر کنفرمیشن کافی
    if side == "LONG" and not (confirm_15 or confirm_30):
        return None
    if side == "SHORT" and not (confirm_15 or confirm_30):
        return None

    # --- Entry = Live Price ---
    entry = live_price

    # --- SL = Sweep Extreme ---
    sl = sweep_extreme * ((1 - BUFFER) if side == "LONG" else (1 + BUFFER))
    risk = (entry - sl) if side == "LONG" else (sl - entry)

    if risk <= 0:
        return None

    risk_pct = (risk / entry) * 100
    if risk_pct < MIN_RISK_PERCENT:
        return None
    if risk_pct > 5.0:  # SL بہت دور نہ ہو
        return None

    # --- 5 TPs ---
    direction = 1 if side == "LONG" else -1
    tps = {rr: entry + direction * risk * rr for rr in [1, 2, 3, 4, 5]}

    # Cooldown اپ ڈیٹ
    cooldown[symbol] = now_utc.isoformat()
    state["cooldown"] = cooldown

    return {
        "symbol": symbol,
        "side": side,
        "entry": entry,
        "sl": sl,
        "tps": tps,
        "risk_pct": round(risk_pct, 3),
        "sweep_source": f"1D+4H" if (daily_sweep and h4_sweep) else ("1D" if daily_sweep else "4H")
    }


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
            headers={
                "Title": title,
                "Priority": "high",
                "Tags": "rotating_light",
            },
            timeout=10,
        )
        resp.raise_for_status()
        print(f"  -> NTFY Sent: {symbol} {side} | Entry: {entry:.6f} | Risk: {signal['risk_pct']}%")
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
            print(f"\nMax signals ({MAX_SIGNALS_PER_SCAN}) reached. Stopping.")
            break

        print(f"[{idx+1}/{len(symbols)}] {symbol}", end="\r")

        live_price = live_prices.get(symbol)
        if live_price is None:
            continue

        try:
            signal = scan_symbol(symbol, live_price, state)
            if signal:
                if send_ntfy(signal):
                    new_count += 1
        except Exception as e:
            continue

    print(" " * 80, end="\r")
    return new_count


# ============================================================
# Main
# ============================================================

def main():
    print("=" * 70)
    print("LIVE SIGNAL SCANNER - Rejection + Div/OS/OB/Climax + Sweep")
    print(f"Symbols: Top {MAX_SYMBOLS} | Entry TFs: {ENTRY_TIMEFRAMES}")
    print(f"RSI: {RSI_PERIOD} | OB: {RSI_OVERBOUGHT} | OS: {RSI_OVERSOLD}")
    print(f"Rejection Wick: {REJECTION_WICK_RATIO}x | Body Max: {REJECTION_BODY_MAX*100}%")
    print(f"Max Signals/Scan: {MAX_SIGNALS_PER_SCAN}")
    print(f"NTFY Topic: [HIDDEN - from GitHub Secret]")
    print("=" * 70)

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
            next_scan += pd.Timedelta(minutes=SCAN_INTERVAL_MINUTES)
            sleep_seconds = (next_scan - now).total_seconds()

        print(f"Next scan at {next_scan.strftime('%H:%M')} UTC (sleeping {sleep_seconds/60:.1f} min)")
        time.sleep(sleep_seconds)


if __name__ == "__main__":
    main()
