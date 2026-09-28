import requests
import pandas as pd
import numpy as np
import time
import sys

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

URL = "https://fapi.binance.com/fapi/v1/klines"

# --- NTFY Configuration ---
NTFY_TOPIC = "ChartMaster786x7k2p9"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

SIGNALED_SWEEPS = set()

def get_top_500_symbols():
    """بائننس سے ٹاپ 500 فیوچرز کوئنز حاصل کرنا (بلحاظ 24 گھنٹے والیوم)"""
    print("Fetching Top 500 USDT Perpetual Symbols from Binance...")
    try:
        # تمام فیوچرز سیمبولز حاصل کریں
        exchange_info = requests.get("https://fapi.binance.com/fapi/v1/exchangeInfo", timeout=20).json()
        valid_symbols = [
            s['symbol'] for s in exchange_info['symbols'] 
            if s['contractType'] == 'PERPETUAL' and s['quoteAsset'] == 'USDT' and s['status'] == 'TRADING'
        ]
        
        # 24 گھنٹے کا والیوم حاصل کریں
        ticker_data = requests.get("https://fapi.binance.com/fapi/v1/ticker/24hr", timeout=20).json()
        filtered_tickers = [t for t in ticker_data if t['symbol'] in valid_symbols]
        
        # والیوم کے لحاظ سے سارٹ کریں
        filtered_tickers.sort(key=lambda x: float(x['quoteVolume']), reverse=True)
        
        top_500 = [t['symbol'] for t in filtered_tickers[:500]]
        print(f"Successfully fetched {len(top_500)} symbols.")
        return top_500
    except Exception as e:
        print(f"Error fetching symbols: {e}. Using default 10 symbols.")
        return ["SOLUSDT", "NEARUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT", "OPUSDT", "DOGEUSDT", "AVAXUSDT", "LINKUSDT", "ADAUSDT"]

def send_ntfy_notification(title, message, tags="chart"):
    """NTFY کے ذریعے موبائل پر نوٹیفکیشن بھیجنا"""
    try:
        headers = {"Title": title, "Tags": tags, "Priority": "high"}
        response = requests.post(NTFY_URL, data=message.encode('utf-8'), headers=headers, timeout=10)
        if response.status_code == 200:
            print(f"   -> NTFY Notification Sent Successfully!")
    except Exception as e:
        print(f"   -> NTFY Error: {e}")

def get_data(symbol, interval, limit=200):
    """ڈیٹا حاصل کرنا (Rate Limit سے بچنے کے لیے 0.15 سیکنڈ کا وقفہ)"""
    end = int(time.time() * 1000)
    params = {"symbol": symbol, "interval": interval, "endTime": end, "limit": limit}
    try:
        r = requests.get(URL, params=params, timeout=15)
        if r.status_code == 429:
            print("Rate limit hit! Sleeping for 10 seconds...")
            time.sleep(10)
            return pd.DataFrame()
        data = r.json()
    except Exception as e:
        return pd.DataFrame()

    time.sleep(0.15) # API Rate limit سے بچنے کے لیے لازمی وقفہ

    if not isinstance(data, list) or len(data) == 0:
        return pd.DataFrame()

    df = pd.DataFrame(data, columns=[
        "time", "open", "high", "low", "close", "volume",
        "close_time", "quote_volume", "trades", "buy_base", "buy_quote", "ignore"
    ])
    
    df["time"] = pd.to_datetime(df["time"], unit="ms", utc=True)
    for c in ["open", "high", "low", "close", "volume"]:
        df[c] = pd.to_numeric(df[c])

    df = df[["time", "open", "high", "low", "close", "volume"]]
    df = df.drop_duplicates("time").sort_values("time").reset_index(drop=True)

    if len(df) > 1:
        df = df.iloc[:-1].copy() # آخری نامکمل کینڈل کو خارج کریں

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

def find_sweeps(df, lookback, hours, tf):
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

    for i in np.where(high_mask)[0]:
        t = time_series.iloc[i]
        sweeps.append({"time": t, "type": "HIGH", "extreme": high[i], "until": t + delta, "tf": tf})

    for i in np.where(low_mask)[0]:
        t = time_series.iloc[i]
        sweeps.append({"time": t, "type": "LOW", "extreme": low[i], "until": t + delta, "tf": tf})

    sweeps.sort(key=lambda x: x["time"])
    return sweeps

def check_live_signal(df, active_sweeps, symbol, tf):
    if len(df) < 2:
        return

    last_candle = df.iloc[-1]
    t = last_candle["time"]
    r = last_candle["rsi"]
    vol_ok = last_candle["vol_ok"]
    close = last_candle["close"]

    if pd.isna(r) or pd.isna(last_candle["vol_avg"]) or not vol_ok:
        return

    for sweep in active_sweeps:
        sweep_key = f"{symbol}_{tf}_{sweep['time']}_{sweep['type']}"
        
        if sweep_key in SIGNALED_SWEEPS:
            continue

        if sweep["type"] == "LOW":
            if r > RSI_OS: continue
            entry = close
            sl = sweep["extreme"] * (1 - BUFFER)
            risk = entry - sl
            if risk <= 0: continue
            
            SIGNALED_SWEEPS.add(sweep_key)
            tp1, tp2, tp3 = entry + risk * 1, entry + risk * 2, entry + risk * 3
            
            title = f"🚀 LONG: {symbol} ({tf})"
            msg = f"Entry: {entry:.4f}\nSL: {sl:.4f}\nTP1: {tp1:.4f}\nTP2: {tp2:.4f}\nTP3: {tp3:.4f}"
            print(f"\n{title}\n{msg}")
            send_ntfy_notification(title, msg, tags="rocket,chart_with_upwards_trend")

        else:  # HIGH sweep
            if r < RSI_OB: continue
            entry = close
            sl = sweep["extreme"] * (1 + BUFFER)
            risk = sl - entry
            if risk <= 0: continue

            SIGNALED_SWEEPS.add(sweep_key)
            tp1, tp2, tp3 = entry - risk * 1, entry - risk * 2, entry - risk * 3
            
            title = f"🔻 SHORT: {symbol} ({tf})"
            msg = f"Entry: {entry:.4f}\nSL: {sl:.4f}\nTP1: {tp1:.4f}\nTP2: {tp2:.4f}\nTP3: {tp3:.4f}"
            print(f"\n{title}\n{msg}")
            send_ntfy_notification(title, msg, tags="arrow_down,chart_with_downwards_trend")

def main():
    print("=" * 65)
    print("LIQUIDITY SWEEP LIVE SCANNER (GitHub Actions Version)")
    print("=" * 65)
    
    send_ntfy_notification("Scanner Started", "GitHub Actions Scanner is now running.", tags="white_check_mark")
    
    # GitHub Actions کے 6 گھنٹے کے ٹائم آؤٹ سے بچنے کے لیے ٹائمر
    start_time = time.time()
    MAX_RUNTIME = 5.5 * 3600  # 5 گھنٹے 30 منٹ

    # ٹاپ 500 کوئنز حاصل کریں
    SYMBOLS = get_top_500_symbols()
    print(f"Scanning {len(SYMBOLS)} symbols...\n")

    while True:
        # چیک کریں کہ کیا ٹائم آؤٹ ہونے والا ہے
        if time.time() - start_time > MAX_RUNTIME:
            print("\nApproaching GitHub Actions time limit. Exiting gracefully...")
            send_ntfy_notification("Scanner Restarting", "Time limit reached. GitHub will restart the scanner shortly.", tags="arrows_counterclockwise")
            sys.exit(0)

        try:
            current_utc = pd.Timestamp.now(tz='UTC')
            print(f"\n[{current_utc.strftime('%Y-%m-%d %H:%M:%S')} UTC] Scanning {len(SYMBOLS)} coins...")

            total_active_sweeps = 0
            signals_found = 0

            for symbol in SYMBOLS:
                daily = get_data(symbol, "1d")
                h4 = get_data(symbol, "4h")
                h1 = get_data(symbol, "1h")
                m30 = get_data(symbol, "30m")

                if daily.empty or h4.empty or h1.empty or m30.empty:
                    continue

                daily_sweeps = find_sweeps(daily, DAILY_LOOKBACK, DAILY_HOURS, "1D")
                h4_sweeps = find_sweeps(h4, H4_LOOKBACK, H4_HOURS, "4H")

                all_sweeps = daily_sweeps + h4_sweeps
                all_sweeps.sort(key=lambda x: x["time"])

                active_sweeps = [s for s in all_sweeps if s["until"] >= current_utc]
                total_active_sweeps += len(active_sweeps)

                if not active_sweeps:
                    continue

                h1 = add_indicators(h1)
                m30 = add_indicators(m30)

                check_live_signal(h1, active_sweeps, symbol, "1H")
                check_live_signal(m30, active_sweeps, symbol, "30M")

            print(f"   Status: Scanned {len(SYMBOLS)} coins. Active Sweeps: {total_active_sweeps}")
            
            # 500 کوئنز کو اسکین کرنے میں وقت لگے گا، اس لیے اگلا اسکین 5 منٹ بعد
            print("   Waiting 5 minutes for next scan...")
            time.sleep(300) 

        except Exception as e:
            print(f"\n⚠️ Error in main loop: {e}")
            time.sleep(60)

if __name__ == "__main__":
    main()
