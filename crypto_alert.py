import requests
import time
import os
import json

NTFY_TOPIC = os.environ["NTFY_TOPIC"]
TOP_N = 500
BUFFER = 0.0010
STATE_FILE = "sweep_state.json"

def load_state():
    if os.path.exists(STATE_FILE):
        with open(STATE_FILE, "r") as f:
            return json.load(f)
    return {}

def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f)

def get_top_symbols():
    all_symbols = []
    for page in range(1, 6):
        url = "https://api.coingecko.com/api/v3/coins/markets"
        params = {"vs_currency": "usd", "order": "market_cap_desc", "per_page": 100, "page": page}
        try:
            r = requests.get(url, params=params, timeout=10)
            if r.status_code != 200:
                break
            data = r.json()
            if not isinstance(data, list):
                break
            all_symbols.extend([c["symbol"].upper() for c in data])
        except Exception as e:
            print(f"CoinGecko API Error: {e}")
            break
        time.sleep(0.5)
    return all_symbols[:TOP_N]

def get_binance_usdt_pairs():
    url = "https://data-api.binance.vision/api/v3/exchangeInfo"
    try:
        r = requests.get(url, timeout=10)
        print("Status code:", r.status_code)
        print("Response (first 500 chars):", r.text[:500])
        
        if r.status_code != 200:
            return set()
            
        data = r.json()
        if not isinstance(data, dict) or "symbols" not in data:
            return set()
            
        return {s["baseAsset"] for s in data["symbols"] if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"}
    except Exception as e:
        print(f"Binance API Error: {e}")
        return set()

def get_klines(symbol, interval, limit=3):
    url = "https://data-api.binance.vision/api/v3/klines"
    params = {"symbol": f"{symbol}USDT", "interval": interval, "limit": limit}
    try:
        r = requests.get(url, params=params, timeout=10)
        if r.status_code != 200:
            return None
        return r.json()
    except Exception:
        return None

def check_sweep(candles):
    prev = candles[-2]
    curr = candles[-1]
    prev_high, prev_low = float(prev[2]), float(prev[3])
    curr_high, curr_low, curr_close = float(curr[2]), float(curr[3]), float(curr[4])
    curr_open_time = curr[0]

    if curr_high > prev_high and curr_close < prev_high * (1 - BUFFER):
        return "high", curr_open_time
    if curr_low < prev_low and curr_close > prev_low * (1 + BUFFER):
        return "low", curr_open_time
    return None, curr_open_time

def main():
    state = load_state()
    symbols = get_top_symbols()
    binance_pairs = get_binance_usdt_pairs()
    
    if not binance_pairs:
        print("بائننس سے پیئرز فیچ نہیں ہو سکے، اگली کوشش میں دیکھیں گے۔")
        return
        
    valid_symbols = [s for s in symbols if s in binance_pairs]

    new_daily_alerts = []
    new_h4_alerts = []

    for sym in valid_symbols:
        for interval, label, alert_list in [("1d", "daily", new_daily_alerts), ("4h", "h4", new_h4_alerts)]:
            candles = get_klines(sym, interval, 3)
            if not candles or len(candles) < 3:
                continue

            direction, open_time = check_sweep(candles)
            key = f"{sym}_{label}"

            if direction:
                if state.get(key) != open_time:
                    alert_list.append(f"{sym} ({direction.upper()})")
                    state[key] = open_time
            time.sleep(0.1)

    if new_daily_alerts or new_h4_alerts:
        message = "📊 نئے Liquidity Sweep الرٹس\n\n"
        if new_daily_alerts:
            message += "🕐 Daily Timeframe:\n" + "\n".join(new_daily_alerts) + "\n\n"
        if new_h4_alerts:
            message += "⏱ 4H Timeframe:\n" + "\n".join(new_h4_alerts)
        try:
            requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=message.encode("utf-8"), timeout=10)
        except Exception as e:
            print(f"Ntfy Error: {e}")
        print(message)
    else:
        print("کوئی نیا سویپ نہیں ملا اس بار")

    save_state(state)

if __name__ == "__main__":
    main()
    
