import requests
import time
import os
import json

NTFY_TOPIC = os.environ["NTFY_TOPIC"]
TOP_N = 500
BUFFER = 0.0015
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
        data = requests.get(url, params=params).json()
        if not isinstance(data, list):
            break
        all_symbols.extend([c["symbol"].upper() for c in data])
        time.sleep(0.5)
    return all_symbols[:TOP_N]

def get_binance_usdt_pairs():
    url = "https://api.binance.com/api/v3/exchangeInfo"
    data = requests.get(url).json()
    return {s["baseAsset"] for s in data["symbols"] if s["quoteAsset"] == "USDT" and s["status"] == "TRADING"}

def get_klines(symbol, interval, limit=3):
    url = "https://api.binance.com/api/v3/klines"
    params = {"symbol": f"{symbol}USDT", "interval": interval, "limit": limit}
    r = requests.get(url, params=params)
    if r.status_code != 200:
        return None
    return r.json()

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
        requests.post(f"https://ntfy.sh/{NTFY_TOPIC}", data=message.encode("utf-8"))
        print(message)
    else:
        print("کوئی نیا سویپ نہیں ملا اس بار")

    save_state(state)

if __name__ == "__main__":
    main()
