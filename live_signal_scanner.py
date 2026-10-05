import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز
# ============================================================
SYMBOL = "BTCUSDT"
TIMEFRAME = "1h"
TICK_SIZE = 10.0

BUY_THRESHOLD = 25
SELL_THRESHOLD = 75
MIN_SIGNALS = 4

NFTY_URL = os.environ.get("NFTY_URL")

if not NFTY_URL:
    print("❌ NFTY_URL set نہیں ہے")
    exit(1)

# ============================================================
# 🌐 Binance Endpoints
# ============================================================
BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
    "https://api2.binance.com",
    "https://api3.binance.com",
]

def get_working_endpoint():
    for url in BINANCE_ENDPOINTS:
        try:
            r = requests.get(f"{url}/api/v3/ping", timeout=10)
            if r.status_code == 200:
                return url
        except:
            continue
    return None

# ============================================================
# 📥 ڈیٹا لوڈ (صرف 24 گھنٹے - بہت تیز)
# ============================================================
def load_history(base_url):
    print("📥 پچھلے 24 گھنٹے کا ڈیٹا...")
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (24 * 60 * 60 * 1000)
    
    all_trades = []
    current = start_time
    
    while current < end_time:
        url = f"{base_url}/api/v3/aggTrades"
        params = {
            "symbol": SYMBOL,
            "startTime": current,
            "endTime": min(current + 3600000, end_time),
            "limit": 1000
        }
        try:
            r = requests.get(url, params=params, timeout=20)
            data = r.json()
            if not data or isinstance(data, dict):
                break
            all_trades.extend(data)
            current = data[-1]['T'] + 1
        except Exception as e:
            break
    
    print(f"✅ {len(all_trades):,} ٹریڈز")
    return all_trades

# ============================================================
# ⚙️ پراسیسنگ
# ============================================================
def process_trades(trades):
    config = FootprintEngineConfig(
        tick_size=TICK_SIZE,
        aggregation_type=AggregationType.TIME,
        aggregation_value=TIMEFRAME,
        value_area_percentage=0.7,
    )
    engine = FootprintEngine(config=config)
    
    for t in trades:
        try:
            engine.process_tick(
                timestamp=datetime.fromtimestamp(t['T'] / 1000),
                price=float(t['p']),
                volume=max(1, int(float(t['q']) * 100000)),
                is_bid_trade=not t['m']
            )
        except:
            continue
    
    bars = engine.get_all_completed_bars()
    data = []
    for bar in bars:
        data.append({
            'start_time': bar.start_time,
            'open': bar.open_price,
            'high': bar.high_price,
            'low': bar.low_price,
            'close': bar.close_price,
            'poc': bar.poc_price,
            'delta': bar.bar_delta,
            'volume': bar.total_bar_volume,
            'bid_volume': bar.total_bar_bid_volume,
            'ask_volume': bar.total_bar_ask_volume,
        })
    
    return pd.DataFrame(data)

# ============================================================
# 🎯 تجزیہ
# ============================================================
def analyze_contrarian(df):
    if df is None or len(df) < 4:
        return None
    
    df['poc_position'] = (df['poc'] - df['low']) / (df['high'] - df['low'])
    df['ratio'] = df['ask_volume'] / df['bid_volume']
    
    df['bull_of'] = (df['delta'] > 0) & (df['poc_position'] > 0.6) & (df['ratio'] > 1.0)
    df['bear_of'] = (df['delta'] < 0) & (df['poc_position'] < 0.4) & (df['ratio'] < 1.0)
    
    df['window'] = df['start_time'].dt.floor('12h')
    df['window_end'] = df['window'] + pd.Timedelta(hours=12)
    
    now = pd.Timestamp.now()
    completed = df[df['window_end'] <= now].copy()
    
    if len(completed) < 4:
        return None
    
    windows = completed.groupby('window').agg(
        bull_count=('bull_of', 'sum'),
        bear_count=('bear_of', 'sum'),
        close_price=('close', 'last'),
    ).reset_index()
    
    if len(windows) < 1:
        return None
    
    latest = windows.iloc[-1]
    total = latest['bull_count'] + latest['bear_count']
    
    if total < MIN_SIGNALS:
        return None
    
    bull_pct = (latest['bull_count'] / total) * 100
    
    signal = None
    if bull_pct >= SELL_THRESHOLD:
        signal = 'SELL'
    elif bull_pct <= BUY_THRESHOLD:
        signal = 'BUY'
    
    return {
        'window': latest['window'],
        'bull_count': int(latest['bull_count']),
        'bear_count': int(latest['bear_count']),
        'total': int(total),
        'bull_pct': bull_pct,
        'signal': signal,
        'price': latest['close_price'],
    }

# ============================================================
# 📲 Nfty
# ============================================================
def send_nfty(result):
    signal = result['signal']
    
    if signal == 'BUY':
        title = "🟢 BUY — BTC Contrarian"
        tags = "chart_with_upwards_trend"
    else:
        title = "🔴 SELL — BTC Contrarian"
        tags = "chart_with_downwards_trend"
    
    message = (
        f"Signal: {signal}\n"
        f"Price: ${result['price']:,.2f}\n"
        f"Bull%: {result['bull_pct']:.1f}%\n"
        f"Signals: {result['bull_count']}B/{result['bear_count']}S\n"
        f"Window: {result['window'].strftime('%m-%d %H:%M')}"
    )
    
    try:
        r = requests.post(
            NFTY_URL,
            data=message.encode('utf-8'),
            headers={"Title": title, "Priority": "high", "Tags": tags},
            timeout=15
        )
        if r.status_code == 200:
            print(f"✅ Nfty بھیجا: {title}")
    except Exception as e:
        print(f"❌ Nfty: {e}")

# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    print("=" * 50)
    print(f"🚀 {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 50)
    
    base_url = get_working_endpoint()
    if not base_url:
        print("❌ کوئی endpoint کام نہیں کر رہا")
        return
    
    trades = load_history(base_url)
    if not trades:
        return
    
    df = process_trades(trades)
    print(f"📊 {len(df)} کینڈلز")
    
    result = analyze_contrarian(df)
    if result is None:
        print("⏳ کوئی سگنل نہیں")
        return
    
    print(f"📈 Bull%: {result['bull_pct']:.1f}%")
    if result['signal']:
        print(f"🎯 سگنل: {result['signal']}")
        send_nfty(result)

if __name__ == "__main__":
    main()
