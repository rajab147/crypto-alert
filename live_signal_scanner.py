import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# ⚙️ حتمی سیٹنگز (بہترین بیک ٹیسٹ سے)
# ============================================================
TIMEFRAME = "1h"
HISTORY_HOURS = 24

# Thresholds
BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

# POC Thresholds (یہ سب سے اہم ہے!)
POC_MIN = 0.65     # Buy Signal کے لیے
POC_MAX = 0.30     # Sell Signal کے لیے

# Ratio Thresholds
RATIO_MIN = 1.0
RATIO_MAX = 1.0

# والیوم فلٹر
MIN_VOLUME_USDT = 5_000_000   # 5 ملین
MAX_SYMBOLS = 300
THREADS = 10

SKIP_SYMBOLS = [
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT", "FDUSDUSDT",
    "DAIUSDT", "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY",
]

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("❌ NFTY_URL set نہیں ہے")
    exit(1)

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
            r = requests.get(f"{url}/api/v3/ping", timeout=5)
            if r.status_code == 200:
                return url
        except:
            continue
    return None


def get_all_symbols(base_url):
    print("📋 USDT کوئنز...")
    try:
        r = requests.get(f"{base_url}/api/v3/exchangeInfo", timeout=30)
        data = r.json()
        if 'symbols' not in data:
            return []
        symbols = []
        for s in data['symbols']:
            if s['quoteAsset'] != 'USDT' or s['status'] != 'TRADING':
                continue
            if s['symbol'] in SKIP_SYMBOLS:
                continue
            if 'UP' in s['baseAsset'] or 'DOWN' in s['baseAsset']:
                continue
            if 'BULL' in s['baseAsset'] or 'BEAR' in s['baseAsset']:
                continue
            symbols.append(s['symbol'])
        return symbols
    except:
        return []


def get_top_symbols(base_url, symbols):
    try:
        r = requests.get(f"{base_url}/api/v3/ticker/24hr", timeout=30)
        data = r.json()
        if not isinstance(data, list):
            return symbols[:MAX_SYMBOLS]
        
        symbol_set = set(symbols)
        volumes = []
        for t in data:
            sym = t.get('symbol')
            if sym not in symbol_set:
                continue
            try:
                vol = float(t.get('quoteVolume', 0))
                if vol >= MIN_VOLUME_USDT:
                    volumes.append((sym, vol))
            except:
                continue
        
        volumes.sort(key=lambda x: x[1], reverse=True)
        return [s for s, v in volumes[:MAX_SYMBOLS]]
    except:
        return symbols[:MAX_SYMBOLS]


def load_data(base_url, symbol):
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": HISTORY_HOURS}
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not isinstance(data, list) or len(data) < 4:
            return None
        rows = []
        for k in data:
            o = float(k[1]); c = float(k[4]); h = float(k[2]); l = float(k[3])
            vol = float(k[5])
            buy_pct = 0.6 if c > o else 0.4 if c < o else 0.5
            delta_est = int((buy_pct - 0.5) * vol * 100000)
            rows.append({
                'start_time': datetime.fromtimestamp(k[0] / 1000),
                'open': o, 'high': h, 'low': l, 'close': c,
                'volume': int(vol * 100000),
                'bid_volume': int(vol * 100000 * (1 - buy_pct)),
                'ask_volume': int(vol * 100000 * buy_pct),
                'poc': (h + l) / 2,
                'delta': delta_est,
            })
        return pd.DataFrame(rows)
    except:
        return None


def analyze(df, symbol):
    if df is None or len(df) < 4:
        return None
    
    df = df.copy()
    df['poc_position'] = (df['poc'] - df['low']) / (df['high'] - df['low']).replace(0, 0.0001)
    df['ratio'] = df['ask_volume'] / df['bid_volume'].replace(0, 1)
    
    # POC اور Ratio کے ساتھ سخت شرائط
    df['bull_of'] = (df['delta'] > 0) & (df['poc_position'] > POC_MIN) & (df['ratio'] > RATIO_MIN)
    df['bear_of'] = (df['delta'] < 0) & (df['poc_position'] < POC_MAX) & (df['ratio'] < RATIO_MAX)
    
    df['window'] = df['start_time'].dt.floor('12h')
    
    windows = df.groupby('window').agg(
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
    if bull_pct <= BUY_THRESHOLD:
        signal = 'BUY'
    elif bull_pct >= SELL_THRESHOLD:
        signal = 'SELL'
    
    if signal is None:
        return None
    
    return {
        'symbol': symbol,
        'window': latest['window'],
        'bull_count': int(latest['bull_count']),
        'bear_count': int(latest['bear_count']),
        'total': int(total),
        'bull_pct': bull_pct,
        'signal': signal,
        'price': latest['close_price'],
    }


def scan(base_url, symbol):
    df = load_data(base_url, symbol)
    return analyze(df, symbol)


def send_nfty(signals):
    if not signals:
        return
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    lines = [f"🎯 {len(signals)} سگنلز ({len(buys)}B/{len(sells)}S)\n"]
    
    if buys:
        lines.append(f"🟢 BUY ({len(buys)}):")
        for s in buys[:15]:
            sym = s['symbol'].replace("USDT", "")
            lines.append(f"  {sym} | ${s['price']:,.4f} | {s['bull_pct']:.0f}%")
        if len(buys) > 15:
            lines.append(f"  +{len(buys)-15} مزید")
    
    if sells:
        lines.append(f"\n🔴 SELL ({len(sells)}):")
        for s in sells[:15]:
            sym = s['symbol'].replace("USDT", "")
            lines.append(f"  {sym} | ${s['price']:,.4f} | {s['bull_pct']:.0f}%")
        if len(sells) > 15:
            lines.append(f"  +{len(sells)-15} مزید")
    
    message = "\n".join(lines)
    title = f"🎯 {len(signals)} سگنلز"
    
    try:
        r = requests.post(
            NFTY_URL,
            data=message.encode('utf-8'),
            headers={"Title": title, "Priority": "high", "Tags": "rotating_light,moneybag"},
            timeout=15
        )
        if r.status_code == 200:
            print(f"✅ Nfty: {title}")
    except Exception as e:
        print(f"❌ Nfty: {e}")


def main():
    start = datetime.now()
    print("=" * 70)
    print(f"🚀 {start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⚙️ POC: {POC_MIN}-{POC_MAX} | Buy≤{BUY_THRESHOLD}% Sell≥{SELL_THRESHOLD}%")
    print("=" * 70)
    
    base_url = get_working_endpoint()
    if not base_url:
        return
    print(f"✅ {base_url}\n")
    
    all_sym = get_all_symbols(base_url)
    symbols = get_top_symbols(base_url, all_sym)
    print(f"🔍 {len(symbols)} کوئنز اسکین...\n")
    
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(scan, base_url, s): s for s in symbols}
        for f in as_completed(futures):
            completed += 1
            if completed % 50 == 0:
                print(f"   ⏳ {completed}/{len(symbols)}")
            try:
                r = f.result(timeout=30)
                if r:
                    signals.append(r)
                    print(f"   🎯 {r['symbol']}: {r['signal']} ({r['bull_pct']:.0f}%)")
            except:
                continue
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{'='*70}")
    print(f"📊 {completed} کوئنز | {len(signals)} سگنلز | {elapsed:.0f}s")
    print("=" * 70)
    
    if signals:
        send_nfty(signals)


if __name__ == "__main__":
    main()
