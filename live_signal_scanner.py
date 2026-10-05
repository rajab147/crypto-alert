import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز
# ============================================================
TIMEFRAME = "1h"
HISTORY_HOURS = 12

BUY_THRESHOLD = 25
SELL_THRESHOLD = 75
MIN_SIGNALS = 4

# صرف یہ کوئنز چھوڑ دیں (سٹیبل کوائنز)
SKIP_SYMBOLS = [
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT", "FDUSDUSDT",
    "DAIUSDT", "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY",
]

# کم از کم 24 گھنٹے کا والیوم (USDT) — کم والیوم والے کوئنز چھوڑ دیں
MIN_VOLUME_USDT = 5_000_000  # 5 ملین USDT

# زیادہ سے زیادہ کوئنز (اگر 400 سے زیادہ ہوں تو صرف ٹاپ 400)
MAX_SYMBOLS = 400

# ایک ساتھ کتنے کوئنز اسکین کریں
THREADS = 10

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
            r = requests.get(f"{url}/api/v3/ping", timeout=5)
            if r.status_code == 200:
                return url
        except:
            continue
    return None


# ============================================================
# 📋 تمام USDT کوئنز حاصل کریں
# ============================================================
def get_all_symbols(base_url):
    print("📋 تمام USDT کوئنز حاصل کیے جا رہے ہیں...")
    
    try:
        r = requests.get(f"{base_url}/api/v3/exchangeInfo", timeout=30)
        data = r.json()
        
        if 'symbols' not in data:
            print("❌ exchangeInfo ناکام")
            return []
        
        symbols = []
        for s in data['symbols']:
            # صرف USDT جوڑے
            if s['quoteAsset'] != 'USDT':
                continue
            # صرف ٹریڈنگ والے
            if s['status'] != 'TRADING':
                continue
            # سٹیبل کوائنز چھوڑ دیں
            if s['symbol'] in SKIP_SYMBOLS:
                continue
            # لیوریج ٹوکن چھوڑ دیں
            if 'UP' in s['baseAsset'] or 'DOWN' in s['baseAsset']:
                continue
            if 'BULL' in s['baseAsset'] or 'BEAR' in s['baseAsset']:
                continue
            
            symbols.append(s['symbol'])
        
        print(f"✅ {len(symbols)} USDT کوئنز ملیں")
        return symbols
    
    except Exception as e:
        print(f"❌ {e}")
        return []


# ============================================================
# 📊 24 گھنٹے کا والیوم (فلٹر کے لیے)
# ============================================================
def get_top_volume_symbols(base_url, symbols, top_n=MAX_SYMBOLS):
    print(f"📊 ٹاپ {top_n} کوئنز والیوم کے لحاظ سے...")
    
    try:
        r = requests.get(f"{base_url}/api/v3/ticker/24hr", timeout=30)
        data = r.json()
        
        if not isinstance(data, list):
            return symbols[:top_n]
        
        # صرف ان کوئنز کا ڈیٹا
        symbol_set = set(symbols)
        volumes = []
        
        for ticker in data:
            sym = ticker.get('symbol')
            if sym not in symbol_set:
                continue
            try:
                vol = float(ticker.get('quoteVolume', 0))
                volumes.append((sym, vol))
            except:
                continue
        
        # والیوم کے لحاظ سے ترتیب دیں
        volumes.sort(key=lambda x: x[1], reverse=True)
        
        # صرف وہ جو کم از کم والیوم رکھتے ہوں
        filtered = [s for s, v in volumes if v >= MIN_VOLUME_USDT]
        
        print(f"✅ {len(filtered)} کوئنز (≥ ${MIN_VOLUME_USDT:,} والیوم)")
        
        return filtered[:top_n]
    
    except Exception as e:
        print(f"⚠️ {e}")
        return symbols[:top_n]


# ============================================================
# 📥 ایک کوئن کا ڈیٹا لوڈ
# ============================================================
def load_data(base_url, symbol):
    url = f"{base_url}/api/v3/klines"
    params = {
        "symbol": symbol,
        "interval": TIMEFRAME,
        "limit": HISTORY_HOURS
    }
    
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        
        if not isinstance(data, list) or len(data) < 4:
            return None
        
        rows = []
        for k in data:
            o = float(k[1])
            c = float(k[4])
            h = float(k[2])
            l = float(k[3])
            vol = float(k[5])
            
            if c > o:
                buy_pct = 0.6
            elif c < o:
                buy_pct = 0.4
            else:
                buy_pct = 0.5
            
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


# ============================================================
# 🎯 تجزیہ
# ============================================================
def analyze_symbol(df, symbol):
    if df is None or len(df) < 4:
        return None
    
    df = df.copy()
    df['poc_position'] = (df['poc'] - df['low']) / (df['high'] - df['low']).replace(0, 0.0001)
    df['ratio'] = df['ask_volume'] / df['bid_volume'].replace(0, 1)
    
    df['bull_of'] = (df['delta'] > 0) & (df['poc_position'] > 0.6) & (df['ratio'] > 1.0)
    df['bear_of'] = (df['delta'] < 0) & (df['poc_position'] < 0.4) & (df['ratio'] < 1.0)
    
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
    if bull_pct >= SELL_THRESHOLD:
        signal = 'SELL'
    elif bull_pct <= BUY_THRESHOLD:
        signal = 'BUY'
    
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


# ============================================================
# 🔄 ایک کوئن کو اسکین کریں
# ============================================================
def scan_one(base_url, symbol):
    df = load_data(base_url, symbol)
    return analyze_symbol(df, symbol)


# ============================================================
# 📲 Nfty (متعدد سگنلز ایک پیغام میں)
# ============================================================
def send_nfty_bulk(signals):
    if not signals:
        return
    
    # BUY اور SELL الگ کریں
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    # پیغام بنائیں
    lines = []
    
    if buys:
        lines.append(f"🟢 BUY ({len(buys)}):")
        for s in buys[:20]:  # زیادہ سے زیادہ 20 دکھائیں
            sym = s['symbol'].replace("USDT", "")
            lines.append(f"  {sym} | ${s['price']:,.2f} | Bull%: {s['bull_pct']:.0f}%")
        if len(buys) > 20:
            lines.append(f"  ... +{len(buys) - 20} مزید")
    
    if sells:
        lines.append(f"\n🔴 SELL ({len(sells)}):")
        for s in sells[:20]:
            sym = s['symbol'].replace("USDT", "")
            lines.append(f"  {sym} | ${s['price']:,.2f} | Bull%: {s['bull_pct']:.0f}%")
        if len(sells) > 20:
            lines.append(f"  ... +{len(sells) - 20} مزید")
    
    message = "\n".join(lines)
    title = f"🎯 {len(signals)} سگنلز ({len(buys)}B/{len(sells)}S)"
    
    try:
        r = requests.post(
            NFTY_URL,
            data=message.encode('utf-8'),
            headers={
                "Title": title,
                "Priority": "high",
                "Tags": "rotating_light,moneybag",
            },
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
    start_time = datetime.now()
    print("=" * 70)
    print(f"🚀 {start_time.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 70)
    
    base_url = get_working_endpoint()
    if not base_url:
        print("❌ کوئی endpoint کام نہیں کر رہا")
        return
    
    print(f"✅ Endpoint: {base_url}\n")
    
    # 1. تمام کوئنز
    all_symbols = get_all_symbols(base_url)
    if not all_symbols:
        return
    
    # 2. والیوم کے لحاظ سے فلٹر
    symbols = get_top_volume_symbols(base_url, all_symbols)
    if not symbols:
        print("❌ کوئی کوئن نہیں ملی")
        return
    
    print(f"\n🔍 {len(symbols)} کوئنز اسکین ہو رہی ہیں...\n")
    
    # 3. متعدد threads میں اسکین کریں
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS) as executor:
        futures = {
            executor.submit(scan_one, base_url, sym): sym 
            for sym in symbols
        }
        
        for future in as_completed(futures):
            sym = futures[future]
            completed += 1
            
            if completed % 50 == 0:
                print(f"   ⏳ {completed}/{len(symbols)} مکمل")
            
            try:
                result = future.result(timeout=30)
                if result:
                    signals.append(result)
                    print(f"   🎯 {result['symbol']}: {result['signal']} "
                          f"(Bull% {result['bull_pct']:.0f}%)")
            except:
                continue
    
    elapsed = (datetime.now() - start_time).total_seconds()
    
    print("\n" + "=" * 70)
    print(f"📊 مکمل: {completed} کوئنز | {len(signals)} سگنلز | {elapsed:.0f} سیکنڈ")
    print("=" * 70)
    
    # 4. Nfty پر بھیجیں
    if signals:
        send_nfty_bulk(signals)
    else:
        print("⏳ کوئی سگنل نہیں ملا")


if __name__ == "__main__":
    main()
