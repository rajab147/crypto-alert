import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز (1h — وہی جو 91.7% WR دیا)
# ============================================================
BASE_URL = "https://data-api.binance.vision"
TIMEFRAME = "1h"
HISTORY_HOURS = 14          # 14 گھنٹے کا ڈیٹا
WINDOW_SIZE = "12h"

# POC
POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.30

# Buy/Sell (Contrarian)
BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

# Risk
TARGET_PCT = 0.7
STOP_PCT = 0.6
MAX_HOLD = 12

# کوئنز
MAX_SYMBOLS = 200
THREADS_KLINES = 25
THREADS_AGG = 8
MAX_CHUNKS = 15             # 14 گھنٹے کے لیے

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    exit(1)


# ============================================================
# 🔧 Safe GET
# ============================================================
def safe_get(url, params=None, timeout=10):
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            return None
        return r.json()
    except:
        return None


# ============================================================
# 📋 200 کوئنز
# ============================================================
def get_all_markets():
    print("Binance مارکیٹس...")
    try:
        data = safe_get(f"{BASE_URL}/api/v3/exchangeInfo", timeout=30)
        if not data or 'symbols' not in data:
            return []
        
        skip = ["USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
                "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY", "USDTBIDR"]
        
        symbols = []
        for s in data['symbols']:
            if s.get('quoteAsset') != 'USDT': continue
            if s.get('status') != 'TRADING': continue
            sym = s.get('symbol', '')
            if sym in skip: continue
            if 'UP' in sym or 'DOWN' in sym or 'BULL' in sym or 'BEAR' in sym: continue
            symbols.append(sym)
        
        print(f"کل USDT مارکیٹس: {len(symbols)}")
        
        # والیوم کے لحاظ سے
        ticker = safe_get(f"{BASE_URL}/api/v3/ticker/24hr", timeout=30)
        if not isinstance(ticker, list):
            return symbols[:MAX_SYMBOLS]
        
        symbol_set = set(symbols)
        volumes = []
        for t in ticker:
            sym = t.get('symbol')
            if sym not in symbol_set: continue
            try:
                vol = float(t.get('quoteVolume', 0))
                volumes.append((sym, vol))
            except: continue
        
        volumes.sort(key=lambda x: x[1], reverse=True)
        result = [s for s, v in volumes[:MAX_SYMBOLS]]
        print(f"{len(result)} کوئنز منتخب")
        return result
    except Exception as e:
        print(f"Error: {e}")
        return []


# ============================================================
# ⚡ مرحلہ 1: Klines سے تیز فلٹر
# ============================================================
def quick_scan(symbol):
    data = safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": TIMEFRAME, "limit": HISTORY_HOURS})
    
    if not isinstance(data, list) or len(data) < 6:
        return None
    
    # آخری 6 کینڈلز چیک کریں
    for k in data[-6:]:
        o = float(k[1]); c = float(k[4])
        if abs(c - o) / o > 0.005:  # 0.5% سے زیادہ حرکت
            return {'symbol': symbol}
    return None


# ============================================================
# 📥 aggTrades — 14 گھنٹے
# ============================================================
def load_aggtrades(symbol):
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (HISTORY_HOURS * 60 * 60 * 1000)
    
    all_trades = []
    current = start_time
    chunks = 0
    empty = 0
    
    while current < end_time and chunks < MAX_CHUNKS:
        data = safe_get(f"{BASE_URL}/api/v3/aggTrades",
                       params={"symbol": symbol, "startTime": current,
                               "endTime": end_time, "limit": 1000})
        
        if not isinstance(data, list): break
        if len(data) == 0:
            empty += 1
            if empty >= 2: break
            current += 60000
            continue
        empty = 0
        all_trades.extend(data)
        if len(data) < 1000: break
        current = data[-1]['T'] + 1
        chunks += 1
    
    return all_trades


# ============================================================
# 🎯 1h پر Order Flow + Contrarian
# ============================================================
def analyze_symbol(symbol):
    trades = load_aggtrades(symbol)
    if not trades or len(trades) < 500:
        return None
    
    config = FootprintEngineConfig(
        tick_size=0.00001,
        aggregation_type=AggregationType.TIME,
        aggregation_value="1h",
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
    if len(bars) < 6:
        return None
    
    # DataFrame
    data = []
    for bar in bars:
        data.append({
            'start_time': bar.start_time,
            'high': bar.high_price,
            'low': bar.low_price,
            'close': bar.close_price,
            'poc': bar.poc_price,
            'delta': bar.bar_delta,
            'bid_volume': bar.total_bar_bid_volume,
            'ask_volume': bar.total_bar_ask_volume,
        })
    
    df = pd.DataFrame(data)
    df['poc_position'] = (df['poc'] - df['low']) / (df['high'] - df['low']).replace(0, 0.0001)
    df['ratio'] = df['ask_volume'] / df['bid_volume'].replace(0, 1)
    
    # Order Flow سگنلز
    df['of_bull'] = (df['delta'] > 0) & (df['poc_position'] > POC_BULL_MIN) & (df['ratio'] > 1.0)
    df['of_bear'] = (df['delta'] < 0) & (df['poc_position'] < POC_BEAR_MAX) & (df['ratio'] < 1.0)
    
    # 12h ونڈو
    df['window'] = df['start_time'].dt.floor(WINDOW_SIZE)
    
    windows = df.groupby('window').agg(
        bull_count=('of_bull', 'sum'),
        bear_count=('of_bear', 'sum'),
        close_price=('close', 'last'),
        last_time=('start_time', 'max'),
    ).reset_index()
    
    if len(windows) < 1:
        return None
    
    # آخری مکمل ونڈو (جو ابھی ختم ہوئی)
    latest = windows.iloc[-1]
    total = latest['bull_count'] + latest['bear_count']
    
    if total < MIN_SIGNALS:
        return None
    
    bull_pct = (latest['bull_count'] / total) * 100
    
    # Contrarian
    signal = None
    if bull_pct <= BUY_THRESHOLD:
        signal = 'BUY'
    elif bull_pct >= SELL_THRESHOLD:
        signal = 'SELL'
    
    if signal is None:
        return None
    
    return {
        'symbol': symbol,
        'signal': signal,
        'price': latest['close_price'],
        'bull_count': int(latest['bull_count']),
        'bear_count': int(latest['bear_count']),
        'bull_pct': bull_pct,
        'window_end': latest['last_time'],
    }


# ============================================================
# 📲 Nfty
# ============================================================
def send_nfty(signals):
    if not signals:
        return
    
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    lines = [f"{len(signals)} Signals (1h Contrarian)\n"]
    
    if buys:
        lines.append(f"BUY ({len(buys)}):")
        for s in buys:
            sym = s['symbol'].replace("USDT", "")
            entry = s['price']
            tp = entry * (1 + TARGET_PCT/100)
            sl = entry * (1 - STOP_PCT/100)
            lines.append("---")
            lines.append(f"{sym} | {entry:.6f}")
            lines.append(f"TP {tp:.6f} | SL {sl:.6f}")
            lines.append(f"Bull% {s['bull_pct']:.0f} | {s['bull_count']}B/{s['bear_count']}S")
    
    if sells:
        lines.append(f"\nSELL ({len(sells)}):")
        for s in sells:
            sym = s['symbol'].replace("USDT", "")
            entry = s['price']
            tp = entry * (1 - TARGET_PCT/100)
            sl = entry * (1 + STOP_PCT/100)
            lines.append("---")
            lines.append(f"{sym} | {entry:.6f}")
            lines.append(f"TP {tp:.6f} | SL {sl:.6f}")
            lines.append(f"Bull% {s['bull_pct']:.0f} | {s['bull_count']}B/{s['bear_count']}S")
    
    message = "\n".join(lines)
    
    try:
        r = requests.post(NFTY_URL, data=message.encode('utf-8'),
            headers={"Title": f"{len(signals)} Signals (1h)",
                     "Priority": "high",
                     "Tags": "rotating_light,moneybag"},
            timeout=15)
        print(f"Nfty: {r.status_code}")
    except Exception as e:
        print(f"Nfty Error: {e}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | 1h | 12h ونڈو | 91.7% WR سسٹم")
    print("=" * 60)
    
    symbols = get_all_markets()
    if not symbols:
        print("مارکیٹس نہیں ملیں")
        return
    
    # مرحلہ 1: Klines
    print(f"\nمرحلہ 1: {len(symbols)} کوئنز Klines فلٹر...")
    candidates = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_KLINES) as ex:
        futures = {ex.submit(quick_scan, s): s for s in symbols}
        for f in as_completed(futures):
            completed += 1
            if completed % 100 == 0:
                print(f"   {completed}/{len(symbols)}")
            try:
                r = f.result(timeout=15)
                if r:
                    candidates.append(r)
            except:
                continue
    
    print(f"{len(candidates)} کوئنز پاس")
    
    if not candidates:
        print("کوئی کوئن نہیں")
        return
    
    # مرحلہ 2: aggTrades
    print(f"\nمرحلہ 2: {len(candidates)} کوئنز aggTrades...")
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(analyze_symbol, c['symbol']): c['symbol'] 
                   for c in candidates}
        for f in as_completed(futures):
            completed += 1
            if completed % 10 == 0:
                print(f"   {completed}/{len(candidates)}")
            try:
                r = f.result(timeout=30)
                if r:
                    signals.append(r)
                    print(f"   {r['symbol']}: {r['signal']} ({r['bull_pct']:.0f}%)")
            except:
                continue
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{len(symbols)} کوئنز | {len(signals)} سگنلز | {elapsed:.0f}s")
    
    if signals:
        send_nfty(signals)
    else:
        print("کوئی سگنل نہیں — اگلی بار")


if __name__ == "__main__":
    main()
