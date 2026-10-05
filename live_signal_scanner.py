import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز (وہی جو بیک ٹیسٹ میں)
# ============================================================
TIMEFRAME = "1h"
HISTORY_HOURS = 12

BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

POC_MIN = 0.65
POC_MAX = 0.30

# Klines فلٹر (پہلا مرحلہ - تیز)
KLINES_THRESHOLD_BUFFER = 15   # th 30/70 سے 15% نرم (تاکہ کوئی چھوٹ نہ جائے)

# والیوم فلٹر
MIN_VOLUME_USDT = 5_000_000
MAX_SYMBOLS = 300
THREADS = 15                    # Klines تیز ہیں، زیادہ threads

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("❌ NFTY_URL set نہیں ہے")
    exit(1)

BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
    "https://api1.binance.com",
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


def get_top_symbols(base_url):
    """والیوم کے لحاظ سے 300 کوئنز"""
    print("📋 USDT کوئنز...")
    try:
        r = requests.get(f"{base_url}/api/v3/ticker/24hr", timeout=30)
        data = r.json()
        if not isinstance(data, list):
            return []
        
        skip = ["USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
                "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY"]
        
        volumes = []
        for t in data:
            sym = t.get('symbol', '')
            if not sym.endswith('USDT') or sym in skip:
                continue
            if 'UP' in sym or 'DOWN' in sym or 'BULL' in sym or 'BEAR' in sym:
                continue
            try:
                vol = float(t.get('quoteVolume', 0))
                if vol >= MIN_VOLUME_USDT:
                    volumes.append((sym, vol))
            except:
                continue
        
        volumes.sort(key=lambda x: x[1], reverse=True)
        result = [s for s, v in volumes[:MAX_SYMBOLS]]
        print(f"✅ {len(result)} کوئنز (≥ ${MIN_VOLUME_USDT/1e6:.0f}M)")
        return result
    except Exception as e:
        print(f"❌ {e}")
        return []


# ============================================================
# مرحلہ 1: Klines سے تیز اسکین (فلٹر)
# ============================================================
def quick_scan_klines(base_url, symbol):
    """Klines سے تیز فلٹر (صرف یہ دیکھیں کہ کوئی ونڈو 30/70 کے قریب ہے یا نہیں)"""
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
            
            # تخمینہ
            if c > o: buy_pct = 0.65
            elif c < o: buy_pct = 0.35
            else: buy_pct = 0.5
            
            delta_est = int((buy_pct - 0.5) * vol * 100000)
            
            # POC کا تخمینہ (Klines سے ممکن نہیں، مگر حد لگائیں)
            # یہ صرف پہلا فلٹر ہے
            rows.append({
                'start_time': datetime.fromtimestamp(k[0] / 1000),
                'open': o, 'high': h, 'low': l, 'close': c,
                'volume': int(vol * 100000),
                'bid_volume': int(vol * 100000 * (1 - buy_pct)),
                'ask_volume': int(vol * 100000 * buy_pct),
                'delta': delta_est,
            })
        
        df = pd.DataFrame(rows)
        df['ratio'] = df['ask_volume'] / df['bid_volume'].replace(0, 1)
        
        # Klines میں POC نہیں، صرف Delta + Ratio دیکھیں (نرم فلٹر)
        df['bull_of'] = (df['delta'] > 0) & (df['ratio'] > 1.0)
        df['bear_of'] = (df['delta'] < 0) & (df['ratio'] < 1.0)
        
        df['window'] = df['start_time'].dt.floor('12h')
        
        windows = df.groupby('window').agg(
            bull_count=('bull_of', 'sum'),
            bear_count=('bear_of', 'sum'),
            close_price=('close', 'last'),
        ).reset_index()
        
        # ہر ونڈو چیک کریں
        for _, w in windows.iterrows():
            total = w['bull_count'] + w['bear_count']
            if total < MIN_SIGNALS:
                continue
            
            bull_pct = (w['bull_count'] / total) * 100
            
            # نرم فلٹر — اگر 30/70 کے 15% قریب بھی ہے تو آگے بھیجیں
            if bull_pct <= (BUY_THRESHOLD + KLINES_THRESHOLD_BUFFER) or \
               bull_pct >= (SELL_THRESHOLD - KLINES_THRESHOLD_BUFFER):
                return {
                    'symbol': symbol,
                    'klines_bull_pct': bull_pct,
                    'klines_signal': 'BUY' if bull_pct <= BUY_THRESHOLD + KLINES_THRESHOLD_BUFFER else 'SELL',
                }
        
        return None
    except:
        return None


# ============================================================
# مرحلہ 2: aggTrades سے اصل POC کی تصدیق
# ============================================================
def verify_with_aggtrades(base_url, symbol):
    """aggTrades سے اصل POC نکال کر تصدیق کریں"""
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (HISTORY_HOURS * 60 * 60 * 1000)
    
    url = f"{base_url}/api/v3/aggTrades"
    params = {
        "symbol": symbol,
        "startTime": start_time,
        "endTime": end_time,
        "limit": 1000
    }
    
    try:
        r = requests.get(url, params=params, timeout=20)
        trades = r.json()
        if not isinstance(trades, list) or len(trades) < 500:
            return None
    except:
        return None
    
    # footprint_analyzer سے اصل POC
    config = FootprintEngineConfig(
        tick_size=0.00001,
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
    if len(bars) < 4:
        return None
    
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
            'bid_volume': bar.total_bar_bid_volume,
            'ask_volume': bar.total_bar_ask_volume,
        })
    
    df = pd.DataFrame(data)
    df['poc_position'] = (df['poc'] - df['low']) / (df['high'] - df['low']).replace(0, 0.0001)
    df['ratio'] = df['ask_volume'] / df['bid_volume'].replace(0, 1)
    
    df['bull_of'] = (df['delta'] > 0) & (df['poc_position'] > POC_MIN) & (df['ratio'] > 1.0)
    df['bear_of'] = (df['delta'] < 0) & (df['poc_position'] < POC_MAX) & (df['ratio'] < 1.0)
    
    df['window'] = df['start_time'].dt.floor('12h')
    
    windows = df.groupby('window').agg(
        bull_count=('bull_of', 'sum'),
        bear_count=('bear_of', 'sum'),
        close_price=('close', 'last'),
    ).reset_index()
    
    for _, w in windows.iterrows():
        total = w['bull_count'] + w['bear_count']
        if total < MIN_SIGNALS:
            continue
        
        bull_pct = (w['bull_count'] / total) * 100
        
        signal = None
        if bull_pct <= BUY_THRESHOLD:
            signal = 'BUY'
        elif bull_pct >= SELL_THRESHOLD:
            signal = 'SELL'
        
        if signal is None:
            continue
        
        return {
            'symbol': symbol,
            'bull_count': int(w['bull_count']),
            'bear_count': int(w['bear_count']),
            'bull_pct': bull_pct,
            'signal': signal,
            'price': w['close_price'],
            'poc_verified': True,
        }
    
    return None


# ============================================================
# 📲 Nfty
# ============================================================
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
    
    try:
        r = requests.post(
            NFTY_URL,
            data="\n".join(lines).encode('utf-8'),
            headers={"Title": f"🎯 {len(signals)} سگنلز", "Priority": "high", "Tags": "rotating_light,moneybag"},
            timeout=15
        )
        print(f"✅ Nfty بھیجا")
    except:
        pass


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 70)
    print(f"🚀 {start.strftime('%H:%M:%S')}")
    print(f"⚙️ POC {POC_MIN}-{POC_MAX} | B≤{BUY_THRESHOLD}% S≥{SELL_THRESHOLD}% | Min{MIN_SIGNALS}")
    print("=" * 70)
    
    base_url = get_working_endpoint()
    if not base_url:
        return
    print(f"✅ {base_url}\n")
    
    # مرحلہ 1: 300 کوئنز حاصل کریں
    symbols = get_top_symbols(base_url)
    if not symbols:
        return
    
    # ============================================================
    # مرحلہ 1: Klines سے تیز فلٹر (300 کوئنز)
    # ============================================================
    print(f"\n🔍 مرحلہ 1: {len(symbols)} کوئنز Klines سے تیز فلٹر...")
    
    candidates = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS) as ex:
        futures = {ex.submit(quick_scan_klines, base_url, s): s for s in symbols}
        for f in as_completed(futures):
            completed += 1
            if completed % 50 == 0:
                print(f"   ⏳ {completed}/{len(symbols)}")
            try:
                r = f.result(timeout=30)
                if r:
                    candidates.append(r)
            except:
                continue
    
    elapsed1 = (datetime.now() - start).total_seconds()
    print(f"✅ {len(candidates)} کوئنز فلٹر پاس | {elapsed1:.0f}s")
    
    if not candidates:
        print("⏳ کوئی کوئن نہیں ملی")
        return
    
    # ============================================================
    # مرحلہ 2: aggTrades سے اصل POC کی تصدیق
    # ============================================================
    print(f"\n🔍 مرحلہ 2: {len(candidates)} کوئنز aggTrades سے تصدیق...")
    
    signals = []
    for c in candidates:
        try:
            r = verify_with_aggtrades(base_url, c['symbol'])
            if r:
                signals.append(r)
                print(f"   🎯 {r['symbol']}: {r['signal']} ({r['bull_pct']:.0f}%)")
        except:
            continue
    
    elapsed2 = (datetime.now() - start).total_seconds()
    print(f"\n{'='*70}")
    print(f"📊 {len(symbols)} کوئنز | {len(signals)} سگنلز | کل {elapsed2:.0f}s")
    print(f"{'='*70}")
    
    if signals:
        send_nfty(signals)


if __name__ == "__main__":
    main()
