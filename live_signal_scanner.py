import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز (POC 0.65 — جو 91.7% WR دیا)
# ============================================================
TIMEFRAME = "1h"
HISTORY_HOURS = 12
WINDOW_SIZE = "12h"

BUY_THRESHOLD = 30
SELL_THRESHOLD = 70
MIN_SIGNALS = 4

POC_MIN = 0.65          # ← 91.7% WR والا
POC_MAX = 0.30          # ← 91.7% WR والا

TARGET_PCT = 0.7        # Take Profit
STOP_PCT = 0.5          # Stop Loss

# 400 کوئنز — کوئی والیوم فلٹر نہیں
MAX_SYMBOLS = 400
KLINES_THRESHOLD_BUFFER = 20
THREADS_KLINES = 20     # Klines کے لیے
THREADS_AGG = 5         # aggTrades کے لیے (سست، کم threads)

# aggTrades کی حد (bug fix)
MAX_CHUNKS = 600        # ← 15 سے 600 (12 گھنٹے کے لیے کافی)

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


# ============================================================
# 📋 400 کوئنز حاصل کریں
# ============================================================
def get_top_symbols(base_url):
    print("📋 USDT کوئنز...")
    try:
        r = requests.get(f"{base_url}/api/v3/ticker/24hr", timeout=30)
        data = r.json()
        if not isinstance(data, list):
            return []
        
        skip = ["USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
                "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY", "USDTBIDR"]
        
        volumes = []
        for t in data:
            sym = t.get('symbol', '')
            if not sym.endswith('USDT') or sym in skip:
                continue
            if 'UP' in sym or 'DOWN' in sym or 'BULL' in sym or 'BEAR' in sym:
                continue
            try:
                vol = float(t.get('quoteVolume', 0))
                volumes.append((sym, vol))
            except:
                continue
        
        volumes.sort(key=lambda x: x[1], reverse=True)
        result = [s for s, v in volumes[:MAX_SYMBOLS]]
        print(f"✅ {len(result)} کوئنز")
        return result
    except Exception as e:
        print(f"❌ {e}")
        return []


# ============================================================
# ⚡ مرحلہ 1: Klines سے تیز فلٹر (20 threads)
# ============================================================
def quick_scan_klines(base_url, symbol):
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
            buy_pct = 0.65 if c > o else 0.35 if c < o else 0.5
            delta_est = int((buy_pct - 0.5) * vol * 100000)
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
        df['bull_of'] = (df['delta'] > 0) & (df['ratio'] > 1.0)
        df['bear_of'] = (df['delta'] < 0) & (df['ratio'] < 1.0)
        df['window'] = df['start_time'].dt.floor(WINDOW_SIZE)
        
        windows = df.groupby('window').agg(
            bull_count=('bull_of', 'sum'),
            bear_count=('bear_of', 'sum'),
        ).reset_index()
        windows['total'] = windows['bull_count'] + windows['bear_count']
        windows['bull_pct'] = windows['bull_count'] / windows['total'] * 100
        
        for _, w in windows.iterrows():
            if w['total'] < MIN_SIGNALS:
                continue
            bp = w['bull_pct']
            if bp <= (BUY_THRESHOLD + KLINES_THRESHOLD_BUFFER) or \
               bp >= (SELL_THRESHOLD - KLINES_THRESHOLD_BUFFER):
                return {'symbol': symbol}
        return None
    except:
        return None


# ============================================================
# 📥 مکمل aggTrades (chunked — bug fix)
# ============================================================
def load_full_aggtrades(base_url, symbol, hours=12):
    """پورے گھنٹوں کا ڈیٹا — MAX_CHUNKS تک"""
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (hours * 60 * 60 * 1000)
    
    all_trades = []
    current = start_time
    chunks = 0
    consecutive_empty = 0
    
    while current < end_time and chunks < MAX_CHUNKS:
        url = f"{base_url}/api/v3/aggTrades"
        params = {
            "symbol": symbol,
            "startTime": current,
            "endTime": end_time,
            "limit": 1000
        }
        try:
            r = requests.get(url, params=params, timeout=15)
            data = r.json()
            
            # کوئی ٹریڈ نہیں؟
            if not data or (isinstance(data, list) and len(data) == 0):
                consecutive_empty += 1
                if consecutive_empty >= 3:
                    break
                current += 60000  # 1 منٹ آگے
                continue
            
            # Dict یعنی error
            if isinstance(data, dict):
                break
            
            consecutive_empty = 0
            
            # 1000 ٹریڈز ملیں — آخری کا ٹائم لیں
            all_trades.extend(data)
            
            if len(data) < 1000:
                # 1000 سے کم = آخری chunk
                break
            
            current = data[-1]['T'] + 1
            chunks += 1
            
        except Exception:
            break
    
    return all_trades


# ============================================================
# 🎯 مرحلہ 2: اصل POC (صرف فلٹر شدہ کوئنز پر)
# ============================================================
def verify_with_aggtrades(base_url, symbol):
    trades = load_full_aggtrades(base_url, symbol, HISTORY_HOURS)
    
    if not trades or len(trades) < 3000:
        return None
    
    # footprint_analyzer — اصل POC
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
    
    # POC 0.65-0.30 والی شرائط
    df['bull_of'] = (df['delta'] > 0) & (df['poc_position'] > POC_MIN) & (df['ratio'] > 1.0)
    df['bear_of'] = (df['delta'] < 0) & (df['poc_position'] < POC_MAX) & (df['ratio'] < 1.0)
    
    df['window'] = df['start_time'].dt.floor(WINDOW_SIZE)
    
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
        }
    
    return None


# ============================================================
# 📲 Nfty (TP اور SL کے ساتھ)
# ============================================================
def send_nfty(signals):
    if not signals:
        return
    
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    lines = [f"🎯 {len(signals)} سگنلز (1h)\n"]
    
    if buys:
        lines.append(f"🟢 BUY ({len(buys)}):")
        for s in buys[:12]:
            sym = s['symbol'].replace("USDT", "")
            entry = s['price']
            tp = entry * (1 + TARGET_PCT/100)
            sl = entry * (1 - STOP_PCT/100)
            lines.append(f"━━━━━━━━━━━━━━━")
            lines.append(f"  {sym} | Bull% {s['bull_pct']:.0f}%")
            lines.append(f"  Entry: ${entry:,.4f}")
            lines.append(f"  TP: ${tp:,.4f} (+{TARGET_PCT}%)")
            lines.append(f"  SL: ${sl:,.4f} (-{STOP_PCT}%)")
        if len(buys) > 12:
            lines.append(f"  +{len(buys)-12} مزید")
    
    if sells:
        lines.append(f"\n🔴 SELL ({len(sells)}):")
        for s in sells[:12]:
            sym = s['symbol'].replace("USDT", "")
            entry = s['price']
            tp = entry * (1 - TARGET_PCT/100)
            sl = entry * (1 + STOP_PCT/100)
            lines.append(f"━━━━━━━━━━━━━━━")
            lines.append(f"  {sym} | Bull% {s['bull_pct']:.0f}%")
            lines.append(f"  Entry: ${entry:,.4f}")
            lines.append(f"  TP: ${tp:,.4f} (-{TARGET_PCT}%)")
            lines.append(f"  SL: ${sl:,.4f} (+{STOP_PCT}%)")
        if len(sells) > 12:
            lines.append(f"  +{len(sells)-12} مزید")
    
    try:
        r = requests.post(
            NFTY_URL,
            data="\n".join(lines).encode('utf-8'),
            headers={
                "Title": f"🎯 {len(signals)} سگنلز (1h)",
                "Priority": "high",
                "Tags": "rotating_light,moneybag",
            },
            timeout=15
        )
        if r.status_code == 200:
            print(f"✅ Nfty بھیجا")
    except Exception as e:
        print(f"❌ Nfty: {e}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 70)
    print(f"🚀 {start.strftime('%H:%M:%S')} | 1h ٹائم فریم")
    print(f"⚙️ POC {POC_MIN}-{POC_MAX} | B≤{BUY_THRESHOLD}% S≥{SELL_THRESHOLD}% | Min{MIN_SIGNALS}")
    print(f"🎯 Target: {TARGET_PCT}% | Stop: {STOP_PCT}%")
    print("=" * 70)
    
    base_url = get_working_endpoint()
    if not base_url:
        print("❌ کوئی endpoint کام نہیں کر رہا")
        return
    print(f"✅ {base_url}\n")
    
    # مرحلہ 1: 400 کوئنز
    symbols = get_top_symbols(base_url)
    if not symbols:
        return
    
    # مرحلہ 2: Klines فلٹر (تیز)
    print(f"\n🔍 مرحلہ 1: {len(symbols)} کوئنز Klines فلٹر...")
    
    candidates = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_KLINES) as ex:
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
    print(f"✅ {len(candidates)} کوئنز پاس | {elapsed1:.0f}s")
    
    if not candidates:
        print("⏳ کوئی کوئن نہیں ملا")
        return
    
    # مرحلہ 3: aggTrades (درست POC)
    print(f"\n🔍 مرحلہ 2: {len(candidates)} کوئنز aggTrades تصدیق...")
    
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(verify_with_aggtrades, base_url, c['symbol']): c['symbol'] 
                   for c in candidates}
        for f in as_completed(futures):
            completed += 1
            if completed % 5 == 0:
                print(f"   ⏳ {completed}/{len(candidates)}")
            try:
                r = f.result(timeout=180)
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
    else:
        print("⏳ کوئی سگنل نہیں — یہ معمول ہے، اگلی بار دیکھیں")


if __name__ == "__main__":
    main()
