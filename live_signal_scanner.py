import os
import requests
import pandas as pd
import numpy as np
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز — صرف آرڈر فلو کے 5 رولز
# ============================================================
TIMEFRAME = "4h"
HISTORY_CANDLES = 60        # 60 × 4h = 10 دن

# 5 رولز
POC_BULL_MIN = 0.65         # رول 1: بلش کے لیے POC > 0.65
POC_BEAR_MAX = 0.25         # رول 1: بیئرش کے لیے POC < 0.25 (Extreme)
DELTA_MIN = 30_000_000      # رول 2: Delta > 30M
RATIO_BULL_MIN = 1.1        # رول 3: Ask > Bid
RATIO_BEAR_MAX = 0.9        # رول 3: Bid > Ask
EMA_TREND = 20              # رول 4: رجحان

# TP/SL
TARGET_PCT = 2.0
STOP_PCT = 1.0
MAX_HOLD = 6                # 6 × 4h = 24h

# کوئنز
MIN_VOLUME_USDT = 10_000_000   # 10 ملین
MAX_SYMBOLS = 200
THREADS_KLINES = 20
THREADS_AGG = 5
MAX_CHUNKS = 120

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
# 📋 200 کوئنز
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
                if vol >= MIN_VOLUME_USDT:
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
# ⚡ مرحلہ 1: Klines تیز فلٹر
# ============================================================
def quick_scan(base_url, symbol):
    """Klines سے تیز فلٹر — صرف Delta + Ratio"""
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": HISTORY_CANDLES}
    
    try:
        r = requests.get(url, params=params, timeout=10)
        data = r.json()
        if not isinstance(data, list) or len(data) < 10:
            return None
        
        # آخری 4 کینڈلز چیک کریں
        for k in data[-4:]:
            o = float(k[1]); c = float(k[4]); vol = float(k[5])
            
            if c > o:
                buy_pct = 0.7
            elif c < o:
                buy_pct = 0.3
            else:
                continue
            
            delta_est = (buy_pct - 0.5) * vol * 1e6
            ratio_est = buy_pct / (1 - buy_pct)
            
            # صرف ان کوئنز کو آگے بھیجیں جن میں کوئی بھی رول قریب ہو
            if abs(delta_est) > DELTA_MIN * 0.5:
                if ratio_est > RATIO_BULL_MIN or ratio_est < RATIO_BEAR_MAX:
                    return {'symbol': symbol}
        return None
    except:
        return None


# ============================================================
# 📥 aggTrades سے اصل POC
# ============================================================
def load_aggtrades(base_url, symbol, chunks_limit):
    end_time = int(datetime.now().timestamp() * 1000)
    # صرف آخری 40 گھنٹے (10 کینڈلز)
    start_time = end_time - (40 * 60 * 60 * 1000)
    
    all_trades = []
    current = start_time
    chunks = 0
    empty = 0
    
    while current < end_time and chunks < chunks_limit:
        url = f"{base_url}/api/v3/aggTrades"
        params = {"symbol": symbol, "startTime": current, "endTime": end_time, "limit": 1000}
        try:
            r = requests.get(url, params=params, timeout=10)
            data = r.json()
            if not isinstance(data, list):
                break
            if len(data) == 0:
                empty += 1
                if empty >= 3:
                    break
                current += 60000
                continue
            empty = 0
            all_trades.extend(data)
            if len(data) < 1000:
                break
            current = data[-1]['T'] + 1
            chunks += 1
        except:
            break
    
    return all_trades


# ============================================================
# 🎯 5 رولز کی جانچ
# ============================================================
def analyze_symbol(base_url, symbol):
    trades = load_aggtrades(base_url, symbol, MAX_CHUNKS)
    if not trades or len(trades) < 2000:
        return None
    
    # footprint_analyzer سے 4h کینڈلز
    config = FootprintEngineConfig(
        tick_size=0.00001,
        aggregation_type=AggregationType.TIME,
        aggregation_value="4h",
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
    if len(bars) < 5:
        return None
    
    # آخری مکمل کینڈل
    last = bars[-1]
    
    # میٹرکس
    poc_position = (last.poc_price - last.low_price) / (last.high_price - last.low_price) if (last.high_price - last.low_price) > 0 else 0.5
    ratio = last.total_bar_ask_volume / last.total_bar_bid_volume if last.total_bar_bid_volume > 0 else 1.0
    delta = last.bar_delta
    
    # رول 4: EMA20 (پچھلے 20 کینڈلز)
    closes = [b.close_price for b in bars[-20:]]
    ema20 = pd.Series(closes).ewm(span=20).mean().iloc[-1]
    close_price = last.close_price
    is_uptrend = close_price > ema20
    
    # ============================================================
    # 5 رولز کی جانچ
    # ============================================================
    
    signal = None
    
    # 🟢 بلش کے لیے 5 رولز
    if (poc_position > POC_BULL_MIN and           # رول 1
        delta > DELTA_MIN and                      # رول 2
        ratio > RATIO_BULL_MIN and                 # رول 3
        is_uptrend):                               # رول 4 (رجحان کے ساتھ)
        signal = 'BUY'
    
    # 🔴 بیئرش کے لیے 5 رولز
    elif (poc_position < POC_BEAR_MAX and         # رول 1 (Extreme)
          delta < -DELTA_MIN and                   # رول 2
          ratio < RATIO_BEAR_MAX and               # رول 3
          not is_uptrend):                         # رول 4 (رجحان کے ساتھ)
        signal = 'SELL'
    
    if signal is None:
        return None
    
    return {
        'symbol': symbol,
        'signal': signal,
        'price': close_price,
        'poc': last.poc_price,
        'poc_position': poc_position,
        'delta': delta,
        'ratio': ratio,
        'is_uptrend': is_uptrend,
    }


# ============================================================
# 📲 Nfty نوٹیفکیشن
# ============================================================
def send_nfty(signals):
    if not signals:
        return
    
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    lines = [f"🎯 {len(signals)} سگنلز (4h Order Flow)\n"]
    
    if buys:
        lines.append(f"🟢 BUY ({len(buys)}):")
        for s in buys:
            sym = s['symbol'].replace("USDT", "")
            entry = s['price']
            tp = entry * (1 + TARGET_PCT/100)
            sl = entry * (1 - STOP_PCT/100)
            lines.append(f"━━━━━━━━━━━━━━━")
            lines.append(f"{sym}")
            lines.append(f"Entry: ${entry:,.4f}")
            lines.append(f"TP: ${tp:,.4f} (+{TARGET_PCT}%)")
            lines.append(f"SL: ${sl:,.4f} (-{STOP_PCT}%)")
            lines.append(f"POC: {s['poc_position']:.2f} | Delta: +{s['delta']/1e6:.0f}M | Ratio: {s['ratio']:.2f}")
    
    if sells:
        lines.append(f"\n🔴 SELL ({len(sells)}):")
        for s in sells:
            sym = s['symbol'].replace("USDT", "")
            entry = s['price']
            tp = entry * (1 - TARGET_PCT/100)
            sl = entry * (1 + STOP_PCT/100)
            lines.append(f"━━━━━━━━━━━━━━━")
            lines.append(f"{sym}")
            lines.append(f"Entry: ${entry:,.4f}")
            lines.append(f"TP: ${tp:,.4f} (-{TARGET_PCT}%)")
            lines.append(f"SL: ${sl:,.4f} (+{STOP_PCT}%)")
            lines.append(f"POC: {s['poc_position']:.2f} | Delta: {s['delta']/1e6:.0f}M | Ratio: {s['ratio']:.2f}")
    
    try:
        r = requests.post(
            NFTY_URL,
            data="\n".join(lines).encode('utf-8'),
            headers={
                "Title": f"🎯 {len(signals)} سگنلز (4h)",
                "Priority": "high",
                "Tags": "rotating_light,moneybag",
            },
            timeout=15
        )
        if r.status_code == 200:
            print(f"✅ Nfty بھیجا")
    except Exception as e:
        print(f"❌ {e}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 70)
    print(f"🚀 {start.strftime('%H:%M:%S')} | 4h Order Flow (5 Rules)")
    print(f"📊 کوئنز: {MAX_SYMBOLS}")
    print(f"⚙️ POC > {POC_BULL_MIN} | Delta > {DELTA_MIN/1e6:.0f}M | Ratio > {RATIO_BULL_MIN}")
    print(f"🎯 TP: {TARGET_PCT}% | SL: {STOP_PCT}%")
    print("=" * 70)
    
    base_url = get_working_endpoint()
    if not base_url:
        return
    print(f"✅ {base_url}\n")
    
    symbols = get_top_symbols(base_url)
    if not symbols:
        return
    
    # مرحلہ 1: تیز فلٹر
    print(f"🔍 مرحلہ 1: {len(symbols)} کوئنز Klines فلٹر...")
    candidates = []
    
    with ThreadPoolExecutor(max_workers=THREADS_KLINES) as ex:
        futures = {ex.submit(quick_scan, base_url, s): s for s in symbols}
        for f in as_completed(futures):
            try:
                r = f.result(timeout=20)
                if r:
                    candidates.append(r)
            except:
                continue
    
    print(f"✅ {len(candidates)} کوئنز پاس")
    
    if not candidates:
        print("⏳ کوئی کوئن نہیں")
        return
    
    # مرحلہ 2: اصل POC
    print(f"\n🔍 مرحلہ 2: {len(candidates)} کوئنز aggTrades...")
    signals = []
    
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(analyze_symbol, base_url, c['symbol']): c['symbol'] 
                   for c in candidates}
        for f in as_completed(futures):
            try:
                r = f.result(timeout=120)
                if r:
                    signals.append(r)
                    print(f"   🎯 {r['symbol']}: {r['signal']} "
                          f"(POC {r['poc_position']:.2f}, Delta {r['delta']/1e6:+.0f}M)")
            except:
                continue
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n📊 {len(symbols)} کوئنز | {len(signals)} سگنلز | {elapsed:.0f}s")
    
    if signals:
        send_nfty(signals)
    else:
        print("⏳ کوئی سگنل نہیں (کوئی کوئن 5 رولز پر پورا نہیں اترا)")


if __name__ == "__main__":
    main()
