import os
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز — 5 رولز (91.7% WR والی حکمت عملی)
# ============================================================
BASE_URL = "https://whitebit.com"
TIMEFRAME = "4h"
HISTORY_CANDLES = 60          # Klines کے لیے
HISTORY_HOURS = 6             # aggTrades کے لیے (صرف تازہ ڈیٹا)

# 5 رولز
POC_BULL_MIN = 0.65           # رول 1: بلش کے لیے POC > 0.65
POC_BEAR_MAX = 0.25           # رول 1: بیئرش کے لیے POC < 0.25 (Extreme)
DELTA_MIN = 30_000_000        # رول 2: Delta > 30M
RATIO_BULL_MIN = 1.1          # رول 3: Ask > Bid
RATIO_BEAR_MAX = 0.9          # رول 3: Bid > Ask
EMA_TREND = 20                # رول 4: رجحان

TARGET_PCT = 2.0              # Take Profit
STOP_PCT = 1.0                # Stop Loss

# کوئنز
MAX_SYMBOLS = 400
THREADS_KLINES = 30
THREADS_AGG = 10
MAX_CHUNKS = 20

# Nfty
NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("❌ NFTY_URL set نہیں ہے")
    exit(1)


# ============================================================
# 📋 WhiteBIT مارکیٹس حاصل کریں (400+ کوئنز)
# ============================================================
def get_all_markets():
    """WhiteBIT سے تمام USDT مارکیٹس حاصل کریں"""
    print("📋 WhiteBIT مارکیٹس...")
    try:
        r = requests.get(f"{BASE_URL}/api/v4/public/markets", timeout=30)
        data = r.json()
        
        markets = []
        for m in data.get('result', []):
            name = m.get('name', '')
            if not name.endswith('_USDT'):
                continue
            # صرف active مارکیٹس
            if m.get('status') != 'active':
                continue
            markets.append(name)
        
        # والیوم کے لحاظ سے ترتیب دیں
        ticker_url = f"{BASE_URL}/api/v4/public/ticker"
        r2 = requests.get(ticker_url, timeout=30)
        tickers = r2.json().get('result', {})
        
        volumes = []
        for m in markets:
            vol = float(tickers.get(m, {}).get('quoteVolume', 0))
            if vol > 1_000_000:  # 1M+ والیوم
                volumes.append((m, vol))
        
        volumes.sort(key=lambda x: x[1], reverse=True)
        result = [m for m, v in volumes[:MAX_SYMBOLS]]
        print(f"✅ {len(result)} کوئنز")
        return result
    except Exception as e:
        print(f"❌ {e}")
        return []


# ============================================================
# ⚡ مرحلہ 1: Klines تیز فلٹر
# ============================================================
def quick_scan(symbol):
    """Klines سے تیز فلٹر"""
    try:
        r = requests.get(
            f"{BASE_URL}/api/v4/public/kline",
            params={"market": symbol, "interval": "4h", "limit": 30},
            timeout=10
        )
        data = r.json()
        if not isinstance(data, list) or len(data) < 5:
            return None
        
        # آخری مکمل 4h کینڈل
        k = data[-2]
        o = float(k[2]); c = float(k[5]); vol = float(k[6])
        
        if c > o:
            buy_pct = 0.7
        elif c < o:
            buy_pct = 0.3
        else:
            return None
        
        delta_est = (buy_pct - 0.5) * vol * 1e6
        ratio_est = buy_pct / (1 - buy_pct)
        
        # تیز فلٹر
        if abs(delta_est) > DELTA_MIN * 0.3 or ratio_est > 1.15 or ratio_est < 0.85:
            return {'symbol': symbol}
        return None
    except:
        return None


# ============================================================
# 📥 aggTrades سے اصل POC
# ============================================================
def load_aggtrades(symbol):
    """WhiteBIT سے ٹریڈز حاصل کریں"""
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (HISTORY_HOURS * 60 * 60 * 1000)
    
    try:
        r = requests.get(
            f"{BASE_URL}/api/v4/public/trades/{symbol}",
            timeout=15
        )
        data = r.json()
        
        if not isinstance(data, list):
            return []
        
        # ٹائم فلٹر (6 گھنٹے)
        trades = []
        for t in data:
            ts = int(t.get('time', 0)) * 1000
            if ts >= start_time:
                trades.append({
                    'T': ts,
                    'p': float(t.get('price', 0)),
                    'q': float(t.get('amount', 0)),
                    'm': t.get('type') == 'sell'  # sell = True (Buyer is Maker)
                })
        return trades
    except:
        return []


# ============================================================
# 🎯 5 رولز کا تجزیہ
# ============================================================
def analyze_symbol(symbol):
    """پانچ رولز کے ساتھ ایک کوئن کا تجزیہ"""
    trades = load_aggtrades(symbol)
    if not trades or len(trades) < 100:
        return None
    
    # footprint_analyzer
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
                price=t['p'],
                volume=max(1, int(t['q'] * 100000)),
                is_bid_trade=not t['m']
            )
        except:
            continue
    
    bars = engine.get_all_completed_bars()
    if len(bars) < 1:
        return None
    
    last = bars[-1]
    
    # میٹرکس
    rng = last.high_price - last.low_price
    poc_position = (last.poc_price - last.low_price) / rng if rng > 0 else 0.5
    ratio = last.total_bar_ask_volume / last.total_bar_bid_volume if last.total_bar_bid_volume > 0 else 1.0
    delta = last.bar_delta
    
    # رول 4: EMA20 (Klines سے)
    try:
        r = requests.get(
            f"{BASE_URL}/api/v4/public/kline",
            params={"market": symbol, "interval": "4h", "limit": 25},
            timeout=8
        )
        klines = r.json()
        closes = [float(k[5]) for k in klines]
        ema20 = pd.Series(closes).ewm(span=EMA_TREND).mean().iloc[-1]
        is_uptrend = last.close_price > ema20
    except:
        is_uptrend = True
    
    # ============================================================
    # 5 رولز
    # ============================================================
    signal = None
    
    # 🟢 BUY — 5 رولز
    if (poc_position > POC_BULL_MIN and           # رول 1
        delta > DELTA_MIN and                      # رول 2
        ratio > RATIO_BULL_MIN and                 # رول 3
        is_uptrend):                               # رول 4
        signal = 'BUY'
    
    # 🔴 SELL — 5 رولز
    elif (poc_position < POC_BEAR_MAX and         # رول 1 (Extreme)
          delta < -DELTA_MIN and                   # رول 2
          ratio < RATIO_BEAR_MAX and               # رول 3
          not is_uptrend):                         # رول 4
        signal = 'SELL'
    
    if signal is None:
        return None
    
    return {
        'symbol': symbol,
        'signal': signal,
        'price': last.close_price,
        'poc_position': poc_position,
        'delta': delta,
        'ratio': ratio,
    }


# ============================================================
# 📲 Nfty (Emoji کے بغیر — پرائیویسی محفوظ)
# ============================================================
def send_nfty(signals):
    if not signals:
        return
    
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    lines = [f"{len(signals)} Signals (4h Order Flow)\n"]
    
    if buys:
        lines.append(f"BUY ({len(buys)}):")
        for s in buys:
            sym = s['symbol'].replace("_USDT", "")
            entry = s['price']
            tp = entry * (1 + TARGET_PCT/100)
            sl = entry * (1 - STOP_PCT/100)
            lines.append("---")
            lines.append(f"{sym} | {entry:.4f}")
            lines.append(f"TP {tp:.4f} | SL {sl:.4f}")
            lines.append(f"POC {s['poc_position']:.2f} | Delta +{s['delta']/1e6:.0f}M | Ratio {s['ratio']:.2f}")
    
    if sells:
        lines.append(f"\nSELL ({len(sells)}):")
        for s in sells:
            sym = s['symbol'].replace("_USDT", "")
            entry = s['price']
            tp = entry * (1 - TARGET_PCT/100)
            sl = entry * (1 + STOP_PCT/100)
            lines.append("---")
            lines.append(f"{sym} | {entry:.4f}")
            lines.append(f"TP {tp:.4f} | SL {sl:.4f}")
            lines.append(f"POC {s['poc_position']:.2f} | Delta {s['delta']/1e6:.0f}M | Ratio {s['ratio']:.2f}")
    
    message = "\n".join(lines)
    
    try:
        r = requests.post(
            NFTY_URL,
            data=message.encode('utf-8'),
            headers={
                "Title": f"{len(signals)} Signals (4h)",
                "Priority": "high",
                "Tags": "rotating_light,moneybag",
            },
            timeout=15
        )
        print(f"Nfty response: {r.status_code}")
        if r.status_code != 200:
            print(f"Error: {r.text}")
    except Exception as e:
        print(f"Nfty exception: {e}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 70)
    print(f"🚀 {start.strftime('%H:%M:%S')} | WhiteBIT | 4h | 5 Rules")
    print(f"📊 کوئنز: {MAX_SYMBOLS}")
    print(f"⚙️ POC>{POC_BULL_MIN} | Δ>{DELTA_MIN/1e6:.0f}M | Ratio>{RATIO_BULL_MIN}")
    print("=" * 70)
    
    symbols = get_all_markets()
    if not symbols:
        return
    
    # مرحلہ 1: Klines تیز فلٹر
    print(f"\n🔍 مرحلہ 1: {len(symbols)} کوئنز Klines فلٹر...")
    candidates = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_KLINES) as ex:
        futures = {ex.submit(quick_scan, s): s for s in symbols}
        for f in as_completed(futures):
            completed += 1
            if completed % 100 == 0:
                print(f"   ⏳ {completed}/{len(symbols)}")
            try:
                r = f.result(timeout=15)
                if r:
                    candidates.append(r)
            except:
                continue
    
    print(f"✅ {len(candidates)} کوئنز پاس")
    
    if not candidates:
        print("⏳ کوئی کوئن نہیں")
        return
    
    # مرحلہ 2: aggTrades
    print(f"\n🔍 مرحلہ 2: {len(candidates)} کوئنز aggTrades...")
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(analyze_symbol, c['symbol']): c['symbol'] 
                   for c in candidates}
        for f in as_completed(futures):
            completed += 1
            if completed % 10 == 0:
                print(f"   ⏳ {completed}/{len(candidates)}")
            try:
                r = f.result(timeout=30)
                if r:
                    signals.append(r)
                    print(f"   🎯 {r['symbol']}: {r['signal']} "
                          f"(POC {r['poc_position']:.2f})")
            except:
                continue
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n📊 {len(symbols)} کوئنز | {len(signals)} سگنلز | {elapsed:.0f}s")
    
    if signals:
        send_nfty(signals)
    else:
        print("⏳ کوئی سگنل نہیں")


if __name__ == "__main__":
    main()
