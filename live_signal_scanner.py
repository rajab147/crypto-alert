import os
import requests
import pandas as pd
import numpy as np
import time
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed
from footprint_analyzer import FootprintEngine, FootprintEngineConfig, AggregationType

# ============================================================
# ⚙️ سیٹنگز
# ============================================================
BASE_URL = "https://data-api.binance.vision"
TIMEFRAME = "4h"
HISTORY_HOURS = 6

# 5 رولز
POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.25
DELTA_MIN = 30_000_000
RATIO_BULL_MIN = 1.1
RATIO_BEAR_MAX = 0.9
EMA_TREND = 20

TARGET_PCT = 2.0
STOP_PCT = 1.0

MAX_SYMBOLS = 400
THREADS_KLINES = 8      # کم threads (rate limit)
THREADS_AGG = 4         # کم threads (rate limit)
DELAY = 0.05            # ہر request کے بعد وقفہ

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    exit(1)


# ============================================================
# Rate Limited GET
# ============================================================
def safe_get(url, params=None, timeout=10):
    """Rate limit کے ساتھ GET"""
    try:
        r = requests.get(url, params=params, timeout=timeout)
        if r.status_code == 429:
            print(f"Rate limited — waiting 5s")
            time.sleep(5)
            r = requests.get(url, params=params, timeout=timeout)
        time.sleep(DELAY)
        return r.json()
    except:
        return None


# ============================================================
# 📋 مارکیٹس
# ============================================================
def get_all_markets():
    print("Binance مارکیٹس...")
    try:
        data = safe_get(f"{BASE_URL}/api/v3/exchangeInfo")
        if not data or 'symbols' not in data:
            return []
        
        skip = ["USDCUSDT", "BUSDUSDT", "TUSDUSDT", "FDUSDUSDT", "DAIUSDT",
                "EURUSDT", "GBPUSDT", "AEURUSDT", "USDTTRY", "USDTBIDR"]
        
        symbols = []
        for s in data['symbols']:
            if s.get('quoteAsset') != 'USDT':
                continue
            if s.get('status') != 'TRADING':
                continue
            sym = s.get('symbol', '')
            if sym in skip:
                continue
            if 'UP' in sym or 'DOWN' in sym or 'BULL' in sym or 'BEAR' in sym:
                continue
            symbols.append(sym)
        
        print(f"کل USDT مارکیٹس: {len(symbols)}")
        
        # والیوم کے لحاظ سے
        ticker_data = safe_get(f"{BASE_URL}/api/v3/ticker/24hr")
        if not isinstance(ticker_data, list):
            return symbols[:MAX_SYMBOLS]
        
        symbol_set = set(symbols)
        volumes = []
        for t in ticker_data:
            sym = t.get('symbol')
            if sym not in symbol_set:
                continue
            try:
                vol = float(t.get('quoteVolume', 0))
                volumes.append((sym, vol))
            except:
                continue
        
        volumes.sort(key=lambda x: x[1], reverse=True)
        result = [s for s, v in volumes[:MAX_SYMBOLS]]
        print(f"{len(result)} کوئنز منتخب")
        return result
    except Exception as e:
        print(f"Error: {e}")
        return []


# ============================================================
# ⚡ مرحلہ 1: Klines
# ============================================================
def quick_scan(symbol):
    data = safe_get(f"{BASE_URL}/api/v3/klines",
                    params={"symbol": symbol, "interval": TIMEFRAME, "limit": 30})
    
    if not isinstance(data, list) or len(data) < 25:
        return None
    
    k = data[-2]
    o = float(k[1]); c = float(k[4]); vol = float(k[5])
    
    if c > o:
        buy_pct = 0.7
    elif c < o:
        buy_pct = 0.3
    else:
        return None
    
    delta_est = (buy_pct - 0.5) * vol * 1e6
    ratio_est = buy_pct / (1 - buy_pct)
    
    if abs(delta_est) > DELTA_MIN * 0.3 or ratio_est > 1.15 or ratio_est < 0.85:
        return {'symbol': symbol}
    return None


# ============================================================
# 📥 aggTrades
# ============================================================
def load_aggtrades(symbol):
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (HISTORY_HOURS * 60 * 60 * 1000)
    
    all_trades = []
    current = start_time
    chunks = 0
    empty = 0
    
    while current < end_time and chunks < 20:
        data = safe_get(f"{BASE_URL}/api/v3/aggTrades",
                       params={"symbol": symbol, "startTime": current, "endTime": end_time, "limit": 1000})
        
        if not isinstance(data, list):
            break
        if len(data) == 0:
            empty += 1
            if empty >= 2:
                break
            current += 60000
            continue
        empty = 0
        all_trades.extend(data)
        if len(data) < 1000:
            break
        current = data[-1]['T'] + 1
        chunks += 1
    
    return all_trades


# ============================================================
# 🎯 5 رولز
# ============================================================
def analyze_symbol(symbol):
    trades = load_aggtrades(symbol)
    if not trades or len(trades) < 500:
        return None
    
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
    if len(bars) < 1:
        return None
    
    last = bars[-1]
    rng = last.high_price - last.low_price
    poc_position = (last.poc_price - last.low_price) / rng if rng > 0 else 0.5
    ratio = last.total_bar_ask_volume / last.total_bar_bid_volume if last.total_bar_bid_volume > 0 else 1.0
    delta = last.bar_delta
    
    # EMA20
    klines = safe_get(f"{BASE_URL}/api/v3/klines",
                     params={"symbol": symbol, "interval": TIMEFRAME, "limit": 25})
    is_uptrend = True
    if isinstance(klines, list) and len(klines) >= 20:
        closes = [float(k[4]) for k in klines]
        ema20 = pd.Series(closes).ewm(span=EMA_TREND).mean().iloc[-1]
        is_uptrend = last.close_price > ema20
    
    signal = None
    
    if (poc_position > POC_BULL_MIN and delta > DELTA_MIN and 
        ratio > RATIO_BULL_MIN and is_uptrend):
        signal = 'BUY'
    elif (poc_position < POC_BEAR_MAX and delta < -DELTA_MIN and 
          ratio < RATIO_BEAR_MAX and not is_uptrend):
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
# 📲 Nfty
# ============================================================
def send_nfty(signals):
    if not signals:
        return
    
    buys = [s for s in signals if s['signal'] == 'BUY']
    sells = [s for s in signals if s['signal'] == 'SELL']
    
    lines = [f"{len(signals)} Signals (4h OF)\n"]
    
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
            lines.append(f"POC {s['poc_position']:.2f} | D+{s['delta']/1e6:.0f}M | R {s['ratio']:.2f}")
    
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
            lines.append(f"POC {s['poc_position']:.2f} | D{s['delta']/1e6:.0f}M | R {s['ratio']:.2f}")
    
    message = "\n".join(lines)
    
    try:
        r = requests.post(NFTY_URL, data=message.encode('utf-8'),
            headers={"Title": f"{len(signals)} Signals (4h)",
                     "Priority": "high",
                     "Tags": "rotating_light,moneybag"},
            timeout=15)
        print(f"Nfty: {r.status_code}")
    except Exception as e:
        print(f"Nfty Exception: {e}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | Binance | 4h | 5 Rules")
    print("=" * 60)
    
    symbols = get_all_markets()
    if not symbols:
        print("مارکیٹس نہیں ملیں")
        return
    
    # مرحلہ 1
    print(f"\nمرحلہ 1: {len(symbols)} کوئنز...")
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
    
    # مرحلہ 2
    print(f"\nمرحلہ 2: {len(candidates)} کوئنز...")
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(analyze_symbol, c['symbol']): c['symbol'] 
                   for c in candidates}
        for f in as_completed(futures):
            completed += 1
            if completed % 5 == 0:
                print(f"   {completed}/{len(candidates)}")
            try:
                r = f.result(timeout=30)
                if r:
                    signals.append(r)
                    print(f"   {r['symbol']}: {r['signal']} (POC {r['poc_position']:.2f})")
            except:
                continue
    
    elapsed = (datetime.now() - start).total_seconds()
    print(f"\n{len(symbols)} کوئنز | {len(signals)} سگنلز | {elapsed:.0f}s")
    
    if signals:
        send_nfty(signals)
    else:
        print("کوئی سگنل نہیں")


if __name__ == "__main__":
    main()
