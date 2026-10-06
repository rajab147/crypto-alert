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
TIMEFRAME = "4h"
HISTORY_HOURS = 6           # صرف 6 گھنٹے (تیز)
HISTORY_CANDLES = 30

# 5 رولز
POC_BULL_MIN = 0.65
POC_BEAR_MAX = 0.25
DELTA_MIN = 30_000_000
RATIO_BULL_MIN = 1.1
RATIO_BEAR_MAX = 0.9
EMA_TREND = 20

TARGET_PCT = 2.0
STOP_PCT = 1.0
MAX_HOLD = 6

MIN_VOLUME_USDT = 10_000_000
MAX_SYMBOLS = 200
THREADS_KLINES = 30
THREADS_AGG = 15
MAX_CHUNKS = 20

NFTY_URL = os.environ.get("NFTY_URL")
if not NFTY_URL:
    print("NFTY_URL set نہیں ہے")
    exit(1)

BINANCE_ENDPOINTS = [
    "https://data-api.binance.vision",
    "https://api.binance.com",
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
    print("USDT کوئنز...")
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
        print(f"{len(result)} کوئنز")
        return result
    except Exception as e:
        print(f"Error: {e}")
        return []


# ============================================================
# ⚡ مرحلہ 1: Klines تیز فلٹر
# ============================================================
def quick_scan(base_url, symbol):
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": HISTORY_CANDLES}
    
    try:
        r = requests.get(url, params=params, timeout=8)
        data = r.json()
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
        
        if (abs(delta_est) > DELTA_MIN * 0.3 or 
            ratio_est > 1.15 or ratio_est < 0.85):
            return {'symbol': symbol}
        return None
    except:
        return None


# ============================================================
# 📥 aggTrades
# ============================================================
def load_aggtrades(base_url, symbol):
    end_time = int(datetime.now().timestamp() * 1000)
    start_time = end_time - (HISTORY_HOURS * 60 * 60 * 1000)
    
    all_trades = []
    current = start_time
    chunks = 0
    empty = 0
    
    while current < end_time and chunks < MAX_CHUNKS:
        url = f"{base_url}/api/v3/aggTrades"
        params = {"symbol": symbol, "startTime": current, "endTime": end_time, "limit": 1000}
        try:
            r = requests.get(url, params=params, timeout=8)
            data = r.json()
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
        except:
            break
    
    return all_trades


# ============================================================
# 🎯 5 رولز
# ============================================================
def analyze_symbol(base_url, symbol):
    trades = load_aggtrades(base_url, symbol)
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
    url = f"{base_url}/api/v3/klines"
    params = {"symbol": symbol, "interval": TIMEFRAME, "limit": 25}
    try:
        r = requests.get(url, params=params, timeout=8)
        klines = r.json()
        closes = [float(k[4]) for k in klines]
        ema20 = pd.Series(closes).ewm(span=20).mean().iloc[-1]
        is_uptrend = last.close_price > ema20
    except:
        is_uptrend = True
    
    signal = None
    
    if (poc_position > POC_BULL_MIN and 
        delta > DELTA_MIN and 
        ratio > RATIO_BULL_MIN and 
        is_uptrend):
        signal = 'BUY'
    elif (poc_position < POC_BEAR_MAX and 
          delta < -DELTA_MIN and 
          ratio < RATIO_BEAR_MAX and 
          not is_uptrend):
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
# 📲 Nfty (Emoji کے بغیر — صاف)
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
            sym = s['symbol'].replace("USDT", "")
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
            sym = s['symbol'].replace("USDT", "")
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
        if r.status_code == 200:
            print(f"Nfty sent successfully")
        else:
            print(f"Nfty error: {r.text}")
    except Exception as e:
        print(f"Nfty exception: {e}")


# ============================================================
# 🎬 MAIN
# ============================================================
def main():
    start = datetime.now()
    print("=" * 60)
    print(f"{start.strftime('%H:%M:%S')} | 4h | صرف 6 گھنٹے")
    print("=" * 60)
    
    base_url = get_working_endpoint()
    if not base_url:
        print("Endpoint نہیں ملا")
        return
    print(f"Endpoint: {base_url}\n")
    
    symbols = get_top_symbols(base_url)
    if not symbols:
        return
    
    # مرحلہ 1
    print(f"مرحلہ 1: {len(symbols)} کوئنز Klines فلٹر...")
    candidates = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_KLINES) as ex:
        futures = {ex.submit(quick_scan, base_url, s): s for s in symbols}
        for f in as_completed(futures):
            completed += 1
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
    print(f"\nمرحلہ 2: {len(candidates)} کوئنز aggTrades...")
    signals = []
    completed = 0
    
    with ThreadPoolExecutor(max_workers=THREADS_AGG) as ex:
        futures = {ex.submit(analyze_symbol, base_url, c['symbol']): c['symbol'] 
                   for c in candidates}
        for f in as_completed(futures):
            completed += 1
            try:
                r = f.result(timeout=30)
                if r:
                    signals.append(r)
                    print(f"{r['symbol']}: {r['signal']} (POC {r['poc_position']:.2f})")
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
