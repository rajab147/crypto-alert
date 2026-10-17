#!/usr/bin/env python3
"""Public-data whale-like trade and order-book scanner for GitHub Actions.

Important: this is a 5-minute snapshot scanner, not a continuous WebSocket feed.
In this first version, detailed large-trade/order-book alerts are implemented for
Binance Spot/Futures and OKX Spot/Swap. The other exchanges contribute universe
lists, but are not yet queried for detailed alerts.
"""
import os, time, json, hashlib, logging
from datetime import datetime, timezone
import requests

TOP_N = int(os.getenv("TOP_N", "100"))
MIN_TRADE_USD = float(os.getenv("MIN_TRADE_USD", "100000"))
WALL_MIN_USD = float(os.getenv("WALL_MIN_USD", "500000"))
WALL_DISTANCE_PCT = float(os.getenv("WALL_DISTANCE_PCT", "1.0"))
COOLDOWN_MIN = int(os.getenv("ALERT_COOLDOWN_MIN", "30"))
NFTY_URL = os.getenv("NFTY_URL", "").strip()
STATE_FILE = "whale_scanner_state.json"
S = requests.Session()
S.headers["User-Agent"] = "WhaleActivityScanner/1.0"
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

def getj(url, params=None):
    try:
        r = S.get(url, params=params, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        logging.warning("API request failed (%s): %s", url, e)
        return None

def num(x, default=0.0):
    try: return float(x)
    except (TypeError, ValueError): return default

def load_state():
    try:
        with open(STATE_FILE, encoding="utf-8") as f: return json.load(f)
    except Exception: return {"sent": {}}

def save_state(state):
    with open(STATE_FILE, "w", encoding="utf-8") as f: json.dump(state, f)

def send(title, body, tags="whale,chart_with_upwards_trend"):
    if not NFTY_URL:
        logging.error("NFTY_URL secret is missing.")
        return False
    try:
        r = S.post(NFTY_URL, data=body.encode(), headers={
            "Title": title[:250], "Tags": tags, "Priority": "high",
            "Content-Type": "text/plain; charset=utf-8"
        }, timeout=12)
        if 200 <= r.status_code < 300:
            logging.info("ntfy accepted alert: %s (HTTP %s)", title, r.status_code)
            return True
        logging.error("ntfy HTTP %s: %s", r.status_code, r.text[:150])
    except Exception as e: logging.error("ntfy failed: %s", e)
    return False

def once(state, key, title, body):
    now = time.time()
    last = num(state.setdefault("sent", {}).get(key), 0)
    if now - last < COOLDOWN_MIN * 60: return False
    if send(title, body):
        state["sent"][key] = now
        return True
    return False

def key_for(s):
    return hashlib.sha256(s.encode()).hexdigest()[:24]

def top_binance(market):
    url = "https://api.binance.com/api/v3/ticker/24hr" if market == "spot" else "https://fapi.binance.com/fapi/v1/ticker/24hr"
    data = getj(url)
    if not isinstance(data, list): return []
    rows = [x for x in data if x.get("symbol", "").endswith("USDT")]
    rows.sort(key=lambda x: num(x.get("quoteVolume")), reverse=True)
    return [x["symbol"] for x in rows[:TOP_N]]

def top_okx(market):
    inst_type = "SPOT" if market == "spot" else "SWAP"
    data = getj("https://www.okx.com/api/v5/market/tickers", {"instType": inst_type})
    if not isinstance(data, dict) or data.get("code") != "0": return []
    rows = []
    for x in data.get("data", []):
        inst = x.get("instId", "")
        if (market == "spot" and inst.endswith("-USDT")) or (market == "futures" and inst.endswith("-USDT-SWAP")):
            rows.append((inst, num(x.get("volCcy24h")) * num(x.get("last"))))
    rows.sort(key=lambda z: z[1], reverse=True)
    return [z[0] for z in rows[:TOP_N]]

def top_bybit(market):
    category = "spot" if market == "spot" else "linear"
    data = getj("https://api.bybit.com/v5/market/tickers", {"category": category})
    if not isinstance(data, dict) or data.get("retCode") != 0: return []
    rows = [(x.get("symbol", ""), num(x.get("turnover24h"))) for x in data.get("result", {}).get("list", []) if x.get("symbol", "").endswith("USDT")]
    rows.sort(key=lambda z: z[1], reverse=True)
    return [z[0] for z in rows[:TOP_N]]

def top_gate(market):
    url = "https://api.gateio.ws/api/v4/spot/tickers" if market == "spot" else "https://api.gateio.ws/api/v4/futures/usdt/tickers"
    data = getj(url)
    if not isinstance(data, list): return []
    rows = []
    for x in data:
        sym = x.get("currency_pair", "") if market == "spot" else x.get("contract", "")
        if sym.endswith("_USDT"):
            vol = num(x.get("quote_volume")) if market == "spot" else num(x.get("volume_24h_quote"))
            rows.append((sym, vol))
    rows.sort(key=lambda z: z[1], reverse=True)
    return [z[0] for z in rows[:TOP_N]]

def top_bitget(market):
    if market == "spot":
        url, params = "https://api.bitget.com/api/v2/spot/market/tickers", {}
    else:
        url, params = "https://api.bitget.com/api/v2/mix/market/tickers", {"productType": "USDT-FUTURES"}
    data = getj(url, params)
    if not isinstance(data, dict) or data.get("code") != "00000": return []
    rows = []
    for x in data.get("data", []):
        sym = x.get("symbol", "")
        if sym.endswith("USDT"):
            vol = num(x.get("usdtVolume")) if market == "spot" else num(x.get("quoteVolume"))
            rows.append((sym, vol))
    rows.sort(key=lambda z: z[1], reverse=True)
    return [z[0] for z in rows[:TOP_N]]

def top_symbols(exchange, market):
    funcs = {"binance": top_binance, "okx": top_okx, "bybit": top_bybit, "gateio": top_gate, "bitget": top_bitget}
    try: return funcs[exchange](market)
    except Exception as e:
        logging.warning("%s %s symbols error: %s", exchange, market, e)
        return []

def binance_detail(symbol, market):
    if market == "spot":
        trades_url, book_url = "https://api.binance.com/api/v3/aggTrades", "https://api.binance.com/api/v3/depth"
    else:
        trades_url, book_url = "https://fapi.binance.com/fapi/v1/aggTrades", "https://fapi.binance.com/fapi/v1/depth"
    trades = getj(trades_url, {"symbol": symbol, "limit": 500})
    book = getj(book_url, {"symbol": symbol, "limit": 100})
    parsed = []
    if isinstance(trades, list):
        for t in trades:
            value = num(t.get("p")) * num(t.get("q"))
            if value >= MIN_TRADE_USD:
                parsed.append({"usd": value, "side": "SELL" if t.get("m") else "BUY"})
    bids, asks = [], []
    if isinstance(book, dict):
        bids = [(num(p), num(q), num(p)*num(q)) for p, q in book.get("bids", [])]
        asks = [(num(p), num(q), num(p)*num(q)) for p, q in book.get("asks", [])]
    return parsed, bids, asks

def okx_detail(inst):
    trades_data = getj("https://www.okx.com/api/v5/market/trades", {"instId": inst, "limit": 500})
    book_data = getj("https://www.okx.com/api/v5/market/books", {"instId": inst, "sz": 100})
    trades, bids, asks = [], [], []
    if isinstance(trades_data, dict) and trades_data.get("code") == "0":
        for t in trades_data.get("data", []):
            value = num(t.get("px")) * num(t.get("sz"))
            if value >= MIN_TRADE_USD:
                trades.append({"usd": value, "side": t.get("side", "").upper()})
    if isinstance(book_data, dict) and book_data.get("code") == "0" and book_data.get("data"):
        row = book_data["data"][0]
        bids = [(num(x[0]), num(x[1]), num(x[0])*num(x[1])) for x in row.get("bids", [])]
        asks = [(num(x[0]), num(x[1]), num(x[0])*num(x[1])) for x in row.get("asks", [])]
    return trades, bids, asks

def scan_detail(state, exchange, market, symbol):
    trades, bids, asks = binance_detail(symbol, market) if exchange == "binance" else okx_detail(symbol)
    alerts = 0
    base = f"{exchange}|{market}|{symbol}"
    buy = sum(t["usd"] for t in trades if t["side"] == "BUY")
    sell = sum(t["usd"] for t in trades if t["side"] == "SELL")
    if trades:
        biggest = max(trades, key=lambda x: x["usd"])
        if biggest["usd"] >= MIN_TRADE_USD:
            k = key_for(f"{base}|trade|{biggest['side']}|{int(biggest['usd']//50000)}")
            body = (f"🐋 WHALE-LIKE TRADE\nExchange: {exchange.upper()}\nMarket: {market.upper()}\nPair: {symbol}\n"
                    f"Largest trade: ${biggest['usd']:,.0f}\nAggressor side: {biggest['side']}\n"
                    f"Recent large buy volume: ${buy:,.0f}\nRecent large sell volume: ${sell:,.0f}\n"
                    "Note: this does not identify the trader.")
            if once(state, k, f"🐋 {symbol} {biggest['side']} large trade", body): alerts += 1
        dominant = "BUY" if buy > sell * 1.5 and buy > MIN_TRADE_USD else "SELL" if sell > buy * 1.5 and sell > MIN_TRADE_USD else ""
        if dominant:
            k = key_for(f"{base}|flow|{dominant}|{int((buy+sell)//250000)}")
            body = f"⚡ WHALE-LIKE FLOW\n{exchange.upper()} {market.upper()} {symbol}\nLarge buy volume: ${buy:,.0f}\nLarge sell volume: ${sell:,.0f}"
            if once(state, k, f"⚡ {symbol} {dominant} flow", body): alerts += 1
    if bids and asks:
        mid = (bids[0][0] + asks[0][0]) / 2
        for label, levels in (("BUY WALL", bids), ("SELL WALL", asks)):
            candidates = [x for x in levels if x[2] >= WALL_MIN_USD and mid > 0 and abs(x[0]-mid)/mid*100 <= WALL_DISTANCE_PCT]
            if candidates:
                wall = max(candidates, key=lambda x: x[2])
                k = key_for(f"{base}|{label}|{round(wall[0], 6)}|{int(wall[2]//100000)}")
                body = (f"📚 ORDER BOOK {label}\nExchange: {exchange.upper()}\nMarket: {market.upper()}\nPair: {symbol}\n"
                        f"Wall price: {wall[0]:g}\nEstimated notional: ${wall[2]:,.0f}\n"
                        f"Distance from mid: {abs(wall[0]-mid)/mid*100:.2f}%\n"
                        "Warning: limit orders can be cancelled.")
                if once(state, k, f"📚 {symbol} {label}", body): alerts += 1
    return alerts

def main():
    started = time.time()
    logging.info("Whale scanner started at %s UTC", datetime.now(timezone.utc).isoformat())
    logging.info("Thresholds: trade >= $%s, wall >= $%s, top N=%s", MIN_TRADE_USD, WALL_MIN_USD, TOP_N)
    if not NFTY_URL: logging.error("Set GitHub Secret NFTY_URL to your ntfy topic URL.")
    state, total_alerts, total_symbols = load_state(), 0, 0
    for exchange in ("binance", "okx", "bybit", "gateio", "bitget"):
        for market in ("spot", "futures"):
            symbols = top_symbols(exchange, market)
            logging.info("%s %s top symbols fetched: %d", exchange, market, len(symbols))
            total_symbols += len(symbols)
            # Detailed large-trade and order-book checks currently use Binance and OKX APIs.
            # Other venues provide their own top-100 universe but are not detailed-scanned yet.
            if exchange not in ("binance", "okx"): continue
            for symbol in symbols:
                try: total_alerts += scan_detail(state, exchange, market, symbol)
                except Exception as e: logging.warning("Scan failed %s %s %s: %s", exchange, market, symbol, e)
                time.sleep(0.04)
    save_state(state)
    logging.info("Done: listed symbols=%d, alerts sent=%d, elapsed=%.1fs", total_symbols, total_alerts, time.time()-started)

if __name__ == "__main__":
    main()
