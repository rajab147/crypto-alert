#!/usr/bin/env python3
"""
Standalone Binance CVD Zero-Line Touch scanner.

Implements the volume-classification logic from the user's TradingView Pine Script
using 1-minute Binance spot klines to approximate lower-timeframe intrabars, then
aggregates the CVD candle on 1H. Default reset is daily, matching the Pine script's
default "On a stepped higher timeframe" when the chart timeframe is 1H.

Important limitations:
- Binance klines do not reproduce TradingView's exact feed or its PineCoders LTF
  selection. This is a close implementation of the supplied algorithm, not a
  guaranteed tick-for-tick replica.
- "Touch" is defined by the configurable absolute CVD tolerance. It does NOT
  require a zero-line cross.

Fixes in this version:
- Alerted keys are now saved to state (no repeated notifications).
- Klines are fetched from the UTC day start (paginated), so the daily CVD
  baseline is correct even late in the day (1000 bars only cover ~16.7 hours).
"""
from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import requests
import yaml

BASE_URL = "https://data-api.binance.vision"
SESSION = requests.Session()
SESSION.headers.update({"User-Agent": "CVD-Zero-Touch-Scanner/1.0"})
LOG = logging.getLogger("cvd_zero_touch")


def load_config(path: str) -> dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        cfg = yaml.safe_load(f) or {}
    return cfg


def api_get(endpoint: str, params: dict | None = None, retries: int = 4) -> Any:
    url = BASE_URL + endpoint
    last_error = None
    for attempt in range(retries):
        try:
            response = SESSION.get(url, params=params, timeout=20)
            if response.status_code in (418, 429):
                wait = min(2 ** (attempt + 1), 30)
                LOG.warning("Binance rate limit (%s); waiting %ss", response.status_code, wait)
                time.sleep(wait)
                continue
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError) as exc:
            last_error = exc
            time.sleep(min(2 ** attempt, 10))
    raise RuntimeError(f"Binance API request failed: {endpoint}: {last_error}")


def get_usdt_symbols(cfg: dict[str, Any]) -> list[str]:
    """Dynamically get all eligible USDT spot pairs; normally well over 400."""
    data = api_get("/api/v3/exchangeInfo")
    excluded = set(cfg.get("exclude_symbols", []))
    quote_volume_min = float(cfg.get("min_quote_volume_usdt", 0))
    tickers = api_get("/api/v3/ticker/24hr")
    qv = {x.get("symbol"): float(x.get("quoteVolume", 0) or 0) for x in tickers}
    result = []
    for item in data.get("symbols", []):
        symbol = item.get("symbol", "")
        if item.get("status") != "TRADING":
            continue
        if item.get("quoteAsset") != "USDT" or item.get("isSpotTradingAllowed") is False:
            continue
        if item.get("baseAsset") in {"USDT", "USDC", "FDUSD", "TUSD", "USDP", "DAI", "EUR", "TRY", "BRL"}:
            continue
        if symbol in excluded or qv.get(symbol, 0) < quote_volume_min:
            continue
        result.append(symbol)
    result.sort(key=lambda s: qv.get(s, 0), reverse=True)
    max_symbols = int(cfg.get("max_symbols", 500))
    if max_symbols > 0:
        result = result[:max_symbols]
    minimum = int(cfg.get("minimum_symbols_required", 400))
    if len(result) < minimum:
        LOG.warning("Only %d symbols qualify (configured minimum %d). Lower min_quote_volume_usdt or raise max_symbols.",
                    len(result), minimum)
    LOG.info("Loaded %d eligible USDT spot symbols", len(result))
    return result


def get_klines_since(symbol: str, interval: str, start_ms: int, end_ms: int) -> list[list]:
    """Fetch all klines from start_ms up to end_ms, paginating at 1000 bars per request."""
    out: list[list] = []
    cursor = start_ms
    for _ in range(5):  # safety cap: 5000 bars
        batch = api_get("/api/v3/klines", {
            "symbol": symbol, "interval": interval,
            "startTime": cursor, "endTime": end_ms, "limit": 1000,
        })
        if not batch:
            break
        out.extend(batch)
        if len(batch) < 1000:
            break
        cursor = int(batch[-1][6]) + 1  # next bar after last close time
    return out


def utc_ms_now() -> int:
    return int(time.time() * 1000)


def classify_intrabar_deltas(klines: list[list]) -> list[float]:
    """
    Mirrors Pine upDnIntrabarVolumes():
      close > open => +volume
      close < open => -volume
      otherwise compare close with previous close
      otherwise inherit previous intrabar direction; flat defaults up.
    """
    deltas: list[float] = []
    prior_up = 0.0
    prior_dn = 0.0
    previous_close: float | None = None
    for k in klines:
        o, c, vol = float(k[1]), float(k[4]), float(k[5])
        up = 0.0
        dn = 0.0
        if c > o:
            up += vol
        elif c < o:
            dn -= vol
        elif previous_close is not None and c > previous_close:
            up += vol
        elif previous_close is not None and c < previous_close:
            dn -= vol
        elif prior_up > 0:
            up += vol
        elif prior_dn < 0:
            dn -= vol
        else:
            up += vol
        deltas.append(up + dn)
        prior_up, prior_dn = up, dn
        previous_close = c
    return deltas


def load_state(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def save_state(path: Path, state: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(state, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def cvd_touch_for_symbol(symbol: str, cfg: dict[str, Any], now_ms: int) -> dict[str, Any] | None:
    interval = str(cfg.get("lower_timeframe", "1m"))

    now_dt = datetime.fromtimestamp(now_ms / 1000, tz=timezone.utc)
    day_start = int(now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp() * 1000)
    current_hour_start = int(now_dt.replace(minute=0, second=0, microsecond=0).timestamp() * 1000)

    # Fetch from the UTC day start so the daily CVD baseline is always complete.
    klines = get_klines_since(symbol, interval, day_start, now_ms)
    if not klines or len(klines) < 5:
        return None

    # Exclude the currently forming lower-timeframe candle to avoid unstable partial volume.
    today = [k for k in klines if int(k[6]) < now_ms and int(k[0]) >= day_start]
    current_hour = [k for k in today if int(k[0]) >= current_hour_start]
    if len(today) < 5 or len(current_hour) < 2:
        return None

    deltas = classify_intrabar_deltas(today)
    # Build cumulative CVD from UTC day reset, same reset intent as Pine's 1H default (daily).
    cvd_values = []
    running = 0.0
    for delta in deltas:
        running += delta
        cvd_values.append(running)

    # Restrict touch detection to the current 1H candle, but keep the session cumulative baseline.
    index_by_open = {int(k[0]): i for i, k in enumerate(today)}
    tolerance = float(cfg.get("zero_tolerance_abs", 0.0))
    if tolerance <= 0:
        # Relative tolerance is configurable; base it on average volume of current-hour intrabars.
        pct = float(cfg.get("zero_tolerance_pct_of_avg_1m_volume", 0.01))
        avg_volume = sum(float(k[5]) for k in current_hour) / max(len(current_hour), 1)
        tolerance = max(avg_volume * pct, 1e-12)

    touches = []
    for k in current_hour:
        idx = index_by_open.get(int(k[0]))
        if idx is not None and abs(cvd_values[idx]) <= tolerance:
            touches.append((int(k[0]), cvd_values[idx]))
    if not touches:
        return None

    touch_ms, cvd_at_touch = touches[-1]
    # Alert only for touches within the configured freshness window.
    fresh_minutes = int(cfg.get("fresh_touch_minutes", 15))
    if now_ms - touch_ms > fresh_minutes * 60_000:
        return None

    return {
        "symbol": symbol,
        "timeframe": "1H",
        "touch_time_utc": datetime.fromtimestamp(touch_ms / 1000, tz=timezone.utc).isoformat(),
        "cvd_at_touch": round(float(cvd_at_touch), 8),
        "tolerance": round(float(tolerance), 8),
        "price": float(current_hour[-1][4]),
    }


def send_ntfy(cfg: dict[str, Any], title: str, message: str) -> bool:
    topic = os.environ.get(str(cfg.get("ntfy_topic_env", "NTFY_TOPIC")), "").strip()
    if not topic:
        LOG.warning("NTFY topic is not configured; signal will only appear in logs.")
        return False
    url = str(cfg.get("ntfy_server", "https://ntfy.sh")).rstrip("/") + "/" + topic
    headers = {"Title": title, "Priority": str(cfg.get("ntfy_priority", "4")),
               "Tags": str(cfg.get("ntfy_tags", "chart_with_upwards_trend"))}
    token = os.environ.get(str(cfg.get("ntfy_token_env", "NTFY_TOKEN")), "").strip()
    if token:
        headers["Authorization"] = f"Bearer {token}"
    try:
        response = SESSION.post(url, data=message.encode("utf-8"), headers=headers, timeout=20)
        response.raise_for_status()
        return True
    except requests.RequestException as exc:
        LOG.error("ntfy send failed: %s", exc)
        return False


def run_once(cfg: dict[str, Any], state: dict[str, Any]) -> dict[str, Any]:
    now_ms = utc_ms_now()
    symbols = get_usdt_symbols(cfg)
    workers = max(1, min(int(cfg.get("workers", 8)), 16))
    found: list[dict[str, Any]] = []
    failures = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        futures = {executor.submit(cvd_touch_for_symbol, sym, cfg, now_ms): sym for sym in symbols}
        for future in as_completed(futures):
            sym = futures[future]
            try:
                result = future.result()
                if not result:
                    continue
                # One alert per symbol per 1H candle, avoids repeated notifications for same touch.
                hour_key = result["touch_time_utc"][:13]
                key = f"{sym}|{hour_key}"
                if state.get(key):
                    continue
                result["_state_key"] = key
                found.append(result)
            except Exception as exc:
                failures += 1
                LOG.debug("%s failed: %s", sym, exc)

    LOG.info("Scan complete: %d symbols, %d fresh touches, %d errors", len(symbols), len(found), failures)
    state_path = Path(str(cfg.get("state_file", "cvd_zero_touch_state.json")))

    # Prune state older than 7 days.
    cutoff = now_ms - 7 * 24 * 60 * 60 * 1000
    state = {k: v for k, v in state.items() if isinstance(v, (int, float)) and v >= cutoff}

    if found:
        found.sort(key=lambda x: x["symbol"])
        lines = []
        for item in found:
            lines.append(
                f'{item["symbol"]} | 1H CVD ZERO-LINE TOUCH\n'
                f'Time: {item["touch_time_utc"]}\n'
                f'CVD: {item["cvd_at_touch"]} (tolerance ±{item["tolerance"]})\n'
                f'Price: {item["price"]}'
            )
        message = "\n\n".join(lines)
        print(message)
        sent = send_ntfy(cfg, "1H CVD ZERO-LINE TOUCH", message)
        # Remember alerted keys so the same touch is not announced again.
        # If the push failed, keep them unmarked so the next run retries.
        if sent:
            for item in found:
                state[item["_state_key"]] = now_ms
    else:
        LOG.info("No fresh 1H CVD zero-line touch.")

    save_state(state_path, state)
    return state


def main() -> None:
    import argparse
    parser = argparse.ArgumentParser(description="1H CVD zero-line touch scanner")
    parser.add_argument("--config", default="cvd_zero_touch.yaml")
    parser.add_argument("--once", action="store_true", help="run one scan and exit")
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, os.environ.get("LOG_LEVEL", "INFO").upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )
    cfg = load_config(args.config)
    state_path = Path(str(cfg.get("state_file", "cvd_zero_touch_state.json")))
    state = load_state(state_path)

    if args.once or not bool(cfg.get("continuous", False)):
        run_once(cfg, state)
        return

    poll_seconds = max(60, int(cfg.get("poll_seconds", 300)))
    while True:
        try:
            state = load_state(state_path)
            run_once(cfg, state)
        except KeyboardInterrupt:
            break
        except Exception:
            LOG.exception("Scan cycle failed")
        time.sleep(poll_seconds)


if __name__ == "__main__":
    main()
