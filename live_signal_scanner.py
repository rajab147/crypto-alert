"""
LIVE LIQUIDITY SWEEP SIGNAL SCANNER
===================================

Binance USDT Perpetual Futures
Entry TF: 30m

Rules:
- Daily liquidity sweep
- 4H liquidity sweep
- Sweep must be CLOSED before it can generate a signal
- 30m candle must also be CLOSED
- RSI filter
- Volume filter
- SL + TP 1:1 / 1:2 / 1:3
- NTFY notification
- Duplicate protection
- Detailed diagnostic logging

LOCAL:
    python3 live_signal_scanner.py --once

NTFY TEST:
    python3 live_signal_scanner.py --test-notify

GITHUB ACTIONS:
    Run with --once every 30 minutes.
"""

import requests
import pandas as pd
import numpy as np
import time
import json
import os
import argparse


# ============================================================
# SETTINGS
# ============================================================

MAX_SYMBOLS = 500

CHECK_INTERVAL_SECONDS = 1800

# Sweep rejection buffer
BUFFER = 0.001

# RSI
RSI_PERIOD = 9
RSI_OB = 75
RSI_OS = 25

# Volume
VOL_PERIOD = 20
VOL_MULT = 1.5

# TP levels
RR_LIST = [1, 2, 3]

# Entry timeframe
ENTRY_TIMEFRAME = "30m"

# Sweep lookback
DAILY_LOOKBACK = 10
H4_LOOKBACK = 20

# Sweep active windows
DAILY_WINDOW_HOURS = 12
H4_WINDOW_HOURS = 6

# Binance API delay
REQUEST_DELAY = 0.25

# NTFY
NTFY_TOPIC = "ChartMaster786x7k2p9"
NTFY_URL = f"https://ntfy.sh/{NTFY_TOPIC}"

# State
STATE_FILE = os.path.join(
    os.path.dirname(os.path.abspath(__file__)),
    "scanner_state.json"
)

# Binance Futures
BASE_URL = "https://fapi.binance.com"


# ============================================================
# GLOBAL DIAGNOSTICS
# ============================================================

STATS = {
    "symbols_total": 0,
    "symbols_scanned": 0,
    "api_errors": 0,
    "empty_data": 0,

    "symbols_with_daily_sweep": 0,
    "symbols_with_h4_sweep": 0,
    "symbols_with_any_sweep": 0,

    "active_sweeps": 0,

    "volume_rejected": 0,
    "rsi_rejected": 0,
    "risk_rejected": 0,

    "signals_found": 0,
    "duplicate_signals": 0,

    "ntfy_success": 0,
    "ntfy_failed": 0,
}


# ============================================================
# RESET STATS
# ============================================================

def reset_stats():
    for key in STATS:
        STATS[key] = 0


# ============================================================
# GET ALL BINANCE USDT PERPETUAL SYMBOLS
# ============================================================

def get_all_symbols(max_symbols=MAX_SYMBOLS):

    url = f"{BASE_URL}/fapi/v1/exchangeInfo"

    try:
        r = requests.get(url, timeout=20)
        r.raise_for_status()
        data = r.json()

    except Exception as e:
        print("exchangeInfo error:", e)
        STATS["api_errors"] += 1
        return []

    symbols = []

    for s in data.get("symbols", []):

        if (
            s.get("status") == "TRADING"
            and s.get("contractType") == "PERPETUAL"
            and s.get("quoteAsset") == "USDT"
        ):
            symbols.append(s["symbol"])

    # Alphabetical order gives predictable scanning
    symbols = sorted(symbols)

    return symbols[:max_symbols]


# ============================================================
# GET KLINES
# ============================================================

def get_klines(symbol, interval, limit=150):

    url = f"{BASE_URL}/fapi/v1/klines"

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    try:

        r = requests.get(
            url,
            params=params,
            timeout=15
        )

        r.raise_for_status()

        data = r.json()

    except Exception as e:

        print(f"\n{symbol} {interval} API error: {e}")

        STATS["api_errors"] += 1

        return pd.DataFrame()

    if not isinstance(data, list) or len(data) == 0:

        STATS["empty_data"] += 1

        return pd.DataFrame()

    df = pd.DataFrame(
        data,
        columns=[
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time",
            "quote_volume",
            "trades",
            "buy_base",
            "buy_quote",
            "ignore"
        ]
    )

    # --------------------------------------------------------
    # IMPORTANT:
    # Keep candle OPEN time separately.
    # close_time tells us when candle actually closed.
    # --------------------------------------------------------

    df["time"] = pd.to_datetime(
        df["time"],
        unit="ms"
    )

    df["close_time"] = pd.to_datetime(
        df["close_time"],
        unit="ms"
    )

    for c in [
        "open",
        "high",
        "low",
        "close",
        "volume"
    ]:
        df[c] = pd.to_numeric(
            df[c],
            errors="coerce"
        )

    df = df[
        [
            "time",
            "open",
            "high",
            "low",
            "close",
            "volume",
            "close_time"
        ]
    ]

    # --------------------------------------------------------
    # REMOVE CURRENT INCOMPLETE CANDLE
    # --------------------------------------------------------

    now_utc = pd.Timestamp.now(tz="UTC").tz_localize(None)

    df = df[
        df["close_time"] <= now_utc
    ].copy()

    return df.reset_index(drop=True)


# ============================================================
# INDICATORS
# ============================================================

def add_indicators(df):

    df = df.copy()

    # RSI
    delta = df["close"].diff()

    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)

    avg_gain = gain.ewm(
        alpha=1 / RSI_PERIOD,
        min_periods=RSI_PERIOD,
        adjust=False
    ).mean()

    avg_loss = loss.ewm(
        alpha=1 / RSI_PERIOD,
        min_periods=RSI_PERIOD,
        adjust=False
    ).mean()

    rs = avg_gain / avg_loss

    df["rsi"] = 100 - (
        100 / (1 + rs)
    )

    # Volume average
    df["vol_avg"] = (
        df["volume"]
        .rolling(VOL_PERIOD)
        .mean()
    )

    df["vol_ok"] = (
        df["volume"]
        >=
        df["vol_avg"] * VOL_MULT
    )

    return df


# ============================================================
# FIND LIQUIDITY SWEEPS
# ============================================================

def find_sweeps(
    df,
    lookback,
    hours,
    tf,
    candle_hours
):

    if df.empty:
        return []

    if len(df) <= lookback:
        return []

    high = df["high"].values
    low = df["low"].values
    close = df["close"].values

    # Previous lookback highs/lows
    old_high = (
        df["high"]
        .rolling(
            lookback,
            min_periods=lookback
        )
        .max()
        .shift(1)
        .values
    )

    old_low = (
        df["low"]
        .rolling(
            lookback,
            min_periods=lookback
        )
        .min()
        .shift(1)
        .values
    )

    # --------------------------------------------------------
    # HIGH SWEEP
    #
    # Price takes previous high
    # but closes back below it
    # --------------------------------------------------------

    high_mask = (
        (high > old_high)
        &
        (close < old_high * (1 - BUFFER))
    )

    # --------------------------------------------------------
    # LOW SWEEP
    #
    # Price takes previous low
    # but closes back above it
    # --------------------------------------------------------

    low_mask = (
        (low < old_low)
        &
        (close > old_low * (1 + BUFFER))
    )

    high_mask = np.nan_to_num(
        high_mask,
        nan=False
    ).astype(bool)

    low_mask = np.nan_to_num(
        low_mask,
        nan=False
    ).astype(bool)

    sweeps = []

    delta = pd.Timedelta(
        hours=hours
    )

    candle_duration = pd.Timedelta(
        hours=candle_hours
    )

    # --------------------------------------------------------
    # CRITICAL NO LOOK-AHEAD RULE
    #
    # Sweep becomes known ONLY AFTER
    # the sweep candle has CLOSED.
    #
    # Therefore:
    #
    # sweep_time = candle OPEN + candle duration
    #
    # NOT candle OPEN.
    # --------------------------------------------------------

    for i in np.where(high_mask)[0]:

        sweep_open = df["time"].iloc[i]

        sweep_close = (
            sweep_open
            + candle_duration
        )

        sweeps.append({
            "time": sweep_close,
            "type": "HIGH",
            "extreme": float(high[i]),
            "until": sweep_close + delta,
            "tf": tf
        })

    for i in np.where(low_mask)[0]:

        sweep_open = df["time"].iloc[i]

        sweep_close = (
            sweep_open
            + candle_duration
        )

        sweeps.append({
            "time": sweep_close,
            "type": "LOW",
            "extreme": float(low[i]),
            "until": sweep_close + delta,
            "tf": tf
        })

    sweeps.sort(
        key=lambda x: x["time"]
    )

    return sweeps


# ============================================================
# CHECK LATEST 30M SIGNAL
# ============================================================

def check_latest_signal(
    symbol,
    entry_df,
    sweeps
):

    if entry_df.empty:
        return None

    if len(entry_df) < VOL_PERIOD + 2:
        return None

    # Latest CLOSED 30m candle
    last = entry_df.iloc[-1]

    candle_open = last["time"]
    candle_close = last["close_time"]

    rsi = last["rsi"]

    # --------------------------------------------------------
    # Indicator data unavailable
    # --------------------------------------------------------

    if pd.isna(rsi):

        return None

    if pd.isna(last["vol_avg"]):

        return None

    # --------------------------------------------------------
    # Volume filter
    # --------------------------------------------------------

    if not bool(last["vol_ok"]):

        STATS["volume_rejected"] += 1

        return None

    # --------------------------------------------------------
    # IMPORTANT:
    #
    # Signal candle can only use a sweep that was already
    # completely closed BEFORE the signal candle opened.
    #
    # This prevents look-ahead.
    # --------------------------------------------------------

    active = [

        s for s in sweeps

        if (
            s["time"] <= candle_open
            and
            candle_open <= s["until"]
        )

    ]

    if not active:

        return None

    STATS["active_sweeps"] += len(active)

    # Newest sweep first
    active.sort(
        key=lambda s: s["time"],
        reverse=True
    )

    # --------------------------------------------------------
    # Try newest active sweep
    # --------------------------------------------------------

    for sweep in active:

        # ====================================================
        # LOW SWEEP -> LONG
        # ====================================================

        if sweep["type"] == "LOW":

            if rsi > RSI_OS:

                STATS["rsi_rejected"] += 1

                continue

            entry = float(last["close"])

            sl = (
                sweep["extreme"]
                * (1 - BUFFER)
            )

            risk = entry - sl

            if risk <= 0:

                STATS["risk_rejected"] += 1

                continue

            tps = {
                rr: entry + risk * rr
                for rr in RR_LIST
            }

            STATS["signals_found"] += 1

            return {
                "time": candle_open,
                "candle_close": candle_close,
                "side": "LONG",
                "entry": entry,
                "sl": sl,
                "tps": tps,
                "sweep_tf": sweep["tf"],
                "sweep_time": sweep["time"],
                "sweep_type": "LOW"
            }

        # ====================================================
        # HIGH SWEEP -> SHORT
        # ====================================================

        else:

            if rsi < RSI_OB:

                STATS["rsi_rejected"] += 1

                continue

            entry = float(last["close"])

            sl = (
                sweep["extreme"]
                * (1 + BUFFER)
            )

            risk = sl - entry

            if risk <= 0:

                STATS["risk_rejected"] += 1

                continue

            tps = {
                rr: entry - risk * rr
                for rr in RR_LIST
            }

            STATS["signals_found"] += 1

            return {
                "time": candle_open,
                "candle_close": candle_close,
                "side": "SHORT",
                "entry": entry,
                "sl": sl,
                "tps": tps,
                "sweep_tf": sweep["tf"],
                "sweep_time": sweep["time"],
                "sweep_type": "HIGH"
            }

    return None


# ============================================================
# STATE
# ============================================================

def load_state():

    if not os.path.exists(STATE_FILE):

        return {}

    try:

        with open(
            STATE_FILE,
            "r",
            encoding="utf-8"
        ) as f:

            return json.load(f)

    except Exception as e:

        print(
            "State file read error:",
            e
        )

        return {}


def save_state(state):

    try:

        with open(
            STATE_FILE,
            "w",
            encoding="utf-8"
        ) as f:

            json.dump(
                state,
                f,
                indent=2
            )

        return True

    except Exception as e:

        print(
            "State save error:",
            e
        )

        return False


# ============================================================
# NTFY
# ============================================================

def send_ntfy(
    symbol,
    signal
):

    side = signal["side"]

    entry = signal["entry"]

    sl = signal["sl"]

    tps = signal["tps"]

    tp_lines = "\n".join(
        f"TP (1:{rr}): {tp:.8f}"
        for rr, tp in sorted(
            tps.items()
        )
    )

    title = (
        f"{symbol} {side} SIGNAL"
    )

    message = (
        f"Symbol: {symbol}\n"
        f"Side: {side}\n"
        f"Entry: {entry:.8f}\n"
        f"SL: {sl:.8f}\n"
        f"{tp_lines}\n"
        f"Sweep TF: {signal['sweep_tf']}\n"
        f"Sweep Type: {signal['sweep_type']}\n"
        f"Sweep Closed: {signal['sweep_time']}\n"
        f"30m Candle Open: {signal['time']}\n"
        f"30m Candle Close: {signal['candle_close']}\n"
    )

    try:

        response = requests.post(

            NTFY_URL,

            data=message.encode(
                "utf-8"
            ),

            headers={
                "Title": title,
                "Priority": "high",
                "Tags": (
                    "chart_with_upwards_trend"
                    if side == "LONG"
                    else
                    "chart_with_downwards_trend"
                )
            },

            timeout=15
        )

        response.raise_for_status()

        STATS["ntfy_success"] += 1

        print(
            f"\n  NTFY SUCCESS: "
            f"{symbol} {side}"
        )

        return True

    except Exception as e:

        STATS["ntfy_failed"] += 1

        print(
            f"\n  NTFY FAILED: "
            f"{symbol} | {e}"
        )

        return False


# ============================================================
# DIAGNOSTIC SUMMARY
# ============================================================

def print_stats():

    print("\n")
    print("=" * 65)
    print("SCAN DIAGNOSTICS")
    print("=" * 65)

    print(
        f"Symbols total:          "
        f"{STATS['symbols_total']}"
    )

    print(
        f"Symbols scanned:        "
        f"{STATS['symbols_scanned']}"
    )

    print(
        f"API errors:             "
        f"{STATS['api_errors']}"
    )

    print(
        f"Empty data:             "
        f"{STATS['empty_data']}"
    )

    print(
        f"Daily sweep symbols:    "
        f"{STATS['symbols_with_daily_sweep']}"
    )

    print(
        f"4H sweep symbols:       "
        f"{STATS['symbols_with_h4_sweep']}"
    )

    print(
        f"Any sweep symbols:      "
        f"{STATS['symbols_with_any_sweep']}"
    )

    print(
        f"Active sweeps:          "
        f"{STATS['active_sweeps']}"
    )

    print(
        f"Volume rejected:        "
        f"{STATS['volume_rejected']}"
    )

    print(
        f"RSI rejected:           "
        f"{STATS['rsi_rejected']}"
    )

    print(
        f"Risk rejected:          "
        f"{STATS['risk_rejected']}"
    )

    print(
        f"Signals found:          "
        f"{STATS['signals_found']}"
    )

    print(
        f"Duplicate signals:      "
        f"{STATS['duplicate_signals']}"
    )

    print(
        f"NTFY success:            "
        f"{STATS['ntfy_success']}"
    )

    print(
        f"NTFY failed:             "
        f"{STATS['ntfy_failed']}"
    )

    print("=" * 65)


# ============================================================
# SCAN ONE CYCLE
# ============================================================

def run_scan_cycle(
    symbols,
    state
):

    reset_stats()

    new_signals_count = 0

    STATS["symbols_total"] = len(
        symbols
    )

    for idx, symbol in enumerate(
        symbols,
        start=1
    ):

        print(
            f"[{idx}/{len(symbols)}] "
            f"Checking {symbol}",
            end="\r",
            flush=True
        )

        # ----------------------------------------------------
        # Daily
        # ----------------------------------------------------

        daily = get_klines(
            symbol,
            "1d",
            limit=max(
                DAILY_LOOKBACK + 5,
                30
            )
        )

        time.sleep(
            REQUEST_DELAY
        )

        # ----------------------------------------------------
        # 4H
        # ----------------------------------------------------

        h4 = get_klines(
            symbol,
            "4h",
            limit=max(
                H4_LOOKBACK + 10,
                60
            )
        )

        time.sleep(
            REQUEST_DELAY
        )

        # ----------------------------------------------------
        # 30m
        # ----------------------------------------------------

        entry_df = get_klines(
            symbol,
            ENTRY_TIMEFRAME,
            limit=60
        )

        time.sleep(
            REQUEST_DELAY
        )

        if (
            daily.empty
            or
            h4.empty
            or
            entry_df.empty
        ):

            continue

        STATS["symbols_scanned"] += 1

        # ----------------------------------------------------
        # Find Daily sweeps
        # ----------------------------------------------------

        daily_sweeps = find_sweeps(

            daily,

            DAILY_LOOKBACK,

            DAILY_WINDOW_HOURS,

            "1D",

            candle_hours=24
        )

        # ----------------------------------------------------
        # Find 4H sweeps
        # ----------------------------------------------------

        h4_sweeps = find_sweeps(

            h4,

            H4_LOOKBACK,

            H4_WINDOW_HOURS,

            "4H",

            candle_hours=4
        )

        if daily_sweeps:

            STATS[
                "symbols_with_daily_sweep"
            ] += 1

        if h4_sweeps:

            STATS[
                "symbols_with_h4_sweep"
            ] += 1

        sweeps = (
            daily_sweeps
            +
            h4_sweeps
        )

        if not sweeps:

            continue

        STATS
        
