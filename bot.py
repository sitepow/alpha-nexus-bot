#!/usr/bin/env python3
"""
bot.py — Single-file SMC + Price Action scalper.

Structure:
    H4  → bias (HH/HL or LH/LL + EMA200 filter)
    M15 → find Fresh Order Block + FVG in discount/premium
    M5  → wait for liquidity sweep + ChoCH confirmation

Safety:
    - One position per symbol max
    - Circuit breaker on 3 consecutive losses
    - Daily loss guard
    - Max daily trades
    - Telegram alerts on every entry/exit/error

Run:
    python bot.py

State file:
    bot_state.json (auto-created in same dir)
"""

import json
import math
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path

import requests
from dotenv import load_dotenv
from mt5linux import MetaTrader5

# =============================================================
# CONFIG — edit these to tune the bot
# =============================================================

SYMBOLS = ["EURUSDm", "GBPUSDm", "USDJPYm", "XAUUSDm"]
MAGIC = 990011

POLL_SECONDS       = 15
RISK_PER_TRADE_USD = 5.0       # risk per trade in account currency
MAX_SPREAD_POINTS  = 30
MAX_OPEN_POSITIONS = 2

# Safety rails
MAX_DAILY_TRADES       = 6
MAX_DAILY_LOSS_PCT     = 0.03
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_SECONDS       = 3600  # after consecutive losses

# SMC params
BIAS_EMA_PERIOD        = 200   # H4
OB_LOOKBACK_BARS       = 50    # M15
FVG_LOOKBACK_BARS      = 30    # M15
LIQUIDITY_SWING_WINDOW = 5     # swing detection window
LIQUIDITY_SWING_AGE    = 30    # max bars back for swing reference
SWEEP_MIN_ATR          = 0.25  # M5 sweep must pierce swing by this ATR
CHoCH_LOOKBACK         = 12    # M5 bars back for CHoCH pivot

# SL / TP
SL_BUFFER_ATR = 0.30           # place SL beyond OB/FVG + this buffer
TP1_RR        = 1.5
TP2_RR        = 2.5

# Timing
SESSION_START_HOUR_UTC = 6
SESSION_END_HOUR_UTC   = 20

# =============================================================
# ENVIRONMENT
# =============================================================

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

STATE_FILE = ROOT / "bot_state.json"

TELEGRAM_TOKEN   = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

MT5_HOST     = os.getenv("MT5_HOST", "localhost")
MT5_PORT     = int(os.getenv("MT5_PORT", "8002"))
MT5_LOGIN    = int(os.getenv("MT5_LOGIN", "0") or "0")
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER   = os.getenv("MT5_SERVER", "")

# =============================================================
# UTILITIES
# =============================================================

def now_utc() -> datetime:
    return datetime.now(timezone.utc)


def log(msg: str) -> None:
    ts = now_utc().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)


def send_telegram(text: str) -> None:
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text,
                "parse_mode": "HTML",
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
    except Exception as e:
        log(f"telegram error: {e}")


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            return json.loads(STATE_FILE.read_text())
        except Exception:
            pass
    return {
        "utc_day": now_utc().date().isoformat(),
        "day_start_balance": None,
        "daily_trades": 0,
        "consecutive_losses": 0,
        "cooldown_until": 0.0,
        "open_tickets": {},       # ticket -> {symbol, entry, risk, sl, tp, opened_at, side}
        "recent_closed": [],      # last 20 closed trades
    }


def save_state(state: dict) -> None:
    try:
        STATE_FILE.write_text(json.dumps(state, indent=2))
    except Exception as e:
        log(f"save_state failed: {e}")


def reset_daily_if_needed(state: dict, balance: float) -> None:
    today = now_utc().date().isoformat()
    if state.get("utc_day") != today:
        state["utc_day"] = today
        state["day_start_balance"] = balance
        state["daily_trades"] = 0
        state["consecutive_losses"] = 0
        state["cooldown_until"] = 0.0
        save_state(state)


# =============================================================
# MT5 WRAPPERS
# =============================================================

def mt5_remote(mt5, expr: str):
    """Evaluate Python expression on remote MT5 server via mt5linux."""
    return mt5._container.eval(expr)


def get_account(mt5) -> dict:
    acct = mt5_remote(mt5, "mt5.account_info()._asdict()")
    return acct


def get_symbol_info(mt5, symbol: str) -> dict:
    return mt5_remote(
        mt5, f"mt5.symbol_info({symbol!r})._asdict()"
    )


def get_tick(mt5, symbol: str) -> dict:
    return mt5_remote(
        mt5, f"mt5.symbol_info_tick({symbol!r})._asdict()"
    )


def get_positions(mt5, symbol: str = None) -> list:
    if symbol:
        raw = mt5_remote(
            mt5,
            f"[p._asdict() for p in mt5.positions_get(symbol={symbol!r}) or []]",
        )
    else:
        raw = mt5_remote(
            mt5, "[p._asdict() for p in mt5.positions_get() or []]"
        )
    return [p for p in raw if int(p.get("magic", 0)) == MAGIC]


def get_bars(mt5, symbol: str, timeframe: int, count: int) -> list[dict]:
    """Fetch closed bars as list of dicts (oldest → newest).
    
    Uses len() check (not `or []`) because copy_rates_from_pos returns
    numpy structured array — truth value is ambiguous.
    """
    expr = (
        f"(lambda arr: "
        f"  [] if arr is None or len(arr) == 0 "
        f"  else [dict(zip(['time','open','high','low','close',"
        f"'tick_volume','spread','real_volume'], "
        f"    [t[0], t[1], t[2], t[3], t[4], t[5], t[6], t[7]])) "
        f"    for t in arr]"
        f")(mt5.copy_rates_from_pos({symbol!r}, {timeframe}, 1, {count}))"
    )
    return mt5_remote(mt5, expr)

def send_order(mt5, request: dict) -> dict:
    """Returns {retcode, order, deal, volume, price} or None."""
    return mt5_remote(
        mt5,
        f"(lambda r: None if r is None else dict("
        f"retcode=int(r.retcode), order=int(r.order), deal=int(r.deal), "
        f"volume=float(r.volume), price=float(r.price)))(mt5.order_send({request!r}))",
    )


def close_position(mt5, position: dict, reason: str) -> bool:
    info  = get_symbol_info(mt5, position["symbol"])
    tick  = get_tick(mt5, position["symbol"])
    is_buy = int(position["type"]) == 0
    price  = float(tick["bid"] if is_buy else tick["ask"])
    point  = float(info["point"])

    filling = choose_filling(info)
    if filling is None:
        return False

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       position["symbol"],
        "position":     int(position["ticket"]),
        "volume":       float(position["volume"]),
        "type":         int(mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY),
        "price":        price,
        "deviation":    int(max(20, (float(tick["ask"]) - float(tick["bid"])) / point * 2)),
        "magic":        MAGIC,
        "comment":      reason,
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": int(filling),
    }
    res = send_order(mt5, req)
    return bool(res and int(res["retcode"]) == int(mt5.TRADE_RETCODE_DONE))


# =============================================================
# TECHNICAL HELPERS
# =============================================================

def ema(values: list[float], period: int) -> list[float]:
    if not values:
        return []
    k = 2.0 / (period + 1.0)
    out = [values[0]]
    for v in values[1:]:
        out.append(out[-1] + k * (v - out[-1]))
    return out


def atr(bars: list[dict], period: int = 14) -> list[float]:
    trs = []
    for i, b in enumerate(bars):
        if i == 0:
            trs.append(float(b["high"]) - float(b["low"]))
        else:
            prev_close = float(bars[i - 1]["close"])
            trs.append(max(
                float(b["high"]) - float(b["low"]),
                abs(float(b["high"]) - prev_close),
                abs(float(b["low"]) - prev_close),
            ))
    return ema(trs, period)


def choose_filling(info: dict):
    flags = int(info.get("filling_mode", 0))
    if flags & 1:
        return 1  # FOK
    if flags & 2:
        return 2  # IOC
    return 0      # RETURN


def round_price(price: float, info: dict, upward: bool = False) -> float:
    step = float(info.get("trade_tick_size") or info["point"])
    digits = int(info["digits"])
    units = price / step
    units = math.ceil(units - 1e-9) if upward else math.floor(units + 1e-9)
    return round(units * step, digits)


def get_spread_points(tick: dict, info: dict) -> float:
    return (float(tick["ask"]) - float(tick["bid"])) / float(info["point"])


# =============================================================
# MARKET STRUCTURE  (SMC building blocks)
# =============================================================

def detect_swings(bars: list[dict], window: int = 5) -> tuple[list, list]:
    """Returns (swing_highs, swing_lows) with .index .price .time."""
    highs, lows = [], []
    for i in range(window, len(bars) - window):
        window_highs = [float(bars[j]["high"]) for j in range(i - window, i + window + 1)]
        window_lows  = [float(bars[j]["low"])  for j in range(i - window, i + window + 1)]
        if float(bars[i]["high"]) == max(window_highs):
            highs.append({"index": i, "price": float(bars[i]["high"]), "time": bars[i]["time"]})
        if float(bars[i]["low"]) == min(window_lows):
            lows.append({"index": i, "price": float(bars[i]["low"]), "time": bars[i]["time"]})
    return highs, lows


def htf_bias(h4_bars: list[dict]) -> tuple[str, str]:
    """Returns (bias, reason). bias in {BUY, SELL, NEUTRAL}."""
    if len(h4_bars) < BIAS_EMA_PERIOD + 5:
        return "NEUTRAL", "insufficient_bars"

    closes = [float(b["close"]) for b in h4_bars]
    ema200 = ema(closes, BIAS_EMA_PERIOD)[-1]
    price  = closes[-1]

    highs, lows = detect_swings(h4_bars, window=5)
    if len(highs) < 2 or len(lows) < 2:
        return "NEUTRAL", "insufficient_swings"

    hh = highs[-1]["price"] > highs[-2]["price"]
    hl = lows[-1]["price"]  > lows[-2]["price"]
    lh = highs[-1]["price"] < highs[-2]["price"]
    ll = lows[-1]["price"]  < lows[-2]["price"]

    if hh and hl and price > ema200:
        return "BUY", "HH+HL & price>EMA200"
    if lh and ll and price < ema200:
        return "SELL", "LH+LL & price<EMA200"
    return "NEUTRAL", f"structure ambiguous (hh={hh} hl={hl} lh={lh} ll={ll})"


def find_order_blocks(m15_bars: list[dict], bias: str, atr_vals: list[float]) -> list[dict]:
    """
    Order Block (simplified SMC):
      - BUY OB : last BEARISH candle before a strong bullish move that breaks structure
      - SELL OB: last BULLISH candle before a strong bearish move
    We only return 'fresh' OBs (not touched since creation).
    """
    if len(m15_bars) < OB_LOOKBACK_BARS + 5:
        return []

    obs = []
    lookback = min(OB_LOOKBACK_BARS, len(m15_bars) - 5)
    for i in range(len(m15_bars) - lookback, len(m15_bars) - 1):
        curr = m15_bars[i]
        prev = m15_bars[i - 1]
        nxt  = m15_bars[i + 1]

        o, c = float(curr["open"]), float(curr["close"])
        body = abs(c - o)
        atr_i = float(atr_vals[i]) if i < len(atr_vals) else 0.0
        if atr_i <= 0 or body < 0.4 * atr_i:
            continue

        is_bearish_ob = (c < o) and (float(nxt["close"]) > float(nxt["open"]))
        is_bullish_ob = (c > o) and (float(nxt["close"]) < float(nxt["open"]))

        if bias == "BUY" and is_bearish_ob:
            # must break previous swing high
            prior_highs = [float(b["high"]) for b in m15_bars[max(0, i - 10):i]]
            if prior_highs and float(nxt["high"]) > max(prior_highs):
                ob = {
                    "type": "bull",
                    "top":    max(o, c),
                    "bottom": min(o, c),
                    "index":  i,
                    "time":   curr["time"],
                }
                obs.append(ob)

        if bias == "SELL" and is_bullish_ob:
            prior_lows = [float(b["low"]) for b in m15_bars[max(0, i - 10):i]]
            if prior_lows and float(nxt["low"]) < min(prior_lows):
                ob = {
                    "type": "bear",
                    "top":    max(o, c),
                    "bottom": min(o, c),
                    "index":  i,
                    "time":   curr["time"],
                }
                obs.append(ob)

    # keep only fresh OBs (price hasn't returned into zone after creation)
    fresh = []
    for ob in obs:
        tested = False
        for j in range(ob["index"] + 2, len(m15_bars)):
            bar = m15_bars[j]
            if ob["type"] == "bull" and float(bar["low"]) <= ob["top"]:
                tested = True
                break
            if ob["type"] == "bear" and float(bar["high"]) >= ob["bottom"]:
                tested = True
                break
        if not tested:
            fresh.append(ob)

    return fresh[-3:]  # newest up to 3


def find_fair_value_gaps(m15_bars: list[dict], bias: str) -> list[dict]:
    """FVG = 3-candle gap where wick of bar[i-1] and bar[i+1] don't overlap."""
    if len(m15_bars) < FVG_LOOKBACK_BARS + 3:
        return []

    fvgs = []
    start = max(1, len(m15_bars) - FVG_LOOKBACK_BARS)
    for i in range(start, len(m15_bars) - 1):
        b1 = m15_bars[i - 1]
        b3 = m15_bars[i + 1]

        # Bullish FVG (b3.low > b1.high) — imbalance up
        if float(b3["low"]) > float(b1["high"]):
            fvgs.append({
                "type": "bull",
                "top":    float(b3["low"]),
                "bottom": float(b1["high"]),
                "index":  i,
                "time":   m15_bars[i]["time"],
            })

        # Bearish FVG (b3.high < b1.low)
        if float(b3["high"]) < float(b1["low"]):
            fvgs.append({
                "type": "bear",
                "top":    float(b1["low"]),
                "bottom": float(b3["high"]),
                "index":  i,
                "time":   m15_bars[i]["time"],
            })

    # keep only unfilled FVGs
    unfilled = []
    for fvg in fvgs:
        filled = False
        for j in range(fvg["index"] + 2, len(m15_bars)):
            bar = m15_bars[j]
            if fvg["type"] == "bull" and float(bar["low"]) <= fvg["bottom"]:
                filled = True
                break
            if fvg["type"] == "bear" and float(bar["high"]) >= fvg["top"]:
                filled = True
                break
        if not filled:
            unfilled.append(fvg)

    if bias == "BUY":
        return [f for f in unfilled if f["type"] == "bull"][-3:]
    if bias == "SELL":
        return [f for f in unfilled if f["type"] == "bear"][-3:]
    return []


def find_zone(m15_bars: list[dict], bias: str, atr_vals: list[float]) -> dict | None:
    """Return best entry zone = closest OB/FVG that price is approaching."""
    obs  = find_order_blocks(m15_bars, bias, atr_vals)
    fvgs = find_fair_value_gaps(m15_bars, bias)

    price = float(m15_bars[-1]["close"])
    candidates = []

    for ob in obs:
        candidates.append({**ob, "kind": "OB"})
    for fvg in fvgs:
        candidates.append({**fvg, "kind": "FVG"})

    if not candidates:
        return None

    # Prefer the one closest to current price (on correct side)
    if bias == "BUY":
        candidates = [c for c in candidates if c["top"] < price]
        if not candidates:
            return None
        candidates.sort(key=lambda c: price - c["top"])
    else:
        candidates = [c for c in candidates if c["bottom"] > price]
        if not candidates:
            return None
        candidates.sort(key=lambda c: c["bottom"] - price)

    return candidates[0]


def detect_liquidity_sweep(m5_bars: list[dict], bias: str, atr_vals: list[float]) -> dict | None:
    """
    BUY sweep:  recent swing low was pierced by wick but bar closed back above.
    SELL sweep: recent swing high was pierced by wick but bar closed back below.
    """
    if len(m5_bars) < LIQUIDITY_SWING_AGE + 5:
        return None

    highs, lows = detect_swings(m5_bars, window=LIQUIDITY_SWING_WINDOW)
    cutoff = len(m5_bars) - LIQUIDITY_SWING_AGE

    recent_highs = [h for h in highs if h["index"] >= cutoff]
    recent_lows  = [l for l in lows  if l["index"] >= cutoff]
    if not recent_highs or not recent_lows:
        return None

    last_bar = m5_bars[-1]
    atr_now  = float(atr_vals[-1]) if atr_vals else 0.0
    if atr_now <= 0:
        return None

    if bias == "BUY":
        ref_low = min(recent_lows, key=lambda x: x["price"])
        pierced = float(last_bar["low"]) < ref_low["price"] - SWEEP_MIN_ATR * atr_now
        closed_back = float(last_bar["close"]) > ref_low["price"]
        if pierced and closed_back:
            return {
                "type": "bull_sweep",
                "ref_price": ref_low["price"],
                "sweep_price": float(last_bar["low"]),
            }

    if bias == "SELL":
        ref_high = max(recent_highs, key=lambda x: x["price"])
        pierced = float(last_bar["high"]) > ref_high["price"] + SWEEP_MIN_ATR * atr_now
        closed_back = float(last_bar["close"]) < ref_high["price"]
        if pierced and closed_back:
            return {
                "type": "bear_sweep",
                "ref_price": ref_high["price"],
                "sweep_price": float(last_bar["high"]),
            }

    return None


def detect_choch(m5_bars: list[dict], bias: str) -> dict | None:
    """
    CHoCH on M5: after a sweep, price must break the most recent swing point
    in the bias direction (confirmation of reversal).
    """
    if len(m5_bars) < CHoCH_LOOKBACK + 3:
        return None

    lookback_bars = m5_bars[-CHoCH_LOOKBACK:]
    highs, lows = detect_swings(lookback_bars, window=2)
    if not highs or not lows:
        return None

    last_close = float(m5_bars[-1]["close"])

    if bias == "BUY":
        # need break above last swing high
        last_high = max(highs, key=lambda x: x["index"])
        if last_close > last_high["price"]:
            return {"broken_level": last_high["price"], "direction": "BUY"}

    if bias == "SELL":
        last_low = max(lows, key=lambda x: x["index"])
        if last_close < last_low["price"]:
            return {"broken_level": last_low["price"], "direction": "SELL"}

    return None


# =============================================================
# POSITION SIZING
# =============================================================

def calc_lot_size(info: dict, balance: float, sl_distance: float, risk_usd: float) -> float:
    point       = float(info["point"])
    tick_value  = float(info.get("trade_tick_value", 1.0))
    tick_size   = float(info.get("trade_tick_size", point)) or point
    per_lot_loss = (sl_distance / point) * (tick_value / tick_size * point)
    if per_lot_loss <= 0:
        return 0.0

    raw = risk_usd / per_lot_loss
    step = float(info["volume_step"])
    vmin = float(info["volume_min"])
    vmax = float(info["volume_max"])
    lot = math.floor(raw / step) * step
    if lot < vmin:
        lot = vmin
    return min(lot, vmax, 5.0)  # sanity cap


# =============================================================
# SCAN PIPELINE
# =============================================================

def scan_symbol(mt5, symbol: str) -> dict | None:
    """Returns candidate dict or None."""
    info = get_symbol_info(mt5, symbol)
    if info is None:
        return None
    mt5_remote(mt5, f"mt5.symbol_select({symbol!r}, True)")

    # --- 1. HTF bias ---
    h4_bars = get_bars(mt5, symbol, mt5.TIMEFRAME_H4, BIAS_EMA_PERIOD + 60)
    if not h4_bars:
        return None
    bias, bias_reason = htf_bias(h4_bars)
    if bias == "NEUTRAL":
        return None

    # --- 2. M15 zone ---
    m15_bars = get_bars(mt5, symbol, mt5.TIMEFRAME_M15, OB_LOOKBACK_BARS + FVG_LOOKBACK_BARS + 10)
    if len(m15_bars) < 40:
        return None
    m15_atr = atr(m15_bars, 14)
    zone = find_zone(m15_bars, bias, m15_atr)
    if zone is None:
        return None

    # --- 3. Check price inside/near zone ---
    tick = get_tick(mt5, symbol)
    price = float(tick["ask"] if bias == "BUY" else tick["bid"])
    zone_top    = zone["top"]
    zone_bottom = zone["bottom"]
    zone_height = zone_top - zone_bottom

    # Only enter when price is INSIDE the zone (or within 0.3 * zone_height)
    if bias == "BUY":
        if not (zone_bottom - 0.3 * zone_height <= price <= zone_top):
            return None
    else:
        if not (zone_bottom <= price <= zone_top + 0.3 * zone_height):
            return None

    # --- 4. M5 sweep + CHoCH confirmation ---
    m5_bars = get_bars(mt5, symbol, mt5.TIMEFRAME_M5, 100)
    if len(m5_bars) < 40:
        return None
    m5_atr = atr(m5_bars, 14)

    sweep = detect_liquidity_sweep(m5_bars, bias, m5_atr)
    if not sweep:
        return None

    choch = detect_choch(m5_bars, bias)
    if not choch:
        return None

    # --- 5. Spread sanity ---
    spread_points = get_spread_points(tick, info)
    if spread_points > MAX_SPREAD_POINTS:
        return None

    return {
        "symbol":      symbol,
        "bias":        bias,
        "bias_reason": bias_reason,
        "zone":        zone,
        "sweep":       sweep,
        "choch":       choch,
        "price":       price,
        "atr_m5":      float(m5_atr[-1]),
        "info":        info,
        "tick":        tick,
        "spread_pts":  spread_points,
    }


def build_order(mt5, candidate: dict, balance: float) -> dict | None:
    symbol = candidate["symbol"]
    bias   = candidate["bias"]
    info   = candidate["info"]
    price  = candidate["price"]
    zone   = candidate["zone"]
    atr_m5 = candidate["atr_m5"]

    buffer = SL_BUFFER_ATR * atr_m5
    if bias == "BUY":
        sl_raw = zone["bottom"] - buffer
        sl = round_price(sl_raw, info, upward=False)
        risk_distance = price - sl
        if risk_distance <= 0:
            return None
        tp1 = round_price(price + TP1_RR * risk_distance, info, upward=True)
        tp2 = round_price(price + TP2_RR * risk_distance, info, upward=True)
        order_type = int(mt5.ORDER_TYPE_BUY)
    else:
        sl_raw = zone["top"] + buffer
        sl = round_price(sl_raw, info, upward=True)
        risk_distance = sl - price
        if risk_distance <= 0:
            return None
        tp1 = round_price(price - TP1_RR * risk_distance, info, upward=False)
        tp2 = round_price(price - TP2_RR * risk_distance, info, upward=False)
        order_type = int(mt5.ORDER_TYPE_SELL)

    # Sanity: cap SL distance
    max_risk_dist = 3.0 * atr_m5
    if risk_distance > max_risk_dist:
        log(f"{symbol} SL too wide ({risk_distance/atr_m5:.1f} ATR) — reject")
        return None

    lot = calc_lot_size(info, balance, risk_distance, RISK_PER_TRADE_USD)
    if lot <= 0:
        return None

    filling = choose_filling(info)
    if filling is None:
        return None

    return {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       symbol,
        "volume":       float(lot),
        "type":         order_type,
        "price":        price,
        "sl":           float(sl),
        "tp":           float(tp2),
        "deviation":    int(max(20, candidate["spread_pts"] * 2)),
        "magic":        MAGIC,
        "comment":      f"SMC-{zone['kind']}-{bias}",
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": int(filling),
    }


# =============================================================
# SAFETY CHECKS
# =============================================================

def in_session() -> bool:
    hour = now_utc().hour
    return SESSION_START_HOUR_UTC <= hour < SESSION_END_HOUR_UTC


def can_open_new_position(state: dict, balance: float, open_count: int) -> tuple[bool, str]:
    if open_count >= MAX_OPEN_POSITIONS:
        return False, f"max_positions ({open_count}/{MAX_OPEN_POSITIONS})"
    if state["daily_trades"] >= MAX_DAILY_TRADES:
        return False, f"daily_trades ({state['daily_trades']}/{MAX_DAILY_TRADES})"
    if state["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
        return False, f"consecutive_losses ({state['consecutive_losses']})"
    if time.time() < state.get("cooldown_until", 0):
        mins = int((state["cooldown_until"] - time.time()) / 60)
        return False, f"cooldown ({mins}m left)"

    day_start = state.get("day_start_balance") or balance
    if day_start > 0:
        dd_pct = (day_start - balance) / day_start
        if dd_pct >= MAX_DAILY_LOSS_PCT:
            return False, f"daily_loss ({dd_pct*100:.1f}%)"

    return True, "ok"


def update_state_on_close(state: dict, trade: dict, balance_now: float) -> None:
    pnl = float(trade.get("pnl", 0))
    state["daily_trades"] += 1
    if pnl < 0:
        state["consecutive_losses"] += 1
        if state["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
            state["cooldown_until"] = time.time() + COOLDOWN_SECONDS
            msg = (
                f"🛑 <b>Cooldown activated</b>\n"
                f"{state['consecutive_losses']} consecutive losses\n"
                f"Pause {COOLDOWN_SECONDS//60} min"
            )
            log(f"COOLDOWN: {msg}")
            send_telegram(msg)
    else:
        state["consecutive_losses"] = 0

    state["recent_closed"].append(trade)
    state["recent_closed"] = state["recent_closed"][-20:]
    save_state(state)


# =============================================================
# RECONCILE (match open positions vs state)
# =============================================================

def reconcile_positions(mt5, state: dict) -> None:
    """
    Detects closed positions since last cycle, updates state + sends alert.
    """
    live = get_positions(mt5, symbol=None)
    live_tickets = {str(p["ticket"]) for p in live}

    for ticket in list(state["open_tickets"].keys()):
        if ticket in live_tickets:
            continue

        # Position disappeared — fetch close details from deals
        rec = state["open_tickets"][ticket]
        close_info = fetch_close_deal(mt5, int(ticket))
        if close_info is None:
            log(f"ticket {ticket} closed but no deal — skip update")
            state["open_tickets"].pop(ticket, None)
            save_state(state)
            continue

        pnl = close_info["pnl"]
        reason = close_info["reason"]
        emoji = "✅" if pnl > 0 else "❌"

        msg = (
            f"{emoji} <b>Trade Closed</b>\n"
            f"• Symbol: {rec['symbol']} ({rec['side']})\n"
            f"• Entry: {rec['entry']:.5f}\n"
            f"• Close: {close_info['close_price']:.5f}\n"
            f"• P&L: ${pnl:+.2f}\n"
            f"• Reason: {reason}"
        )
        log(f"CLOSED {ticket} {rec['symbol']} pnl={pnl:+.2f} reason={reason}")
        send_telegram(msg)

        trade = {
            "ticket": ticket,
            "symbol": rec["symbol"],
            "side":   rec["side"],
            "entry":  rec["entry"],
            "close":  close_info["close_price"],
            "pnl":    pnl,
            "reason": reason,
            "closed_at": now_utc().isoformat(),
        }
                # Log + update state
        state["open_tickets"].pop(ticket, None)
        update_state_on_close(state, trade, balance_now=get_account(mt5)["balance"])


def fetch_close_deal(mt5, position_ticket: int) -> dict | None:
    """
    ดึงข้อมูล deal ที่ปิด position นี้ (entry=DEAL_ENTRY_OUT)
    คืน dict {pnl, close_price, reason} หรือ None ถ้าไม่เจอ
    """
    raw = mt5_remote(
        mt5,
        f"[d._asdict() for d in mt5.history_deals_get(position={position_ticket}) or []]",
    )
    if not raw:
        return None

    # หา deal ขาออก (entry == 1 = DEAL_ENTRY_OUT)
    out_deals = [d for d in raw if int(d.get("entry", -1)) == 1]
    if not out_deals:
        return None

    # รวม pnl (บางโบรกแยก commission/swap เป็น deal อีกอัน)
    total_pnl = sum(float(d.get("profit", 0.0)) for d in raw)
    total_pnl += sum(float(d.get("commission", 0.0)) for d in raw)
    total_pnl += sum(float(d.get("swap", 0.0)) for d in raw)

    last = out_deals[-1]
    reason_code = int(last.get("reason", 0))
    reason_map = {
        0: "SL",
        1: "TP",
        2: "SO",       # stop out
        3: "SO",
        4: "TP",
        5: "SL",
        6: "Rollover",
        7: "Rollover",
        8: "Manual",
        9: "Manual",
        10: "Mobile",
        11: "Web",
        12: "Expert",  # EA/bot
    }
    reason = reason_map.get(reason_code, f"code_{reason_code}")

    return {
        "pnl":         total_pnl,
        "close_price": float(last.get("price", 0.0)),
        "reason":      reason,
    }


# =============================================================
# POSITION MANAGEMENT (TP1 partial, BE, trailing)
# =============================================================

def manage_open_positions(mt5, state: dict) -> None:
    """
    จัดการ TP1 / BE / trailing สำหรับ position ที่เปิดอยู่
    - ถ้าราคาไปถึง TP1 → ปิดครึ่ง + ย้าย SL ไป BE
    - ถ้าราคาเลย 1.5 ATR → trailing SL ตาม
    """
    live = get_positions(mt5, symbol=None)
    balance_now = get_account(mt5)["balance"]

    for pos in live:
        ticket = str(pos["ticket"])
        rec = state["open_tickets"].get(ticket)
        if rec is None:
            # position ที่ไม่ได้อยู่ใน state (เช่น เพิ่งเปิดจาก bot อื่น) → บันทึกไว้
            state["open_tickets"][ticket] = {
                "symbol":    pos["symbol"],
                "side":      "BUY" if int(pos["type"]) == 0 else "SELL",
                "entry":     float(pos["price_open"]),
                "risk":      abs(float(pos["price_open"]) - float(pos["sl"])),
                "sl":        float(pos["sl"]),
                "tp":        float(pos["tp"]),
                "opened_at": float(pos["time"]),
                "partial_done": False,
            }
            save_state(state)
            rec = state["open_tickets"][ticket]

        info = get_symbol_info(mt5, pos["symbol"])
        tick = get_tick(mt5, pos["symbol"])
        entry = float(pos["price_open"])
        sl    = float(pos["sl"])
        tp    = float(pos["tp"])
        is_buy = int(pos["type"]) == 0
        price = float(tick["bid"] if is_buy else tick["ask"])

        risk_dist = abs(entry - sl) if sl != 0 else 0.0
        if risk_dist <= 0:
            continue

        # --- TP1 partial (ปิดครึ่งเมื่อถึง 1.5R) ---
        if not rec.get("partial_done", False):
            tp1 = entry + (TP1_RR * risk_dist if is_buy else -TP1_RR * risk_dist)
            hit_tp1 = (is_buy and price >= tp1) or (not is_buy and price <= tp1)
            if hit_tp1:
                vol = float(pos["volume"])
                half = vol / 2.0
                step = float(info["volume_step"])
                half = math.floor(half / step) * step
                vmin = float(info["volume_min"])

                if half >= vmin:
                    if partial_close(mt5, pos, half, "TP1"):
                        rec["partial_done"] = True
                        # ย้าย SL ไป BE
                        new_sl = round_price(entry, info, upward=not is_buy)
                        if modify_sl(mt5, pos, new_sl):
                            rec["sl"] = new_sl
                            send_telegram(
                                f"🎯 <b>TP1 Hit</b> {pos['symbol']}\n"
                                f"• ปิดครึ่ง + ย้าย SL → BE\n"
                                f"• Entry: {entry:.5f}"
                            )
                else:
                    # volume เล็กเกินกว่าจะแบ่ง → ปิดเต็มแทน
                    rec["partial_done"] = True
                save_state(state)

        # --- Trailing SL (หลังเลย 1.5R) ---
        if rec.get("partial_done", False):
            m5_bars = get_bars(mt5, pos["symbol"], mt5.TIMEFRAME_M5, 30)
            if len(m5_bars) < 20:
                continue
            m5_atr = atr(m5_bars, 14)[-1]
            trail_dist = 1.2 * m5_atr

            if is_buy:
                new_sl_raw = price - trail_dist
                new_sl = round_price(new_sl_raw, info, upward=False)
                if new_sl > sl + 0.5 * m5_atr:
                    if modify_sl(mt5, pos, new_sl):
                        rec["sl"] = new_sl
                        save_state(state)
            else:
                new_sl_raw = price + trail_dist
                new_sl = round_price(new_sl_raw, info, upward=True)
                if new_sl < sl - 0.5 * m5_atr:
                    if modify_sl(mt5, pos, new_sl):
                        rec["sl"] = new_sl
                        save_state(state)

        # --- อัปเดต current price ใน state (สำหรับ debug) ---
        rec["last_price"] = price
        save_state(state)


def partial_close(mt5, position: dict, volume: float, reason: str) -> bool:
    """ปิด position บางส่วน"""
    info = get_symbol_info(mt5, position["symbol"])
    tick = get_tick(mt5, position["symbol"])
    is_buy = int(position["type"]) == 0
    price = float(tick["bid"] if is_buy else tick["ask"])
    point = float(info["point"])

    filling = choose_filling(info)
    if filling is None:
        return False

    req = {
        "action":       mt5.TRADE_ACTION_DEAL,
        "symbol":       position["symbol"],
        "position":     int(position["ticket"]),
        "volume":       float(volume),
        "type":         int(mt5.ORDER_TYPE_SELL if is_buy else mt5.ORDER_TYPE_BUY),
        "price":        price,
        "deviation":    int(max(20, (float(tick["ask"]) - float(tick["bid"])) / point * 2)),
        "magic":        MAGIC,
        "comment":      reason,
        "type_time":    mt5.ORDER_TIME_GTC,
        "type_filling": int(filling),
    }
    res = send_order(mt5, req)
    return bool(res and int(res["retcode"]) == int(mt5.TRADE_RETCODE_DONE))


def modify_sl(mt5, position: dict, new_sl: float) -> bool:
    """แก้ SL ของ position ที่เปิดอยู่"""
    req = {
        "action":   mt5.TRADE_ACTION_SLTP,
        "symbol":   position["symbol"],
        "position": int(position["ticket"]),
        "sl":       float(new_sl),
        "tp":       float(position["tp"]),
        "magic":    MAGIC,
    }
    res = send_order(mt5, req)
    return bool(res and int(res["retcode"]) == int(mt5.TRADE_RETCODE_DONE))


# =============================================================
# MAIN LOOP
# =============================================================

def init_mt5():
    """เชื่อมต่อ MT5 ผ่าน mt5linux"""
    mt5 = MetaTrader5(
        host=MT5_HOST,
        port=MT5_PORT,
    )
    ok = mt5.initialize(
        login=MT5_LOGIN if MT5_LOGIN > 0 else None,
        password=MT5_PASSWORD or None,
        server=MT5_SERVER or None,
    )
    if not ok:
        log(f"MT5 init failed: {mt5.last_error()}")
        sys.exit(1)
    log(f"MT5 connected — account {get_account(mt5)['login']}")
    return mt5


def main():
    log("=== SMC Scalper Bot starting ===")
    send_telegram("🤖 <b>Bot started</b>")

    state = load_state()
    mt5 = init_mt5()

    try:
        while True:
            try:
                acct = get_account(mt5)
                balance = float(acct["balance"])
                reset_daily_if_needed(state, balance)

                # 1) จัดการ position ที่เปิดอยู่ (TP1 / BE / trailing)
                manage_open_positions(mt5, state)

                # 2) ตรวจ position ที่ปิดไปแล้ว → อัปเดต state + แจ้งเตือน
                reconcile_positions(mt5, state)

                # 3) ตรวจว่ารับออเดอร์ใหม่ได้ไหม
                open_positions = get_positions(mt5, symbol=None)
                can_open, why = can_open_new_position(state, balance, len(open_positions))

                if not can_open:
                    log(f"⏸️ skip new trades — {why}")
                elif not in_session():
                    log("⏸️ outside session window")
                else:
                    # 4) สแกนทุก symbol
                    for symbol in SYMBOLS:
                        # one position per symbol
                        if any(p["symbol"] == symbol for p in open_positions):
                            continue

                        try:
                            cand = scan_symbol(mt5, symbol)
                        except Exception as e:
                            log(f"scan {symbol} error: {e}")
                            continue

                        if cand is None:
                            continue

                        # 5) สร้างออเดอร์ + ส่ง
                        req = build_order(mt5, cand, balance)
                        if req is None:
                            continue

                        res = send_order(mt5, req)
                        if res and int(res["retcode"]) == int(mt5.TRADE_RETCODE_DONE):
                            ticket = str(res["order"])
                            side = "BUY" if cand["bias"] == "BUY" else "SELL"
                            state["open_tickets"][ticket] = {
                                "symbol":       symbol,
                                "side":         side,
                                "entry":        float(res["price"]),
                                "risk":         abs(float(res["price"]) - req["sl"]),
                                "sl":           float(req["sl"]),
                                "tp":           float(req["tp"]),
                                "opened_at":    time.time(),
                                "partial_done": False,
                            }
                            save_state(state)

                            msg = (
                                f"🟢 <b>New {side}</b> {symbol}\n"
                                f"• Entry: {res['price']:.5f}\n"
                                f"• SL: {req['sl']:.5f}\n"
                                f"• TP: {req['tp']:.5f}\n"
                                f"• Lot: {req['volume']:.2f}\n"
                                f"• Zone: {cand['zone']['kind']} | Bias: {cand['bias']}\n"
                                f"• Sweep: {cand['sweep']['type']}"
                            )
                            log(msg.replace("<b>", "").replace("</b>", ""))
                            send_telegram(msg)
                        else:
                            retcode = res["retcode"] if res else "None"
                            log(f"❌ order failed {symbol} retcode={retcode}")

                time.sleep(POLL_SECONDS)

            except KeyboardInterrupt:
                raise
            except Exception as e:
                log(f"main loop error: {e}")
                send_telegram(f"⚠️ <b>Bot error</b>\n<code>{e}</code>")
                time.sleep(POLL_SECONDS)

    finally:
        log("Shutting down — closing MT5")
        try:
            mt5.shutdown()
        except Exception:
            pass
        send_telegram("🔴 <b>Bot stopped</b>")


if __name__ == "__main__":
    main()