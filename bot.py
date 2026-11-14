#!/usr/bin/env python3
"""
Single-file rule-based SMC / Price Action bot for mt5linux.

Signal rules:
- H4 confirmed swing structure and EMA bias.
- Closed candles only.
- M15 displacement with first close-based structure break.
- OB and FVG confluence from the same displacement impulse.
- Frozen premium / discount filter at zone creation.
- First post-creation M5 touch only.
- M5 liquidity sweep and subsequent close-based confirmation.
- Optional additional M15 confirmation after the sweep.
- Zone age and touch timeout limits.

Execution rules:
- Safe defaults: DRY_RUN=true and ALLOW_LIVE_TRADING=false unless explicitly enabled in .env.
- Live mode requires ALLOW_LIVE_TRADING=true.
- Hedging accounts only.
- One position or active order per symbol, regardless of magic.
- Durable execution journal before every trading request.
- Ambiguous execution blocks further requests until reconciled.
- Stable position identifiers.
- Immutable initial risk distance.
- Stop updates must not increase risk.
- Partial and full exits are reconciled from actual deals.

Limitations:
- OHLC data cannot establish institutional intent.
- Intrabar ordering cannot be reconstructed from OHLC.
- The local news calendar is operator-maintained, not an automatic feed.
- Dry-run logs signals; it is not a fill simulator or backtester.
- Server-side SL / TP remain necessary.
- Gaps and slippage can exceed estimated risk.
- Broker-specific demo testing is required.

Application-authored messages and comments are English.
"""

import logging
import hashlib
import json
import math
import os
import queue
import re
import sys
import threading
import time
import uuid

from datetime import datetime, timezone
from decimal import Decimal, ROUND_FLOOR
from pathlib import Path

import requests

from dotenv import load_dotenv
from mt5linux import MetaTrader5
from bot_news import NewsFetcher
import bot_config as CFG

try:
    import fcntl
except ImportError:
    fcntl = None

CRYPTO_BASE_CURRENCIES = {
    "BTC", "XBT", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "LINK", "DOT", "AVAX", "LTC", "BCH"
}


def is_crypto_symbol(symbol):
    base_c, _, _ = NewsFetcher.map_symbol_currencies(str(symbol))
    return base_c in CRYPTO_BASE_CURRENCIES


def is_weekend(symbols=None):
    active_list = symbols if symbols is not None else CFG.SYMBOLS
    current_utc_time = datetime.now(timezone.utc)
    if current_utc_time.weekday() < 5:
        return False
    if current_utc_time.weekday() == 6 and current_utc_time.hour >= 22:
        return False
    return not any(is_crypto_symbol(sym) for sym in active_list)


def is_monday_opening_gap_window(symbol):
    if is_crypto_symbol(symbol):
        return False
    cooldown_mins = int(getattr(CFG, "MONDAY_OPEN_COOLDOWN_MINUTES", 30))
    if cooldown_mins <= 0:
        return False
    now = utc_now()
    if now.weekday() == 6:
        if now.hour < 22:
            return True
        minutes_since_reopen = (now.hour - 22) * 60 + now.minute
        return minutes_since_reopen < cooldown_mins
    if now.weekday() == 0:
        minutes_since_sunday_reopen = 120 + now.hour * 60 + now.minute
        return minutes_since_sunday_reopen < cooldown_mins
    return False


_TELEGRAM_CRITICAL_QUEUE = queue.Queue(maxsize=200)
_TELEGRAM_INFO_QUEUE = queue.Queue(maxsize=1000)


def _configure_logging():
    logging.Formatter.converter = time.gmtime
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)
    formatter = logging.Formatter(
        "%(asctime)s UTC | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    if not any(
        isinstance(h, logging.StreamHandler) and not isinstance(h, TelegramLogHandler)
        for h in root_logger.handlers
    ):
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.setLevel(logging.INFO)
        console_handler.setFormatter(formatter)
        root_logger.addHandler(console_handler)
    return root_logger, formatter


def _telegram_worker():
    while True:
        is_critical = False
        message = None
        try:
            message = _TELEGRAM_CRITICAL_QUEUE.get_nowait()
            is_critical = True
        except queue.Empty:
            try:
                message = _TELEGRAM_INFO_QUEUE.get(timeout=1.0)
            except queue.Empty:
                continue

        if message is None:
            if is_critical:
                _TELEGRAM_CRITICAL_QUEUE.task_done()
            break

        try:
            attempts = 3 if is_critical else 1
            for attempt in range(attempts):
                try:
                    resp = requests.post(
                        f"https://api.telegram.org/bot{CFG.TELEGRAM_TOKEN}/sendMessage",
                        json={
                            "chat_id": CFG.TELEGRAM_CHAT_ID,
                            "text": message,
                            "disable_web_page_preview": True,
                        },
                        timeout=5,
                    )
                    if resp.status_code == 200:
                        break
                except Exception:
                    pass
                if attempt + 1 < attempts:
                    time.sleep(1.5)
        finally:
            if is_critical:
                _TELEGRAM_CRITICAL_QUEUE.task_done()
            else:
                _TELEGRAM_INFO_QUEUE.task_done()


if CFG.TELEGRAM_TOKEN and CFG.TELEGRAM_CHAT_ID:
    threading.Thread(target=_telegram_worker, daemon=True).start()


class TelegramLogHandler(logging.Handler):
    def emit(self, record):
        try:
            if record.name == "NewsFetcher" and record.levelno < logging.ERROR:
                return
            log_entry = self.format(record)
            if record.levelno >= logging.WARNING:
                send_telegram_log_async(log_entry, critical=True)
            elif record.levelno >= logging.INFO:
                send_telegram_log_async(log_entry, critical=False)
        except Exception:
            self.handleError(record)


def send_telegram_log_async(message, critical=False):
    if not CFG.TELEGRAM_TOKEN or not CFG.TELEGRAM_CHAT_ID:
        return
    clean_msg = safe_text(message)
    target_q = _TELEGRAM_CRITICAL_QUEUE if critical else _TELEGRAM_INFO_QUEUE
    try:
        target_q.put_nowait(clean_msg)
    except queue.Full:
        if critical:
            try:
                target_q.get_nowait()
                target_q.task_done()
            except queue.Empty:
                pass
            try:
                target_q.put_nowait(clean_msg)
            except queue.Full:
                pass

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")
_configure_logging()

SHARED_NEWS_FETCHER = NewsFetcher(check_interval=300)
SHARED_NEWS_FETCHER.start()

LOCK_HANDLE = None
LAST_STATUS = {}
NOTIFIED_ZONES = set()
SYMBOL_RADAR_STATE = {}
LAST_RADAR_SIGNATURE = ""
LAST_RADAR_SENT_TS = 0.0


class SafetyError(RuntimeError):
    pass


class FatalError(RuntimeError):
    pass


def utc_now():
    return datetime.now(timezone.utc)


def utc_midnight_timestamp():
    return utc_now().replace(
        hour=0, minute=0, second=0, microsecond=0
    ).timestamp()

def analyze_order_flow(broker, symbol, side, reference_bar, bars=None, include_volume_surge=True):
    if not getattr(CFG, "ENABLE_ORDER_FLOW_SCORING", True):
        return 0.0

    flow_score = 0.0
    buy = (side == "BUY")

    try:
        if bars is not None and len(bars) >= 2:
            if include_volume_surge:
                bar_volume = float(reference_bar.get("volume", 0))
                vol_ma_list = volume_ma(bars, CFG.VOLUME_MA_PERIOD)
                if vol_ma_list:
                    avg_vol = vol_ma_list[-1]
                    if avg_vol > 0 and bar_volume >= avg_vol * CFG.TICK_VOLUME_SURGE_MULTIPLIER:
                        flow_score += 1.5
                    elif avg_vol > 0 and bar_volume >= avg_vol * 1.35:
                        flow_score += 0.75

            rng = float(reference_bar["high"]) - float(reference_bar["low"])
            if rng > 0:
                close_pos = (
                    (float(reference_bar["close"]) - float(reference_bar["low"])) / rng
                    if buy
                    else (float(reference_bar["high"]) - float(reference_bar["close"])) / rng
                )
                if close_pos >= 0.75:
                    flow_score += 0.75
    except Exception:
        pass

    try:
        book = broker.market_book(symbol) if hasattr(broker, "market_book") else broker.read(f"mt5.market_book_get({symbol!r})")
        if book and isinstance(book, list) and len(book) >= 4:
            vols = [float(item.get("volume", 0)) for item in book if float(item.get("volume", 0)) > 0]
            if len(set(vols)) > 1:
                ask_volume_total = sum(float(item.get("volume", 0)) for item in book if int(item.get("type", 0)) in {1, 4})
                bid_volume_total = sum(float(item.get("volume", 0)) for item in book if int(item.get("type", 0)) in {2, 3})
                if buy and bid_volume_total > ask_volume_total * 1.35:
                    flow_score += 0.5
                elif not buy and ask_volume_total > bid_volume_total * 1.35:
                    flow_score += 0.5
    except Exception:
        pass

    return min(flow_score, CFG.ORDER_FLOW_SCORE_BOOST)

def scan_momentum_breakout_candidate(broker, symbol, m5_bars, side, volatility, m15_bars=None):
    if len(m5_bars) < max(210, CFG.VOLUME_MA_PERIOD):
        return None

    latest = m5_bars[-1]
    buy = side == "BUY"

    closes = [b["close"] for b in m5_bars]
    m5_ema50 = ema(closes, 50)[-1]
    m5_ema200 = ema(closes, 200)[-1]
    if buy and not (latest["close"] > m5_ema200 and m5_ema50 >= m5_ema200):
        return None
    if not buy and not (latest["close"] < m5_ema200 and m5_ema50 <= m5_ema200):
        return None

    if m15_bars and len(m15_bars) >= 55:
        m15_closes = [b["close"] for b in m15_bars]
        m15_ema50_now = ema(m15_closes, 50)[-1]
        m15_atr_vals = atr(m15_bars)
        m15_ref_atr = m15_atr_vals[-1] if m15_atr_vals and m15_atr_vals[-1] > 0 else 0.0
        max_ext_atr = float(getattr(CFG, "MAX_MOMENTUM_M15_EMA50_ATR", 3.5))
        if m15_ref_atr > 0:
            if buy and (latest["close"] - m15_ema50_now) > (max_ext_atr * m15_ref_atr):
                return None
            if not buy and (m15_ema50_now - latest["close"]) > (max_ext_atr * m15_ref_atr):
                return None

    bigbeluga = calculate_bigbeluga_momentum(m5_bars, length=50, smooth=50)

    if bigbeluga["direction"] == 0:
        return None

    if buy and bigbeluga["direction"] != 1:
        return None

    if not buy and bigbeluga["direction"] != -1:
        return None

    body = abs(latest["close"] - latest["open"])
    ref_atr = volatility[-2] if len(volatility) >= 2 else volatility[-1]

    if ref_atr <= 0 or body < ref_atr * 1.0 or body > ref_atr * 2.8:
        return None

    zone_key = f"{symbol}:MOMENTUM_BIGBELUGA:{side}:{latest['time']}"

    range_lookback = 12
    prior_box = m5_bars[-(range_lookback + 1):-1]
    box_high = max(b["high"] for b in prior_box)
    box_low = min(b["low"] for b in prior_box)
    close_buffer = CFG.BOS_BUFFER_ATR * ref_atr

    macro_window = m5_bars[-49:-1]
    macro_high = max(b["high"] for b in macro_window)
    macro_low = min(b["low"] for b in macro_window)
    if buy and latest["close"] < macro_high and (macro_high - latest["close"]) < ref_atr * 2.5:
        return None
    if not buy and latest["close"] > macro_low and (latest["close"] - macro_low) < ref_atr * 2.5:
        return None

    prev_three = m5_bars[-4:-1]
    if buy and all(b["close"] > b["open"] for b in prev_three):
        prior_run = latest["open"] - prev_three[0]["open"]
        if prior_run >= ref_atr * 1.8:
            return None
    elif not buy and all(b["close"] < b["open"] for b in prev_three):
        prior_run = prev_three[0]["open"] - latest["open"]
        if prior_run >= ref_atr * 1.8:
            return None

    is_breakout = False
    extreme = None

    if buy and latest["close"] > box_high + close_buffer and latest["close"] > latest["open"]:
        is_breakout = True
        swing_low = min(b["low"] for b in m5_bars[-10:])
        extreme = max(swing_low, latest["close"] - (ref_atr * 3.5))
    elif not buy and latest["close"] < box_low - close_buffer and latest["close"] < latest["open"]:
        is_breakout = True
        swing_high = max(b["high"] for b in m5_bars[-10:])
        extreme = min(swing_high, latest["close"] + (ref_atr * 3.5))

    if not is_breakout or extreme is None:
        return None

    max_wick_ratio = min(getattr(CFG, "MAX_SWEEP_WICK_RATIO", 0.70), 0.35)
    if not wick_rejection_ok(latest, side, max_wick_ratio):
        return None

    if not has_consolidation_before_breakout(m5_bars, len(m5_bars) - 1, lookback=range_lookback):
        return None

    score = 4.0
    has_volume_boost = False
    has_sweep_boost = False

    if body >= ref_atr * 1.6:
        score += 2.0
    elif body >= ref_atr * 1.3:
        score += 1.2
    else:
        score += 0.5

    if bigbeluga["is_strong"]:
        score += 2.5

    vol_ma = volume_ma(m5_bars[:-1], CFG.VOLUME_MA_PERIOD)
    if vol_ma:
        avg_vol = vol_ma[-1]
        if avg_vol > 0:
            vol_ratio = latest["volume"] / avg_vol
            threshold_mult = getattr(CFG, "DISPLACEMENT_VOLUME_MULTIPLIER", 1.5)
            if vol_ratio >= threshold_mult:
                score += 3.0
                has_volume_boost = True
            elif vol_ratio >= 1.2:
                score += 1.5
                has_volume_boost = True

    if is_volume_building_up(m5_bars, len(m5_bars) - 1, getattr(CFG, "VOL_BUILDUP_LOOKBACK", 3)):
        score += 0.5

    try:
        if check_trendline_liquidity_sweep(m5_bars, side):
            score += 1.5
            has_sweep_boost = True
    except Exception:
        pass

    order_flow_points = analyze_order_flow(broker, symbol, side, latest, m5_bars[:-1], include_volume_surge=False)
    score += order_flow_points

    if not has_volume_boost and not has_sweep_boost and order_flow_points <= 0:
        return None

    final_score = min(score, 12.0)

    if buy:
        zone = {"kind": "MOMENTUM_BIGBELUGA", "side": "BUY", "bottom": extreme, "top": latest["close"]}
    else:
        zone = {"kind": "MOMENTUM_BIGBELUGA", "side": "SELL", "bottom": latest["close"], "top": extreme}

    recent_vol_floor = max(volatility[-1], max(volatility[-24:]) * 0.85) if len(volatility) >= 24 else volatility[-1]
    return {
        "zone": zone,
        "side": side,
        "extreme": extreme,
        "atr": volatility[-1],
        "sl_floor_atr": recent_vol_floor,
        "signal_close": latest["close"],
        "signal_time": int(latest["time"] + 300),
        "score": final_score,
        "zone_id": zone_key,
        "bigbeluga_target": bigbeluga["target_up"] if buy else bigbeluga["target_down"],
    }

def calculate_efficiency_ratio(closes, period=10):
    if len(closes) < period + 1:
        return 0.5
    net_change = abs(closes[-1] - closes[-(period + 1)])
    sum_changes = sum(
        abs(closes[i] - closes[i - 1])
        for i in range(len(closes) - period, len(closes))
    )
    if sum_changes <= 1e-12:
        return 0.0
    return net_change / sum_changes


def detect_market_regime(h4_bars, m15_bars=None):
    if len(h4_bars) < 35:
        return "RETRACEMENT_SMC"

    closes = [bar["close"] for bar in h4_bars]
    ema_fast = ema(closes, 50)
    ema_slow = ema(closes, 200)
    h4_volatility = atr(h4_bars)
    ref_h4_atr = h4_volatility[-1] if h4_volatility and h4_volatility[-1] > 0 else (ema_slow[-1] * 0.002)

    ema_gap_now = abs(ema_fast[-1] - ema_slow[-1])
    ema_gap_prev = abs(ema_fast[-4] - ema_slow[-4])
    ema_expanding = ema_gap_now >= ema_gap_prev * 0.98
    h4_bullish = ema_fast[-1] > ema_slow[-1]

    h4_er = calculate_efficiency_ratio(closes, period=10)
    m15_er = 0.35
    m15_vol_expanding = True
    m15_aligned_with_h4 = True

    if m15_bars and len(m15_bars) >= 35:
        m15_closes = [b["close"] for b in m15_bars]
        m15_er = calculate_efficiency_ratio(m15_closes, period=16)
        m15_ema50 = ema(m15_closes, 50)
        m15_ema200 = ema(m15_closes, 200)
        m15_atr_series = atr(m15_bars)
        m15_ref_atr = m15_atr_series[-1] if m15_atr_series and m15_atr_series[-1] > 0 else 0.0
        m15_net_move_16 = m15_closes[-1] - m15_closes[-17]

        if h4_bullish:
            m15_aligned_with_h4 = (
                m15_closes[-1] >= m15_ema50[-1]
                and m15_ema50[-1] >= m15_ema200[-1]
                and m15_net_move_16 >= -(m15_ref_atr * 0.5)
            )
        else:
            m15_aligned_with_h4 = (
                m15_closes[-1] <= m15_ema50[-1]
                and m15_ema50[-1] <= m15_ema200[-1]
                and m15_net_move_16 <= (m15_ref_atr * 0.5)
            )

        if len(m15_atr_series) >= 25:
            recent_m15_atr = sum(m15_atr_series[-5:]) / 5.0
            base_m15_atr = sum(m15_atr_series[-25:-5]) / 20.0
            m15_vol_expanding = recent_m15_atr >= base_m15_atr * 0.80

    if ema_gap_now < (ref_h4_atr * 0.30):
        return "RANGING_SIDEWAY"
    if h4_er < 0.18 and m15_er < 0.18 and not m15_vol_expanding:
        return "RANGING_SIDEWAY"

    if (
        ema_gap_now >= (ref_h4_atr * 0.75)
        and ema_expanding
        and m15_aligned_with_h4
        and (
            (h4_er >= 0.40 and m15_er >= 0.25)
            or m15_er >= 0.42
        )
        and m15_vol_expanding
    ):
        return "STRONG_TREND"

    return "RETRACEMENT_SMC"

def is_volume_building_up(bars, index, lookback=3):
    if index < lookback:
        return True
    subset = bars[index - lookback + 1 : index + 1]
    vols = [b["volume"] for b in subset]
    return vols[-1] >= sum(vols) / len(vols)

def safe_text(text):
    return "".join(
        char
        for char in str(text)
        if char in "\n\t" or 32 <= ord(char) <= 126
    )


def log(message, level=logging.INFO):
    logging.log(level, safe_text(message))


def notify(message, critical=False):
    log(message, level=logging.WARNING if critical else logging.INFO)

def get_dynamic_sweep_atr(symbol, base_atr):
    if base_atr <= 0:
        return 0.0
    if not symbol:
        return base_atr * getattr(CFG, "SWEEP_MIN_ATR", 0.10)

    base_c, _, _ = NewsFetcher.map_symbol_currencies(symbol)
    clean = re.sub(r"[^A-Z0-9]", "", symbol.upper())

    if base_c == "IDX" or any(k in clean for k in ("USOIL", "UKOIL", "WTI", "BRENT", "XTI", "XBR", "XNG", "XCU")):
        multiplier = 0.20
    elif base_c in {"XAU", "XAG", "XPT", "XPD", "BTC", "XBT", "ETH", "SOL", "BNB", "AVAX", "LTC", "BCH"}:
        multiplier = 0.12
    elif base_c in {"XRP", "ADA", "DOGE", "LINK", "DOT"}:
        multiplier = 0.15
    else:
        multiplier = 0.10

    return base_atr * multiplier

def check_trendline_liquidity_sweep(bars, zone_side):
    highs, lows = swings(bars, 2)
    if len(highs) < 3 or len(lows) < 3:
        return False

    buy = zone_side == "BUY"
    latest_bar = bars[-1]
    prev_bar = bars[-2]

    if buy:
        recent_low = min(l["price"] for l in lows[-3:])
        swept = prev_bar["low"] < recent_low and latest_bar["close"] > recent_low
        return swept
    else:
        recent_high = max(h["price"] for h in highs[-3:])
        swept = prev_bar["high"] > recent_high and latest_bar["close"] < recent_high
        return swept

def calculate_pdh_pdl(broker, symbol):
    daily_bars = broker.bars(
        symbol, broker.constant("TIMEFRAME_D1"), 6
    )
    if not daily_bars:
        return None, None

    srv_offset = int(round(getattr(broker, "_server_offset_sec", 0.0)))

    def _bar_server_weekday(bar_item):
        return datetime.fromtimestamp(int(bar_item["time"]) + srv_offset, tz=timezone.utc).weekday()

    ranges = [
        b["high"] - b["low"]
        for b in daily_bars
        if (b["high"] - b["low"]) > 0
        and _bar_server_weekday(b) not in {5, 6}
    ]
    avg_range = (sum(ranges) / len(ranges)) if ranges else 0.0

    chosen_day = daily_bars[-1]
    for bar in reversed(daily_bars):
        wd = _bar_server_weekday(bar)
        bar_range = bar["high"] - bar["low"]
        if wd == 6 or (wd == 5 and not is_crypto_symbol(symbol)):
            continue
        if avg_range > 0 and bar_range < avg_range * 0.25:
            continue
        chosen_day = bar
        break

    return chosen_day["high"], chosen_day["low"]


def is_prime_killzone_utc():
    if getattr(CFG, "USE_AUTO_DST_KILLZONES", True):
        try:
            from zoneinfo import ZoneInfo
            now_utc = utc_now()
            london_hour = now_utc.astimezone(ZoneInfo("Europe/London")).hour
            ny_hour = now_utc.astimezone(ZoneInfo("America/New_York")).hour
            return (8 <= london_hour < 11) or (8 <= ny_hour < 12)
        except Exception:
            pass
    hour = utc_now().hour
    for start_k, end_k in CFG.KILLZONES_UTC:
        if start_k <= hour < end_k:
            return True
    return False


def _risk_asset_group(symbol):
    base_c, _, _ = SHARED_NEWS_FETCHER.map_symbol_currencies(symbol)
    if base_c in {"IDX", "XAU", "XAG", "XPT", "XPD"} or base_c in CRYPTO_BASE_CURRENCIES:
        return "USD_RISK_ASSET"
    return None


def currency_exposure_ok(symbol, side, active_positions):
    max_per_cur = int(getattr(CFG, "MAX_POSITIONS_PER_CURRENCY", 2))
    if max_per_cur <= 0 or not active_positions:
        return True

    base_c, quote_c, _ = SHARED_NEWS_FETCHER.map_symbol_currencies(symbol)
    new_dir = {}
    if base_c and base_c != "IDX":
        new_dir[base_c] = 1 if side == "BUY" else -1
    if quote_c:
        new_dir[quote_c] = -1 if side == "BUY" else 1

    exposure_counts = {}
    risk_group_counts = {}
    for pos in active_positions:
        p_sym = pos.get("symbol", "")
        p_buy = int(pos.get("type", 0)) == 0
        b_c, q_c, _ = SHARED_NEWS_FETCHER.map_symbol_currencies(p_sym)
        if b_c and b_c != "IDX":
            d = 1 if p_buy else -1
            exposure_counts[(b_c, d)] = exposure_counts.get((b_c, d), 0) + 1
        if q_c:
            d = -1 if p_buy else 1
            exposure_counts[(q_c, d)] = exposure_counts.get((q_c, d), 0) + 1

        rg = _risk_asset_group(p_sym)
        if rg:
            rg_dir = 1 if p_buy else -1
            risk_group_counts[(rg, rg_dir)] = risk_group_counts.get((rg, rg_dir), 0) + 1

    for cur, d in new_dir.items():
        if exposure_counts.get((cur, d), 0) >= max_per_cur:
            return False

    cand_rg = _risk_asset_group(symbol)
    max_rg = int(getattr(CFG, "MAX_RISK_GROUP_EXPOSURE", 2))
    if cand_rg and max_rg > 0:
        cand_rg_dir = 1 if side == "BUY" else -1
        if risk_group_counts.get((cand_rg, cand_rg_dir), 0) >= max_rg:
            return False

    return True


def portfolio_open_heat_usd(broker, state, active_positions=None):
    if active_positions is None:
        try:
            active_positions = broker.positions()
        except Exception:
            return 0.0

    total_open_risk = 0.0
    for pos in active_positions:
        if int(pos.get("magic", 0)) != CFG.MAGIC:
            continue
        pid = position_identifier(pos)
        record = state.get("positions", {}).get(pid)
        sym = pos.get("symbol", "")
        vol = float(pos.get("volume", 0.0))
        sl = float(pos.get("sl", 0.0))
        entry = float(record["entry"]) if record and "entry" in record else float(pos.get("price_open", 0.0))
        buy = int(pos.get("type", 0)) == broker.constant("POSITION_TYPE_BUY")

        if vol <= 0 or sl <= 0 or entry <= 0:
            continue
        if buy and sl >= entry - CFG.EPSILON:
            continue
        if not buy and sl <= entry + CFG.EPSILON:
            continue

        order_type = broker.constant("ORDER_TYPE_BUY" if buy else "ORDER_TYPE_SELL")
        try:
            info = broker.info(sym)
            allowance = CFG.RISK_SLIPPAGE_POINTS * float(info["point"])
            worse_entry = entry + allowance if buy else entry - allowance
            worse_sl = sl - allowance if buy else sl + allowance
            if min(worse_entry, worse_sl) <= 0:
                worse_entry, worse_sl = entry, sl
            pos_risk = abs(broker.profit(order_type, sym, vol, worse_entry, worse_sl)) + CFG.COMMISSION_PER_LOT * vol
            total_open_risk += pos_risk
        except Exception:
            pass

    return total_open_risk

def status(key, message, interval=300):
    now = time.time()
    if now - LAST_STATUS.get(key, 0.0) >= interval:
        LAST_STATUS[key] = now
        log(message)

def volume_ma(bars, period=20):
    volumes = [bar["volume"] for bar in bars]
    if not volumes:
        return []
    result = []
    for i in range(len(volumes)):
        start = max(0, i - period + 1)
        chunk = volumes[start:i+1]
        result.append(sum(chunk) / len(chunk))
    return result

def calculate_wma(values, period):
    if len(values) < period or period <= 0:
        return list(values)
    result = []
    weights = list(range(1, period + 1))
    weight_sum = sum(weights)
    for i in range(len(values)):
        if i < period - 1:
            result.append(values[i])
            continue
        chunk = values[i - period + 1 : i + 1]
        result.append(sum(v * w for v, w in zip(chunk, weights)) / weight_sum)
    return result


def calculate_hma(values, period):
    if len(values) < period or period <= 1:
        return list(values)
    half_period = max(2, period // 2)
    sqrt_period = max(2, int(period ** 0.5))
    wma_half = calculate_wma(values, half_period)
    wma_full = calculate_wma(values, period)
    diff = [2 * wma_half[i] - wma_full[i] for i in range(len(values))]
    return calculate_wma(diff, sqrt_period)


def calculate_bigbeluga_momentum(m5_bars, length=50, smooth=50, mult_dist=1.0):
    default_out = {
        "direction": 0, "mom_value": 0.0, "is_strong": False,
        "dist": 0.0, "target_up": 0.0, "target_down": 0.0,
    }

    if len(m5_bars) < length + smooth + 10:
        return default_out

    closes = [bar["close"] for bar in m5_bars]
    highs = [bar["high"] for bar in m5_bars]
    lows = [bar["low"] for bar in m5_bars]

    raw_mom = [closes[i] - closes[i - length] for i in range(length, len(closes))]
    if len(raw_mom) < smooth + 5:
        return default_out

    mom_series = calculate_hma(raw_mom, smooth)
    if len(mom_series) < 3:
        return default_out

    cur = mom_series[-1]
    prev = mom_series[-2]
    prev2 = mom_series[-3]

    rising = cur > prev
    falling = cur < prev

    raw_cur = raw_mom[-1]
    if cur > 0 and (rising or raw_cur >= cur * 0.90):
        direction = 1
    elif cur < 0 and (falling or raw_cur <= cur * 0.90):
        direction = -1
    else:
        direction = 1 if (rising and raw_cur > 0) else (-1 if (falling and raw_cur < 0) else 0)

    bull_accel = (direction == 1) and ((cur > 0 and raw_cur > cur) or ((cur - prev) >= (prev - prev2) and rising))
    bear_accel = (direction == -1) and ((cur < 0 and raw_cur < cur) or ((prev - cur) >= (prev2 - prev) and falling))
    is_strong = bull_accel if direction == 1 else (bear_accel if direction == -1 else False)

    recent_ranges = [(highs[i] - lows[i]) for i in range(max(0, len(highs) - 100), len(highs))]
    dist = (sum(recent_ranges) / len(recent_ranges)) * mult_dist if recent_ranges else 0.0

    current_high = highs[-1]
    current_low = lows[-1]

    return {
        "direction": direction,
        "mom_value": cur,
        "is_strong": is_strong,
        "dist": dist,
        "target_up": current_high + dist * 6.0,
        "target_down": current_low - dist * 6.0,
    }

def validate_config():
    if not CFG.SYMBOLS or len(CFG.SYMBOLS) != len(set(CFG.SYMBOLS)):
        raise FatalError("The symbol list is empty or duplicated.")

    if any(
        re.fullmatch(r"[A-Za-z0-9._-]+", symbol) is None
        for symbol in CFG.SYMBOLS
    ):
        raise FatalError("Unsupported symbol name.")

    if not CFG.DRY_RUN and not CFG.ALLOW_LIVE_TRADING:
        raise FatalError(
            "Live mode requires ALLOW_LIVE_TRADING=true."
        )

    positive = [
        CFG.POLL_SECONDS,
        CFG.RISK_PER_TRADE,
        CFG.MAX_SPREAD_POINTS,
        CFG.MAX_LOT,
        CFG.DISPLACEMENT_ATR,
        CFG.MIN_FVG_ATR,
        CFG.MAX_SL_ATR,
        CFG.MAX_ENTRY_DRIFT_ATR,
        CFG.TP1_RR,
        CFG.TP2_RR,
        CFG.TRAIL_ATR,
        CFG.TIME_EXIT_MINUTES,
        CFG.MAX_TICK_AGE_SECONDS,
        CFG.MAX_CLOSED_M5_AGE_SECONDS,
    ]

    if any(not math.isfinite(x) or x <= 0 for x in positive):
        raise FatalError("Positive finite configuration is required.")

    nonnegative = [
        CFG.COMMISSION_PER_LOT,
        CFG.BOS_BUFFER_ATR,
        CFG.CONFIRM_CLOSE_BUFFER_ATR,
        CFG.CONFIRM_MIN_BODY_ATR,
        CFG.SL_BUFFER_ATR,
        CFG.SWEEP_MIN_ATR,
        CFG.NEWS_BEFORE_MINUTES,
        CFG.NEWS_AFTER_MINUTES,
        CFG.COOLDOWN_SECONDS,
    ]

    if any(not math.isfinite(x) or x < 0 for x in nonnegative):
        raise FatalError("A nonnegative configuration is invalid.")

    if not isinstance(CFG.MAX_SPREAD_BY_SYMBOL, dict):
        raise FatalError("Spread overrides must be a JSON object.")

    for value in CFG.MAX_SPREAD_BY_SYMBOL.values():
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise FatalError("Invalid spread override.")

    integer_positive = [
        CFG.MAX_OPEN_POSITIONS,
        CFG.MAX_DAILY_TRADES,
        CFG.MAX_CONSECUTIVE_LOSSES,
        CFG.BIAS_EMA_PERIOD,
        CFG.HTF_SWING_WINDOW,
        CFG.LTF_SWING_WINDOW,
        CFG.STRUCTURE_SWING_WINDOW,
        CFG.ZONE_LOOKBACK,
        CFG.DEALING_RANGE_BARS,
        CFG.OB_SEARCH_BACK,
        CFG.MAX_ZONE_AGE_M15_BARS,
        CFG.LIQUIDITY_MAX_AGE,
        CFG.CONFIRM_AFTER_SWEEP_BARS,
        CFG.MAX_TOUCH_WAIT_BARS,
    ]

    if any(x <= 0 for x in integer_positive):
        raise FatalError("Positive integer configuration is required.")

    if CFG.SWEEP_AFTER_TOUCH_BARS < 0:
        raise FatalError("Invalid sweep timeout.")

    if CFG.DEVIATION_POINTS < 0 or CFG.RISK_SLIPPAGE_POINTS < 0:
        raise FatalError("Invalid slippage configuration.")

    if not 0 < CFG.MAX_DAILY_LOSS_PCT < 1:
        raise FatalError("Invalid daily loss limit.")

    if CFG.TP2_RR <= CFG.TP1_RR:
        raise FatalError("TP2_RR must exceed TP1_RR.")

    if not 0 < CFG.DISPLACEMENT_BODY_RATIO <= 1:
        raise FatalError("Invalid displacement body ratio.")

    if not 0 <= CFG.PD_EQUILIBRIUM_BUFFER < 0.5:
        raise FatalError("Invalid equilibrium buffer.")

    if not math.isfinite(CFG.TIME_EXIT_MIN_R):
        raise FatalError("Invalid time exit threshold.")

    if not (
        0 <= CFG.SESSION_START_HOUR_UTC <= 23
        and 0 <= CFG.SESSION_END_HOUR_UTC <= 23
    ):
        raise FatalError("Session hours must be between 0 and 23.")

    if CFG.H4_BAR_COUNT < CFG.BIAS_EMA_PERIOD * 3 + 2:
        raise FatalError("H4 history is too short.")

    if CFG.M15_BAR_COUNT < CFG.DEALING_RANGE_BARS + CFG.ZONE_LOOKBACK + 10:
        raise FatalError("M15 history is too short.")

    if CFG.M5_BAR_COUNT < CFG.MAX_ZONE_AGE_M15_BARS * 3 + 50:
        raise FatalError("M5 history is too short for zone validation.")


def acquire_lock(account_key):
    global LOCK_HANDLE

    if fcntl is None:
        raise FatalError("This build requires a Linux process lock.")

    lock_path = ROOT / f"smc_{account_key}_{CFG.MAGIC}.lock"
    LOCK_HANDLE = open(lock_path, "a", encoding="ascii")

    try:
        fcntl.flock(
            LOCK_HANDLE.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )
    except OSError:
        LOCK_HANDLE.close()
        LOCK_HANDLE = None
        raise FatalError("Another instance holds the account lock.")


def release_lock():
    global LOCK_HANDLE

    if LOCK_HANDLE is not None:
        try:
            fcntl.flock(LOCK_HANDLE.fileno(), fcntl.LOCK_UN)
            LOCK_HANDLE.close()
        except Exception:
            pass
        LOCK_HANDLE = None


def atomic_save(path, state):
    temporary = path.with_suffix(".tmp")

    try:
        text = json.dumps(
            state,
            indent=2,
            sort_keys=True,
            ensure_ascii=True,
            allow_nan=False,
        )

        with open(temporary, "w", encoding="ascii") as handle:
            handle.write(text)
            handle.flush()
            os.fsync(handle.fileno())

        os.replace(temporary, path)

        directory_fd = os.open(str(path.parent), os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)

    except Exception:
        raise FatalError("State persistence failed. Trading stopped.")


def load_state(path, account_key):
    expected_mode = "DRY_RUN" if CFG.DRY_RUN else "LIVE"

    if not path.exists():
        return {
            "version": CFG.STATE_VERSION,
            "account_key": account_key,
            "mode": expected_mode,
            "day": "",
            "day_start_balance": None,
            "peak_balance": None,
            "daily_loss_latched": False,
            "consecutive_losses": 0,
            "cooldown_until": 0.0,
            "positions": {},
            "seen_zones": {},
            "pending": None,
            "halt_reason": "",
            "dry_run_entries": 0,
        }

    try:
        state = json.loads(path.read_text(encoding="ascii"))
    except Exception:
        raise FatalError("The state file is unreadable.")

    if (
        state.get("version") != CFG.STATE_VERSION
        or state.get("account_key") != account_key
        or state.get("mode") != expected_mode
    ):
        raise FatalError("The state file does not match account or mode.")

    required = {
        "positions",
        "seen_zones",
        "pending",
        "halt_reason",
        "consecutive_losses",
        "cooldown_until",
        "day",
        "day_start_balance",
        "daily_loss_latched",
    }

    if not required.issubset(state):
        raise FatalError("The state file is incomplete.")

    if not isinstance(state["positions"], dict):
        raise FatalError("Invalid position state.")

    if not isinstance(state["seen_zones"], dict):
        raise FatalError("Invalid signal state.")

    state.setdefault("dry_run_entries", 0)
    state.setdefault("peak_balance", state.get("day_start_balance"))

    for record in state["positions"].values():
        record.setdefault("last_sl", record["initial_sl"])

    return state


class Broker:
    def __init__(self):
        self.mt5 = MetaTrader5(host=CFG.MT5_HOST, port=CFG.MT5_PORT)

        options = {}
        if CFG.MT5_LOGIN > 0:
            options["login"] = CFG.MT5_LOGIN
        if CFG.MT5_PASSWORD:
            options["password"] = CFG.MT5_PASSWORD
        if CFG.MT5_SERVER:
            options["server"] = CFG.MT5_SERVER

        if not self.mt5.initialize(**options):
            raise FatalError("MT5 initialization failed.")
        self._server_offset_sec = 0.0
        self.active_symbols = list(CFG.SYMBOLS)
        self.base_symbol_map = {s: s for s in CFG.SYMBOLS}
        self._dom_cache = {}

    def constant(self, name):
        return int(getattr(self.mt5, name))

    def read(self, expression):
        """
        Serialize on the remote side to avoid retaining remote object proxies.
        """
        serialized = self.mt5._container.eval(
            f"__import__('json').dumps({expression}, allow_nan=False)"
        )
        return json.loads(str(serialized))

    def named(self, expression):
        result = self.read(
            "(lambda value: None if value is None "
            f"else value._asdict())({expression})"
        )
        if result is None:
            raise SafetyError("Required MT5 information is unavailable.")
        return result

    def records(self, expression):
        result = self.read(
            "(lambda values: None if values is None "
            "else [value._asdict() for value in values])"
            f"({expression})"
        )
        if result is None:
            raise SafetyError("An MT5 collection query failed.")
        return result

    def account(self):
        return self.named("mt5.account_info()")

    def terminal(self):
        return self.named("mt5.terminal_info()")

    def info(self, symbol):
        return self.named(f"mt5.symbol_info({symbol!r})")

    def update_server_offset(self, raw_server_ts, max_Select_drift_sec=90.0):
        if raw_server_ts is None or float(raw_server_ts) <= 0:
            return self._server_offset_sec
        diff = float(raw_server_ts) - time.time()
        nearest_half_hour = round(diff / 1800.0) * 1800.0
        if abs(nearest_half_hour) <= 14 * 3600 and abs(diff - nearest_half_hour) <= max_Select_drift_sec:
            self._server_offset_sec = float(nearest_half_hour)
        return self._server_offset_sec

    def calibrate_server_offset(self):
        offsets = []
        probe_list = (self.active_symbols or CFG.SYMBOLS)[:8]
        for sym in probe_list:
            try:
                raw_tick = self.named(f"mt5.symbol_info_tick({sym!r})")
                ts = tick_timestamp(raw_tick)
                diff = float(ts) - time.time()
                nh = round(diff / 1800.0) * 1800.0
                if abs(nh) <= 14 * 3600 and abs(diff - nh) <= 90.0:
                    offsets.append(float(nh))
            except Exception:
                continue
        if offsets:
            offsets.sort()
            self._server_offset_sec = offsets[len(offsets) // 2]
        return self._server_offset_sec

    def to_utc_ts(self, server_ts):
        return float(server_ts) - float(getattr(self, "_server_offset_sec", 0.0))

    def tick(self, symbol):
        tick = self.named(f"mt5.symbol_info_tick({symbol!r})")
        bid = float(tick["bid"])
        ask = float(tick["ask"])
        raw_stamp = tick_timestamp(tick)
        self.update_server_offset(raw_stamp)
        stamp = self.to_utc_ts(raw_stamp)
        age = time.time() - stamp

        if (
            not math.isfinite(bid)
            or not math.isfinite(ask)
            or bid <= 0
            or ask < bid
            or age > CFG.MAX_TICK_AGE_SECONDS
            or age < -5
        ):
            raise SafetyError("A valid recent tick is unavailable.")

        return tick

    def positions(self):
        return self.records("mt5.positions_get()")

    def orders(self):
        return self.records("mt5.orders_get()")

    def position_deals(self, position_id):
        return self.records(
            f"mt5.history_deals_get(position={int(position_id)})"
        )

    def history(self, start, end):
        offset = float(getattr(self, "_server_offset_sec", 0.0))
        q_start = float(start) + min(0.0, offset) - 60.0
        q_end = float(end) + max(0.0, offset) + 60.0
        module = "__import__('datetime')"
        first = (
            f"{module}.datetime.fromtimestamp("
            f"{q_start}, tz={module}.timezone.utc)"
        )
        last = (
            f"{module}.datetime.fromtimestamp("
            f"{q_end}, tz={module}.timezone.utc)"
        )
        raw_deals = self.records(
            f"mt5.history_deals_get({first}, {last})"
        )
        return [
            d for d in raw_deals
            if float(start) <= self.to_utc_ts(float(d.get("time", 0.0))) <= float(end)
        ]

    def bars(self, symbol, timeframe, count):
        expression = (
            "(lambda values: None if values is None else ["
            "{'time': int(row['time']), "
            "'open': float(row['open']), "
            "'high': float(row['high']), "
            "'low': float(row['low']), "
            "'close': float(row['close']), "
            "'volume': float(row['tick_volume'])} "
            "for row in values])"
            "(mt5.copy_rates_from_pos("
            f"{symbol!r}, {int(timeframe)}, 1, {int(count)}))"
        )

        values = self.read(expression)

        if values is None or len(values) < count:
            raise SafetyError("Required closed-bar history is unavailable.")

        previous = None

        if values and getattr(self, "_server_offset_sec", 0.0) == 0.0:
            self.calibrate_server_offset()

        offset_int = int(round(getattr(self, "_server_offset_sec", 0.0)))
        for bar in values:
            bar["time"] = int(bar["time"]) - offset_int
            if previous is not None and bar["time"] <= previous:
                raise SafetyError("Invalid market-bar timestamps.")
            previous = bar["time"]

            numbers = [
                bar["open"], bar["high"], bar["low"], bar["close"]
            ]

            if any(
                not math.isfinite(x) or x <= 0
                for x in numbers
            ):
                raise SafetyError("Invalid market-bar prices.")

            if (
                bar["high"] < max(
                    bar["open"], bar["close"], bar["low"]
                )
                or bar["low"] > min(
                    bar["open"], bar["close"], bar["high"]
                )
            ):
                raise SafetyError("Invalid market-bar ranges.")

        return values

    def check(self, request):
        return self.read(
            "(lambda result: None if result is None else "
            "{'retcode': int(result.retcode)})"
            f"(mt5.order_check({request!r}))"
        )

    def send(self, request):
        return self.read(
            "(lambda result: None if result is None else "
            "{'retcode': int(result.retcode), "
            "'order': int(result.order), "
            "'deal': int(result.deal), "
            "'volume': float(result.volume), "
            "'price': float(result.price)})"
            f"(mt5.order_send({request!r}))"
        )

    def profit(self, order_type, symbol, volume, entry, stop):
        value = self.read(
            "mt5.order_calc_profit("
            f"{int(order_type)}, {symbol!r}, {float(volume)}, "
            f"{float(entry)}, {float(stop)})"
        )
        if value is None or not math.isfinite(float(value)):
            raise SafetyError("Profit calculation is unavailable.")
        return float(value)

    def margin(self, order_type, symbol, volume, price):
        value = self.read(
            "mt5.order_calc_margin("
            f"{int(order_type)}, {symbol!r}, "
            f"{float(volume)}, {float(price)})"
        )
        if value is None or not math.isfinite(float(value)):
            raise SafetyError("Margin calculation is unavailable.")
        return float(value)

    def market_book(self, symbol, ttl_sec=5.0):
        now = time.time()
        cached = self._dom_cache.get(symbol)
        if cached is not None and (now - cached[0]) <= ttl_sec:
            return cached[1]
        book = self.read(f"mt5.market_book_get({symbol!r})")
        self._dom_cache[symbol] = (now, book)
        return book

    def spread_limit_for(self, symbol):
        if symbol in CFG.MAX_SPREAD_BY_SYMBOL:
            return float(CFG.MAX_SPREAD_BY_SYMBOL[symbol])
        base_sym = self.base_symbol_map.get(symbol, symbol)
        return float(CFG.MAX_SPREAD_BY_SYMBOL.get(base_sym, CFG.MAX_SPREAD_POINTS))

    def select_symbols(self):
        valid_symbols = []
        base_map = {}
        disabled_mode = self.constant("SYMBOL_TRADE_MODE_DISABLED")
        closeonly_mode = self.constant("SYMBOL_TRADE_MODE_CLOSEONLY")

        for symbol in CFG.SYMBOLS:
            chosen = None
            for candidate_sym in (symbol, f"{symbol}m", f"{symbol}.a", f"{symbol}.pro", f"{symbol}.raw"):
                try:
                    success = self.read(f"mt5.symbol_select({candidate_sym!r}, True)")
                    if not success:
                        continue
                    info = self.info(candidate_sym)
                    if int(info.get("trade_mode", disabled_mode)) not in {disabled_mode, closeonly_mode}:
                        chosen = candidate_sym
                        break
                except Exception:
                    continue

            if chosen is not None:
                if chosen != symbol:
                    log(f"Auto-mapped symbol {symbol} -> {chosen} (tradable).")
                if chosen not in valid_symbols:
                    valid_symbols.append(chosen)
                    base_map[chosen] = symbol
            else:
                log(f"Symbol {symbol} is unavailable or disabled and will be skipped.", level=logging.WARNING)

        self.active_symbols = valid_symbols
        self.base_symbol_map = base_map
        if not self.active_symbols:
            raise FatalError("No valid symbols could be selected.")
        return list(self.active_symbols)

    def shutdown(self):
        self.mt5.shutdown()


def tick_timestamp(tick):
    stamp = float(tick.get("time_msc", 0)) / 1000.0
    if stamp <= 0:
        stamp = float(tick["time"])
    return stamp


def ema(values, period):
    if not values:
        return []

    result = [float(values[0])]
    alpha = 2.0 / (period + 1.0)

    for value in values[1:]:
        result.append(
            result[-1] + alpha * (float(value) - result[-1])
        )

    return result


def atr(bars, period=14):
    result = []

    for index, bar in enumerate(bars):
        true_range = bar["high"] - bar["low"]

        if index:
            previous_close = bars[index - 1]["close"]
            true_range = max(
                true_range,
                abs(bar["high"] - previous_close),
                abs(bar["low"] - previous_close),
            )

        if not result:
            result.append(true_range)
        else:
            result.append(
                result[-1] + (true_range - result[-1]) / period
            )

    return result


def swings(bars, window):
    highs = []
    lows = []

    for index in range(window, len(bars) - window):
        center = bars[index]
        neighbors = (
            bars[index - window:index]
            + bars[index + 1:index + window + 1]
        )

        item = {
            "index": index,
            "confirmed_index": index + window,
            "time": center["time"],
        }

        if all(center["high"] > x["high"] for x in neighbors):
            highs.append(dict(item, price=center["high"]))

        if all(center["low"] < x["low"] for x in neighbors):
            lows.append(dict(item, price=center["low"]))

    return highs, lows


def htf_bias(bars):
    if len(bars) < CFG.BIAS_EMA_PERIOD * 3:
        return "NEUTRAL"

    highs, lows = swings(bars, CFG.HTF_SWING_WINDOW)

    if len(highs) < 2 or len(lows) < 2:
        return "NEUTRAL"

    closes = [bar["close"] for bar in bars]
    close = closes[-1]
    ema50 = ema(closes, 50)[-1]
    ema200 = ema(closes, CFG.BIAS_EMA_PERIOD)[-1]

    bullish_swings = (
        highs[-1]["price"] > highs[-2]["price"]
        and lows[-1]["price"] > lows[-2]["price"]
    )
    bearish_swings = (
        highs[-1]["price"] < highs[-2]["price"]
        and lows[-1]["price"] < lows[-2]["price"]
    )

    fresh_bull_bos = (
        close > highs[-1]["price"]
        and lows[-1]["price"] >= lows[-2]["price"]
        and ema50 > ema200
    )
    fresh_bear_bos = (
        close < lows[-1]["price"]
        and highs[-1]["price"] <= highs[-2]["price"]
        and ema50 < ema200
    )

    if (bullish_swings or fresh_bull_bos) and close > ema200:
        pivot = lows[-1]
        broken = any(
            bar["close"] < pivot["price"]
            for bar in bars[pivot["index"] + 1:]
        )
        if not broken:
            return "BUY"

    if (bearish_swings or fresh_bear_bos) and close < ema200:
        pivot = highs[-1]
        broken = any(
            bar["close"] > pivot["price"]
            for bar in bars[pivot["index"] + 1:]
        )
        if not broken:
            return "SELL"

    return "NEUTRAL"

def find_extreme_swing_ob_index(bars, start_index, impulse_index, buy):
    search_start = max(0, start_index)
    search_end = impulse_index - 1
    if search_end < search_start:
        return None

    best_idx = search_end
    if buy:
        min_low = bars[search_end]["low"]
        for i in range(search_end, search_start - 1, -1):
            if bars[i]["low"] <= min_low:
                min_low = bars[i]["low"]
                best_idx = i
        if best_idx - 1 >= 0 and bars[best_idx - 1]["low"] < bars[best_idx]["low"]:
            best_idx -= 1
    else:
        max_high = bars[search_end]["high"]
        for i in range(search_end, search_start - 1, -1):
            if bars[i]["high"] >= max_high:
                max_high = bars[i]["high"]
                best_idx = i
        if best_idx - 1 >= 0 and bars[best_idx - 1]["high"] > bars[best_idx]["high"]:
            best_idx -= 1

    return best_idx


def first_structure_break(bars, index, side, volatility):
    prior = bars[:index]
    highs, lows = swings(prior, CFG.STRUCTURE_SWING_WINDOW)
    buy = side == "BUY"
    pivots = highs if buy else lows

    if not pivots:
        return None

    pivot = dict(pivots[-1])
    level = float(pivot["price"])
    close = bars[index]["close"]
    buffer = CFG.BOS_BUFFER_ATR * volatility

    intervening = bars[pivot["confirmed_index"] + 1:index]

    if buy:
        for bar in intervening:
            if bar["close"] > level:
                return None
            if bar["high"] > level and bar["close"] <= level:
                level = float(bar["high"])
        valid = close > level + buffer
    else:
        for bar in intervening:
            if bar["close"] < level:
                return None
            if bar["low"] < level and bar["close"] >= level:
                level = float(bar["low"])
        valid = close < level - buffer

    if not valid:
        return None

    pivot["effective_level"] = level
    return pivot

def check_harmonic_bat_pattern(bars, side):
    highs, lows = swings(bars, CFG.STRUCTURE_SWING_WINDOW)
    if len(highs) < 3 or len(lows) < 3:
        return False

    buy = side == "BUY"

    if buy:
        X = lows[-3]["price"]
        A = highs[-2]["price"]
        B = lows[-2]["price"]
        C = highs[-1]["price"]
        D = bars[-1]["low"]

        if not (lows[-3]["index"] < highs[-2]["index"] < lows[-2]["index"] < highs[-1]["index"]):
            return False

        XA = A - X
        AB = A - B

        if XA <= 0 or AB <= 0 or C > A:
            return False

        b_ratio = (A - B) / XA
        c_ratio = (C - B) / AB
        d_ratio = (A - D) / XA

        valid_b = 0.45 <= b_ratio <= 0.65
        valid_c = 0.35 <= c_ratio <= 0.90
        valid_d = 0.75 <= d_ratio <= 0.90

        return valid_b and valid_c and valid_d
    else:
        X = highs[-3]["price"]
        A = lows[-2]["price"]
        B = highs[-2]["price"]
        C = lows[-1]["price"]
        D = bars[-1]["high"]

        if not (highs[-3]["index"] < lows[-2]["index"] < highs[-2]["index"] < lows[-1]["index"]):
            return False

        XA = X - A
        AB = B - A

        if XA <= 0 or AB <= 0 or C < A:
            return False

        b_ratio = (B - A) / XA
        c_ratio = (B - C) / AB
        d_ratio = (D - A) / XA

        valid_b = 0.45 <= b_ratio <= 0.65
        valid_c = 0.35 <= c_ratio <= 0.90
        valid_d = 0.75 <= d_ratio <= 0.90

        return valid_b and valid_c and valid_d

def calculate_scoring_logic(broker, bars, zone, sweep_bar, confirmation_bar, reference_atr, pdh=None, pdl=None, conf_idx=None):
    score = 0.0

    if zone.get("is_mitigation", False):
        score += getattr(CFG, "MITIGATION_SCORE_BOOST", 2.0)

    if zone.get("confluence", False):
        score += 3.0
    else:
        score += 1.5

    bar_range = sweep_bar["high"] - sweep_bar["low"]
    if bar_range > 0:
        buy = zone["side"] == "BUY"
        wick_ratio = (
            (min(sweep_bar["open"], sweep_bar["close"]) - sweep_bar["low"]) / bar_range
            if buy
            else (sweep_bar["high"] - max(sweep_bar["open"], sweep_bar["close"])) / bar_range
        )
        if wick_ratio >= 0.40:
            score += 2.5
        elif wick_ratio >= 0.20:
            score += 1.0

    vol_ma = volume_ma(bars, CFG.VOLUME_MA_PERIOD)
    if conf_idx is None:
        conf_idx = bars.index(confirmation_bar) if confirmation_bar in bars else -1
    if conf_idx >= 0 and conf_idx < len(vol_ma):
        vm = vol_ma[conf_idx]
        if vm > 0 and confirmation_bar["volume"] >= vm * CFG.SWEEP_CONFIRM_VOLUME_MULTIPLIER:
            score += 2.5

    if pdh is not None and pdl is not None:
        if zone["side"] == "SELL" and abs(sweep_bar["high"] - pdh) <= reference_atr * 1.5:
            score += 2.0
        elif zone["side"] == "BUY" and abs(sweep_bar["low"] - pdl) <= reference_atr * 1.5:
            score += 2.0

    if getattr(CFG, "ENABLE_HARMONIC_BAT", True):
        bars_upto_conf = bars[:conf_idx + 1] if (conf_idx is not None and conf_idx >= 0) else bars
        if check_harmonic_bat_pattern(bars_upto_conf, zone["side"]):
            score += getattr(CFG, "HARMONIC_SCORE_BOOST", 2.0)

    symbol = zone.get("symbol", "EURUSD")
    flow_bars = bars[:conf_idx] if conf_idx > 0 else None
    order_flow_points = analyze_order_flow(broker, symbol, zone["side"], confirmation_bar, flow_bars)
    score += order_flow_points

    score += 1.5
    return min(score, 12.0)

def create_zones(symbol, bars, side):
    volatility = atr(bars)
    buy = side == "BUY"
    result = []

    first = max(CFG.DEALING_RANGE_BARS + 1, len(bars) - CFG.ZONE_LOOKBACK)

    for final_index in range(first, len(bars)):
        impulse_index = final_index - 1

        if impulse_index < CFG.DEALING_RANGE_BARS:
            continue

        impulse = bars[impulse_index]
        left = bars[final_index - 2]
        right = bars[final_index]
        reference_atr = volatility[impulse_index - 1]

        if reference_atr <= 0:
            continue

        body = abs(impulse["close"] - impulse["open"])
        candle_range = impulse["high"] - impulse["low"]
        directional = impulse["close"] > impulse["open"] if buy else impulse["close"] < impulse["open"]

        single_ok = (
            directional
            and candle_range > 0
            and body >= (CFG.DISPLACEMENT_ATR * 0.85 * reference_atr)
            and (body / candle_range) >= (CFG.DISPLACEMENT_BODY_RATIO * 0.90)
        )
        two_bar_ok = False
        if not single_ok and directional and candle_range > 0:
            prev_imp = bars[impulse_index - 1]
            prev_dir = prev_imp["close"] > prev_imp["open"] if buy else prev_imp["close"] < prev_imp["open"]
            combined_body = body + abs(prev_imp["close"] - prev_imp["open"])
            if prev_dir and combined_body >= (CFG.DISPLACEMENT_ATR * 1.35 * reference_atr) and (body / candle_range) >= 0.45:
                two_bar_ok = True

        if not (single_ok or two_bar_ok):
            continue

        pivot = first_structure_break(bars, impulse_index, side, reference_atr)
        if pivot is None:
            continue

        fvg_bottom = right["high"] if not buy else left["high"]
        fvg_top = left["low"] if not buy else right["low"]
        fvg_size = fvg_top - fvg_bottom
        has_fvg = fvg_size >= (CFG.MIN_FVG_ATR * reference_atr)

        local_ob_index = None
        for source in range(impulse_index - 1, max(-1, impulse_index - CFG.OB_SEARCH_BACK - 1), -1):
            source_bar = bars[source]
            opposite = source_bar["close"] < source_bar["open"] if buy else source_bar["close"] > source_bar["open"]
            if opposite:
                local_ob_index = source
                break

        swing_start = max(0, min(int(pivot["index"]), impulse_index - CFG.OB_SEARCH_BACK))
        extreme_ob_index = find_extreme_swing_ob_index(bars, swing_start, impulse_index, buy)
        ob_index = extreme_ob_index if extreme_ob_index is not None else local_ob_index

        dealing = bars[impulse_index - CFG.DEALING_RANGE_BARS:impulse_index + 1]
        range_high = max(bar["high"] for bar in dealing)
        range_low = min(bar["low"] for bar in dealing)
        width = range_high - range_low

        if width <= 0:
            continue

        eq_buffer = width * CFG.PD_EQUILIBRIUM_BUFFER
        equilibrium = (range_high + range_low) / 2.0

        if buy:
            pd_zone_top = equilibrium - eq_buffer
            pd_zone_bottom = range_low - (reference_atr * 0.5)
        else:
            pd_zone_bottom = equilibrium + eq_buffer
            pd_zone_top = range_high + (reference_atr * 0.5)

        created = int(right["time"] + 900)
        bos_level_val = float(pivot.get("effective_level", pivot["price"]))

        def add_zone(kind, bottom, top, source_index, is_mitigation=False, confluence=False):
            if bottom >= top:
                return

            midpoint = (bottom + top) / 2.0

            if not (pd_zone_bottom <= midpoint <= pd_zone_top):
                return

            subsequent_m15 = bars[final_index + 1 : -3] if final_index + 1 < len(bars) - 3 else []
            inv_buf = reference_atr * 0.15
            prior_deep_touches = 0
            for m_bar in subsequent_m15:
                if buy:
                    if m_bar["close"] < bottom - inv_buf:
                        return
                    if m_bar["low"] <= midpoint:
                        prior_deep_touches += 1
                else:
                    if m_bar["close"] > top + inv_buf:
                        return
                    if m_bar["high"] >= midpoint:
                        prior_deep_touches += 1

            if prior_deep_touches >= 2:
                return

            key = f"{side}:{kind}:{bars[source_index]['time']}:{created}"
            result.append({
                "kind": kind,
                "side": side,
                "symbol": symbol,
                "bottom": float(bottom),
                "top": float(top),
                "created": created,
                "source_time": int(bars[source_index]["time"]),
                "local_id": key,
                "equilibrium": float(equilibrium),
                "bos_level": bos_level_val,
                "is_mitigation": bool(is_mitigation),
                "confluence": bool(confluence),
                "is_fresh": (prior_deep_touches == 0),
            })

        ob_candidates = []
        if extreme_ob_index is not None:
            ob_candidates.append(extreme_ob_index)
        if local_ob_index is not None and local_ob_index not in ob_candidates:
            ob_candidates.append(local_ob_index)

        for cand_idx in ob_candidates:
            source_bar = bars[cand_idx]
            ob_atr = volatility[cand_idx] if volatility[cand_idx] > 0 else reference_atr

            if buy:
                ob_bottom = source_bar["low"]
                ob_top = min(source_bar["high"], ob_bottom + (ob_atr * 1.2))
            else:
                ob_top = source_bar["high"]
                ob_bottom = max(source_bar["low"], ob_top - (ob_atr * 1.2))

            path_after_ob = bars[cand_idx + 1 : impulse_index]
            broken_ob = False
            for b in path_after_ob:
                if buy and b["low"] < ob_bottom:
                    broken_ob = True
                    break
                elif not buy and b["high"] > ob_top:
                    broken_ob = True
                    break

            if broken_ob and CFG.ENABLE_MITIGATION_BLOCK:
                add_zone(
                    "MITIGATION_BLOCK",
                    ob_bottom,
                    ob_top,
                    cand_idx,
                    is_mitigation=True,
                    confluence=has_fvg,
                )
            elif not broken_ob:
                add_zone(
                    "OB",
                    ob_bottom,
                    ob_top,
                    cand_idx,
                    is_mitigation=False,
                    confluence=has_fvg,
                )

        if has_fvg:
            add_zone(
                "FVG",
                fvg_bottom,
                fvg_top,
                impulse_index,
                is_mitigation=False,
                confluence=(ob_index is not None),
            )

    unique = {zone["local_id"]: zone for zone in result}
    return sorted(unique.values(), key=lambda zone: zone["created"], reverse=True)

def zone_signal(zone, bars, volatility, pdh=None, pdl=None, broker=None):
    if not bars or bars[0]["time"] > zone["created"]:
        return None

    latest = len(bars) - 1
    decision_time = bars[-1]["time"] + 300

    base_score_modifier = 0.0

    if decision_time - zone["created"] > CFG.MAX_ZONE_AGE_M15_BARS * 900:
        base_score_modifier -= 2.0

    buy = zone["side"] == "BUY"
    eligible = [index for index, bar in enumerate(bars) if bar["time"] >= zone["created"]]
    if not eligible:
        return None

    first_touch = None
    for index in eligible:
        bar = bars[index]
        reached = bar["low"] <= zone["top"] if buy else bar["high"] >= zone["bottom"]
        if reached:
            first_touch = index
            break

    if first_touch is None:
        return None

    invalidation_buffer = volatility[first_touch] * 0.15 if volatility[first_touch] > 0 else 0.0
    if any(
        (bar["close"] < zone["bottom"] - invalidation_buffer if buy else bar["close"] > zone["top"] + invalidation_buffer)
        for bar in bars[first_touch:]
    ):
        return None

    recent_window_start = max(first_touch, latest - (CFG.SWEEP_AFTER_TOUCH_BARS + CFG.CONFIRM_AFTER_SWEEP_BARS))
    recent_touches = [
        idx for idx in range(recent_window_start, latest + 1)
        if (bars[idx]["low"] <= zone["top"] if buy else bars[idx]["high"] >= zone["bottom"])
    ]
    touch = recent_touches[0] if recent_touches else first_touch

    if latest - touch > CFG.MAX_TOUCH_WAIT_BARS:
        base_score_modifier -= 1.5

    sweep_end = min(touch + CFG.SWEEP_AFTER_TOUCH_BARS, latest - 1)

    for sweep_index in range(touch, sweep_end + 1):
        before = bars[:sweep_index]
        highs, lows = swings(before, CFG.LTF_SWING_WINDOW)
        if len(highs) < 2 or len(lows) < 2:
            continue

        sweep_bar = bars[sweep_index]
        reference_atr = volatility[sweep_index - 1]
        if reference_atr <= 0:
            continue

        overlaps = sweep_bar["low"] <= zone["top"] and sweep_bar["high"] >= zone["bottom"]
        if not overlaps:
            continue

        sweep_modifier = base_score_modifier

        if not sweep_wick_ok(sweep_bar, zone["side"]):
            sweep_modifier -= 1.0

        if buy:
            counter_structure = highs[-1]["price"] < highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]
            liquidity = lows[-1]
            confirmation = highs[-1]
        else:
            counter_structure = highs[-1]["price"] > highs[-2]["price"] and lows[-1]["price"] > lows[-2]["price"]
            liquidity = highs[-1]
            confirmation = lows[-1]

        if not counter_structure:
            sweep_modifier -= 1.5

        if sweep_index - liquidity["index"] > CFG.LIQUIDITY_MAX_AGE:
            sweep_modifier -= 1.0

        intervening = bars[liquidity["confirmed_index"] + 1:sweep_index]
        already_taken = any(
            bar["low"] <= liquidity["price"] if buy else bar["high"] >= liquidity["price"]
            for bar in intervening
        )
        if already_taken:
            sweep_modifier -= 1.5

        dynamic_sweep_min = get_dynamic_sweep_atr(zone["symbol"] if "symbol" in zone else "EURUSD", reference_atr)

        if buy:
            swept = (
                sweep_bar["low"] <= liquidity["price"] - dynamic_sweep_min
                and sweep_bar["close"] > liquidity["price"]
            )
            zone_mitigated = (
                sweep_bar["low"] <= zone["top"]
                and sweep_bar["close"] >= zone["bottom"]
            )
        else:
            swept = (
                sweep_bar["high"] >= liquidity["price"] + dynamic_sweep_min
                and sweep_bar["close"] < liquidity["price"]
            )
            zone_mitigated = (
                sweep_bar["high"] >= zone["bottom"]
                and sweep_bar["close"] <= zone["top"]
            )

        if not swept:
            if not zone_mitigated:
                continue
            sweep_modifier -= 1.5

        level = float(confirmation["price"])
        confirmation_end = min(sweep_index + CFG.CONFIRM_AFTER_SWEEP_BARS, latest)

        for confirmation_index in range(sweep_index + 1, confirmation_end + 1):
            bar = bars[confirmation_index]
            raw_break = bar["close"] > level if buy else bar["close"] < level
            if not raw_break:
                if buy and bar["high"] > level and bar["close"] <= level:
                    level = float(bar["high"])
                elif not buy and bar["low"] < level and bar["close"] >= level:
                    level = float(bar["low"])
                continue

            if confirmation_index < latest:
                break

            m5_closes_up_to_conf = [b["close"] for b in bars[:confirmation_index + 1]]
            if len(m5_closes_up_to_conf) >= 20:
                m5_ema20_now = ema(m5_closes_up_to_conf, 20)[-1]
                if buy and bar["close"] < m5_ema20_now:
                    continue
                if not buy and bar["close"] > m5_ema20_now:
                    continue

            conf_modifier = sweep_modifier
            reference = volatility[confirmation_index - 1]
            close_buffer = CFG.CONFIRM_CLOSE_BUFFER_ATR * reference
            body = abs(bar["close"] - bar["open"])

            strong_close = (
                (bar["close"] > level + close_buffer and bar["close"] > bar["open"])
                if buy
                else (bar["close"] < level - close_buffer and bar["close"] < bar["open"])
            )

            if not strong_close:
                conf_modifier -= 1.0
            if body < CFG.CONFIRM_MIN_BODY_ATR * reference:
                conf_modifier -= 1.0

            score = calculate_scoring_logic(broker, bars, zone, sweep_bar, bar, reference, pdh, pdl, conf_idx=confirmation_index)
            final_score = score + conf_modifier

            if final_score < CFG.MIN_SCORING_THRESHOLD:
                break

            path = bars[touch:confirmation_index + 1]
            extreme = min(item["low"] for item in path) if buy else max(item["high"] for item in path)

            recent_vol_floor = max(volatility[-1], max(volatility[-24:]) * 0.85) if len(volatility) >= 24 else volatility[-1]
            return {
                "zone": zone,
                "side": zone["side"],
                "extreme": extreme,
                "atr": volatility[-1],
                "sl_floor_atr": recent_vol_floor,
                "signal_close": bar["close"],
                "signal_time": int(bar["time"] + 300),
                "sweep_time": int(sweep_bar["time"] + 300),
                "sweep_open_time": int(sweep_bar["time"]),
                "confirmation_level": level,
                "score": final_score,
            }

    return None

def m15_confirmation(candidate, bars):
    if not CFG.ENABLE_M15_SCORE_BOOST:
        return 0.0

    if not bars:
        return 0.0

    latest = bars[-1]
    buy = candidate["side"] == "BUY"
    
    if buy and latest["close"] >= latest["open"]:
        return getattr(CFG, "M15_SCORE_BOOST_VALUE", 1.5)
    elif not buy and latest["close"] <= latest["open"]:
        return getattr(CFG, "M15_SCORE_BOOST_VALUE", 1.5)

    return 0.0

def get_news_sentiment_score(symbol, side, m5_bars=None):
    if not getattr(CFG, "ENABLE_NEWS_SCORING_PENALTY", True):
        return 0.0

    from bot_news import parse_economic_number, is_inverse_indicator

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    base_currency, quote_currency, relevant_currencies = SHARED_NEWS_FETCHER.map_symbol_currencies(symbol)
    if not relevant_currencies:
        return 0.0

    post_window = float(getattr(CFG, "NEWS_POST_SENTIMENT_MINUTES", 90.0))
    full_boost_mins = float(getattr(CFG, "NEWS_FULL_BOOST_MINUTES", 40.0))
    sentiment_score = 0.0
    max_boost = float(getattr(CFG, "NEWS_SCORE_BOOST_HIGH", 3.0)) * 1.15
    max_penalty = float(getattr(CFG, "NEWS_HIGH_IMPACT_PENALTY", 4.0))
    seen_price_fallback_ts = set()

    with SHARED_NEWS_FETCHER._lock:
        for news in SHARED_NEWS_FETCHER.high_impact_news:
            cur = news.get("currency")
            if cur not in relevant_currencies:
                continue

            news_time = news.get("time")
            if not news_time:
                continue

            minutes_since_release = (now - news_time).total_seconds() / 60.0
            if not (0.0 <= minutes_since_release <= post_window):
                continue

            impact = news.get("impact", "Medium")
            title = news.get("title", "")
            act_val = parse_economic_number(news.get("actual"))
            ref_val = parse_economic_number(news.get("forecast"))
            if ref_val is None:
                ref_val = parse_economic_number(news.get("previous"))

            favorable_side = "NEUTRAL"
            surprise_mult = 1.0

            if act_val is not None and ref_val is not None and abs(act_val - ref_val) > 1e-12:
                inverse = is_inverse_indicator(title)
                if act_val > ref_val:
                    currency_bias = "BEARISH" if inverse else "BULLISH"
                else:
                    currency_bias = "BULLISH" if inverse else "BEARISH"

                is_base = (cur == base_currency)
                if currency_bias == "BULLISH":
                    favorable_side = "BUY" if is_base else "SELL"
                else:
                    favorable_side = "SELL" if is_base else "BUY"

                denom = max(abs(ref_val), 0.20)
                rel_surprise = abs(act_val - ref_val) / denom
                if rel_surprise >= 0.40:
                    surprise_mult = 1.15
                elif rel_surprise >= 0.15:
                    surprise_mult = 1.0
                else:
                    surprise_mult = 0.70

            elif m5_bars and len(m5_bars) >= 3:
                release_ts = int(news_time.replace(tzinfo=timezone.utc).timestamp())
                if release_ts in seen_price_fallback_ts:
                    continue
                seen_price_fallback_ts.add(release_ts)
                reaction_bars = [b for b in m5_bars if release_ts <= b["time"] <= release_ts + 900]
                if reaction_bars:
                    open_p = reaction_bars[0]["open"]
                    close_p = reaction_bars[-1]["close"]
                    net_move = close_p - open_p
                    ref_range = sum(b["high"] - b["low"] for b in m5_bars[-20:]) / 20.0
                    if ref_range > 0 and abs(net_move) >= ref_range * 1.2:
                        favorable_side = "BUY" if net_move > 0 else "SELL"
                        if abs(net_move) >= ref_range * 2.0:
                            surprise_mult = 1.15

            if minutes_since_release <= full_boost_mins or post_window <= full_boost_mins:
                time_decay = 1.0
            else:
                remaining_ratio = (post_window - minutes_since_release) / (post_window - full_boost_mins)
                time_decay = max(0.25, min(1.0, remaining_ratio))

            base_boost = getattr(CFG, "NEWS_SCORE_BOOST_HIGH", 3.0) if impact == "High" else getattr(CFG, "NEWS_SCORE_BOOST_MED", 1.5)
            boost_val = base_boost * surprise_mult * time_decay
            penalty_val = (CFG.NEWS_HIGH_IMPACT_PENALTY if impact == "High" else CFG.NEWS_MEDIUM_IMPACT_PENALTY) * max(0.5, time_decay)

            if favorable_side == side:
                sentiment_score += boost_val
            elif favorable_side != "NEUTRAL" and favorable_side != side:
                sentiment_score -= penalty_val

    return max(-max_penalty, min(sentiment_score, max_boost))

def news_permission(broker, symbol):
    if not CFG.ENABLE_NEWS_FILTER:
        return True, "News filter disabled."

    if SHARED_NEWS_FETCHER.is_restricted_time(symbol):
        return False, "A high/medium-impact news blackout window is active."

    return True, "News checks passed."

def scan_symbol(broker, symbol, state):
    m5 = broker.bars(
        symbol, broker.constant("TIMEFRAME_M5"), CFG.M5_BAR_COUNT
    )

    decision_time = int(m5[-1]["time"] + 300)
    age = time.time() - decision_time

    if age < 0 or age > CFG.MAX_CLOSED_M5_AGE_SECONDS:
        return None

    h4 = broker.bars(
        symbol, broker.constant("TIMEFRAME_H4"), CFG.H4_BAR_COUNT
    )
    m15 = broker.bars(
        symbol, broker.constant("TIMEFRAME_M15"), CFG.M15_BAR_COUNT
    )

    pdh, pdl = calculate_pdh_pdl(broker, symbol)

    h4 = [bar for bar in h4 if bar["time"] + 14400 <= decision_time]
    m15 = [bar for bar in m15 if bar["time"] + 900 <= decision_time]

    regime = detect_market_regime(h4, m15)
    side = htf_bias(h4)

    if side == "NEUTRAL" or len(m15) < CFG.DEALING_RANGE_BARS + 10:
        SYMBOL_RADAR_STATE[symbol] = {
            "category": "BLOCKED",
            "side": "NEUTRAL",
            "regime": regime,
            "stage": "H4_NEUTRAL",
            "detail": "H4 structure neutral",
        }
        return None

    volatility = atr(m5)
    current_price = m5[-1]["close"]
    ref_m5_atr = volatility[-1] if volatility and volatility[-1] > 0 else 0.0

    m15_closes = [bar["close"] for bar in m15]
    if len(m15_closes) >= 200:
        m15_ema200_now = ema(m15_closes, 200)[-1]
        if side == "BUY" and (m15[-1]["close"] < m15_ema200_now or current_price < m15_ema200_now):
            SYMBOL_RADAR_STATE[symbol] = {
                "category": "BLOCKED",
                "side": side,
                "regime": regime,
                "stage": "M15_EMA200_CONFLICT",
                "detail": "H4 BUY vs M15 below EMA200",
            }
            return None
        if side == "SELL" and (m15[-1]["close"] > m15_ema200_now or current_price > m15_ema200_now):
            SYMBOL_RADAR_STATE[symbol] = {
                "category": "BLOCKED",
                "side": side,
                "regime": regime,
                "stage": "M15_EMA200_CONFLICT",
                "detail": "H4 SELL vs M15 above EMA200",
            }
            return None

    if regime == "RANGING_SIDEWAY":
        SYMBOL_RADAR_STATE[symbol] = {
            "category": "BLOCKED",
            "side": side,
            "regime": regime,
            "stage": "RANGING_SIDEWAY",
            "detail": "Sideways / low efficiency",
        }
        return None

    candidates = []

    news_catalyst = get_news_sentiment_score(symbol, side, m5_bars=m5)

    m15_counter_knife = False
    if regime == "RETRACEMENT_SMC" and len(m15) >= 15:
        m15_atr_vals = atr(m15)
        ref_m15_atr = m15_atr_vals[-2] if len(m15_atr_vals) >= 2 and m15_atr_vals[-2] > 0 else (m15_atr_vals[-1] if m15_atr_vals else 0.0)
        max_counter_atr = float(getattr(CFG, "MAX_COUNTER_M15_IMPULSE_ATR", 1.20))
        if ref_m15_atr > 0:
            last_m15 = m15[-1]
            prev_m15 = m15[-2]
            if side == "BUY":
                last_bear_body = max(0.0, last_m15["open"] - last_m15["close"])
                two_bar_bear_drop = max(0.0, prev_m15["open"] - last_m15["close"]) if (last_m15["close"] < last_m15["open"] and prev_m15["close"] < prev_m15["open"]) else 0.0
                if last_bear_body >= ref_m15_atr * max_counter_atr or two_bar_bear_drop >= ref_m15_atr * (max_counter_atr * 1.35):
                    m15_counter_knife = True
            else:
                last_bull_body = max(0.0, last_m15["close"] - last_m15["open"])
                two_bar_bull_jump = max(0.0, last_m15["close"] - prev_m15["open"]) if (last_m15["close"] > last_m15["open"] and prev_m15["close"] > prev_m15["open"]) else 0.0
                if last_bull_body >= ref_m15_atr * max_counter_atr or two_bar_bull_jump >= ref_m15_atr * (max_counter_atr * 1.35):
                    m15_counter_knife = True

    active_zones = []
    if regime != "RANGING_SIDEWAY" and not m15_counter_knife:
        for zone in create_zones(symbol, m15, side):
            zone_id = f"{symbol}:{zone['local_id']}"

            if zone_id in state["seen_zones"]:
                continue

            invalidation_buf = ref_m5_atr * 0.15
            eligible_m5 = [b for b in m5 if b["time"] >= zone["created"]]
            if eligible_m5:
                broken_zone = any(
                    (b["close"] < zone["bottom"] - invalidation_buf if side == "BUY" else b["close"] > zone["top"] + invalidation_buf)
                    for b in eligible_m5
                )
                if not broken_zone:
                    active_zones.append(zone)

            sig = zone_signal(zone, m5, volatility, pdh, pdl, broker=broker)
            if sig is not None:
                sig["zone_id"] = zone_id
                candidates.append(("smc", sig))
                if len(candidates) >= 6:
                    break

    if regime == "STRONG_TREND" or (regime == "RETRACEMENT_SMC" and news_catalyst >= 1.5):
        mom = scan_momentum_breakout_candidate(broker, symbol, m5, side, volatility, m15_bars=m15)
        if mom is not None and mom["side"] == side:
            candidates.append(("momentum", mom))

    def _record_waiting_radar():
        cat = "STRONG" if regime == "STRONG_TREND" else "NORMAL"
        if m15_counter_knife:
            SYMBOL_RADAR_STATE[symbol] = {
                "category": cat,
                "side": side,
                "regime": regime,
                "stage": "ANTI_KNIFE_WAIT",
                "detail": f"{symbol} [{side}] | Waiting M15 counter-impulse to cool down",
            }
            return
        if active_zones:
            def _z_dist(z):
                if z["bottom"] <= current_price <= z["top"]:
                    return 0.0
                return min(abs(current_price - z["top"]), abs(current_price - z["bottom"]))
            nearest_z = min(active_zones, key=_z_dist)
            dist = _z_dist(nearest_z)
            dist_atr = (dist / ref_m5_atr) if ref_m5_atr > 0 else 0.0
            z_kind = "MIT" if nearest_z["kind"] == "MITIGATION_BLOCK" else nearest_z["kind"]
            if dist == 0.0 or dist_atr <= 0.35:
                stage = f"IN_ZONE:{z_kind}"
                detail = f"{symbol} [{side}] | In {z_kind} zone -> waiting M5 sweep/EMA20 close"
            elif dist_atr <= 2.5:
                stage = f"NEAR_ZONE:{z_kind}"
                detail = f"{symbol} [{side}] | Near {z_kind} ({dist_atr:.1f} ATR away)"
            else:
                stage = f"WAIT_PULLBACK:{z_kind}"
                detail = f"{symbol} [{side}] | Waiting pullback to {z_kind} ({dist_atr:.1f} ATR)"
        else:
            if regime == "STRONG_TREND":
                stage = "WAIT_BREAKOUT_OR_ZONE"
                detail = f"{symbol} [{side}] | Strong trend -> watching 1H box breakout / new zone"
            else:
                stage = "WAIT_NEW_ZONE"
                detail = f"{symbol} [{side}] | Trend aligned -> waiting new M15 OB/FVG"
        SYMBOL_RADAR_STATE[symbol] = {
            "category": cat,
            "side": side,
            "regime": regime,
            "stage": stage,
            "detail": detail,
        }

    if not candidates:
        _record_waiting_radar()
        return None

    for kind, candidate in candidates:
        m15_bonus = m15_confirmation(candidate, m15)
        score_boost = m15_bonus

        if kind == "momentum" and pdh is not None and pdl is not None:
            if candidate["side"] == "BUY" and current_price >= pdh - (volatility[-1] * 0.5):
                score_boost += 1.5
            elif candidate["side"] == "SELL" and current_price <= pdl + (volatility[-1] * 0.5):
                score_boost += 1.5

        if kind == "smc":
            try:
                if check_trendline_liquidity_sweep(m5, candidate["side"]):
                    score_boost += 1.5
            except Exception:
                pass

        if is_prime_killzone_utc():
            score_boost += 0.75

        candidate["score"] = candidate.get("score", 6.0) + score_boost

        news_sentiment = (
            news_catalyst
            if candidate["side"] == side
            else get_news_sentiment_score(symbol, candidate["side"], m5_bars=m5)
        )
        candidate["score"] += news_sentiment

    mom_min_threshold = max(CFG.MIN_SCORING_THRESHOLD + 1.5, 7.5)
    eligible = [
        (k, c) for (k, c) in candidates
        if c["score"] >= (CFG.MIN_SCORING_THRESHOLD if k == "smc" else mom_min_threshold)
    ]
    if not eligible:
        _record_waiting_radar()
        return None

    mom_pool = [c for (k, c) in eligible if k == "momentum"]
    smc_pool = [c for (k, c) in eligible if k == "smc"]

    best_mom = max(mom_pool, key=lambda c: c["score"]) if mom_pool else None
    best_smc = max(smc_pool, key=lambda c: c["score"]) if smc_pool else None

    if best_smc is not None and best_mom is not None:
        if best_mom["score"] >= best_smc["score"] + 1.5:
            chosen = best_mom
        else:
            chosen = best_smc
    else:
        chosen = best_smc if best_smc is not None else best_mom

    h4_highs, h4_lows = swings(h4, CFG.HTF_SWING_WINDOW)
    structural_Resistance = []
    structural_support = []
    if pdh is not None:
        structural_Resistance.append(float(pdh))
    if pdl is not None:
        structural_support.append(float(pdl))
    for h_item in h4_highs[-4:]:
        structural_Resistance.append(float(h_item["price"]))
    for l_item in h4_lows[-4:]:
        structural_support.append(float(l_item["price"]))

    chosen["symbol"] = symbol
    chosen["regime"] = regime
    chosen["structural_highs"] = structural_Resistance
    chosen["structural_lows"] = structural_support
    SYMBOL_RADAR_STATE[symbol] = {
        "category": "SIGNAL_READY",
        "side": chosen["side"],
        "regime": regime,
        "stage": f"SIGNAL:{chosen['zone']['kind']}",
        "detail": f"{symbol} [{chosen['side']}] | READY ({chosen['zone']['kind']} Score {chosen['score']:.1f})",
    }
    return chosen

def dynamic_risk_budget(account, score=6.0, peak_balance=None):
    balance = float(account.get("balance", 0.0))
    equity = float(account.get("equity", balance))
    effective_capital = min(balance, equity) if (balance > 0 and equity > 0) else max(balance, equity)

    if not getattr(CFG, "USE_DYNAMIC_RISK", True) or effective_capital <= 0:
        return float(CFG.RISK_PER_TRADE)

    base_pct = float(getattr(CFG, "RISK_PCT", 1.25))
    if effective_capital >= 10000.0:
        tier_pct = min(base_pct, 1.0)
    elif effective_capital >= 3000.0:
        tier_pct = min(base_pct, 1.15)
    else:
        tier_pct = base_pct

    dd_mult = 1.0
    if peak_balance is not None and float(peak_balance) > effective_capital:
        peak_val = float(peak_balance)
        dd_ratio = (peak_val - effective_capital) / peak_val
        slope = float(getattr(CFG, "DRAWDOWN_THROTTLE_SLOPE", 5.0))
        dd_mult = max(0.50, 1.0 - (dd_ratio * slope))

    conviction_mult = 1.0 if score >= 9.5 else (0.92 if score >= 7.5 else 0.85)
    raw_budget = effective_capital * (tier_pct / 100.0) * conviction_mult * dd_mult
    return max(0.50, raw_budget)


def dynamic_max_open_positions(account):
    base_max = int(CFG.MAX_OPEN_POSITIONS)
    balance = float(account.get("balance", 0.0))
    if balance <= 0:
        return base_max
    if balance < 80.0:
        return min(base_max, 1)
    if balance < 200.0:
        return min(base_max, 2)
    return base_max


def dynamic_daily_trade_limit(snapshot):
    base_limit = int(CFG.MAX_DAILY_TRADES)
    if not getattr(CFG, "USE_DYNAMIC_DAILY_TRADES", True):
        return base_limit

    account = snapshot.get("account", {})
    balance = float(account.get("balance", 0.0))
    daily_pnl = float(snapshot.get("daily_pnl", 0.0))
    if balance <= 0:
        return base_limit

    pnl_pct = (daily_pnl / balance) * 100.0
    if pnl_pct >= 2.5:
        return max(base_limit, min(base_limit + 2, 10))
    if pnl_pct >= 0.5:
        return max(base_limit, 8)
    if pnl_pct <= -2.5:
        return max(4, base_limit - 2)
    return base_limit


def dynamic_tp_rr(candidate):
    base_tp2 = float(CFG.TP2_RR)
    if not getattr(CFG, "USE_DYNAMIC_TP_SCALING", True):
        return base_tp2

    regime = candidate.get("regime", "RETRACEMENT_SMC")
    score = float(candidate.get("score", 7.0))
    kind = candidate.get("zone", {}).get("kind", "OB")

    if regime == "STRONG_TREND" and score >= 11.0:
        return max(base_tp2, 3.2)
    if regime == "STRONG_TREND" or score >= 11.0:
        return max(base_tp2, 2.8)
    if regime == "RETRACEMENT_SMC" and score < 7.5 and kind != "MOMENTUM_BIGBELUGA":
        return min(base_tp2, 2.2)
    return base_tp2


def floor_volume(volume, step):
    if step <= 0 or volume <= 0:
        return 0.0

    number = Decimal(str(volume))
    increment = Decimal(str(step))
    units = (number / increment).to_integral_value(
        rounding=ROUND_FLOOR
    )
    return float(units * increment)


def price_round(price, info, upward=False):
    increment = float(
        info.get("trade_tick_size") or info["point"]
    )

    if increment <= 0 or not math.isfinite(price):
        raise SafetyError("Price normalization failed.")

    units = price / increment

    if upward:
        units = math.ceil(units - 1e-9)
    else:
        units = math.floor(units + 1e-9)

    return round(units * increment, int(info["digits"]))


def spread_points(tick, info):
    point = float(info["point"])

    if point <= 0:
        raise SafetyError("Invalid symbol point size.")

    return (
        float(tick["ask"]) - float(tick["bid"])
    ) / point


def choose_filling(broker, info):
    flags = int(info.get("filling_mode", 0))

    if flags & 1:
        return broker.constant("ORDER_FILLING_FOK")

    if flags & 2:
        return broker.constant("ORDER_FILLING_IOC")

    execution = int(info.get("trade_exemode", -1))

    if execution != broker.constant("SYMBOL_TRADE_EXECUTION_MARKET"):
        return broker.constant("ORDER_FILLING_RETURN")

    raise SafetyError("No supported filling policy is available.")


def stop_gap(info, include_freeze=False):
    points = float(info.get("trade_stops_level", 0))

    if include_freeze:
        points = max(
            points, float(info.get("trade_freeze_level", 0))
        )

    tick_size = float(
        info.get("trade_tick_size") or info["point"]
    )

    return points * float(info["point"]) + tick_size


def build_order(broker, candidate, account, state=None, active_positions=None):
    symbol = candidate["symbol"]
    buy = candidate["side"] == "BUY"
    tag = f"{symbol} {candidate['side']} [{candidate['zone']['kind']}]"

    def reject(reason, permanent=True):
        status(f"reject_{symbol}", f"[REJECT] {tag}\nReason: {reason}", 60)
        candidate["_permanent_reject"] = permanent
        return None

    if is_monday_opening_gap_window(symbol):
        return reject("Monday market-open gap cooldown active", permanent=False)

    allowed, reason = news_permission(broker, symbol)
    if not allowed:
        return reject(f"News restriction ({reason})", permanent=False)

    info = broker.info(symbol)
    tick = broker.tick(symbol)

    if broker.to_utc_ts(tick_timestamp(tick)) < candidate["signal_time"]:
        candidate["_permanent_reject"] = False
        return None

    signal_age = time.time() - candidate["signal_time"]
    if signal_age < 0 or signal_age > CFG.MAX_CLOSED_M5_AGE_SECONDS:
        return reject(f"Signal expired (Age: {signal_age:.1f}s, Max: {CFG.MAX_CLOSED_M5_AGE_SECONDS}s)")

    allowed_modes = {
        broker.constant("SYMBOL_TRADE_MODE_FULL"),
        broker.constant("SYMBOL_TRADE_MODE_LONGONLY" if buy else "SYMBOL_TRADE_MODE_SHORTONLY"),
    }
    if int(info["trade_mode"]) not in allowed_modes:
        return reject(f"Broker trade mode blocked (mode={info['trade_mode']})")

    entry = float(tick["ask"] if buy else tick["bid"])
    spread_price = float(tick["ask"]) - float(tick["bid"])
    volatility = float(candidate["atr"])
    if volatility <= 0:
        return reject("ATR is <= 0")

    floor_atr = max(volatility, float(candidate.get("sl_floor_atr", volatility)))
    kz_mult = 1.20 if is_prime_killzone_utc() else 1.0

    if getattr(CFG, "USE_ATR_NORMALIZED_SPREAD", True):
        max_atr_ratio = float(getattr(CFG, "MAX_SPREAD_ATR_RATIO", 0.18)) * kz_mult
        if spread_price > floor_atr * max_atr_ratio:
            return reject(
                f"Spread/ATR too high ({spread_price / floor_atr * 100:.1f}% > {max_atr_ratio * 100:.1f}% of M5 ATR)",
                permanent=False,
            )
    else:
        base_spread_limit = (
            broker.spread_limit_for(symbol)
            if hasattr(broker, "spread_limit_for")
            else float(CFG.MAX_SPREAD_BY_SYMBOL.get(symbol, CFG.MAX_SPREAD_POINTS))
        )
        spread_limit = base_spread_limit * kz_mult
        current_spread = spread_points(tick, info)
        if current_spread > spread_limit:
            return reject(f"Spread too high ({current_spread:.1f} pts > {spread_limit:.1f} pts)", permanent=False)

    drift = abs(entry - candidate["signal_close"])
    max_drift = CFG.MAX_ENTRY_DRIFT_ATR * volatility
    if drift > max_drift:
        return reject(f"Entry drift too high ({drift:.5f} > Max: {max_drift:.5f})", permanent=False)

    zone = candidate["zone"]
    buffer = CFG.SL_BUFFER_ATR * volatility
    gap = stop_gap(info)
    if zone.get("kind") != "MOMENTUM_BIGBELUGA":
        min_smc_sl_dist = float(getattr(CFG, "MIN_SMC_SL_ATR", 1.30)) * floor_atr
    else:
        min_smc_sl_dist = float(getattr(CFG, "MIN_MOMENTUM_SL_ATR", 1.10)) * floor_atr

    if buy:
        raw_sl = min(zone["bottom"], candidate["extreme"]) - buffer
        if min_smc_sl_dist > 0:
            raw_sl = min(raw_sl, entry - min_smc_sl_dist)
        raw_sl = min(raw_sl, float(tick["bid"]) - gap)
        stop = price_round(raw_sl, info)
        risk_distance = entry - stop
        order_type = broker.constant("ORDER_TYPE_BUY")
    else:
        raw_sl = max(zone["top"], candidate["extreme"]) + buffer + spread_price
        if min_smc_sl_dist > 0:
            raw_sl = max(raw_sl, entry + min_smc_sl_dist + spread_price)
        raw_sl = max(raw_sl, float(tick["ask"]) + gap)
        stop = price_round(raw_sl, info, upward=True)
        risk_distance = stop - entry
        order_type = broker.constant("ORDER_TYPE_SELL")

    if stop <= 0 or risk_distance <= 0:
        return reject(f"Invalid stop geometry (Stop: {stop}, Dist: {risk_distance:.5f})")
    if risk_distance > CFG.MAX_SL_ATR * floor_atr:
        return reject(f"Stop distance too wide ({risk_distance:.5f} > Max: {CFG.MAX_SL_ATR * floor_atr:.5f})")

    if getattr(CFG, "USE_ATR_NORMALIZED_SPREAD", True):
        max_sl_ratio = float(getattr(CFG, "MAX_SPREAD_SL_RATIO", 0.12)) * kz_mult
        if spread_price > risk_distance * max_sl_ratio:
            return reject(
                f"Spread/SL ratio too high ({spread_price / risk_distance * 100:.1f}% > {max_sl_ratio * 100:.1f}% of SL)",
                permanent=False,
            )

    effective_tp2_rr = dynamic_tp_rr(candidate)
    if buy:
        tp_from_rr = entry + effective_tp2_rr * risk_distance
        target = price_round(max(tp_from_rr, float(tick["bid"]) + gap), info, upward=True)
    else:
        tp_from_rr = entry - effective_tp2_rr * risk_distance
        target = price_round(min(tp_from_rr, float(tick["ask"]) - gap), info)

    min_rr = max(CFG.TP1_RR + 0.35, 1.85)
    if getattr(CFG, "USE_STRUCTURAL_TP", True):
        pad = 0.10 * volatility
        if buy:
            min_struct_tp = entry + min_rr * risk_distance
            walls = [
                h - pad for h in candidate.get("structural_highs", [])
                if (h - pad) >= min_struct_tp and (h - pad) < target
            ]
            if walls:
                target = price_round(min(walls), info, upward=True)
        else:
            min_struct_tp = entry - min_rr * risk_distance
            walls = [
                l + pad for l in candidate.get("structural_lows", [])
                if (l + pad) <= min_struct_tp and (l + pad) > target
            ]
            if walls:
                target = price_round(max(walls), info, upward=False)

    if "bigbeluga_target" in candidate and candidate["bigbeluga_target"] > 0:
        bb_target = candidate["bigbeluga_target"]
        if buy:
            min_profitable_tp = entry + min_rr * risk_distance
            if bb_target >= min_profitable_tp and bb_target < target:
                target = price_round(bb_target, info, upward=True)
        else:
            min_profitable_tp = entry - min_rr * risk_distance
            if bb_target <= min_profitable_tp and bb_target > target:
                target = price_round(bb_target, info, upward=False)

    if target <= 0:
        return reject("Target price is invalid (<=0)")

    allowance = CFG.RISK_SLIPPAGE_POINTS * float(info["point"])
    worse_entry = entry + allowance if buy else entry - allowance
    worse_stop = stop - allowance if buy else stop + allowance

    if min(worse_entry, worse_stop) <= 0:
        return reject("Slippage calculation resulted in price <= 0")

    step = float(info["volume_step"])
    minimum = float(info["volume_min"])
    maximum = min(float(info["volume_max"]), CFG.MAX_LOT)

    if step <= 0 or minimum <= 0 or maximum < minimum:
        return reject(f"Broker volume limits invalid (Min: {minimum}, Max: {maximum}, Step: {step})")

    reference_loss = abs(broker.profit(order_type, symbol, minimum, worse_entry, worse_stop)) + CFG.COMMISSION_PER_LOT * minimum
    if reference_loss <= 0:
        return reject("Broker profit calculation returned <= 0")

    peak_bal = state.get("peak_balance") if isinstance(state, dict) else None
    risk_budget = dynamic_risk_budget(account, score=float(candidate.get("score", 8.0)), peak_balance=peak_bal)
    balance = float(account.get("balance", 0.0))
    equity = float(account.get("equity", balance))
    effective_cap = min(balance, equity) if (balance > 0 and equity > 0) else max(balance, equity)

    if isinstance(state, dict) and effective_cap > 0:
        max_heat_pct = float(getattr(CFG, "MAX_OPEN_PORTFOLIO_HEAT_PCT", 3.0))
        max_heat_usd = effective_cap * (max_heat_pct / 100.0)
        current_heat_usd = portfolio_open_heat_usd(broker, state, active_positions=active_positions)
        remaining_heat_usd = max_heat_usd - current_heat_usd
        if remaining_heat_usd <= 0.50:
            return reject(
                f"Portfolio open risk heat cap reached (${current_heat_usd:.2f} / ${max_heat_usd:.2f})",
                permanent=False,
            )
        risk_budget = min(risk_budget, remaining_heat_usd)

    max_fallback_pct = float(getattr(CFG, "MIN_LOT_FALLBACK_MAX_RISK_PCT", 2.5))
    max_fallback_budget = max(risk_budget, effective_cap * (max_fallback_pct / 100.0)) if effective_cap > 0 else risk_budget

    estimated_per_lot = reference_loss / minimum
    volume = floor_volume(min(risk_budget / estimated_per_lot, maximum), step)

    if volume + CFG.EPSILON < minimum:
        if reference_loss <= max_fallback_budget + CFG.EPSILON:
            volume = minimum
            risk_budget = max(risk_budget, reference_loss)
        else:
            return reject(
                f"Calculated volume ({volume:.4f}) < Min lot ({minimum}) "
                f"(Min lot risk ${reference_loss:.2f} > Max safe budget ${max_fallback_budget:.2f})"
            )

    estimated_loss = abs(broker.profit(order_type, symbol, volume, worse_entry, worse_stop)) + CFG.COMMISSION_PER_LOT * volume
    if estimated_loss > risk_budget + CFG.EPSILON:
        return reject(f"Estimated loss ({estimated_loss:.2f}) > Risk budget ({risk_budget:.2f})")

    margin = broker.margin(order_type, symbol, volume, entry)
    if margin < 0 or margin > float(account["margin_free"]) * 0.90:
        return reject(f"Insufficient margin (Needed: {margin:.2f}, Free: {float(account['margin_free']):.2f})")

    request = {
        "action": broker.constant("TRADE_ACTION_DEAL"),
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": entry,
        "sl": stop,
        "tp": target,
        "deviation": CFG.DEVIATION_POINTS,
        "magic": CFG.MAGIC,
        "type_time": broker.constant("ORDER_TIME_GTC"),
        "type_filling": choose_filling(broker, info),
    }

    return request, estimated_loss


def position_identifier(position):
    return str(int(position["identifier"]))


def trade_deal(broker, deal):
    return int(deal.get("type", -1)) in {
        broker.constant("DEAL_TYPE_BUY"),
        broker.constant("DEAL_TYPE_SELL"),
    }


def deal_cash(deal):
    return sum(
        float(deal.get(field, 0.0))
        for field in ("profit", "commission", "swap", "fee")
    )


def deal_matches(deal, pending):
    result = pending.get("result") or {}
    comment = pending["request"]["comment"]

    return (
        str(deal.get("comment", "")) == comment
        or (
            int(result.get("deal", 0)) > 0
            and int(deal.get("ticket", 0)) == int(result["deal"])
        )
        or (
            int(result.get("order", 0)) > 0
            and int(deal.get("order", 0)) == int(result["order"])
        )
    )


def rejection_is_definite(broker, result):
    if result is None:
        return False

    names = [
        "TRADE_RETCODE_REQUOTE",
        "TRADE_RETCODE_REJECT",
        "TRADE_RETCODE_INVALID",
        "TRADE_RETCODE_INVALID_VOLUME",
        "TRADE_RETCODE_INVALID_PRICE",
        "TRADE_RETCODE_INVALID_STOPS",
        "TRADE_RETCODE_TRADE_DISABLED",
        "TRADE_RETCODE_MARKET_CLOSED",
        "TRADE_RETCODE_NO_MONEY",
        "TRADE_RETCODE_INVALID_FILL",
    ]

    return int(result["retcode"]) in {
        broker.constant(name) for name in names
    }


def mark_halt(state, path, reason):
    if not state["halt_reason"]:
        state["halt_reason"] = reason
        atomic_save(path, state)
        notify(f"Safety halt activated. Reason: {reason}", critical=True)


def execute_request(broker, state, path, kind, request, metadata):
    if CFG.DRY_RUN or not CFG.ALLOW_LIVE_TRADING:
        raise FatalError("A trading request was blocked by execution mode.")

    if state["pending"] is not None or state["halt_reason"]:
        raise SafetyError("An execution requires reconciliation.")

    token = "SMC" + uuid.uuid4().hex[:18]
    request = dict(request, comment=token)

    check = broker.check(request)

    if check is None or int(check["retcode"]) != 0:
        notify(f"Order check failed. Action: {kind}")
        return False

    state["pending"] = {
        "kind": kind,
        "request": request,
        "metadata": metadata,
        "created": time.time(),
        "result": None,
    }

    atomic_save(path, state)

    try:
        result = broker.send(request)
    except Exception:
        notify(
            "Execution status is unknown. "
            "Further requests are blocked pending reconciliation.",
            critical=True,
        )
        return True

    state["pending"]["result"] = result
    atomic_save(path, state)

    notify(f"Execution submitted. Action: {kind}")
    return True


def resolve_pending(broker, state, path):
    pending = state["pending"]

    if pending is None:
        return

    request = pending["request"]
    kind = pending["kind"]
    result = pending.get("result") or {}

    if any(
        str(order.get("comment", "")) == request["comment"]
        or (
            int(result.get("order", 0)) > 0
            and int(order["ticket"]) == int(result["order"])
        )
        for order in broker.orders()
    ):
        return

    positions = broker.positions()

    if kind == "MODIFY_SL":
        pid = pending["metadata"]["position_id"]

        position = next(
            (
                item for item in positions
                if position_identifier(item) == pid
            ),
            None,
        )

        if position is None:
            state["pending"] = None
            atomic_save(path, state)
            return

        current_sl = float(position["sl"])
        desired_sl = float(request["sl"])
        buy = (
            int(position["type"])
            == broker.constant("POSITION_TYPE_BUY")
        )

        info = broker.info(position["symbol"])
        tolerance = float(
            info.get("trade_tick_size") or info["point"]
        ) / 2.0

        satisfied = current_sl > 0 and (
            current_sl >= desired_sl - tolerance
            if buy
            else current_sl <= desired_sl + tolerance
        )

        if satisfied:
            record = state["positions"].get(pid)
            if record is not None:
                record["last_sl"] = current_sl

            state["pending"] = None
            atomic_save(path, state)
            notify(f"Stop update confirmed. Symbol: {position['symbol']}")
            return

    elif kind in {"ENTRY", "PARTIAL", "FULL_CLOSE"}:
        if kind == "ENTRY":
            deals = broker.history(
                pending["created"] - 120, time.time() + 1
            )

            matches = [
                deal for deal in deals
                if trade_deal(broker, deal)
                and int(deal.get("magic", 0)) == CFG.MAGIC
                and int(deal.get("entry", -1))
                == broker.constant("DEAL_ENTRY_IN")
                and deal_matches(deal, pending)
            ]
        else:
            pid = pending["metadata"]["position_id"]
            deals = broker.position_deals(pid)

            allowed = {
                broker.constant("DEAL_ENTRY_OUT"),
                broker.constant("DEAL_ENTRY_OUT_BY"),
            }

            matches = [
                deal for deal in deals
                if trade_deal(broker, deal)
                and int(deal.get("entry", -1)) in allowed
                and deal_matches(deal, pending)
            ]

        if matches:
            if kind == "ENTRY":
                identifiers = {
                    str(int(deal["position_id"]))
                    for deal in matches
                }

                if len(identifiers) != 1:
                    mark_halt(
                        state, path,
                        "An entry affected multiple position identifiers.",
                    )
                    return

                pid = identifiers.pop()
                all_deals = broker.position_deals(pid)

                entries = [
                    deal for deal in all_deals
                    if trade_deal(broker, deal)
                    and int(deal.get("entry", -1))
                    == broker.constant("DEAL_ENTRY_IN")
                ]

                if not entries:
                    return

                if any(
                    int(deal.get("magic", 0)) != CFG.MAGIC
                    for deal in entries
                ):
                    mark_halt(
                        state, path,
                        "Position ownership could not be verified.",
                    )
                    return

                volume = sum(
                    float(deal["volume"]) for deal in entries
                )

                if volume <= 0:
                    return

                entry = sum(
                    float(deal["price"]) * float(deal["volume"])
                    for deal in entries
                ) / volume

                side = pending["metadata"]["side"]
                stop = float(request["sl"])
                initial_risk = (
                    entry - stop if side == "BUY" else stop - entry
                )

                zone_id_meta = pending["metadata"].get("zone_id")
                if zone_id_meta:
                    state["seen_zones"][zone_id_meta] = time.time()

                state["positions"][pid] = {
                    "symbol": request["symbol"],
                    "side": side,
                    "entry": entry,
                    "initial_sl": stop,
                    "last_sl": stop,
                    "initial_risk": initial_risk,
                    "initial_volume": volume,
                    "initial_tp": float(request["tp"]),
                    "partial_done": False,
                    "opened_at": min(
                        broker.to_utc_ts(float(deal["time"])) for deal in entries
                    ),
                    "zone_id": zone_id_meta,
                }

                state["pending"] = None
                atomic_save(path, state)

                notify(
                    f"Entry confirmed. Symbol: {request['symbol']}\n"
                    f"Side: {side}\n"
                    f"Entry: {entry:.8f}\n"
                    f"Stop: {stop:.8f}\n"
                    f"Target: {float(request['tp']):.8f}\n"
                    f"Volume: {volume:.8f}"
                )

                if initial_risk <= 0:
                    mark_halt(
                        state, path,
                        "The confirmed fill has invalid initial stop geometry.",
                    )
                    return

                actual_risk = abs(
                    broker.profit(
                        request["type"],
                        request["symbol"],
                        volume,
                        entry,
                        stop,
                    )
                ) + CFG.COMMISSION_PER_LOT * volume

                try:
                    acct_now = broker.account()
                    bal_now = float(acct_now.get("balance", 0.0))
                except Exception:
                    bal_now = 0.0
                max_pct_Allowance = max(
                    float(getattr(CFG, "RISK_PCT", 1.25)),
                    float(getattr(CFG, "MIN_LOT_FALLBACK_MAX_RISK_PCT", 2.5)),
                    float(getattr(CFG, "MAX_OPEN_PORTFOLIO_HEAT_PCT", 3.0)),
                )
                dynamic_acct_ceiling = bal_now * (max_pct_Allowance / 100.0) * 1.50 if bal_now > 0 else 0.0
                est_loss_meta = float(pending["metadata"].get("estimated_loss", CFG.RISK_PER_TRADE))
                allowed_risk_ceiling = max(
                    float(CFG.RISK_PER_TRADE) * 1.50,
                    est_loss_meta * 1.50,
                    dynamic_acct_ceiling,
                )
                if actual_risk > allowed_risk_ceiling + CFG.EPSILON:
                    mark_halt(
                        state, path,
                        "Confirmed entry risk exceeds the configured budget.",
                    )

                return

            pid = pending["metadata"]["position_id"]
            closed_volume = sum(
                float(deal["volume"]) for deal in matches
            )

            if kind == "PARTIAL":
                record = state["positions"].get(pid)
                if record is not None:
                    record["partial_done"] = True

                state["pending"] = None
                atomic_save(path, state)

                notify(
                    f"Partial close confirmed. Symbol: {request['symbol']}\n"
                    f"Closed volume: {closed_volume:.8f}"
                )
                return

            remaining_position = next(
                (
                    item for item in positions
                    if position_identifier(item) == pid
                ),
                None,
            )

            requested_volume = float(request["volume"])

            if remaining_position is not None:
                remaining = float(remaining_position["volume"])
                expected_remaining = max(
                    0.0, requested_volume - closed_volume
                )

                if remaining > expected_remaining + CFG.EPSILON:
                    return

            state["pending"] = None
            atomic_save(path, state)

            notify(
                f"Time-exit fill confirmed. Symbol: {request['symbol']}\n"
                f"Closed volume: {closed_volume:.8f}"
            )
            return

    else:
        raise FatalError("Unknown execution journal action.")

    if rejection_is_definite(broker, pending.get("result")):
        code = int(pending["result"]["retcode"])
        permanent_codes = {
            broker.constant("TRADE_RETCODE_INVALID"),
            broker.constant("TRADE_RETCODE_INVALID_VOLUME"),
            broker.constant("TRADE_RETCODE_INVALID_STOPS"),
            broker.constant("TRADE_RETCODE_TRADE_DISABLED"),
            broker.constant("TRADE_RETCODE_NO_MONEY"),
            broker.constant("TRADE_RETCODE_INVALID_FILL"),
        }
        if kind == "ENTRY" and code in permanent_codes:
            zone_id_meta = pending["metadata"].get("zone_id")
            if zone_id_meta:
                state["seen_zones"][zone_id_meta] = time.time()
        state["pending"] = None
        atomic_save(path, state)
        notify(f"Execution rejected. Action: {kind}. Code: {code}")
        return

    pending_age = time.time() - float(pending.get("created", time.time()))
    stale_timeout = float(getattr(CFG, "PENDING_STALE_RECONCILE_SECONDS", 90.0))

    if pending_age >= stale_timeout:
        if kind in {"PARTIAL", "FULL_CLOSE"}:
            pid = pending["metadata"]["position_id"]
            live_pos = next(
                (item for item in positions if position_identifier(item) == pid),
                None,
            )
            record = state["positions"].get(pid)
            if live_pos is None:
                state["pending"] = None
                atomic_save(path, state)
                notify(f"Stale {kind} cleared: position {pid} is already closed.")
                return
            if (
                kind == "PARTIAL"
                and record is not None
                and float(live_pos["volume"]) < float(record["initial_volume"]) - CFG.EPSILON
            ):
                record["partial_done"] = True
                state["pending"] = None
                atomic_save(path, state)
                notify(f"Stale PARTIAL reconciled via live volume reduction on {request['symbol']}.")
                return
            state["pending"] = None
            atomic_save(path, state)
            notify(f"Stale unacknowledged {kind} cleared after {pending_age:.0f}s (no broker order/deal found).")
            return

        if kind == "MODIFY_SL":
            state["pending"] = None
            atomic_save(path, state)
            notify(f"Stale unacknowledged MODIFY_SL cleared after {pending_age:.0f}s.")
            return

        if kind == "ENTRY":
            sym_has_bot_pos = any(
                item["symbol"] == request["symbol"] and int(item.get("magic", 0)) == CFG.MAGIC
                for item in positions
            )
            if not sym_has_bot_pos:
                state["pending"] = None
                atomic_save(path, state)
                notify(f"Stale unacknowledged ENTRY cleared after {pending_age:.0f}s (no open position or deal).")
                return

    status(
        "pending_execution",
        "Execution remains unresolved. Automatic requests are blocked.",
    )


def close_reason(broker, code):
    mapping = {
        broker.constant("DEAL_REASON_CLIENT"): "Desktop client",
        broker.constant("DEAL_REASON_MOBILE"): "Mobile client",
        broker.constant("DEAL_REASON_WEB"): "Web client",
        broker.constant("DEAL_REASON_EXPERT"): "Expert or API",
        broker.constant("DEAL_REASON_SL"): "Stop loss",
        broker.constant("DEAL_REASON_TP"): "Take profit",
        broker.constant("DEAL_REASON_SO"): "Stop out",
        broker.constant("DEAL_REASON_ROLLOVER"): "Rollover",
    }

    return mapping.get(int(code), "Other broker reason")


def reconcile_positions(broker, state, path):
    positions = broker.positions()
    live = {
        position_identifier(position): position
        for position in positions
    }

    if state["pending"] is None:
        for pid, position in live.items():
            if (
                int(position.get("magic", 0)) == CFG.MAGIC
                and pid not in state["positions"]
            ):
                mark_halt(
                    state, path,
                    "An untracked bot position requires manual reconciliation.",
                )

    pending_pid = None

    if state["pending"] is not None:
        pending_pid = state["pending"]["metadata"].get("position_id")

    closed = []
    updated = False

    for pid, record in list(state["positions"].items()):
        if pid in live:
            position = live[pid]
            buy = record["side"] == "BUY"
            actual_buy = (
                int(position["type"])
                == broker.constant("POSITION_TYPE_BUY")
            )

            if (
                int(position.get("magic", 0)) != CFG.MAGIC
                or position["symbol"] != record["symbol"]
                or actual_buy != buy
            ):
                mark_halt(
                    state, path,
                    "Tracked position ownership or direction changed.",
                )

            if (
                float(position["volume"])
                > float(record["initial_volume"]) + CFG.EPSILON
            ):
                mark_halt(
                    state, path,
                    "Tracked position volume increased unexpectedly.",
                )

            info = broker.info(position["symbol"])
            tolerance = float(
                info.get("trade_tick_size") or info["point"]
            ) / 2.0

            actual_sl = float(position["sl"])
            last_sl = float(
                record.get("last_sl", record["initial_sl"])
            )

            if actual_sl <= 0:
                mark_halt(
                    state, path,
                    "A tracked position has no server-side stop loss.",
                )
            else:
                worsened = (
                    actual_sl < last_sl - tolerance
                    if buy
                    else actual_sl > last_sl + tolerance
                )

                if worsened:
                    mark_halt(
                        state, path,
                        "A tracked stop moved in a risk-increasing direction.",
                    )
                elif abs(actual_sl - last_sl) > tolerance:
                    record["last_sl"] = actual_sl
                    updated = True

            if float(position["tp"]) <= 0:
                mark_halt(
                    state, path,
                    "A tracked position has no server-side take profit.",
                )

            continue

        if pending_pid == pid:
            continue

        deals = broker.position_deals(pid)

        if not deals:
            continue

        trading = [
            deal for deal in deals
            if trade_deal(broker, deal)
        ]

        if any(
            int(deal.get("entry", -1))
            == broker.constant("DEAL_ENTRY_INOUT")
            for deal in trading
        ):
            mark_halt(
                state, path,
                "A position reversal requires manual reconciliation.",
            )
            continue

        entries = [
            deal for deal in trading
            if int(deal.get("entry", -1))
            == broker.constant("DEAL_ENTRY_IN")
        ]

        exit_types = {
            broker.constant("DEAL_ENTRY_OUT"),
            broker.constant("DEAL_ENTRY_OUT_BY"),
        }

        exits = [
            deal for deal in trading
            if int(deal.get("entry", -1)) in exit_types
        ]

        if not entries or not exits:
            continue

        entered = sum(float(deal["volume"]) for deal in entries)
        exited = sum(float(deal["volume"]) for deal in exits)

        if abs(entered - exited) > CFG.EPSILON:
            continue

        last_exit = max(
            exits,
            key=lambda deal: (
                int(deal.get("time_msc", 0)),
                int(deal["ticket"]),
            ),
        )

        pnl = sum(deal_cash(deal) for deal in deals)

        closed.append((
            broker.to_utc_ts(float(last_exit["time"])),
            int(last_exit["ticket"]),
            pid,
            record,
            pnl,
            last_exit,
        ))

    if updated:
        atomic_save(path, state)

    for closed_time, _, pid, record, pnl, last_exit in sorted(closed):
        if pnl < -CFG.EPSILON:
            state["consecutive_losses"] += 1

            if state["consecutive_losses"] >= CFG.MAX_CONSECUTIVE_LOSSES:
                state["cooldown_until"] = max(
                    float(state["cooldown_until"]),
                    closed_time + CFG.COOLDOWN_SECONDS,
                )
                state["consecutive_losses"] = 0

        elif pnl > CFG.EPSILON:
            state["consecutive_losses"] = 0

        state["positions"].pop(pid, None)
        atomic_save(path, state)

        reason = close_reason(
            broker, last_exit.get("reason", -1)
        )

        notify(
            f"Position closed. Symbol: {record['symbol']}\n"
            f"Side: {record['side']}\n"
            f"Net position PnL: {pnl:+.2f} account currency\n"
            f"Reason: {reason}"
        )

    return positions

def is_friday_weekend_close_window(symbol=None):
    if symbol is not None and is_crypto_symbol(symbol):
        return False
    if not getattr(CFG, "ENABLE_FRIDAY_WEEKEND_EXIT", True):
        return False
    now = utc_now()
    if now.weekday() != 4:
        return False
    exit_h = int(getattr(CFG, "FRIDAY_EXIT_HOUR_UTC", 20))
    exit_m = int(getattr(CFG, "FRIDAY_EXIT_MINUTE_UTC", 30))
    return (now.hour > exit_h) or (now.hour == exit_h and now.minute >= exit_m)


def send_daily_summary_report(broker, report_day, prev_start_balance, current_balance):
    if not getattr(CFG, "ENABLE_DAILY_TELEGRAM_SUMMARY", True) or not report_day:
        return
    try:
        day_start_dt = datetime.strptime(report_day, "%Y-%m-%d").replace(tzinfo=timezone.utc)
        start_ts = day_start_dt.timestamp()
        end_ts = start_ts + 86400.0
        deals = broker.history(start_ts, end_ts)

        exit_types = {
            broker.constant("DEAL_ENTRY_OUT"),
            broker.constant("DEAL_ENTRY_OUT_BY"),
        }
        pnl_by_pos = {}
        sym_by_pos = {}
        for deal in deals:
            if not trade_deal(broker, deal):
                continue
            if int(deal.get("magic", 0)) != CFG.MAGIC and int(deal.get("entry", -1)) not in exit_types:
                continue
            pid = str(int(deal.get("position_id", 0)))
            if pid == "0":
                continue
            pnl_by_pos[pid] = pnl_by_pos.get(pid, 0.0) + deal_cash(deal)
            if deal.get("symbol"):
                sym_by_pos[pid] = str(deal["symbol"])

        closed_positions = [
            (pid, pnl) for pid, pnl in pnl_by_pos.items()
            if any(
                str(int(d.get("position_id", 0))) == pid and int(d.get("entry", -1)) in exit_types
                for d in deals
            )
        ]

        total_trades = len(closed_positions)
        wins = sum(1 for _, pnl in closed_positions if pnl > CFG.EPSILON)
        losses = sum(1 for _, pnl in closed_positions if pnl < -CFG.EPSILON)
        win_rate = (wins / total_trades * 100.0) if total_trades > 0 else 0.0
        net_pnl = sum(pnl for _, pnl in closed_positions)
        start_bal = float(prev_start_balance) if prev_start_balance and float(prev_start_balance) > 0 else (current_balance - net_pnl)
        pnl_pct = (net_pnl / start_bal * 100.0) if start_bal > 0 else 0.0

        notify(
            f"=== DAILY PNL SUMMARY ({report_day}) ===\n"
            f"Start Balance: {start_bal:.2f} USD\n"
            f"End Balance: {current_balance:.2f} USD\n"
            f"Net PnL: {net_pnl:+.2f} USD ({pnl_pct:+.2f}%)\n"
            f"Closed Trades: {total_trades} (Wins: {wins} | Losses: {losses} | Win Rate: {win_rate:.1f}%)"
        )
    except Exception:
        pass


def daily_snapshot(broker, state, path):
    start = utc_midnight_timestamp()
    deals = broker.history(start, time.time() + 1)
    account = broker.account()

    cash_flow_types = {
        broker.constant("DEAL_TYPE_BALANCE"),
        broker.constant("DEAL_TYPE_CREDIT"),
        broker.constant("DEAL_TYPE_BONUS"),
        broker.constant("DEAL_TYPE_CORRECTION"),
    }

    external_cash_flow = any(
        int(deal.get("type", -1)) in cash_flow_types
        and abs(deal_cash(deal)) > CFG.EPSILON
        for deal in deals
    )

    today = utc_now().date().isoformat()
    total_cash = sum(deal_cash(deal) for deal in deals)

    cur_bal = float(account.get("balance", 0.0))
    prev_peak = float(state.get("peak_balance") or 0.0)
    if cur_bal > prev_peak:
        state["peak_balance"] = cur_bal
        atomic_save(path, state)

    if state["day"] != today:
        prev_day = state.get("day", "")
        prev_start_balance = state.get("day_start_balance")
        new_start_balance = float(account["balance"]) - total_cash

        state["day"] = today
        state["day_start_balance"] = new_start_balance
        state["daily_loss_latched"] = False
        state["dry_run_entries"] = 0

        cutoff = time.time() - 14 * 86400
        state["seen_zones"] = {
            key: stamp
            for key, stamp in state["seen_zones"].items()
            if float(stamp) >= cutoff
        }

        atomic_save(path, state)
        if prev_day:
            send_daily_summary_report(broker, prev_day, prev_start_balance, new_start_balance)

    realized = sum(
        deal_cash(deal)
        for deal in deals
        if int(deal.get("type", -1)) not in cash_flow_types
    )

    floating = float(account["equity"]) - float(account["balance"])
    daily_pnl = realized + floating
    baseline = float(state["day_start_balance"] or 0.0)

    if baseline <= 0:
        external_cash_flow = True

    if (
        baseline > 0
        and daily_pnl <= -baseline * CFG.MAX_DAILY_LOSS_PCT
        and not state["daily_loss_latched"]
    ):
        state["daily_loss_latched"] = True
        atomic_save(path, state)
        notify(
            "Daily loss guard activated. "
            "New entries are disabled for the UTC day."
        )

    entry_ids = {
        str(int(deal["position_id"]))
        for deal in deals
        if trade_deal(broker, deal)
        and int(deal.get("magic", 0)) == CFG.MAGIC
        and int(deal.get("entry", -1)) in {
            broker.constant("DEAL_ENTRY_IN"),
            broker.constant("DEAL_ENTRY_INOUT"),
        }
    }

    for pid, record in state["positions"].items():
        if float(record["opened_at"]) >= start:
            entry_ids.add(pid)

    daily_entries = len(entry_ids)

    if CFG.DRY_RUN:
        daily_entries += int(state.get("dry_run_entries", 0))

    return {
        "account": account,
        "daily_entries": daily_entries,
        "daily_pnl": daily_pnl,
        "external_cash_flow": external_cash_flow,
    }


def in_session_for_symbol(symbol):
    if is_crypto_symbol(symbol):
        if CFG.USE_KILLZONES_ONLY:
            return is_prime_killzone_utc()
        return True

    now_utc = utc_now()
    if now_utc.weekday() == 5 or (now_utc.weekday() == 6 and now_utc.hour < 22):
        return False
    if is_friday_weekend_close_window(symbol):
        return False
    if is_monday_opening_gap_window(symbol):
        return False

    if CFG.USE_KILLZONES_ONLY:
        return is_prime_killzone_utc()

    if getattr(CFG, "USE_AUTO_DST_KILLZONES", True):
        try:
            from zoneinfo import ZoneInfo
            london_hour = now_utc.astimezone(ZoneInfo("Europe/London")).hour
            ny_hour = now_utc.astimezone(ZoneInfo("America/New_York")).hour
            if (7 <= london_hour < 17) or (7 <= ny_hour < 17):
                return True
            return False
        except Exception:
            pass

    hour = now_utc.hour
    if CFG.SESSION_START_HOUR_UTC == CFG.SESSION_END_HOUR_UTC:
        return True

    if CFG.SESSION_START_HOUR_UTC < CFG.SESSION_END_HOUR_UTC:
        return CFG.SESSION_START_HOUR_UTC <= hour < CFG.SESSION_END_HOUR_UTC

    return (
        hour >= CFG.SESSION_START_HOUR_UTC
        or hour < CFG.SESSION_END_HOUR_UTC
    )


def in_session(symbols=None):
    active_list = symbols if symbols is not None else CFG.SYMBOLS
    return any(in_session_for_symbol(s) for s in active_list)


def entry_permission(broker, state, snapshot, positions, orders):
    if state["halt_reason"]:
        return False, "A persistent safety halt is active."

    if state["pending"] is not None:
        return False, "An execution requires reconciliation."

    if state["daily_loss_latched"]:
        return False, "The daily loss guard is active."

    if snapshot["external_cash_flow"]:
        return False, "Account cash flow prevents safe daily accounting."

    if time.time() < float(state["cooldown_until"]):
        return False, "The consecutive-loss cooldown is active."

    if snapshot["daily_entries"] >= dynamic_daily_trade_limit(snapshot):
        return False, "The daily entry limit has been reached."

    bot_positions = [
        position for position in positions
        if int(position.get("magic", 0)) == CFG.MAGIC
    ]

    if len(bot_positions) >= dynamic_max_open_positions(snapshot["account"]):
        return False, "The open-position limit has been reached."

    if any(int(order.get("magic", 0)) == CFG.MAGIC for order in orders):
        return False, "A bot order is still active."

    active_syms = getattr(broker, "active_symbols", CFG.SYMBOLS)
    if not in_session(active_syms):
        return False, "The entry session is closed."

    account = snapshot["account"]
    terminal = broker.terminal()

    if not terminal.get("connected", False):
        return False, "The terminal is disconnected."

    if not CFG.DRY_RUN:
        if not account.get("trade_allowed", False):
            return False, "Account trading permission is unavailable."

        if not account.get("trade_expert", False):
            return False, "Automated trading permission is unavailable."

        if not terminal.get("trade_allowed", False):
            return False, "Terminal trading permission is unavailable."

        if terminal.get("tradeapi_disabled", False):
            return False, "External trading API access is disabled."

    return True, "Entry checks passed."


def partial_request(broker, position, volume):
    symbol = position["symbol"]
    info = broker.info(symbol)
    tick = broker.tick(symbol)

    buy = (
        int(position["type"])
        == broker.constant("POSITION_TYPE_BUY")
    )

    return {
        "action": broker.constant("TRADE_ACTION_DEAL"),
        "symbol": symbol,
        "position": int(position["ticket"]),
        "volume": float(volume),
        "type": broker.constant(
            "ORDER_TYPE_SELL" if buy else "ORDER_TYPE_BUY"
        ),
        "price": float(tick["bid"] if buy else tick["ask"]),
        "deviation": CFG.DEVIATION_POINTS,
        "magic": CFG.MAGIC,
        "type_time": broker.constant("ORDER_TIME_GTC"),
        "type_filling": choose_filling(broker, info),
    }


def stop_request(broker, position_id, desired_sl):
    position = next(
        (
            item for item in broker.positions()
            if position_identifier(item) == position_id
        ),
        None,
    )

    if position is None:
        return None

    info = broker.info(position["symbol"])
    tick = broker.tick(position["symbol"])

    buy = (
        int(position["type"])
        == broker.constant("POSITION_TYPE_BUY")
    )

    current_sl = float(position["sl"])
    gap = stop_gap(info, include_freeze=True)
    increment = float(
        info.get("trade_tick_size") or info["point"]
    )

    desired_sl = price_round(
        desired_sl, info, upward=not buy
    )

    if desired_sl <= 0 or current_sl <= 0:
        return None

    if buy:
        if desired_sl <= current_sl + increment / 2:
            return None
        if desired_sl > float(tick["bid"]) - gap:
            return None
        if float(tick["bid"]) - current_sl < gap:
            return None
    else:
        if desired_sl >= current_sl - increment / 2:
            return None
        if desired_sl < float(tick["ask"]) + gap:
            return None
        if current_sl - float(tick["ask"]) < gap:
            return None

    return {
        "action": broker.constant("TRADE_ACTION_SLTP"),
        "symbol": position["symbol"],
        "position": int(position["ticket"]),
        "sl": desired_sl,
        "tp": float(position["tp"]),
        "magic": CFG.MAGIC,
    }


def manage_positions(broker, state, path):
    if (
        CFG.DRY_RUN
        or state["pending"] is not None
        or state["halt_reason"]
    ):
        return

    for position in broker.positions():
        pid = position_identifier(position)
        record = state["positions"].get(pid)

        if record is None:
            continue

        symbol = position["symbol"]
        info = broker.info(symbol)
        tick = broker.tick(symbol)

        buy = record["side"] == "BUY"
        entry = float(record["entry"])
        initial_risk = float(record["initial_risk"])
        volume = float(position["volume"])

        if initial_risk <= 0:
            mark_halt(
                state, path,
                "A tracked position has invalid initial risk.",
            )
            return

        price = float(tick["bid"] if buy else tick["ask"])
        current_r = (
            (price - entry) / initial_risk
            if buy
            else (entry - price) / initial_risk
        )

        age_seconds = time.time() - float(record["opened_at"])
        base_exit_min = float(CFG.TIME_EXIT_MINUTES)
        if getattr(CFG, "USE_DYNAMIC_TIME_EXIT", True):
            if current_r >= 0.05 or is_prime_killzone_utc():
                effective_exit_min = base_exit_min * 1.35
            elif current_r <= -0.35:
                effective_exit_min = max(45.0, base_exit_min * 0.72)
            else:
                effective_exit_min = base_exit_min
        else:
            effective_exit_min = base_exit_min

        if is_friday_weekend_close_window(symbol):
            request = partial_request(broker, position, volume)
            execute_request(
                broker,
                state,
                path,
                "FULL_CLOSE",
                request,
                {
                    "position_id": pid,
                    "reason": "FRIDAY_WEEKEND_EXIT",
                    "current_r": current_r,
                },
            )
            return

        if (
            CFG.ENABLE_TIME_EXIT
            and not record.get("partial_done", False)
            and age_seconds >= effective_exit_min * 60
            and current_r < CFG.TIME_EXIT_MIN_R
        ):
            request = partial_request(broker, position, volume)

            execute_request(
                broker,
                state,
                path,
                "FULL_CLOSE",
                request,
                {
                    "position_id": pid,
                    "reason": "TIME_EXIT",
                    "current_r": current_r,
                },
            )
            return

        target1 = (
            entry + CFG.TP1_RR * initial_risk
            if buy
            else entry - CFG.TP1_RR * initial_risk
        )

        reached = price >= target1 if buy else price <= target1

        if (
            volume < float(record["initial_volume"]) - CFG.EPSILON
            and not record["partial_done"]
        ):
            record["partial_done"] = True
            atomic_save(path, state)
            notify(
                f"Existing volume reduction detected. Symbol: {symbol}\n"
                "The bot will not repeat the partial close."
            )

        if reached and not record["partial_done"]:
            step = float(info["volume_step"])
            minimum = float(info["volume_min"])

            closing = floor_volume(volume / 2.0, step)
            remaining = round(volume - closing, 10)

            if (
                closing + CFG.EPSILON >= minimum
                and remaining + CFG.EPSILON >= minimum
            ):
                request = partial_request(
                    broker, position, closing
                )

                execute_request(
                    broker,
                    state,
                    path,
                    "PARTIAL",
                    request,
                    {"position_id": pid},
                )
                return

            record["partial_done"] = True
            atomic_save(path, state)
            notify(
                f"Partial close skipped. Symbol: {symbol}\n"
                "Broker volume limits prevent a valid split. "
                "Stop management remains enabled."
            )

        be_trigger_r = float(getattr(CFG, "BREAKEVEN_TRIGGER_RR", 1.0))
        stage1_lock_r = float(getattr(CFG, "STAGE1_REDUCE_RISK_RR", -0.35))
        be_lock_r = float(getattr(CFG, "BREAKEVEN_LOCK_RR", 0.10))
        if not record["partial_done"]:
            if current_r >= be_trigger_r:
                stage1_sl = (
                    entry + (stage1_lock_r * initial_risk)
                    if buy
                    else entry - (stage1_lock_r * initial_risk)
                )
                be_req = stop_request(broker, pid, stage1_sl)
                if be_req is not None:
                    execute_request(
                        broker,
                        state,
                        path,
                        "MODIFY_SL",
                        be_req,
                        {"position_id": pid},
                    )
                    return
            continue

        bars = broker.bars(
            symbol, broker.constant("TIMEFRAME_M5"), 100
        )

        closed_time = bars[-1]["time"] + 300
        bar_age = time.time() - closed_time

        if (
            bar_age < 0
            or bar_age > CFG.MAX_CLOSED_M5_AGE_SECONDS
        ):
            continue

        volatility = atr(bars)[-1]

        if volatility <= 0:
            continue

        highs, lows = swings(bars, CFG.LTF_SWING_WINDOW)
        buffer = CFG.SL_BUFFER_ATR * volatility
        be_floor = (
            entry + (be_lock_r * initial_risk)
            if buy
            else entry - (be_lock_r * initial_risk)
        )
        desired_sl = be_floor

        if buy:
            valid_lows = [l["price"] for l in lows if l["price"] < price]
            if valid_lows:
                structural_sl = valid_lows[-1] - buffer
                desired_sl = max(desired_sl, structural_sl)
            atr_trail = price - (CFG.TRAIL_ATR * volatility)
            if atr_trail > entry:
                desired_sl = max(desired_sl, atr_trail)
        else:
            valid_highs = [h["price"] for h in highs if h["price"] > price]
            if valid_highs:
                structural_sl = valid_highs[-1] + buffer
                desired_sl = min(desired_sl, structural_sl)
            atr_trail = price + (CFG.TRAIL_ATR * volatility)
            if atr_trail < entry:
                desired_sl = min(desired_sl, atr_trail)

        request = stop_request(broker, pid, desired_sl)

        if request is None:
            continue

        execute_request(
            broker,
            state,
            path,
            "MODIFY_SL",
            request,
            {"position_id": pid},
        )
        return


def account_key_from(account):
    raw = (
        f"{account.get('server', '')}:"
        f"{int(account['login'])}"
    )
    return hashlib.sha256(
        raw.encode("utf-8")
    ).hexdigest()[:16]


def validate_account(broker, expected_key):
    account = broker.account()

    if account_key_from(account) != expected_key:
        raise FatalError("The connected account changed.")

    if (
        int(account["margin_mode"])
        != broker.constant("ACCOUNT_MARGIN_MODE_RETAIL_HEDGING")
    ):
        raise FatalError("This build supports hedging accounts only.")

    for field in ("balance", "equity", "margin_free"):
        if not math.isfinite(float(account[field])):
            raise SafetyError("Account financial data is invalid.")

    return account


def has_consolidation_before_breakout(bars, breakout_idx, lookback=12, atr_ratio_threshold=1.15):
    if breakout_idx < lookback + 20:
        return True

    prior_bars = bars[breakout_idx - lookback:breakout_idx]
    if not prior_bars:
        return True

    atr_series = atr(bars[:breakout_idx])
    if len(atr_series) < 50:
        return True

    baseline_atr = sum(atr_series[-50:]) / 50.0
    if baseline_atr <= 0:
        return True

    avg_prior_range = sum(b["high"] - b["low"] for b in prior_bars) / len(prior_bars)
    box_width = max(b["high"] for b in prior_bars) - min(b["low"] for b in prior_bars)

    range_compressed = avg_prior_range <= baseline_atr * atr_ratio_threshold
    box_contained = box_width <= baseline_atr * (lookback * 0.45)

    return range_compressed and box_contained

def wick_rejection_ok(bar, side, max_wick_ratio):
    bar_range = bar["high"] - bar["low"]
    if bar_range <= 0:
        return False

    if side == "BUY":
        wick = bar["high"] - max(bar["open"], bar["close"])
    else:
        wick = min(bar["open"], bar["close"]) - bar["low"]

    return (wick / bar_range) <= max_wick_ratio


def sweep_wick_ok(bar, side, min_sweep_wick_ratio=0.15, max_counter_wick_ratio=0.65):
    bar_range = bar["high"] - bar["low"]
    if bar_range <= 0:
        return False

    if side == "BUY":
        sweep_wick = min(bar["open"], bar["close"]) - bar["low"]
        counter_wick = bar["high"] - max(bar["open"], bar["close"])
    else:
        sweep_wick = bar["high"] - max(bar["open"], bar["close"])
        counter_wick = min(bar["open"], bar["close"]) - bar["low"]

    return (sweep_wick / bar_range) >= min_sweep_wick_ratio and (counter_wick / bar_range) <= max_counter_wick_ratio



def emit_smart_market_pulse_radar(active_symbols_list, open_positions, news_blocked):
    global LAST_RADAR_SIGNATURE, LAST_RADAR_SENT_TS

    if not getattr(CFG, "ENABLE_MARKET_PULSE_RADAR", True):
        return

    now_ts = time.time()
    min_interval = float(getattr(CFG, "MARKET_PULSE_MIN_INTERVAL_SEC", 60.0))
    if now_ts - LAST_RADAR_SENT_TS < min_interval:
        return

    open_sym_Set = {p.get("symbol") for p in open_positions}
    news_set = set(news_blocked)

    sig_parts = []
    ready_list = []
    strong_list = []
    normal_list = []
    conflict_count = 0
    neutral_count = 0
    sideway_count = 0

    for sym in active_symbols_list:
        if sym in open_sym_Set:
            sig_parts.append(f"{sym}:OPEN_POS")
            continue
        if sym in news_set:
            sig_parts.append(f"{sym}:NEWS_BLACKOUT")
            continue

        info = SYMBOL_RADAR_STATE.get(sym)
        if not info:
            continue

        cat = info.get("category", "BLOCKED")
        side = info.get("side", "NEUTRAL")
        stage = info.get("stage", "")
        detail = info.get("detail", "")

        sig_parts.append(f"{sym}:{cat}:{side}:{stage}")

        if cat == "SIGNAL_READY":
            ready_list.append(detail)
        elif cat == "STRONG":
            strong_list.append(detail)
        elif cat == "NORMAL":
            normal_list.append(detail)
        else:
            if stage == "M15_EMA200_CONFLICT":
                conflict_count += 1
            elif stage == "RANGING_SIDEWAY":
                sideway_count += 1
            else:
                neutral_count += 1

    if not sig_parts:
        return

    signature = "|".join(sig_parts)
    if signature == LAST_RADAR_SIGNATURE:
        return

    LAST_RADAR_SIGNATURE = signature
    LAST_RADAR_SENT_TS = now_ts

    lines = [f"[MARKET PULSE RADAR] Active: {len(active_symbols_list)} symbols"]

    if open_sym_Set:
        lines.append(f"Open Positions ({len(open_sym_Set)}): " + ", ".join(sorted(open_sym_Set)))

    if ready_list:
        lines.append("SIGNAL READY:")
        for item in ready_list[:6]:
            lines.append(f" - {item}")

    if strong_list:
        lines.append(f"1. STRONG TREND ({len(strong_list)}):")
        for item in strong_list[:8]:
            lines.append(f" - {item}")
        if len(strong_list) > 8:
            lines.append(f" - ...and {len(strong_list) - 8} more")

    if normal_list:
        lines.append(f"2. NORMAL TREND - SMC WATCH ({len(normal_list)}):")
        for item in normal_list[:8]:
            lines.append(f" - {item}")
        if len(normal_list) > 8:
            lines.append(f" - ...and {len(normal_list) - 8} more")

    blocked_total = conflict_count + sideway_count + neutral_count + len(news_set)
    if blocked_total > 0:
        b_parts = []
        if conflict_count:
            b_parts.append(f"H4/M15 conflict: {conflict_count}")
        if sideway_count:
            b_parts.append(f"Sideways: {sideway_count}")
        if neutral_count:
            b_parts.append(f"H4 neutral: {neutral_count}")
        if news_set:
            b_parts.append(f"News blackout: {len(news_set)}")
        lines.append(f"3. STAND ASIDE / FILTERED ({blocked_total}): " + ", ".join(b_parts))

    notify("\n".join(lines))


def main():
    validate_config()
    broker = None

    root_logger, formatter = _configure_logging()
    if CFG.TELEGRAM_TOKEN and CFG.TELEGRAM_CHAT_ID:
        if not any(isinstance(h, TelegramLogHandler) for h in root_logger.handlers):
            tg_handler = TelegramLogHandler()
            tg_handler.setFormatter(formatter)
            root_logger.addHandler(tg_handler)

    try:
        broker = Broker()
        account = broker.account()
        account_key = account_key_from(account)

        acquire_lock(account_key)
        validate_account(broker, account_key)
        broker.select_symbols()
        broker.calibrate_server_offset()

        mode = "dry" if CFG.DRY_RUN else "live"

        state_path = ROOT / (
            f"bot_state_v2_{account_key}_{CFG.MAGIC}_{mode}.json"
        )

        state = load_state(state_path, account_key)
        atomic_save(state_path, state)

        notify(
            "SMC bot started.\n"
            f"Mode: {'DRY_RUN' if CFG.DRY_RUN else 'LIVE'}\n"
            f"Balance: {float(account.get('balance', 0.0)):.2f} {account.get('currency', 'USD')} | Dynamic risk: {getattr(CFG, 'USE_DYNAMIC_RISK', True)} ({getattr(CFG, 'RISK_PCT', 1.25):.2f}%)\n"
            f"Dynamic daily quota: {getattr(CFG, 'USE_DYNAMIC_DAILY_TRADES', True)} (Base: {CFG.MAX_DAILY_TRADES})\n"
            f"Dynamic TP scaling: {getattr(CFG, 'USE_DYNAMIC_TP_SCALING', True)} | Dynamic time exit: {getattr(CFG, 'USE_DYNAMIC_TIME_EXIT', True)}\n"
            f"Auto-DST London/NY session: {getattr(CFG, 'USE_AUTO_DST_KILLZONES', True)} | News filter: {CFG.ENABLE_NEWS_FILTER}"
        )

        if CFG.COMMISSION_PER_LOT == 0:
            log(
                "Commission reserve is zero. "
                "Configure actual broker costs before live use."
            )

        if not CFG.ENABLE_NEWS_FILTER:
            log(
                "News protection is disabled. "
                "No automatic news feed is included."
            )

        last_weekend_notify_date = None
        consecutive_runtime_errors = 0

        while True:
            try:
                if is_weekend(broker.active_symbols):
                    current_date = datetime.now(timezone.utc).date()
                    
                    if last_weekend_notify_date != current_date:
                        msg = "Weekend detected. Bot is sleeping to save resources. Resuming on Monday."
                        notify(msg)
                        last_weekend_notify_date = current_date
                    
                    time.sleep(min(60.0, float(CFG.POLL_SECONDS)))
                    continue
                
                validate_account(broker, account_key)
                resolve_pending(broker, state, state_path)
                reconcile_positions(broker, state, state_path)

                if state["pending"] is not None:
                    time.sleep(CFG.POLL_SECONDS)
                    continue

                if state["halt_reason"]:
                    status(
                        "persistent_halt",
                        f"A persistent safety halt is active ({state['halt_reason']}). "
                        f"Clear 'halt_reason' in {state_path.name} after checking MT5.",
                        1800,
                    )
                    time.sleep(CFG.POLL_SECONDS)
                    continue

                manage_positions(broker, state, state_path)

                if (
                    state["pending"] is not None
                    or state["halt_reason"]
                ):
                    time.sleep(CFG.POLL_SECONDS)
                    continue

                all_candidates = []

                snapshot = daily_snapshot(broker, state, state_path)
                positions = broker.positions()
                orders = broker.orders()
                permitted, reason = entry_permission(
                    broker, state, snapshot, positions, orders
                )

                if not permitted:
                    status("entry_gate", reason)
                else:
                    active_symbols = {item["symbol"] for item in positions + orders}
                    news_blocked_symbols = []

                    now_utc_scan = utc_now()
                    is_fx_weekend_closed = (
                        now_utc_scan.weekday() == 5
                        or (now_utc_scan.weekday() == 6 and now_utc_scan.hour < 22)
                        or is_friday_weekend_close_window(None)
                    )

                    logging.debug("Scanning %d active symbol(s)...", len(broker.active_symbols))
                    for symbol in broker.active_symbols:
                        if symbol in active_symbols:
                            continue
                        if not in_session_for_symbol(symbol):
                            continue
                        if is_fx_weekend_closed and not is_crypto_symbol(symbol):
                            continue
                        if is_monday_opening_gap_window(symbol):
                            continue

                        news_ok, news_reason = news_permission(broker, symbol)
                        if not news_ok:
                            news_blocked_symbols.append(symbol)
                            continue

                        logging.debug("Checking symbol: %s", symbol)
                        candidate = scan_symbol(broker, symbol, state)

                        if candidate is not None:
                            if not currency_exposure_ok(symbol, candidate["side"], positions):
                                continue
                            zone_id = candidate["zone_id"]
                            if zone_id not in state["seen_zones"]:
                                all_candidates.append(candidate)

                    if news_blocked_symbols:
                        status(
                            "news_blackout_summary",
                            f"News blackout active for {len(news_blocked_symbols)} symbol(s): "
                            + ", ".join(news_blocked_symbols[:8])
                            + ("..." if len(news_blocked_symbols) > 8 else ""),
                            900,
                        )

                    emit_smart_market_pulse_radar(
                        broker.active_symbols,
                        positions,
                        news_blocked_symbols,
                    )

                if all_candidates:
                    all_candidates.sort(key=lambda x: x["score"], reverse=True)

                    new_for_notify = [c for c in all_candidates[:5] if c["zone_id"] not in NOTIFIED_ZONES]
                    if new_for_notify:
                        top_signal_text = "Top Ranked Signals:\n"
                        for c in new_for_notify:
                            top_signal_text += f"- {c['symbol']} ({c['side']}) [{c['zone']['kind']}] | Score: {c['score']:.2f}\n"
                            NOTIFIED_ZONES.add(c["zone_id"])
                        if len(NOTIFIED_ZONES) > 500:
                            NOTIFIED_ZONES.clear()
                        notify(top_signal_text)

                    for candidate in all_candidates:
                        symbol = candidate["symbol"]
                        zone_id = candidate["zone_id"]

                        if zone_id in state["seen_zones"]:
                            continue

                        fresh_snapshot = daily_snapshot(broker, state, state_path)
                        fresh_positions = broker.positions()
                        fresh_orders = broker.orders()

                        permitted, reason = entry_permission(
                            broker,
                            state,
                            fresh_snapshot,
                            fresh_positions,
                            fresh_orders,
                        )

                        if not permitted:
                            break

                        if any(
                            item["symbol"] == symbol
                            for item in fresh_positions + fresh_orders
                        ):
                            continue

                        if not currency_exposure_ok(symbol, candidate["side"], fresh_positions):
                            continue

                        bot_positions = [
                            p for p in fresh_positions 
                            if int(p.get("magic", 0)) == CFG.MAGIC
                        ]
                        if len(bot_positions) >= dynamic_max_open_positions(fresh_snapshot["account"]):
                            break

                        built = build_order(
                            broker,
                            candidate,
                            fresh_snapshot["account"],
                            state=state,
                            active_positions=fresh_positions,
                        )

                        if built is None:
                            if candidate.get("_permanent_reject", True):
                                state["seen_zones"][zone_id] = time.time()
                                atomic_save(state_path, state)
                            continue

                        if not in_session_for_symbol(symbol):
                            continue

                        request, estimated_loss = built

                        if CFG.DRY_RUN:
                            state["seen_zones"][zone_id] = time.time()
                            state["dry_run_entries"] = (
                                int(state.get("dry_run_entries", 0)) + 1
                            )
                            atomic_save(state_path, state)
                            if CFG.TELEGRAM_TOKEN and CFG.TELEGRAM_CHAT_ID:
                                try:
                                    requests.post(
                                        f"https://api.telegram.org/bot{CFG.TELEGRAM_TOKEN}/sendMessage",
                                        json={
                                            "chat_id": CFG.TELEGRAM_CHAT_ID,
                                            "text": safe_text(
                                                f"Dry-run signal (Ranked). Symbol: {symbol}\n"
                                                f"Side: {candidate['side']}\n"
                                                f"Zone: {candidate['zone']['kind']}\n"
                                                f"Score: {candidate['score']:.2f}\n"
                                                f"Entry estimate: {request['price']:.8f}\n"
                                                f"Stop: {request['sl']:.8f}\n"
                                                f"Target: {request['tp']:.8f}\n"
                                                f"Volume: {request['volume']:.8f}\n"
                                                f"Estimated risk: {estimated_loss:.2f}\n"
                                                "No trading request was sent."
                                            ),
                                            "disable_web_page_preview": True,
                                        },
                                        timeout=10,
                                    )
                                except Exception:
                                    pass
                            continue

                        submitted = execute_request(
                            broker,
                            state,
                            state_path,
                            "ENTRY",
                            request,
                            {
                                "side": candidate["side"],
                                "zone_id": candidate["zone_id"],
                                "estimated_loss": estimated_loss,
                                "signal_time": candidate["signal_time"],
                            },
                        )
                        if submitted:
                            break

                consecutive_runtime_errors = 0
                time.sleep(CFG.POLL_SECONDS)

            except FatalError:
                raise

            except KeyboardInterrupt:
                raise

            except SafetyError:
                status(
                    "safety_cycle_error",
                    "A safety or data check failed. "
                    "This cycle will not submit further requests.",
                    60,
                )
                time.sleep(CFG.POLL_SECONDS)

            except Exception as e:
                import traceback
                consecutive_runtime_errors += 1
                max_err = int(getattr(CFG, "MAX_CONSECUTIVE_RUNTIME_ERRORS", 5))
                error_msg = (
                    f"Unexpected error ({consecutive_runtime_errors}/{max_err}): "
                    f"{str(e)}\n{traceback.format_exc()}"
                )
                notify(error_msg, critical=True)
                if consecutive_runtime_errors >= max_err:
                    mark_halt(
                        state,
                        state_path,
                        f"Repeated runtime exceptions ({consecutive_runtime_errors} consecutive).",
                    )
                time.sleep(CFG.POLL_SECONDS)

    finally:
        if broker is not None:
            try:
                broker.shutdown()
            except Exception:
                log("MT5 shutdown did not complete normally.")

        release_lock()


if __name__ == "__main__":
    try:
        main()

    except KeyboardInterrupt:
        notify(
            "SMC bot stopped by the operator. "
            "Open positions were not closed automatically."
        )

    except FatalError as error:
        notify(str(error), critical=True)
        sys.exit(1)

    except Exception:
        notify(
            "SMC bot stopped because initialization or execution failed. "
            "No external exception details were forwarded.",
            critical=True,
        )
        sys.exit(1)