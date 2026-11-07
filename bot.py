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
- Dry-run by default.
- Live mode additionally requires ALLOW_LIVE_TRADING=true.
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
import re
import sys
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

class TelegramLogHandler(logging.Handler):
    def emit(self, record):
        try:
            log_entry = self.format(record)
            if record.levelno >= logging.INFO:
                send_telegram_log_async(log_entry)
        except Exception:
            self.handleError(record)

def send_telegram_log_async(message):
    if not CFG.TELEGRAM_TOKEN or not CFG.TELEGRAM_CHAT_ID:
        return
    try:
        formatted_text = safe_text(message)
        requests.post(
            f"https://api.telegram.org/bot{CFG.TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": CFG.TELEGRAM_CHAT_ID,
                "text": formatted_text,
                "disable_web_page_preview": True,
            },
            timeout=5,
        )
    except Exception:
        pass

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")

SHARED_NEWS_FETCHER = NewsFetcher(check_interval=1800)
SHARED_NEWS_FETCHER.start()

LOCK_HANDLE = None
LAST_STATUS = {}


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

def scan_momentum_breakout_candidate(broker, symbol, m5_bars, side, volatility):
    if len(m5_bars) < 5:
        return None

    latest = m5_bars[-1]
    prev = m5_bars[-2]
    buy = side == "BUY"

    body = abs(latest["close"] - latest["open"])
    ref_atr = volatility[-1]

    if ref_atr <= 0 or body < ref_atr * 1.2:
        return None

    zone_key = f"{symbol}:MOMENTUM_BREAKOUT:{side}:{latest['time']}"

    if buy and latest["close"] > prev["high"] and latest["close"] > latest["open"]:
        extreme = min(b["low"] for b in m5_bars[-3:])
        return {
            "zone": {"kind": "MOMENTUM_BREAKOUT", "side": "BUY", "bottom": extreme, "top": latest["close"]},
            "side": "BUY",
            "extreme": extreme,
            "atr": ref_atr,
            "signal_close": latest["close"],
            "signal_time": int(latest["time"] + 300),
            "score": 6.5,
            "zone_id": zone_key,
        }
    elif not buy and latest["close"] < prev["low"] and latest["close"] < latest["open"]:
        extreme = max(b["high"] for b in m5_bars[-3:])
        return {
            "zone": {"kind": "MOMENTUM_BREAKOUT", "side": "SELL", "bottom": latest["close"], "top": extreme},
            "side": "SELL",
            "extreme": extreme,
            "atr": ref_atr,
            "signal_close": latest["close"],
            "signal_time": int(latest["time"] + 300),
            "score": 6.5,
            "zone_id": zone_key,
        }

    return None

def detect_market_regime(h4_bars):
    if len(h4_bars) < 30:
        return "RETRACEMENT_SMC"

    closes = [bar["close"] for bar in h4_bars]
    ema_fast = ema(closes, 50)
    ema_slow = ema(closes, 200)

    trend_aligned = abs(ema_fast[-1] - ema_slow[-1]) > (ema_slow[-1] * 0.01)

    highs, lows = swings(h4_bars, CFG.HTF_SWING_WINDOW)
    if len(highs) >= 2 and len(lows) >= 2:
        recent_range_span = abs(highs[-1]["price"] - lows[-1]["price"])
        avg_span = sum(h["price"] - l["price"] for h, l in zip(highs[-5:], lows[-5:])) / min(5, len(highs))
        
        if trend_aligned and recent_range_span > avg_span * 1.3:
            return "STRONG_TREND"

    if not trend_aligned:
        return "RANGING_SIDEWAY"

    return "RETRACEMENT_SMC"

def is_volume_building_up(bars, index, lookback=3):
    if index < lookback:
        return True
    subset = bars[index - lookback + 1 : index + 1]
    vols = [b["volume"] for b in subset]
    return vols[-1] >= sum(vols) / len(vols)

def safe_text(text):
    """
    Keep application output ASCII-only.

    External broker comments and raw exception messages are never forwarded.
    """
    return "".join(
        char
        for char in str(text)
        if char in "\n\t" or 32 <= ord(char) <= 126
    )


def log(message):
    logging.info(safe_text(message))

def notify(message):
    message = safe_text(message)
    log(message)

    if not CFG.TELEGRAM_TOKEN or not CFG.TELEGRAM_CHAT_ID:
        return

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{CFG.TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": CFG.TELEGRAM_CHAT_ID,
                "text": message,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )
        if response.status_code != 200:
            log("Telegram delivery failed.")
            return

        if not response.json().get("ok", False):
            log("Telegram delivery was rejected.")

    except Exception:
        log("Telegram delivery failed.")

def get_dynamic_sweep_atr(symbol, base_atr):
    if not symbol:
        return base_atr * 0.10
        
    symbol_upper = symbol.upper()
    
    if any(k in symbol_upper for k in ["XAU", "XPT", "XPD", "BTC", "ETH", "SOL", "BNB", "AVAX"]):
        multiplier = 0.12
    elif any(k in symbol_upper for k in ["30", "TEC", "500", "DE30", "FR40", "UK100", "JP225", "HK50", "AU200", "OIL", "XNG", "XCU"]):
        multiplier = 0.20
    elif any(k in symbol_upper for k in ["XRP", "ADA", "DOGE", "LINK", "DOT"]):
        multiplier = 0.15
    else:
        multiplier = 0.10
        
    return max(0.08, base_atr * multiplier)

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
        symbol, broker.constant("TIMEFRAME_D1"), 3
    )
    if not daily_bars or len(daily_bars) < 2:
        return None, None
    
    previous_day = daily_bars[-2]
    pdh = previous_day["high"]
    pdl = previous_day["low"]
    return pdh, pdl

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
        CFG.NEWS_FILE_MAX_AGE_HOURS,
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

    def tick(self, symbol):
        tick = self.named(f"mt5.symbol_info_tick({symbol!r})")
        bid = float(tick["bid"])
        ask = float(tick["ask"])
        stamp = tick_timestamp(tick)
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
        module = "__import__('datetime')"
        first = (
            f"{module}.datetime.fromtimestamp("
            f"{float(start)}, tz={module}.timezone.utc)"
        )
        last = (
            f"{module}.datetime.fromtimestamp("
            f"{float(end)}, tz={module}.timezone.utc)"
        )
        return self.records(
            f"mt5.history_deals_get({first}, {last})"
        )

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

        for bar in values:
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

    def select_symbols(self):
        valid_symbols = []
        for symbol in CFG.SYMBOLS:
            success = self.read(f"mt5.symbol_select({symbol!r}, True)")
            if success:
                valid_symbols.append(symbol)
            else:
                print(f"--- [WARNING] Symbol {symbol} cannot be selected and will be skipped. ---", flush=True)
        CFG.SYMBOLS = valid_symbols
        if not CFG.SYMBOLS:
            raise FatalError("No valid symbols could be selected.")

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
    """
    A swing is available only after all right confirmation bars close.
    """
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

    close = bars[-1]["close"]
    average = ema(
        [bar["close"] for bar in bars], CFG.BIAS_EMA_PERIOD
    )[-1]

    bullish = (
        highs[-1]["price"] > highs[-2]["price"]
        and lows[-1]["price"] > lows[-2]["price"]
    )
    bearish = (
        highs[-1]["price"] < highs[-2]["price"]
        and lows[-1]["price"] < lows[-2]["price"]
    )

    if bullish and close > average:
        pivot = lows[-1]
        broken = any(
            bar["close"] < pivot["price"]
            for bar in bars[pivot["index"] + 1:]
        )
        if not broken:
            return "BUY"

    if bearish and close < average:
        pivot = highs[-1]
        broken = any(
            bar["close"] > pivot["price"]
            for bar in bars[pivot["index"] + 1:]
        )
        if not broken:
            return "SELL"

    return "NEUTRAL"


def first_structure_break(bars, index, side, volatility):
    prior = bars[:index]
    highs, lows = swings(prior, CFG.STRUCTURE_SWING_WINDOW)
    buy = side == "BUY"
    pivots = highs if buy else lows

    if not pivots:
        return None

    pivot = pivots[-1]
    level = pivot["price"]
    close = bars[index]["close"]
    buffer = CFG.BOS_BUFFER_ATR * volatility

    intervening = bars[pivot["confirmed_index"] + 1:index]

    if buy:
        valid = (
            close > level + buffer
            and all(bar["close"] <= level for bar in intervening)
        )
    else:
        valid = (
            close < level - buffer
            and all(bar["close"] >= level for bar in intervening)
        )

    return pivot if valid else None

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

        XA = A - X
        AB = A - B
        XC = C - X
        XD = C - D

        if XA <= 0 or AB <= 0 or XC <= 0:
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

        XA = X - A
        AB = B - A
        XC = X - C

        if XA <= 0 or AB <= 0 or XC <= 0:
            return False

        b_ratio = (B - A) / XA
        c_ratio = (B - C) / AB
        d_ratio = (D - A) / XA

        valid_b = 0.45 <= b_ratio <= 0.65
        valid_c = 0.35 <= c_ratio <= 0.90
        valid_d = 0.75 <= d_ratio <= 0.90

        return valid_b and valid_c and valid_d

def calculate_scoring_logic(bars, zone, sweep_bar, confirmation_bar, reference_atr, pdh=None, pdl=None):
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
            (sweep_bar["high"] - sweep_bar["close"]) / bar_range 
            if buy 
            else (sweep_bar["close"] - sweep_bar["low"]) / bar_range
        )
        if wick_ratio >= 0.40:
            score += 2.5
        elif wick_ratio >= 0.20:
            score += 1.0

    vol_ma = volume_ma(bars, CFG.VOLUME_MA_PERIOD)
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
        if check_harmonic_bat_pattern(bars, zone["side"]):
            score += getattr(CFG, "HARMONIC_SCORE_BOOST", 2.0)

    score += 1.5
    return min(score, 10.0)

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

        if not directional or candle_range <= 0 or body < CFG.DISPLACEMENT_ATR * reference_atr:
            continue

        pivot = first_structure_break(bars, impulse_index, side, reference_atr)
        if pivot is None:
            continue

        ob_index = None
        for source in range(impulse_index - 1, max(-1, impulse_index - CFG.OB_SEARCH_BACK - 1), -1):
            source_bar = bars[source]
            opposite = source_bar["close"] < source_bar["open"] if buy else source_bar["close"] > source_bar["open"]
            if opposite:
                ob_index = source
                break

        dealing = bars[impulse_index - CFG.DEALING_RANGE_BARS:impulse_index]
        range_high = max(bar["high"] for bar in dealing)
        range_low = min(bar["low"] for bar in dealing)
        width = range_high - range_low

        if width <= 0:
            continue

        if buy:
            fibo_zone_top = range_high - (width * 0.618)
            fibo_zone_bottom = range_high - (width * 0.786)
        else:
            fibo_zone_bottom = range_low + (width * 0.618)
            fibo_zone_top = range_low + (width * 0.786)

        equilibrium = (range_high + range_low) / 2.0
        created = int(right["time"] + 900)

        def add_zone(kind, bottom, top, source_index, is_mitigation=False):
            if bottom >= top:
                return
            
            midpoint = (bottom + top) / 2.0

            if buy:
                if not (fibo_zone_bottom <= midpoint <= fibo_zone_top):
                    return
            else:
                if not (fibo_zone_bottom <= midpoint <= fibo_zone_top):
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
                "bos_level": float(pivot["price"]),
                "is_mitigation": bool(is_mitigation),
                "confluence": True,
            })

        if CFG.ENABLE_MITIGATION_BLOCK and ob_index is not None:
            source_bar = bars[ob_index]
            path_after_ob = bars[ob_index + 1 : impulse_index]
            broken_ob = False
            for b in path_after_ob:
                if buy and b["low"] < source_bar["low"]:
                    broken_ob = True
                    break
                elif not buy and b["high"] > source_bar["high"]:
                    broken_ob = True
                    break
            
            if broken_ob:
                add_zone("MITIGATION_BLOCK", source_bar["low"], source_bar["high"], ob_index, is_mitigation=True)
            else:
                add_zone("OB", source_bar["low"], source_bar["high"], ob_index, is_mitigation=False)

    unique = {zone["local_id"]: zone for zone in result}
    return sorted(unique.values(), key=lambda zone: zone["created"], reverse=True)

def zone_signal(zone, bars, volatility, pdh=None, pdl=None):
    if not bars or bars[0]["time"] > zone["created"]:
        return None

    latest = len(bars) - 1
    decision_time = bars[-1]["time"] + 300

    signal_score_modifier = 0.0

    if decision_time - zone["created"] > CFG.MAX_ZONE_AGE_M15_BARS * 900:
        signal_score_modifier -= 2.0

    buy = zone["side"] == "BUY"
    eligible = [index for index, bar in enumerate(bars) if bar["time"] >= zone["created"]]
    if not eligible:
        return None

    touch = None
    for index in eligible:
        bar = bars[index]
        reached = bar["low"] <= zone["top"] if buy else bar["high"] >= zone["bottom"]
        if reached:
            touch = index
            break

    if touch is None:
        return None

    if latest - touch > CFG.MAX_TOUCH_WAIT_BARS:
        signal_score_modifier -= 1.5

    for bar in bars[touch:]:
        if buy and bar["close"] < zone["bottom"]:
            signal_score_modifier -= 3.0
        if not buy and bar["close"] > zone["top"]:
            signal_score_modifier -= 3.0

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

        bar_range = sweep_bar["high"] - sweep_bar["low"]
        if bar_range > 0:
            wick_ratio = (
                (sweep_bar["high"] - sweep_bar["close"]) / bar_range
                if buy
                else (sweep_bar["close"] - sweep_bar["low"]) / bar_range
            )
            if wick_ratio > CFG.MAX_SWEEP_WICK_RATIO:
                signal_score_modifier -= 1.0

        overlaps = sweep_bar["low"] <= zone["top"] and sweep_bar["high"] >= zone["bottom"]
        if not overlaps:
            continue

        if buy:
            counter_structure = highs[-1]["price"] < highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]
            liquidity = lows[-1]
            confirmation = highs[-1]
        else:
            counter_structure = highs[-1]["price"] > highs[-2]["price"] and lows[-1]["price"] < lows[-2]["price"]
            liquidity = highs[-1]
            confirmation = lows[-1]

        if not counter_structure:
            signal_score_modifier -= 1.5

        if sweep_index - liquidity["index"] > CFG.LIQUIDITY_MAX_AGE:
            signal_score_modifier -= 1.0

        intervening = bars[liquidity["confirmed_index"] + 1:sweep_index]
        already_taken = any(
            bar["low"] <= liquidity["price"] if buy else bar["high"] >= liquidity["price"]
            for bar in intervening
        )
        if already_taken:
            signal_score_modifier -= 1.5

        dynamic_sweep_min = get_dynamic_sweep_atr(zone["symbol"] if "symbol" in zone else "EURUSD", reference_atr)

        if buy:
            swept = (
                sweep_bar["low"] < liquidity["price"] - dynamic_sweep_min
                and sweep_bar["close"] > liquidity["price"]
            )
        else:
            swept = (
                sweep_bar["high"] > liquidity["price"] + dynamic_sweep_min
                and sweep_bar["close"] < liquidity["price"]
            )

        if not swept:
            signal_score_modifier -= 1.0

        level = confirmation["price"]
        confirmation_end = min(sweep_index + CFG.CONFIRM_AFTER_SWEEP_BARS, latest)

        for confirmation_index in range(sweep_index + 1, confirmation_end + 1):
            bar = bars[confirmation_index]
            raw_break = bar["close"] > level if buy else bar["close"] < level
            if not raw_break:
                signal_score_modifier -= 0.5
                continue

            reference = volatility[confirmation_index - 1]
            close_buffer = CFG.CONFIRM_CLOSE_BUFFER_ATR * reference
            body = abs(bar["close"] - bar["open"])

            strong_close = (
                (bar["close"] > level + close_buffer and bar["close"] > bar["open"])
                if buy
                else (bar["close"] < level - close_buffer and bar["close"] < bar["open"])
            )

            if not strong_close:
                signal_score_modifier -= 1.0
            if body < CFG.CONFIRM_MIN_BODY_ATR * reference:
                signal_score_modifier -= 1.0

            score = calculate_scoring_logic(bars, zone, sweep_bar, bar, reference, pdh, pdl)
            final_score = score + signal_score_modifier

            if final_score < CFG.MIN_SCORING_THRESHOLD:
                break

            path = bars[touch:confirmation_index + 1]
            extreme = min(item["low"] for item in path) if buy else max(item["high"] for item in path)

            return {
                "zone": zone,
                "side": zone["side"],
                "extreme": extreme,
                "atr": volatility[-1],
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

def parse_utc_timestamp(value):
    if not isinstance(value, str):
        raise ValueError("UTC timestamp must be a string.")

    stamp = datetime.fromisoformat(value.replace("Z", "+00:00"))

    if stamp.tzinfo is None:
        raise ValueError("Timezone-aware timestamp required.")

    return stamp.astimezone(timezone.utc).timestamp()


def get_news_sentiment_score(symbol, side):
    if not getattr(CFG, "ENABLE_NEWS_SCORING_PENALTY", False):
        return 0.0

    now = datetime.now(timezone.utc).replace(tzinfo=None)
    clean = re.sub(r"[^A-Z]", "", symbol.upper())
    if len(clean) < 6:
        return 0.0
    clean = clean[:6]

    base_currency = clean[:3]
    quote_currency = clean[3:6]
    
    relevant_currencies = {base_currency, quote_currency}
    if "USD" in relevant_currencies or "XAU" in clean or "XAG" in clean:
        relevant_currencies.update(["USD", "EUR", "GBP", "JPY", "AUD", "CAD"])

    sentiment_score = 0.0

    with SHARED_NEWS_FETCHER._lock:
        for news in SHARED_NEWS_FETCHER.high_impact_news:
            cur = news.get("currency")
            if cur not in relevant_currencies:
                continue
            
            news_time = news.get("time")
            if not news_time:
                continue
                
            diff_minutes = (news_time - now).total_seconds() / 60
            if -30 <= diff_minutes <= 30:
                impact = news.get("impact", "Medium")
                actual = news.get("actual")
                forecast = news.get("forecast")
                
                direction_bias = "NEUTRAL"
                if actual is not None and actual != "" and forecast is not None and forecast != "":
                    try:
                        act_val = float(str(actual).replace('%', '').strip())
                        fore_val = float(str(forecast).replace('%', '').strip())
                        
                        if act_val > fore_val:
                            direction_bias = "BULLISH"
                        elif act_val < fore_val:
                            direction_bias = "BEARISH"
                    except ValueError:
                        pass
                
                is_base = (cur == base_currency or (cur == "USD" and base_currency in ["XAU", "XAG"]))
                
                if direction_bias == "BULLISH":
                    favorable_side = "BUY" if is_base else "SELL"
                elif direction_bias == "BEARISH":
                    favorable_side = "SELL" if is_base else "BUY"
                else:
                    favorable_side = "NEUTRAL"

                boost_val = getattr(CFG, "NEWS_SCORE_BOOST_HIGH", 3.0) if impact == "High" else getattr(CFG, "NEWS_SCORE_BOOST_MED", 1.5)
                penalty_val = CFG.NEWS_HIGH_IMPACT_PENALTY if impact == "High" else CFG.NEWS_MEDIUM_IMPACT_PENALTY

                if favorable_side == side:
                    sentiment_score += boost_val
                elif favorable_side != "NEUTRAL" and favorable_side != side:
                    sentiment_score -= penalty_val

    return sentiment_score

def news_permission(broker, symbol):
    if getattr(CFG, "ENABLE_NEWS_SCORING_PENALTY", False):
        return True, "News sentiment scoring mode active."

    if not CFG.ENABLE_NEWS_FILTER:
        return True, "News filter disabled."

    if SHARED_NEWS_FETCHER.is_restricted_time(symbol, buffer_minutes=CFG.NEWS_BEFORE_MINUTES):
        return False, "A high-impact news blackout is active (Dynamic API)."

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

    h4 = [
        bar for bar in h4
        if bar["time"] + 14400 <= decision_time
    ]
    m15 = [
        bar for bar in m15
        if bar["time"] + 900 <= decision_time
    ]

    regime = detect_market_regime(h4)
    side = htf_bias(h4)

    if side == "NEUTRAL" or len(m15) < CFG.DEALING_RANGE_BARS + 10:
        return None

    volatility = atr(m5)
    current_price = m5[-1]["close"]

    candidate = None

    if regime == "STRONG_TREND":
        candidate = scan_momentum_breakout_candidate(broker, symbol, m5, side, volatility)
    
    elif regime == "RANGING_SIDEWAY":
        candidate = None
    
    else:
        for zone in create_zones(symbol, m15, side):
            zone_id = f"{symbol}:{zone['local_id']}"

            if zone_id in state["seen_zones"]:
                continue

            candidate = zone_signal(zone, m5, volatility, pdh, pdl)
            if candidate is not None:
                candidate["zone_id"] = zone_id
                break

    if candidate is None:
        return None

    m15_bonus = m15_confirmation(candidate, m15)
    score_boost = m15_bonus

    if pdh is not None and pdl is not None:
        if candidate["side"] == "SELL" and current_price >= pdh - (volatility[-1] * 1.5):
            score_boost += 2.0
        elif candidate["side"] == "BUY" and current_price <= pdl + (volatility[-1] * 1.5):
            score_boost += 2.0

    if check_trendline_liquidity_sweep(m5, candidate["side"]):
        score_boost += 1.5

    candidate["score"] = candidate.get("score", 6.0) + score_boost

    news_sentiment = get_news_sentiment_score(symbol, candidate["side"])
    candidate["score"] += news_sentiment

    if candidate["score"] < CFG.MIN_SCORING_THRESHOLD:
        return None

    candidate["symbol"] = symbol
    return candidate

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

    if flags & broker.constant("SYMBOL_FILLING_FOK"):
        return broker.constant("ORDER_FILLING_FOK")

    if flags & broker.constant("SYMBOL_FILLING_IOC"):
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


def build_order(broker, candidate, account):
    symbol = candidate["symbol"]
    buy = candidate["side"] == "BUY"

    allowed, reason = news_permission(broker, symbol)

    if not allowed:
        status(f"news_{symbol}", f"{symbol}: {reason}", 60)
        return None

    info = broker.info(symbol)
    tick = broker.tick(symbol)

    if tick_timestamp(tick) < candidate["signal_time"]:
        return None

    signal_age = time.time() - candidate["signal_time"]

    if (
        signal_age < 0
        or signal_age > CFG.MAX_CLOSED_M5_AGE_SECONDS
    ):
        return None

    allowed_modes = {
        broker.constant("SYMBOL_TRADE_MODE_FULL"),
        broker.constant(
            "SYMBOL_TRADE_MODE_LONGONLY"
            if buy
            else "SYMBOL_TRADE_MODE_SHORTONLY"
        ),
    }

    if int(info["trade_mode"]) not in allowed_modes:
        return None

    spread_limit = float(
        CFG.MAX_SPREAD_BY_SYMBOL.get(symbol, CFG.MAX_SPREAD_POINTS)
    )

    if spread_points(tick, info) > spread_limit:
        return None

    entry = float(tick["ask"] if buy else tick["bid"])
    volatility = float(candidate["atr"])

    if volatility <= 0:
        return None

    if (
        abs(entry - candidate["signal_close"])
        > CFG.MAX_ENTRY_DRIFT_ATR * volatility
    ):
        return None

    zone = candidate["zone"]
    buffer = CFG.SL_BUFFER_ATR * volatility
    gap = stop_gap(info)

    if buy:
        raw_sl = min(zone["bottom"], candidate["extreme"]) - buffer
        raw_sl = min(raw_sl, float(tick["bid"]) - gap)
        stop = price_round(raw_sl, info)
        risk_distance = entry - stop
        order_type = broker.constant("ORDER_TYPE_BUY")
    else:
        spread_price = float(tick["ask"]) - float(tick["bid"])
        raw_sl = (
            max(zone["top"], candidate["extreme"])
            + buffer
            + spread_price
        )
        raw_sl = max(raw_sl, float(tick["ask"]) + gap)
        stop = price_round(raw_sl, info, upward=True)
        risk_distance = stop - entry
        order_type = broker.constant("ORDER_TYPE_SELL")

    if (
        stop <= 0
        or risk_distance <= 0
        or risk_distance > CFG.MAX_SL_ATR * volatility
    ):
        return None

    if buy:
        target = price_round(
            max(
                entry + CFG.TP2_RR * risk_distance,
                float(tick["bid"]) + gap,
            ),
            info,
            upward=True,
        )
    else:
        target = price_round(
            min(
                entry - CFG.TP2_RR * risk_distance,
                float(tick["ask"]) - gap,
            ),
            info,
        )

    if target <= 0:
        return None

    allowance = CFG.RISK_SLIPPAGE_POINTS * float(info["point"])
    worse_entry = entry + allowance if buy else entry - allowance
    worse_stop = stop - allowance if buy else stop + allowance

    if min(worse_entry, worse_stop) <= 0:
        return None

    step = float(info["volume_step"])
    minimum = float(info["volume_min"])
    maximum = min(float(info["volume_max"]), CFG.MAX_LOT)

    if step <= 0 or minimum <= 0 or maximum < minimum:
        return None

    reference_loss = abs(
        broker.profit(
            order_type, symbol, minimum, worse_entry, worse_stop
        )
    ) + CFG.COMMISSION_PER_LOT * minimum

    if reference_loss <= 0:
        return None

    estimated_per_lot = reference_loss / minimum

    volume = floor_volume(
        min(CFG.RISK_PER_TRADE / estimated_per_lot, maximum),
        step,
    )

    if volume + CFG.EPSILON < minimum:
        return None

    estimated_loss = abs(
        broker.profit(
            order_type, symbol, volume, worse_entry, worse_stop
        )
    ) + CFG.COMMISSION_PER_LOT * volume

    if estimated_loss > CFG.RISK_PER_TRADE + CFG.EPSILON:
        return None

    margin = broker.margin(order_type, symbol, volume, entry)

    if margin < 0 or margin > float(account["margin_free"]) * 0.90:
        return None

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
        notify(f"Safety halt activated. Reason: {reason}")


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
            "Further requests are blocked pending reconciliation."
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
                        float(deal["time"]) for deal in entries
                    ),
                    "zone_id": pending["metadata"]["zone_id"],
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

                if actual_risk > CFG.RISK_PER_TRADE + CFG.EPSILON:
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
        state["pending"] = None
        atomic_save(path, state)
        notify(f"Execution rejected. Action: {kind}. Code: {code}")
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
            float(last_exit["time"]),
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

    if state["day"] != today:
        state["day"] = today
        state["day_start_balance"] = (
            float(account["balance"]) - total_cash
        )
        state["daily_loss_latched"] = False
        state["dry_run_entries"] = 0

        cutoff = time.time() - 14 * 86400
        state["seen_zones"] = {
            key: stamp
            for key, stamp in state["seen_zones"].items()
            if float(stamp) >= cutoff
        }

        atomic_save(path, state)

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


def in_session():
    hour = utc_now().hour

    if CFG.USE_KILLZONES_ONLY:
        for start_k, end_k in CFG.KILLZONES_UTC:
            if start_k <= hour < end_k:
                return True
        return False

    if CFG.SESSION_START_HOUR_UTC == CFG.SESSION_END_HOUR_UTC:
        return True

    if CFG.SESSION_START_HOUR_UTC < CFG.SESSION_END_HOUR_UTC:
        return CFG.SESSION_START_HOUR_UTC <= hour < CFG.SESSION_END_HOUR_UTC

    return (
        hour >= CFG.SESSION_START_HOUR_UTC
        or hour < CFG.SESSION_END_HOUR_UTC
    )


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

    if snapshot["daily_entries"] >= CFG.MAX_DAILY_TRADES:
        return False, "The daily entry limit has been reached."

    bot_positions = [
        position for position in positions
        if int(position.get("magic", 0)) == CFG.MAGIC
    ]

    if len(bot_positions) >= CFG.MAX_OPEN_POSITIONS:
        return False, "The open-position limit has been reached."

    if any(int(order.get("magic", 0)) == CFG.MAGIC for order in orders):
        return False, "A bot order is still active."

    if not in_session():
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

        if (
            CFG.ENABLE_TIME_EXIT
            and age_seconds >= CFG.TIME_EXIT_MINUTES * 60
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

        if not record["partial_done"]:
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
        desired_sl = None

        if buy:
            valid_lows = [l["price"] for l in lows if l["price"] < price]
            if valid_lows:
                structural_sl = valid_lows[-1] - buffer
                desired_sl = max(entry, structural_sl)
        else:
            valid_highs = [h["price"] for h in highs if h["price"] > price]
            if valid_highs:
                structural_sl = valid_highs[-1] + buffer
                desired_sl = min(entry, structural_sl)

        if desired_sl is None:
            continue

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


def main():
    validate_config()
    broker = None

    if CFG.TELEGRAM_TOKEN and CFG.TELEGRAM_CHAT_ID:
        root_logger = logging.getLogger()
        root_logger.setLevel(logging.INFO)
        
        if not any(isinstance(h, TelegramLogHandler) for h in root_logger.handlers):
            tg_handler = TelegramLogHandler()
            formatter = logging.Formatter("%(asctime)s UTC | %(levelname)s | %(message)s", datefmt="%Y-%m-%d %H:%M:%S")
            tg_handler.setFormatter(formatter)
            root_logger.addHandler(tg_handler)

    try:
        broker = Broker()
        account = broker.account()
        account_key = account_key_from(account)

        acquire_lock(account_key)
        validate_account(broker, account_key)
        broker.select_symbols()

        mode = "dry" if CFG.DRY_RUN else "live"

        state_path = ROOT / (
            f"bot_state_v2_{account_key}_{CFG.MAGIC}_{mode}.json"
        )

        state = load_state(state_path, account_key)
        atomic_save(state_path, state)

        notify(
            "SMC bot started.\n"
            f"Mode: {'DRY_RUN' if CFG.DRY_RUN else 'LIVE'}\n"
            "Account type: Hedging\n"
            "Time basis: UTC\n"
            f"OB/FVG confluence: {CFG.REQUIRE_OB_FVG_CONFLUENCE}\n"
            f"M15 confirmation: {CFG.REQUIRE_M15_CONFIRMATION}\n"
            f"News filter: {CFG.ENABLE_NEWS_FILTER}\n"
            f"Time exit: {CFG.ENABLE_TIME_EXIT}"
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

        while True:
            try:
                validate_account(broker, account_key)

                resolve_pending(broker, state, state_path)
                reconcile_positions(broker, state, state_path)

                if state["pending"] is not None:
                    time.sleep(CFG.POLL_SECONDS)
                    continue

                if state["halt_reason"]:
                    status(
                        "persistent_halt",
                        "A persistent safety halt is active. "
                        "No automatic trading requests will be sent.",
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

                for symbol in CFG.SYMBOLS:
                    print(f"--- [SCANNING] Checking symbol: {symbol} ---", flush=True)

                    snapshot = daily_snapshot(
                        broker, state, state_path
                    )
                    positions = broker.positions()
                    orders = broker.orders()

                    permitted, reason = entry_permission(
                        broker, state, snapshot, positions, orders
                    )

                    if not permitted:
                        status("entry_gate", reason)
                        continue

                    if any(
                        item["symbol"] == symbol
                        for item in positions + orders
                    ):
                        continue

                    news_ok, news_reason = news_permission(
                        broker, symbol
                    )

                    if not news_ok:
                        status(
                            f"news_{symbol}",
                            f"{symbol}: {news_reason}",
                            60,
                        )
                        continue

                    candidate = scan_symbol(
                        broker, symbol, state
                    )

                    if candidate is not None:
                        all_candidates.append(candidate)

                if all_candidates:
                    all_candidates.sort(key=lambda x: x["score"], reverse=True)

                    top_signal_text = "Top Ranked Signals:\n"
                    for c in all_candidates[:5]:
                        top_signal_text += f"- {c['symbol']} ({c['side']}) [{c['zone']['kind']}] | Score: {c['score']:.2f}\n"
                    notify(top_signal_text)

                    for candidate in all_candidates:
                        symbol = candidate["symbol"]

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

                        bot_positions = [
                            p for p in fresh_positions 
                            if int(p.get("magic", 0)) == CFG.MAGIC
                        ]
                        if len(bot_positions) >= CFG.MAX_OPEN_POSITIONS:
                            break

                        built = build_order(
                            broker,
                            candidate,
                            fresh_snapshot["account"],
                        )

                        if built is None:
                            continue

                        request, estimated_loss = built

                        state["seen_zones"][
                            candidate["zone_id"]
                        ] = time.time()

                        if CFG.DRY_RUN:
                            state["dry_run_entries"] = (
                                int(state.get("dry_run_entries", 0)) + 1
                            )

                        atomic_save(state_path, state)

                        if CFG.DRY_RUN:
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

                        execute_request(
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

            except Exception:
                status(
                    "unexpected_cycle_error",
                    "An unexpected runtime error occurred. "
                    "This cycle stopped without exposing external details.",
                    60,
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
        notify(str(error))
        sys.exit(1)

    except Exception:
        notify(
            "SMC bot stopped because initialization or execution failed. "
            "No external exception details were forwarded."
        )
        sys.exit(1)