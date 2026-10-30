#!/usr/bin/env python3
"""
Single-file Price Action and SMC bot for mt5linux.

Operational rules:
- H4 confirmed swing structure and EMA filter.
- M15 displacement order blocks or three-candle fair value gaps.
- Frozen premium or discount filter at zone creation.
- First post-creation M5 touch only.
- M5 liquidity sweep at or shortly after that touch.
- Subsequent M5 close through a pre-sweep confirmed swing.
- Closed candles only.
- No repeated entry from the same zone.

Execution rules:
- Dry-run mode is enabled by default.
- Hedging accounts only.
- One position or active order per symbol, regardless of magic.
- Durable execution journal before every trading request.
- Ambiguous execution blocks further requests until reconciled.
- Position tracking uses the stable position identifier.
- Initial risk distance is immutable.
- Stop changes must not increase risk.
- Partial closes are reconciled from actual deals.

Limitations:
- This is a rule-based interpretation of SMC.
- OHLC data does not reveal institutional intent.
- Intrabar event ordering cannot be reconstructed from OHLC.
- Server-side SL and TP remain necessary.
- Standalone account fees may not be attributable to a position.
- Live execution requires broker-specific testing.

All application-authored messages and comments are English.
"""

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

try:
    import fcntl
except ImportError:
    fcntl = None


ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def env_bool(name, default):
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false", "1", "0", "yes", "no"}:
        raise ValueError("Invalid Boolean configuration.")
    return value in {"true", "1", "yes"}


DRY_RUN = env_bool("DRY_RUN", False)

SYMBOLS = [
    item.strip()
    for item in os.getenv(
        "SYMBOLS",
        "EURUSDm,GBPUSDm,USDJPYm,XAUUSDm",
    ).split(",")
    if item.strip()
]

MAGIC = 990011
STATE_VERSION = 2

POLL_SECONDS = float(os.getenv("POLL_SECONDS", "15"))
RISK_PER_TRADE = float(os.getenv("RISK_PER_TRADE", "5"))
COMMISSION_PER_LOT = float(os.getenv("COMMISSION_PER_LOT", "0"))

MAX_SPREAD_POINTS = float(os.getenv("MAX_SPREAD_POINTS", "30"))
MAX_SPREAD_BY_SYMBOL = json.loads(
    os.getenv("MAX_SPREAD_BY_SYMBOL_JSON", "{}")
)

MAX_OPEN_POSITIONS = 2
MAX_DAILY_TRADES = 6
MAX_DAILY_LOSS_PCT = 0.03
MAX_CONSECUTIVE_LOSSES = 3
COOLDOWN_SECONDS = 3600

SESSION_START_HOUR_UTC = 6
SESSION_END_HOUR_UTC = 20

BIAS_EMA_PERIOD = 200
H4_BAR_COUNT = 650
M15_BAR_COUNT = 180
M5_BAR_COUNT = 650

HTF_SWING_WINDOW = 5
LTF_SWING_WINDOW = 2
STRUCTURE_SWING_WINDOW = 2

ZONE_LOOKBACK = 50
DEALING_RANGE_BARS = 50
OB_SEARCH_BACK = 4
DISPLACEMENT_ATR = 1.0
MIN_FVG_ATR = 0.05

LIQUIDITY_MAX_AGE = 30
SWEEP_MIN_ATR = 0.25
SWEEP_AFTER_TOUCH_BARS = 3
CONFIRM_AFTER_SWEEP_BARS = 6

SL_BUFFER_ATR = 0.30
MAX_SL_ATR = 3.0
MAX_ENTRY_DRIFT_ATR = 0.50
TP1_RR = 1.5
TP2_RR = 2.5
TRAIL_ATR = 1.2

MAX_LOT = 5.0
DEVIATION_POINTS = 20
RISK_SLIPPAGE_POINTS = 20

MAX_TICK_AGE_SECONDS = 60
MAX_CLOSED_M5_AGE_SECONDS = 420
EPSILON = 1e-8

MT5_HOST = os.getenv("MT5_HOST", "localhost").strip()
MT5_PORT = int(os.getenv("MT5_PORT", "8002"))
MT5_LOGIN = int(os.getenv("MT5_LOGIN", "0") or "0")
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER = os.getenv("MT5_SERVER", "")

TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

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
        hour=0,
        minute=0,
        second=0,
        microsecond=0,
    ).timestamp()


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
    timestamp = utc_now().strftime("%Y-%m-%d %H:%M:%S UTC")
    print(f"[{timestamp}] {safe_text(message)}", flush=True)


def notify(message):
    message = safe_text(message)
    log(message)

    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return

    try:
        response = requests.post(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            json={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": message,
                "disable_web_page_preview": True,
            },
            timeout=10,
        )

        if response.status_code != 200:
            log("Telegram delivery failed.")
            return

        payload = response.json()
        if not payload.get("ok", False):
            log("Telegram delivery was rejected.")

    except Exception:
        log("Telegram delivery failed.")


def status(key, message, interval=300):
    now = time.time()
    previous = LAST_STATUS.get(key, 0.0)
    if now - previous >= interval:
        LAST_STATUS[key] = now
        log(message)


def validate_config():
    if not SYMBOLS or len(SYMBOLS) != len(set(SYMBOLS)):
        raise FatalError("The symbol list is empty or contains duplicates.")

    if any(
        re.fullmatch(r"[A-Za-z0-9._-]+", symbol) is None
        for symbol in SYMBOLS
    ):
        raise FatalError("Symbol names must contain supported ASCII characters.")

    values = [
        POLL_SECONDS,
        RISK_PER_TRADE,
        MAX_SPREAD_POINTS,
        MAX_LOT,
    ]

    if any(not math.isfinite(value) or value <= 0 for value in values):
        raise FatalError("Positive finite configuration values are required.")

    if not math.isfinite(COMMISSION_PER_LOT) or COMMISSION_PER_LOT < 0:
        raise FatalError("Commission configuration is invalid.")

    if not isinstance(MAX_SPREAD_BY_SYMBOL, dict):
        raise FatalError("Spread overrides must be a JSON object.")

    for value in MAX_SPREAD_BY_SYMBOL.values():
        number = float(value)
        if not math.isfinite(number) or number <= 0:
            raise FatalError("A spread override is invalid.")

    if not 0 < MAX_DAILY_LOSS_PCT < 1:
        raise FatalError("The daily loss limit is invalid.")

    if TP1_RR <= 0 or TP2_RR <= TP1_RR:
        raise FatalError("Reward targets are invalid.")


def acquire_lock(account_key):
    global LOCK_HANDLE

    if fcntl is None:
        raise FatalError("This build requires a Linux process lock.")

    lock_path = ROOT / f"smc_{account_key}_{MAGIC}.lock"
    LOCK_HANDLE = open(lock_path, "a", encoding="ascii")

    try:
        fcntl.flock(
            LOCK_HANDLE.fileno(),
            fcntl.LOCK_EX | fcntl.LOCK_NB,
        )
    except OSError:
        raise FatalError("Another bot instance already holds the account lock.")


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
        raise FatalError("State persistence failed. Trading has stopped.")


def load_state(path, account_key):
    if not path.exists():
        return {
            "version": STATE_VERSION,
            "account_key": account_key,
            "mode": "DRY_RUN" if DRY_RUN else "LIVE",
            "day": "",
            "day_start_balance": None,
            "daily_loss_latched": False,
            "consecutive_losses": 0,
            "cooldown_until": 0.0,
            "positions": {},
            "seen_zones": {},
            "pending": None,
            "halt_reason": "",
        }

    try:
        state = json.loads(path.read_text(encoding="ascii"))
    except Exception:
        raise FatalError("The state file is unreadable. Trading has stopped.")

    expected_mode = "DRY_RUN" if DRY_RUN else "LIVE"

    if (
        state.get("version") != STATE_VERSION
        or state.get("account_key") != account_key
        or state.get("mode") != expected_mode
    ):
        raise FatalError("The state file does not match this account or mode.")

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
        raise FatalError("Position state is invalid.")

    if not isinstance(state["seen_zones"], dict):
        raise FatalError("Signal state is invalid.")

    return state


class Broker:
    def __init__(self):
        self.mt5 = MetaTrader5(host=MT5_HOST, port=MT5_PORT)

        options = {}
        if MT5_LOGIN > 0:
            options["login"] = MT5_LOGIN
        if MT5_PASSWORD:
            options["password"] = MT5_PASSWORD
        if MT5_SERVER:
            options["server"] = MT5_SERVER

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
            f"(lambda value: None if value is None "
            f"else value._asdict())({expression})"
        )
        if result is None:
            raise SafetyError("Required MT5 information is unavailable.")
        return result

    def records(self, expression):
        result = self.read(
            f"(lambda values: None if values is None "
            f"else [value._asdict() for value in values])({expression})"
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
        stamp = float(tick.get("time_msc", 0)) / 1000.0

        if stamp <= 0:
            stamp = float(tick["time"])

        age = time.time() - stamp

        if (
            not math.isfinite(bid)
            or not math.isfinite(ask)
            or bid <= 0
            or ask < bid
            or age > MAX_TICK_AGE_SECONDS
            or age < -5
        ):
            raise SafetyError("A valid recent market tick is unavailable.")

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

        start_expr = (
            f"{module}.datetime.fromtimestamp("
            f"{float(start)}, tz={module}.timezone.utc)"
        )
        end_expr = (
            f"{module}.datetime.fromtimestamp("
            f"{float(end)}, tz={module}.timezone.utc)"
        )

        return self.records(
            f"mt5.history_deals_get({start_expr}, {end_expr})"
        )

    def bars(self, symbol, timeframe, count):
        expression = (
            "(lambda values: None if values is None else ["
            "{'time': int(row['time']), "
            "'open': float(row['open']), "
            "'high': float(row['high']), "
            "'low': float(row['low']), "
            "'close': float(row['close'])} "
            "for row in values])"
            f"(mt5.copy_rates_from_pos("
            f"{symbol!r}, {int(timeframe)}, 1, {int(count)}))"
        )

        values = self.read(expression)

        if values is None or len(values) < count:
            raise SafetyError("Required closed-bar history is unavailable.")

        previous = None

        for bar in values:
            if previous is not None and bar["time"] <= previous:
                raise SafetyError("Market-bar timestamps are invalid.")

            previous = bar["time"]

            numbers = [
                bar["open"],
                bar["high"],
                bar["low"],
                bar["close"],
            ]

            if any(not math.isfinite(value) or value <= 0 for value in numbers):
                raise SafetyError("Market-bar prices are invalid.")

            if (
                bar["high"] < max(bar["open"], bar["close"], bar["low"])
                or bar["low"] > min(bar["open"], bar["close"], bar["high"])
            ):
                raise SafetyError("Market-bar ranges are invalid.")

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
            f"mt5.order_calc_profit("
            f"{int(order_type)}, {symbol!r}, {float(volume)}, "
            f"{float(entry)}, {float(stop)})"
        )

        if value is None or not math.isfinite(float(value)):
            raise SafetyError("Profit calculation is unavailable.")

        return float(value)

    def margin(self, order_type, symbol, volume, price):
        value = self.read(
            f"mt5.order_calc_margin("
            f"{int(order_type)}, {symbol!r}, "
            f"{float(volume)}, {float(price)})"
        )

        if value is None or not math.isfinite(float(value)):
            raise SafetyError("Margin calculation is unavailable.")

        return float(value)

    def select_symbols(self):
        for symbol in SYMBOLS:
            if not self.read(f"mt5.symbol_select({symbol!r}, True)"):
                raise FatalError("A configured symbol could not be selected.")

    def shutdown(self):
        self.mt5.shutdown()


def ema(values, period):
    if not values:
        return []

    result = [float(values[0])]
    alpha = 2.0 / (period + 1.0)

    for value in values[1:]:
        result.append(result[-1] + alpha * (float(value) - result[-1]))

    return result


def atr(bars, period=14):
    """
    Wilder-style recursive smoothing, initialized from the first range.
    Sufficient warm-up bars are requested before signals are evaluated.
    """
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
    Strict extrema only.

    Each swing becomes available after its right-hand confirmation bars close.
    """
    highs = []
    lows = []

    for index in range(window, len(bars) - window):
        center = bars[index]
        neighbors = (
            bars[index - window:index]
            + bars[index + 1:index + window + 1]
        )

        if all(center["high"] > item["high"] for item in neighbors):
            highs.append({
                "index": index,
                "confirmed_index": index + window,
                "price": center["high"],
                "time": center["time"],
            })

        if all(center["low"] < item["low"] for item in neighbors):
            lows.append({
                "index": index,
                "confirmed_index": index + window,
                "price": center["low"],
                "time": center["time"],
            })

    return highs, lows


def htf_bias(bars):
    if len(bars) < BIAS_EMA_PERIOD * 3:
        return "NEUTRAL"

    highs, lows = swings(bars, HTF_SWING_WINDOW)

    if len(highs) < 2 or len(lows) < 2:
        return "NEUTRAL"

    close = bars[-1]["close"]
    average = ema(
        [bar["close"] for bar in bars],
        BIAS_EMA_PERIOD,
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
        protected_low = lows[-1]
        broken = any(
            bar["close"] < protected_low["price"]
            for bar in bars[protected_low["index"] + 1:]
        )
        if not broken:
            return "BUY"

    if bearish and close < average:
        protected_high = highs[-1]
        broken = any(
            bar["close"] > protected_high["price"]
            for bar in bars[protected_high["index"] + 1:]
        )
        if not broken:
            return "SELL"

    return "NEUTRAL"


def create_zones(bars, side):
    """
    Return historical zone candidates without deleting their first touch.

    Freshness and invalidation are evaluated later on the M5 timeline.
    Zone creation time is the close of the final required M15 candle.
    """
    volatility = atr(bars)
    result = []
    first = max(DEALING_RANGE_BARS, len(bars) - ZONE_LOOKBACK)

    for index in range(first, len(bars)):
        bar = bars[index]
        prior = bars[:index]
        previous_atr = volatility[index - 1]

        if previous_atr <= 0:
            continue

        dealing = bars[index - DEALING_RANGE_BARS:index]
        range_high = max(item["high"] for item in dealing)
        range_low = min(item["low"] for item in dealing)
        equilibrium = (range_high + range_low) / 2.0

        def add_zone(kind, bottom, top, source_index):
            if bottom >= top:
                return

            midpoint = (bottom + top) / 2.0

            if side == "BUY" and midpoint > equilibrium:
                return

            if side == "SELL" and midpoint < equilibrium:
                return

            result.append({
                "kind": kind,
                "side": side,
                "bottom": float(bottom),
                "top": float(top),
                "created": int(bar["time"] + 900),
                "source_time": int(bars[source_index]["time"]),
            })

        highs, lows = swings(prior, STRUCTURE_SWING_WINDOW)
        body = abs(bar["close"] - bar["open"])

        if body >= DISPLACEMENT_ATR * previous_atr:
            if (
                side == "BUY"
                and highs
                and bar["close"] > bar["open"]
            ):
                pivot = highs[-1]

                first_break = (
                    bar["close"] > pivot["price"]
                    and all(
                        item["close"] <= pivot["price"]
                        for item in bars[
                            pivot["confirmed_index"] + 1:index
                        ]
                    )
                )

                if first_break:
                    for source in range(
                        index - 1,
                        max(-1, index - OB_SEARCH_BACK - 1),
                        -1,
                    ):
                        source_bar = bars[source]

                        if source_bar["close"] < source_bar["open"]:
                            add_zone(
                                "OB",
                                source_bar["low"],
                                source_bar["high"],
                                source,
                            )
                            break

            if (
                side == "SELL"
                and lows
                and bar["close"] < bar["open"]
            ):
                pivot = lows[-1]

                first_break = (
                    bar["close"] < pivot["price"]
                    and all(
                        item["close"] >= pivot["price"]
                        for item in bars[
                            pivot["confirmed_index"] + 1:index
                        ]
                    )
                )

                if first_break:
                    for source in range(
                        index - 1,
                        max(-1, index - OB_SEARCH_BACK - 1),
                        -1,
                    ):
                        source_bar = bars[source]

                        if source_bar["close"] > source_bar["open"]:
                            add_zone(
                                "OB",
                                source_bar["low"],
                                source_bar["high"],
                                source,
                            )
                            break

        if index >= 2:
            first_bar = bars[index - 2]
            middle_bar = bars[index - 1]

            if side == "BUY":
                gap = bar["low"] - first_bar["high"]

                if (
                    gap >= MIN_FVG_ATR * previous_atr
                    and middle_bar["close"] > middle_bar["open"]
                ):
                    add_zone(
                        "FVG",
                        first_bar["high"],
                        bar["low"],
                        index - 1,
                    )

            else:
                gap = first_bar["low"] - bar["high"]

                if (
                    gap >= MIN_FVG_ATR * previous_atr
                    and middle_bar["close"] < middle_bar["open"]
                ):
                    add_zone(
                        "FVG",
                        bar["high"],
                        first_bar["low"],
                        index - 1,
                    )

    unique = {}

    for zone in result:
        key = (
            f"{side}:{zone['kind']}:"
            f"{zone['source_time']}:{zone['created']}"
        )
        unique[key] = dict(zone, local_id=key)

    return sorted(
        unique.values(),
        key=lambda zone: zone["created"],
        reverse=True,
    )


def zone_signal(zone, bars, volatility):
    """
    Require:
    1. A first touch after zone creation.
    2. A sweep on the touch bar or up to three bars later.
    3. A later close through a confirmed pre-sweep opposing swing.

    A sweep and confirmation on the same candle are not accepted.
    """
    if bars[0]["time"] > zone["created"]:
        return None

    side = zone["side"]
    buy = side == "BUY"

    eligible = [
        index
        for index, bar in enumerate(bars)
        if bar["time"] >= zone["created"]
    ]

    if not eligible:
        return None

    touch = None

    for index in eligible:
        bar = bars[index]

        reached = (
            bar["low"] <= zone["top"]
            if buy
            else bar["high"] >= zone["bottom"]
        )

        if reached:
            touch = index
            break

    if touch is None or touch < 40:
        return None

    latest = len(bars) - 1

    if latest - touch > (
        SWEEP_AFTER_TOUCH_BARS + CONFIRM_AFTER_SWEEP_BARS
    ):
        return None

    for bar in bars[touch:]:
        if buy and bar["close"] < zone["bottom"]:
            return None
        if not buy and bar["close"] > zone["top"]:
            return None

    sweep_end = min(
        touch + SWEEP_AFTER_TOUCH_BARS,
        latest - 1,
    )

    for sweep_index in range(touch, sweep_end + 1):
        before = bars[:sweep_index]
        highs, lows = swings(before, LTF_SWING_WINDOW)

        if len(highs) < 2 or len(lows) < 2:
            continue

        sweep_bar = bars[sweep_index]
        previous_atr = volatility[sweep_index - 1]

        if previous_atr <= 0:
            continue

        overlaps = (
            sweep_bar["low"] <= zone["top"]
            and sweep_bar["high"] >= zone["bottom"]
        )

        if not overlaps:
            continue

        if buy:
            counter_structure = (
                highs[-1]["price"] < highs[-2]["price"]
                and lows[-1]["price"] < lows[-2]["price"]
            )
            liquidity = lows[-1]
            confirmation = highs[-1]
        else:
            counter_structure = (
                highs[-1]["price"] > highs[-2]["price"]
                and lows[-1]["price"] > lows[-2]["price"]
            )
            liquidity = highs[-1]
            confirmation = lows[-1]

        if not counter_structure:
            continue

        if sweep_index - liquidity["index"] > LIQUIDITY_MAX_AGE:
            continue

        after_liquidity = bars[
            liquidity["confirmed_index"] + 1:sweep_index
        ]

        already_taken = any(
            item["low"] <= liquidity["price"]
            if buy
            else item["high"] >= liquidity["price"]
            for item in after_liquidity
        )

        if already_taken:
            continue

        if buy:
            swept = (
                sweep_bar["low"]
                < liquidity["price"] - SWEEP_MIN_ATR * previous_atr
                and sweep_bar["close"] > liquidity["price"]
            )
        else:
            swept = (
                sweep_bar["high"]
                > liquidity["price"] + SWEEP_MIN_ATR * previous_atr
                and sweep_bar["close"] < liquidity["price"]
            )

        if not swept:
            continue

        level = confirmation["price"]

        broken_before_confirmation = any(
            item["close"] > level
            if buy
            else item["close"] < level
            for item in bars[
                confirmation["confirmed_index"] + 1:sweep_index + 1
            ]
        )

        if broken_before_confirmation:
            continue

        confirmation_end = min(
            sweep_index + CONFIRM_AFTER_SWEEP_BARS,
            latest,
        )

        for confirmation_index in range(
            sweep_index + 1,
            confirmation_end + 1,
        ):
            confirmed = (
                bars[confirmation_index]["close"] > level
                if buy
                else bars[confirmation_index]["close"] < level
            )

            if not confirmed:
                continue

            if confirmation_index != latest:
                break

            path = bars[touch:confirmation_index + 1]

            extreme = (
                min(item["low"] for item in path)
                if buy
                else max(item["high"] for item in path)
            )

            return {
                "zone": zone,
                "side": side,
                "extreme": extreme,
                "atr": volatility[-1],
                "signal_close": bars[-1]["close"],
                "signal_time": int(bars[-1]["time"] + 300),
                "sweep_time": int(sweep_bar["time"] + 300),
                "confirmation_level": level,
            }

    return None


def scan_symbol(broker, symbol, state):
    m5 = broker.bars(
        symbol,
        broker.constant("TIMEFRAME_M5"),
        M5_BAR_COUNT,
    )

    decision_time = int(m5[-1]["time"] + 300)
    age = time.time() - decision_time

    if age < 0 or age > MAX_CLOSED_M5_AGE_SECONDS:
        return None

    h4 = broker.bars(
        symbol,
        broker.constant("TIMEFRAME_H4"),
        H4_BAR_COUNT,
    )

    m15 = broker.bars(
        symbol,
        broker.constant("TIMEFRAME_M15"),
        M15_BAR_COUNT,
    )

    h4 = [
        bar for bar in h4
        if bar["time"] + 14400 <= decision_time
    ]

    m15 = [
        bar for bar in m15
        if bar["time"] + 900 <= decision_time
    ]

    side = htf_bias(h4)

    if side == "NEUTRAL" or len(m15) < DEALING_RANGE_BARS + 10:
        return None

    volatility = atr(m5)

    for zone in create_zones(m15, side):
        zone_id = f"{symbol}:{zone['local_id']}"

        if zone_id in state["seen_zones"]:
            continue

        candidate = zone_signal(zone, m5, volatility)

        if candidate:
            candidate["symbol"] = symbol
            candidate["zone_id"] = zone_id
            return candidate

    return None


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

    return round(
        units * increment,
        int(info["digits"]),
    )


def spread_points(tick, info):
    point = float(info["point"])

    if point <= 0:
        raise SafetyError("The symbol point size is invalid.")

    return (float(tick["ask"]) - float(tick["bid"])) / point


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
            points,
            float(info.get("trade_freeze_level", 0)),
        )

    tick_size = float(
        info.get("trade_tick_size") or info["point"]
    )

    return points * float(info["point"]) + tick_size


def build_order(broker, candidate, account):
    symbol = candidate["symbol"]
    buy = candidate["side"] == "BUY"

    info = broker.info(symbol)
    tick = broker.tick(symbol)

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
        MAX_SPREAD_BY_SYMBOL.get(symbol, MAX_SPREAD_POINTS)
    )

    if spread_points(tick, info) > spread_limit:
        return None

    entry = float(tick["ask"] if buy else tick["bid"])
    volatility = float(candidate["atr"])

    if volatility <= 0:
        return None

    if (
        abs(entry - candidate["signal_close"])
        > MAX_ENTRY_DRIFT_ATR * volatility
    ):
        return None

    zone = candidate["zone"]
    buffer = SL_BUFFER_ATR * volatility
    gap = stop_gap(info)

    if buy:
        raw_sl = min(
            zone["bottom"],
            candidate["extreme"],
        ) - buffer

        raw_sl = min(raw_sl, float(tick["bid"]) - gap)
        stop = price_round(raw_sl, info)
        risk_distance = entry - stop
        order_type = broker.constant("ORDER_TYPE_BUY")

    else:
        spread_price = float(tick["ask"]) - float(tick["bid"])

        raw_sl = max(
            zone["top"],
            candidate["extreme"],
        ) + buffer + spread_price

        raw_sl = max(raw_sl, float(tick["ask"]) + gap)
        stop = price_round(raw_sl, info, upward=True)
        risk_distance = stop - entry
        order_type = broker.constant("ORDER_TYPE_SELL")

    if (
        stop <= 0
        or risk_distance <= 0
        or risk_distance > MAX_SL_ATR * volatility
    ):
        return None

    if buy:
        target = price_round(
            max(
                entry + TP2_RR * risk_distance,
                float(tick["bid"]) + gap,
            ),
            info,
            upward=True,
        )
    else:
        target = price_round(
            min(
                entry - TP2_RR * risk_distance,
                float(tick["ask"]) - gap,
            ),
            info,
        )

    if target <= 0:
        return None

    point = float(info["point"])
    allowance = RISK_SLIPPAGE_POINTS * point

    worse_entry = entry + allowance if buy else entry - allowance
    worse_stop = stop - allowance if buy else stop + allowance

    if min(worse_entry, worse_stop) <= 0:
        return None

    step = float(info["volume_step"])
    minimum = float(info["volume_min"])
    maximum = min(float(info["volume_max"]), MAX_LOT)

    if step <= 0 or minimum <= 0 or maximum < minimum:
        return None

    reference_loss = abs(
        broker.profit(
            order_type,
            symbol,
            minimum,
            worse_entry,
            worse_stop,
        )
    ) + COMMISSION_PER_LOT * minimum

    if reference_loss <= 0:
        return None

    estimated_per_lot = reference_loss / minimum

    volume = floor_volume(
        min(RISK_PER_TRADE / estimated_per_lot, maximum),
        step,
    )

    if volume + EPSILON < minimum:
        return None

    estimated_loss = abs(
        broker.profit(
            order_type,
            symbol,
            volume,
            worse_entry,
            worse_stop,
        )
    ) + COMMISSION_PER_LOT * volume

    if estimated_loss > RISK_PER_TRADE + EPSILON:
        return None

    margin = broker.margin(order_type, symbol, volume, entry)

    if margin > float(account["margin_free"]) * 0.90:
        return None

    request = {
        "action": broker.constant("TRADE_ACTION_DEAL"),
        "symbol": symbol,
        "volume": volume,
        "type": order_type,
        "price": entry,
        "sl": stop,
        "tp": target,
        "deviation": DEVIATION_POINTS,
        "magic": MAGIC,
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

    codes = {broker.constant(name) for name in names}
    return int(result["retcode"]) in codes


def mark_halt(state, path, reason):
    if not state["halt_reason"]:
        state["halt_reason"] = reason
        atomic_save(path, state)
        notify(f"Safety halt activated. Reason: {reason}")


def execute_request(broker, state, path, kind, request, metadata):
    if DRY_RUN:
        raise FatalError("A trading request was blocked by dry-run mode.")

    if state["pending"] is not None or state["halt_reason"]:
        raise SafetyError("Another execution requires reconciliation.")

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

    active_orders = broker.orders()

    order_still_active = any(
        str(order.get("comment", "")) == request["comment"]
        or (
            int(result.get("order", 0)) > 0
            and int(order["ticket"]) == int(result["order"])
        )
        for order in active_orders
    )

    if order_still_active:
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
        buy = int(position["type"]) == broker.constant("POSITION_TYPE_BUY")
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
            state["pending"] = None
            atomic_save(path, state)
            notify(f"Stop update confirmed. Symbol: {position['symbol']}")
            return

    else:
        if kind == "ENTRY":
            deals = broker.history(
                pending["created"] - 120,
                time.time() + 1,
            )
            wanted_entry = broker.constant("DEAL_ENTRY_IN")

            matches = [
                deal for deal in deals
                if trade_deal(broker, deal)
                and int(deal.get("magic", 0)) == MAGIC
                and int(deal.get("entry", -1)) == wanted_entry
                and deal_matches(deal, pending)
            ]

        elif kind == "PARTIAL":
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

        else:
            raise FatalError("The execution journal contains an unknown action.")

        if matches:
            if kind == "ENTRY":
                identifiers = {
                    str(int(deal["position_id"]))
                    for deal in matches
                }

                if len(identifiers) != 1:
                    mark_halt(
                        state,
                        path,
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
                    int(deal.get("magic", 0)) != MAGIC
                    for deal in entries
                ):
                    mark_halt(
                        state,
                        path,
                        "Position ownership could not be verified.",
                    )
                    return

                volume = sum(float(deal["volume"]) for deal in entries)

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
                        state,
                        path,
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
                ) + COMMISSION_PER_LOT * volume

                if actual_risk > RISK_PER_TRADE + EPSILON:
                    mark_halt(
                        state,
                        path,
                        "Confirmed entry risk exceeds the configured budget.",
                    )

            else:
                pid = pending["metadata"]["position_id"]
                record = state["positions"].get(pid)

                if record is not None:
                    record["partial_done"] = True

                state["pending"] = None
                atomic_save(path, state)

                closed_volume = sum(
                    float(deal["volume"]) for deal in matches
                )

                notify(
                    f"Partial close confirmed. Symbol: {request['symbol']}\n"
                    f"Closed volume: {closed_volume:.8f}"
                )

            return

    if rejection_is_definite(broker, pending.get("result")):
        code = int(pending["result"]["retcode"])
        state["pending"] = None
        atomic_save(path, state)
        notify(f"Execution rejected. Action: {kind}. Code: {code}")
        return

    status(
        "pending_execution",
        "Execution remains unresolved. Automatic trading requests are blocked.",
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
                int(position.get("magic", 0)) == MAGIC
                and pid not in state["positions"]
            ):
                mark_halt(
                    state,
                    path,
                    "An untracked bot position requires manual reconciliation.",
                )

    pending_pid = None

    if state["pending"] is not None:
        pending_pid = state["pending"]["metadata"].get("position_id")

    closed = []

    for pid, record in list(state["positions"].items()):
        if pid in live:
            position = live[pid]

            if int(position.get("magic", 0)) != MAGIC:
                mark_halt(
                    state,
                    path,
                    "Tracked position ownership changed.",
                )

            buy = record["side"] == "BUY"
            actual_buy = (
                int(position["type"])
                == broker.constant("POSITION_TYPE_BUY")
            )

            if actual_buy != buy:
                mark_halt(
                    state,
                    path,
                    "Tracked position direction changed.",
                )

            if float(position["sl"]) <= 0:
                mark_halt(
                    state,
                    path,
                    "A tracked position has no server-side stop loss.",
                )

            if (
                float(position["volume"])
                > record["initial_volume"] + EPSILON
            ):
                mark_halt(
                    state,
                    path,
                    "Tracked position volume increased unexpectedly.",
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
                state,
                path,
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

        if abs(entered - exited) > EPSILON:
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

    for closed_time, _, pid, record, pnl, last_exit in sorted(closed):
        if pnl < -EPSILON:
            state["consecutive_losses"] += 1

            if state["consecutive_losses"] >= MAX_CONSECUTIVE_LOSSES:
                state["cooldown_until"] = max(
                    float(state["cooldown_until"]),
                    closed_time + COOLDOWN_SECONDS,
                )

                state["consecutive_losses"] = 0

        elif pnl > EPSILON:
            state["consecutive_losses"] = 0

        state["positions"].pop(pid, None)
        atomic_save(path, state)

        reason = close_reason(
            broker,
            last_exit.get("reason", -1),
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
        and abs(deal_cash(deal)) > EPSILON
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
        and daily_pnl <= -baseline * MAX_DAILY_LOSS_PCT
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
        and int(deal.get("magic", 0)) == MAGIC
        and int(deal.get("entry", -1))
        in {
            broker.constant("DEAL_ENTRY_IN"),
            broker.constant("DEAL_ENTRY_INOUT"),
        }
    }

    for pid, record in state["positions"].items():
        if float(record["opened_at"]) >= start:
            entry_ids.add(pid)

    return {
        "account": account,
        "daily_entries": len(entry_ids),
        "daily_pnl": daily_pnl,
        "external_cash_flow": external_cash_flow,
    }


def in_session():
    hour = utc_now().hour

    if SESSION_START_HOUR_UTC < SESSION_END_HOUR_UTC:
        return SESSION_START_HOUR_UTC <= hour < SESSION_END_HOUR_UTC

    return (
        hour >= SESSION_START_HOUR_UTC
        or hour < SESSION_END_HOUR_UTC
    )


def entry_permission(broker, state, snapshot, positions, orders):
    if state["halt_reason"]:
        return False, "A persistent safety halt is active."

    if state["pending"] is not None:
        return False, "An execution requires reconciliation."

    if state["daily_loss_latched"]:
        return False, "The daily loss guard is active."

    if snapshot["external_cash_flow"]:
        return False, "Account cash-flow activity prevents safe daily accounting."

    if time.time() < float(state["cooldown_until"]):
        return False, "The consecutive-loss cooldown is active."

    if snapshot["daily_entries"] >= MAX_DAILY_TRADES:
        return False, "The daily entry limit has been reached."

    bot_positions = [
        position for position in positions
        if int(position.get("magic", 0)) == MAGIC
    ]

    if len(bot_positions) >= MAX_OPEN_POSITIONS:
        return False, "The open-position limit has been reached."

    if any(int(order.get("magic", 0)) == MAGIC for order in orders):
        return False, "A bot order is still active."

    if not in_session():
        return False, "The entry session is closed."

    account = snapshot["account"]

    if not account.get("trade_allowed", False):
        return False, "Account trading permission is unavailable."

    if not account.get("trade_expert", False):
        return False, "Automated trading permission is unavailable."

    terminal = broker.terminal()

    if not terminal.get("connected", False):
        return False, "The terminal is disconnected."

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
        "deviation": DEVIATION_POINTS,
        "magic": MAGIC,
        "type_time": broker.constant("ORDER_TIME_GTC"),
        "type_filling": choose_filling(broker, info),
    }


def stop_request(broker, position_id, desired_sl):
    positions = broker.positions()

    position = next(
        (
            item for item in positions
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
        desired_sl,
        info,
        upward=not buy,
    )

    if desired_sl <= 0:
        return None

    if buy:
        if current_sl > 0 and desired_sl <= current_sl + increment / 2:
            return None

        if desired_sl > float(tick["bid"]) - gap:
            return None

        if current_sl > 0 and float(tick["bid"]) - current_sl < gap:
            return None

    else:
        if current_sl > 0 and desired_sl >= current_sl - increment / 2:
            return None

        if desired_sl < float(tick["ask"]) + gap:
            return None

        if current_sl > 0 and current_sl - float(tick["ask"]) < gap:
            return None

    return {
        "action": broker.constant("TRADE_ACTION_SLTP"),
        "symbol": position["symbol"],
        "position": int(position["ticket"]),
        "sl": desired_sl,
        "tp": float(position["tp"]),
        "magic": MAGIC,
    }


def manage_positions(broker, state, path):
    if DRY_RUN or state["pending"] is not None or state["halt_reason"]:
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
                state,
                path,
                "A tracked position has invalid initial risk.",
            )
            return

        price = float(tick["bid"] if buy else tick["ask"])

        target1 = (
            entry + TP1_RR * initial_risk
            if buy
            else entry - TP1_RR * initial_risk
        )

        reached = price >= target1 if buy else price <= target1

        if (
            volume < float(record["initial_volume"]) - EPSILON
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
                closing + EPSILON >= minimum
                and remaining + EPSILON >= minimum
            ):
                request = partial_request(
                    broker,
                    position,
                    closing,
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
            symbol,
            broker.constant("TIMEFRAME_M5"),
            100,
        )

        volatility = atr(bars)[-1]

        if volatility <= 0:
            continue

        if buy:
            desired_sl = max(
                entry,
                price - TRAIL_ATR * volatility,
            )
        else:
            desired_sl = min(
                entry,
                price + TRAIL_ATR * volatility,
            )

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

    return hashlib.sha256(raw.encode("utf-8")).hexdigest()[:16]


def validate_account(broker, expected_key):
    account = broker.account()

    if account_key_from(account) != expected_key:
        raise FatalError("The connected account changed. Trading has stopped.")

    if (
        int(account["margin_mode"])
        != broker.constant("ACCOUNT_MARGIN_MODE_RETAIL_HEDGING")
    ):
        raise FatalError("This build supports hedging accounts only.")

    return account


def main():
    validate_config()

    broker = None

    try:
        broker = Broker()
        account = broker.account()
        account_key = account_key_from(account)

        acquire_lock(account_key)
        validate_account(broker, account_key)
        broker.select_symbols()

        mode = "dry" if DRY_RUN else "live"
        state_path = ROOT / (
            f"bot_state_v2_{account_key}_{MAGIC}_{mode}.json"
        )

        state = load_state(state_path, account_key)
        atomic_save(state_path, state)

        notify(
            "SMC bot started.\n"
            f"Mode: {'DRY_RUN' if DRY_RUN else 'LIVE'}\n"
            "Account type: Hedging\n"
            "Time basis: UTC"
        )

        if COMMISSION_PER_LOT == 0:
            log(
                "Commission reserve is zero. "
                "Configure actual broker costs before live use."
            )

        while True:
            try:
                validate_account(broker, account_key)

                resolve_pending(broker, state, state_path)
                reconcile_positions(broker, state, state_path)

                if state["pending"] is not None:
                    time.sleep(POLL_SECONDS)
                    continue

                if state["halt_reason"]:
                    status(
                        "persistent_halt",
                        "A persistent safety halt is active. "
                        "Server-side stops remain in place.",
                    )
                    time.sleep(POLL_SECONDS)
                    continue

                manage_positions(broker, state, state_path)

                if state["pending"] is not None:
                    time.sleep(POLL_SECONDS)
                    continue

                for symbol in SYMBOLS:
                    snapshot = daily_snapshot(
                        broker,
                        state,
                        state_path,
                    )

                    positions = broker.positions()
                    orders = broker.orders()

                    permitted, reason = entry_permission(
                        broker,
                        state,
                        snapshot,
                        positions,
                        orders,
                    )

                    if not permitted:
                        status("entry_gate", reason)
                        break

                    if any(
                        item["symbol"] == symbol
                        for item in positions + orders
                    ):
                        continue

                    candidate = scan_symbol(
                        broker,
                        symbol,
                        state,
                    )

                    if candidate is None:
                        continue

                    built = build_order(
                        broker,
                        candidate,
                        snapshot["account"],
                    )

                    if built is None:
                        continue

                    request, estimated_loss = built

                    fresh_snapshot = daily_snapshot(
                        broker,
                        state,
                        state_path,
                    )

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

                    state["seen_zones"][candidate["zone_id"]] = time.time()
                    atomic_save(state_path, state)

                    if DRY_RUN:
                        notify(
                            f"Dry-run signal. Symbol: {symbol}\n"
                            f"Side: {candidate['side']}\n"
                            f"Zone: {candidate['zone']['kind']}\n"
                            f"Entry estimate: {request['price']:.8f}\n"
                            f"Stop: {request['sl']:.8f}\n"
                            f"Target: {request['tp']:.8f}\n"
                            f"Volume: {request['volume']:.8f}\n"
                            f"Estimated risk: {estimated_loss:.2f} "
                            "account currency\n"
                            "No trading request was sent."
                        )
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
                        },
                    )

                    break

                time.sleep(POLL_SECONDS)

            except FatalError:
                raise

            except KeyboardInterrupt:
                raise

            except SafetyError:
                status(
                    "safety_cycle_error",
                    "A safety or data check failed. "
                    "This cycle will not submit further requests.",
                    interval=60,
                )
                time.sleep(POLL_SECONDS)

            except Exception:
                status(
                    "unexpected_cycle_error",
                    "An unexpected runtime error occurred. "
                    "This cycle was stopped without exposing external details.",
                    interval=60,
                )
                time.sleep(POLL_SECONDS)

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