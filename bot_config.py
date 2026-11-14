"""
Central configuration module for SMC/Price Action Trading Bot & NewsFetcher.
"""

import math
import os
import json
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parent
load_dotenv(ROOT / ".env")


def env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    value = (str(default) if raw is None or not raw.strip() else raw).strip().lower()
    if value not in {"true", "false", "1", "0", "yes", "no"}:
        raise ValueError(f"Invalid Boolean configuration: {name}")
    return value in {"true", "1", "yes"}


def env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    text = str(default) if raw is None or not raw.strip() else raw.strip()
    try:
        value = float(text)
    except ValueError as exc:
        raise ValueError(f"Invalid float configuration: {name}") from exc
    if not math.isfinite(value):
        raise ValueError(f"Non-finite configuration: {name}")
    return value


def env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    text = str(default) if raw is None or not raw.strip() else raw.strip()
    try:
        return int(text)
    except ValueError as exc:
        raise ValueError(f"Invalid integer configuration: {name}") from exc


def env_json_dict(name: str, default: dict) -> dict:
    raw = os.getenv(name)
    if raw is None or not raw.strip():
        return dict(default)
    try:
        parsed = json.loads(raw)
    except ValueError as exc:
        raise ValueError(f"Invalid JSON configuration: {name}") from exc
    if not isinstance(parsed, dict):
        raise ValueError(f"JSON object configuration required: {name}")
    return parsed


# ==========================================
# 1. GENERAL & EXECUTION CONTROL
# ==========================================
DRY_RUN = env_bool("DRY_RUN", False)
ALLOW_LIVE_TRADING = env_bool("ALLOW_LIVE_TRADING", True)
MAGIC = env_int("MAGIC", 990011)
STATE_VERSION = 2
POLL_SECONDS = env_float("POLL_SECONDS", 15)
EPSILON = 1e-8

SYMBOLS = [
    item.strip()
    for item in os.getenv(
        "SYMBOLS",
        "EURUSD,GBPUSD,USDJPY,AUDUSD,USDCAD,NZDUSD,USDCHF,"
        "EURGBP,EURJPY,GBPJPY,AUDJPY,EURAUD,GBPCAD,"
        "XAUUSD,USOIL,"
        "US30,USTEC,US500,DE30,"
        "BTCUSD,ETHUSD"
    ).split(",")
    if item.strip()
]

# ==========================================
# 2. MT5 CONNECTION SETTINGS
# ==========================================
MT5_HOST = os.getenv("MT5_HOST", "localhost").strip()
MT5_PORT = env_int("MT5_PORT", 8002)
MT5_LOGIN = env_int("MT5_LOGIN", 0)
MT5_PASSWORD = os.getenv("MT5_PASSWORD", "")
MT5_SERVER = os.getenv("MT5_SERVER", "")

# ==========================================
# 3. TELEGRAM NOTIFICATION SETTINGS
# ==========================================
TELEGRAM_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN", "").strip()
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID", "").strip()

# ==========================================
# 4. RISK & ACCOUNT MANAGEMENT
# ==========================================
RISK_PER_TRADE = env_float("RISK_PER_TRADE", 10.0)
USE_DYNAMIC_RISK = env_bool("USE_DYNAMIC_RISK", True)
RISK_PCT = env_float("RISK_PCT", 1.0)
MIN_LOT_FALLBACK_MAX_RISK_PCT = env_float("MIN_LOT_FALLBACK_MAX_RISK_PCT", 1.5)
MAX_OPEN_PORTFOLIO_HEAT_PCT = env_float("MAX_OPEN_PORTFOLIO_HEAT_PCT", 2.5)
DRAWDOWN_THROTTLE_SLOPE = env_float("DRAWDOWN_THROTTLE_SLOPE", 5.0)
COMMISSION_PER_LOT = env_float("COMMISSION_PER_LOT", 7.0)
MAX_LOT = env_float("MAX_LOT", 2.0)
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 2)
MAX_POSITIONS_PER_CURRENCY = env_int("MAX_POSITIONS_PER_CURRENCY", 1)
MAX_RISK_GROUP_EXPOSURE = env_int("MAX_RISK_GROUP_EXPOSURE", 1)
BREAKEVEN_TRIGGER_RR = env_float("BREAKEVEN_TRIGGER_RR", 1.0)
STAGE1_REDUCE_RISK_RR = env_float("STAGE1_REDUCE_RISK_RR", -0.35)
BREAKEVEN_LOCK_RR = env_float("BREAKEVEN_LOCK_RR", 0.10)
MAX_DAILY_TRADES = env_int("MAX_DAILY_TRADES", 6)
USE_DYNAMIC_DAILY_TRADES = env_bool("USE_DYNAMIC_DAILY_TRADES", True)
USE_DYNAMIC_TP_SCALING = env_bool("USE_DYNAMIC_TP_SCALING", True)
USE_DYNAMIC_TIME_EXIT = env_bool("USE_DYNAMIC_TIME_EXIT", True)
USE_AUTO_DST_KILLZONES = env_bool("USE_AUTO_DST_KILLZONES", True)
MAX_DAILY_LOSS_PCT = env_float("MAX_DAILY_LOSS_PCT", 0.03)
MAX_CONSECUTIVE_LOSSES = env_int("MAX_CONSECUTIVE_LOSSES", 3)
COOLDOWN_SECONDS = env_float("COOLDOWN_SECONDS", 3600)

MAX_SPREAD_POINTS = env_float("MAX_SPREAD_POINTS", 25)
USE_ATR_NORMALIZED_SPREAD = env_bool("USE_ATR_NORMALIZED_SPREAD", True)
MAX_SPREAD_ATR_RATIO = env_float("MAX_SPREAD_ATR_RATIO", 0.18)
MAX_SPREAD_SL_RATIO = env_float("MAX_SPREAD_SL_RATIO", 0.12)
MAX_SPREAD_BY_SYMBOL = env_json_dict(
    "MAX_SPREAD_BY_SYMBOL_JSON",
    {
        "EURUSD": 20,
        "USDJPY": 22,
        "GBPUSD": 25,
        "AUDUSD": 25,
        "USDCAD": 25,
        "NZDUSD": 25,
        "USDCHF": 25,
        "EURGBP": 25,
        "EURJPY": 30,
        "AUDJPY": 30,
        "GBPJPY": 35,
        "EURAUD": 40,
        "GBPCAD": 40,
        "XAUUSD": 60,
        "USOIL": 50,
        "US30": 300,
        "USTEC": 250,
        "US500": 120,
        "DE30": 180,
        "BTCUSD": 3000,
        "ETHUSD": 1200,
    },
)

DEVIATION_POINTS = env_int("DEVIATION_POINTS", 20)
RISK_SLIPPAGE_POINTS = env_int("RISK_SLIPPAGE_POINTS", 20)
MAX_TICK_AGE_SECONDS = env_float("MAX_TICK_AGE_SECONDS", 60)
MAX_CLOSED_M5_AGE_SECONDS = env_float("MAX_CLOSED_M5_AGE_SECONDS", 540)
MAX_CONSECUTIVE_RUNTIME_ERRORS = env_int("MAX_CONSECUTIVE_RUNTIME_ERRORS", 5)

# ==========================================
# 5. SESSION & TIME EXIT SETTINGS
# ==========================================
SESSION_START_HOUR_UTC = env_int("SESSION_START_HOUR_UTC", 6)
SESSION_END_HOUR_UTC = env_int("SESSION_END_HOUR_UTC", 21)

ENABLE_TIME_EXIT = env_bool("ENABLE_TIME_EXIT", True)
TIME_EXIT_MINUTES = env_float("TIME_EXIT_MINUTES", 90)
TIME_EXIT_MIN_R = env_float("TIME_EXIT_MIN_R", 0.25)

ENABLE_DAILY_TELEGRAM_SUMMARY = env_bool("ENABLE_DAILY_TELEGRAM_SUMMARY", True)
ENABLE_MARKET_PULSE_RADAR = env_bool("ENABLE_MARKET_PULSE_RADAR", True)
MARKET_PULSE_MIN_INTERVAL_SEC = env_float("MARKET_PULSE_MIN_INTERVAL_SEC", 60)
ENABLE_FRIDAY_WEEKEND_EXIT = env_bool("ENABLE_FRIDAY_WEEKEND_EXIT", True)
FRIDAY_EXIT_HOUR_UTC = env_int("FRIDAY_EXIT_HOUR_UTC", 20)
FRIDAY_EXIT_MINUTE_UTC = env_int("FRIDAY_EXIT_MINUTE_UTC", 30)
MONDAY_OPEN_COOLDOWN_MINUTES = env_int("MONDAY_OPEN_COOLDOWN_MINUTES", 30)
PENDING_STALE_RECONCILE_SECONDS = env_float("PENDING_STALE_RECONCILE_SECONDS", 90)

# ==========================================
# 6. SMC / PRICE ACTION STRATEGY PARAMETERS
# ==========================================
BIAS_EMA_PERIOD = env_int("BIAS_EMA_PERIOD", 200)
H4_BAR_COUNT = env_int("H4_BAR_COUNT", 650)
M15_BAR_COUNT = env_int("M15_BAR_COUNT", 240)
M5_BAR_COUNT = env_int("M5_BAR_COUNT", 650)

HTF_SWING_WINDOW = env_int("HTF_SWING_WINDOW", 5)
LTF_SWING_WINDOW = env_int("LTF_SWING_WINDOW", 2)
STRUCTURE_SWING_WINDOW = env_int("STRUCTURE_SWING_WINDOW", 2)

ZONE_LOOKBACK = env_int("ZONE_LOOKBACK", 50)
DEALING_RANGE_BARS = env_int("DEALING_RANGE_BARS", 50)
OB_SEARCH_BACK = env_int("OB_SEARCH_BACK", 4)

DISPLACEMENT_ATR = env_float("DISPLACEMENT_ATR", 1.0)
DISPLACEMENT_BODY_RATIO = env_float("DISPLACEMENT_BODY_RATIO", 0.55)
MIN_FVG_ATR = env_float("MIN_FVG_ATR", 0.10)
BOS_BUFFER_ATR = env_float("BOS_BUFFER_ATR", 0.05)

PD_EQUILIBRIUM_BUFFER = env_float("PD_EQUILIBRIUM_BUFFER", 0.02)

MAX_ZONE_AGE_M15_BARS = env_int("MAX_ZONE_AGE_M15_BARS", 32)
LIQUIDITY_MAX_AGE = env_int("LIQUIDITY_MAX_AGE", 30)
SWEEP_MIN_ATR = env_float("SWEEP_MIN_ATR", 0.15)
SWEEP_AFTER_TOUCH_BARS = env_int("SWEEP_AFTER_TOUCH_BARS", 6)
CONFIRM_AFTER_SWEEP_BARS = env_int("CONFIRM_AFTER_SWEEP_BARS", 6)
MAX_TOUCH_WAIT_BARS = env_int("MAX_TOUCH_WAIT_BARS", 12)

CONFIRM_CLOSE_BUFFER_ATR = env_float("CONFIRM_CLOSE_BUFFER_ATR", 0.05)
CONFIRM_MIN_BODY_ATR = env_float("CONFIRM_MIN_BODY_ATR", 0.10)

SL_BUFFER_ATR = env_float("SL_BUFFER_ATR", 0.55)
MIN_SMC_SL_ATR = env_float("MIN_SMC_SL_ATR", 1.30)
MIN_MOMENTUM_SL_ATR = env_float("MIN_MOMENTUM_SL_ATR", 1.10)
MAX_SL_ATR = env_float("MAX_SL_ATR", 5.0)
MAX_COUNTER_M15_IMPULSE_ATR = env_float("MAX_COUNTER_M15_IMPULSE_ATR", 1.20)
MAX_MOMENTUM_M15_EMA50_ATR = env_float("MAX_MOMENTUM_M15_EMA50_ATR", 3.5)
MAX_ENTRY_DRIFT_ATR = env_float("MAX_ENTRY_DRIFT_ATR", 1)

USE_STRUCTURAL_TP = env_bool("USE_STRUCTURAL_TP", True)
TP1_RR = env_float("TP1_RR", 1.5)
TP2_RR = env_float("TP2_RR", 2.5)
TRAIL_ATR = env_float("TRAIL_ATR", 1.2)

# ==========================================
# 7. NEWS FETCHER & FILTER SETTINGS
# ==========================================
ENABLE_NEWS_FILTER = env_bool("ENABLE_NEWS_FILTER", True)
NEWS_BEFORE_MINUTES = env_float("NEWS_BEFORE_MINUTES", 25)
NEWS_AFTER_MINUTES = env_float("NEWS_AFTER_MINUTES", 10)
NEWS_SUPER_HIGH_BEFORE_MINUTES = env_float("NEWS_SUPER_HIGH_BEFORE_MINUTES", 35)
NEWS_SUPER_HIGH_AFTER_MINUTES = env_float("NEWS_SUPER_HIGH_AFTER_MINUTES", 15)
NEWS_MEDIUM_BEFORE_MINUTES = env_float("NEWS_MEDIUM_BEFORE_MINUTES", 15)
NEWS_MEDIUM_AFTER_MINUTES = env_float("NEWS_MEDIUM_AFTER_MINUTES", 5)
NEWS_POST_SENTIMENT_MINUTES = env_float("NEWS_POST_SENTIMENT_MINUTES", 90)
NEWS_FULL_BOOST_MINUTES = env_float("NEWS_FULL_BOOST_MINUTES", 40)
NEWS_POST_RELEASE_REFRESH_SEC = env_float("NEWS_POST_RELEASE_REFRESH_SEC", 90)

NEWS_MIN_FETCH_INTERVAL_SEC = 300
NEWS_DEFAULT_BACKOFF_SEC = 60
NEWS_MAX_BACKOFF_SEC = 3600
NEWS_CACHE_MAX_AGE_SEC = 86400
NEWS_CACHE_DIR_NAME = ".mtf-bot-cache"
NEWS_CACHE_FILE_NAME = "news_calendar_cache.json"
NEWS_REQUEST_TIMEOUT_SEC = 10
NEWS_LOOKBACK_HOURS = 24
NEWS_IMPACTS_TRACKED = ["High", "Medium"]

NEWS_SOURCES = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
]

# ==========================================
# 8. VOLUME SETTINGS
# ==========================================

VOLUME_MA_PERIOD = env_int("VOLUME_MA_PERIOD", 20)
DISPLACEMENT_VOLUME_MULTIPLIER = env_float("DISPLACEMENT_VOLUME_MULTIPLIER", 1.5)
SWEEP_CONFIRM_VOLUME_MULTIPLIER = env_float("SWEEP_CONFIRM_VOLUME_MULTIPLIER", 1.2)

# ==========================================
# 9. SESSION & KILLZONE SETTINGS
# ==========================================

USE_KILLZONES_ONLY = env_bool("USE_KILLZONES_ONLY", False)
KILLZONES_UTC = [
    (7, 10),   # London Open / Expansion
    (12, 16),  # New York Open / Overlap
]
MAX_SWEEP_WICK_RATIO = env_float("MAX_SWEEP_WICK_RATIO", 0.45)
VOL_BUILDUP_LOOKBACK = env_int("VOL_BUILDUP_LOOKBACK", 3)

# ==========================================
# 10. PRO TRADING FEATURES
# ==========================================
MIN_SCORING_THRESHOLD = env_float("MIN_SCORING_THRESHOLD", 6.0)

# ==========================================
# 11. MITIGATION BLOCK & PRO FEATURES
# ==========================================
ENABLE_MITIGATION_BLOCK = env_bool("ENABLE_MITIGATION_BLOCK", True)
MITIGATION_SCORE_BOOST = env_float("MITIGATION_SCORE_BOOST", 2.0)

# ==========================================
# 12. HARMONIC BAT PATTERN CONFLUENCE
# ==========================================
ENABLE_HARMONIC_BAT = env_bool("ENABLE_HARMONIC_BAT", True)
HARMONIC_SCORE_BOOST = env_float("HARMONIC_SCORE_BOOST", 2.0)

# ==========================================
# 13. M15 SCORE BOOST SETTINGS
# ==========================================
ENABLE_M15_SCORE_BOOST = env_bool("ENABLE_M15_SCORE_BOOST", True)
M15_SCORE_BOOST_VALUE = env_float("M15_SCORE_BOOST_VALUE", 1.5)

# ==========================================
# 14. NEWS SCORING & SENTIMENT SETTINGS
# ==========================================
ENABLE_NEWS_SCORING_PENALTY = env_bool("ENABLE_NEWS_SCORING_PENALTY", True)
NEWS_HIGH_IMPACT_PENALTY = env_float("NEWS_HIGH_IMPACT_PENALTY", 4.0)
NEWS_MEDIUM_IMPACT_PENALTY = env_float("NEWS_MEDIUM_IMPACT_PENALTY", 2.0)
NEWS_SCORE_BOOST_HIGH = env_float("NEWS_SCORE_BOOST_HIGH", 3.0)
NEWS_SCORE_BOOST_MED = env_float("NEWS_SCORE_BOOST_MED", 1.5)

# ==========================================
# 15. ORDER FLOW & DOM SCORING SETTINGS
# ==========================================
ENABLE_ORDER_FLOW_SCORING = env_bool("ENABLE_ORDER_FLOW_SCORING", True)
ORDER_FLOW_SCORE_BOOST = env_float("ORDER_FLOW_SCORE_BOOST", 3.0)
TICK_VOLUME_SURGE_MULTIPLIER = env_float("TICK_VOLUME_SURGE_MULTIPLIER", 1.8)