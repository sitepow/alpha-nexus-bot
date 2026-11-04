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
    value = os.getenv(name, str(default)).strip().lower()
    if value not in {"true", "false", "1", "0", "yes", "no"}:
        raise ValueError(f"Invalid Boolean configuration: {name}")
    return value in {"true", "1", "yes"}


def env_float(name: str, default: float) -> float:
    value = float(os.getenv(name, str(default)))
    if not math.isfinite(value):
        raise ValueError(f"Non-finite configuration: {name}")
    return value


def env_int(name: str, default: int) -> int:
    return int(os.getenv(name, str(default)))


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
        "EURUSD,GBPUSD,USDJPY,AUDUSD,USDCAD,NZDUSD,XAUUSD,XAGUSD,USOIL,UKOIL,US30,USTEC,DE30,BTCUSD,ETHUSD,SOLUSD"
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
RISK_PER_TRADE = env_float("RISK_PER_TRADE", 15.0)
COMMISSION_PER_LOT = env_float("COMMISSION_PER_LOT", 0.0)
MAX_LOT = env_float("MAX_LOT", 5.0)
MAX_OPEN_POSITIONS = env_int("MAX_OPEN_POSITIONS", 3)
MAX_DAILY_TRADES = env_int("MAX_DAILY_TRADES", 4)
MAX_DAILY_LOSS_PCT = env_float("MAX_DAILY_LOSS_PCT", 0.05)
MAX_CONSECUTIVE_LOSSES = env_int("MAX_CONSECUTIVE_LOSSES", 3)
COOLDOWN_SECONDS = env_float("COOLDOWN_SECONDS", 3600)

MAX_SPREAD_POINTS = env_float("MAX_SPREAD_POINTS", 30)
MAX_SPREAD_BY_SYMBOL = json.loads(
    os.getenv(
        "MAX_SPREAD_BY_SYMBOL_JSON",
        '{"XAUUSD": 80, "US30": 100, "USTEC": 100, "USOIL": 60, "BTCUSD": 150}'
    )
)

DEVIATION_POINTS = env_int("DEVIATION_POINTS", 20)
RISK_SLIPPAGE_POINTS = env_int("RISK_SLIPPAGE_POINTS", 20)
MAX_TICK_AGE_SECONDS = env_float("MAX_TICK_AGE_SECONDS", 60)
MAX_CLOSED_M5_AGE_SECONDS = env_float("MAX_CLOSED_M5_AGE_SECONDS", 420)

# ==========================================
# 5. SESSION & TIME EXIT SETTINGS
# ==========================================
SESSION_START_HOUR_UTC = env_int("SESSION_START_HOUR_UTC", 6)
SESSION_END_HOUR_UTC = env_int("SESSION_END_HOUR_UTC", 20)

ENABLE_TIME_EXIT = env_bool("ENABLE_TIME_EXIT", True)
TIME_EXIT_MINUTES = env_float("TIME_EXIT_MINUTES", 90)
TIME_EXIT_MIN_R = env_float("TIME_EXIT_MIN_R", 0.25)

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
DISPLACEMENT_BODY_RATIO = env_float("DISPLACEMENT_BODY_RATIO", 0.60)
MIN_FVG_ATR = env_float("MIN_FVG_ATR", 0.10)
BOS_BUFFER_ATR = env_float("BOS_BUFFER_ATR", 0.05)

REQUIRE_OB_FVG_CONFLUENCE = env_bool("REQUIRE_OB_FVG_CONFLUENCE", True)
REQUIRE_FULL_ZONE_PD = env_bool("REQUIRE_FULL_ZONE_PD", True)
PD_EQUILIBRIUM_BUFFER = env_float("PD_EQUILIBRIUM_BUFFER", 0.02)

MAX_ZONE_AGE_M15_BARS = env_int("MAX_ZONE_AGE_M15_BARS", 32)
LIQUIDITY_MAX_AGE = env_int("LIQUIDITY_MAX_AGE", 30)
SWEEP_MIN_ATR = env_float("SWEEP_MIN_ATR", 0.25)
SWEEP_AFTER_TOUCH_BARS = env_int("SWEEP_AFTER_TOUCH_BARS", 3)
CONFIRM_AFTER_SWEEP_BARS = env_int("CONFIRM_AFTER_SWEEP_BARS", 6)
MAX_TOUCH_WAIT_BARS = env_int("MAX_TOUCH_WAIT_BARS", 9)

CONFIRM_CLOSE_BUFFER_ATR = env_float("CONFIRM_CLOSE_BUFFER_ATR", 0.05)
CONFIRM_MIN_BODY_ATR = env_float("CONFIRM_MIN_BODY_ATR", 0.10)
REQUIRE_M15_CONFIRMATION = env_bool("REQUIRE_M15_CONFIRMATION", True)

SL_BUFFER_ATR = env_float("SL_BUFFER_ATR", 0.30)
MAX_SL_ATR = env_float("MAX_SL_ATR", 3.0)
MAX_ENTRY_DRIFT_ATR = env_float("MAX_ENTRY_DRIFT_ATR", 0.50)

TP1_RR = env_float("TP1_RR", 1.5)
TP2_RR = env_float("TP2_RR", 2.5)
TRAIL_ATR = env_float("TRAIL_ATR", 1.2)

# ==========================================
# 7. NEWS FETCHER & FILTER SETTINGS
# ==========================================
ENABLE_NEWS_FILTER = env_bool("ENABLE_NEWS_FILTER", True)
NEWS_BEFORE_MINUTES = env_float("NEWS_BEFORE_MINUTES", 30)
NEWS_AFTER_MINUTES = env_float("NEWS_AFTER_MINUTES", 30)
NEWS_FILE_MAX_AGE_HOURS = env_float("NEWS_FILE_MAX_AGE_HOURS", 24)

NEWS_MIN_FETCH_INTERVAL_SEC = 300
NEWS_DEFAULT_BACKOFF_SEC = 60
NEWS_MAX_BACKOFF_SEC = 3600
NEWS_CACHE_MAX_AGE_SEC = 86400
NEWS_CACHE_DIR_NAME = ".mtf-bot-cache"
NEWS_CACHE_FILE_NAME = "news_calendar_cache.json"
NEWS_REQUEST_TIMEOUT_SEC = 10
NEWS_LOOKBACK_HOURS = 24
NEWS_IMPACTS_TRACKED = ["High", "Medium"]
NEWS_HIGH_IMPACT_BUFFER_MIN = int(NEWS_BEFORE_MINUTES)
NEWS_MEDIUM_IMPACT_BUFFER_MIN = 15

NEWS_SOURCES = [
    "https://nfs.faireconomy.media/ff_calendar_thisweek.json",
]

# ==========================================
# 8. VOLUME SETTINGS
# ==========================================

USE_VOLUME_FILTER = env_bool("USE_VOLUME_FILTER", True)
VOLUME_MA_PERIOD = env_int("VOLUME_MA_PERIOD", 20)
DISPLACEMENT_VOLUME_MULTIPLIER = env_float("DISPLACEMENT_VOLUME_MULTIPLIER", 1.5)
SWEEP_CONFIRM_VOLUME_MULTIPLIER = env_float("SWEEP_CONFIRM_VOLUME_MULTIPLIER", 1.2)

# ==========================================
# 9. SESSION & KILLZONE SETTINGS
# ==========================================

USE_KILLZONES_ONLY = env_bool("USE_KILLZONES_ONLY", True)
KILLZONES_UTC = [
    (7, 10),   # London Open / Expansion
    (12, 16),  # New York Open / Overlap
]
MAX_SWEEP_WICK_RATIO = env_float("MAX_SWEEP_WICK_RATIO", 0.70)
REQUIRE_CUMULATIVE_VOLUME_BUILDUP = env_bool("REQUIRE_CUMULATIVE_VOLUME_BUILDUP", True)
VOL_BUILDUP_LOOKBACK = env_int("VOL_BUILDUP_LOOKBACK", 3)

# ==========================================
# 10. PRO TRADING FEATURES
# ==========================================
SKIP_IDM_TRAP_ZONES = env_bool("SKIP_IDM_TRAP_ZONES", True)
MIN_SCORING_THRESHOLD = env_float("MIN_SCORING_THRESHOLD", 7.0)

# ==========================================
# 11. MITIGATION BLOCK & PRO FEATURES
# ==========================================
ENABLE_MITIGATION_BLOCK = env_bool("ENABLE_MITIGATION_BLOCK", True)
MITIGATION_SCORE_BOOST = env_float("MITIGATION_SCORE_BOOST", 2.0)