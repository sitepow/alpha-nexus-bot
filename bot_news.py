from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import re
import threading
import time
import requests
import bot_config as CFG

log = logging.getLogger("NewsFetcher")

INVERSE_ECONOMIC_KEYWORDS = (
    "unemployment",
    "jobless",
    "claims",
    "claimant",
    "layoffs",
)

SUPER_HIGH_IMPACT_KEYWORDS = (
    "non-farm",
    "nonfarm",
    "fomc",
    "fed interest rate",
    "federal funds rate",
    "powell",
    "cpi",
    "core pce",
    "ecb press conference",
    "interest rate decision",
    "official bank rate",
    "boj policy rate",
)


def is_super_high_impact(title: str) -> bool:
    if not title:
        return False
    lower_title = str(title).lower()
    return any(keyword in lower_title for keyword in SUPER_HIGH_IMPACT_KEYWORDS)


def parse_calendar_utc(time_str: str):
    if not time_str or not isinstance(time_str, str):
        return None
    text = time_str.strip()
    try:
        dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if dt.tzinfo is not None:
            return dt.astimezone(timezone.utc).replace(tzinfo=None)
        return dt
    except ValueError:
        pass
    try:
        return datetime.strptime(text[:19], "%Y-%m-%dT%H:%M:%S")
    except (TypeError, ValueError):
        return None

def parse_economic_number(raw_value):
    if raw_value is None:
        return None
    text = str(raw_value).strip().upper().replace(",", "")
    if not text:
        return None

    multiplier = 1.0
    if text.endswith("K"):
        multiplier = 1e3
        text = text[:-1]
    elif text.endswith("M"):
        multiplier = 1e6
        text = text[:-1]
    elif text.endswith("B"):
        multiplier = 1e9
        text = text[:-1]
    elif text.endswith("T"):
        multiplier = 1e12
        text = text[:-1]

    text = text.replace("%", "").replace("$", "").replace("€", "").replace("£", "").strip()
    match = re.search(r"[-+]?\d*\.?\d+", text)
    if not match or match.group(0) in {"", "-", "+", "."}:
        return None
    try:
        return float(match.group(0)) * multiplier
    except ValueError:
        return None


def is_inverse_indicator(title: str) -> bool:
    if not title:
        return False
    lower_title = str(title).lower()
    return any(keyword in lower_title for keyword in INVERSE_ECONOMIC_KEYWORDS)


class NewsFetcher:
    def __init__(self, check_interval=300, cache_dir=None):
        self.check_interval = max(check_interval, CFG.NEWS_MIN_FETCH_INTERVAL_SEC)
        self.high_impact_news = []
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread = None

        self._last_fetch_ts = 0.0
        self._last_success_ts = 0.0
        self._next_allowed_ts = 0.0
        self._consecutive_failures = 0

        base = Path(cache_dir) if cache_dir else Path.home() / CFG.NEWS_CACHE_DIR_NAME
        base.mkdir(parents=True, exist_ok=True)
        self._cache_file = base / CFG.NEWS_CACHE_FILE_NAME

        self._load_cache_from_disk()

    def _load_cache_from_disk(self) -> None:
        if not self._cache_file.exists():
            return
        try:
            raw = json.loads(self._cache_file.read_text(encoding="utf-8"))
            ts = float(raw.get("fetched_at", 0))
            age = time.time() - ts

            if age > CFG.NEWS_CACHE_MAX_AGE_SEC:
                log.info("Disk cache too old (%.1fh), ignored", age / 3600)
                return

            parsed = self._parse_items(raw.get("items", []))
            with self._lock:
                self.high_impact_news = parsed
            self._last_success_ts = ts
            log.info("Loaded %d news from disk cache (age=%.0fmin)", len(parsed), age / 60)
        except Exception as e:
            log.warning("Failed to load disk cache: %s", e)

    def _save_cache_to_disk(self, raw_items: list) -> None:
        try:
            payload = {"fetched_at": time.time(), "items": raw_items}
            tmp = self._cache_file.with_suffix(".tmp")
            tmp.write_text(json.dumps(payload), encoding="utf-8")
            tmp.replace(self._cache_file)
        except Exception as e:
            log.warning("Failed to write disk cache: %s", e)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = threading.Thread(target=self._update_loop, daemon=True)
        self._thread.start()
        log.info("NewsFetcher worker started (interval=%ds)", self.check_interval)

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=3)

    def _update_loop(self) -> None:
        cache_age = time.time() - self._last_success_ts if self._last_success_ts else None
        if cache_age is None or cache_age >= CFG.NEWS_MIN_FETCH_INTERVAL_SEC:
            self._safe_fetch()
        else:
            log.info("Skipping initial fetch — cache age=%.0fmin", cache_age / 60)

        while not self._stop_event.is_set():
            if self._stop_event.wait(30):
                break
            now_ts = time.time()
            if self._needs_post_release_refresh():
                post_refresh_sec = float(getattr(CFG, "NEWS_POST_RELEASE_REFRESH_SEC", 90.0))
                if now_ts - self._last_fetch_ts >= post_refresh_sec:
                    self._safe_fetch(force=True)
            elif now_ts - self._last_fetch_ts >= self.check_interval:
                self._safe_fetch(force=False)

    def _needs_post_release_refresh(self) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        with self._lock:
            for item in self.high_impact_news:
                nt = item.get("time")
                if not nt:
                    continue
                mins_since = (now - nt).total_seconds() / 60.0
                if 1.0 <= mins_since <= 20.0:
                    if parse_economic_number(item.get("actual")) is None:
                        return True
        return False

    def _safe_fetch(self, force: bool = False) -> None:
        try:
            self.fetch_calendar(force=force)
        except Exception as e:
            log.error("Unexpected fetch error: %s", e)

    def fetch_calendar(self, force: bool = False) -> None:
        now = time.time()
        min_interval = (
            float(getattr(CFG, "NEWS_POST_RELEASE_REFRESH_SEC", 90.0))
            if force
            else float(CFG.NEWS_MIN_FETCH_INTERVAL_SEC)
        )
        if now < self._next_allowed_ts or (now - self._last_fetch_ts < min_interval):
            return

        self._last_fetch_ts = now
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        last_error = "all sources failed"
        for src in CFG.NEWS_SOURCES:
            try:
                resp = requests.get(src, headers=headers, timeout=CFG.NEWS_REQUEST_TIMEOUT_SEC)
            except requests.RequestException as e:
                last_error = f"network: {e}"
                continue

            if resp.status_code == 200:
                try:
                    data = resp.json()
                except ValueError:
                    last_error = "invalid JSON"
                    continue

                parsed = self._parse_items(data)
                with self._lock:
                    self.high_impact_news = parsed

                self._last_success_ts = now
                self._consecutive_failures = 0
                self._next_allowed_ts = 0.0
                self._save_cache_to_disk(data)

                log.info("Fetched %d High/Medium news from %s", len(parsed), src)
                return

            if resp.status_code == 429:
                wait = self._parse_retry_after(resp.headers.get("Retry-After"))
                self._next_allowed_ts = time.time() + wait
                last_error = f"HTTP 429 (wait {wait}s)"
                continue

            last_error = f"HTTP {resp.status_code}"

        self._handle_failure(last_error)

    def _parse_retry_after(self, retry_after: str) -> int:
        wait = CFG.NEWS_DEFAULT_BACKOFF_SEC
        if retry_after:
            try:
                wait = int(retry_after) + 5
            except ValueError:
                pass
        return max(60, min(wait, CFG.NEWS_MAX_BACKOFF_SEC))

    def _parse_items(self, data: list) -> list:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        out = []
        for item in data:
            impact = item.get("impact")
            if impact not in CFG.NEWS_IMPACTS_TRACKED:
                continue

            time_str = item.get("date")
            news_time = parse_calendar_utc(time_str)
            if news_time is None:
                continue

            if news_time >= now - timedelta(hours=CFG.NEWS_LOOKBACK_HOURS):
                out.append({
                    "title": item.get("title") or "",
                    "currency": (item.get("country") or "").upper(),
                    "time": news_time,
                    "impact": impact,
                    "actual": item.get("actual"),
                    "forecast": item.get("forecast"),
                    "previous": item.get("previous"),
                })
        return out

    def _handle_failure(self, reason: str) -> None:
        self._consecutive_failures += 1
        if self._next_allowed_ts <= time.time():
            backoff_time = CFG.NEWS_DEFAULT_BACKOFF_SEC * (2 ** (self._consecutive_failures - 1))
            self._next_allowed_ts = time.time() + min(CFG.NEWS_MAX_BACKOFF_SEC, backoff_time)

    @staticmethod
    def map_symbol_currencies(symbol: str):
        clean = re.sub(r"[^A-Z0-9]", "", symbol.upper())
        base_cur = clean[:3] if len(clean) >= 3 else ""
        quote_cur = clean[3:6] if len(clean) >= 6 else "USD"

        if any(idx in clean for idx in ("US30", "DJ30", "USTEC", "NAS100", "US100", "US500", "SPX500", "US2000", "USOIL", "WTI", "XTI", "XNG")):
            base_cur, quote_cur = "IDX", "USD"
        elif any(idx in clean for idx in ("UK100", "FTSE100", "UKOIL", "BRENT", "XBR")):
            base_cur, quote_cur = "IDX", "GBP"
        elif any(idx in clean for idx in ("DE30", "DE40", "GER30", "GER40", "DAX", "FR40", "EU50", "STOXX50")):
            base_cur, quote_cur = "IDX", "EUR"
        elif any(idx in clean for idx in ("JP225", "JPN225", "NIKKEI")):
            base_cur, quote_cur = "IDX", "JPY"
        elif any(idx in clean for idx in ("AUS200", "AU200")):
            base_cur, quote_cur = "IDX", "AUD"
        elif any(idx in clean for idx in ("HK50", "HSI", "CN50", "CHINA50")):
            base_cur, quote_cur = "IDX", "CNY"

        direct = {c for c in (base_cur, quote_cur) if c and c != "IDX"}
        if base_cur in {"IDX", "XAU", "XAG", "XPT", "XPD", "BTC", "XBT", "ETH", "SOL", "XRP", "BNB", "ADA", "DOGE", "LINK", "DOT", "AVAX", "LTC", "BCH"}:
            direct.add("USD")
        direct.add("ALL")

        return base_cur, quote_cur, direct

    def is_restricted_time(self, symbol: str, buffer_minutes: int = 15) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        _, _, currencies = self.map_symbol_currencies(symbol)
        if not currencies:
            return False

        high_before = float(getattr(CFG, "NEWS_BEFORE_MINUTES", buffer_minutes))
        high_after = float(getattr(CFG, "NEWS_AFTER_MINUTES", 10.0))
        super_before = float(getattr(CFG, "NEWS_SUPER_HIGH_BEFORE_MINUTES", 35.0))
        super_after = float(getattr(CFG, "NEWS_SUPER_HIGH_AFTER_MINUTES", 15.0))
        med_before = float(getattr(CFG, "NEWS_MEDIUM_BEFORE_MINUTES", 10.0))
        med_after = float(getattr(CFG, "NEWS_MEDIUM_AFTER_MINUTES", 5.0))

        with self._lock:
            for news in self.high_impact_news:
                if news["currency"] not in currencies:
                    continue

                is_high = (news["impact"] == "High")
                is_super = is_high and is_super_high_impact(news.get("title", ""))
                if is_super:
                    before_window = max(float(buffer_minutes), high_before, super_before)
                    after_window = max(high_after, super_after)
                elif is_high:
                    before_window = max(float(buffer_minutes), high_before)
                    after_window = high_after
                else:
                    before_window = med_before
                    after_window = med_after

                minutes_until = (news["time"] - now).total_seconds() / 60.0
                if -after_window <= minutes_until <= before_window:
                    log.debug(
                        "RESTRICTED | %s | [%s] %s (%s) @ %s UTC (Δ%.1fmin)",
                        symbol, news["impact"], news["title"], news["currency"],
                        news["time"].strftime("%H:%M"), minutes_until,
                    )
                    return True
        return False
