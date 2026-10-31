from datetime import datetime, timedelta, timezone
import json
import logging
from pathlib import Path
import threading
import time
import requests
import bot_config as CFG

logging.Formatter.converter = time.gmtime
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s UTC | %(levelname)s | %(message)s",
)
log = logging.getLogger("NewsFetcher")

class NewsFetcher:
    def __init__(self, check_interval=1800, cache_dir=None):
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
            if self._stop_event.wait(self.check_interval):
                break
            self._safe_fetch()

    def _safe_fetch(self) -> None:
        try:
            self.fetch_calendar()
        except Exception as e:
            log.error("Unexpected fetch error: %s", e)

    def fetch_calendar(self) -> None:
        now = time.time()
        if now < self._next_allowed_ts or (now - self._last_fetch_ts < CFG.NEWS_MIN_FETCH_INTERVAL_SEC):
            return

        self._last_fetch_ts = now
        headers = {
            "User-Agent": "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
            "Accept": "application/json, text/plain, */*",
        }

        last_error = "all sources failed"
        for src in CFG.NEWS_SOURCES:
            log.info("Fetching calendar: %s", src)
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

                log.info("✓ Fetched %d High/Medium news from %s", len(parsed), src)
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
            try:
                news_time = datetime.strptime(time_str[:19], "%Y-%m-%dT%H:%M:%S")
            except (TypeError, ValueError):
                continue

            if news_time >= now - timedelta(hours=CFG.NEWS_LOOKBACK_HOURS):
                out.append({
                    "title": item.get("title"),
                    "currency": item.get("country"),
                    "time": news_time,
                    "impact": impact,
                })
        return out

    def _handle_failure(self, reason: str) -> None:
        self._consecutive_failures += 1
        if self._next_allowed_ts <= time.time():
            backoff_time = CFG.NEWS_DEFAULT_BACKOFF_SEC * (2 ** (self._consecutive_failures - 1))
            self._next_allowed_ts = time.time() + min(CFG.NEWS_MAX_BACKOFF_SEC, backoff_time)

    def is_restricted_time(self, symbol: str, buffer_minutes: int = 30) -> bool:
        now = datetime.now(timezone.utc).replace(tzinfo=None)
        import re as _re
        clean = _re.sub(r"[^A-Z]", "", symbol.upper())
        if len(clean) < 6:
            return False
        clean = clean[:6]

        currencies = {clean[:3], clean[3:6]}
        related = set()
        if "USD" in currencies:
            related.update(["EUR", "GBP", "JPY", "AUD", "NZD", "CAD", "CHF", "XAU", "XAG"])
        if "EUR" in currencies:
            related.update(["USD", "GBP", "CHF"])
        if "GBP" in currencies:
            related.update(["USD", "EUR"])
        if "JPY" in currencies:
            related.update(["USD"])
        currencies.update(related)

        with self._lock:
            for news in self.high_impact_news:
                if news["currency"] not in currencies:
                    continue
                    
                impact_buffer = CFG.NEWS_HIGH_IMPACT_BUFFER_MIN if news["impact"] == "High" else CFG.NEWS_MEDIUM_IMPACT_BUFFER_MIN
                effective_buffer = max(buffer_minutes, impact_buffer)
                
                diff = (news["time"] - now).total_seconds() / 60
                if -effective_buffer <= diff <= effective_buffer:
                    log.info(
                        "RESTRICTED | %s | [%s] %s (%s) @ %s UTC (Δ%.1fmin)",
                        symbol, news["impact"], news["title"], news["currency"],
                        news["time"].strftime("%H:%M"), diff,
                    )
                    return True
        return False