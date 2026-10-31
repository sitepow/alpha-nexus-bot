#!/usr/bin/env python3
"""
Tick-replay backtester for the accompanying bot.py.

READ-ONLY MT5 ACCESS:
- Downloads historical bars and Bid/Ask ticks.
- Never calls order_send(), bot.main(), or bot.execute_request().

Implemented:
- Symbols and signal settings imported from bot.py.
- Closed historical bars only.
- Confirmed swings via the original signal functions.
- Multi-symbol replay ordered by tick timestamp.
- Historical Bid/Ask spread.
- Configurable adverse slippage and entry latency.
- Market entries on the next eligible tick.
- Server-like SL / TP checks before strategy management.
- SL gaps filled at the available quote, plus adverse slippage.
- TP fills conservatively capped at the requested target.
- Partial close, ATR trailing, optional time exit.
- Daily entry / loss limits and consecutive-loss cooldown.
- Historical currency conversion from available replay quotes.
- Tick cache, exit CSV, equity CSV, summary JSON.

Not implemented:
- Broker liquidity / queue / rejection reconstruction.
- Historical margin rules, margin reservation, stop-out.
- Historical symbol-specification changes.
- Swap and historical commissions inferred automatically.
- The live bot's operator-maintained news calendar.
- Exact synchronization / durable journal behavior of the live bot.
- Exchange session validation.

Missing ticks:
- No synthetic prices or fills are created.
- Large gaps are reported.
- A gap alone is not classified as market closure.

Historical bars are broker bars. They are not rebuilt from ticks.
"""

import os

# Force safe import mode BEFORE importing bot.
os.environ["DRY_RUN"] = "true"
os.environ["ALLOW_LIVE_TRADING"] = "false"

import argparse
import bisect
import csv
import gzip
import heapq
import json
import math
from collections import deque
from datetime import datetime, timedelta, timezone
from pathlib import Path

import bot


UTC = timezone.utc
EPS = 1e-8
TF_SECONDS = {"H4": 14400, "M15": 900, "M5": 300}


def parse_time(text):
    dt = datetime.fromisoformat(text.replace("Z", "+00:00"))
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def iso(ms):
    return datetime.fromtimestamp(ms / 1000, UTC).isoformat()


def dt_expr(timestamp):
    return (
        "__import__('datetime').datetime.fromtimestamp("
        f"{float(timestamp)}, "
        "tz=__import__('datetime').timezone.utc)"
    )


def write_csv(path, rows, fields):
    with open(path, "w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


class HistoricalData:
    """Uses bot.Broker only for read-only historical access."""

    def __init__(self, broker, args):
        self.broker = broker
        self.args = args
        self.specs = {}
        self.bars = {}
        self.close_times = {}
        self.tick_counts = {symbol: 0 for symbol in bot.SYMBOLS}
        self.gaps = []
        self.previous_tick = {}

        account = broker.account()
        self.account_currency = str(account["currency"]).upper()

        if self.account_currency != args.currency.upper():
            raise RuntimeError(
                "Backtest currency must match the connected account "
                "currency for this build."
            )

        fingerprint = (
            str(account.get("server", ""))
            + ":"
            + str(account.get("login", ""))
        )
        import hashlib
        key = hashlib.sha256(
            fingerprint.encode("utf-8")
        ).hexdigest()[:12]

        self.cache = Path(args.cache) / key
        self.cache.mkdir(parents=True, exist_ok=True)

    def prepare(self):
        start = self.args.start.timestamp()
        end = self.args.end.timestamp()

        # Allow enough calendar time for EMA warm-up and market closures.
        warmup_days = max(
            260,
            math.ceil(bot.H4_BAR_COUNT * 4 / 24 * 2.5),
        )
        warmup = start - warmup_days * 86400

        constants = {
            "H4": self.broker.constant("TIMEFRAME_H4"),
            "M15": self.broker.constant("TIMEFRAME_M15"),
            "M5": self.broker.constant("TIMEFRAME_M5"),
        }

        for symbol in bot.SYMBOLS:
            if not self.broker.read(
                f"mt5.symbol_select({symbol!r}, True)"
            ):
                raise RuntimeError(f"Cannot select symbol: {symbol}")

            info = self.broker.info(symbol)

            for field in (
                "point",
                "trade_contract_size",
                "volume_min",
                "volume_max",
                "volume_step",
            ):
                value = float(info[field])
                if not math.isfinite(value) or value <= 0:
                    raise RuntimeError(
                        f"Invalid symbol specification: {symbol}/{field}"
                    )

            # This PnL model supports standard FX / CFD contract modes.
            # It intentionally refuses unsupported instruments.
            allowed_modes = set()
            for name in (
                "SYMBOL_CALC_MODE_FOREX",
                "SYMBOL_CALC_MODE_FOREX_NO_LEVERAGE",
                "SYMBOL_CALC_MODE_CFD",
                "SYMBOL_CALC_MODE_CFDLEVERAGE",
            ):
                try:
                    allowed_modes.add(self.broker.constant(name))
                except Exception:
                    pass

            if int(info["trade_calc_mode"]) not in allowed_modes:
                raise RuntimeError(
                    f"Unsupported profit calculation mode: {symbol}"
                )

            self.specs[symbol] = info
            self.bars[symbol] = {}
            self.close_times[symbol] = {}

            for name, timeframe in constants.items():
                required = {
                    "H4": bot.H4_BAR_COUNT,
                    "M15": bot.M15_BAR_COUNT,
                    "M5": bot.M5_BAR_COUNT,
                }[name]

                rows = self._load_bars_with_retry(
                    symbol,
                    name,
                    timeframe,
                    warmup,
                    end,
                    required,
                )

                if not rows:
                    raise RuntimeError(
                        f"No historical {name} bars: {symbol}"
                    )

                times = [row["time"] for row in rows]
                if len(times) != len(set(times)):
                    raise RuntimeError(
                        f"Duplicate historical bar timestamps: {symbol}"
                    )

                self.bars[symbol][name] = rows
                self.close_times[symbol][name] = [
                    row["time"] + TF_SECONDS[name]
                    for row in rows
                ]

            h4 = self.window(
                symbol, "H4", start, bot.H4_BAR_COUNT
            )
            m15 = self.window(
                symbol, "M15", start, bot.M15_BAR_COUNT
            )
            m5 = self.window(
                symbol, "M5", start, bot.M5_BAR_COUNT
            )

            if (
                len(h4) < bot.H4_BAR_COUNT
                or len(m15) < bot.M15_BAR_COUNT
                or len(m5) < bot.M5_BAR_COUNT
            ):
                raise RuntimeError(
                    f"Insufficient warm-up bars: {symbol}. "
                    "Load more MT5 history and check terminal bar limits."
                )

            print(f"Historical bars ready: {symbol}", flush=True)

    def _load_bars_with_retry(
        self,
        symbol,
        name,
        timeframe,
        warmup,
        end,
        required,
        attempts=8,
        delay_seconds=1.0,
    ):
        """Load bars via copy_rates_from_pos, filter to [warmup, end]."""
        import time as _time

        rows = []
        last_reported = 0
        tf_seconds = TF_SECONDS[name]

        span_seconds = max(0, end - warmup)
        needed = max(
            required + 500,
            int(span_seconds / tf_seconds * 2) + required,
        )

        for attempt in range(1, attempts + 1):
            expression = (
                "(lambda rows: None if rows is None else ["
                "{'time': int(r['time']), "
                "'open': float(r['open']), "
                "'high': float(r['high']), "
                "'low': float(r['low']), "
                "'close': float(r['close'])} "
                "for r in rows])"
                f"(mt5.copy_rates_from_pos({symbol!r}, "
                f"{timeframe}, 0, {needed}))"
            )

            try:
                fresh = self.broker.read(expression) or []
            except Exception as exc:
                print(f"  from_pos failed {symbol}/{name}: {exc}",
                      flush=True)
                fresh = []

            fresh = [
                r for r in fresh
                if warmup <= r["time"] <= end
            ]

            merged = {row["time"]: row for row in rows}
            for row in fresh:
                merged[row["time"]] = row
            rows = list(merged.values())
            rows.sort(key=lambda row: row["time"])

            if len(rows) >= required:
                print(
                    f"  loaded {symbol}/{name}: {len(rows)} bars",
                    flush=True,
                )
                return rows

            if len(rows) != last_reported:
                print(
                    f"  loading {symbol}/{name}: "
                    f"{len(rows)}/{required} bars "
                    f"(attempt {attempt}/{attempts}, "
                    f"requested {needed})",
                    flush=True,
                )
                last_reported = len(rows)

            if attempt < attempts:
                needed = int(needed * 1.5)
                _time.sleep(delay_seconds)

        return rows

    def window(self, symbol, name, decision_seconds, count):
        times = self.close_times[symbol][name]
        index = bisect.bisect_right(times, decision_seconds)
        rows = self.bars[symbol][name]
        return rows[max(0, index - count):index]

    def download_ticks(self, symbol, day):
        filename = self.cache / (
            f"{symbol}_{day.strftime('%Y%m%d')}.json.gz"
        )

        if filename.exists():
            with gzip.open(
                filename, "rt", encoding="utf-8"
            ) as handle:
                return json.load(handle)

        start = day.timestamp()
        end = (day + timedelta(days=1)).timestamp()

        # MT5 range endpoints can be inclusive. Replay filters end below.
        expression = (
            "(lambda rows: None if rows is None else ["
            "[int(r['time_msc']), float(r['bid']), float(r['ask'])] "
            "for r in rows])"
            "(mt5.copy_ticks_range("
            f"{symbol!r}, {dt_expr(start)}, {dt_expr(end)}, "
            "mt5.COPY_TICKS_ALL))"
        )

        rows = self.broker.read(expression)

        if rows is None:
            raise RuntimeError(
                f"Tick download failed: {symbol}/{day.date()}"
            )

        valid = []
        last_timestamp = None

        for timestamp, bid, ask in rows:
            if not (
                math.isfinite(bid)
                and math.isfinite(ask)
                and bid > 0
                and ask >= bid
            ):
                continue

            if not start * 1000 <= timestamp < end * 1000:
                continue

            if (
                last_timestamp is not None
                and timestamp < last_timestamp
            ):
                raise RuntimeError(
                    f"Historical ticks are unordered: {symbol}"
                )

            last_timestamp = timestamp
            valid.append([timestamp, bid, ask])

        temporary = filename.with_name(filename.name + ".tmp")

        with gzip.open(
            temporary, "wt", encoding="utf-8"
        ) as handle:
            json.dump(valid, handle)

        temporary.replace(filename)
        return valid

    def events(self):
        start_ms = int(self.args.start.timestamp() * 1000)
        end_ms = int(self.args.end.timestamp() * 1000)

        day = self.args.start.replace(
            hour=0, minute=0, second=0, microsecond=0
        )

        while day < self.args.end:
            streams = []

            for symbol in bot.SYMBOLS:
                rows = self.download_ticks(symbol, day)
                streams.append(
                    (
                        (row[0], symbol, i, row[1], row[2])
                        for i, row in enumerate(rows)
                        if start_ms <= row[0] < end_ms
                    )
                )

            for timestamp, symbol, _, bid, ask in heapq.merge(
                *streams
            ):
                previous = self.previous_tick.get(symbol)

                if (
                    previous is not None
                    and timestamp - previous
                    >= self.args.gap_report_minutes * 60_000
                ):
                    self.gaps.append({
                        "symbol": symbol,
                        "from": iso(previous),
                        "to": iso(timestamp),
                        "minutes": (
                            timestamp - previous
                        ) / 60_000,
                        "classification": "unknown",
                    })

                self.previous_tick[symbol] = timestamp
                self.tick_counts[symbol] += 1
                yield timestamp, symbol, bid, ask

            day += timedelta(days=1)


class Simulator:
    def __init__(self, data, args):
        self.data = data
        self.args = args
        self.balance = args.balance
        self.quotes = {}
        self.positions = {}
        self.pending = {}
        self.seen_zones = set()

        self.day = None
        self.day_start_balance = args.balance
        self.daily_entries = 0
        self.daily_latched = False
        self.consecutive_losses = 0
        self.cooldown_until = 0

        self.next_poll = {}
        self.next_manage = {}
        self.next_equity = 0

        self.exits = []
        self.equity_rows = []
        self.completed = []
        self.trade_counter = 0
        self.order_expired = 0
        self.order_rejected = 0

        self.peak_equity = args.balance
        self.max_drawdown_cash = 0.0
        self.max_drawdown_pct = 0.0

        self.conversion_failures = 0
        self.now = 0
        self.last_equity = args.balance

    def convert(self, amount, currency, now):
        """Conservative Bid/Ask conversion with stale-quote protection."""
        target = self.args.currency.upper()
        currency = currency.upper()

        if currency == target:
            return amount

        positive = amount >= 0
        graph = {}

        for symbol, quote in self.quotes.items():
            stamp, bid, ask = quote

            if (
                now - stamp
                > self.args.conversion_max_age_seconds * 1000
            ):
                continue

            info = self.data.specs[symbol]
            base = str(info.get("currency_base", "")).upper()
            profit = str(info.get("currency_profit", "")).upper()

            if not base or not profit:
                continue

            graph.setdefault(base, []).append(
                (profit, bid if positive else ask)
            )
            graph.setdefault(profit, []).append(
                (base, 1 / ask if positive else 1 / bid)
            )

        queue = deque([(currency, 1.0)])
        visited = {currency}

        while queue:
            current, rate = queue.popleft()

            for next_currency, multiplier in graph.get(
                current, []
            ):
                if next_currency in visited:
                    continue

                next_rate = rate * multiplier

                if next_currency == target:
                    return amount * next_rate

                visited.add(next_currency)
                queue.append((next_currency, next_rate))

        raise RuntimeError(
            f"No fresh historical conversion: {currency}->{target}"
        )

    def gross_pnl(self, symbol, side, entry, exit_price, volume, now):
        info = self.data.specs[symbol]
        direction = 1 if side == "BUY" else -1
        amount = (
            (exit_price - entry)
            * direction
            * float(info["trade_contract_size"])
            * volume
        )
        return self.convert(
            amount, str(info["currency_profit"]), now
        )

    def equity(self, now):
        result = self.balance

        for symbol, position in self.positions.items():
            quote = self.quotes.get(symbol)
            if quote is None:
                raise RuntimeError("Missing position mark.")

            _, bid, ask = quote
            price = bid if position["side"] == "BUY" else ask

            result += self.gross_pnl(
                symbol,
                position["side"],
                position["entry"],
                price,
                position["volume"],
                now,
            )

        return result

    def update_equity(self, now, force=False):
        equity = self.equity(now)
        self.last_equity = equity
        self.peak_equity = max(self.peak_equity, equity)

        drawdown = self.peak_equity - equity
        percentage = (
            drawdown / self.peak_equity
            if self.peak_equity > 0
            else 0.0
        )

        self.max_drawdown_cash = max(
            self.max_drawdown_cash, drawdown
        )
        self.max_drawdown_pct = max(
            self.max_drawdown_pct, percentage
        )

        if force or now >= self.next_equity:
            self.equity_rows.append({
                "time": iso(now),
                "balance": self.balance,
                "equity": equity,
                "drawdown": drawdown,
                "open_positions": len(self.positions),
            })
            self.next_equity = (
                now + self.args.equity_interval_seconds * 1000
            )

        baseline = self.day_start_balance

        if (
            baseline > 0
            and equity - baseline
            <= -baseline * bot.MAX_DAILY_LOSS_PCT
        ):
            self.daily_latched = True

        return equity

    def new_day(self, now):
        day = datetime.fromtimestamp(
            now / 1000, UTC
        ).date().isoformat()

        if day != self.day:
            self.day = day
            self.day_start_balance = self.balance
            self.daily_entries = 0
            self.daily_latched = False

    def in_session(self, now):
        hour = datetime.fromtimestamp(now / 1000, UTC).hour
        start = bot.SESSION_START_HOUR_UTC
        end = bot.SESSION_END_HOUR_UTC

        if start == end:
            return True
        if start < end:
            return start <= hour < end
        return hour >= start or hour < end

    def entry_allowed(self, now):
        return (
            not self.daily_latched
            and now >= self.cooldown_until
            and self.daily_entries < bot.MAX_DAILY_TRADES
            and len(self.positions) + len(self.pending)
            < bot.MAX_OPEN_POSITIONS
            and self.in_session(now)
            and self.balance > 0
        )

    def scan(self, symbol, now):
        if symbol in self.positions or symbol in self.pending:
            return

        if not self.entry_allowed(now):
            return

        seconds = now / 1000
        m5 = self.data.window(
            symbol, "M5", seconds, bot.M5_BAR_COUNT
        )

        if len(m5) < bot.M5_BAR_COUNT:
            return

        decision_time = m5[-1]["time"] + 300

        if not (
            0 <= seconds - decision_time
            <= bot.MAX_CLOSED_M5_AGE_SECONDS
        ):
            return

        h4 = self.data.window(
            symbol, "H4", decision_time, bot.H4_BAR_COUNT
        )
        m15 = self.data.window(
            symbol, "M15", decision_time, bot.M15_BAR_COUNT
        )

        side = bot.htf_bias(h4)
        if side == "NEUTRAL":
            return

        volatility = bot.atr(m5)

        for zone in bot.create_zones(m15, side):
            zone_id = f"{symbol}:{zone['local_id']}"

            if zone_id in self.seen_zones:
                continue

            candidate = bot.zone_signal(
                zone, m5, volatility
            )

            if candidate is None:
                continue

            if not bot.m15_confirmation(candidate, m15):
                continue

            candidate["symbol"] = symbol
            candidate["zone_id"] = zone_id

            plan = self.build_plan(candidate, now)

            if plan is None:
                continue

            self.seen_zones.add(zone_id)

            # Orders never fill on the tick that generated the plan.
            self.pending[symbol] = {
                "candidate": candidate,
                "plan": plan,
                "submitted": now,
                "due": now + self.args.latency_ms,
                "expires": now + self.args.order_expiry_seconds * 1000,
            }
            return

    def build_plan(self, candidate, now):
        symbol = candidate["symbol"]
        info = self.data.specs[symbol]
        _, bid, ask = self.quotes[symbol]
        point = float(info["point"])

        spread_limit = float(
            bot.MAX_SPREAD_BY_SYMBOL.get(
                symbol, bot.MAX_SPREAD_POINTS
            )
        )

        if (ask - bid) / point > spread_limit:
            return None

        buy = candidate["side"] == "BUY"
        entry = ask if buy else bid
        volatility = candidate["atr"]

        if volatility <= 0:
            return None

        if (
            abs(entry - candidate["signal_close"])
            > bot.MAX_ENTRY_DRIFT_ATR * volatility
        ):
            return None

        zone = candidate["zone"]
        buffer = bot.SL_BUFFER_ATR * volatility
        gap = bot.stop_gap(info)

        if buy:
            raw_sl = min(
                zone["bottom"], candidate["extreme"]
            ) - buffer
            stop = bot.price_round(
                min(raw_sl, bid - gap), info
            )
            distance = entry - stop
        else:
            raw_sl = (
                max(zone["top"], candidate["extreme"])
                + buffer
                + ask - bid
            )
            stop = bot.price_round(
                max(raw_sl, ask + gap),
                info,
                upward=True,
            )
            distance = stop - entry

        if (
            stop <= 0
            or distance <= 0
            or distance > bot.MAX_SL_ATR * volatility
        ):
            return None

        if buy:
            target = bot.price_round(
                max(
                    entry + bot.TP2_RR * distance,
                    bid + gap,
                ),
                info,
                upward=True,
            )
        else:
            target = bot.price_round(
                min(
                    entry - bot.TP2_RR * distance,
                    ask - gap,
                ),
                info,
            )

        if target <= 0:
            return None

        # Include an adverse fill reserve in sizing.
        reserve = max(
            bot.RISK_SLIPPAGE_POINTS,
            self.args.slippage_points,
        ) * point

        worse_entry = entry + reserve if buy else entry - reserve
        worse_stop = stop - reserve if buy else stop + reserve

        minimum = float(info["volume_min"])
        step = float(info["volume_step"])
        maximum = min(
            float(info["volume_max"]), bot.MAX_LOT
        )

        try:
            loss = abs(self.gross_pnl(
                symbol,
                candidate["side"],
                worse_entry,
                worse_stop,
                minimum,
                now,
            ))
        except RuntimeError:
            self.conversion_failures += 1
            return None

        # CLI commission is per lot PER SIDE.
        loss += 2 * self.args.commission_per_lot_side * minimum

        if loss <= 0:
            return None

        volume = bot.floor_volume(
            min(
                bot.RISK_PER_TRADE / (loss / minimum),
                maximum,
            ),
            step,
        )

        if volume + EPS < minimum:
            return None

        return {
            "side": candidate["side"],
            "sl": stop,
            "tp": target,
            "volume": volume,
        }

    def try_fill(self, symbol, now, bid, ask):
        pending = self.pending.get(symbol)
        if pending is None:
            return

        if now > pending["expires"]:
            self.pending.pop(symbol)
            self.order_expired += 1
            return

        if (
            now <= pending["submitted"]
            or now < pending["due"]
        ):
            return

        self.pending.pop(symbol)

        if (
            self.daily_latched
            or now < self.cooldown_until
            or self.daily_entries >= bot.MAX_DAILY_TRADES
            or len(self.positions) >= bot.MAX_OPEN_POSITIONS
            or not self.in_session(now)
        ):
            self.order_rejected += 1
            return

        plan = pending["plan"]
        info = self.data.specs[symbol]
        buy = plan["side"] == "BUY"
        slippage = (
            self.args.slippage_points * float(info["point"])
        )

        entry = ask + slippage if buy else bid - slippage
        risk = (
            entry - plan["sl"]
            if buy
            else plan["sl"] - entry
        )

        gap = bot.stop_gap(info)

        geometry_ok = (
            plan["sl"] <= bid - gap
            and plan["tp"] >= bid + gap
            if buy
            else plan["sl"] >= ask + gap
            and plan["tp"] <= ask - gap
        )

        if entry <= 0 or risk <= 0 or not geometry_ok:
            self.order_rejected += 1
            return

        volume = plan["volume"]
        commission = (
            self.args.commission_per_lot_side * volume
        )

        self.balance -= commission
        self.trade_counter += 1
        self.daily_entries += 1

        self.positions[symbol] = {
            "id": self.trade_counter,
            "symbol": symbol,
            "side": plan["side"],
            "opened": now,
            "entry": entry,
            "sl": plan["sl"],
            "tp": plan["tp"],
            "initial_risk": risk,
            "initial_volume": volume,
            "volume": volume,
            "partial_done": False,
            "net_pnl": -commission,
            "zone_id": pending["candidate"]["zone_id"],
        }

    def close(self, symbol, volume, price, now, reason):
        position = self.positions[symbol]
        volume = min(volume, position["volume"])

        gross = self.gross_pnl(
            symbol,
            position["side"],
            position["entry"],
            price,
            volume,
            now,
        )
        commission = (
            self.args.commission_per_lot_side * volume
        )
        net = gross - commission

        self.balance += net
        position["net_pnl"] += net
        position["volume"] = max(
            0.0, position["volume"] - volume
        )

        self.exits.append({
            "trade_id": position["id"],
            "symbol": symbol,
            "side": position["side"],
            "entry_time": iso(position["opened"]),
            "exit_time": iso(now),
            "entry": position["entry"],
            "exit": price,
            "closed_volume": volume,
            "gross_pnl": gross,
            "exit_commission": commission,
            "net_exit_pnl": net,
            "reason": reason,
            "balance": self.balance,
        })

        if position["volume"] <= EPS:
            total = position["net_pnl"]
            self.completed.append({
                "trade_id": position["id"],
                "symbol": symbol,
                "net_pnl": total,
                "entry_time": iso(position["opened"]),
                "exit_time": iso(now),
                "zone_id": position["zone_id"],
            })
            self.positions.pop(symbol)

            if total < -EPS:
                self.consecutive_losses += 1

                if (
                    self.consecutive_losses
                    >= bot.MAX_CONSECUTIVE_LOSSES
                ):
                    self.cooldown_until = (
                        now + bot.COOLDOWN_SECONDS * 1000
                    )
                    self.consecutive_losses = 0

            elif total > EPS:
                self.consecutive_losses = 0

    def protective_exits(self, symbol, now, bid, ask):
        position = self.positions.get(symbol)
        if position is None:
            return

        buy = position["side"] == "BUY"
        quote = bid if buy else ask
        slippage = (
            self.args.slippage_points
            * float(self.data.specs[symbol]["point"])
        )

        hit_sl = (
            quote <= position["sl"]
            if buy
            else quote >= position["sl"]
        )

        hit_tp = (
            quote >= position["tp"]
            if buy
            else quote <= position["tp"]
        )

        if hit_sl:
            # Includes gaps and spread widening.
            fill = quote - slippage if buy else quote + slippage
            self.close(
                symbol, position["volume"], fill, now, "SL"
            )

        elif hit_tp:
            # Conservative limit-style fill: no favorable gap improvement.
            self.close(
                symbol,
                position["volume"],
                position["tp"],
                now,
                "TP",
            )

    def manage(self, symbol, now, bid, ask):
        position = self.positions.get(symbol)
        if position is None:
            return

        info = self.data.specs[symbol]
        buy = position["side"] == "BUY"
        quote = bid if buy else ask
        risk = position["initial_risk"]
        current_r = (
            (quote - position["entry"]) / risk
            if buy
            else (position["entry"] - quote) / risk
        )

        slippage = (
            self.args.slippage_points * float(info["point"])
        )
        market_exit = (
            quote - slippage if buy else quote + slippage
        )

        if (
            bot.ENABLE_TIME_EXIT
            and now - position["opened"]
            >= bot.TIME_EXIT_MINUTES * 60_000
            and current_r < bot.TIME_EXIT_MIN_R
        ):
            self.close(
                symbol,
                position["volume"],
                market_exit,
                now,
                "TIME_EXIT",
            )
            return

        if (
            not position["partial_done"]
            and current_r >= bot.TP1_RR
        ):
            minimum = float(info["volume_min"])
            closing = bot.floor_volume(
                position["volume"] / 2,
                float(info["volume_step"]),
            )
            remaining = position["volume"] - closing

            position["partial_done"] = True

            if (
                closing + EPS >= minimum
                and remaining + EPS >= minimum
            ):
                self.close(
                    symbol,
                    closing,
                    market_exit,
                    now,
                    "PARTIAL",
                )

                # Match live bot behavior: stop management starts
                # on a subsequent management cycle.
                return

        if not position["partial_done"]:
            return

        m5 = self.data.window(
            symbol, "M5", now / 1000, 100
        )

        if len(m5) < 100:
            return

        age = now / 1000 - (m5[-1]["time"] + 300)
        if not 0 <= age <= bot.MAX_CLOSED_M5_AGE_SECONDS:
            return

        volatility = bot.atr(m5)[-1]
        gap = bot.stop_gap(info, include_freeze=True)
        tick_size = float(
            info.get("trade_tick_size") or info["point"]
        )

        if buy:
            desired = bot.price_round(
                max(
                    position["entry"],
                    quote - bot.TRAIL_ATR * volatility,
                ),
                info,
            )

            if (
                desired > position["sl"] + tick_size / 2
                and desired <= bid - gap
                and bid - position["sl"] >= gap
            ):
                position["sl"] = desired

        else:
            desired = bot.price_round(
                min(
                    position["entry"],
                    quote + bot.TRAIL_ATR * volatility,
                ),
                info,
                upward=True,
            )

            if (
                desired < position["sl"] - tick_size / 2
                and desired >= ask + gap
                and position["sl"] - ask >= gap
            ):
                position["sl"] = desired

    def run(self):
        for now, symbol, bid, ask in self.data.events():
            self.now = now
            self.new_day(now)
            self.quotes[symbol] = (now, bid, ask)

            # Protective orders have priority over polling-based logic.
            self.protective_exits(symbol, now, bid, ask)
            self.update_equity(now)

            self.try_fill(symbol, now, bid, ask)
            self.update_equity(now)

            if now >= self.next_manage.get(symbol, 0):
                self.next_manage[symbol] = (
                    now + bot.POLL_SECONDS * 1000
                )
                self.manage(symbol, now, bid, ask)
                self.update_equity(now)

            if now >= self.next_poll.get(symbol, 0):
                self.next_poll[symbol] = (
                    now + bot.POLL_SECONDS * 1000
                )
                self.scan(symbol, now)

        if self.now:
            # Do not invent a tradable quote at the requested end time.
            # Open positions remain marked at available historical quotes.
            self.update_equity(self.now, force=True)

    def save(self):
        output = Path(self.args.output)
        output.mkdir(parents=True, exist_ok=True)

        write_csv(
            output / "exits.csv",
            self.exits,
            [
                "trade_id", "symbol", "side",
                "entry_time", "exit_time",
                "entry", "exit", "closed_volume",
                "gross_pnl", "exit_commission",
                "net_exit_pnl", "reason", "balance",
            ],
        )

        write_csv(
            output / "trades.csv",
            self.completed,
            [
                "trade_id", "symbol", "net_pnl",
                "entry_time", "exit_time", "zone_id",
            ],
        )

        write_csv(
            output / "equity.csv",
            self.equity_rows,
            [
                "time", "balance", "equity",
                "drawdown", "open_positions",
            ],
        )

        write_csv(
            output / "tick_gaps.csv",
            self.data.gaps,
            [
                "symbol", "from", "to",
                "minutes", "classification",
            ],
        )

        profits = [
            trade["net_pnl"] for trade in self.completed
        ]
        wins = sum(value > EPS for value in profits)
        losses = sum(value < -EPS for value in profits)
        total_profit = sum(
            value for value in profits if value > 0
        )
        total_loss = -sum(
            value for value in profits if value < 0
        )

        summary = {
            "symbols": bot.SYMBOLS,
            "start": self.args.start.isoformat(),
            "end_exclusive": self.args.end.isoformat(),
            "account_currency": self.args.currency.upper(),
            "initial_balance": self.args.balance,
            "ending_balance": self.balance,
            "ending_equity": self.last_equity,
            "equity_return_pct": (
                self.last_equity / self.args.balance - 1
            ) * 100,
            "completed_trades": len(profits),
            "wins": wins,
            "losses": losses,
            "win_rate_pct": (
                wins / len(profits) * 100 if profits else 0
            ),
            "profit_factor": (
                total_profit / total_loss
                if total_loss > 0 else None
            ),
            "mean_completed_trade_pnl": (
                sum(profits) / len(profits)
                if profits else 0
            ),
            "max_drawdown_cash": self.max_drawdown_cash,
            "max_drawdown_pct": self.max_drawdown_pct * 100,
            "open_positions": list(self.positions.values()),
            "pending_entries_at_end": len(self.pending),
            "expired_entries": self.order_expired,
            "simulated_rejected_entries": self.order_rejected,
            "conversion_filter_skips": self.conversion_failures,
            "tick_counts": self.data.tick_counts,
            "reported_tick_gaps": len(self.data.gaps),
            "commission_per_lot_per_side": (
                self.args.commission_per_lot_side
            ),
            "slippage_points": self.args.slippage_points,
            "entry_latency_ms": self.args.latency_ms,
            "swap_included": False,
            "margin_and_stopout_simulated": False,
            "historical_symbol_specs": False,
            "news_filter_simulated": False,
            "notes": [
                "Profit factor is null if there are no gross losing trades.",
                "Trade PnL includes entry and exit commission.",
                "Open positions are not force-closed at the end.",
                "Large tick gaps are not automatically market closures.",
                "Symbol specifications are the current broker values.",
                "Conversion uses available historical Bid/Ask quotes.",
                "Trailing and discretionary exits use polling intervals.",
                "No real trading requests were submitted.",
            ],
        }

        with open(
            output / "summary.json", "w", encoding="utf-8"
        ) as handle:
            json.dump(
                summary,
                handle,
                indent=2,
                ensure_ascii=False,
                allow_nan=False,
            )

        with open(
            output / "symbol_specs.json", "w", encoding="utf-8"
        ) as handle:
            json.dump(
                self.data.specs,
                handle,
                indent=2,
                ensure_ascii=False,
            )

        print(json.dumps(summary, indent=2, ensure_ascii=False))
        print(f"\nReports written to: {output.resolve()}")


def arguments():
    parser = argparse.ArgumentParser(
        description="Read-only MT5 historical tick backtest."
    )

    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--balance", type=float, default=1000)
    parser.add_argument("--currency", default="USD")

    parser.add_argument(
        "--commission-per-lot-side",
        type=float,
        default=0,
        help="Account currency per lot per side, not round-trip.",
    )
    parser.add_argument(
        "--slippage-points", type=float, default=0
    )
    parser.add_argument(
        "--latency-ms", type=int, default=200
    )
    parser.add_argument(
        "--order-expiry-seconds", type=float, default=30
    )
    parser.add_argument(
        "--conversion-max-age-seconds",
        type=float,
        default=300,
    )
    parser.add_argument(
        "--equity-interval-seconds", type=float, default=60
    )
    parser.add_argument(
        "--gap-report-minutes", type=float, default=15
    )

    parser.add_argument("--cache", default="backtest_cache")
    parser.add_argument("--output", default="backtest_output")

    args = parser.parse_args()
    args.start = parse_time(args.start)
    args.end = parse_time(args.end)

    if args.end <= args.start:
        parser.error("--end must be after --start.")

    if not math.isfinite(args.balance) or args.balance <= 0:
        parser.error("--balance must be positive.")

    if args.latency_ms < 0:
        parser.error("--latency-ms must be nonnegative.")

    for field in (
        "commission_per_lot_side",
        "slippage_points",
    ):
        value = getattr(args, field)
        if not math.isfinite(value) or value < 0:
            parser.error(f"--{field.replace('_', '-')} is invalid.")

    for field in (
        "order_expiry_seconds",
        "conversion_max_age_seconds",
        "equity_interval_seconds",
        "gap_report_minutes",
    ):
        value = getattr(args, field)
        if not math.isfinite(value) or value <= 0:
            parser.error(f"--{field.replace('_', '-')} must be positive.")

    if bot.ENABLE_NEWS_FILTER:
        parser.error(
            "This backtester does not replay historical news. "
            "Use ENABLE_NEWS_FILTER=false for this build."
        )

    return args


def main():
    args = arguments()
    bot.validate_config()

    broker = None

    try:
        broker = bot.Broker()
        data = HistoricalData(broker, args)

        print("READ-ONLY backtest. No order_send calls.")
        print("Symbols:", ", ".join(bot.SYMBOLS))
        print("Preparing historical data...", flush=True)

        data.prepare()

        simulator = Simulator(data, args)
        simulator.run()
        simulator.save()

        empty = [
            symbol
            for symbol, count in data.tick_counts.items()
            if count == 0
        ]

        if empty:
            print(
                "\nWARNING: No valid ticks for:",
                ", ".join(empty),
            )

        if args.commission_per_lot_side == 0:
            print(
                "\nWARNING: Commission is zero. "
                "Results may overstate net performance."
            )

        print(
            "\nIMPORTANT: Swap, margin and stop-out are not simulated."
        )

    finally:
        if broker is not None:
            broker.shutdown()


if __name__ == "__main__":
    main()