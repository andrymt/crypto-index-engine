"""A Redis-backed, per-second cross-venue volume-weighted price index."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import os
import re
import signal
import time
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, localcontext

import redis.asyncio as redis
from redis.exceptions import RedisError, WatchError

LOG = logging.getLogger(__name__)
METHOD = "cross_venue_vwap_1s_v1"
TOKEN = re.compile(r"^[a-z][a-z0-9]{1,19}$")


def canonical(data: dict) -> str:
    return json.dumps(data, sort_keys=True, separators=(",", ":"), allow_nan=False)


@dataclass(frozen=True)
class Config:
    venues: tuple[str, ...] = ("coinbase", "kraken", "gemini")
    base: str = "xbt"
    quote: str = "usd"
    prefix: str = ""
    min_venues: int = 2
    decimal_places: int = 8
    publication_delay: int = 2
    correction_window: int = 30
    max_catchup: int = 300
    retries: int = 5

    def __post_init__(self):
        for value in (
            self.min_venues,
            self.decimal_places,
            self.publication_delay,
            self.correction_window,
            self.max_catchup,
            self.retries,
        ):
            if isinstance(value, bool) or not isinstance(value, int):
                raise ValueError("Configuration counts and intervals must be integers")
        for value in (*self.venues, self.base, self.quote):
            if not isinstance(value, str) or not TOKEN.fullmatch(value):
                raise ValueError("Venue and asset names must be lowercase alphanumeric tokens")
        if not 1 <= len(self.venues) <= 20 or len(set(self.venues)) != len(self.venues):
            raise ValueError("Configure 1 to 20 unique venues")
        if not 1 <= self.min_venues <= len(self.venues):
            raise ValueError("min_venues must fit the configured venue count")
        if not 0 <= self.decimal_places <= 18:
            raise ValueError("decimal_places must be between 0 and 18")
        if not 1 <= self.publication_delay <= 60:
            raise ValueError("publication_delay must be between 1 and 60 seconds")
        if not 1 <= self.correction_window <= self.max_catchup <= 3600:
            raise ValueError("Require 1 <= correction_window <= max_catchup <= 3600")
        if not 1 <= self.retries <= 20:
            raise ValueError("retries must be between 1 and 20")
        if not isinstance(self.prefix, str) or len(self.prefix) > 100:
            raise ValueError("prefix must be a string of at most 100 characters")

    @property
    def output_key(self) -> str:
        return f"{self.prefix}i{self.base}-{self.quote}"

    @property
    def meta_key(self) -> str:
        return self.output_key + ":meta"

    def input_key(self, venue: str) -> str:
        return f"{self.prefix}{venue}-{self.base}-{self.quote}-1"

    @property
    def fingerprint(self) -> str:
        # Timing/recovery tuning does not change the calculation methodology.
        methodology = {
            "method": METHOD,
            "venues": sorted(self.venues),
            "base": self.base,
            "quote": self.quote,
            "min_venues": self.min_venues,
            "decimal_places": self.decimal_places,
        }
        return hashlib.sha256(canonical(methodology).encode()).hexdigest()


def decimal_value(value: object, *, allow_zero: bool = False) -> Decimal:
    if isinstance(value, (float, bool)) or value is None:
        raise ValueError("Use decimal strings or JSON numbers, not binary floats")
    try:
        result = Decimal(str(value))
    except InvalidOperation as exc:
        raise ValueError("Invalid decimal") from exc
    if not result.is_finite() or result < 0 or (not allow_zero and result == 0):
        raise ValueError("Value must be finite and positive (volume may be zero)")
    if len(result.as_tuple().digits) > 30 or abs(result.as_tuple().exponent) > 18:
        raise ValueError("Decimal exceeds supported precision")
    return result


def validate_second(second: int) -> None:
    if isinstance(second, bool) or not isinstance(second, int) or second < 1:
        raise ValueError("Timestamp must be a positive integer Unix second")


def calculate(second: int, inputs: dict[str, str | None], config: Config) -> dict:
    """Pure calculation. One price is a venue VWAP for that SAME one-second bucket."""
    validate_second(second)
    included = {}
    excluded = {}
    with localcontext() as context:
        context.prec = 80
        for venue in config.venues:
            raw = inputs.get(venue)
            if raw is None:
                excluded[venue] = "missing"
                continue
            try:
                sample = json.loads(raw, parse_float=Decimal)
                if not isinstance(sample, dict):
                    raise ValueError("Sample must be an object")
                if "timestamp" in sample and (
                    isinstance(sample["timestamp"], bool)
                    or not isinstance(sample["timestamp"], int)
                    or sample["timestamp"] != second
                ):
                    excluded[venue] = "timestamp_mismatch"
                    continue
                price = decimal_value(sample["price"])
                volume = decimal_value(sample["volume"], allow_zero=True)
                if volume == 0:
                    excluded[venue] = "zero_volume"
                    continue
                included[venue] = price, volume
            except (ValueError, TypeError, KeyError, InvalidOperation):
                excluded[venue] = "invalid_sample"
        total = sum((v for _, v in included.values()), Decimal(0))
        enough = len(included) >= config.min_venues
        quantum = Decimal(1).scaleb(-config.decimal_places)
        value = None
        if enough:
            numerator = sum((p * v for p, v in included.values()), Decimal(0))
            value = format((numerator / total).quantize(quantum), "f")
        return {
            "methodology": METHOD,
            "timestamp": second,
            "symbol": f"{config.base}-{config.quote}".upper(),
            "interval_seconds": 1,
            "status": "ok" if enough else "insufficient_venues",
            "price": value,
            "total_volume": format(total, "f"),
            "venue_count": len(included),
            "min_venues": config.min_venues,
            "contributions": {
                venue: {
                    "price": format(p, "f"),
                    "volume": format(v, "f"),
                    "weight": format((v / total).quantize(Decimal("0.000000000001")), "f"),
                }
                for venue, (p, v) in included.items()
            },
            "excluded": excluded,
        }


class CatchupRequired(ValueError):
    """An outage exceeds the configured bounded automatic recovery range."""


class SnapshotConflict(RuntimeError):
    """Source or destination changed during all optimistic-transaction attempts."""


class Engine:
    def __init__(self, client, config: Config):
        self.client, self.config = client, config

    async def process(self, target: int, *, start: int | None = None) -> dict:
        """Atomic source validation, output revisions and checkpoint advancement.

        Explicit start recalculates a bounded historical range without advancing
        the live checkpoint. Repeating the same snapshot leaves tick JSON unchanged.
        """
        validate_second(target)
        if start is not None:
            validate_second(start)
            if not start <= target or target - start + 1 > self.config.max_catchup:
                raise ValueError("Recalculation range exceeds max_catchup")
        config = self.config
        keys = [config.input_key(v) for v in config.venues]
        for attempt in range(config.retries):
            try:
                async with self.client.pipeline() as pipe:
                    await pipe.watch(*keys, config.output_key, config.meta_key)
                    metadata = await pipe.hgetall(config.meta_key)
                    fingerprint = metadata.get("methodology_fingerprint")
                    if fingerprint and fingerprint != config.fingerprint:
                        raise ValueError("Output key already uses a different methodology")
                    previous = metadata.get("last_processed_second")
                    checkpoint = int(previous) if previous is not None else None
                    if start is not None:
                        begin = start
                    elif checkpoint is None:
                        begin = target
                    else:
                        if target < checkpoint:
                            raise ValueError("Clock is behind the saved checkpoint")
                        begin = min(checkpoint + 1, target - config.correction_window + 1)
                        begin = max(1, begin)
                        if target - begin + 1 > config.max_catchup:
                            raise CatchupRequired(
                                "Outage exceeds max_catchup; use explicit recovery"
                            )
                    seconds = list(range(begin, target + 1))
                    fields = [str(s) for s in seconds]
                    # WATCH applies before all reads. EXEC fails if any watched hash changed.
                    samples = [await pipe.hmget(key, fields) for key in keys]
                    existing = await pipe.hmget(config.output_key, fields)
                    changed = {}
                    statuses = {}
                    for offset, second in enumerate(seconds):
                        tick = calculate(
                            second,
                            {venue: samples[i][offset] for i, venue in enumerate(config.venues)},
                            config,
                        )
                        statuses[tick["status"]] = statuses.get(tick["status"], 0) + 1
                        old = json.loads(existing[offset]) if existing[offset] else None
                        comparable = (
                            {
                                k: v
                                for k, v in old.items()
                                if k not in ("revision", "calculated_at_ns")
                            }
                            if old
                            else None
                        )
                        if tick == comparable:
                            continue
                        tick["revision"] = int(old["revision"]) + 1 if old else 1
                        tick["calculated_at_ns"] = time.time_ns()
                        changed[str(second)] = canonical(tick)
                    pipe.multi()
                    if changed:
                        pipe.hset(config.output_key, mapping=changed)
                    meta = {"methodology_fingerprint": config.fingerprint}
                    if start is None:
                        meta["last_processed_second"] = str(target)
                    pipe.hset(config.meta_key, mapping=meta)
                    await pipe.execute()
                    return {
                        "target": target,
                        "start": begin,
                        "output_key": config.output_key,
                        "checked": len(seconds),
                        "written": len(changed),
                        "unchanged": len(seconds) - len(changed),
                        "statuses": statuses,
                    }
            except WatchError:
                if attempt + 1 < config.retries:
                    await asyncio.sleep(0.005 * (attempt + 1))
        raise SnapshotConflict("Redis hashes kept changing; no partial tick batch was committed")

    async def serve(self, stop: asyncio.Event, *, duration: float | None = None) -> None:
        deadline = time.monotonic() + duration if duration else None
        while not stop.is_set():
            target = int(time.time()) - self.config.publication_delay
            try:
                result = await self.process(target)
                print(canonical(result), flush=True)
            except (RedisError, SnapshotConflict) as exc:
                # Do not log Redis URLs or error messages that may embed credentials.
                LOG.warning(
                    "Tick not committed (%s); retrying on next boundary", type(exc).__name__
                )
            if deadline and time.monotonic() >= deadline:
                return
            delay = max(0.01, int(time.time()) + 1 - time.time())
            if deadline:
                delay = min(delay, max(0.001, deadline - time.monotonic()))
            try:
                async with asyncio.timeout(delay):
                    await stop.wait()
            except TimeoutError:
                pass


def parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["run", "tick", "recalculate", "demo"])
    p.add_argument("--config", help="Optional JSON configuration file")
    p.add_argument("--timestamp", type=int, help="Bucket timestamp for tick or demo")
    p.add_argument("--start", type=int, help="First bucket for explicit recalculation")
    p.add_argument("--end", type=int, help="Last bucket for explicit recalculation")
    p.add_argument("--duration", type=float, help="Run duration in seconds")
    return p


def read_config(path: str | None) -> Config:
    if path is None:
        return Config()
    with open(path, encoding="utf-8") as stream:
        values = json.load(stream)
    if "venues" in values:
        values["venues"] = tuple(values["venues"])
    return Config(**values)


async def run(args) -> None:
    config = read_config(args.config)
    if args.command == "demo":
        # Demo must not overwrite unprefixed application data.
        config = Config(prefix="demo:", correction_window=1)
    client = redis.from_url(
        os.environ.get("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True,
        protocol=2,
        socket_connect_timeout=3,
        socket_timeout=3,
    )
    try:
        await client.ping()
        engine = Engine(client, config)
        if args.command == "demo":
            second = args.timestamp or int(time.time()) - 2
            validate_second(second)
            async with client.pipeline(transaction=True) as pipe:
                for venue, price, volume in [
                    ("coinbase", "60000", "1"),
                    ("kraken", "60100", "2"),
                    ("gemini", "60200", "3"),
                ]:
                    pipe.hset(
                        config.input_key(venue),
                        str(second),
                        canonical({"price": price, "volume": volume}),
                    )
                await pipe.execute()
            result = await engine.process(second, start=second)
            print(
                canonical(
                    {
                        "demo": True,
                        "summary": result,
                        "tick": json.loads(await client.hget(config.output_key, str(second))),
                    }
                )
            )
        elif args.command == "tick":
            second = (
                args.timestamp
                if args.timestamp is not None
                else int(time.time()) - config.publication_delay
            )
            print(canonical(await engine.process(second)))
        elif args.command == "recalculate":
            if args.start is None or args.end is None:
                raise ValueError("recalculate requires --start and --end")
            print(canonical(await engine.process(args.end, start=args.start)))
        else:
            stop = asyncio.Event()
            for sig in (signal.SIGINT, signal.SIGTERM):
                try:
                    asyncio.get_running_loop().add_signal_handler(sig, stop.set)
                except NotImplementedError:
                    pass
            await engine.serve(stop, duration=args.duration)
    finally:
        await client.aclose()


def main() -> None:
    p = parser()
    args = p.parse_args()
    if args.duration is not None and not 0 < args.duration < float("inf"):
        p.error("duration must be finite and positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    try:
        asyncio.run(run(args))
    except KeyboardInterrupt:
        pass
    except (ValueError, SnapshotConflict) as exc:
        p.exit(2, f"{exc}\n")
    except RedisError as exc:
        p.exit(
            2, f"Redis operation failed ({type(exc).__name__}); check REDIS_URL and connectivity\n"
        )


if __name__ == "__main__":
    main()
