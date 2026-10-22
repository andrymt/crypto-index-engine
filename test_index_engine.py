import asyncio
import json
import os
import unittest
import uuid
from dataclasses import replace
from unittest.mock import patch

import fakeredis.aioredis
import redis.asyncio as redis
from redis.exceptions import WatchError

from index_engine import CatchupRequired, Config, Engine, calculate, canonical

SECOND = 1_760_000_000


def sample(price, volume, **kwargs):
    return canonical({"price": price, "volume": volume, **kwargs})


def complete():
    return {
        "coinbase": sample("60000", "1"),
        "kraken": sample("60100", "2"),
        "gemini": sample("60200", "3"),
    }


class CalculationTests(unittest.TestCase):
    def setUp(self):
        self.config = Config()

    def test_volume_weighted_price_not_simple_mean(self):
        tick = calculate(SECOND, complete(), self.config)
        self.assertEqual(tick["price"], "60133.33333333")
        self.assertEqual(tick["total_volume"], "6")
        self.assertEqual(tick["venue_count"], 3)
        self.assertEqual(tick["contributions"]["gemini"]["weight"], "0.500000000000")

    def test_missing_venue_excluded_without_carry_forward(self):
        tick = calculate(
            SECOND, {"coinbase": sample("100", "1"), "kraken": sample("102", "3")}, self.config
        )
        self.assertEqual(tick["price"], "101.50000000")
        self.assertEqual(tick["excluded"], {"gemini": "missing"})

    def test_quorum_failure_is_explicit_null_price(self):
        tick = calculate(SECOND, {"coinbase": sample("100", "1")}, self.config)
        self.assertEqual(tick["status"], "insufficient_venues")
        self.assertIsNone(tick["price"])

    def test_zero_volume_is_excluded(self):
        tick = calculate(
            SECOND, {"coinbase": sample("100", "0"), "kraken": sample("102", "0")}, self.config
        )
        self.assertIsNone(tick["price"])
        self.assertEqual(tick["total_volume"], "0")
        self.assertEqual(tick["excluded"]["coinbase"], "zero_volume")

    def test_invalid_values_are_not_allowed_to_poison_index(self):
        for value in ("NaN", "Infinity", "-1", "0", True, None):
            with self.subTest(value=value):
                data = complete()
                data["coinbase"] = sample(value, "1")
                tick = calculate(SECOND, data, self.config)
                self.assertEqual(tick["excluded"]["coinbase"], "invalid_sample")
                self.assertEqual(tick["status"], "ok")

    def test_invalid_json_and_wrong_timestamp(self):
        tick = calculate(
            SECOND,
            {"coinbase": "{", "kraken": "[]", "gemini": sample("100", "1", timestamp=SECOND - 1)},
            self.config,
        )
        self.assertEqual(
            tick["excluded"],
            {
                "coinbase": "invalid_sample",
                "kraken": "invalid_sample",
                "gemini": "timestamp_mismatch",
            },
        )

    def test_decimal_json_numbers_and_precision(self):
        data = {
            "coinbase": '{"price":100.00000001,"volume":0.1}',
            "kraken": '{"price":100.00000003,"volume":0.3}',
        }
        tick = calculate(SECOND, data, self.config)
        self.assertEqual(tick["price"], "100.00000002")  # exact 100.000000025, half-even
        self.assertEqual(tick["total_volume"], "0.4")

    def test_negative_volume_and_excess_precision_rejected(self):
        for volume in ("-1", "1e999999", "0.0000000000000000001"):
            with self.subTest(volume=volume):
                tick = calculate(SECOND, {"coinbase": sample("100", volume)}, self.config)
                self.assertEqual(tick["excluded"]["coinbase"], "invalid_sample")

    def test_configurable_quorum_and_rounding(self):
        config = replace(self.config, min_venues=1, decimal_places=2)
        tick = calculate(SECOND, {"coinbase": sample("100.125", "1")}, config)
        self.assertEqual(tick["price"], "100.12")

    def test_keys_match_requested_redis_contract(self):
        self.assertEqual(self.config.input_key("coinbase"), "coinbase-xbt-usd-1")
        self.assertEqual(self.config.output_key, "ixbt-usd")
        self.assertEqual(replace(self.config, prefix="demo:").output_key, "demo:ixbt-usd")

    def test_invalid_config_and_timestamp(self):
        for options in (
            {"min_venues": 4},
            {"venues": ("kraken", "kraken")},
            {"publication_delay": 0},
            {"max_catchup": 10},
            {"base": "BTC-USD"},
        ):
            with self.subTest(options=options), self.assertRaises(ValueError):
                Config(**options)
        for second in (True, -1, 0, 1.5):
            with self.subTest(second=second), self.assertRaises(ValueError):
                calculate(second, complete(), self.config)

    def test_fingerprint_tracks_methodology_only(self):
        self.assertEqual(
            self.config.fingerprint, replace(self.config, correction_window=20).fingerprint
        )
        self.assertNotEqual(self.config.fingerprint, replace(self.config, min_venues=1).fingerprint)


class RedisEngineTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        self.config = Config(prefix=f"test:{uuid.uuid4().hex}:", correction_window=2)
        url = os.environ.get("REDIS_TEST_URL")
        self.client = (
            redis.from_url(url, decode_responses=True, protocol=2)
            if url
            else fakeredis.aioredis.FakeRedis(decode_responses=True)
        )
        self.engine = Engine(self.client, self.config)

    async def asyncTearDown(self):
        # Remove only this test's random namespace; never FLUSHDB shared Redis.
        await self.client.delete(
            self.config.output_key,
            self.config.meta_key,
            *[self.config.input_key(v) for v in self.config.venues],
        )
        await self.client.aclose()

    async def seed(self, second=SECOND, data=None):
        for venue, value in (data or complete()).items():
            await self.client.hset(self.config.input_key(venue), str(second), value)

    async def stored(self, second=SECOND):
        return json.loads(await self.client.hget(self.config.output_key, str(second)))

    async def test_persist_hash_tick_and_checkpoint(self):
        await self.seed()
        summary = await self.engine.process(SECOND)
        self.assertEqual(summary["written"], 1)
        tick = await self.stored()
        self.assertEqual(tick["price"], "60133.33333333")
        self.assertEqual(tick["revision"], 1)
        self.assertEqual(
            await self.client.hget(self.config.meta_key, "last_processed_second"), str(SECOND)
        )

    async def test_idempotent_rerun_preserves_tick_bytes_and_revision(self):
        await self.seed()
        await self.engine.process(SECOND)
        before = await self.client.hget(self.config.output_key, str(SECOND))
        await self.engine.process(SECOND)
        self.assertEqual(await self.client.hget(self.config.output_key, str(SECOND)), before)
        self.assertEqual((await self.stored())["revision"], 1)

    async def test_late_data_revises_earlier_tick_in_correction_window(self):
        await self.seed(data={"coinbase": sample("100", "1"), "kraken": sample("102", "3")})
        await self.engine.process(SECOND)
        await self.client.hset(self.config.input_key("gemini"), str(SECOND), sample("110", "4"))
        await self.seed(second=SECOND + 1)
        await self.engine.process(SECOND + 1)
        tick = await self.stored()
        self.assertEqual(tick["price"], "105.75000000")
        self.assertEqual(tick["revision"], 2)
        self.assertEqual(tick["excluded"], {})

    async def test_corrected_volume_reweights_index(self):
        await self.seed()
        await self.engine.process(SECOND)
        await self.client.hset(
            self.config.input_key("coinbase"), str(SECOND), sample("60000", "10")
        )
        await self.engine.process(SECOND)
        tick = await self.stored()
        self.assertEqual(tick["price"], "60053.33333333")
        self.assertEqual(tick["total_volume"], "15")
        self.assertEqual(tick["revision"], 2)

    async def test_source_deletion_invalidates_prior_price(self):
        await self.seed()
        await self.engine.process(SECOND)
        for venue in ("coinbase", "kraken"):
            await self.client.hdel(self.config.input_key(venue), str(SECOND))
        await self.engine.process(SECOND)
        tick = await self.stored()
        self.assertIsNone(tick["price"])
        self.assertEqual(tick["revision"], 2)

    async def test_catchup_fills_every_missed_second(self):
        for second in range(SECOND, SECOND + 4):
            await self.seed(second)
        await self.engine.process(SECOND)
        await self.engine.process(SECOND + 3)
        for second in range(SECOND, SECOND + 4):
            self.assertEqual((await self.stored(second))["status"], "ok")

    async def test_large_gap_refuses_silent_skip(self):
        await self.engine.process(SECOND)
        with self.assertRaises(CatchupRequired):
            await self.engine.process(SECOND + 301)
        self.assertIsNone(await self.client.hget(self.config.output_key, str(SECOND + 301)))

    async def test_explicit_recalculation_does_not_move_live_checkpoint(self):
        await self.seed()
        await self.engine.process(SECOND)
        await self.seed(SECOND - 100)
        await self.engine.process(SECOND - 100, start=SECOND - 100)
        self.assertEqual((await self.stored(SECOND - 100))["status"], "ok")
        self.assertEqual(
            await self.client.hget(self.config.meta_key, "last_processed_second"), str(SECOND)
        )

    async def test_clock_rollback_fails_visibly(self):
        await self.engine.process(SECOND)
        with self.assertRaisesRegex(ValueError, "Clock"):
            await self.engine.process(SECOND - 1)

    async def test_mismatched_methodology_cannot_overwrite_existing_index(self):
        await self.engine.process(SECOND)
        other = Engine(self.client, replace(self.config, min_venues=1))
        with self.assertRaisesRegex(ValueError, "methodology"):
            await other.process(SECOND)

    async def test_two_workers_converge_without_spurious_revisions(self):
        await self.seed()
        other = Engine(self.client, self.config)
        await asyncio.gather(self.engine.process(SECOND), other.process(SECOND))
        self.assertEqual((await self.stored())["revision"], 1)

    async def test_late_data_outside_window_requires_explicit_recalculation(self):
        await self.seed()
        await self.engine.process(SECOND)
        await self.client.hset(
            self.config.input_key("coinbase"), str(SECOND), sample("61000", "10")
        )
        await self.engine.process(SECOND + 3)
        self.assertEqual((await self.stored())["revision"], 1)
        await self.engine.process(SECOND, start=SECOND)
        self.assertEqual((await self.stored())["revision"], 2)

    async def test_transaction_conflict_retries_entire_snapshot(self):
        await self.seed()
        real_pipeline = self.client.pipeline
        conflicts = [True]

        class ConflictPipeline:
            def __init__(self):
                self.inner = real_pipeline()

            async def __aenter__(self):
                await self.inner.__aenter__()
                return self

            async def __aexit__(self, *args):
                return await self.inner.__aexit__(*args)

            def __getattr__(self, name):
                return getattr(self.inner, name)

            async def execute(self):
                if conflicts:
                    conflicts.pop()
                    raise WatchError("synthetic conflict")
                return await self.inner.execute()

        with patch.object(self.client, "pipeline", side_effect=ConflictPipeline):
            await self.engine.process(SECOND)
        self.assertEqual((await self.stored())["revision"], 1)

    async def test_serve_stops_without_extra_tick_when_already_stopped(self):
        stop = asyncio.Event()
        stop.set()
        await self.engine.serve(stop)
        self.assertEqual(await self.client.hlen(self.config.output_key), 0)

    async def test_changed_source_aborts_stale_snapshot(self):
        await self.seed()
        original_pipeline = self.client.pipeline
        changed = False
        client = self.client
        config = self.config

        class ChangingPipeline:
            def __init__(self):
                self.inner = original_pipeline()

            async def __aenter__(self):
                await self.inner.__aenter__()
                return self

            async def __aexit__(self, *args):
                return await self.inner.__aexit__(*args)

            def __getattr__(self, name):
                return getattr(self.inner, name)

            async def execute(self):
                nonlocal changed
                if not changed:
                    changed = True
                    await client.hset(
                        config.input_key("coinbase"), str(SECOND), sample("61000", "1")
                    )
                return await self.inner.execute()

        with patch.object(self.client, "pipeline", side_effect=ChangingPipeline):
            await self.engine.process(SECOND)
        tick = await self.stored()
        self.assertEqual(tick["price"], "60300.00000000")
        self.assertEqual(tick["revision"], 1)

    async def test_scheduler_targets_closed_second(self):
        stop = asyncio.Event()
        targets = []

        async def process(target):
            targets.append(target)
            stop.set()
            return {"target": target}

        with (
            patch.object(self.engine, "process", side_effect=process),
            patch("index_engine.time.time", return_value=1000.9),
            patch("builtins.print"),
        ):
            await self.engine.serve(stop)
        self.assertEqual(targets, [998])


if __name__ == "__main__":
    unittest.main()
