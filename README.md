# Crypto Index Engine

Calculate a volume-weighted crypto price index every second from exchange data in Redis, then store each calculated tick in a Redis hash.

For Bitcoin/USD:

- Read `coinbase-xbt-usd-1`, `kraken-xbt-usd-1`, and `gemini-xbt-usd-1`.
- Calculate `sum(venue_price × venue_volume) / sum(venue_volume)` for the same second.
- Write the result to hash **`ixbt-usd`**, using the Unix-second timestamp as its field.

An original portfolio project by [Andrii Trush](https://github.com/andrymt), complementing the [Market Data WebSocket Pool](https://github.com/andrymt/market-data-websocket-pool). It is independently written demonstration code, not an employer's implementation or a claim of production deployment.

## Redis contract

Each input key is a **hash**. A field is an integer Unix timestamp in UTC, identifying the half-open bucket `[t, t + 1 second)`. The field value is JSON:

```sh
HSET coinbase-xbt-usd-1 1760000000 '{"price":"60000","volume":"1"}'
HSET kraken-xbt-usd-1 1760000000 '{"price":"60100","volume":"2"}'
HSET gemini-xbt-usd-1 1760000000 '{"price":"60200","volume":"3"}'
```

Here `1` in the **key name** means a one-second interval, not a timestamp. `price` is the venue's VWAP for that one-second bucket; `volume` is traded **base-asset quantity** during the same bucket. All sources must use the same unit and quote currency. Do not substitute rolling 24-hour volume or quote-currency turnover.

An upstream producer should aggregate trades as `venue_price = sum(trade_price × trade_quantity) / sum(trade_quantity)` and `venue_volume = sum(trade_quantity)`. Map BTC and XBT consistently upstream. This repository consumes these completed aggregates; the WebSocket project's NDJSON is not wired to Redis automatically.

`price` and `volume` may be decimal strings or JSON numbers. Strings are recommended. An optional `timestamp` inside the JSON must exactly match the integer hash field. Samples support up to 30 significant digits and 18 fractional/exponent places.

For the example above:

```text
(60000×1 + 60100×2 + 60200×3) / (1+2+3) = 60133.33333333
```

Read the output:

```sh
HGET ixbt-usd 1760000000
```

Illustrative output (calculation timestamp shortened for readability):

```json
{
  "methodology": "cross_venue_vwap_1s_v1",
  "timestamp": 1760000000,
  "symbol": "XBT-USD",
  "interval_seconds": 1,
  "status": "ok",
  "price": "60133.33333333",
  "total_volume": "6",
  "venue_count": 3,
  "min_venues": 2,
  "contributions": {
    "coinbase": {"price":"60000","volume":"1","weight":"0.166666666667"},
    "kraken": {"price":"60100","volume":"2","weight":"0.333333333333"},
    "gemini": {"price":"60200","volume":"3","weight":"0.500000000000"}
  },
  "excluded": {},
  "revision": 1,
  "calculated_at_ns": 1760000002000000000
}
```

This is a USD-denominated volume-weighted price series, not a rebased total-return index. Weighting uses the same second's volume, not a rolling window. Calculation uses Python `Decimal` with 80-digit working precision and half-even rounding; output precision defaults to eight decimals. Display weights are rounded to twelve decimals and are not fed back into the price calculation.

## Run

Python 3.11+ and a standalone Redis server are required. No exchange credentials are needed by this calculator.

```sh
python -m venv .venv
source .venv/bin/activate
python -m pip install -e '.[dev]'

# Use an existing Redis server. Keep credentials in the environment.
export REDIS_URL=redis://localhost:6379/0

# Calculate continuously; defaults match the keys above.
crypto-index run

# Optional explicit configuration and a bounded run.
crypto-index run --config config.example.json --duration 30

# Process one bucket and its applicable recent correction range.
crypto-index tick

# Recalculate historical buckets without moving the live checkpoint.
crypto-index recalculate --start 1760000000 --end 1760000020
```

A provided config overrides defaults field by field. `prefix` namespaces input, output and metadata keys together. For example, `prefix: "demo:"` produces `demo:coinbase-xbt-usd-1` and `demo:ixbt-usd`. `base: "eth"` produces `coinbase-eth-usd-1` and `ieth-usd`.

### Synthetic demo

```sh
crypto-index demo --timestamp 1760000000
```

This command explicitly seeds the three sample aggregates shown above and calculates their tick. It **always** uses the `demo:` namespace, leaving unprefixed application data alone. Its printed output includes `"demo":true`. It does not connect to exchanges or claim to use live prices.

### Docker Compose

```sh
# Start an isolated example Redis on localhost port 16379.
docker compose up -d redis

# Run the demo inside the calculator container.
docker compose run --rm index demo --timestamp 1760000000

# Run the continuously scheduled calculator against the example Redis.
docker compose up --build index

# Inspect the demo tick from another terminal.
docker compose exec redis redis-cli HGET demo:ixbt-usd 1760000000
```

Compose keeps Redis data in a named volume and enables AOF. This is a local example, not a hardened deployment. The collector cannot calculate real prices until an upstream producer populates its configured input hashes.

## Timing, corrections and recovery

The service targets each new wall-clock second without accumulating fixed-sleep drift. With `publication_delay: 2`, wall-clock second `N` processes bucket `N−2`, giving that bucket one second after closing for data to arrive. It does not promise hard real-time deadlines under load or network failure.

After initialization, each cycle also rechecks the most recent **30 seconds** by default. Late or corrected source data revises affected output fields in place and increments their `revision`. Unchanged results retain identical JSON, revision and calculation time. If an earlier valid tick loses enough inputs, its new revision has `price: null`; old valid prices are never carried forward to conceal missing data.

`ixbt-usd:meta` stores a methodology fingerprint and `last_processed_second`. Short outages are caught up without silently skipping seconds, subject to `max_catchup` (300 seconds by default, at most 3600). A larger gap stops with an explicit recovery error. An operator must backfill in bounded ranges and deliberately reconcile the checkpoint before resuming, or increase the configured catch-up bound when the gap fits it. Historical recalculation intentionally does not advance the live checkpoint.

Corrections older than the configured window require explicit recalculation. No moving-average methodology or window-dependent recalculation is implemented in this version; every bucket's formula is independent.

## Data quality and consistency

- Missing, malformed, timestamp-mismatched, non-finite, negative and zero-volume samples are excluded with explicit reasons.
- At least two valid venues are required by default. Otherwise the tick is stored as `status: "insufficient_venues"` and `price: null`, with source diagnostics.
- A positive but erroneous price can still influence the result. Outlier filtering, volume caps, venue-quality scoring and wash-trade detection are outside this methodology.
- Redis `WATCH` monitors every source hash, the output hash and metadata before reads. `MULTI/EXEC` commits changed ticks and checkpoint together only if the watched keys stayed unchanged. Concurrent source updates or competing writers trigger bounded whole-snapshot retries.
- A methodology fingerprint prevents writers with different source sets, precision or quorum rules from overwriting the same index key.
- Source hashes must contain complete bucket values, replaced atomically with `HSET` when corrected. Cross-venue input arrival does not have to be simultaneous.
- Any update anywhere in a watched source hash can cause a retry. Very hot hashes or remote Redis round trips can cause contention; this design favors consistency over maximizing throughput.
- This version supports standalone Redis. Its key names do not share a Redis Cluster hash tag; multi-key transactions are not cluster-compatible.
- Hashes grow with time. A production deployment needs an explicit retention/archive policy and source retention sufficient for the configured correction and catch-up ranges. No hidden expiry removes historical ticks.
- Redis persistence and replication determine durability. Atomic execution alone is not a guarantee against data loss after server failure.

## Design

`calculate()` is a pure, independently testable function. `Engine` owns optimistic transactions, idempotent revisions and checkpointed recovery. The scheduler and CLI handle boundary timing, stop signals and sanitized connection failures. Every successful cycle prints a compact JSON summary, while Redis holds the actual index history.

## Test

```sh
ruff check index_engine.py test_index_engine.py
ruff format --check index_engine.py test_index_engine.py
python -m unittest -v
python -m build
```

The default local tests use `fakeredis`; set `REDIS_TEST_URL` to run the same integration cases against a real standalone Redis server. Every test uses a random namespace and deletes only its own keys; no shared database is flushed.

The 28 tests cover weighted arithmetic, decimal precision, missing/invalid data, quorum, idempotency, late data, revised volume, source deletion, catch-up, explicit recalculation, clock rollback, methodology conflicts, concurrent writers, optimistic transaction conflicts and scheduler targeting. CI runs on Python 3.11, 3.12 and 3.13 with a real Redis service.

## References

- [Redis hashes](https://redis.io/docs/latest/develop/data-types/hashes/)
- [Redis transactions and WATCH](https://redis.io/docs/latest/develop/using-commands/transactions/)
- [redis-py asyncio and cleanup](https://redis.readthedocs.io/en/stable/examples/asyncio_examples.html)

MIT licensed source code. Exchange data rights remain with their respective providers.
