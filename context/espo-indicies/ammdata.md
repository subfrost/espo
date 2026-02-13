# ammdata indices

DB namespace prefix: `ammdata:`

This document inventories the indices (key spaces) written/read by the `ammdata` module and evaluates each against the refactor rules:
- Indices must be representable as either list-based (enumerated via prefix scan) or key-based (direct K/V lookup).
- Index values must not be “bags of data” like `Vec<T>` or `Map<K,V>` containing many logical rows.

Key patterns below are **relative** (they do not include the `ammdata:` namespace prefix).

Assumption: this refactor will ship with a mandatory full reindex. No backwards compatibility, in-place migrations, or mixed-schema reads are required.

## /index_height

Key pattern: `/index_height`

Value: `u32` (little-endian, 4 bytes)

Compatible: Yes (key-based)

## /pools/ (pool id -> market defs)

Key pattern:
- `/pools/<pool_id_be>`

Value:
- `SchemaMarketDefs { base_alkane_id, quote_alkane_id, pool_alkane_id }` (borsh)

Compatible: Yes (key-based)

## Candle Series (pool and token time series)

These are time-series keys that are read by scanning a prefix and taking newest/oldest via iteration.

### Pool candles (full candles)

Key patterns:
- `fc1:<pool_block_hex>:<pool_tx_hex>:<tf_code>:<bucket_ts_decimal> -> SchemaFullCandleV1`

Value:
- `SchemaFullCandleV1 { base_candle, quote_candle }` (borsh, fixed-size)

Compatible: Yes (list-based via scan)

Refactor note (optional hygiene):
- `bucket_ts` is encoded as a decimal ASCII string. This relies on stable digit width for correct lexicographic ordering. If you want ordering to be future-proof, switch to fixed-width binary `u64be` in the key.

### Token candles (USD, derived USD, MCAP)

Key patterns:
- `tuc1:<token_block_hex>:<token_tx_hex>:<tf_code>:<bucket_ts_decimal> -> SchemaCandleV1`
- `tud1:<token_block_hex>:<token_tx_hex>:<quote_block_hex>:<quote_tx_hex>:<tf_code>:<bucket_ts_decimal> -> SchemaCandleV1`
- `tmc1:<token_block_hex>:<token_tx_hex>:<tf_code>:<bucket_ts_decimal> -> SchemaCandleV1`
- `tdmc1:<token_block_hex>:<token_tx_hex>:<quote_block_hex>:<quote_tx_hex>:<tf_code>:<bucket_ts_decimal> -> SchemaCandleV1`

Value:
- `SchemaCandleV1 { open, high, low, close, volume }` (borsh, fixed-size)

Compatible: Yes (list-based via scan)

## BTC Price Indices

### /btc_usd_price/v1/

Key pattern:
- `/btc_usd_price/v1/<height:u64be> -> u128` (little-endian in value)

Compatible: Yes (key-based series)

### BTC USD line series

Key pattern:
- `btu1:<tf_code>:<bucket_ts_decimal> -> u128`

Compatible: Yes (list-based via scan)

## Activity Log (primary)

Key pattern:
- `activity:v1:<pool.block_dec>:<pool.tx_dec>:<ts_dec>:<seq_dec> -> SchemaActivityV1`

Value:
- `SchemaActivityV1 { timestamp, txid, kind, direction, base_delta, quote_delta, address_spk, success }` (borsh)

Compatible: Yes (key-based by (pool,ts,seq))

Refactor note (ordering hygiene):
- `ts` and `seq` are decimal strings; if you rely on lexicographic iteration for correct ordering within a timestamp, consider switching to `u64be + u32be` in-key encoding.

## Activity Secondary Indexes (sorted paging)

Source: `src/modules/ammdata/utils/activity.rs`

These keys already follow the “list-based via scan” model: each index row is its own key, and values are small fixed payloads.

Namespace prefix:
- `activity:idx:v1:<pool.block_dec>:<pool.tx_dec>:` or
- `activity:idx:v1:<pool.block_dec>:<pool.tx_dec>:<group>:` where `<group>` is `trades` or `events`

Index families (key tails are fixed-width big-endian numbers):
- `...ts:<ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`
- `...absb:<abs_base:u128be><ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`
- `...absq:<abs_quote:u128be><ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`
- `...sb_absb:<side:u8><abs_base:u128be><ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`
- `...sq_absq:<side:u8><abs_quote:u128be><ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`
- `...sb_ts:<side:u8><ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`
- `...sq_ts:<side:u8><ts:u64be><seq:u32be> -> <ts:u64be><seq:u32be>`

Count keys:
- `activity:idx:v1:<pool...>:__count -> u64be`
- `activity:idx:v1:<pool...>:<group>:__count -> u64be`

Compatible: Yes (list-based)

## /reserves_snapshot_v1 (all pools snapshot in one key)

Key pattern:
- `/reserves_snapshot_v1`

Value:
- `SchemaReservesSnapshot { entries: BTreeMap<pool_id, SchemaPoolSnapshot> }` (borsh)

Compatible: No

Violations:
- Stores a large map of many logical rows in a single value.

Integration plan:
1. Replace the single global snapshot value with per-pool rows.

```text
reserves_snapshot/v2/pool/<pool_id_be> -> SchemaPoolSnapshot
```

2. Provide listability without an aggregate payload.

Option A: scan `reserves_snapshot/v2/pool/` and parse pool ids from keys.

Option B: maintain an explicit pool id list.

```text
reserves_snapshot/v2/pool_ids/<idx:u32be> -> <pool_id_be>
reserves_snapshot/v2/pool_ids/length -> u128
```

3. Update writers: when reserves change for a pool, only update that pool’s snapshot key.
4. Update readers: `get_reserves_snapshot` becomes a scan + multi_get (or scan + decode per value).
5. Reindex: mandatory. Populate the per-pool snapshot keys during reindex; do not preserve `/reserves_snapshot_v1`.

## /canonical_pool/v1 (token -> Vec<canonical pools>)

Key pattern:
- `/canonical_pool/v1/<token_id_be>`

Value:
- `Vec<SchemaCanonicalPoolEntry { pool_id, quote_id }>` (borsh)

Compatible: No

Violations:
- Stores a logical multi-row index as a vector value.

Integration plan:
1. Replace with direct key-based rows per (token, quote).

```text
canonical_pool/v2/<token_id_be>/<quote_id_be> -> <pool_id_be>
```

2. Reads.
All canonical pools for token: scan `canonical_pool/v2/<token_id>/` and decode `(quote_id -> pool_id)` pairs.
Canonical pool for (token, quote): direct get on the key.
3. Writes: update only the affected quote entry instead of rewriting a vec.
4. Reindex: mandatory. Populate the per-(token, quote) keys during reindex; do not preserve `/canonical_pool/v1/*`.

## Token Metrics (direct and derived)

Key patterns:
- `/token_metrics/v1/<token_id_be> -> SchemaTokenMetricsV1`
- `/token_metrics/derived/v1/<quote_id_be><token_id_be> -> SchemaTokenMetricsV1`

Compatible: Yes (key-based)

Notes:
- The metrics structs include several `String` fields (changes). These are small and bounded in practice.

## Token Metrics Sorted Indices

These indices store ordering in the key and typically use an empty value.

Key patterns:
- `/token_metrics/index/<field>/<score_be><token_id_be> -> empty`
- `/token_metrics/derived/index/<quote_id_be>/<field>/<score_be><token_id_be> -> empty`
- `/token_metrics/index_count -> u64(le)`
- `/token_metrics/derived/index_count/<quote_id_be> -> u64(le)`

Compatible: Yes (list-based)

## Pool Metrics and Sorted Indices

Key patterns:
- `/pool_metrics/v1/<pool_id_be> -> SchemaPoolMetricsV1`
- `/pool_metrics/v2/<pool_id_be> -> SchemaPoolMetricsV2`
- `/pool_metrics/index/<field>/<score_be><pool_id_be> -> empty`
- `/pool_metrics/index_count -> u64(le)`

Compatible: Yes (key-based for metrics, list-based for ordered index)

## Token Search Index (prefix search)

Key patterns:
- `/token_search_index/v1/<field>/<prefix>/<score_be><token_id_be> -> empty`
- `/token_search_index/derived/v1/<quote_id_be>/<field>/<prefix>/<score_be><token_id_be> -> empty`

Compatible: Yes (list-based)

## Pool Name Index

Key pattern:
- `/pool_name_index/<name>/<pool_id_be> -> empty`

Compatible: Yes (list-based)

## Factory and Pool Relationship Indices

Key patterns:
- `/amm_factories/v1/<factory_id_be> -> empty`
- `/factory_pools/v1/<factory_id_be>/<pool_id_be> -> empty`
- `/pool_factory/v1/<pool_id_be> -> <factory_id_be>` (12 bytes, big-endian parts)
- `/token_pools/v1/<token_id_be><pool_id_be> -> empty`

Compatible: Yes (list-based for membership, key-based for pool->factory)

## Pool Creation and LP Supply

Key patterns:
- `/pool_creation_info/v1/<pool_id_be> -> SchemaPoolCreationInfoV1`
- `/pool_lp_supply/latest/<pool_id_be> -> u128(le)`

Compatible: Yes (key-based)

## /pool_details_snapshot/v1 (includes embedded JSON blob)

Key pattern:
- `/pool_details_snapshot/v1/<pool_id_be> -> SchemaPoolDetailsSnapshot`

Value:
- `SchemaPoolDetailsSnapshot { value_json: Vec<u8>, ...numeric fields... }` (borsh)

Compatible: No

Violations:
- `value_json` is a potentially large “bag of data” payload embedded in an index value.

Integration plan:
1. Remove `value_json` from the persisted representation.

Option A: new struct `SchemaPoolDetailsSnapshotV2` without `value_json`.

Option B: store fields as separate direct keys.

```text
pool_details/v2/<pool_id>/token0_tvl_usd -> u128
pool_details/v2/<pool_id>/token1_tvl_usd -> u128
pool_details/v2/<pool_id>/pool_tvl_usd -> u128
pool_details/v2/<pool_id>/pool_volume_1d_usd -> u128
pool_details/v2/<pool_id>/pool_volume_30d_usd -> u128
pool_details/v2/<pool_id>/pool_apr -> f64
pool_details/v2/<pool_id>/tvl_change_24h -> f64
pool_details/v2/<pool_id>/lp_supply -> u128
```

2. Reconstruct JSON at read time from the structured fields and existing indices (pool defs, metrics, creation info, LP supply).
3. If you must persist a JSON-ish blob, chunk it as a list instead of one value.

```text
pool_details/v2/<pool_id>/json/<idx:u32be> -> bytes
pool_details/v2/<pool_id>/json/length -> u32
```

4. Reindex: mandatory. Write the v2 snapshot representation during reindex; do not preserve `/pool_details_snapshot/v1/*`.

## TVL Versioned Series

Key pattern:
- `/tvlVersioned/<pool_id_be>/<height:u32be> -> u128(le)`

Compatible: Yes (key-based series with scan-friendly prefix)

## Swap and History Indices

These are event lists represented as keys with empty values.

Key patterns:
- `/token_swaps/v1/<token_id_be>/<ts_dec>:<seq_dec>/<pool_id_be> -> empty`
- `/pool_creations/v1/<ts_dec>:<seq_dec>/<pool_id_be> -> empty`
- `/address_pool_swaps/v1/<spk_len:u16be><spk><pool_id_be><ts:u64be><seq:u32be> -> empty`
- `/address_token_swaps/v1/<spk_len:u16be><spk><token_id_be><ts:u64be><seq:u32be><pool_id_be> -> empty`
- `/address_pool_creations/v1/<spk_len:u16be><spk><ts:u64be><seq:u32be><pool_id_be> -> empty`
- `/address_pool_mints/v1/<spk_len:u16be><spk><ts:u64be><seq:u32be><pool_id_be> -> empty`
- `/address_pool_burns/v1/<spk_len:u16be><spk><ts:u64be><seq:u32be><pool_id_be> -> empty`
- `/address_amm_history/v1/<spk_len:u16be><spk><ts:u64be><seq:u32be><kind:u8><pool_id_be> -> empty`
- `/amm_history_all/v1/<ts:u64be><seq:u32be><kind:u8><pool_id_be> -> empty`

Compatible: Yes (list-based)

Refactor note (optional):
- Some keys use `ts_dec:seq_dec` ASCII components; consider moving all event keys to fixed-width big-endian encoding for robust ordering.
