# essentials indices

DB namespace prefix: `essentials:`

This document inventories the indices (key spaces) written/read by the `essentials` module and evaluates each against the refactor rules:
- Indices must be representable as either list-based (enumerated via prefix scan) or key-based (direct K/V lookup).
- Index values must not be “bags of data” like `Vec<T>` or `Map<K,V>` containing many logical rows (example forbidden: `holders/<alkane> -> Vec<holders>`).
- Large/aggregate payloads should be split into per-item keys, with lists acting as pointers when needed.

Key patterns below are **relative** (they do not include the `essentials:` namespace prefix).

Assumption: this refactor will ship with a mandatory full reindex. No backwards compatibility, in-place migrations, or mixed-schema reads are required.

## /index_height

Key pattern: `/index_height`

Value: `u32` (little-endian, 4 bytes)

Compatible: Yes (key-based)

Notes:
- This is already a simple scalar and should remain as-is.

## Alkane KV Mirror (KV_ROWS and DIR_ROWS)

Source: `src/modules/essentials/main.rs`, `src/modules/essentials/storage.rs`

### KV_ROWS (contract storage values)

Key pattern (binary):
- `[0x01] <alkane_block:u32be> <alkane_tx:u64be> <key_len:u16be> <key_bytes>`

Value:
- `[last_txid:32 bytes] + [raw_value_bytes...]` (raw contract value bytes, length unbounded by schema)

Read pattern:
- Key-based lookup via `kv_row_key(alkane, skey)`

Compatible: Mostly yes (key-based), with one potential rule risk

Why it might violate the rules:
- If “keys should not store a bunch of data” is interpreted strictly across the whole DB, KV_ROWS can store arbitrarily large contract values.

Integration plan (only if you want KV_ROWS to follow strict “no big values”):
1. Add a maximum value size policy and instrumentation (log and cap) to surface outliers.
2. If large values must be supported, chunk them using:

```text
kv/v2/<alkane>/<key>/chunk/<idx:u32be> -> bytes
kv/v2/<alkane>/<key>/chunk/length -> u32
kv/v2/<alkane>/<key>/last_txid -> [32]
```

3. Keep a small “head” value for common fast reads:

```text
kv/v2/<alkane>/<key>/value -> bytes (only if size <= threshold)
```

4. Reindex: mandatory. If you adopt chunking, write the v2 layout during reindex.

### DIR_ROWS (directory entries for scanning keys)

Key pattern (binary):
- `[0x03] <alkane_block:u32be> <alkane_tx:u64be> <key_len:u16be> <key_bytes>`

Value: empty

Read pattern:
- List-based enumeration by prefix scan on `[0x03] <alkane_block:u32be> <alkane_tx:u64be>` (see `dir_scan_prefix`)

Compatible: Yes (list-based via scan)

Notes:
- This already matches the desired “scan prefix yields rows” abstraction.

## UTXO and Address Balance Indices

These indices currently store **vectors** of per-token balances inside a single key, which violates the “no Vec/Map as an index payload” rule.

### /balances/ (address + outpoint -> Vec<BalanceEntry>)

Key pattern:
- `/balances/<address>/<borsh(EspoOutpoint)>`

Value:
- `Vec<BalanceEntry>` (borsh), where `BalanceEntry { alkane: SchemaAlkaneId, amount: u128 }`

Compatible: No

Violations:
- Stores an index payload as `Vec<BalanceEntry>` (many logical rows in one value).

Integration plan:
1. Introduce per-token K/V rows (no vectors).

```text
address/<address>/outpoint/<outpoint_id> -> empty
outpoint/<outpoint_id>/addr -> <address>
outpoint/<outpoint_id>/spk -> <script_pubkey_bytes>
outpoint/<outpoint_id>/spent_by -> <txid_32> (optional)
outpoint/<outpoint_id>/balance/<alkane_id> -> u128
```

2. Add the address-level direct lookup the new model wants.

```text
address/<address>/balance/<alkane_id> -> u128
```

3. Update the writer: when a new outpoint is created, write `outpoint/.../addr`, `outpoint/.../spk`, and one `outpoint/.../balance/<alkane>` per alkane touched. Maintain `address/<address>/outpoint/<outpoint_id>` for enumeration. Maintain `address/<address>/balance/<alkane>` by applying deltas (increment on receive, decrement on spend).
4. Update readers/RPC: replace reads that decode `Vec<BalanceEntry>` with scans over `outpoint/<outpoint_id>/balance/` or direct lookups on `address/<address>/balance/`.
5. Reindex: mandatory. Populate the new per-token keys during reindex; do not preserve `/balances/`.

### /outpoint_balances/ (outpoint -> Vec<BalanceEntry>)

Key pattern:
- `/outpoint_balances/<borsh(EspoOutpoint)>`
- There can be multiple keys per `(txid,vout)` because the current `EspoOutpoint` serialization can vary (spent vs unspent variants); the refactor should avoid this by using a stable `outpoint_id`.

Value:
- `Vec<BalanceEntry>` (borsh)

Compatible: No

Violations:
- Stores an index payload as `Vec<BalanceEntry>`.
- Encodes “spentness” by changing the key (multiple variants), which makes the index harder to model cleanly as list/key-based data.

Integration plan:
1. Replace with stable, fixed outpoint identity keys. Define `outpoint_id = <txid_32be><vout_u32be>` (44 bytes) or a readable ASCII form; keep it fixed for life.
2. Store spend metadata in values, not in key identity.

```text
outpoint/<outpoint_id>/spent_by -> <txid_32be> (or missing)
```

3. Store per-token balances as direct K/V.

```text
outpoint/<outpoint_id>/balance/<alkane_id> -> u128
```

4. Update any code that relied on `outpoint_balances_prefix(txid,vout)` scans to instead read `spent_by` directly.
5. Reindex: mandatory. Populate the stable `outpoint_id` keys during reindex; drop all `/outpoint_balances/*` variants.

### /outpoint_addr/ (outpoint -> address)

Key pattern:
- `/outpoint_addr/<borsh(EspoOutpoint)>`

Value:
- `<address>` (UTF-8 bytes)

Compatible: Yes (key-based)

Refactor note:
- If you adopt the `outpoint/<outpoint_id>/...` scheme above, this becomes `outpoint/<outpoint_id>/addr`.

### /utxo_spk/ and /addr_spk/

Key patterns:
- `/utxo_spk/<borsh(EspoOutpoint)> -> <script_pubkey_bytes>`
- `/addr_spk/<address> -> <script_pubkey_bytes>`

Value: `Vec<u8>` of script bytes (bounded in practice)

Compatible: Yes (key-based)

Refactor note:
- If you want to avoid `borsh(EspoOutpoint)` in keys, move to `outpoint/<outpoint_id>/spk`.

## Holder and Activity Indices

### /holders/ (alkane -> Vec<HolderEntry>)

Key pattern:
- `/holders/<alkane_id_be>`

Value:
- `Vec<HolderEntry>` (borsh), each entry is `(holder_id, amount)`

Compatible: No

Violations:
- Stores a holder list as a vector in one value.

Integration plan:
1. Store each holder amount as a direct key-based row.

```text
alkane/<alkane_id>/holder/<holder_id> -> u128
```

2. Model `holder_id` without ambiguity. Use `addr/<address>` for addresses and `alk/<block:u32be><tx:u64be>` for alkane holders.
3. Add optional list-based “top holders” ordering without vectors.

```text
alkane/<alkane_id>/holders_by_amount/<amount:u128be>/<holder_id> -> empty
```

4. Keep count as a scalar.

```text
alkane/<alkane_id>/holders_count -> u128 (or u64)
```

5. Update writer: on holder delta, read previous amount from `.../holder/<holder_id>`, compute new amount, update that key. If maintaining ordered index, delete `.../holders_by_amount/<old_amount>/<holder_id>` and insert `.../<new_amount>/<holder_id>`. Update `holders_count` on transitions 0->nonzero and nonzero->0.
6. Reader changes: all holders is a scan of `alkane/<alkane_id>/holder/`. Top holders is an `iter_prefix_rev` over `.../holders_by_amount/`.
7. Reindex: mandatory. Populate the new holder keys during reindex; do not preserve `/holders/<alkane> -> Vec<...>`.

### /holders/count/ (alkane -> count)

Key pattern:
- `/holders/count/<alkane_id_be>`

Value:
- `HoldersCountEntry { count: u64 }` (borsh)

Compatible: Yes (key-based)

Refactor note:
- Can be replaced by `alkane/<alkane_id>/holders_count -> u128/u64` if you consolidate holder keys under `alkane/...`.

### /alkanes/holders/ordered/ (alkanes ordered by holder count)

Key pattern:
- `/alkanes/holders/ordered/<count:u64be><alkane_id_be>`

Value: empty

Compatible: Yes (list-based sorted index)

Notes:
- This is already a “scan prefix to get ordering” index.
- Optional: add a `.../length` key if you want a fast count.

### /alkanes/transfer_volume/ and /alkanes/total_received/

Key pattern:
- `/alkanes/transfer_volume/<alkane_id_be> -> Vec<AddressAmountEntry>`
- `/alkanes/total_received/<alkane_id_be> -> Vec<AddressAmountEntry>`

Value:
- `Vec<AddressAmountEntry>` where `AddressAmountEntry { address: String, amount: u128 }`

Compatible: No

Violations:
- Stores per-address maps as a vector in one value.

Integration plan:
1. Store per address directly.

```text
alkane/<alkane_id>/transfer_volume/<address> -> u128
alkane/<alkane_id>/total_received/<address> -> u128
```

2. Preserve “top addresses” queries with an ordered list index.

```text
alkane/<alkane_id>/transfer_volume_by_amount/<amount:u128be>/<address> -> empty
alkane/<alkane_id>/total_received_by_amount/<amount:u128be>/<address> -> empty
```

3. Update writer: on delta, read previous amount, write new amount, and update the ordered index by deleting the old key and inserting the new key.
4. Reader changes: “by address” becomes a direct get. “top N” becomes `iter_prefix_rev` over the ordered index prefix.
5. Reindex: mandatory. Populate the per-address keys (and ordered index keys, if you keep them) during reindex.

### /addresses/alkane_activity/ (address -> BTreeMap of alkane metrics)

Key pattern:
- `/addresses/alkane_activity/<address>`

Value:
- `AddressActivityEntry { transfer_volume: BTreeMap<alkane,u128>, total_received: BTreeMap<alkane,u128> }`

Compatible: No

Violations:
- Stores per-alkane maps inside one value (many logical rows).

Integration plan:
1. Store per (address, alkane) metric as direct keys.

```text
address/<address>/alkane/<alkane_id>/transfer_volume -> u128
address/<address>/alkane/<alkane_id>/total_received -> u128
```

2. Provide listability: scan `address/<address>/alkane/` to enumerate the alkanes touched.
3. Optional: add per-address ordered indexes (if you need “top tokens for address” queries).

```text
address/<address>/alkane_by_transfer_volume/<amount:u128be>/<alkane_id> -> empty
address/<address>/alkane_by_total_received/<amount:u128be>/<alkane_id> -> empty
```

4. Reindex: mandatory. Populate the per-(address, alkane) keys during reindex.

## Alkane Holdings (alkane owns other alkanes)

### /alkane_balances/ and /alkane_balances_by_height/

Key patterns:
- `/alkane_balances/<owner_alkane_id_be> -> Vec<BalanceEntry>`
- `/alkane_balances_by_height/<owner_alkane_id_be>/<height:u32be> -> Vec<BalanceEntry>`

Compatible: No

Violations:
- Uses vectors as the primary storage for a multi-row relation.
- The per-height snapshots multiply the vector problem (large repeated payloads).

Integration plan:
1. Replace with per-token direct keys.

```text
alkane/<owner_id>/balance/<token_id> -> u128
```

2. Replace “balances at or before height” with per-token time series.

```text
alkane/<owner_id>/balance_by_height/<token_id>/<height:u32be> -> u128 (write only when changed)
alkane/<owner_id>/balance_latest/<token_id> -> u128 (optional)
```

3. Reads: current balances is a scan of `alkane/<owner_id>/balance/`. Balance at or before height uses `iter_prefix_rev` under `balance_by_height/<token_id>/` starting at `<height>`.
4. Reindex: mandatory. If you keep history, populate the per-height series during reindex; if you drop history, omit the `balance_by_height` series keys.

## Balance Change Tx Indices

### /alkane_balance_txs_* (paged vectors + meta)

Key patterns:
- `/alkane_balance_txs/<alkane_id_be> -> Vec<AlkaneBalanceTxEntry>`
- `/alkane_balance_txs_paged/<alkane_id>/<page:u64be> -> Vec<AlkaneBalanceTxEntry>`
- `/alkane_balance_txs_meta/<alkane_id> -> AlkaneBalanceTxsMeta`
- `/alkane_balance_txs_by_token/<owner_id_be>/<token_id_be> -> Vec<AlkaneBalanceTxEntry>`
- `/alkane_balance_txs_by_token_paged/<owner_id>/<token_id>/<page> -> Vec<AlkaneBalanceTxEntry>`
- `/alkane_balance_txs_by_token_meta/<owner_id>/<token_id> -> AlkaneBalanceTxsMeta`

Compatible: No

Violations:
- Pages are vectors of entries (still a “bag of data” per key).
- `AlkaneBalanceTxEntry` contains `outflow: BTreeMap<SchemaAlkaneId, SignedU128>` which is itself a multi-row payload.

Integration plan:
1. Represent “tx list for token” as a list-based index of txids.

```text
alkane/<token_id>/balance_txs/<idx:u64be> -> <txid_32be>
alkane/<token_id>/balance_txs/length -> u128
```

2. Represent “tx list for (owner, token)” similarly.

```text
alkane/<owner_id>/balance_txs_by_token/<token_id>/<idx:u64be> -> <txid_32be>
alkane/<owner_id>/balance_txs_by_token/<token_id>/length -> u128
```

3. Move per-tx details into per-key rows.

```text
tx/<txid>/height -> u32
tx/<txid>/outflow/<alkane_id> -> i128 (or fixed SignedU128 encoding)
```

4. If you need “outflows for token only”, add a filtered prefix.

```text
tx/<txid>/outflow_for/<token_id>/<alkane_id> -> i128 (optional)
```

5. For pagination, use the list index and range-scan based on `idx` (no page vectors).
6. Reindex: mandatory. Write the new txid-list and per-tx outflow keys during reindex.

### /alkane_balance_txs_by_height/ (height -> BTreeMap token -> Vec<...>)

Key pattern:
- `/alkane_balance_txs_by_height/<height:u32be> -> BTreeMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>`

Compatible: No

Violations:
- Nested map and vectors in a single key.

Integration plan:
1. Store a list-based index for “txids that affected balances at this height”.

```text
balance_changes/<height:u32be>/<idx:u32be> -> <txid_32be>
balance_changes/<height:u32be>/length -> u128
```

2. If you also need “tokens affected at this height”, store a set-like list.

```text
balance_changes_tokens/<height:u32be>/<alkane_id> -> empty
```

3. Use `tx/<txid>/outflow/...` keys to fetch details.

## Token Metadata and Discovery

### /alkane_info/

Key pattern:
- `/alkane_info/<alkane_id_be> -> AlkaneInfo`

Value:
- `AlkaneInfo { creation_txid:[32], creation_height:u32, creation_timestamp:u32 }`

Compatible: Yes (key-based)

### /alkanes/name/ and /alkanes/symbol/

Key patterns:
- `/alkanes/name/<normalized_name>/<alkane_id_be> -> empty`
- `/alkanes/symbol/<normalized_symbol>/<alkane_id_be> -> empty`

Compatible: Yes (list-based via scan)

Refactor note:
- Optional: add counts per prefix if you need fast totals.

### /orbitals/collection/name/

Key pattern:
- `/orbitals/collection/name/<factory_id_be> -> <base_name_utf8>`

Compatible: Yes (key-based)

## Creation Record Indices

### /alkanes/creation/id/

Key pattern:
- `/alkanes/creation/id/<alkane_id_be> -> AlkaneCreationRecord`

Compatible: Yes (key-based), with one size caveat

Notes:
- `AlkaneCreationRecord` includes `inspection` and `names/symbols` vectors. If inspections grow large, consider splitting inspection into separate per-field keys or chunked storage.

### /alkanes/creation/ordered/

Key pattern:
- `/alkanes/creation/ordered/<timestamp:u32be><height:u32be><tx_index:u32be><alkane_id_be> -> AlkaneCreationRecord`

Compatible: Mostly yes (list-based ordered index), with a duplication caveat

Why it may be suboptimal:
- It stores the full record again as the list value.

Integration plan (optional optimization):
1. Store only a pointer (empty value) in the ordered index.

```text
/alkanes/creation/ordered/... -> empty
```

2. Keep the record only at `/alkanes/creation/id/<alkane_id>`.
3. Readers: scan ordered keys, parse alkane_id from the key tail, then multi_get records by id.
4. Reindex: mandatory. If you adopt the pointer-only optimization, write the ordered index values as empty during reindex.

### /alkanes/creation/count

Key pattern:
- `/alkanes/creation/count -> u64 (le)`

Compatible: Yes (key-based)

## Supply Time Series

### /circulating_supply/v1/ and /total_minted/v1/

Key patterns:
- `/circulating_supply/v1/<alkane_id_be><height:u32be> -> u128`
- `/circulating_supply/latest/<alkane_id_be> -> u128`
- `/total_minted/v1/<alkane_id_be><height:u32be> -> u128`
- `/total_minted/latest/<alkane_id_be> -> u128`

Compatible: Yes (key-based per (alkane,height) with scan-friendly prefix for series)

Notes:
- Height is big-endian in the key, so prefix scans are naturally ordered by height.

## Transaction Summary and Reverse Indexes

### /alkane_tx_summary/ (txid -> AlkaneTxSummary)

Key pattern:
- `/alkane_tx_summary/<txid_32> -> AlkaneTxSummary`

Value:
- `AlkaneTxSummary { traces: Vec<...>, outflows: Vec<...>, ... }` (large, nested vectors and maps)

Compatible: No

Violations:
- Large aggregate payload with vectors and maps.

Integration plan:
1. Store a minimal per-tx header.

```text
tx/<txid>/height -> u32
tx/<txid>/trace_count -> u32
```

2. Store traces as a list index (if you must persist them).

```text
tx/<txid>/trace/<idx:u32be> -> <trace_bytes>
tx/<txid>/trace/length -> u32
```

3. Store outflows as per-token keys.

```text
tx/<txid>/outflow/<alkane_id> -> i128 (or fixed SignedU128 encoding)
```

4. Update readers: replace single `get(tx_summary)` with a few direct gets plus optional scans.
5. Reindex: mandatory. Do not preserve `/alkane_tx_summary/*`; write the normalized per-tx keys during reindex (or compute on demand and do not persist at all).

### /alkane_block/ (height -> list of txids)

Key patterns:
- `/alkane_block/<height:u64be>/<idx:u64be> -> <txid_32>`
- `/alkane_block/<height:u64be>/length -> u64 (le)`

Compatible: Yes (list-based)

Notes:
- This already matches the target abstraction closely.
- Optional: use `u128` for `length` to match the proposed convention.

### /alkane_addr/ (address -> list of txids)

Key patterns:
- `/alkane_addr/<address>/<idx:u64be> -> <txid_32>`
- `/alkane_addr/<address>/length -> u64 (le)`

Compatible: Yes (list-based)

### /alkane_latest_traces (global latest txids)

Key pattern:
- `/alkane_latest_traces -> Vec<[u8;32]>` (currently capped to ~20)

Compatible: No (vector payload)

Integration plan:
1. Replace with list-based keys.

```text
/alkane_latest_traces/<idx:u32be> -> <txid_32>
/alkane_latest_traces/length -> u32
```

2. Writer: maintain as a fixed-size ring or a shift list. If you want O(1) updates, store a `head` pointer and use modulo indexing.

## Block Summary

### /block_summary/

Key pattern:
- `/block_summary/<height:u32be> -> BlockSummary`

Value:
- `BlockSummary { trace_count:u32, header: Vec<u8> }` (header is 80 bytes in practice)

Compatible: Yes (key-based), with a small schema hygiene note

Refactor note (optional):
- Change `header: Vec<u8>` to `[u8;80]` in schema to make size fixed and avoid “vec” semantics.
