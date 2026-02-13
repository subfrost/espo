# ESPO DB Growth Findings (2026-02-13)

This file summarizes findings from `docs/index-size-audit.md`, generated with:

```bash
cargo run --bin index_size_audit -- --db db/espo --index-docs context/espo-indicies --out-md docs/index-size-audit.md --top 120
```

## Executive Summary

- DB folder size is already **47.98 GiB** at ~10.5k indexed blocks.
- Raw storage is almost entirely `__espo_tree:node`:
  - `__espo_tree:node`: **48.70 GiB** (`42,876,128` entries)
- Active logical keyspace is much smaller:
  - Active logical KV total: **0.2621 GiB** (`2,114,810` entries)
- Amplification is extreme:
  - Historical node amplification (`all tree nodes` / `live reachable nodes`): **212.57x**
  - Physical-to-active-logical amplification: **183.11x**

Conclusion: growth is not one giant value/index. The main issue is **versioned trie write amplification** from many index writes per block, plus list-chunk rewrite patterns.

## Largest Active Logical Index Families

From active logical data (key+value GiB):

1. `essentials:/tx/` -> **0.0853 GiB**
2. `essentials:/outpoint/` -> **0.0525 GiB**
3. `essentials:/alkane_addr/` -> **0.0398 GiB**
4. `essentials:/_chunked_list/` -> **0.0193 GiB**
5. `essentials:/address/` -> **0.0186 GiB**
6. `essentials:/alkane/` -> **0.0156 GiB**
7. `essentials:[0x01]` (KV rows) -> **0.0100 GiB**
8. `essentials:[0x03]` (DIR rows) -> **0.0067 GiB**

## What To Optimize

## 1) Stop Rewriting Chunk Values (High impact, near-term)

Current `/_chunked_list/v1/` behavior rewrites chunk values repeatedly:
- append path rewrites the current tail chunk
- delete path can rebuild full family chunks

In a persistent/versioned trie, each rewrite produces new nodes and leaves old versions in DB, causing rapid growth.

Recommended:
- move list metadata to append-friendly keys (avoid rewriting existing chunk payloads)
- for non-append lists (ex: ordered holders), apply delta updates per key instead of rebuild/rewrite-all

## 2) Reduce Write Volume in Heavy Families (High impact)

Top families by live footprint and write activity are `/tx/`, `/outpoint/`, `/address/`, `/alkane/`.

Recommended:
- keep only indices required by hot RPC paths
- gate expensive trace-level persistence (full `/tx/.../trace/*`) behind a config flag
- collapse duplicate or low-value reverse indices where possible

## 3) Shorten Key Layouts (Medium impact)

Current keys are verbose ASCII path strings. In a trie, key length amplifies node payload size and cloned-path cost.

Recommended:
- use compact binary prefixes and fixed-width encoded ids for high-churn families
- especially for `/tx/v2/*`, `/outpoint/v2/*`, `/address/v2/*`, `/alkane/v2/*`

## 4) Tune `chunk_size` Upward as a Fast Mitigation (Medium impact, immediate)

Default chunk size is `256` (`src/config.rs`).

Larger chunks reduce frequency of tail-chunk rewrites in current list implementation.

Recommended quick test:
- run with `chunk_size` = `1024` or `2048` in staging
- compare DB growth per 1k blocks before/after

## 5) Add Historical Node Pruning Strategy (High impact, longer-term)

Right now all historical trie nodes are retained indefinitely.

Recommended:
- keep full roots for reorg window + periodic checkpoints
- garbage-collect nodes unreachable from retained roots

Without pruning, disk will continue growing superlinearly vs active logical state.

## Suggested Next Measurements

1. Re-run the audit every 1k blocks and diff `docs/index-size-audit.md`.
2. Add per-block metrics:
   - logical writes per family
   - bytes written per family
   - number of chunk rewrites per family
3. A/B test:
   - current behavior
   - larger `chunk_size`
   - no tx trace persistence
   and compare GiB growth slope.
