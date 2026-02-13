# ESPO Index Size Audit

This report measures **active logical index footprint** from the current active trie root and compares it to raw on-disk RocksDB usage.

- DB path: `db/espo`
- Index docs path: `context/espo-indicies`
- Active root: `a1913dfadebb052d64225597792ffbe2b59ad61c5342ad8a03c3f6b6c736146f`
- DB directory size: `51522358433` bytes (47.9839 GiB)

## Raw RocksDB Breakdown

| Family | Entries | Key GiB | Value GiB | Total GiB |
|---|---:|---:|---:|---:|
| `__espo_tree:node` | 42876128 | 1.9566 | 46.7387 | 48.6953 |
| `__espo_tree:block` | 10517 | 0.0005 | 0.0003 | 0.0008 |
| `__espo_tree:height` | 10517 | 0.0002 | 0.0003 | 0.0005 |
| `__espo_tree:meta` | 2 | 0.0000 | 0.0000 | 0.0000 |
| **TOTAL** | 42897164 | 1.9574 | 46.7393 | 48.6967 |

## Active Logical Index Breakdown

`Total GiB` here is logical key+value bytes for entries reachable from the active root (not historical node history).

| Index | Entries | Key GiB | Value GiB | Total GiB |
|---|---:|---:|---:|---:|
| `essentials:__unmatched` | 1564188 | 0.1074 | 0.1017 | 0.2091 |
| `essentials:/alkane_addr/` | 362563 | 0.0292 | 0.0106 | 0.0398 |
| `essentials:/alkane_block/` | 155327 | 0.0061 | 0.0044 | 0.0105 |
| `essentials:/block_summary/` | 10517 | 0.0003 | 0.0009 | 0.0012 |
| `essentials:/circulating_supply/v1/` | 9010 | 0.0004 | 0.0001 | 0.0006 |
| `essentials:/total_minted/v1/` | 8867 | 0.0004 | 0.0001 | 0.0005 |
| `essentials:/addr_spk/` | 3688 | 0.0003 | 0.0001 | 0.0004 |
| `essentials:/alkanes/creation/ordered/` | 68 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/alkanes/creation/id/` | 68 | 0.0000 | 0.0000 | 0.0000 |
| `ammdata:/token_search_index/v1/` | 198 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/circulating_supply/latest/` | 58 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/alkanes/holders/ordered/` | 68 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/alkanes/name/` | 66 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/alkanes/symbol/` | 64 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/total_minted/latest/` | 35 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/alkane_latest_traces` | 21 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/alkanes/creation/count` | 1 | 0.0000 | 0.0000 | 0.0000 |
| `essentials:/index_height` | 1 | 0.0000 | 0.0000 | 0.0000 |
| `pizzafun:/index_height` | 1 | 0.0000 | 0.0000 | 0.0000 |
| `subfrost:/index_height` | 1 | 0.0000 | 0.0000 | 0.0000 |
| **TOTAL (active logical)** | 2114810 | 0.1440 | 0.1180 | 0.2621 |

## Auto Bucket Breakdown (For Unmatched/Unknown Keys)

These buckets are derived from real keys and help pinpoint large key families missing from the docs-driven pattern map.

| Auto Bucket | Entries | Key GiB | Value GiB | Total GiB |
|---|---:|---:|---:|---:|
| `essentials:/tx/` | 442338 | 0.0251 | 0.0602 | 0.0853 |
| `essentials:/outpoint/` | 575017 | 0.0391 | 0.0134 | 0.0525 |
| `essentials:/_chunked_list/` | 901 | 0.0000 | 0.0193 | 0.0193 |
| `essentials:/address/` | 158433 | 0.0181 | 0.0005 | 0.0186 |
| `essentials:/alkane/` | 153458 | 0.0109 | 0.0047 | 0.0156 |
| `essentials:[0x01]` | 107306 | 0.0067 | 0.0033 | 0.0100 |
| `essentials:[0x03]` | 107306 | 0.0067 | 0.0000 | 0.0067 |
| `essentials:/balance_changes/` | 19429 | 0.0007 | 0.0003 | 0.0010 |

## Amplification Snapshot

- Unique live trie nodes (reachable from active root): `931650`
- Live trie node KV bytes (unique reachable nodes): `245977130` bytes (0.2291 GiB)
- Historical node amplification (`all __espo_tree:node` / live reachable node bytes): `212.57x`
- Physical-to-active-logical amplification (`db dir size` / `active logical bytes`): `183.11x`

