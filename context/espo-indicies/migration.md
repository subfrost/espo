Here’s a more digestible agent prompt. I kept your intent, but tightened wording, removed repetition, and made the deliverables unambiguous.

---

# Espo DB Migration Spec (Agent Instructions)

This is a large migration for **Espo**. This document has two sections:

1. **Explanation** of the migration and required constraints
2. **Questions** you (the agent) should ask after reviewing the indices in `context/espo-indicies`

Your job is to read all indices for each module (see `context/espo-indicies`, and the real definitions in each module’s `storage.rs`) and make sure every index can be represented under the new umbrella model.

---

## EXPLANATION

### Why we’re doing this

Espo currently does not handle reorgs well (see `src/runtime/aof.rs`).

Today Espo stores only the **latest state**. When a reorg occurs, we “undo” the changed keys using an AOF/undo log. This is:

- expensive to store (so we only keep ~100 blocks)
- expensive to replay
- causes RPC downtime during reindex/revert

### Goal

Make Espo an **append-only, versioned database**, where each block corresponds to a new immutable version of state. This removes rollback logic entirely.

Reorg handling becomes:

```
active_root = block/{new_tip}
```

No undo. No replay. No downtime.

---

## EXPLANATION DB PRE-MIGRATION (must be completed first)

Before changing the underlying database implementation, we must standardize the way indices are defined and accessed.

### Required: two core pointer traits

You must implement two traits and route all storage through them:

#### 1) `KeyValuePointer`

For scalar / direct K/V data.

Example shape:

```
balance/{addr}/{asset} -> u128
metadata/{id} -> struct
```

#### 2) `ListBasedPointer`

For any index that is list-like, enumerable, or would otherwise require storing grouped/bulk data.

Example shape:

```
holders_chunk/{asset}/{chunk_id} -> [address x N]
holders_length/{asset} -> u64
```

### Rules

- Every index must be represented via **either** `KeyValuePointer` or `ListBasedPointer`.
- `KeyValuePointer` must **not** store “grouped” / bulk data:
  - no `Vec<T>`, no “big blob” values, no “all holders in one key”, etc.

- Anything list-like must be routed through `ListBasedPointer`.

**Your task in the pre-migration phase** is to migrate every index (from `storage.rs`) to one of these pointers and flag any index that does not cleanly fit.

Do not worry yet about how the pointers are implemented internally — just make sure every index can be expressed through these two interfaces and follows the rules above.

---

## EXPLANATION DB MIGRATION (how pointers will behave)

### Core principle

Espo will become a **persistent key–value map**:

- all writes are append-only
- nothing is overwritten
- each block produces a new `state_root`
- reads at any block are resolved from that block’s root

### Version model

For each block:

```
block/{blockhash} -> state_root
height/{height} -> blockhash
```

Internally this is stored as a content-addressed persistent structure (Merkle/HAMT-like):

- nodes are immutable
- updating a key creates new nodes only along its path
- unchanged branches are reused
- storage grows roughly as:

```
keys_changed_per_block × tree_depth
```

---

## Single umbrella schema

Everything is a logical key in the versioned map:

```
logical_key -> value
```

No separate storage types.

Different value shapes are allowed (still under the same umbrella):

- scalar values
- chunk blobs (for lists)

---

## List indices strategy (chunking)

Prefix scans over a historically-versioned keyspace return duplicates across versions, so “raw prefix scan” is not compatible with historical reads.

To preserve fast enumeration, list indices must be stored in fixed-size chunks:

```
{list}_chunk/{namespace}/{chunk_id} -> [item x N]
{list}_length/{namespace} -> u64
```

Where:

```
chunk_id = index / CHUNK_SIZE
offset   = index % CHUNK_SIZE
```

Properties:

- enumeration cost: `O(total_items / CHUNK_SIZE)`
- updates rewrite only affected chunks
- chunk size target: ~4–16KB per value (often 100–300 items)

Use this for:

- holders
- addresses
- candles
- time-series
- pagination-driven enumerations

---

## Read semantics + reorg behavior

- RPC should always serve from the best-known active chain tip (as reported by bitcoind).
- During reindex after a reorg, RPC can continue serving from the previous tip’s root until the new chain catches up.
- If there are competing blockhashes at the same height, the active one is the one on the chain reported by bitcoind.

To read at a block:

```
root = block/{blockhash}
lookup(logical_key, root)
```

Bulk reads should use chunked list indices to avoid huge numbers of point lookups.

---

## What you must produce (agent output)

### For each module in `context/espo-indicies`

Create an MD file that lists each index and includes:

1. **Which pointer** it maps to:
   - `KeyValuePointer` or `ListBasedPointer`

2. **Compatibility check**
   - Compatible with the new rules? (Yes/No/Unclear)

3. **If not compatible**
   - What rule it violates (bulk blob, prefix-scan dependency, overwrite semantics, etc.)
   - A concrete redesign plan under the new pointer system (including chunk key layout if list-like)

---

# QUESTIONS (agent must include after index review)

After reviewing all indices, add a “Questions” section containing anything you need clarified to implement the migration correctly. Examples:

- Does this list require stable ordering across reorgs, or is “best effort ordering” acceptable?
- What is the expected churn for this list per block?
- Is historical access required for this index, or only “latest state”?
- Maximum expected size (holders count, candles count, etc.)?
- Any RPC endpoints that require scanning entire datasets frequently?

## Questions (from current index review)

- Must every current scan-based index be converted to chunk lists, even when it already stores one logical row per key (for example, sorted index keys with empty values)?
  --> If its strictly just an index, handle on a case by case basis (I think you mean). Like if its not too large maybe you can just store on the key.

- Should `CHUNK_SIZE` be one global constant, or configurable per index family (candles, holders, activity, address history)?
  ---> make it confugrable through the global config.json

- What ordering guarantees are required for list results: strict deterministic ordering across reindex/reorg, or best-effort ordering?
  ---> strict deterministic

- For score-sorted lists, what is the required tie-break rule (for example secondary sort by `SchemaAlkaneId`)?
  ----> I dont understand what u mean by this

- Do all indices need full historical reads by `blockhash`, or can some remain latest-only?
  ----> all

- Do all RPC endpoints need historical reads, or only a subset?
  ----> all (but optional, and also on oylapi no need for historical reads)

- Is full reindex with no mixed-schema backward reads an explicit requirement for this rollout?
  ----> FULL REINDEX, NO BACKWARDS COMPATIBVILITY

- Should we retain roots for orphaned branches indefinitely, or is there a pruning policy?
  ----> No prunign policy (for now)

- During reorg catch-up, should writes continue while reads stay pinned to prior tip, or should indexing pause writes until catch-up?
  -----> Reads should continue while writes catchup to build the active chain

- For `essentials` `KV_ROWS`, what maximum value size is acceptable before mandatory chunking?
  ----> CHUNK SIZE

- For `essentials` outpoints, should canonical identity be fixed as `txid(32)+vout(u32)` with spentness always in value fields (not key variants)?
  ----> I dont understand what u mean

- Must `address/*` aggregates and `outpoint/*` balances be strongly consistent within the same block version (single atomic update contract)?
  ----> Yes

- For holder identity, is `HolderId::Address` vs `HolderId::Alkane` the final canonical model, and how should equal-balance ties be ordered?
  -----> I dont understand, do it how its currently done

- Should zero balances be deleted (sparse model) or retained as explicit zero-value keys?
  -----> retained as zero value (append only db). The rpc acts as if they dont exist tho.

- For `transfer_volume` and `total_received`, is latest-state only sufficient, or is per-height historical access required?
  -----> historical access required (you already get this automatically via the new db structure)

IF YOU HAVE ANY MORE QUESTIONS, USE THE ASK HUMAN TOOL.
