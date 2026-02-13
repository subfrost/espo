# pizzafun indices

DB namespace prefix: `pizzafun:`

Key patterns below are **relative** (they do not include the `pizzafun:` namespace prefix).

Assumption: this refactor will ship with a mandatory full reindex. No backwards compatibility, in-place migrations, or mixed-schema reads are required.

## /index_height

Key pattern: `/index_height`

Value: `u32` (little-endian, 4 bytes)

Compatible: Yes (key-based)

## Series Indexes

These are direct key-based lookups with optional prefix-scan discovery.

### /series/by_id/

Key pattern:
- `/series/by_id/<series_id_norm> -> SeriesEntry`

Value:
- `SeriesEntry { series_id: String, alkane_id: SchemaAlkaneId, creation_height: u32 }` (borsh)

Compatible: Yes (key-based)

Notes:
- Prefix scans over `/series/by_id/<prefix>` are used for name-style lookups.

### /series/by_alkane/

Key pattern:
- `/series/by_alkane/<alkane_id_be> -> SeriesEntry`

Value:
- Same `SeriesEntry` as above.

Compatible: Yes (key-based)

Refactor note (optional):
- If you want an explicit list of all series, add a list index:

```text
/series/all/<idx:u32be> -> <series_id>
/series/all/length -> u128
```
