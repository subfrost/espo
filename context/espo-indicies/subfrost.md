# subfrost indices

DB namespace prefix: `subfrost:`

Key patterns below are **relative** (they do not include the `subfrost:` namespace prefix).

Assumption: this refactor will ship with a mandatory full reindex. No backwards compatibility, in-place migrations, or mixed-schema reads are required.

## /index_height

Key pattern: `/index_height`

Value: `u32` (little-endian, 4 bytes)

Compatible: Yes (key-based)

## Wrap/Unwrap Event Logs

These are append-only event lists keyed by `(timestamp, sequence)` and are read via prefix scans or reverse-iteration.

### /wrap_events_all/v1/

Key pattern:
- `/wrap_events_all/v1/<ts:u64be><seq:u32be> -> SchemaWrapEventV1`

Value:
- `SchemaWrapEventV1 { timestamp, txid:[32], amount:u128, address_spk: Vec<u8>, success: bool }` (borsh)

Compatible: Yes (list-based)

Optional improvements:
- Add `/wrap_events_all/v1/length -> u128` if you want O(1) counts.

### /unwrap_events_all/v1/

Key pattern:
- `/unwrap_events_all/v1/<ts:u64be><seq:u32be> -> SchemaWrapEventV1`

Compatible: Yes (list-based)

### /wrap_events_by_address/v1/

Key pattern:
- `/wrap_events_by_address/v1/<spk_len:u16be><spk><ts:u64be><seq:u32be> -> SchemaWrapEventV1`

Compatible: Yes (list-based)

### /unwrap_events_by_address/v1/

Key pattern:
- `/unwrap_events_by_address/v1/<spk_len:u16be><spk><ts:u64be><seq:u32be> -> SchemaWrapEventV1`

Compatible: Yes (list-based)

Notes:
- Using the scriptPubKey bytes as the address identity keeps keys canonical and avoids address-format ambiguities.

## Unwrap Totals

### /unwrap_total_latest/v1 and /unwrap_total_latest_success/v1

Key patterns:
- `/unwrap_total_latest/v1 -> u128be`
- `/unwrap_total_latest_success/v1 -> u128be`

Compatible: Yes (key-based)

### /unwrap_total_by_height/v1/

Key patterns:
- `/unwrap_total_by_height/v1/<height:u32be> -> u128be`
- `/unwrap_total_by_height_success/v1/<height:u32be> -> u128be`

Compatible: Yes (key-based series)
