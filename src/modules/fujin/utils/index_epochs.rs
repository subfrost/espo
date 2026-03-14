use crate::alkanes::trace::EspoSandshrewLikeTraceReturnData;
use crate::schemas::SchemaAlkaneId;

use super::super::schemas::SchemaEpochInfo;

/// Parse the 112-byte InitEpoch response data to extract pool, long, short IDs and epoch.
///
/// Layout (all u128 LE):
///   [0..16]   pool block
///   [16..32]  pool tx
///   [32..48]  long block
///   [48..64]  long tx
///   [64..80]  short block
///   [80..96]  short tx
///   [96..112] epoch
pub fn parse_init_epoch_response(
    ret: &EspoSandshrewLikeTraceReturnData,
    height: u32,
    block_ts: u64,
) -> Option<SchemaEpochInfo> {
    let hex_data = &ret.response.data;
    let bytes = decode_hex_data(hex_data)?;
    if bytes.len() < 112 {
        return None;
    }

    let pool_block = u128_le(&bytes[0..16])? as u32;
    let pool_tx = u128_le(&bytes[16..32])? as u64;
    let long_block = u128_le(&bytes[32..48])? as u32;
    let long_tx = u128_le(&bytes[48..64])? as u64;
    let short_block = u128_le(&bytes[64..80])? as u32;
    let short_tx = u128_le(&bytes[80..96])? as u64;
    let epoch = u128_le(&bytes[96..112])?;

    Some(SchemaEpochInfo {
        epoch,
        pool_id: SchemaAlkaneId { block: pool_block, tx: pool_tx },
        long_id: SchemaAlkaneId { block: long_block, tx: long_tx },
        short_id: SchemaAlkaneId { block: short_block, tx: short_tx },
        creation_height: height,
        creation_ts: block_ts,
    })
}

fn u128_le(bytes: &[u8]) -> Option<u128> {
    if bytes.len() < 16 {
        return None;
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes[..16]);
    Some(u128::from_le_bytes(arr))
}

fn decode_hex_data(s: &str) -> Option<Vec<u8>> {
    let s = s.strip_prefix("0x").unwrap_or(s);
    if s.is_empty() {
        return Some(Vec::new());
    }
    hex::decode(s).ok()
}
