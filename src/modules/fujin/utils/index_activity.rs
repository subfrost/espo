use crate::alkanes::trace::{
    EspoSandshrewLikeTraceReturnData, EspoSandshrewLikeTraceStatus,
};
use crate::schemas::SchemaAlkaneId;

use super::super::schemas::{FujinActivityKind, SchemaFujinActivityV1};
use super::detect::FujinInvocation;

/// Build an activity record from a matched invocation and its return.
pub fn build_activity(
    inv: &FujinInvocation,
    ret: &EspoSandshrewLikeTraceReturnData,
    kind: FujinActivityKind,
    pool_id: SchemaAlkaneId,
    epoch: u128,
    txid: [u8; 32],
    block_ts: u64,
    address_spk: Vec<u8>,
) -> SchemaFujinActivityV1 {
    let success = ret.status == EspoSandshrewLikeTraceStatus::Success;

    // Extract deltas from incoming alkanes
    let diesel_id = SchemaAlkaneId { block: 2, tx: 0 };
    let mut diesel_delta: u128 = 0;
    let long_delta: u128 = 0;
    let short_delta: u128 = 0;
    let mut lp_delta: u128 = 0;

    for (id, amount) in &inv.incoming_alkanes {
        if *id == diesel_id {
            diesel_delta = diesel_delta.saturating_add(*amount);
        } else if id == &pool_id {
            // LP tokens have the same ID as the pool
            lp_delta = lp_delta.saturating_add(*amount);
        }
        // We can't easily distinguish long/short from just incoming without knowing their IDs,
        // but for the activity record, what matters most is diesel and LP.
        // For specific pool invocations, the incoming tokens that aren't diesel or LP are long/short.
    }

    // For operations like MintPair/BurnPair, diesel_delta is the key metric.
    // For AddLiquidity, incoming long+short tokens are the metrics.
    // We store what we can extract from the invocation context.

    SchemaFujinActivityV1 {
        timestamp: block_ts,
        txid,
        kind,
        pool_id,
        epoch,
        long_delta,
        short_delta,
        diesel_delta,
        lp_delta,
        address_spk,
        success,
    }
}
