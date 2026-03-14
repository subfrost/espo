use crate::schemas::SchemaAlkaneId;

use super::super::schemas::{SchemaPoolState, SchemaSettlementV1};

/// Check if a pool has transitioned from unsettled to settled.
/// Returns a settlement record if the transition occurred.
pub fn detect_settlement(
    prev_state: Option<&SchemaPoolState>,
    new_state: &SchemaPoolState,
    pool_id: SchemaAlkaneId,
    height: u32,
) -> Option<SchemaSettlementV1> {
    let was_settled = prev_state.map(|s| s.settled).unwrap_or(false);
    if was_settled || !new_state.settled {
        return None;
    }

    // Compute difficulty change percentage (basis points * 100 for precision)
    let difficulty_change_pct = if new_state.start_bits != 0 && new_state.long_payout_q64 > 0 {
        // If long wins (payout > 0.5 Q64), difficulty went up
        let half_q64 = 1u128 << 63;
        if new_state.long_payout_q64 > half_q64 {
            // Difficulty increased, approximate pct from payout ratio
            let excess = new_state.long_payout_q64.saturating_sub(half_q64);
            (excess as i64 * 10000) / (half_q64 as i64)
        } else {
            let deficit = half_q64.saturating_sub(new_state.long_payout_q64);
            -((deficit as i64 * 10000) / (half_q64 as i64))
        }
    } else {
        0
    };

    Some(SchemaSettlementV1 {
        epoch: new_state.epoch,
        pool_id,
        start_bits: new_state.start_bits,
        end_bits: 0, // We don't have end_bits directly in pool state
        long_payout_q64: new_state.long_payout_q64,
        short_payout_q64: new_state.short_payout_q64,
        settled_height: height,
        difficulty_change_pct,
    })
}
