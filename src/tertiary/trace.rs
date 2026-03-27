//! Trace processing — reads AlkanesTrace from secondary storage,
//! classifies events, builds activity indexes.

// Include the generated protobuf code
pub mod proto {
    include!(concat!(env!("OUT_DIR"), "/alkanes.rs"));
}

use super::keys;
use prost::Message;
use qubitcoin_tertiary_support::{secondary_get, set, log};
use std::sync::Arc;

/// Read all trace outpoints for a given block height from alkanes secondary storage.
///
/// Key format: `/trace/<height_8bytes_le>` is a list:
///   `/trace/<height_8bytes_le>/length` → u32 LE count
///   `/trace/<height_8bytes_le>/{index}` → 36-byte outpoint
pub fn get_trace_outpoints_for_height(height: u64) -> Vec<Vec<u8>> {
    let mut base_key = b"/trace/".to_vec();
    base_key.extend_from_slice(&height.to_le_bytes());

    // Read list length
    let mut len_key = base_key.clone();
    len_key.extend_from_slice(b"/length");

    let count = match secondary_get("alkanes", &len_key) {
        Some(bytes) if bytes.len() >= 4 => {
            u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])
        }
        _ => return vec![],
    };

    let mut outpoints = Vec::new();
    for i in 0..count {
        let mut item_key = base_key.clone();
        item_key.extend_from_slice(format!("/{}", i).as_bytes());

        if let Some(outpoint) = secondary_get("alkanes", &item_key) {
            outpoints.push(outpoint);
        }
    }

    outpoints
}

/// Read a single trace for an outpoint from alkanes secondary storage.
///
/// Key: `/trace/<outpoint_36bytes>`
/// Value: protobuf-encoded AlkanesTrace
pub fn read_trace(outpoint: &[u8]) -> Option<proto::AlkanesTrace> {
    let mut key = b"/trace/".to_vec();
    key.extend_from_slice(outpoint);

    let bytes = secondary_get("alkanes", &key)?;
    proto::AlkanesTrace::decode(bytes.as_slice()).ok()
}

/// Compact activity record stored in quspo's own storage.
#[derive(Clone)]
pub struct ActivityRecord {
    pub height: u32,
    pub txid: [u8; 32], // from outpoint
    pub vout: u32,       // from outpoint
    pub target_block: u128,
    pub target_tx: u128,
    pub opcode: u128,
    pub event_kind: u8,
    pub success: bool,
}

/// Event classification
pub const EVENT_UNKNOWN: u8 = 0;
pub const EVENT_SWAP: u8 = 1;
pub const EVENT_ADD_LIQUIDITY: u8 = 2;
pub const EVENT_REMOVE_LIQUIDITY: u8 = 3;
pub const EVENT_POOL_CREATED: u8 = 4;
pub const EVENT_STAKE: u8 = 5;
pub const EVENT_UNSTAKE: u8 = 6;
pub const EVENT_CLAIM: u8 = 7;
pub const EVENT_MINT: u8 = 8;
pub const EVENT_BURN: u8 = 9;
pub const EVENT_DEPOSIT: u8 = 10;
pub const EVENT_WITHDRAW: u8 = 11;
pub const EVENT_FEE_DEPOSIT: u8 = 12;
pub const EVENT_BRIDGE_BURN: u8 = 13;
pub const EVENT_SETTLEMENT: u8 = 14;
pub const EVENT_EPOCH_CREATED: u8 = 15;
pub const EVENT_REDEEM: u8 = 16;
pub const EVENT_MINT_PAIR: u8 = 17;
pub const EVENT_BURN_PAIR: u8 = 18;
pub const EVENT_CREATE: u8 = 19;

impl ActivityRecord {
    /// Serialize to compact binary (fixed 83 bytes)
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(83);
        buf.extend_from_slice(&self.height.to_le_bytes());       // 4
        buf.extend_from_slice(&self.txid);                        // 32
        buf.extend_from_slice(&self.vout.to_le_bytes());          // 4
        buf.extend_from_slice(&self.target_block.to_le_bytes()); // 16
        buf.extend_from_slice(&self.target_tx.to_le_bytes());    // 16
        buf.extend_from_slice(&self.opcode.to_le_bytes());       // 16 (overkill but consistent)
        buf.push(self.event_kind);                                // 1
        buf.push(if self.success { 1 } else { 0 });              // 1
        buf
    }

    /// Deserialize from bytes
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 74 { return None; } // minimum without full opcode
        Some(Self {
            height: u32::from_le_bytes(data[0..4].try_into().ok()?),
            txid: data[4..36].try_into().ok()?,
            vout: u32::from_le_bytes(data[36..40].try_into().ok()?),
            target_block: u128::from_le_bytes(data[40..56].try_into().ok()?),
            target_tx: u128::from_le_bytes(data[56..72].try_into().ok()?),
            opcode: if data.len() >= 88 {
                u128::from_le_bytes(data[72..88].try_into().ok()?)
            } else { 0 },
            event_kind: if data.len() > 88 { data[88] } else { 0 },
            success: if data.len() > 89 { data[89] != 0 } else { false },
        })
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> String {
        let txid_hex = super::views::hex_encode_internal(&self.txid);
        format!(
            r#"{{"height":{},"txid":"{}","vout":{},"target":"{}:{}","opcode":{},"kind":{},"success":{}}}"#,
            self.height, txid_hex, self.vout,
            self.target_block, self.target_tx,
            self.opcode, self.event_kind, self.success
        )
    }
}

/// Process all traces for a block and build activity records.
pub fn process_block_traces(height: u32) -> Vec<ActivityRecord> {
    let outpoints = get_trace_outpoints_for_height(height as u64);
    let mut records = Vec::new();

    for outpoint_bytes in &outpoints {
        if let Some(trace) = read_trace(outpoint_bytes) {
            // Parse outpoint (32-byte txid + 4-byte vout)
            let mut txid = [0u8; 32];
            let mut vout: u32 = 0;
            if outpoint_bytes.len() >= 36 {
                txid.copy_from_slice(&outpoint_bytes[0..32]);
                vout = u32::from_le_bytes(outpoint_bytes[32..36].try_into().unwrap_or([0; 4]));
            }

            // Process trace events — look for enter_context (contract calls)
            let mut last_enter: Option<(u128, u128, u128)> = None; // (target_block, target_tx, opcode)
            let mut success = false;

            for event in &trace.events {
                if let Some(ref ev) = event.event {
                    match ev {
                        proto::alkanes_trace_event::Event::EnterContext(enter) => {
                            if let Some(ref ctx) = enter.context {
                                if let Some(ref inner) = ctx.inner {
                                    if let Some(ref myself) = inner.myself {
                                        let block = myself.block.as_ref().map(|u| u.lo as u128 | ((u.hi as u128) << 64)).unwrap_or(0);
                                        let tx = myself.tx.as_ref().map(|u| u.lo as u128 | ((u.hi as u128) << 64)).unwrap_or(0);
                                        let opcode = inner.inputs.first()
                                            .map(|u| u.lo as u128 | ((u.hi as u128) << 64))
                                            .unwrap_or(0);
                                        last_enter = Some((block, tx, opcode));
                                    }
                                }
                            }
                        }
                        proto::alkanes_trace_event::Event::ExitContext(exit) => {
                            if let Some((target_block, target_tx, opcode)) = last_enter.take() {
                                success = exit.status == proto::AlkanesTraceStatusFlag::Success as i32;

                                // Classify the event
                                let kind = classify_event(target_block, target_tx, opcode);

                                if kind != EVENT_UNKNOWN {
                                    records.push(ActivityRecord {
                                        height,
                                        txid,
                                        vout,
                                        target_block,
                                        target_tx,
                                        opcode,
                                        event_kind: kind,
                                        success,
                                    });
                                }
                            }
                        }
                        proto::alkanes_trace_event::Event::CreateAlkane(create) => {
                            if let Some(ref new_id) = create.new_alkane {
                                let block = new_id.block.as_ref().map(|u| u.lo as u128 | ((u.hi as u128) << 64)).unwrap_or(0);
                                let tx = new_id.tx.as_ref().map(|u| u.lo as u128 | ((u.hi as u128) << 64)).unwrap_or(0);
                                records.push(ActivityRecord {
                                    height, txid, vout,
                                    target_block: block, target_tx: tx,
                                    opcode: 0, event_kind: EVENT_CREATE, success: true,
                                });
                            }
                        }
                        _ => {}
                    }
                }
            }
        }
    }

    records
}

/// Classify an event based on the contract target and opcode.
///
/// This uses a GENERIC approach — it classifies by opcode number since
/// we don't hardcode contract addresses. The UI can further filter by
/// checking which contract was targeted.
fn classify_event(_target_block: u128, _target_tx: u128, opcode: u128) -> u8 {
    // Generic opcode classification (works across AMM, Fujin, FIRE, etc.)
    // This maps common opcode patterns — the UI resolves which protocol
    // by checking the target contract address.
    match opcode {
        0 => EVENT_UNKNOWN,     // Initialize — not interesting for activity
        1 => EVENT_DEPOSIT,     // Most contracts: opcode 1 = primary action (stake, deposit, bond, add liquidity)
        2 => EVENT_WITHDRAW,    // Most contracts: opcode 2 = reverse action (unstake, withdraw, burn)
        3 => EVENT_SWAP,        // AMM/Fujin pools: opcode 3 = swap; FIRE staking: claim
        4 => EVENT_UNKNOWN,     // Various: accept, extend lock, etc.
        5 => EVENT_BRIDGE_BURN, // frUSD: BurnAndBridge
        6 => EVENT_FEE_DEPOSIT, // dxBTC: DepositFees
        11 => EVENT_MINT_PAIR,  // Fujin: MintPair
        12 => EVENT_BURN_PAIR,  // Fujin: BurnPair
        13 => EVENT_SWAP,       // AMM factory: SwapExactTokensForTokens
        14 => EVENT_REDEEM,     // Fujin: Redeem; AMM factory: SwapTokensForExactTokens
        77 => EVENT_MINT,       // DIESEL/frBTC: Mint
        78 => EVENT_BURN,       // frBTC: Unwrap (burn)
        88 => EVENT_BURN,       // FIRE: Burn
        _ => EVENT_UNKNOWN,
    }
}

/// Store activity records in quspo's own storage.
pub fn store_activity_records(records: &[ActivityRecord]) {
    for (seq, record) in records.iter().enumerate() {
        // Global activity index: /activity/all/{height_be4}/{seq_be2}
        let mut global_key = b"/activity/all/".to_vec();
        global_key.extend_from_slice(&record.height.to_be_bytes());
        global_key.push(b'/');
        global_key.extend_from_slice(&(seq as u16).to_be_bytes());
        set(Arc::new(global_key), Arc::new(record.to_bytes()));
    }

    // Update activity count
    let count_key = b"/activity/count".to_vec();
    let current = keys::read_raw_own(&count_key).map(|b| {
        if b.len() >= 4 { u32::from_le_bytes(b[0..4].try_into().unwrap()) } else { 0 }
    }).unwrap_or(0);
    let new_count = current + records.len() as u32;
    set(Arc::new(count_key), Arc::new(new_count.to_le_bytes().to_vec()));
}
