use super::config::FujinConfig;
use super::consts::{fujin_genesis_block, PRICE_SCALE};
use super::rpc::register_rpc;
use super::schemas::*;
use super::storage::{
    FujinProvider, GetIndexHeightParams, SetBatchParams, SetIndexHeightParams,
};
use super::utils::detect::{
    classify_activity, FujinContracts, FujinInvocation, FujinTarget,
};
use super::utils::index_activity::build_activity;
use super::utils::index_epochs::parse_init_epoch_response;
use super::utils::index_settlement::detect_settlement;
use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceStatus,
};
use crate::config::{debug_enabled, get_espo_db};
use crate::debug;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::EssentialsProvider;
use crate::modules::essentials::utils::balances::clean_espo_sandshrew_like_trace;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::hashes::Hash as _;
use bitcoin::Network;
use std::collections::HashMap;
use std::sync::Arc;

pub struct Fujin {
    provider: Option<Arc<FujinProvider>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
}

impl Fujin {
    pub fn new() -> Self {
        Self {
            provider: None,
            index_height: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    #[inline]
    fn provider(&self) -> &FujinProvider {
        self.provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        let resp = self.provider().get_index_height(GetIndexHeightParams)?;
        Ok(resp.height)
    }

    fn persist_index_height(&self, height: u32) -> Result<()> {
        self.provider()
            .set_index_height(SetIndexHeightParams { height })
            .map_err(|e| anyhow!("[FUJIN] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_height: u32) -> Result<()> {
        if let Some(prev) = *self.index_height.read().unwrap() {
            if new_height < prev {
                eprintln!(
                    "[FUJIN] index height rollback detected ({} -> {})",
                    prev, new_height
                );
            }
        }
        self.persist_index_height(new_height)?;
        *self.index_height.write().unwrap() = Some(new_height);
        Ok(())
    }
}

impl Default for Fujin {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for Fujin {
    fn get_name(&self) -> &'static str {
        "fujin"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let essentials_mdb = Mdb::from_db(get_espo_db(), b"essentials:");
        let essentials_provider = Arc::new(EssentialsProvider::new(Arc::new(essentials_mdb)));
        self.provider = Some(Arc::new(FujinProvider::new(mdb, essentials_provider)));
        match self.load_index_height() {
            Ok(h) => {
                *self.index_height.write().unwrap() = h;
                eprintln!("[FUJIN] loaded index height: {:?}", h);
            }
            Err(e) => eprintln!("[FUJIN] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        fujin_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();
        let provider = self.provider();
        let height = block.height;

        if let Some(prev) = *self.index_height.read().unwrap() {
            if height <= prev {
                eprintln!("[FUJIN] skipping already indexed block #{height} (last={prev})");
                return Ok(());
            }
        }

        let timer = debug::start_if(debug);
        let block_ts = block.block_header.time as u64;
        let config = super::config::get_fujin_config();

        // Load current snapshot (or default)
        let mut snapshot = provider.get_snapshot()?.unwrap_or_default();

        // Build the set of known pool IDs from snapshot
        let known_pools: Vec<SchemaAlkaneId> =
            snapshot.epochs.iter().map(|e| e.pool_id).collect();

        let contracts = FujinContracts {
            factory_id: config.factory_id,
            vault_id: config.vault_id,
            zap_id: config.zap_id,
            known_pools,
        };

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut activity_seq: u32 = 0;
        let mut new_epochs: Vec<SchemaEpochInfo> = Vec::new();
        let mut pool_states_updated: HashMap<SchemaAlkaneId, SchemaPoolState> = HashMap::new();
        let mut settlements: Vec<SchemaSettlementV1> = Vec::new();
        debug::log_elapsed(module, "init_context", timer);

        let timer = debug::start_if(debug);

        // Process each transaction's traces
        for tx in &block.transactions {
            let txid_hash = tx.transaction.compute_txid();
            let txid_bytes: [u8; 32] = txid_hash.to_byte_array();
            let Some(traces) = &tx.traces else { continue };

            let mut address_spk_bytes: Option<Vec<u8>> = None;

            for trace in traces {
                let Some(cleaned) = clean_espo_sandshrew_like_trace(
                    &trace.sandshrew_trace,
                    &block.host_function_values,
                ) else {
                    continue;
                };

                // Stack-based Invoke/Return matching
                let mut stack: Vec<Option<FujinInvocation>> = Vec::new();

                for ev in &cleaned.events {
                    match ev {
                        EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                            let matched = contracts.match_invoke(inv);
                            stack.push(matched);
                        }
                        EspoSandshrewLikeTraceEvent::Return(ret) => {
                            let Some(pending) = stack.pop().flatten() else {
                                continue;
                            };
                            let success =
                                ret.status == EspoSandshrewLikeTraceStatus::Success;

                            match &pending.target {
                                FujinTarget::Factory if pending.opcode == 1 && success => {
                                    // InitEpoch
                                    if let Some(epoch_info) =
                                        parse_init_epoch_response(ret, height, block_ts)
                                    {
                                        eprintln!(
                                            "[FUJIN] new epoch {} pool={}:{} at height {}",
                                            epoch_info.epoch,
                                            epoch_info.pool_id.block,
                                            epoch_info.pool_id.tx,
                                            height
                                        );
                                        new_epochs.push(epoch_info);
                                    }
                                }
                                target => {
                                    // Activity recording for pool/vault/zap ops
                                    if let Some(kind) = classify_activity(target, pending.opcode) {
                                        let pool_id = match target {
                                            FujinTarget::Pool(id) => *id,
                                            _ => SchemaAlkaneId::default(),
                                        };

                                        // Find epoch for this pool
                                        let epoch = snapshot
                                            .epochs
                                            .iter()
                                            .chain(new_epochs.iter())
                                            .find(|e| e.pool_id == pool_id)
                                            .map(|e| e.epoch)
                                            .unwrap_or(0);

                                        let addr = address_spk_bytes
                                            .get_or_insert_with(|| {
                                                tx_owner_spk(&tx.transaction)
                                                    .map(|s| s.as_bytes().to_vec())
                                                    .unwrap_or_default()
                                            })
                                            .clone();

                                        let activity = build_activity(
                                            &pending, ret, kind, pool_id, epoch,
                                            txid_bytes, block_ts, addr,
                                        );

                                        let table = provider.table();
                                        let bytes = borsh::to_vec(&activity)?;

                                        // Write to pool-specific, global, and address indices
                                        if pool_id != SchemaAlkaneId::default() {
                                            puts.push((
                                                table.activity_pool_key(
                                                    &pool_id,
                                                    block_ts,
                                                    activity_seq,
                                                ),
                                                bytes.clone(),
                                            ));
                                        }
                                        puts.push((
                                            table.activity_all_key(block_ts, activity_seq),
                                            bytes.clone(),
                                        ));
                                        if !activity.address_spk.is_empty() {
                                            puts.push((
                                                table.activity_addr_key(
                                                    &activity.address_spk,
                                                    block_ts,
                                                    activity_seq,
                                                ),
                                                bytes,
                                            ));
                                        }
                                        activity_seq += 1;
                                    }
                                }
                            }
                        }
                        _ => {}
                    }
                }

                // After processing traces, read storage changes for pool state updates
                for (alkane_id, storage_map) in &trace.storage_changes {
                    // Check if this alkane is a known pool (existing or newly registered)
                    let is_pool = snapshot.epochs.iter().any(|e| e.pool_id == *alkane_id)
                        || new_epochs.iter().any(|e| e.pool_id == *alkane_id);
                    if !is_pool {
                        continue;
                    }

                    let pool_id = *alkane_id;
                    let epoch = snapshot
                        .epochs
                        .iter()
                        .chain(new_epochs.iter())
                        .find(|e| e.pool_id == pool_id)
                        .map(|e| e.epoch)
                        .unwrap_or(0);

                    let prev_state = pool_states_updated
                        .get(&pool_id)
                        .cloned()
                        .or_else(|| {
                            snapshot
                                .pool_states
                                .iter()
                                .find(|ps| ps.epoch == epoch)
                                .cloned()
                        });

                    let mut state = prev_state.clone().unwrap_or_else(|| SchemaPoolState {
                        epoch,
                        ..Default::default()
                    });

                    // Read storage values from the trace's storage changes
                    for (key, (_txid, value)) in storage_map {
                        update_pool_state_from_storage(&mut state, key, value, height);
                    }

                    // Compute derived prices
                    let total = state.reserve_long.saturating_add(state.reserve_short);
                    if total > 0 {
                        state.long_price_scaled =
                            state.reserve_short.saturating_mul(PRICE_SCALE) / total;
                        state.short_price_scaled =
                            state.reserve_long.saturating_mul(PRICE_SCALE) / total;
                    }

                    // Blocks remaining
                    if state.end_height > 0 {
                        state.blocks_remaining =
                            state.end_height.saturating_sub(height as u128) as u64;
                    }

                    // Check for settlement transition
                    if let Some(settlement) =
                        detect_settlement(prev_state.as_ref(), &state, pool_id, height)
                    {
                        eprintln!(
                            "[FUJIN] settlement detected epoch={} at height {}",
                            epoch, height
                        );
                        let table = provider.table();
                        puts.push((
                            table.settlement_key(epoch),
                            borsh::to_vec(&settlement)?,
                        ));
                        settlements.push(settlement);
                    }

                    pool_states_updated.insert(pool_id, state);
                }
            }
        }
        debug::log_elapsed(module, "process_traces", timer);

        // ── Update vault state from essentials storage ──
        let timer = debug::start_if(debug);
        if let Ok(Some(vault_bytes)) =
            provider.read_alkane_storage(config.vault_id, b"/lp_balance")
        {
            let lp_balance = read_u128_le(&vault_bytes);
            let total_supply = provider
                .read_alkane_storage(config.vault_id, b"/total_supply")
                .ok()
                .flatten()
                .map(|b| read_u128_le(&b))
                .unwrap_or(0);
            let pool_bytes = provider
                .read_alkane_storage(config.vault_id, b"/pool")
                .ok()
                .flatten();
            let pool_id = pool_bytes
                .as_ref()
                .and_then(|b| read_alkane_id_from_bytes(b))
                .unwrap_or_default();

            let share_price_scaled = if total_supply > 0 {
                lp_balance.saturating_mul(PRICE_SCALE) / total_supply
            } else {
                0
            };

            snapshot.vault_state = SchemaVaultState {
                factory_id: config.factory_id,
                pool_id,
                lp_balance,
                total_supply,
                share_price_scaled,
            };
        }
        debug::log_elapsed(module, "update_vault", timer);

        // ── Persist everything ──
        let timer = debug::start_if(debug);
        let table = provider.table();

        // Add new epochs
        if !new_epochs.is_empty() {
            let mut epoch_list = provider.get_epoch_list().unwrap_or_default();
            for info in &new_epochs {
                epoch_list.push(info.epoch);
                puts.push((table.epoch_key(info.epoch), borsh::to_vec(info)?));
            }
            puts.push((table.EPOCH_LIST.key().to_vec(), borsh::to_vec(&epoch_list)?));
            snapshot.epochs.extend(new_epochs);
        }

        // Update pool states in snapshot
        for (pool_id, state) in &pool_states_updated {
            puts.push((table.pool_state_key(pool_id), borsh::to_vec(state)?));
            // Update snapshot pool_states
            if let Some(existing) = snapshot
                .pool_states
                .iter_mut()
                .find(|ps| ps.epoch == state.epoch)
            {
                *existing = state.clone();
            } else {
                snapshot.pool_states.push(state.clone());
            }
        }

        // Vault state
        puts.push((
            table.VAULT_STATE.key().to_vec(),
            borsh::to_vec(&snapshot.vault_state)?,
        ));

        // Snapshot
        snapshot.last_height = height;
        puts.push((table.SNAPSHOT.key().to_vec(), borsh::to_vec(&snapshot)?));

        if !puts.is_empty() {
            provider.set_batch(SetBatchParams {
                puts,
                deletes: Vec::new(),
            })?;
        }
        debug::log_elapsed(module, "write_batch", timer);

        self.set_index_height(height)?;
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?} (epochs={}, activities={}, settlements={})",
            self.get_name(),
            height,
            t0.elapsed(),
            snapshot.epochs.len(),
            activity_seq,
            settlements.len(),
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        if let Some(provider) = self.provider.as_ref() {
            register_rpc(reg, provider.clone());
        }
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some(FujinConfig::spec())
    }

    fn set_config(&mut self, config: &serde_json::Value) -> Result<()> {
        FujinConfig::from_value(config).map(|_| ())
    }
}

// ── Helpers ──

/// Update pool state fields from a storage key/value pair.
fn update_pool_state_from_storage(
    state: &mut SchemaPoolState,
    key: &[u8],
    value: &[u8],
    _height: u32,
) {
    // The storage keys come as raw bytes matching the contract's StoragePointer keywords
    if key == b"/diesel" {
        state.diesel_locked = read_u128_le(value);
    } else if key == b"/epoch" {
        state.epoch = read_u128_le(value);
    } else if key == b"/totalfeeper1000" {
        state.total_fee_per_1000 = read_u128_le(value);
    } else if key == b"/event/start_bits" {
        if value.len() >= 4 {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&value[..4]);
            state.start_bits = u32::from_le_bytes(arr);
        }
    } else if key == b"/event/end_height" {
        state.end_height = read_u128_le(value);
    } else if key == b"/event/settled" {
        state.settled = read_u128_le(value) != 0;
    } else if key == b"/event/long_payout" {
        state.long_payout_q64 = read_u128_le(value);
    } else if key == b"/event/short_payout" {
        state.short_payout_q64 = read_u128_le(value);
    }
    // Note: reserves (reserve_long, reserve_short) are derived from alkane balances,
    // not direct storage keys. We read them via total supply tracking.
    // lp_total_supply is the pool's own token total supply.
}

fn read_u128_le(bytes: &[u8]) -> u128 {
    if bytes.len() >= 16 {
        let mut arr = [0u8; 16];
        arr.copy_from_slice(&bytes[..16]);
        u128::from_le_bytes(arr)
    } else if !bytes.is_empty() {
        let mut arr = [0u8; 16];
        arr[..bytes.len()].copy_from_slice(bytes);
        u128::from_le_bytes(arr)
    } else {
        0
    }
}

fn read_alkane_id_from_bytes(bytes: &[u8]) -> Option<SchemaAlkaneId> {
    if bytes.len() < 32 {
        return None;
    }
    let block = read_u128_le(&bytes[0..16]) as u32;
    let tx = read_u128_le(&bytes[16..32]) as u64;
    Some(SchemaAlkaneId { block, tx })
}

fn tx_owner_spk(tx: &bitcoin::Transaction) -> Option<bitcoin::ScriptBuf> {
    // Try protostone pointer first
    use ordinals::{Artifact, Runestone};
    use protorune_support::protostone::Protostone;

    if let Some(Artifact::Runestone(ref runestone)) = Runestone::decipher(tx) {
        if let Ok(protos) = Protostone::from_runestone(runestone) {
            for ps in protos {
                if ps.protocol_tag != 1 {
                    continue;
                }
                if let Some(ptr) = ps.pointer {
                    let idx = ptr as usize;
                    if let Some(out) = tx.output.get(idx) {
                        return Some(out.script_pubkey.clone());
                    }
                }
            }
        }
    }

    // Fallback: first non-OP_RETURN output
    tx.output
        .iter()
        .find(|o| !o.script_pubkey.is_op_return())
        .map(|o| o.script_pubkey.clone())
}
