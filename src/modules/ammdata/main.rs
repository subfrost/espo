use super::schemas::{
    ActivityDirection, ActivityKind, SchemaActivityV1, SchemaMarketDefs, SchemaPoolSnapshot,
    active_timeframes,
};
use super::storage::{decode_reserves_snapshot, encode_reserves_snapshot, reserves_snapshot_key};
use super::utils::activity::{ActivityIndexAcc, ActivityWriteAcc};
use super::utils::candles::CandleCache;
use crate::alkanes::trace::EspoBlock;
use crate::config::{get_espo_db, get_network};
use crate::modules::ammdata::consts::{ammdata_genesis_block, get_amm_contract};
use crate::modules::ammdata::utils::candles::{price_base_per_quote, price_quote_per_base};
use crate::modules::ammdata::utils::reserves::{
    NewPoolInfo, extract_new_pools_from_espo_transaction,
};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    AlkaneBalanceTxEntry, alkane_balance_txs_by_height_key, load_creation_record,
};
use crate::modules::essentials::utils::balances::SignedU128;
use crate::modules::essentials::utils::inspections::StoredInspectionResult;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use bitcoin::hashes::Hash;
use borsh::BorshDeserialize;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;

use super::rpc::register_rpc;

/* ---------- module ---------- */

const KV_KEY_IMPLEMENTATION: &[u8] = b"/implementation";
const KV_KEY_BEACON: &[u8] = b"/beacon";
const UPGRADEABLE_METHODS: [(&str, u128); 2] = [("initialize", 32767), ("forward", 36863)];

fn is_upgradeable_proxy(inspection: &StoredInspectionResult) -> bool {
    let Some(meta) = inspection.metadata.as_ref() else { return false };
    UPGRADEABLE_METHODS.iter().all(|(name, opcode)| {
        meta.methods.iter().any(|m| m.name.eq_ignore_ascii_case(name) && m.opcode == *opcode)
    })
}

fn kv_row_key(alk: &SchemaAlkaneId, skey: &[u8]) -> Vec<u8> {
    let mut v = Vec::with_capacity(1 + 4 + 8 + 2 + skey.len());
    v.push(0x01);
    v.extend_from_slice(&alk.block.to_be_bytes());
    v.extend_from_slice(&alk.tx.to_be_bytes());
    let len = u16::try_from(skey.len()).unwrap_or(u16::MAX);
    v.extend_from_slice(&len.to_be_bytes());
    if len as usize != skey.len() {
        v.extend_from_slice(&skey[..(len as usize)]);
    } else {
        v.extend_from_slice(skey);
    }
    v
}

fn decode_kv_implementation(raw: &[u8]) -> Option<SchemaAlkaneId> {
    if raw.len() < 32 {
        return None;
    }
    let block_bytes: [u8; 16] = raw[0..16].try_into().ok()?;
    let tx_bytes: [u8; 16] = raw[16..32].try_into().ok()?;
    let block = u128::from_le_bytes(block_bytes);
    let tx = u128::from_le_bytes(tx_bytes);
    if block > u32::MAX as u128 || tx > u64::MAX as u128 {
        return None;
    }
    Some(SchemaAlkaneId { block: block as u32, tx: tx as u64 })
}

fn resolve_factory_target(
    network: Network,
    essentials_mdb: &Mdb,
) -> Result<Option<SchemaAlkaneId>> {
    let base = match get_amm_contract(network) {
        Ok(id) => id,
        Err(_) => return Ok(None),
    };

    if network != Network::Bitcoin {
        return Ok(Some(base));
    }

    let lookup = |key| -> Result<Option<SchemaAlkaneId>> {
        if let Some(raw) = essentials_mdb.get(&kv_row_key(&base, key))? {
            let slice = if raw.len() >= 32 { &raw[32..] } else { raw.as_slice() };
            if let Some(decoded) = decode_kv_implementation(slice) {
                return Ok(Some(decoded));
            }
        }
        Ok(None)
    };
    if let Some(decoded) = lookup(KV_KEY_IMPLEMENTATION)? {
        return Ok(Some(decoded));
    }
    if let Some(decoded) = lookup(KV_KEY_BEACON)? {
        return Ok(Some(decoded));
    }

    let inspection = load_creation_record(essentials_mdb, &base)?.and_then(|rec| rec.inspection);
    if let Some(inspection) = inspection.as_ref() {
        if is_upgradeable_proxy(inspection) {
            return Ok(Some(base));
        }
    }

    Ok(Some(base))
}

fn signed_from_delta(delta: Option<&SignedU128>) -> i128 {
    let Some(d) = delta else { return 0 };
    let (neg, amt) = d.as_parts();
    if neg { -(amt as i128) } else { amt as i128 }
}

fn apply_delta_u128(current: u128, delta: i128) -> u128 {
    if delta >= 0 {
        current.saturating_add(delta as u128)
    } else {
        current.saturating_sub((-delta) as u128)
    }
}

fn load_balance_txs_by_height(
    essentials_mdb: &Mdb,
    height: u32,
) -> Result<BTreeMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>> {
    let key = alkane_balance_txs_by_height_key(height);
    let Some(bytes) = essentials_mdb.get(&key)? else { return Ok(BTreeMap::new()) };
    let parsed = BTreeMap::<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>::try_from_slice(&bytes)
        .map_err(|e| anyhow!("failed to decode balance txs by height: {e}"))?;
    Ok(parsed)
}

pub struct AmmData {
    mdb: Option<Arc<Mdb>>,
    essentials_mdb: Option<Arc<Mdb>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
}

impl AmmData {
    pub fn new() -> Self {
        Self {
            mdb: None,
            essentials_mdb: None,
            index_height: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    #[inline]
    fn mdb(&self) -> &Mdb {
        self.mdb.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
    }

    #[inline]
    fn essentials_mdb(&self) -> &Mdb {
        self.essentials_mdb.as_ref().expect("Essentials DB must be initialized").as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        if let Some(bytes) = self.mdb().get(AmmData::get_key_index_height())? {
            if bytes.len() != 4 {
                return Err(anyhow!("invalid /index_height length {}", bytes.len()));
            }
            let mut arr = [0u8; 4];
            arr.copy_from_slice(&bytes);
            Ok(Some(u32::from_le_bytes(arr)))
        } else {
            Ok(None)
        }
    }

    fn persist_index_height(&self, height: u32) -> Result<()> {
        self.mdb()
            .put(AmmData::get_key_index_height(), &height.to_le_bytes())
            .map_err(|e| anyhow!("[AMMDATA] rocksdb put(/index_height) failed: {e}"))
    }

    fn set_index_height(&self, new_height: u32) -> Result<()> {
        if let Some(prev) = *self.index_height.read().unwrap() {
            if new_height < prev {
                eprintln!("[AMMDATA] index height rollback detected ({} -> {})", prev, new_height);
            }
        }
        self.persist_index_height(new_height)?;
        *self.index_height.write().unwrap() = Some(new_height);
        Ok(())
    }
}

impl Default for AmmData {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for AmmData {
    fn get_name(&self) -> &'static str {
        "ammdata"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        self.mdb = Some(mdb.clone());
        let essentials_mdb = Mdb::from_db(get_espo_db(), b"essentials:");
        self.essentials_mdb = Some(Arc::new(essentials_mdb));
        match self.load_index_height() {
            Ok(h) => {
                *self.index_height.write().unwrap() = h;
                eprintln!("[AMMDATA] loaded index height: {:?}", h);
            }
            Err(e) => eprintln!("[AMMDATA] failed to load /index_height: {e:?}"),
        }
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        ammdata_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let block_ts = block.block_header.time as u64;
        let height = block.height;
        println!("[AMMDATA] Indexing block #{height} for candles and activity...");

        let essentials_mdb = self.essentials_mdb();

        // ---- Load existing snapshot (single read) ----
        let mut reserves_snapshot: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
            if let Some(bytes) = self.mdb().get(reserves_snapshot_key())? {
                match decode_reserves_snapshot(&bytes) {
                    Ok(m) => m,
                    Err(e) => {
                        eprintln!("[AMMDATA] WARNING: failed to decode reserves snapshot: {e:?}");
                        HashMap::new()
                    }
                }
            } else {
                HashMap::new()
            };

        // Build pools map the extractors expect
        let mut pools_map: HashMap<SchemaAlkaneId, SchemaMarketDefs> = HashMap::new();
        for (pool, snap) in reserves_snapshot.iter() {
            pools_map.insert(
                *pool,
                SchemaMarketDefs {
                    pool_alkane_id: *pool,
                    base_alkane_id: snap.base_id,
                    quote_alkane_id: snap.quote_id,
                },
            );
        }

        let network = get_network();
        let base_factory = get_amm_contract(network).ok();
        let factory_target = match resolve_factory_target(network, essentials_mdb) {
            Ok(target) => target,
            Err(e) => {
                eprintln!("[AMMDATA] failed to resolve factory target: {e:?}");
                None
            }
        };

        let mut candle_cache = CandleCache::new();
        let mut activity_acc = ActivityWriteAcc::new();
        let mut index_acc = ActivityIndexAcc::new();

        // Discover new pools (per-tx) and record pool creation activity.
        let mut seen_new_pools: HashSet<(u32, u64)> = HashSet::new();
        for transaction in block.transactions.iter() {
            if transaction.traces.is_none() {
                continue;
            }

            if let Ok(new_pools) = extract_new_pools_from_espo_transaction(transaction) {
                for NewPoolInfo { pool_id, defs, factory_id } in new_pools {
                    if !seen_new_pools.insert((pool_id.block, pool_id.tx)) {
                        continue;
                    }

                    let should_track = match factory_target {
                        Some(factory) => match factory_id {
                            Some(fid) => {
                                let matches_base = base_factory.map(|b| b == fid).unwrap_or(false);
                                fid == factory || matches_base
                            }
                            None => match load_creation_record(essentials_mdb, &pool_id)? {
                                Some(rec) => rec
                                    .inspection
                                    .and_then(|i| i.factory_alkane)
                                    .map(|id| id == factory)
                                    .unwrap_or(true),
                                None => true,
                            },
                        },
                        None => {
                            if let Some(fid) = factory_id {
                                base_factory.map(|b| b == fid).unwrap_or(true)
                            } else {
                                true
                            }
                        }
                    };

                    if !should_track {
                        continue;
                    }

                    pools_map.insert(pool_id, defs);
                    reserves_snapshot.entry(pool_id).or_insert(SchemaPoolSnapshot {
                        base_reserve: 0,
                        quote_reserve: 0,
                        base_id: defs.base_alkane_id,
                        quote_id: defs.quote_alkane_id,
                    });

                    let txid_bytes = transaction.transaction.compute_txid().to_byte_array();

                    let activity = SchemaActivityV1 {
                        timestamp: block_ts,
                        txid: txid_bytes,
                        kind: ActivityKind::PoolCreate,
                        direction: None,
                        base_delta: 0,
                        quote_delta: 0,
                    };

                    if let Ok(seq) = activity_acc.push(pool_id, block_ts, activity.clone()) {
                        index_acc.add(&pool_id, block_ts, seq, &activity);
                    }

                    println!(
                        "[AMMDATA] New pool created @ block #{blk}, ts={ts}\n\
                         [AMMDATA]   Pool:  {pb}:{pt}\n\
                         [AMMDATA]   Base:  {bb}:{bt}\n\
                         [AMMDATA]   Quote: {qb}:{qt}",
                        blk = block.height,
                        ts = block_ts,
                        pb = pool_id.block,
                        pt = pool_id.tx,
                        bb = defs.base_alkane_id.block,
                        bt = defs.base_alkane_id.tx,
                        qb = defs.quote_alkane_id.block,
                        qt = defs.quote_alkane_id.tx
                    );
                }
            }
        }

        let balance_txs = match load_balance_txs_by_height(essentials_mdb, height) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[AMMDATA] failed to load balance txs for height {height}: {e:?}");
                BTreeMap::new()
            }
        };

        // Apply balance deltas per pool and emit activity + candles.
        for (owner, entries) in balance_txs {
            let Some(defs) = pools_map.get(&owner) else { continue };
            let Some(snapshot) = reserves_snapshot.get_mut(&owner) else { continue };

            for entry in entries {
                let base_delta = signed_from_delta(entry.outflow.get(&defs.base_alkane_id));
                let quote_delta = signed_from_delta(entry.outflow.get(&defs.quote_alkane_id));
                if base_delta == 0 && quote_delta == 0 {
                    continue;
                }

                let prev_base = snapshot.base_reserve;
                let prev_quote = snapshot.quote_reserve;
                let new_base = apply_delta_u128(prev_base, base_delta);
                let new_quote = apply_delta_u128(prev_quote, quote_delta);
                snapshot.base_reserve = new_base;
                snapshot.quote_reserve = new_quote;

                let (kind, direction) = match (base_delta.signum(), quote_delta.signum()) {
                    (1, -1) => (ActivityKind::TradeSell, Some(ActivityDirection::BaseIn)),
                    (-1, 1) => (ActivityKind::TradeBuy, Some(ActivityDirection::QuoteIn)),
                    (1, 1) => (ActivityKind::LiquidityAdd, None),
                    (-1, -1) => (ActivityKind::LiquidityRemove, None),
                    _ => continue,
                };

                let activity = SchemaActivityV1 {
                    timestamp: block_ts,
                    txid: entry.txid,
                    kind,
                    direction,
                    base_delta,
                    quote_delta,
                };

                if let Ok(seq) = activity_acc.push(owner, block_ts, activity.clone()) {
                    index_acc.add(&owner, block_ts, seq, &activity);
                }

                if matches!(kind, ActivityKind::TradeBuy | ActivityKind::TradeSell) {
                    let p_q_per_b = price_quote_per_base(new_base, new_quote);
                    let p_b_per_q = price_base_per_quote(new_base, new_quote);
                    let base_in = if base_delta > 0 { base_delta as u128 } else { 0 };
                    let quote_out = if quote_delta < 0 { (-quote_delta) as u128 } else { 0 };

                    candle_cache.apply_trade_for_frames(
                        block_ts,
                        owner,
                        &active_timeframes(),
                        p_b_per_q,
                        p_q_per_b,
                        base_in,
                        quote_out,
                    );
                }
            }
        }

        // ---------- one atomic DB write (candles + activity + indexes + reserves snapshot) ----------
        let candle_writes = candle_cache.into_writes(self.mdb())?;
        let activity_writes = activity_acc.into_writes();
        let idx_delta = index_acc.clone().per_pool_delta();
        let idx_group_delta = index_acc.clone().per_pool_group_delta();
        let mut index_writes = index_acc.into_writes();

        // Update per-pool index counts
        for ((blk_id, tx_id), delta) in idx_delta {
            let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
            let count_k_rel = crate::modules::ammdata::utils::activity::idx_count_key(&pool);

            let current = if let Some(v) = self.mdb().get(&count_k_rel)? {
                crate::modules::ammdata::utils::activity::decode_u64_be(&v).unwrap_or(0)
            } else {
                0u64
            };
            let newv = current.saturating_add(delta);

            index_writes.push((
                count_k_rel,
                crate::modules::ammdata::utils::activity::encode_u64_be(newv).to_vec(),
            ));
        }
        for ((blk_id, tx_id, group), delta) in idx_group_delta {
            let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
            let count_k_rel =
                crate::modules::ammdata::utils::activity::idx_count_key_group(&pool, group);

            let current = if let Some(v) = self.mdb().get(&count_k_rel)? {
                crate::modules::ammdata::utils::activity::decode_u64_be(&v).unwrap_or(0)
            } else {
                0u64
            };
            let newv = current.saturating_add(delta);

            index_writes.push((
                count_k_rel,
                crate::modules::ammdata::utils::activity::encode_u64_be(newv).to_vec(),
            ));
        }

        let reserves_blob = encode_reserves_snapshot(&reserves_snapshot)?;
        let reserves_key_rel = reserves_snapshot_key();

        let c_cnt = candle_writes.len();
        let a_cnt = activity_writes.len();
        let i_cnt = index_writes.len();

        eprintln!(
            "[AMMDATA] block #{h} prepare writes: candles={c_cnt}, activity={a_cnt}, indexes+counts={i_cnt}, reserves_snapshot=1",
            h = block.height,
            c_cnt = c_cnt,
            a_cnt = a_cnt,
            i_cnt = i_cnt,
        );

        if !candle_writes.is_empty()
            || !activity_writes.is_empty()
            || !index_writes.is_empty()
            || !reserves_blob.is_empty()
        {
            let db_prefix = self.mdb().prefix().to_vec(); // strip if candles were FULL
            let _ = self.mdb().bulk_write(|wb| {
                for (k_full_or_rel, v) in candle_writes.iter() {
                    let k_rel = if k_full_or_rel.starts_with(&db_prefix) {
                        &k_full_or_rel[db_prefix.len()..]
                    } else {
                        k_full_or_rel.as_slice()
                    };
                    wb.put(k_rel, v);
                }
                for (k_rel, v) in activity_writes.iter() {
                    wb.put(k_rel, v);
                }
                for (k_rel, v) in index_writes.iter() {
                    wb.put(k_rel, v);
                }
                wb.put(reserves_key_rel, &reserves_blob);
            });
        }

        println!(
            "[AMMDATA] Finished processing block #{} with {} traces",
            block.height,
            block.transactions.len()
        );
        self.set_index_height(block.height)?;
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let mdb = self.mdb().clone();
        register_rpc(reg, mdb);
    }
}
