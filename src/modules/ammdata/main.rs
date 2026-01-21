use super::schemas::{
    ActivityDirection, ActivityKind, SchemaActivityV1, SchemaCanonicalPoolEntry,
    SchemaCandleV1, SchemaFullCandleV1, SchemaMarketDefs, SchemaPoolSnapshot, SchemaTokenMetricsV1,
    Timeframe,
    active_timeframes,
};
use super::storage::{
    AmmDataProvider, GetIterPrefixRevParams, GetRawValueParams, SetBatchParams, decode_candle_v1,
    decode_canonical_pools, decode_full_candle_v1, decode_reserves_snapshot, encode_candle_v1,
    encode_canonical_pools, encode_reserves_snapshot, encode_token_metrics,
};
use super::utils::activity::{ActivityIndexAcc, ActivityWriteAcc};
use crate::alkanes::trace::EspoBlock;
use crate::config::{get_espo_db, get_network};
use crate::modules::ammdata::config::AmmDataConfig;
use crate::modules::ammdata::consts::{
    CanonicalQuoteUnit, PRICE_SCALE, ammdata_genesis_block, canonical_quotes, get_amm_contract,
};
use crate::modules::ammdata::price_feeds::{PriceFeed, UniswapPriceFeed};
use crate::modules::ammdata::utils::candles::{
    CandleCache, bucket_start_for, price_base_per_quote, price_quote_per_base,
};
use crate::modules::ammdata::utils::reserves::{
    NewPoolInfo, extract_new_pools_from_espo_transaction,
};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    AlkaneBalanceTxEntry, GetCreationRecordParams,
    GetRawValueParams as EssentialsGetRawValueParams, EssentialsProvider,
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
    essentials: &EssentialsProvider,
) -> Result<Option<SchemaAlkaneId>> {
    let base = match get_amm_contract(network) {
        Ok(id) => id,
        Err(_) => return Ok(None),
    };

    if network != Network::Bitcoin {
        return Ok(Some(base));
    }

    let table = essentials.table();
    let lookup = |key| -> Result<Option<SchemaAlkaneId>> {
        if let Some(raw) = essentials
            .get_raw_value(EssentialsGetRawValueParams {
                key: table.kv_row_key(&base, key),
            })?
            .value
        {
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

    let inspection = essentials
        .get_creation_record(GetCreationRecordParams { alkane: base })?
        .record
        .and_then(|rec| rec.inspection);
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
    essentials: &EssentialsProvider,
    height: u32,
) -> Result<BTreeMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>> {
    let table = essentials.table();
    let key = table.alkane_balance_txs_by_height_key(height);
    let Some(bytes) = essentials
        .get_raw_value(EssentialsGetRawValueParams { key })?
        .value
    else {
        return Ok(BTreeMap::new());
    };
    let parsed = BTreeMap::<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>::try_from_slice(&bytes)
        .map_err(|e| anyhow!("failed to decode balance txs by height: {e}"))?;
    Ok(parsed)
}

pub struct AmmData {
    provider: Option<Arc<AmmDataProvider>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
}

impl AmmData {
    pub fn new() -> Self {
        Self {
            provider: None,
            index_height: Arc::new(std::sync::RwLock::new(None)),
        }
    }

    #[inline]
    fn provider(&self) -> &AmmDataProvider {
        self.provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    fn load_index_height(&self) -> Result<Option<u32>> {
        let resp = self.provider().get_index_height(super::storage::GetIndexHeightParams)?;
        Ok(resp.height)
    }

    fn persist_index_height(&self, height: u32) -> Result<()> {
        self.provider()
            .set_index_height(super::storage::SetIndexHeightParams { height })
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
        let essentials_mdb = Mdb::from_db(get_espo_db(), b"essentials:");
        let essentials_provider = Arc::new(EssentialsProvider::new(Arc::new(essentials_mdb)));
        self.provider = Some(Arc::new(AmmDataProvider::new(mdb.clone(), essentials_provider)));
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

        let provider = self.provider();
        let essentials = provider.essentials();
        let table = provider.table();

        // ---- Load existing snapshot (single read) ----
        let mut reserves_snapshot: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
            if let Some(bytes) = provider
                .get_raw_value(GetRawValueParams {
                    key: table.reserves_snapshot_key(),
                })?
                .value
            {
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
        let canonical_quotes_list = canonical_quotes(network);
        let mut canonical_quote_units: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for cq in canonical_quotes_list.iter() {
            canonical_quote_units.insert(cq.id, cq.unit);
        }
        let base_factory = get_amm_contract(network).ok();
        let factory_target = match resolve_factory_target(network, essentials) {
            Ok(target) => target,
            Err(e) => {
                eprintln!("[AMMDATA] failed to resolve factory target: {e:?}");
                None
            }
        };

        let mut candle_cache = CandleCache::new();
        let frames = active_timeframes();
        let mut activity_acc = ActivityWriteAcc::new();
        let mut index_acc = ActivityIndexAcc::new();
        let mut canonical_pool_updates: HashMap<SchemaAlkaneId, Vec<SchemaCanonicalPoolEntry>> =
            HashMap::new();
        let mut pool_name_index_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut alkane_label_cache: HashMap<SchemaAlkaneId, String> = HashMap::new();
        let get_alkane_label = |essentials: &EssentialsProvider,
                                cache: &mut HashMap<SchemaAlkaneId, String>,
                                alkane: &SchemaAlkaneId|
         -> String {
            if let Some(label) = cache.get(alkane) {
                return label.clone();
            }
            let label = essentials
                .get_creation_record(GetCreationRecordParams { alkane: *alkane })
                .ok()
                .and_then(|resp| resp.record)
                .and_then(|rec| {
                    rec.symbols
                        .first()
                        .cloned()
                        .or_else(|| rec.names.first().cloned())
                })
                .unwrap_or_else(|| format!("{}:{}", alkane.block, alkane.tx));
            cache.insert(*alkane, label.clone());
            label
        };

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
                            None => match essentials
                                .get_creation_record(GetCreationRecordParams { alkane: pool_id })?
                                .record
                            {
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
                    if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
                        canonical_pool_updates
                            .entry(defs.base_alkane_id)
                            .or_default()
                            .push(SchemaCanonicalPoolEntry {
                                pool_id,
                                quote_id: defs.quote_alkane_id,
                            });
                    }

                    let base_label = get_alkane_label(essentials, &mut alkane_label_cache, &defs.base_alkane_id);
                    let quote_label =
                        get_alkane_label(essentials, &mut alkane_label_cache, &defs.quote_alkane_id);
                    let pool_name = format!("{base_label} / {quote_label}");
                    let pool_name_norm = pool_name.trim().to_ascii_lowercase();
                    if !pool_name_norm.is_empty() {
                        pool_name_index_writes
                            .push((table.pool_name_index_key(&pool_name_norm, &pool_id), Vec::new()));
                    }

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

        let balance_txs = match load_balance_txs_by_height(essentials, height) {
            Ok(m) => m,
            Err(e) => {
                eprintln!("[AMMDATA] failed to load balance txs for height {height}: {e:?}");
                BTreeMap::new()
            }
        };

        let mut has_trades = false;
        let mut canonical_trade_buckets: HashMap<SchemaAlkaneId, HashSet<(Timeframe, u64)>> =
            HashMap::new();

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
                    has_trades = true;
                    let p_q_per_b = price_quote_per_base(new_base, new_quote);
                    let p_b_per_q = price_base_per_quote(new_base, new_quote);
                    let base_in = if base_delta > 0 { base_delta as u128 } else { 0 };
                    let quote_out = if quote_delta < 0 { (-quote_delta) as u128 } else { 0 };

                    candle_cache.apply_trade_for_frames(
                        block_ts,
                        owner,
                        &frames,
                        p_b_per_q,
                        p_q_per_b,
                        base_in,
                        quote_out,
                    );

                    if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
                        let entry = canonical_trade_buckets
                            .entry(defs.base_alkane_id)
                            .or_default();
                        for tf in &frames {
                            entry.insert((*tf, bucket_start_for(block_ts, *tf)));
                        }
                    }
                }
            }
        }

        // ---------- derived data (canonical pools + token USD candles + metrics) ----------
        let btc_usd_price = if has_trades {
            match UniswapPriceFeed::from_global_config() {
                Ok(feed) => Some(feed.get_bitcoin_price_usd_at_block_height(height as u64)),
                Err(e) => {
                    eprintln!("[AMMDATA] btc/usd price_feed failed at height {height}: {e:?}");
                    None
                }
            }
        } else {
            None
        };

        let mut canonical_pools_by_token: HashMap<SchemaAlkaneId, Vec<SchemaCanonicalPoolEntry>> =
            HashMap::new();
        for (pool, defs) in pools_map.iter() {
            if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
                canonical_pools_by_token
                    .entry(defs.base_alkane_id)
                    .or_default()
                    .push(SchemaCanonicalPoolEntry {
                        pool_id: *pool,
                        quote_id: defs.quote_alkane_id,
                    });
            }
        }

        let (candle_writes, candle_entries) = candle_cache.into_writes_with_entries(provider)?;
        let mut pool_candle_overrides: HashMap<(SchemaAlkaneId, Timeframe, u64), SchemaFullCandleV1> =
            HashMap::new();
        for (pool, tf, bucket_ts, candle) in candle_entries {
            pool_candle_overrides.insert((pool, tf, bucket_ts), candle);
        }

        let mut canonical_pool_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for (token, new_entries) in canonical_pool_updates.iter() {
            let key = table.canonical_pool_key(token);
            let mut existing = if let Some(raw) =
                provider.get_raw_value(GetRawValueParams { key: key.clone() })?.value
            {
                decode_canonical_pools(&raw).unwrap_or_default()
            } else {
                Vec::new()
            };
            let mut changed = false;
            for entry in new_entries {
                if !existing.iter().any(|e| e == entry) {
                    existing.push(*entry);
                    changed = true;
                }
            }
            if changed {
                let encoded = encode_canonical_pools(&existing)?;
                canonical_pool_writes.push((key, encoded));
            }
        }

        let mut token_usd_candle_overrides: HashMap<(SchemaAlkaneId, Timeframe, u64), SchemaCandleV1> =
            HashMap::new();
        let mut token_usd_candle_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_metrics_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        if !canonical_trade_buckets.is_empty() {
            let load_pool_candle = |pool: &SchemaAlkaneId,
                                    tf: Timeframe,
                                    bucket_ts: u64,
                                    provider: &AmmDataProvider,
                                    table: &super::storage::AmmDataTable<'_>,
                                    overrides: &HashMap<
                                        (SchemaAlkaneId, Timeframe, u64),
                                        SchemaFullCandleV1,
                                    >|
             -> Result<Option<SchemaFullCandleV1>> {
                if let Some(c) = overrides.get(&(*pool, tf, bucket_ts)) {
                    return Ok(Some(*c));
                }
                let key = table.candle_key(pool, tf, bucket_ts);
                if let Some(raw) = provider.get_raw_value(GetRawValueParams { key })?.value {
                    return Ok(Some(decode_full_candle_v1(&raw)?));
                }
                Ok(None)
            };

            for (token, buckets) in canonical_trade_buckets.iter() {
                let Some(pools) = canonical_pools_by_token.get(token) else { continue };
                for (tf, bucket_ts) in buckets {
                    let mut btc_candle: Option<SchemaCandleV1> = None;
                    let mut usd_candle: Option<SchemaCandleV1> = None;

                    for entry in pools.iter() {
                        let Some(unit) = canonical_quote_units.get(&entry.quote_id) else { continue };
                        let Some(pool_candle) = load_pool_candle(
                            &entry.pool_id,
                            *tf,
                            *bucket_ts,
                            provider,
                            &table,
                            &pool_candle_overrides,
                        )? else {
                            continue;
                        };

                        let quote_volume = pool_candle.quote_candle.volume;
                        let conv = |p: u128| -> Option<u128> {
                            match unit {
                                CanonicalQuoteUnit::Usd => Some(p),
                                CanonicalQuoteUnit::Btc => btc_usd_price
                                    .map(|btc| p.saturating_mul(btc) / PRICE_SCALE),
                            }
                        };
                        let conv_vol = |v: u128| -> Option<u128> {
                            match unit {
                                CanonicalQuoteUnit::Usd => Some(v),
                                CanonicalQuoteUnit::Btc => btc_usd_price
                                    .map(|btc| v.saturating_mul(btc) / PRICE_SCALE),
                            }
                        };

                        let Some(open) = conv(pool_candle.base_candle.open) else { continue };
                        let Some(high) = conv(pool_candle.base_candle.high) else { continue };
                        let Some(low) = conv(pool_candle.base_candle.low) else { continue };
                        let Some(close) = conv(pool_candle.base_candle.close) else { continue };
                        let Some(volume) = conv_vol(quote_volume) else { continue };

                        let converted = SchemaCandleV1 { open, high, low, close, volume };
                        match unit {
                            CanonicalQuoteUnit::Usd => {
                                usd_candle = Some(converted);
                            }
                            CanonicalQuoteUnit::Btc => {
                                btc_candle = Some(converted);
                            }
                        }
                    }

                    let mut derived = match (btc_candle, usd_candle) {
                        (Some(btc), Some(usd)) => SchemaCandleV1 {
                            open: 0,
                            high: (btc.high.saturating_add(usd.high)) / 2,
                            low: (btc.low.saturating_add(usd.low)) / 2,
                            close: (btc.close.saturating_add(usd.close)) / 2,
                            volume: btc.volume.saturating_add(usd.volume),
                        },
                        (Some(one), None) | (None, Some(one)) => SchemaCandleV1 {
                            open: 0,
                            high: one.high,
                            low: one.low,
                            close: one.close,
                            volume: one.volume,
                        },
                        _ => continue,
                    };

                    let existing = if let Some(c) =
                        token_usd_candle_overrides.get(&(*token, *tf, *bucket_ts))
                    {
                        Some(*c)
                    } else {
                        let key = table.token_usd_candle_key(token, *tf, *bucket_ts);
                        if let Some(raw) = provider.get_raw_value(GetRawValueParams { key })?.value
                        {
                            Some(decode_candle_v1(&raw)?)
                        } else {
                            None
                        }
                    };

                    let open = if let Some(prev) = existing {
                        prev.open
                    } else {
                        let prev_bucket = bucket_ts
                            .checked_sub(tf.duration_secs())
                            .unwrap_or(*bucket_ts);
                        if let Some(c) =
                            token_usd_candle_overrides.get(&(*token, *tf, prev_bucket))
                        {
                            c.close
                        } else {
                            let key = table.token_usd_candle_key(token, *tf, prev_bucket);
                            provider
                                .get_raw_value(GetRawValueParams { key })?
                                .value
                                .and_then(|raw| decode_candle_v1(&raw).ok())
                                .map(|c| c.close)
                                .unwrap_or(0)
                        }
                    };
                    derived.open = open;
                    if derived.open > derived.high {
                        derived.high = derived.open;
                    }
                    if derived.open < derived.low {
                        derived.low = derived.open;
                    }

                    token_usd_candle_overrides.insert((*token, *tf, *bucket_ts), derived);
                }
            }

            for ((token, tf, bucket_ts), candle) in token_usd_candle_overrides.iter() {
                let key = table.token_usd_candle_key(token, *tf, *bucket_ts);
                let encoded = encode_candle_v1(candle)?;
                token_usd_candle_writes.push((key, encoded));
            }

            for token in canonical_trade_buckets.keys() {
                let prefix = table.token_usd_candle_ns_prefix(token, Timeframe::M10);
                let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
                for (k, v) in provider
                    .get_iter_prefix_rev(GetIterPrefixRevParams { prefix: prefix.clone() })?
                    .entries
                {
                    if let Some(ts_bytes) = k.rsplit(|&b| b == b':').next() {
                        if let Ok(ts_str) = std::str::from_utf8(ts_bytes) {
                            if let Ok(ts) = ts_str.parse::<u64>() {
                                if !per_bucket.contains_key(&ts) {
                                    if let Ok(c) = decode_candle_v1(&v) {
                                        per_bucket.insert(ts, c);
                                    }
                                }
                            }
                        }
                    }
                }

                for ((tok, tf, bucket), candle) in token_usd_candle_overrides.iter() {
                    if tok == token && *tf == Timeframe::M10 {
                        per_bucket.insert(*bucket, *candle);
                    }
                }

                let now_bucket = bucket_start_for(block_ts, Timeframe::M10);
                let earliest_bucket = per_bucket.keys().next().copied().unwrap_or(now_bucket);

                let close_at = |target_bucket: u64| -> u128 {
                    if per_bucket.is_empty() {
                        return 0;
                    }
                    if target_bucket <= earliest_bucket {
                        return per_bucket
                            .get(&earliest_bucket)
                            .map(|c| c.close)
                            .unwrap_or(0);
                    }
                    let mut bts = earliest_bucket;
                    let mut last_close = 0u128;
                    while bts <= target_bucket {
                        if let Some(c) = per_bucket.get(&bts) {
                            last_close = c.close;
                        }
                        bts = match bts.checked_add(Timeframe::M10.duration_secs()) {
                            Some(n) => n,
                            None => break,
                        };
                    }
                    last_close
                };

                let latest_close = close_at(now_bucket);
                let first_close = per_bucket
                    .get(&earliest_bucket)
                    .map(|c| c.close)
                    .unwrap_or(0);

                let window_close = |secs: u64| -> u128 {
                    let target = now_bucket.saturating_sub(secs);
                    close_at(target)
                };

                let percent_change = |prev: u128, now: u128| -> String {
                    if prev == 0 {
                        return "0".to_string();
                    }
                    let prev_f = prev as f64;
                    let now_f = now as f64;
                    let pct = (now_f - prev_f) / prev_f * 100.0;
                    format!("{:.4}", pct)
                };

                let volume_window = |secs: u64| -> u128 {
                    let start = now_bucket.saturating_sub(secs);
                    per_bucket
                        .range(start..=now_bucket)
                        .map(|(_, c)| c.volume)
                        .sum()
                };

                let volume_all_time: u128 = per_bucket.values().map(|c| c.volume).sum();

                let supply = {
                    let table_e = essentials.table();
                    let key = table_e.circulating_supply_latest_key(token);
                    essentials
                        .get_raw_value(EssentialsGetRawValueParams { key })?
                        .value
                        .and_then(|v| crate::modules::essentials::storage::decode_u128_value(&v).ok())
                        .unwrap_or(0)
                };

                let price_usd = latest_close;
                let fdv_usd = price_usd.saturating_mul(supply) / PRICE_SCALE;
                let marketcap_usd = fdv_usd;

                let metrics = SchemaTokenMetricsV1 {
                    price_usd,
                    fdv_usd,
                    marketcap_usd,
                    volume_all_time,
                    volume_1d: volume_window(24 * 60 * 60),
                    volume_7d: volume_window(7 * 24 * 60 * 60),
                    volume_30d: volume_window(30 * 24 * 60 * 60),
                    change_1d: percent_change(window_close(24 * 60 * 60), latest_close),
                    change_7d: percent_change(window_close(7 * 24 * 60 * 60), latest_close),
                    change_30d: percent_change(window_close(30 * 24 * 60 * 60), latest_close),
                    change_all_time: percent_change(first_close, latest_close),
                };

                let encoded = encode_token_metrics(&metrics)?;
                token_metrics_writes.push((table.token_metrics_key(token), encoded));
            }
        }

        // ---------- one atomic DB write (candles + activity + indexes + reserves snapshot) ----------
        let activity_writes = activity_acc.into_writes();
        let idx_delta = index_acc.clone().per_pool_delta();
        let idx_group_delta = index_acc.clone().per_pool_group_delta();
        let mut index_writes = index_acc.into_writes();

        // Update per-pool index counts
        for ((blk_id, tx_id), delta) in idx_delta {
            let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
            let count_k_rel = crate::modules::ammdata::utils::activity::idx_count_key(&pool);

            let current = if let Some(v) = provider
                .get_raw_value(GetRawValueParams { key: count_k_rel.clone() })?
                .value
            {
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

            let current = if let Some(v) = provider
                .get_raw_value(GetRawValueParams { key: count_k_rel.clone() })?
                .value
            {
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
        let reserves_key_rel = table.reserves_snapshot_key();

        let c_cnt = candle_writes.len();
        let tc_cnt = token_usd_candle_writes.len();
        let tm_cnt = token_metrics_writes.len();
        let cp_cnt = canonical_pool_writes.len();
        let pn_cnt = pool_name_index_writes.len();
        let a_cnt = activity_writes.len();
        let i_cnt = index_writes.len();

        eprintln!(
            "[AMMDATA] block #{h} prepare writes: candles={c_cnt}, token_usd_candles={tc_cnt}, token_metrics={tm_cnt}, canonical_pools={cp_cnt}, pool_name_index={pn_cnt}, activity={a_cnt}, indexes+counts={i_cnt}, reserves_snapshot=1",
            h = block.height,
            c_cnt = c_cnt,
            tc_cnt = tc_cnt,
            tm_cnt = tm_cnt,
            cp_cnt = cp_cnt,
            pn_cnt = pn_cnt,
            a_cnt = a_cnt,
            i_cnt = i_cnt,
        );

        if !candle_writes.is_empty()
            || !token_usd_candle_writes.is_empty()
            || !token_metrics_writes.is_empty()
            || !canonical_pool_writes.is_empty()
            || !pool_name_index_writes.is_empty()
            || !activity_writes.is_empty()
            || !index_writes.is_empty()
            || !reserves_blob.is_empty()
        {
            let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            puts.extend(candle_writes);
            puts.extend(token_usd_candle_writes);
            puts.extend(token_metrics_writes);
            puts.extend(canonical_pool_writes);
            puts.extend(pool_name_index_writes);
            puts.extend(activity_writes);
            puts.extend(index_writes);
            puts.push((reserves_key_rel, reserves_blob));

            let _ = provider.set_batch(SetBatchParams {
                puts,
                deletes: Vec::new(),
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
        let provider = self
            .provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .clone();
        register_rpc(reg, provider);
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some(AmmDataConfig::spec())
    }

    fn set_config(&mut self, config: &serde_json::Value) -> Result<()> {
        AmmDataConfig::from_value(config).map(|_| ())
    }
}
