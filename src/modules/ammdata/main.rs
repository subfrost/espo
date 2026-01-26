use super::schemas::{
    ActivityDirection, ActivityKind, SchemaActivityV1, SchemaCandleV1, SchemaCanonicalPoolEntry,
    SchemaFullCandleV1, SchemaMarketDefs, SchemaPoolCreationInfoV1, SchemaPoolDetailsSnapshot,
    SchemaPoolMetricsV1, SchemaPoolMetricsV2, SchemaPoolSnapshot, SchemaTokenMetricsV1, Timeframe,
    active_timeframes,
};
use super::storage::{
    AmmDataProvider, GetAmmFactoriesParams, GetIterPrefixRevParams, GetRawValueParams,
    GetPoolCreationInfoParams, GetTokenMetricsParams, GetTvlVersionedAtOrBeforeParams,
    SearchIndexField, SetBatchParams, TokenMetricsIndexField, decode_candle_v1,
    decode_canonical_pools, decode_full_candle_v1, decode_reserves_snapshot,
    decode_token_metrics, encode_candle_v1, encode_canonical_pools, encode_pool_creation_info,
    encode_pool_details_snapshot, encode_pool_metrics,
    encode_pool_metrics_v2, encode_reserves_snapshot, encode_token_metrics, encode_u128_value,
    parse_change_basis_points,
};
use super::utils::activity::{ActivityIndexAcc, ActivityWriteAcc};
use crate::alkanes::trace::EspoSandshrewLikeTraceStatus;
use crate::alkanes::trace::{
    EspoBlock, EspoSandshrewLikeTraceEvent, EspoSandshrewLikeTraceInvokeData,
};
use crate::config::{debug_enabled, get_electrum_like, get_espo_db, get_network};
use crate::debug;
use crate::modules::ammdata::config::{AmmDataConfig, DerivedMergeStrategy, DerivedQuoteConfig};
use crate::modules::ammdata::consts::{
    CanonicalQuoteUnit, PRICE_SCALE, ammdata_genesis_block, canonical_quotes,
};
use crate::modules::ammdata::price_feeds::{PriceFeed, UniswapPriceFeed};
use crate::modules::ammdata::utils::candles::{
    CandleCache, bucket_start_for, price_base_per_quote, price_quote_per_base,
};
use crate::modules::ammdata::utils::search::collect_search_prefixes;
use crate::modules::ammdata::utils::reserves::{
    NewPoolInfo, extract_new_pools_from_espo_transaction,
};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    AlkaneBalanceTxEntry, EssentialsProvider, GetAlkaneStorageValueParams,
    GetCreationRecordParams, GetCreationRecordsOrderedParams, GetLatestCirculatingSupplyParams,
    GetRawValueParams as EssentialsGetRawValueParams, spk_to_address_str,
};
use crate::modules::essentials::utils::balances::{
    SignedU128, clean_espo_sandshrew_like_trace, get_alkane_balances,
};
use crate::modules::essentials::utils::inspections::{
    StoredInspectionMetadata, StoredInspectionResult,
};
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::Network;
use bitcoin::consensus::encode::deserialize;
use bitcoin::hashes::Hash;
use bitcoin::{ScriptBuf, Transaction, Txid};
use borsh::BorshDeserialize;
use ordinals::{Artifact, Runestone};
use protorune_support::protostone::Protostone;
use serde_json::json;
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use super::rpc::register_rpc;

/* ---------- module ---------- */

const KV_KEY_IMPLEMENTATION: &[u8] = b"/implementation";
const AMM_FACTORY_OPCODES: [u128; 14] = [0, 1, 2, 3, 4, 7, 10, 11, 12, 13, 14, 21, 29, 50];
const KV_KEY_BEACON: &[u8] = b"/beacon";

fn is_amm_factory_metadata(meta: &StoredInspectionMetadata) -> bool {
    let has_opcodes = AMM_FACTORY_OPCODES
        .iter()
        .all(|opcode| meta.methods.iter().any(|m| m.opcode == *opcode));
    if has_opcodes {
        return true;
    }
    meta.name.eq_ignore_ascii_case("ammfactory")
}

fn inspection_is_amm_factory(inspection: &StoredInspectionResult) -> bool {
    let Some(meta) = inspection.metadata.as_ref() else { return false };
    is_amm_factory_metadata(meta)
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

fn lookup_proxy_target(essentials: &EssentialsProvider, alkane: SchemaAlkaneId) -> Option<SchemaAlkaneId> {
    let lookup = |key: &[u8]| {
        essentials
            .get_alkane_storage_value(GetAlkaneStorageValueParams {
                alkane,
                key: key.to_vec(),
            })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_kv_implementation(&raw))
    };
    lookup(KV_KEY_IMPLEMENTATION).or_else(|| lookup(KV_KEY_BEACON))
}

fn parse_hex_u32(s: &str) -> Option<u32> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(trimmed, 16)
        .ok()
        .and_then(|v| if v > u32::MAX as u128 { None } else { Some(v as u32) })
}

fn parse_hex_u64(s: &str) -> Option<u64> {
    let trimmed = s.strip_prefix("0x").unwrap_or(s);
    u128::from_str_radix(trimmed, 16)
        .ok()
        .and_then(|v| if v > u64::MAX as u128 { None } else { Some(v as u64) })
}

fn parse_hex_u128(s: &str) -> Option<u128> {
    u128::from_str_radix(s.trim_start_matches("0x"), 16).ok()
}

fn merge_candles(
    base: SchemaCandleV1,
    other: SchemaCandleV1,
    strategy: &DerivedMergeStrategy,
) -> SchemaCandleV1 {
    let merge = |a: u128, b: u128| -> u128 {
        match strategy {
            DerivedMergeStrategy::Optimistic => a.max(b),
            DerivedMergeStrategy::Pessimistic => a.min(b),
            DerivedMergeStrategy::Neutral => (a.saturating_add(b)) / 2,
        }
    };
    let volume = if base.volume == 0 { other.volume } else { base.volume };
    SchemaCandleV1 {
        open: merge(base.open, other.open),
        high: merge(base.high, other.high),
        low: merge(base.low, other.low),
        close: merge(base.close, other.close),
        volume,
    }
}

fn invert_price_value(p: u128) -> Option<u128> {
    if p == 0 {
        None
    } else {
        Some(PRICE_SCALE.saturating_mul(PRICE_SCALE) / p)
    }
}

fn parse_factory_create_call(
    inv: &EspoSandshrewLikeTraceInvokeData,
    factories: &HashSet<SchemaAlkaneId>,
) -> Option<SchemaAlkaneId> {
    let inputs = &inv.context.inputs;
    let opcode0 = inputs.get(0).and_then(|s| parse_hex_u128(s));
    let opcode2 = inputs.get(2).and_then(|s| parse_hex_u128(s));
    if opcode0 != Some(1) && opcode2 != Some(1) {
        return None;
    }

    // Legacy layout: [factory_block, factory_tx, opcode, ...]
    if opcode2 == Some(1) {
        if let (Some(block), Some(tx)) = (parse_hex_u32(&inputs[0]), parse_hex_u64(&inputs[1])) {
            let id = SchemaAlkaneId { block, tx };
            if factories.contains(&id) {
                return Some(id);
            }
        }
    }

    // Current layout: [opcode, ...], invoked contract is the factory.
    if let (Some(block), Some(tx)) =
        (parse_hex_u32(&inv.context.myself.block), parse_hex_u64(&inv.context.myself.tx))
    {
        let id = SchemaAlkaneId { block, tx };
        if factories.contains(&id) {
            return Some(id);
        }
    }

    None
}

fn pool_creator_spk_from_protostone(tx: &Transaction) -> Option<ScriptBuf> {
    let runestone = match Runestone::decipher(tx) {
        Some(Artifact::Runestone(r)) => r,
        _ => return None,
    };
    let protos = Protostone::from_runestone(&runestone).ok()?;
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
    None
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

#[inline]
fn scale_price_u128(value: u128) -> f64 {
    (value as f64) / (PRICE_SCALE as f64)
}

#[inline]
fn parse_change_f64(raw: &str) -> f64 {
    raw.parse::<f64>().unwrap_or(0.0)
}

#[inline]
fn pool_name_display(raw: &str) -> String {
    raw.to_ascii_uppercase()
}

fn alkane_id_json(alkane: &SchemaAlkaneId) -> serde_json::Value {
    json!({ "block": alkane.block.to_string(), "tx": alkane.tx.to_string() })
}

fn canonical_quote_amount_tvl_usd(
    amount: u128,
    unit: CanonicalQuoteUnit,
    btc_price_usd: Option<u128>,
) -> Option<u128> {
    match unit {
        CanonicalQuoteUnit::Usd => Some(amount),
        CanonicalQuoteUnit::Btc => {
            let btc_price = btc_price_usd?;
            Some(amount.saturating_mul(btc_price) / PRICE_SCALE)
        }
    }
}

fn load_balance_txs_by_height(
    essentials: &EssentialsProvider,
    height: u32,
) -> Result<BTreeMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>> {
    let table = essentials.table();
    let key = table.alkane_balance_txs_by_height_key(height);
    let Some(bytes) = essentials.get_raw_value(EssentialsGetRawValueParams { key })?.value else {
        return Ok(BTreeMap::new());
    };
    let parsed = BTreeMap::<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>::try_from_slice(&bytes)
        .map_err(|e| anyhow!("failed to decode balance txs by height: {e}"))?;
    Ok(parsed)
}

pub struct AmmData {
    provider: Option<Arc<AmmDataProvider>>,
    index_height: Arc<std::sync::RwLock<Option<u32>>>,
    factories_bootstrapped: AtomicBool,
}

impl AmmData {
    pub fn new() -> Self {
        Self {
            provider: None,
            index_height: Arc::new(std::sync::RwLock::new(None)),
            factories_bootstrapped: AtomicBool::new(false),
        }
    }

    #[inline]
    fn provider(&self) -> &AmmDataProvider {
        self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").as_ref()
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
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();
        let block_ts = block.block_header.time as u64;
        let height = block.height;
        println!("[AMMDATA] Indexing block #{height} for candles and activity...");
        if let Some(prev) = *self.index_height.read().unwrap() {
            if height <= prev {
                eprintln!("[AMMDATA] skipping already indexed block #{height} (last={prev})");
                return Ok(());
            }
        }

        let provider = self.provider();
        let essentials = provider.essentials();
        let table = provider.table();
        let search_cfg = AmmDataConfig::load_from_global_config().ok();
        let search_index_enabled = search_cfg.as_ref().map(|c| c.search_index_enabled).unwrap_or(false);
        let mut search_prefix_min = search_cfg.as_ref().map(|c| c.search_prefix_min_len as usize).unwrap_or(2);
        let mut search_prefix_max = search_cfg.as_ref().map(|c| c.search_prefix_max_len as usize).unwrap_or(6);
        let derived_quotes: Vec<DerivedQuoteConfig> = search_cfg
            .as_ref()
            .and_then(|c| c.derived_liquidity.as_ref())
            .map(|c| c.derived_quotes.clone())
            .unwrap_or_default();
        if search_prefix_min == 0 {
            search_prefix_min = 2;
        }
        if search_prefix_max < search_prefix_min {
            search_prefix_max = search_prefix_min;
        }

        let timer = debug::start_if(debug);
        // ---- Load existing snapshot (single read) ----
        let mut reserves_snapshot: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> =
            if let Some(bytes) = provider
                .get_raw_value(GetRawValueParams { key: table.reserves_snapshot_key() })?
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
        let mut amm_factories: HashSet<SchemaAlkaneId> = provider
            .get_amm_factories(GetAmmFactoriesParams)
            .map(|res| res.factories.into_iter().collect())
            .unwrap_or_default();
        let mut amm_factory_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

        if amm_factories.is_empty()
            && !self.factories_bootstrapped.swap(true, Ordering::Relaxed)
        {
            if let Ok(resp) = essentials.get_creation_records_ordered(GetCreationRecordsOrderedParams)
            {
                let records_len = resp.records.len();
                let mut discovered = 0usize;
                for rec in resp.records {
                    if amm_factories.contains(&rec.alkane) {
                        continue;
                    }
                    let mut is_factory = rec
                        .inspection
                        .as_ref()
                        .map(inspection_is_amm_factory)
                        .unwrap_or(false);
                    if !is_factory {
                        if let Some(proxy_target) = lookup_proxy_target(essentials, rec.alkane) {
                            if let Ok(resp) = essentials
                                .get_creation_record(GetCreationRecordParams { alkane: proxy_target })
                            {
                                if let Some(rec) = resp.record {
                                    if let Some(inspection) = rec.inspection.as_ref() {
                                        if inspection_is_amm_factory(inspection) {
                                            is_factory = true;
                                        }
                                    }
                                }
                            }
                        }
                    }
                    if is_factory {
                        amm_factories.insert(rec.alkane);
                        amm_factory_writes.push((table.amm_factory_key(&rec.alkane), Vec::new()));
                        discovered += 1;
                    }
                }
                eprintln!(
                    "[AMMDATA] factory bootstrap scanned {} creation records, discovered {} factories",
                    records_len,
                    discovered
                );
            }
        }

        let mut created_alkanes: Vec<SchemaAlkaneId> = Vec::new();
        for tx in &block.transactions {
            if let Some(traces) = &tx.traces {
                for trace in traces {
                    for ev in &trace.sandshrew_trace.events {
                        if let EspoSandshrewLikeTraceEvent::Create(c) = ev {
                            if let (Some(block), Some(tx)) =
                                (parse_hex_u32(&c.block), parse_hex_u64(&c.tx))
                            {
                                created_alkanes.push(SchemaAlkaneId { block, tx });
                            }
                        }
                    }
                }
            }
        }

        for alk in created_alkanes {
            if amm_factories.contains(&alk) {
                continue;
            }
            let mut is_factory = false;
            if let Ok(resp) =
                essentials.get_creation_record(GetCreationRecordParams { alkane: alk })
            {
                if let Some(rec) = resp.record {
                    if let Some(inspection) = rec.inspection.as_ref() {
                        if inspection_is_amm_factory(inspection) {
                            is_factory = true;
                        }
                    }
                }
            }
            if !is_factory {
                if let Some(proxy_target) = lookup_proxy_target(essentials, alk) {
                    if let Ok(resp) = essentials
                        .get_creation_record(GetCreationRecordParams { alkane: proxy_target })
                    {
                        if let Some(rec) = resp.record {
                            if let Some(inspection) = rec.inspection.as_ref() {
                                if inspection_is_amm_factory(inspection) {
                                    is_factory = true;
                                }
                            }
                        }
                    }
                }
            }
            if is_factory {
                amm_factories.insert(alk);
                amm_factory_writes.push((table.amm_factory_key(&alk), Vec::new()));
            }
        }

        debug::log_elapsed(module, "load_snapshot_and_factories", timer);
        let timer = debug::start_if(debug);
        let mut candle_cache = CandleCache::new();
        let frames = active_timeframes();
        let mut activity_acc = ActivityWriteAcc::new();
        let mut index_acc = ActivityIndexAcc::new();
        let mut canonical_pool_updates: HashMap<SchemaAlkaneId, Vec<SchemaCanonicalPoolEntry>> =
            HashMap::new();
        let mut pool_name_index_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut factory_pools_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_factory_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_creation_info_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_defs_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_metrics_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_lp_supply_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_details_snapshot_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut tvl_versioned_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_swaps_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut pool_creations_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut address_pool_swaps_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut address_token_swaps_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut address_pool_creations_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut address_pool_mints_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut address_pool_burns_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut address_amm_history_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut amm_history_all_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_pools_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_metrics_cache: HashMap<SchemaAlkaneId, SchemaTokenMetricsV1> = HashMap::new();
        let mut alkane_label_cache: HashMap<SchemaAlkaneId, String> = HashMap::new();
        let mut pool_creation_info_cache: HashMap<SchemaAlkaneId, SchemaPoolCreationInfoV1> =
            HashMap::new();
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
                .and_then(|rec| rec.symbols.first().cloned().or_else(|| rec.names.first().cloned()))
                .unwrap_or_else(|| format!("{}:{}", alkane.block, alkane.tx));
            cache.insert(*alkane, label.clone());
            label
        };

        let mut block_tx_map: HashMap<Txid, &Transaction> = HashMap::new();
        for atx in &block.transactions {
            block_tx_map.insert(atx.transaction.compute_txid(), &atx.transaction);
        }
        let mut prev_tx_cache: HashMap<Txid, Transaction> = HashMap::new();
        let mut tx_meta: HashMap<Txid, (Vec<u8>, bool)> = HashMap::new();
        for atx in &block.transactions {
            let txid = atx.transaction.compute_txid();
            let spk_bytes = pool_creator_spk_from_protostone(&atx.transaction)
                .map(|s| s.as_bytes().to_vec())
                .unwrap_or_default();
            let success = atx.traces.as_ref().map_or(true, |traces| {
                !traces.iter().any(|trace| {
                    trace.sandshrew_trace.events.iter().any(|ev| {
                        matches!(
                            ev,
                            EspoSandshrewLikeTraceEvent::Return(r)
                                if r.status == EspoSandshrewLikeTraceStatus::Failure
                        )
                    })
                })
            });
            tx_meta.insert(txid, (spk_bytes, success));
        }

        debug::log_elapsed(module, "init_accumulators", timer);
        let timer = debug::start_if(debug);
        // Discover new pools (per-tx) and record pool creation activity.
        let mut seen_new_pools: HashSet<(u32, u64)> = HashSet::new();
        for transaction in block.transactions.iter() {
            if transaction.traces.is_none() {
                continue;
            }

            let mut pool_factory_by_id: HashMap<SchemaAlkaneId, SchemaAlkaneId> = HashMap::new();
            if let Some(traces) = &transaction.traces {
                for trace in traces {
                    let Some(cleaned) = clean_espo_sandshrew_like_trace(
                        &trace.sandshrew_trace,
                        &block.host_function_values,
                    ) else {
                        continue;
                    };
                    let mut pending_factory: Option<SchemaAlkaneId> = None;
                    for ev in &cleaned.events {
                        match ev {
                            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                                if let Some(factory) = parse_factory_create_call(inv, &amm_factories)
                                {
                                    pending_factory = Some(factory);
                                }
                            }
                            EspoSandshrewLikeTraceEvent::Create(c) => {
                                if let Some(factory) = pending_factory.take() {
                                    if let (Some(block), Some(tx)) =
                                        (parse_hex_u32(&c.block), parse_hex_u64(&c.tx))
                                    {
                                        pool_factory_by_id
                                            .insert(SchemaAlkaneId { block, tx }, factory);
                                    }
                                }
                            }
                            _ => {}
                        }
                    }
                }
            }

            if let Ok(new_pools) =
                extract_new_pools_from_espo_transaction(transaction, &block.host_function_values)
            {
                for NewPoolInfo { pool_id, defs, factory_id } in new_pools {
                    if !seen_new_pools.insert((pool_id.block, pool_id.tx)) {
                        continue;
                    }
                    let factory_from_call = pool_factory_by_id.get(&pool_id).copied();
                    let factory_id = factory_from_call.or(factory_id);
                    let factory_ok = factory_id
                        .map(|id| amm_factories.contains(&id))
                        .unwrap_or(false);
                    if !factory_ok {
                        continue;
                    }

                    pools_map.insert(pool_id, defs);
                    if let Ok(encoded_defs) = borsh::to_vec(&defs) {
                        pool_defs_writes.push((table.pools_key(&pool_id), encoded_defs));
                    }
                    token_pools_writes.push((
                        table.token_pools_key(&defs.base_alkane_id, &pool_id),
                        Vec::new(),
                    ));
                    token_pools_writes.push((
                        table.token_pools_key(&defs.quote_alkane_id, &pool_id),
                        Vec::new(),
                    ));
                    reserves_snapshot.entry(pool_id).or_insert(SchemaPoolSnapshot {
                        base_reserve: 0,
                        quote_reserve: 0,
                        base_id: defs.base_alkane_id,
                        quote_id: defs.quote_alkane_id,
                    });
                    if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
                        canonical_pool_updates.entry(defs.base_alkane_id).or_default().push(
                            SchemaCanonicalPoolEntry { pool_id, quote_id: defs.quote_alkane_id },
                        );
                    }

                    let base_label =
                        get_alkane_label(essentials, &mut alkane_label_cache, &defs.base_alkane_id);
                    let quote_label = get_alkane_label(
                        essentials,
                        &mut alkane_label_cache,
                        &defs.quote_alkane_id,
                    );
                    let pool_name = format!("{base_label} / {quote_label}");
                    let pool_name_norm = pool_name.trim().to_ascii_lowercase();
                    if !pool_name_norm.is_empty() {
                        pool_name_index_writes.push((
                            table.pool_name_index_key(&pool_name_norm, &pool_id),
                            Vec::new(),
                        ));
                    }

                    if let Some(factory_id) = factory_id {
                        factory_pools_writes
                            .push((table.factory_pools_key(&factory_id, &pool_id), Vec::new()));
                        let mut factory_bytes = Vec::with_capacity(12);
                        factory_bytes.extend_from_slice(&factory_id.block.to_be_bytes());
                        factory_bytes.extend_from_slice(&factory_id.tx.to_be_bytes());
                        pool_factory_writes.push((table.pool_factory_key(&pool_id), factory_bytes));
                    }

                    // Pool creation info
                    let mut creator_spk =
                        pool_creator_spk_from_protostone(&transaction.transaction);
                    if creator_spk.is_none() {
                        let mut lowest_spk: Option<ScriptBuf> = None;
                        let mut lowest_value: Option<u64> = None;
                        for vin in &transaction.transaction.input {
                            if vin.previous_output.is_null() {
                                continue;
                            }
                            let prev_txid = vin.previous_output.txid;
                            let prev_tx = if let Some(tx) = block_tx_map.get(&prev_txid) {
                                Some((*tx).clone())
                            } else if let Some(tx) = prev_tx_cache.get(&prev_txid) {
                                Some(tx.clone())
                            } else {
                                let raw = get_electrum_like()
                                    .batch_transaction_get_raw(&[prev_txid])
                                    .unwrap_or_default()
                                    .into_iter()
                                    .next()
                                    .unwrap_or_default();
                                if raw.is_empty() {
                                    None
                                } else {
                                    deserialize::<Transaction>(&raw).ok().map(|tx| {
                                        prev_tx_cache.insert(prev_txid, tx.clone());
                                        tx
                                    })
                                }
                            };
                            let Some(prev_tx) = prev_tx else { continue };
                            let idx = vin.previous_output.vout as usize;
                            let Some(prev_out) = prev_tx.output.get(idx) else { continue };
                            let value = prev_out.value.to_sat();
                            if lowest_value.map_or(true, |v| value < v) {
                                lowest_value = Some(value);
                                lowest_spk = Some(prev_out.script_pubkey.clone());
                            }
                        }
                        creator_spk = lowest_spk;
                    }

                    let mut pool_balances =
                        get_alkane_balances(essentials, &pool_id).unwrap_or_default();
                    let initial_token0_amount =
                        pool_balances.remove(&defs.base_alkane_id).unwrap_or(0);
                    let initial_token1_amount =
                        pool_balances.remove(&defs.quote_alkane_id).unwrap_or(0);
                    let initial_lp_supply = essentials
                        .get_latest_circulating_supply(
                            crate::modules::essentials::storage::GetLatestCirculatingSupplyParams {
                                alkane: pool_id,
                            },
                        )
                        .map(|res| res.supply)
                        .unwrap_or(0);

                    pool_lp_supply_writes.push((
                        table.pool_lp_supply_latest_key(&pool_id),
                        encode_u128_value(initial_lp_supply)?,
                    ));

                    let creation_info = SchemaPoolCreationInfoV1 {
                        creator_spk: creator_spk.map(|s| s.as_bytes().to_vec()).unwrap_or_default(),
                        creation_height: block.height,
                        initial_token0_amount,
                        initial_token1_amount,
                        initial_lp_supply,
                    };
                    pool_creation_info_cache.insert(pool_id, creation_info.clone());
                    pool_creation_info_writes.push((
                        table.pool_creation_info_key(&pool_id),
                        encode_pool_creation_info(&creation_info)?,
                    ));

                    let txid = transaction.transaction.compute_txid();
                    let txid_bytes = txid.to_byte_array();
                    let (address_spk, success) =
                        tx_meta.get(&txid).cloned().unwrap_or_else(|| (Vec::new(), true));

                    let activity = SchemaActivityV1 {
                        timestamp: block_ts,
                        txid: txid_bytes,
                        kind: ActivityKind::PoolCreate,
                        direction: None,
                        base_delta: 0,
                        quote_delta: 0,
                        address_spk,
                        success,
                    };

                    if let Ok(seq) = activity_acc.push(pool_id, block_ts, activity.clone()) {
                        index_acc.add(&pool_id, block_ts, seq, &activity);
                        pool_creations_writes
                            .push((table.pool_creations_key(block_ts, seq, &pool_id), Vec::new()));
                        if !activity.address_spk.is_empty() {
                            address_pool_creations_writes.push((
                                table.address_pool_creations_key(
                                    &activity.address_spk,
                                    block_ts,
                                    seq,
                                    &pool_id,
                                ),
                                Vec::new(),
                            ));
                            address_amm_history_writes.push((
                                table.address_amm_history_key(
                                    &activity.address_spk,
                                    block_ts,
                                    seq,
                                    activity.kind,
                                    &pool_id,
                                ),
                                Vec::new(),
                            ));
                        }
                        amm_history_all_writes.push((
                            table.amm_history_all_key(
                                block_ts,
                                seq,
                                activity.kind,
                                &pool_id,
                            ),
                            Vec::new(),
                        ));
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
        let mut pools_touched: HashSet<SchemaAlkaneId> = HashSet::new();

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
                pools_touched.insert(owner);

                let txid = Txid::from_byte_array(entry.txid);
                let (address_spk, success) =
                    tx_meta.get(&txid).cloned().unwrap_or_else(|| (Vec::new(), true));
                let address_spk = address_spk.clone();

                let activity = SchemaActivityV1 {
                    timestamp: block_ts,
                    txid: entry.txid,
                    kind,
                    direction,
                    base_delta,
                    quote_delta,
                    address_spk: address_spk.clone(),
                    success,
                };

                if let Ok(seq) = activity_acc.push(owner, block_ts, activity.clone()) {
                    index_acc.add(&owner, block_ts, seq, &activity);
                    if matches!(kind, ActivityKind::TradeBuy | ActivityKind::TradeSell) {
                        token_swaps_writes.push((
                            table.token_swaps_key(&defs.base_alkane_id, block_ts, seq, &owner),
                            Vec::new(),
                        ));
                        token_swaps_writes.push((
                            table.token_swaps_key(&defs.quote_alkane_id, block_ts, seq, &owner),
                            Vec::new(),
                        ));
                        if !address_spk.is_empty() {
                            address_pool_swaps_writes.push((
                                table.address_pool_swaps_key(&address_spk, &owner, block_ts, seq),
                                Vec::new(),
                            ));
                            address_token_swaps_writes.push((
                                table.address_token_swaps_key(
                                    &address_spk,
                                    &defs.base_alkane_id,
                                    block_ts,
                                    seq,
                                    &owner,
                                ),
                                Vec::new(),
                            ));
                            address_token_swaps_writes.push((
                                table.address_token_swaps_key(
                                    &address_spk,
                                    &defs.quote_alkane_id,
                                    block_ts,
                                    seq,
                                    &owner,
                                ),
                                Vec::new(),
                            ));
                        }
                    }
                    if !address_spk.is_empty() {
                        match kind {
                            ActivityKind::LiquidityAdd => {
                                address_pool_mints_writes.push((
                                    table.address_pool_mints_key(
                                        &address_spk,
                                        block_ts,
                                        seq,
                                        &owner,
                                    ),
                                    Vec::new(),
                                ));
                            }
                            ActivityKind::LiquidityRemove => {
                                address_pool_burns_writes.push((
                                    table.address_pool_burns_key(
                                        &address_spk,
                                        block_ts,
                                        seq,
                                        &owner,
                                    ),
                                    Vec::new(),
                                ));
                            }
                            _ => {}
                        }
                    }
                    amm_history_all_writes.push((
                        table.amm_history_all_key(block_ts, seq, kind, &owner),
                        Vec::new(),
                    ));
                    if !address_spk.is_empty() {
                        address_amm_history_writes.push((
                            table.address_amm_history_key(&address_spk, block_ts, seq, kind, &owner),
                            Vec::new(),
                        ));
                    }
                }

                if matches!(kind, ActivityKind::TradeBuy | ActivityKind::TradeSell) {
                    has_trades = true;
                    let p_q_per_b = price_quote_per_base(new_base, new_quote);
                    let p_b_per_q = price_base_per_quote(new_base, new_quote);
                    let base_in = if base_delta > 0 { base_delta as u128 } else { 0 };
                    let quote_out = if quote_delta < 0 { (-quote_delta) as u128 } else { 0 };

                    candle_cache.apply_trade_for_frames(
                        block_ts, owner, &frames, p_b_per_q, p_q_per_b, base_in, quote_out,
                    );

                    if canonical_quote_units.contains_key(&defs.quote_alkane_id) {
                        let entry = canonical_trade_buckets.entry(defs.base_alkane_id).or_default();
                        for tf in &frames {
                            entry.insert((*tf, bucket_start_for(block_ts, *tf)));
                        }
                    }
                }
            }
        }

        debug::log_elapsed(module, "process_traces_activity", timer);
        // ---------- derived data (canonical pools + token USD candles + metrics) ----------
        let timer = debug::start_if(debug);
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
                canonical_pools_by_token.entry(defs.base_alkane_id).or_default().push(
                    SchemaCanonicalPoolEntry { pool_id: *pool, quote_id: defs.quote_alkane_id },
                );
            }
        }

        let (candle_writes, candle_entries) = candle_cache.into_writes_with_entries(provider)?;
        let mut pool_candle_overrides: HashMap<
            (SchemaAlkaneId, Timeframe, u64),
            SchemaFullCandleV1,
        > = HashMap::new();
        for (pool, tf, bucket_ts, candle) in candle_entries {
            pool_candle_overrides.insert((pool, tf, bucket_ts), candle);
        }

        debug::log_elapsed(module, "derive_pool_candles", timer);
        let timer = debug::start_if(debug);
        let load_pool_candle = |pool: &SchemaAlkaneId,
                                tf: Timeframe,
                                bucket_ts: u64|
         -> Result<Option<SchemaFullCandleV1>> {
            if let Some(c) = pool_candle_overrides.get(&(*pool, tf, bucket_ts)) {
                return Ok(Some(*c));
            }
            let key = table.candle_key(pool, tf, bucket_ts);
            if let Some(raw) = provider.get_raw_value(GetRawValueParams { key })?.value {
                return Ok(Some(decode_full_candle_v1(&raw)?));
            }
            Ok(None)
        };

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

        debug::log_elapsed(module, "derive_canonical_pools", timer);
        let timer = debug::start_if(debug);
        let mut token_usd_candle_overrides: HashMap<
            (SchemaAlkaneId, Timeframe, u64),
            SchemaCandleV1,
        > = HashMap::new();
        let mut token_usd_candle_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_mcusd_candle_overrides: HashMap<
            (SchemaAlkaneId, Timeframe, u64),
            SchemaCandleV1,
        > = HashMap::new();
        let mut token_mcusd_candle_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_derived_usd_candle_overrides: HashMap<
            (SchemaAlkaneId, SchemaAlkaneId, Timeframe, u64),
            SchemaCandleV1,
        > = HashMap::new();
        let mut token_derived_usd_candle_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_metrics_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_metrics_index_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_metrics_index_deletes: Vec<Vec<u8>> = Vec::new();
        let mut token_metrics_index_new: u64 = 0;
        let mut token_search_index_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut token_search_index_deletes: Vec<Vec<u8>> = Vec::new();
        let mut derived_metrics_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut derived_metrics_index_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut derived_metrics_index_deletes: Vec<Vec<u8>> = Vec::new();
        let mut derived_metrics_index_new: HashMap<SchemaAlkaneId, u64> = HashMap::new();
        let mut derived_search_index_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        let mut derived_search_index_deletes: Vec<Vec<u8>> = Vec::new();
        let mut supply_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();

        if !canonical_trade_buckets.is_empty() || !derived_quotes.is_empty() {
            for (token, buckets) in canonical_trade_buckets.iter() {
                let Some(pools) = canonical_pools_by_token.get(token) else { continue };
                for (tf, bucket_ts) in buckets {
                    let mut btc_candle: Option<SchemaCandleV1> = None;
                    let mut usd_candle: Option<SchemaCandleV1> = None;

                    for entry in pools.iter() {
                        let Some(unit) = canonical_quote_units.get(&entry.quote_id) else {
                            continue;
                        };
                        let Some(pool_candle) = load_pool_candle(&entry.pool_id, *tf, *bucket_ts)?
                        else {
                            continue;
                        };

                        let quote_volume = pool_candle.quote_candle.volume;
                        let conv = |p: u128| -> Option<u128> {
                            match unit {
                                CanonicalQuoteUnit::Usd => Some(p),
                                CanonicalQuoteUnit::Btc => {
                                    btc_usd_price.map(|btc| p.saturating_mul(btc) / PRICE_SCALE)
                                }
                            }
                        };
                        let conv_vol = |v: u128| -> Option<u128> {
                            match unit {
                                CanonicalQuoteUnit::Usd => Some(v),
                                CanonicalQuoteUnit::Btc => {
                                    btc_usd_price.map(|btc| v.saturating_mul(btc) / PRICE_SCALE)
                                }
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
                        let prev_bucket =
                            bucket_ts.checked_sub(tf.duration_secs()).unwrap_or(*bucket_ts);
                        if let Some(c) = token_usd_candle_overrides.get(&(*token, *tf, prev_bucket))
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

            if !derived_quotes.is_empty() {
                #[derive(Clone, Copy)]
                struct DerivedPoolInfo {
                    pool_id: SchemaAlkaneId,
                    token_is_base: bool,
                }

                let mut derived_quote_strategies: HashMap<SchemaAlkaneId, DerivedMergeStrategy> =
                    HashMap::new();
                for dq in &derived_quotes {
                    derived_quote_strategies.insert(dq.alkane, dq.strategy.clone());
                }

                let derived_quote_set: HashSet<SchemaAlkaneId> =
                    derived_quotes.iter().map(|dq| dq.alkane).collect();

                let mut derived_pool_by_token_quote: HashMap<
                    (SchemaAlkaneId, SchemaAlkaneId),
                    DerivedPoolInfo,
                > = HashMap::new();

                let mut maybe_insert_pool = |token: SchemaAlkaneId,
                                             quote: SchemaAlkaneId,
                                             pool: SchemaAlkaneId,
                                             token_is_base: bool| {
                    let key = (token, quote);
                    match derived_pool_by_token_quote.get(&key) {
                        None => {
                            derived_pool_by_token_quote.insert(
                                key,
                                DerivedPoolInfo { pool_id: pool, token_is_base },
                            );
                        }
                        Some(existing) => {
                            let prefer = token_is_base && !existing.token_is_base;
                            let smaller = pool.block < existing.pool_id.block
                                || (pool.block == existing.pool_id.block
                                    && pool.tx < existing.pool_id.tx);
                            if prefer || (existing.token_is_base == token_is_base && smaller) {
                                derived_pool_by_token_quote.insert(
                                    key,
                                    DerivedPoolInfo { pool_id: pool, token_is_base },
                                );
                            }
                        }
                    }
                };

                for (pool, defs) in pools_map.iter() {
                    if derived_quote_set.contains(&defs.quote_alkane_id) {
                        maybe_insert_pool(
                            defs.base_alkane_id,
                            defs.quote_alkane_id,
                            *pool,
                            true,
                        );
                    }
                    if derived_quote_set.contains(&defs.base_alkane_id) {
                        maybe_insert_pool(
                            defs.quote_alkane_id,
                            defs.base_alkane_id,
                            *pool,
                            false,
                        );
                    }
                }

                let mut pool_to_edges: HashMap<
                    SchemaAlkaneId,
                    Vec<(SchemaAlkaneId, SchemaAlkaneId, bool)>,
                > = HashMap::new();
                let mut quote_to_tokens: HashMap<SchemaAlkaneId, Vec<SchemaAlkaneId>> =
                    HashMap::new();
                for ((token, quote), info) in derived_pool_by_token_quote.iter() {
                    pool_to_edges
                        .entry(info.pool_id)
                        .or_default()
                        .push((*token, *quote, info.token_is_base));
                    quote_to_tokens.entry(*quote).or_default().push(*token);
                }

                let mut pool_overrides_by_pool_tf: HashMap<
                    (SchemaAlkaneId, Timeframe),
                    BTreeMap<u64, SchemaFullCandleV1>,
                > = HashMap::new();
                for ((pool, tf, bucket), candle) in pool_candle_overrides.iter() {
                    pool_overrides_by_pool_tf
                        .entry((*pool, *tf))
                        .or_default()
                        .insert(*bucket, *candle);
                }

                let mut token_usd_overrides_by_token_tf: HashMap<
                    (SchemaAlkaneId, Timeframe),
                    BTreeMap<u64, SchemaCandleV1>,
                > = HashMap::new();
                for ((token, tf, bucket), candle) in token_usd_candle_overrides.iter() {
                    token_usd_overrides_by_token_tf
                        .entry((*token, *tf))
                        .or_default()
                        .insert(*bucket, *candle);
                }

                let mut derived_overrides_by_token_quote_tf: HashMap<
                    (SchemaAlkaneId, SchemaAlkaneId, Timeframe),
                    BTreeMap<u64, SchemaCandleV1>,
                > = HashMap::new();

                let parse_ts = |key: &[u8]| -> Option<u64> {
                    key.rsplit(|&b| b == b':')
                        .next()
                        .and_then(|ts_bytes| std::str::from_utf8(ts_bytes).ok())
                        .and_then(|ts_str| ts_str.parse::<u64>().ok())
                };

                let latest_pool_candle = |pool: &SchemaAlkaneId,
                                          tf: Timeframe,
                                          target: u64|
                 -> Option<(u64, SchemaFullCandleV1)> {
                    let mut best: Option<(u64, SchemaFullCandleV1)> = None;
                    if let Some(map) = pool_overrides_by_pool_tf.get(&(*pool, tf)) {
                        if let Some((&ts, candle)) = map.range(..=target).next_back() {
                            best = Some((ts, *candle));
                        }
                    }
                    let prefix = table.candle_ns_prefix(pool, tf);
                    if let Ok(resp) = provider.get_iter_prefix_rev(GetIterPrefixRevParams { prefix }) {
                        for (k, v) in resp.entries {
                            let Some(ts) = parse_ts(&k) else { continue };
                            if ts > target {
                                continue;
                            }
                            if let Ok(c) = decode_full_candle_v1(&v) {
                                match best {
                                    Some((best_ts, _)) if best_ts >= ts => {}
                                    _ => best = Some((ts, c)),
                                }
                            }
                            break;
                        }
                    }
                    best
                };

                let latest_token_usd_candle = |token: &SchemaAlkaneId,
                                               tf: Timeframe,
                                               target: u64|
                 -> Option<(u64, SchemaCandleV1)> {
                    let mut best: Option<(u64, SchemaCandleV1)> = None;
                    if let Some(map) = token_usd_overrides_by_token_tf.get(&(*token, tf)) {
                        if let Some((&ts, candle)) = map.range(..=target).next_back() {
                            best = Some((ts, *candle));
                        }
                    }
                    let prefix = table.token_usd_candle_ns_prefix(token, tf);
                    if let Ok(resp) = provider.get_iter_prefix_rev(GetIterPrefixRevParams { prefix }) {
                        for (k, v) in resp.entries {
                            let Some(ts) = parse_ts(&k) else { continue };
                            if ts > target {
                                continue;
                            }
                            if let Ok(c) = decode_candle_v1(&v) {
                                match best {
                                    Some((best_ts, _)) if best_ts >= ts => {}
                                    _ => best = Some((ts, c)),
                                }
                            }
                            break;
                        }
                    }
                    best
                };

                let latest_derived_candle = |map: &HashMap<
                    (SchemaAlkaneId, SchemaAlkaneId, Timeframe),
                    BTreeMap<u64, SchemaCandleV1>,
                >,
                                             token: &SchemaAlkaneId,
                                             quote: &SchemaAlkaneId,
                                             tf: Timeframe,
                                             target: u64|
                 -> Option<(u64, SchemaCandleV1)> {
                    let mut best: Option<(u64, SchemaCandleV1)> = None;
                    if let Some(bucket_map) = map.get(&(*token, *quote, tf)) {
                        if let Some((&ts, candle)) = bucket_map.range(..=target).next_back() {
                            best = Some((ts, *candle));
                        }
                    }
                    let prefix = table.token_derived_usd_candle_ns_prefix(token, quote, tf);
                    if let Ok(resp) = provider.get_iter_prefix_rev(GetIterPrefixRevParams { prefix }) {
                        for (k, v) in resp.entries {
                            let Some(ts) = parse_ts(&k) else { continue };
                            if ts > target {
                                continue;
                            }
                            if let Ok(c) = decode_candle_v1(&v) {
                                match best {
                                    Some((best_ts, _)) if best_ts >= ts => {}
                                    _ => best = Some((ts, c)),
                                }
                            }
                            break;
                        }
                    }
                    best
                };

                let mut derived_buckets: HashSet<(SchemaAlkaneId, SchemaAlkaneId, Timeframe, u64)> =
                    HashSet::new();
                for ((pool, tf, bucket), _candle) in pool_candle_overrides.iter() {
                    if let Some(edges) = pool_to_edges.get(pool) {
                        for (token, quote, _token_is_base) in edges {
                            derived_buckets.insert((*token, *quote, *tf, *bucket));
                        }
                    }
                }
                for ((quote, tf, bucket), _candle) in token_usd_candle_overrides.iter() {
                    if let Some(tokens) = quote_to_tokens.get(quote) {
                        for token in tokens {
                            derived_buckets.insert((*token, *quote, *tf, *bucket));
                        }
                    }
                }

                for (token, quote, tf, bucket_ts) in derived_buckets.into_iter() {
                    let Some(info) = derived_pool_by_token_quote.get(&(token, quote)) else {
                        continue;
                    };
                    let Some((_pool_ts, pool_candle)) =
                        latest_pool_candle(&info.pool_id, tf, bucket_ts)
                    else {
                        continue;
                    };
                    let Some((_usd_ts, q_usd_candle)) =
                        latest_token_usd_candle(&quote, tf, bucket_ts)
                    else {
                        continue;
                    };

                    let (q_per_t_open, q_per_t_high, q_per_t_low, q_per_t_close) = if info
                        .token_is_base
                    {
                        (
                            pool_candle.base_candle.open,
                            pool_candle.base_candle.high,
                            pool_candle.base_candle.low,
                            pool_candle.base_candle.close,
                        )
                    } else {
                        let inv_open = match invert_price_value(pool_candle.base_candle.open) {
                            Some(v) => v,
                            None => continue,
                        };
                        let inv_close = match invert_price_value(pool_candle.base_candle.close) {
                            Some(v) => v,
                            None => continue,
                        };
                        let inv_high = match invert_price_value(pool_candle.base_candle.low) {
                            Some(v) => v,
                            None => continue,
                        };
                        let inv_low = match invert_price_value(pool_candle.base_candle.high) {
                            Some(v) => v,
                            None => continue,
                        };
                        (inv_open, inv_high, inv_low, inv_close)
                    };

                    let conv = |q_usd: u128, q_per_t: u128| -> Option<u128> {
                        if q_per_t == 0 {
                            None
                        } else {
                            Some(q_usd.saturating_mul(q_per_t) / PRICE_SCALE)
                        }
                    };

                    let Some(open_conv) = conv(q_usd_candle.open, q_per_t_open) else { continue };
                    let Some(high_conv) = conv(q_usd_candle.high, q_per_t_high) else { continue };
                    let Some(low_conv) = conv(q_usd_candle.low, q_per_t_low) else { continue };
                    let Some(close_conv) = conv(q_usd_candle.close, q_per_t_close) else { continue };

                    let token_volume = if info.token_is_base {
                        pool_candle.base_candle.volume
                    } else {
                        pool_candle.quote_candle.volume
                    };
                    let volume_usd = token_volume.saturating_mul(close_conv) / PRICE_SCALE;

                    let mut derived = SchemaCandleV1 {
                        open: open_conv,
                        high: high_conv,
                        low: low_conv,
                        close: close_conv,
                        volume: volume_usd,
                    };

                    if let Some((_ts, canonical)) =
                        latest_token_usd_candle(&token, tf, bucket_ts)
                    {
                        let strategy = derived_quote_strategies
                            .get(&quote)
                            .unwrap_or(&DerivedMergeStrategy::Neutral);
                        let mut merged = merge_candles(derived, canonical, strategy);
                        merged.volume = derived.volume;
                        derived = merged;
                    }

                    let prev_bucket = bucket_ts.saturating_sub(tf.duration_secs());
                    if let Some((_ts, prev)) = latest_derived_candle(
                        &derived_overrides_by_token_quote_tf,
                        &token,
                        &quote,
                        tf,
                        prev_bucket,
                    ) {
                        derived.open = prev.close;
                        if derived.open > derived.high {
                            derived.high = derived.open;
                        }
                        if derived.open < derived.low {
                            derived.low = derived.open;
                        }
                    }

                    token_derived_usd_candle_overrides
                        .insert((token, quote, tf, bucket_ts), derived);
                    derived_overrides_by_token_quote_tf
                        .entry((token, quote, tf))
                        .or_default()
                        .insert(bucket_ts, derived);
                }

                for ((token, quote, tf, bucket_ts), candle) in
                    token_derived_usd_candle_overrides.iter()
                {
                    let key = table.token_derived_usd_candle_key(token, quote, *tf, *bucket_ts);
                    let encoded = encode_candle_v1(candle)?;
                    token_derived_usd_candle_writes.push((key, encoded));
                }
            }

            for ((token, tf, bucket_ts), candle) in token_usd_candle_overrides.iter() {
                let supply = if let Some(v) = supply_cache.get(token) {
                    *v
                } else {
                    let table_e = essentials.table();
                    let key = table_e.circulating_supply_latest_key(token);
                    let v = essentials
                        .get_raw_value(EssentialsGetRawValueParams { key })?
                        .value
                        .and_then(|raw| {
                            crate::modules::essentials::storage::decode_u128_value(&raw).ok()
                        })
                        .unwrap_or(0);
                    supply_cache.insert(*token, v);
                    v
                };
                if supply == 0 {
                    continue;
                }
                let scale = |p: u128| -> u128 { p.saturating_mul(supply) / PRICE_SCALE };
                let mc_candle = SchemaCandleV1 {
                    open: scale(candle.open),
                    high: scale(candle.high),
                    low: scale(candle.low),
                    close: scale(candle.close),
                    volume: candle.volume,
                };
                token_mcusd_candle_overrides.insert((*token, *tf, *bucket_ts), mc_candle);
            }

            for ((token, tf, bucket_ts), candle) in token_usd_candle_overrides.iter() {
                let key = table.token_usd_candle_key(token, *tf, *bucket_ts);
                let encoded = encode_candle_v1(candle)?;
                token_usd_candle_writes.push((key, encoded));
            }

            for ((token, tf, bucket_ts), candle) in token_mcusd_candle_overrides.iter() {
                let key = table.token_mcusd_candle_key(token, *tf, *bucket_ts);
                let encoded = encode_candle_v1(candle)?;
                token_mcusd_candle_writes.push((key, encoded));
            }

            let mut tokens_for_metrics: HashSet<SchemaAlkaneId> = HashSet::new();
            for token in canonical_trade_buckets.keys() {
                tokens_for_metrics.insert(*token);
            }
            for ((token, _tf, _bucket), _candle) in token_usd_candle_overrides.iter() {
                tokens_for_metrics.insert(*token);
            }

            for token in tokens_for_metrics.iter() {
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
                        return per_bucket.get(&earliest_bucket).map(|c| c.close).unwrap_or(0);
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
                let first_close = per_bucket.get(&earliest_bucket).map(|c| c.close).unwrap_or(0);

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
                    per_bucket.range(start..=now_bucket).map(|(_, c)| c.volume).sum()
                };

                let volume_all_time: u128 = per_bucket.values().map(|c| c.volume).sum();

                let supply = {
                    let table_e = essentials.table();
                    let key = table_e.circulating_supply_latest_key(token);
                    essentials
                        .get_raw_value(EssentialsGetRawValueParams { key })?
                        .value
                        .and_then(|v| {
                            crate::modules::essentials::storage::decode_u128_value(&v).ok()
                        })
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

                let metrics_key = table.token_metrics_key(token);
                let prev_raw = provider.get_raw_value(GetRawValueParams { key: metrics_key.clone() })?;
                let prev_metrics = prev_raw
                    .value
                    .as_ref()
                    .and_then(|raw| decode_token_metrics(raw).ok());
                if prev_raw.value.is_none() {
                    token_metrics_index_new = token_metrics_index_new.saturating_add(1);
                }

                let build_index_keys = |m: &SchemaTokenMetricsV1| -> Vec<(TokenMetricsIndexField, Vec<u8>)> {
                    vec![
                        (
                            TokenMetricsIndexField::PriceUsd,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::PriceUsd,
                                m.price_usd,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::MarketcapUsd,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::MarketcapUsd,
                                m.marketcap_usd,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume1d,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::Volume1d,
                                m.volume_1d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume7d,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::Volume7d,
                                m.volume_7d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Volume30d,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::Volume30d,
                                m.volume_30d,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::VolumeAllTime,
                            table.token_metrics_index_key_u128(
                                TokenMetricsIndexField::VolumeAllTime,
                                m.volume_all_time,
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change1d,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::Change1d,
                                parse_change_basis_points(&m.change_1d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change7d,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::Change7d,
                                parse_change_basis_points(&m.change_7d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::Change30d,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::Change30d,
                                parse_change_basis_points(&m.change_30d),
                                token,
                            ),
                        ),
                        (
                            TokenMetricsIndexField::ChangeAllTime,
                            table.token_metrics_index_key_i64(
                                TokenMetricsIndexField::ChangeAllTime,
                                parse_change_basis_points(&m.change_all_time),
                                token,
                            ),
                        ),
                    ]
                };

                let new_keys = build_index_keys(&metrics);
                if let Some(prev) = prev_metrics.as_ref() {
                    let prev_keys = build_index_keys(prev);
                    for (idx, (_field, new_key)) in new_keys.iter().enumerate() {
                        if let Some((_pf, prev_key)) = prev_keys.get(idx) {
                            if prev_key != new_key {
                                token_metrics_index_deletes.push(prev_key.clone());
                                token_metrics_index_writes.push((new_key.clone(), Vec::new()));
                            }
                        }
                    }
                } else {
                    for (_field, new_key) in new_keys.into_iter() {
                        token_metrics_index_writes.push((new_key, Vec::new()));
                    }
                }

                if search_index_enabled {
                    let rec = essentials
                        .get_creation_record(GetCreationRecordParams { alkane: *token })
                        .ok()
                        .and_then(|resp| resp.record);
                    if let Some(rec) = rec {
                        let prefixes = collect_search_prefixes(
                            &rec.names,
                            &rec.symbols,
                            search_prefix_min,
                            search_prefix_max,
                        );
                        if !prefixes.is_empty() {
                            let new_marketcap = metrics.marketcap_usd;
                            let new_volume_7d = metrics.volume_7d;
                            let new_change_7d = parse_change_basis_points(&metrics.change_7d);
                            let new_volume_all = metrics.volume_all_time;

                            let prev_marketcap = prev_metrics.as_ref().map(|m| m.marketcap_usd);
                            let prev_volume_7d = prev_metrics.as_ref().map(|m| m.volume_7d);
                            let prev_change_7d =
                                prev_metrics.as_ref().map(|m| parse_change_basis_points(&m.change_7d));
                            let prev_volume_all = prev_metrics.as_ref().map(|m| m.volume_all_time);

                            for prefix in prefixes {
                                token_search_index_writes.push((
                                    table.token_search_index_key_u128(
                                        SearchIndexField::Marketcap,
                                        &prefix,
                                        new_marketcap,
                                        token,
                                    ),
                                    Vec::new(),
                                ));
                                token_search_index_writes.push((
                                    table.token_search_index_key_u128(
                                        SearchIndexField::Volume7d,
                                        &prefix,
                                        new_volume_7d,
                                        token,
                                    ),
                                    Vec::new(),
                                ));
                                token_search_index_writes.push((
                                    table.token_search_index_key_i64(
                                        SearchIndexField::Change7d,
                                        &prefix,
                                        new_change_7d,
                                        token,
                                    ),
                                    Vec::new(),
                                ));
                                token_search_index_writes.push((
                                    table.token_search_index_key_u128(
                                        SearchIndexField::VolumeAllTime,
                                        &prefix,
                                        new_volume_all,
                                        token,
                                    ),
                                    Vec::new(),
                                ));

                                if let Some(prev) = prev_marketcap {
                                    if prev != new_marketcap {
                                        token_search_index_deletes.push(
                                            table.token_search_index_key_u128(
                                                SearchIndexField::Marketcap,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                                if let Some(prev) = prev_volume_7d {
                                    if prev != new_volume_7d {
                                        token_search_index_deletes.push(
                                            table.token_search_index_key_u128(
                                                SearchIndexField::Volume7d,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                                if let Some(prev) = prev_change_7d {
                                    if prev != new_change_7d {
                                        token_search_index_deletes.push(
                                            table.token_search_index_key_i64(
                                                SearchIndexField::Change7d,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                                if let Some(prev) = prev_volume_all {
                                    if prev != new_volume_all {
                                        token_search_index_deletes.push(
                                            table.token_search_index_key_u128(
                                                SearchIndexField::VolumeAllTime,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                token_metrics_cache.insert(*token, metrics.clone());
                let encoded = encode_token_metrics(&metrics)?;
                token_metrics_writes.push((metrics_key, encoded));
            }

            let mut derived_tokens_for_metrics: HashSet<(SchemaAlkaneId, SchemaAlkaneId)> =
                HashSet::new();
            for ((token, quote, _tf, _bucket), _candle) in token_derived_usd_candle_overrides.iter()
            {
                derived_tokens_for_metrics.insert((*token, *quote));
            }

            for (token, quote) in derived_tokens_for_metrics.iter() {
                let prefix =
                    table.token_derived_usd_candle_ns_prefix(token, quote, Timeframe::M10);
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

                for ((tok, q, tf, bucket), candle) in token_derived_usd_candle_overrides.iter() {
                    if tok == token && q == quote && *tf == Timeframe::M10 {
                        per_bucket.insert(*bucket, *candle);
                    }
                }

                if per_bucket.is_empty() {
                    continue;
                }

                let now_bucket = bucket_start_for(block_ts, Timeframe::M10);
                let earliest_bucket = per_bucket.keys().next().copied().unwrap_or(now_bucket);

                let close_at = |target_bucket: u64| -> u128 {
                    if per_bucket.is_empty() {
                        return 0;
                    }
                    if target_bucket <= earliest_bucket {
                        return per_bucket.get(&earliest_bucket).map(|c| c.close).unwrap_or(0);
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
                let first_close = per_bucket.get(&earliest_bucket).map(|c| c.close).unwrap_or(0);

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
                    per_bucket.range(start..=now_bucket).map(|(_, c)| c.volume).sum()
                };

                let volume_all_time: u128 = per_bucket.values().map(|c| c.volume).sum();

                let supply = if let Some(v) = supply_cache.get(token) {
                    *v
                } else {
                    let table_e = essentials.table();
                    let key = table_e.circulating_supply_latest_key(token);
                    let v = essentials
                        .get_raw_value(EssentialsGetRawValueParams { key })?
                        .value
                        .and_then(|raw| {
                            crate::modules::essentials::storage::decode_u128_value(&raw).ok()
                        })
                        .unwrap_or(0);
                    supply_cache.insert(*token, v);
                    v
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

                let metrics_key = table.token_derived_metrics_key(token, quote);
                let prev_raw =
                    provider.get_raw_value(GetRawValueParams { key: metrics_key.clone() })?;
                let prev_metrics = prev_raw
                    .value
                    .as_ref()
                    .and_then(|raw| decode_token_metrics(raw).ok());
                if prev_raw.value.is_none() {
                    let entry = derived_metrics_index_new.entry(*quote).or_insert(0);
                    *entry = entry.saturating_add(1);
                }

                let build_index_keys =
                    |m: &SchemaTokenMetricsV1| -> Vec<(TokenMetricsIndexField, Vec<u8>)> {
                        vec![
                            (
                                TokenMetricsIndexField::PriceUsd,
                                table.token_derived_metrics_index_key_u128(
                                    quote,
                                    TokenMetricsIndexField::PriceUsd,
                                    m.price_usd,
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::MarketcapUsd,
                                table.token_derived_metrics_index_key_u128(
                                    quote,
                                    TokenMetricsIndexField::MarketcapUsd,
                                    m.marketcap_usd,
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::Volume1d,
                                table.token_derived_metrics_index_key_u128(
                                    quote,
                                    TokenMetricsIndexField::Volume1d,
                                    m.volume_1d,
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::Volume7d,
                                table.token_derived_metrics_index_key_u128(
                                    quote,
                                    TokenMetricsIndexField::Volume7d,
                                    m.volume_7d,
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::Volume30d,
                                table.token_derived_metrics_index_key_u128(
                                    quote,
                                    TokenMetricsIndexField::Volume30d,
                                    m.volume_30d,
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::VolumeAllTime,
                                table.token_derived_metrics_index_key_u128(
                                    quote,
                                    TokenMetricsIndexField::VolumeAllTime,
                                    m.volume_all_time,
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::Change1d,
                                table.token_derived_metrics_index_key_i64(
                                    quote,
                                    TokenMetricsIndexField::Change1d,
                                    parse_change_basis_points(&m.change_1d),
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::Change7d,
                                table.token_derived_metrics_index_key_i64(
                                    quote,
                                    TokenMetricsIndexField::Change7d,
                                    parse_change_basis_points(&m.change_7d),
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::Change30d,
                                table.token_derived_metrics_index_key_i64(
                                    quote,
                                    TokenMetricsIndexField::Change30d,
                                    parse_change_basis_points(&m.change_30d),
                                    token,
                                ),
                            ),
                            (
                                TokenMetricsIndexField::ChangeAllTime,
                                table.token_derived_metrics_index_key_i64(
                                    quote,
                                    TokenMetricsIndexField::ChangeAllTime,
                                    parse_change_basis_points(&m.change_all_time),
                                    token,
                                ),
                            ),
                        ]
                    };

                let new_keys = build_index_keys(&metrics);
                if let Some(prev) = prev_metrics.as_ref() {
                    let prev_keys = build_index_keys(prev);
                    for (idx, (_field, new_key)) in new_keys.iter().enumerate() {
                        if let Some((_pf, prev_key)) = prev_keys.get(idx) {
                            if prev_key != new_key {
                                derived_metrics_index_deletes.push(prev_key.clone());
                                derived_metrics_index_writes.push((new_key.clone(), Vec::new()));
                            }
                        }
                    }
                } else {
                    for (_field, new_key) in new_keys.into_iter() {
                        derived_metrics_index_writes.push((new_key, Vec::new()));
                    }
                }

                if search_index_enabled {
                    let rec = essentials
                        .get_creation_record(GetCreationRecordParams { alkane: *token })
                        .ok()
                        .and_then(|resp| resp.record);
                    if let Some(rec) = rec {
                        let prefixes = collect_search_prefixes(
                            &rec.names,
                            &rec.symbols,
                            search_prefix_min,
                            search_prefix_max,
                        );
                        if !prefixes.is_empty() {
                            let new_marketcap = metrics.marketcap_usd;
                            let new_volume_7d = metrics.volume_7d;
                            let new_change_7d = parse_change_basis_points(&metrics.change_7d);
                            let new_volume_all = metrics.volume_all_time;

                            let prev_marketcap = prev_metrics.as_ref().map(|m| m.marketcap_usd);
                            let prev_volume_7d = prev_metrics.as_ref().map(|m| m.volume_7d);
                            let prev_change_7d = prev_metrics
                                .as_ref()
                                .map(|m| parse_change_basis_points(&m.change_7d));
                            let prev_volume_all = prev_metrics.as_ref().map(|m| m.volume_all_time);

                            for prefix in prefixes {
                                derived_search_index_writes.push((
                                    table.token_derived_search_index_key_u128(
                                        quote,
                                        SearchIndexField::Marketcap,
                                        &prefix,
                                        new_marketcap,
                                        token,
                                    ),
                                    Vec::new(),
                                ));
                                derived_search_index_writes.push((
                                    table.token_derived_search_index_key_u128(
                                        quote,
                                        SearchIndexField::Volume7d,
                                        &prefix,
                                        new_volume_7d,
                                        token,
                                    ),
                                    Vec::new(),
                                ));
                                derived_search_index_writes.push((
                                    table.token_derived_search_index_key_i64(
                                        quote,
                                        SearchIndexField::Change7d,
                                        &prefix,
                                        new_change_7d,
                                        token,
                                    ),
                                    Vec::new(),
                                ));
                                derived_search_index_writes.push((
                                    table.token_derived_search_index_key_u128(
                                        quote,
                                        SearchIndexField::VolumeAllTime,
                                        &prefix,
                                        new_volume_all,
                                        token,
                                    ),
                                    Vec::new(),
                                ));

                                if let Some(prev) = prev_marketcap {
                                    if prev != new_marketcap {
                                        derived_search_index_deletes.push(
                                            table.token_derived_search_index_key_u128(
                                                quote,
                                                SearchIndexField::Marketcap,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                                if let Some(prev) = prev_volume_7d {
                                    if prev != new_volume_7d {
                                        derived_search_index_deletes.push(
                                            table.token_derived_search_index_key_u128(
                                                quote,
                                                SearchIndexField::Volume7d,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                                if let Some(prev) = prev_change_7d {
                                    if prev != new_change_7d {
                                        derived_search_index_deletes.push(
                                            table.token_derived_search_index_key_i64(
                                                quote,
                                                SearchIndexField::Change7d,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                                if let Some(prev) = prev_volume_all {
                                    if prev != new_volume_all {
                                        derived_search_index_deletes.push(
                                            table.token_derived_search_index_key_u128(
                                                quote,
                                                SearchIndexField::VolumeAllTime,
                                                &prefix,
                                                prev,
                                                token,
                                            ),
                                        );
                                    }
                                }
                            }
                        }
                    }
                }

                let encoded = encode_token_metrics(&metrics)?;
                derived_metrics_writes.push((metrics_key, encoded));
            }
        }

        debug::log_elapsed(module, "derive_token_metrics", timer);
        // ---------- pool metrics + TVL versioned + LP supply ----------
        let timer = debug::start_if(debug);
        if !pools_touched.is_empty() {
            let mut token_price_usd_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();
            let mut token_price_sats_cache: HashMap<SchemaAlkaneId, u128> = HashMap::new();

            let mut get_token_price_usd = |token: &SchemaAlkaneId| -> u128 {
                if let Some(price) = token_price_usd_cache.get(token) {
                    return *price;
                }
                let metrics = token_metrics_cache
                    .get(token)
                    .cloned()
                    .or_else(|| {
                        provider
                            .get_token_metrics(GetTokenMetricsParams { token: *token })
                            .ok()
                            .map(|res| res.metrics)
                    })
                    .unwrap_or_default();
                let price = metrics.price_usd;
                token_price_usd_cache.insert(*token, price);
                price
            };

            let mut get_token_price_sats = |token: &SchemaAlkaneId| -> u128 {
                if let Some(price) = token_price_sats_cache.get(token) {
                    return *price;
                }
                let mut price = 0u128;
                if let Some(pools) = canonical_pools_by_token.get(token) {
                    for entry in pools {
                        let unit = match canonical_quote_units.get(&entry.quote_id) {
                            Some(u) => *u,
                            None => continue,
                        };
                        if unit != CanonicalQuoteUnit::Btc {
                            continue;
                        }
                        let bucket = bucket_start_for(block_ts, Timeframe::M10);
                        if let Ok(Some(candle)) =
                            load_pool_candle(&entry.pool_id, Timeframe::M10, bucket)
                        {
                            price = candle.base_candle.close;
                        } else {
                            let prefix = table.candle_ns_prefix(&entry.pool_id, Timeframe::M10);
                            if let Ok(res) =
                                provider.get_iter_prefix_rev(GetIterPrefixRevParams { prefix })
                            {
                                if let Some((_k, v)) = res.entries.into_iter().next() {
                                    if let Ok(candle) = decode_full_candle_v1(&v) {
                                        price = candle.base_candle.close;
                                    }
                                }
                            }
                        }
                        break;
                    }
                }
                token_price_sats_cache.insert(*token, price);
                price
            };

            let bucket_1d_now = bucket_start_for(block_ts, Timeframe::D1);
            let window_7d_start =
                bucket_1d_now.saturating_sub(6 * Timeframe::D1.duration_secs());
            let mut pool_volume_cache: HashMap<SchemaAlkaneId, (u128, u128, u128, u128)> =
                HashMap::new();
            let parse_bucket_ts = |key: &[u8]| -> Option<u64> {
                key.rsplit(|&b| b == b':')
                    .next()
                    .and_then(|ts_bytes| std::str::from_utf8(ts_bytes).ok())
                    .and_then(|ts_str| ts_str.parse::<u64>().ok())
            };
            let mut pool_volume_from_candles = |pool: &SchemaAlkaneId| -> Result<(u128, u128, u128, u128)> {
                if let Some(cached) = pool_volume_cache.get(pool) {
                    return Ok(*cached);
                }
                let mut overrides: HashMap<u64, SchemaFullCandleV1> = HashMap::new();
                for ((pid, tf, bucket), candle) in pool_candle_overrides.iter() {
                    if *pid == *pool && *tf == Timeframe::D1 {
                        overrides.insert(*bucket, *candle);
                    }
                }

                let prefix = table.candle_ns_prefix(pool, Timeframe::D1);
                let mut token0_volume_7d = 0u128;
                let mut token1_volume_7d = 0u128;
                let mut token0_volume_all = 0u128;
                let mut token1_volume_all = 0u128;

                for (k, v) in provider
                    .get_iter_prefix_rev(GetIterPrefixRevParams { prefix })?
                    .entries
                {
                    let Some(ts) = parse_bucket_ts(&k) else { continue };
                    let candle = if let Some(override_candle) = overrides.remove(&ts) {
                        override_candle
                    } else {
                        decode_full_candle_v1(&v)?
                    };
                    let base_vol = candle.base_candle.volume;
                    let quote_vol = candle.quote_candle.volume;
                    token0_volume_all = token0_volume_all.saturating_add(base_vol);
                    token1_volume_all = token1_volume_all.saturating_add(quote_vol);
                    if ts >= window_7d_start {
                        token0_volume_7d = token0_volume_7d.saturating_add(base_vol);
                        token1_volume_7d = token1_volume_7d.saturating_add(quote_vol);
                    }
                }

                for (ts, candle) in overrides {
                    let base_vol = candle.base_candle.volume;
                    let quote_vol = candle.quote_candle.volume;
                    token0_volume_all = token0_volume_all.saturating_add(base_vol);
                    token1_volume_all = token1_volume_all.saturating_add(quote_vol);
                    if ts >= window_7d_start {
                        token0_volume_7d = token0_volume_7d.saturating_add(base_vol);
                        token1_volume_7d = token1_volume_7d.saturating_add(quote_vol);
                    }
                }

                let out = (token0_volume_7d, token1_volume_7d, token0_volume_all, token1_volume_all);
                pool_volume_cache.insert(*pool, out);
                Ok(out)
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

            let tvl_at_height = |pool: &SchemaAlkaneId, h: u32| -> u128 {
                provider
                    .get_tvl_versioned_at_or_before(GetTvlVersionedAtOrBeforeParams {
                        pool: *pool,
                        height: h,
                    })
                    .ok()
                    .and_then(|res| res.value)
                    .unwrap_or(0)
            };

            for pool in pools_touched.iter() {
                let Some(defs) = pools_map.get(pool) else { continue };

                let mut balances = get_alkane_balances(essentials, pool).unwrap_or_default();
                let token0_amount = balances.remove(&defs.base_alkane_id).unwrap_or(0);
                let token1_amount = balances.remove(&defs.quote_alkane_id).unwrap_or(0);

                let token0_price_usd = get_token_price_usd(&defs.base_alkane_id);
                let token1_price_usd = get_token_price_usd(&defs.quote_alkane_id);
                let token0_price_sats = get_token_price_sats(&defs.base_alkane_id);
                let token1_price_sats = get_token_price_sats(&defs.quote_alkane_id);

                let mut token0_tvl_usd =
                    token0_amount.saturating_mul(token0_price_usd) / PRICE_SCALE;
                let mut token1_tvl_usd =
                    token1_amount.saturating_mul(token1_price_usd) / PRICE_SCALE;
                let token0_tvl_sats = token0_amount.saturating_mul(token0_price_sats) / PRICE_SCALE;
                let token1_tvl_sats = token1_amount.saturating_mul(token1_price_sats) / PRICE_SCALE;

                if let Some(unit) = canonical_quote_units.get(&defs.base_alkane_id) {
                    if let Some(value) = canonical_quote_amount_tvl_usd(
                        token0_amount,
                        *unit,
                        btc_usd_price,
                    ) {
                        token0_tvl_usd = value;
                    }
                }
                if let Some(unit) = canonical_quote_units.get(&defs.quote_alkane_id) {
                    if let Some(value) = canonical_quote_amount_tvl_usd(
                        token1_amount,
                        *unit,
                        btc_usd_price,
                    ) {
                        token1_tvl_usd = value;
                    }
                }

                let pool_tvl_usd = token0_tvl_usd.saturating_add(token1_tvl_usd);
                let pool_tvl_sats = token0_tvl_sats.saturating_add(token1_tvl_sats);

                let bucket_1d = bucket_1d_now;
                let bucket_30d = bucket_start_for(block_ts, Timeframe::M1);

                let (token0_volume_1d, token1_volume_1d) =
                    if let Ok(Some(c)) = load_pool_candle(pool, Timeframe::D1, bucket_1d) {
                        (c.base_candle.volume, c.quote_candle.volume)
                    } else {
                        (0, 0)
                    };
                let (token0_volume_30d, token1_volume_30d) =
                    if let Ok(Some(c)) = load_pool_candle(pool, Timeframe::M1, bucket_30d) {
                        (c.base_candle.volume, c.quote_candle.volume)
                    } else {
                        (0, 0)
                    };

                let pool_volume_1d_usd = token0_volume_1d
                    .saturating_mul(token0_price_usd)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_1d
                            .saturating_mul(token1_price_usd)
                            .saturating_div(PRICE_SCALE),
                    );
                let pool_volume_30d_usd = token0_volume_30d
                    .saturating_mul(token0_price_usd)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_30d
                            .saturating_mul(token1_price_usd)
                            .saturating_div(PRICE_SCALE),
                    );
                let pool_volume_1d_sats = token0_volume_1d
                    .saturating_mul(token0_price_sats)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_1d
                            .saturating_mul(token1_price_sats)
                            .saturating_div(PRICE_SCALE),
                    );
                let pool_volume_30d_sats = token0_volume_30d
                    .saturating_mul(token0_price_sats)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_30d
                            .saturating_mul(token1_price_sats)
                            .saturating_div(PRICE_SCALE),
                    );

                let (token0_volume_7d, token1_volume_7d, token0_volume_all, token1_volume_all) =
                    pool_volume_from_candles(pool)?;
                let pool_volume_7d_usd = token0_volume_7d
                    .saturating_mul(token0_price_usd)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_7d
                            .saturating_mul(token1_price_usd)
                            .saturating_div(PRICE_SCALE),
                    );
                let pool_volume_all_time_usd = token0_volume_all
                    .saturating_mul(token0_price_usd)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_all
                            .saturating_mul(token1_price_usd)
                            .saturating_div(PRICE_SCALE),
                    );
                let pool_volume_7d_sats = token0_volume_7d
                    .saturating_mul(token0_price_sats)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_7d
                            .saturating_mul(token1_price_sats)
                            .saturating_div(PRICE_SCALE),
                    );
                let pool_volume_all_time_sats = token0_volume_all
                    .saturating_mul(token0_price_sats)
                    .saturating_div(PRICE_SCALE)
                    .saturating_add(
                        token1_volume_all
                            .saturating_mul(token1_price_sats)
                            .saturating_div(PRICE_SCALE),
                    );

                let prev_1d = tvl_at_height(pool, height.saturating_sub(144));
                let prev_7d = tvl_at_height(pool, height.saturating_sub(1008));
                let tvl_change_24h = percent_change(prev_1d, pool_tvl_usd);
                let tvl_change_7d = percent_change(prev_7d, pool_tvl_usd);

                let pool_apr = if pool_tvl_usd == 0 {
                    "0".to_string()
                } else {
                    let fees = (pool_volume_30d_usd as f64) * 0.003;
                    let apr = fees / (pool_tvl_usd as f64) * 12.0 * 100.0;
                    format!("{:.4}", apr)
                };

                let metrics = SchemaPoolMetricsV1 {
                    token0_volume_1d,
                    token1_volume_1d,
                    token0_volume_30d,
                    token1_volume_30d,
                    pool_volume_1d_usd,
                    pool_volume_30d_usd,
                    pool_volume_1d_sats,
                    pool_volume_30d_sats,
                    pool_tvl_usd,
                    pool_tvl_sats,
                    tvl_change_24h: tvl_change_24h.clone(),
                    tvl_change_7d: tvl_change_7d.clone(),
                    pool_apr: pool_apr.clone(),
                };
                let metrics_v2 = SchemaPoolMetricsV2 {
                    token0_volume_1d,
                    token1_volume_1d,
                    token0_volume_30d,
                    token1_volume_30d,
                    pool_volume_1d_usd,
                    pool_volume_30d_usd,
                    pool_volume_1d_sats,
                    pool_volume_30d_sats,
                    pool_volume_7d_usd,
                    pool_volume_all_time_usd,
                    pool_volume_7d_sats,
                    pool_volume_all_time_sats,
                    pool_tvl_usd,
                    pool_tvl_sats,
                    tvl_change_24h,
                    tvl_change_7d,
                    pool_apr,
                };

                pool_metrics_writes
                    .push((table.pool_metrics_key(pool), encode_pool_metrics(&metrics)?));
                pool_metrics_writes
                    .push((table.pool_metrics_v2_key(pool), encode_pool_metrics_v2(&metrics_v2)?));

                let lp_supply = essentials
                    .get_latest_circulating_supply(GetLatestCirculatingSupplyParams {
                        alkane: *pool,
                    })
                    .map(|res| res.supply)
                    .unwrap_or(0);
                pool_lp_supply_writes
                    .push((table.pool_lp_supply_latest_key(pool), encode_u128_value(lp_supply)?));

                let token0_label =
                    get_alkane_label(essentials, &mut alkane_label_cache, &defs.base_alkane_id);
                let token1_label =
                    get_alkane_label(essentials, &mut alkane_label_cache, &defs.quote_alkane_id);
                let pool_name =
                    pool_name_display(&format!("{token0_label} / {token1_label}"));

                let creation_info = pool_creation_info_cache
                    .get(pool)
                    .cloned()
                    .or_else(|| {
                        provider
                            .get_pool_creation_info(GetPoolCreationInfoParams { pool: *pool })
                            .ok()
                            .and_then(|res| res.info)
                    });
                let (creator_address, creation_height, initial_token0_amount, initial_token1_amount) =
                    if let Some(info) = creation_info {
                        let creator = if info.creator_spk.is_empty() {
                            None
                        } else {
                            let spk = ScriptBuf::from(info.creator_spk.clone());
                            spk_to_address_str(&spk, network)
                        };
                        (
                            creator,
                            Some(info.creation_height),
                            info.initial_token0_amount,
                            info.initial_token1_amount,
                        )
                    } else {
                        (None, None, 0, 0)
                    };

                let lp_value_sats =
                    if lp_supply == 0 { 0 } else { pool_tvl_sats.saturating_div(lp_supply) };
                let lp_value_usd =
                    if lp_supply == 0 { 0 } else { pool_tvl_usd.saturating_div(lp_supply) };

                let pool_apr = parse_change_f64(&metrics.pool_apr);
                let tvl_change_24h = parse_change_f64(&metrics.tvl_change_24h);
                let tvl_change_7d = parse_change_f64(&metrics.tvl_change_7d);

                let value = json!({
                    "token0": alkane_id_json(&defs.base_alkane_id),
                    "token1": alkane_id_json(&defs.quote_alkane_id),
                    "token0Amount": token0_amount.to_string(),
                    "token1Amount": token1_amount.to_string(),
                    "tokenSupply": lp_supply.to_string(),
                    "poolName": pool_name,
                    "poolId": alkane_id_json(pool),
                    "token0TvlInSats": token0_tvl_sats.to_string(),
                    "token0TvlInUsd": scale_price_u128(token0_tvl_usd),
                    "token1TvlInSats": token1_tvl_sats.to_string(),
                    "token1TvlInUsd": scale_price_u128(token1_tvl_usd),
                    "poolVolume30dInSats": metrics.pool_volume_30d_sats.to_string(),
                    "poolVolume1dInSats": metrics.pool_volume_1d_sats.to_string(),
                    "poolVolume30dInUsd": scale_price_u128(metrics.pool_volume_30d_usd),
                    "poolVolume1dInUsd": scale_price_u128(metrics.pool_volume_1d_usd),
                    "token0Volume30d": metrics.token0_volume_30d.to_string(),
                    "token1Volume30d": metrics.token1_volume_30d.to_string(),
                    "token0Volume1d": metrics.token0_volume_1d.to_string(),
                    "token1Volume1d": metrics.token1_volume_1d.to_string(),
                    "lPTokenValueInSats": lp_value_sats.to_string(),
                    "lPTokenValueInUsd": scale_price_u128(lp_value_usd),
                    "poolTvlInSats": pool_tvl_sats.to_string(),
                    "poolTvlInUsd": scale_price_u128(pool_tvl_usd),
                    "tvlChange24h": tvl_change_24h,
                    "tvlChange7d": tvl_change_7d,
                    "totalSupply": lp_supply.to_string(),
                    "poolApr": pool_apr,
                    "initialToken0Amount": initial_token0_amount.to_string(),
                    "initialToken1Amount": initial_token1_amount.to_string(),
                    "creatorAddress": creator_address,
                    "creationBlockHeight": creation_height,
                    "tvl": scale_price_u128(pool_tvl_usd),
                    "volume1d": scale_price_u128(metrics.pool_volume_1d_usd),
                    "volume7d": scale_price_u128(pool_volume_7d_usd),
                    "volume30d": scale_price_u128(metrics.pool_volume_30d_usd),
                    "volumeAllTime": scale_price_u128(pool_volume_all_time_usd),
                    "apr": pool_apr,
                    "tvlChange": tvl_change_24h,
                });

                let snapshot = SchemaPoolDetailsSnapshot {
                    value_json: serde_json::to_vec(&value)?,
                    token0_tvl_usd,
                    token1_tvl_usd,
                    token0_tvl_sats,
                    token1_tvl_sats,
                    pool_tvl_usd,
                    pool_volume_1d_usd: metrics.pool_volume_1d_usd,
                    pool_volume_30d_usd: metrics.pool_volume_30d_usd,
                    pool_apr,
                    tvl_change_24h,
                    lp_supply,
                };

                pool_details_snapshot_writes.push((
                    table.pool_details_snapshot_key(pool),
                    encode_pool_details_snapshot(&snapshot)?,
                ));

                tvl_versioned_writes.push((
                    table.tvl_versioned_key(pool, height),
                    encode_u128_value(pool_tvl_usd)?,
                ));
            }
        }

        debug::log_elapsed(module, "pool_metrics_tvl", timer);
        // ---------- one atomic DB write (candles + activity + indexes + reserves snapshot) ----------
        let timer = debug::start_if(debug);
        let activity_writes = activity_acc.into_writes();
        let idx_delta = index_acc.clone().per_pool_delta();
        let idx_group_delta = index_acc.clone().per_pool_group_delta();
        let mut index_writes = index_acc.into_writes();

        // Update per-pool index counts
        for ((blk_id, tx_id), delta) in idx_delta {
            let pool = SchemaAlkaneId { block: blk_id, tx: tx_id };
            let count_k_rel = crate::modules::ammdata::utils::activity::idx_count_key(&pool);

            let current = if let Some(v) =
                provider.get_raw_value(GetRawValueParams { key: count_k_rel.clone() })?.value
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

            let current = if let Some(v) =
                provider.get_raw_value(GetRawValueParams { key: count_k_rel.clone() })?.value
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

        if token_metrics_index_new > 0 {
            let count_key = table.token_metrics_index_count_key();
            let current = provider
                .get_raw_value(GetRawValueParams { key: count_key.clone() })?
                .value
                .and_then(|raw| {
                    if raw.len() == 8 {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(&raw);
                        Some(u64::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            let updated = current.saturating_add(token_metrics_index_new);
            token_metrics_index_writes.push((count_key, updated.to_le_bytes().to_vec()));
        }

        if !derived_metrics_index_new.is_empty() {
            for (quote, add) in derived_metrics_index_new.into_iter() {
                if add == 0 {
                    continue;
                }
                let count_key = table.token_derived_metrics_index_count_key(&quote);
                let current = provider
                    .get_raw_value(GetRawValueParams { key: count_key.clone() })?
                    .value
                    .and_then(|raw| {
                        if raw.len() == 8 {
                            let mut arr = [0u8; 8];
                            arr.copy_from_slice(&raw);
                            Some(u64::from_le_bytes(arr))
                        } else {
                            None
                        }
                    })
                    .unwrap_or(0);
                let updated = current.saturating_add(add);
                derived_metrics_index_writes.push((count_key, updated.to_le_bytes().to_vec()));
            }
        }

        let reserves_blob = encode_reserves_snapshot(&reserves_snapshot)?;
        let reserves_key_rel = table.reserves_snapshot_key();

        let c_cnt = candle_writes.len();
        let tc_cnt = token_usd_candle_writes.len();
        let tmc_cnt = token_mcusd_candle_writes.len();
        let tdc_cnt = token_derived_usd_candle_writes.len();
        let tm_cnt = token_metrics_writes.len();
        let tmi_cnt = token_metrics_index_writes.len();
        let tsi_cnt = token_search_index_writes.len();
        let tdm_cnt = derived_metrics_writes.len();
        let tdmi_cnt = derived_metrics_index_writes.len();
        let tdsi_cnt = derived_search_index_writes.len();
        let cp_cnt = canonical_pool_writes.len();
        let pn_cnt = pool_name_index_writes.len();
        let af_cnt = amm_factory_writes.len();
        let fp_cnt = factory_pools_writes.len();
        let pf_cnt = pool_factory_writes.len();
        let pc_cnt = pool_creation_info_writes.len();
        let pcg_cnt = pool_creations_writes.len();
        let aps_cnt = address_pool_swaps_writes.len();
        let ats_cnt = address_token_swaps_writes.len();
        let apc_cnt = address_pool_creations_writes.len();
        let apm_cnt = address_pool_mints_writes.len();
        let apb_cnt = address_pool_burns_writes.len();
        let aah_cnt = address_amm_history_writes.len();
        let ah_cnt = amm_history_all_writes.len();
        let tp_cnt = token_pools_writes.len();
        let pd_cnt = pool_defs_writes.len();
        let pm_cnt = pool_metrics_writes.len();
        let pls_cnt = pool_lp_supply_writes.len();
        let pds_cnt = pool_details_snapshot_writes.len();
        let tvl_cnt = tvl_versioned_writes.len();
        let ts_cnt = token_swaps_writes.len();
        let a_cnt = activity_writes.len();
        let i_cnt = index_writes.len();

        eprintln!(
            "[AMMDATA] block #{h} prepare writes: candles={c_cnt}, token_usd_candles={tc_cnt}, token_mcusd_candles={tmc_cnt}, token_derived_usd_candles={tdc_cnt}, token_metrics={tm_cnt}, token_metrics_index={tmi_cnt}, token_search_index={tsi_cnt}, token_derived_metrics={tdm_cnt}, token_derived_metrics_index={tdmi_cnt}, token_derived_search_index={tdsi_cnt}, canonical_pools={cp_cnt}, pool_name_index={pn_cnt}, amm_factories={af_cnt}, factory_pools={fp_cnt}, pool_factory={pf_cnt}, pool_creation_info={pc_cnt}, pool_creations={pcg_cnt}, token_pools={tp_cnt}, pool_defs={pd_cnt}, pool_metrics={pm_cnt}, pool_lp_supply={pls_cnt}, pool_details_snapshot={pds_cnt}, tvl_versioned={tvl_cnt}, token_swaps={ts_cnt}, address_pool_swaps={aps_cnt}, address_token_swaps={ats_cnt}, address_pool_creations={apc_cnt}, address_pool_mints={apm_cnt}, address_pool_burns={apb_cnt}, address_amm_history={aah_cnt}, amm_history_all={ah_cnt}, activity={a_cnt}, indexes+counts={i_cnt}, reserves_snapshot=1",
            h = block.height,
            c_cnt = c_cnt,
            tc_cnt = tc_cnt,
            tmc_cnt = tmc_cnt,
            tdc_cnt = tdc_cnt,
            tm_cnt = tm_cnt,
            tmi_cnt = tmi_cnt,
            tsi_cnt = tsi_cnt,
            tdm_cnt = tdm_cnt,
            tdmi_cnt = tdmi_cnt,
            tdsi_cnt = tdsi_cnt,
            cp_cnt = cp_cnt,
            pn_cnt = pn_cnt,
            af_cnt = af_cnt,
            fp_cnt = fp_cnt,
            pf_cnt = pf_cnt,
            pc_cnt = pc_cnt,
            pcg_cnt = pcg_cnt,
            pd_cnt = pd_cnt,
            pm_cnt = pm_cnt,
            pls_cnt = pls_cnt,
            tvl_cnt = tvl_cnt,
            ts_cnt = ts_cnt,
            apc_cnt = apc_cnt,
            apm_cnt = apm_cnt,
            apb_cnt = apb_cnt,
            aah_cnt = aah_cnt,
            ah_cnt = ah_cnt,
            tp_cnt = tp_cnt,
            a_cnt = a_cnt,
            i_cnt = i_cnt,
        );

        debug::log_elapsed(module, "prepare_writes", timer);
        let timer = debug::start_if(debug);
        if !candle_writes.is_empty()
            || !token_usd_candle_writes.is_empty()
            || !token_mcusd_candle_writes.is_empty()
            || !token_derived_usd_candle_writes.is_empty()
            || !token_metrics_writes.is_empty()
            || !token_metrics_index_writes.is_empty()
            || !token_metrics_index_deletes.is_empty()
            || !token_search_index_writes.is_empty()
            || !token_search_index_deletes.is_empty()
            || !derived_metrics_writes.is_empty()
            || !derived_metrics_index_writes.is_empty()
            || !derived_metrics_index_deletes.is_empty()
            || !derived_search_index_writes.is_empty()
            || !derived_search_index_deletes.is_empty()
            || !canonical_pool_writes.is_empty()
            || !pool_name_index_writes.is_empty()
            || !amm_factory_writes.is_empty()
            || !factory_pools_writes.is_empty()
            || !pool_factory_writes.is_empty()
            || !pool_creation_info_writes.is_empty()
            || !pool_creations_writes.is_empty()
            || !pool_defs_writes.is_empty()
            || !token_pools_writes.is_empty()
            || !pool_metrics_writes.is_empty()
            || !pool_lp_supply_writes.is_empty()
            || !pool_details_snapshot_writes.is_empty()
            || !tvl_versioned_writes.is_empty()
            || !token_swaps_writes.is_empty()
            || !address_pool_swaps_writes.is_empty()
            || !address_token_swaps_writes.is_empty()
            || !address_pool_creations_writes.is_empty()
            || !address_pool_mints_writes.is_empty()
            || !address_pool_burns_writes.is_empty()
            || !address_amm_history_writes.is_empty()
            || !amm_history_all_writes.is_empty()
            || !activity_writes.is_empty()
            || !index_writes.is_empty()
            || !reserves_blob.is_empty()
        {
            let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
            puts.extend(candle_writes);
            puts.extend(token_usd_candle_writes);
            puts.extend(token_mcusd_candle_writes);
            puts.extend(token_derived_usd_candle_writes);
            puts.extend(token_metrics_writes);
            puts.extend(token_metrics_index_writes);
            puts.extend(token_search_index_writes);
            puts.extend(derived_metrics_writes);
            puts.extend(derived_metrics_index_writes);
            puts.extend(derived_search_index_writes);
            puts.extend(canonical_pool_writes);
            puts.extend(pool_name_index_writes);
            puts.extend(amm_factory_writes);
            puts.extend(factory_pools_writes);
            puts.extend(pool_factory_writes);
            puts.extend(pool_creation_info_writes);
            puts.extend(pool_creations_writes);
            puts.extend(pool_defs_writes);
            puts.extend(token_pools_writes);
            puts.extend(pool_metrics_writes);
            puts.extend(pool_lp_supply_writes);
            puts.extend(pool_details_snapshot_writes);
            puts.extend(tvl_versioned_writes);
            puts.extend(token_swaps_writes);
            puts.extend(address_pool_swaps_writes);
            puts.extend(address_token_swaps_writes);
            puts.extend(address_pool_creations_writes);
            puts.extend(address_pool_mints_writes);
            puts.extend(address_pool_burns_writes);
            puts.extend(address_amm_history_writes);
            puts.extend(amm_history_all_writes);
            puts.extend(activity_writes);
            puts.extend(index_writes);
            puts.push((reserves_key_rel, reserves_blob));

            let mut deletes: Vec<Vec<u8>> = Vec::new();
            deletes.extend(token_metrics_index_deletes);
            deletes.extend(token_search_index_deletes);
            deletes.extend(derived_metrics_index_deletes);
            deletes.extend(derived_search_index_deletes);
            let _ = provider.set_batch(SetBatchParams { puts, deletes });
        }

        debug::log_elapsed(module, "write_batch", timer);
        println!(
            "[AMMDATA] Finished processing block #{} with {} traces",
            block.height,
            block.transactions.len()
        );
        self.set_index_height(block.height)?;
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            height,
            t0.elapsed()
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let provider = self.provider.as_ref().expect("ModuleRegistry must call set_mdb()").clone();
        register_rpc(reg, provider);
    }

    fn config_spec(&self) -> Option<&'static str> {
        Some(AmmDataConfig::spec())
    }

    fn set_config(&mut self, config: &serde_json::Value) -> Result<()> {
        AmmDataConfig::from_value(config).map(|_| ())
    }
}
