use super::schemas::{
    ActivityKind, SchemaActivityV1, SchemaCanonicalPoolEntry, SchemaCandleV1, SchemaMarketDefs,
    SchemaPoolCreationInfoV1, SchemaPoolMetricsV1, SchemaPoolSnapshot, SchemaReservesSnapshot,
    SchemaTokenMetricsV1, Timeframe,
};
use crate::modules::ammdata::consts::{
    CanonicalQuoteUnit, KEY_INDEX_HEIGHT, PRICE_SCALE, canonical_quotes,
};
use crate::config::get_network;
use crate::modules::ammdata::schemas::SchemaFullCandleV1;
use crate::modules::ammdata::utils::activity::{
    ActivityFilter, ActivityPage, ActivitySideFilter, ActivitySortKey, SortDir,
    decode_activity_v1, read_activity_for_pool, read_activity_for_pool_sorted,
};
use crate::modules::ammdata::utils::candles::{CandleSlice, PriceSide, read_candles_v1};
use crate::modules::ammdata::utils::live_reserves::fetch_all_pools;
use crate::modules::ammdata::utils::pathfinder::{
    DEFAULT_FEE_BPS, plan_best_mev_swap, plan_exact_in_default_fee, plan_exact_out_default_fee,
    plan_implicit_default_fee, plan_swap_exact_tokens_for_tokens,
    plan_swap_exact_tokens_for_tokens_implicit, plan_swap_tokens_for_exact_tokens,
};
use crate::modules::essentials::storage::EssentialsProvider;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use borsh::{BorshDeserialize, BorshSerialize};
use serde_json::{Value, json, map::Map};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct MdbPointer<'a> {
    mdb: &'a Mdb,
    key: Vec<u8>,
}

impl<'a> MdbPointer<'a> {
    pub fn root(mdb: &'a Mdb) -> Self {
        Self { mdb, key: Vec::new() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn keyword(&self, suffix: &str) -> Self {
        self.select(suffix.as_bytes())
    }

    pub fn select(&self, suffix: &[u8]) -> Self {
        let mut key = self.key.clone();
        key.extend_from_slice(suffix);
        Self { mdb: self.mdb, key }
    }

    pub fn get(&self) -> Result<Option<Vec<u8>>> {
        self.mdb.get(&self.key).map_err(|e| anyhow!("mdb.get failed: {e}"))
    }

    pub fn put(&self, value: &[u8]) -> Result<()> {
        self.mdb.put(&self.key, value).map_err(|e| anyhow!("mdb.put failed: {e}"))
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let full_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| {
                let mut key = self.key.clone();
                key.extend_from_slice(k);
                key
            })
            .collect();
        self.mdb.multi_get(&full_keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}"))
    }

    pub fn scan_prefix(&self) -> Result<Vec<Vec<u8>>> {
        self.mdb.scan_prefix(&self.key).map_err(|e| anyhow!("mdb.scan_prefix failed: {e}"))
    }

    pub fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        self.mdb.bulk_write(build).map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }
}

#[allow(non_snake_case)]
#[derive(Clone)]
pub struct AmmDataTable<'a> {
    pub ROOT: MdbPointer<'a>,
    // Core index height for the ammdata module.
    pub INDEX_HEIGHT: MdbPointer<'a>,
    // Reserve snapshots and pool summaries.
    pub RESERVES_SNAPSHOT: MdbPointer<'a>,
    pub POOLS: MdbPointer<'a>,
    // Candle series (fc1:<blk>:<tx>:<tf>:...).
    pub CANDLES: MdbPointer<'a>,
    pub TOKEN_USD_CANDLES: MdbPointer<'a>,
    // Activity logs + secondary indexes for sort/paging.
    pub ACTIVITY: MdbPointer<'a>,
    pub ACTIVITY_INDEX: MdbPointer<'a>,
    // Token-level indices.
    pub CANONICAL_POOL: MdbPointer<'a>,
    pub TOKEN_METRICS: MdbPointer<'a>,
    pub POOL_NAME_INDEX: MdbPointer<'a>,
    // Factory + pool indices.
    pub AMM_FACTORIES: MdbPointer<'a>,
    pub FACTORY_POOLS: MdbPointer<'a>,
    pub POOL_FACTORY: MdbPointer<'a>,
    pub POOL_METRICS: MdbPointer<'a>,
    pub POOL_CREATION_INFO: MdbPointer<'a>,
    pub POOL_LP_SUPPLY: MdbPointer<'a>,
    pub TVL_VERSIONED: MdbPointer<'a>,
    pub TOKEN_SWAPS: MdbPointer<'a>,
    pub POOL_CREATIONS: MdbPointer<'a>,
    pub ADDRESS_POOL_SWAPS: MdbPointer<'a>,
    pub ADDRESS_TOKEN_SWAPS: MdbPointer<'a>,
    pub ADDRESS_POOL_CREATIONS: MdbPointer<'a>,
    pub ADDRESS_POOL_MINTS: MdbPointer<'a>,
    pub ADDRESS_POOL_BURNS: MdbPointer<'a>,
}

impl<'a> AmmDataTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = MdbPointer::root(mdb);
        AmmDataTable {
            ROOT: root.clone(),
            INDEX_HEIGHT: root.select(KEY_INDEX_HEIGHT),
            RESERVES_SNAPSHOT: root.keyword("/reserves_snapshot_v1"),
            POOLS: root.keyword("/pools/"),
            CANDLES: root.keyword("fc1:"),
            TOKEN_USD_CANDLES: root.keyword("tuc1:"),
            ACTIVITY: root.keyword("activity:v1:"),
            ACTIVITY_INDEX: root.keyword("activity:idx:"),
            CANONICAL_POOL: root.keyword("/canonical_pool/v1/"),
            TOKEN_METRICS: root.keyword("/token_metrics/v1/"),
            POOL_NAME_INDEX: root.keyword("/pool_name_index/"),
            AMM_FACTORIES: root.keyword("/amm_factories/v1/"),
            FACTORY_POOLS: root.keyword("/factory_pools/v1/"),
            POOL_FACTORY: root.keyword("/pool_factory/v1/"),
            POOL_METRICS: root.keyword("/pool_metrics/v1/"),
            POOL_CREATION_INFO: root.keyword("/pool_creation_info/v1/"),
            POOL_LP_SUPPLY: root.keyword("/pool_lp_supply/latest/"),
            TVL_VERSIONED: root.keyword("/tvlVersioned/"),
            TOKEN_SWAPS: root.keyword("/token_swaps/v1/"),
            POOL_CREATIONS: root.keyword("/pool_creations/v1/"),
            ADDRESS_POOL_SWAPS: root.keyword("/address_pool_swaps/v1/"),
            ADDRESS_TOKEN_SWAPS: root.keyword("/address_token_swaps/v1/"),
            ADDRESS_POOL_CREATIONS: root.keyword("/address_pool_creations/v1/"),
            ADDRESS_POOL_MINTS: root.keyword("/address_pool_mints/v1/"),
            ADDRESS_POOL_BURNS: root.keyword("/address_pool_burns/v1/"),
        }
    }
}

impl<'a> AmmDataTable<'a> {
    pub fn candle_ns_prefix(&self, pool: &SchemaAlkaneId, tf: Timeframe) -> Vec<u8> {
        let blk_hex = format!("{:x}", pool.block);
        let tx_hex = format!("{:x}", pool.tx);
        let suffix = format!("{}:{}:{}:", blk_hex, tx_hex, tf.code());
        self.CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn activity_ns_prefix(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let suffix = format!("{}:{}:", pool.block, pool.tx);
        self.ACTIVITY.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn activity_key(&self, pool: &SchemaAlkaneId, ts: u64, seq: u32) -> Vec<u8> {
        let mut k = self.activity_ns_prefix(pool);
        k.extend_from_slice(ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k
    }

    pub fn candle_key_seq(
        &self,
        pool: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
        seq: u32,
    ) -> Vec<u8> {
        let mut k = self.candle_ns_prefix(pool, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k
    }

    pub fn candle_key(&self, pool: &SchemaAlkaneId, tf: Timeframe, bucket_ts: u64) -> Vec<u8> {
        let mut k = self.candle_ns_prefix(pool, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn token_usd_candle_ns_prefix(&self, token: &SchemaAlkaneId, tf: Timeframe) -> Vec<u8> {
        let blk_hex = format!("{:x}", token.block);
        let tx_hex = format!("{:x}", token.tx);
        let suffix = format!("{}:{}:{}:", blk_hex, tx_hex, tf.code());
        self.TOKEN_USD_CANDLES.select(suffix.as_bytes()).key().to_vec()
    }

    pub fn token_usd_candle_key(
        &self,
        token: &SchemaAlkaneId,
        tf: Timeframe,
        bucket_ts: u64,
    ) -> Vec<u8> {
        let mut k = self.token_usd_candle_ns_prefix(token, tf);
        k.extend_from_slice(bucket_ts.to_string().as_bytes());
        k
    }

    pub fn canonical_pool_key(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&token.block.to_be_bytes());
        suffix.extend_from_slice(&token.tx.to_be_bytes());
        self.CANONICAL_POOL.select(&suffix).key().to_vec()
    }

    pub fn token_metrics_key(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&token.block.to_be_bytes());
        suffix.extend_from_slice(&token.tx.to_be_bytes());
        self.TOKEN_METRICS.select(&suffix).key().to_vec()
    }

    pub fn pool_name_index_key(&self, name: &str, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(name.len() + 1 + 12);
        suffix.extend_from_slice(name.as_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_NAME_INDEX.select(&suffix).key().to_vec()
    }

    pub fn pool_name_index_prefix(&self, name_prefix: &str) -> Vec<u8> {
        self.POOL_NAME_INDEX.select(name_prefix.as_bytes()).key().to_vec()
    }

    pub fn parse_pool_name_index_key(&self, key: &[u8]) -> Option<(String, SchemaAlkaneId)> {
        let prefix = self.POOL_NAME_INDEX.key();
        if !key.starts_with(prefix) {
            return None;
        }
        let rest = &key[prefix.len()..];
        let split = rest.iter().rposition(|b| *b == b'/')?;
        let name_bytes = &rest[..split];
        let id_bytes = &rest[split + 1..];
        if id_bytes.len() != 12 {
            return None;
        }
        let mut block_arr = [0u8; 4];
        block_arr.copy_from_slice(&id_bytes[..4]);
        let mut tx_arr = [0u8; 8];
        tx_arr.copy_from_slice(&id_bytes[4..12]);
        let name = String::from_utf8(name_bytes.to_vec()).ok()?;
        Some((name, SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) }))
    }

    pub fn amm_factory_key(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        self.AMM_FACTORIES.select(&suffix).key().to_vec()
    }

    pub fn factory_pools_key(&self, factory: &SchemaAlkaneId, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1 + 12);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        suffix.push(b'/');
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.FACTORY_POOLS.select(&suffix).key().to_vec()
    }

    pub fn factory_pools_prefix(&self, factory: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&factory.block.to_be_bytes());
        suffix.extend_from_slice(&factory.tx.to_be_bytes());
        suffix.push(b'/');
        self.FACTORY_POOLS.select(&suffix).key().to_vec()
    }

    pub fn pool_factory_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_FACTORY.select(&suffix).key().to_vec()
    }

    pub fn pool_metrics_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_METRICS.select(&suffix).key().to_vec()
    }

    pub fn pool_creation_info_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_CREATION_INFO.select(&suffix).key().to_vec()
    }

    pub fn pool_lp_supply_latest_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOL_LP_SUPPLY.select(&suffix).key().to_vec()
    }

    pub fn tvl_versioned_prefix(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        suffix.push(b'/');
        self.TVL_VERSIONED.select(&suffix).key().to_vec()
    }

    pub fn tvl_versioned_key(&self, pool: &SchemaAlkaneId, height: u32) -> Vec<u8> {
        let mut k = self.tvl_versioned_prefix(pool);
        k.extend_from_slice(&height.to_be_bytes());
        k
    }

    pub fn token_swaps_prefix(&self, token: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12 + 1);
        suffix.extend_from_slice(&token.block.to_be_bytes());
        suffix.extend_from_slice(&token.tx.to_be_bytes());
        suffix.push(b'/');
        self.TOKEN_SWAPS.select(&suffix).key().to_vec()
    }

    pub fn token_swaps_key(
        &self,
        token: &SchemaAlkaneId,
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.token_swaps_prefix(token);
        k.extend_from_slice(ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k.push(b'/');
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn pool_creations_prefix(&self) -> Vec<u8> {
        self.POOL_CREATIONS.key().to_vec()
    }

    pub fn pool_creations_key(&self, ts: u64, seq: u32, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.pool_creations_prefix();
        k.extend_from_slice(ts.to_string().as_bytes());
        k.push(b':');
        k.extend_from_slice(seq.to_string().as_bytes());
        k.push(b'/');
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_swaps_prefix(
        &self,
        address_spk: &[u8],
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_SWAPS.key().to_vec();
        push_spk(&mut k, address_spk);
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_swaps_key(
        &self,
        address_spk: &[u8],
        pool: &SchemaAlkaneId,
        ts: u64,
        seq: u32,
    ) -> Vec<u8> {
        let mut k = self.address_pool_swaps_prefix(address_spk, pool);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    pub fn address_token_swaps_prefix(
        &self,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.ADDRESS_TOKEN_SWAPS.key().to_vec();
        push_spk(&mut k, address_spk);
        k.extend_from_slice(&token.block.to_be_bytes());
        k.extend_from_slice(&token.tx.to_be_bytes());
        k
    }

    pub fn address_token_swaps_key(
        &self,
        address_spk: &[u8],
        token: &SchemaAlkaneId,
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_token_swaps_prefix(address_spk, token);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_creations_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_CREATIONS.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_pool_creations_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_pool_creations_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_mints_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_MINTS.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_pool_mints_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_pool_mints_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn address_pool_burns_prefix(&self, address_spk: &[u8]) -> Vec<u8> {
        let mut k = self.ADDRESS_POOL_BURNS.key().to_vec();
        push_spk(&mut k, address_spk);
        k
    }

    pub fn address_pool_burns_key(
        &self,
        address_spk: &[u8],
        ts: u64,
        seq: u32,
        pool: &SchemaAlkaneId,
    ) -> Vec<u8> {
        let mut k = self.address_pool_burns_prefix(address_spk);
        k.extend_from_slice(&ts.to_be_bytes());
        k.extend_from_slice(&seq.to_be_bytes());
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn pools_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&pool.block.to_be_bytes());
        suffix.extend_from_slice(&pool.tx.to_be_bytes());
        self.POOLS.select(&suffix).key().to_vec()
    }

    pub fn reserves_snapshot_key(&self) -> Vec<u8> {
        self.RESERVES_SNAPSHOT.key().to_vec()
    }
}

fn parse_ts_seq_from_key(bytes: &[u8]) -> Option<(u64, u32)> {
    let mut parts = bytes.splitn(2, |b| *b == b':');
    let ts_bytes = parts.next()?;
    let seq_bytes = parts.next()?;
    let ts = std::str::from_utf8(ts_bytes).ok()?.parse::<u64>().ok()?;
    let seq = std::str::from_utf8(seq_bytes).ok()?.parse::<u32>().ok()?;
    Some((ts, seq))
}

fn parse_ts_seq_from_tail_be(bytes: &[u8]) -> Option<(u64, u32)> {
    if bytes.len() < 12 {
        return None;
    }
    let ts_bytes = &bytes[bytes.len() - 12..bytes.len() - 4];
    let seq_bytes = &bytes[bytes.len() - 4..];
    let mut ts_arr = [0u8; 8];
    let mut seq_arr = [0u8; 4];
    ts_arr.copy_from_slice(ts_bytes);
    seq_arr.copy_from_slice(seq_bytes);
    Some((u64::from_be_bytes(ts_arr), u32::from_be_bytes(seq_arr)))
}

fn push_spk(dst: &mut Vec<u8>, spk: &[u8]) {
    let len = spk.len().min(u16::MAX as usize) as u16;
    dst.extend_from_slice(&len.to_be_bytes());
    dst.extend_from_slice(&spk[..len as usize]);
}

fn read_address_pool_events(
    provider: &AmmDataProvider,
    prefix: Vec<u8>,
    offset: usize,
    limit: usize,
) -> Result<GetAddressPoolEventsPageResult> {
    let entries = match provider.get_iter_prefix_rev(GetIterPrefixRevParams { prefix: prefix.clone() })
    {
        Ok(v) => v.entries,
        Err(_) => Vec::new(),
    };

    let mut parsed: Vec<AddressPoolEventEntry> = Vec::new();
    for (k, _v) in entries {
        if !k.starts_with(&prefix) {
            continue;
        }
        if k.len() < prefix.len() + 24 {
            continue;
        }
        let pool_bytes = &k[k.len() - 12..];
        let ts_seq_bytes = &k[k.len() - 24..k.len() - 12];
        let (ts, seq) = match parse_ts_seq_from_tail_be(ts_seq_bytes) {
            Some(v) => v,
            None => continue,
        };
        let pool = match decode_alkane_id_be(pool_bytes) {
            Some(p) => p,
            None => continue,
        };
        parsed.push(AddressPoolEventEntry { ts, seq, pool });
    }

    let total = parsed.len();
    let offset = offset.min(total);
    let end = (offset + limit).min(total);
    let page = if offset >= total { &[] } else { &parsed[offset..end] };

    Ok(GetAddressPoolEventsPageResult { entries: page.to_vec(), total })
}

#[derive(Clone)]
pub struct AmmDataProvider {
    mdb: Arc<Mdb>,
    essentials: Arc<EssentialsProvider>,
}

impl AmmDataProvider {
    pub fn new(mdb: Arc<Mdb>, essentials: Arc<EssentialsProvider>) -> Self {
        Self { mdb, essentials }
    }

    pub fn table(&self) -> AmmDataTable<'_> {
        AmmDataTable::new(self.mdb.as_ref())
    }

    pub fn essentials(&self) -> &EssentialsProvider {
        self.essentials.as_ref()
    }

    pub fn get_raw_value(&self, params: GetRawValueParams) -> Result<GetRawValueResult> {
        let value = self.mdb.get(&params.key).map_err(|e| anyhow!("mdb.get failed: {e}"))?;
        Ok(GetRawValueResult { value })
    }

    pub fn get_multi_values(&self, params: GetMultiValuesParams) -> Result<GetMultiValuesResult> {
        let values =
            self.mdb.multi_get(&params.keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}"))?;
        Ok(GetMultiValuesResult { values })
    }

    pub fn get_scan_prefix(&self, params: GetScanPrefixParams) -> Result<GetScanPrefixResult> {
        let keys = self
            .mdb
            .scan_prefix(&params.prefix)
            .map_err(|e| anyhow!("mdb.scan_prefix failed: {e}"))?;
        Ok(GetScanPrefixResult { keys })
    }

    pub fn get_iter_prefix_rev(
        &self,
        params: GetIterPrefixRevParams,
    ) -> Result<GetIterPrefixRevResult> {
        let full_prefix = self.mdb.prefixed(&params.prefix);
        let mut entries = Vec::new();
        for res in self.mdb.iter_prefix_rev(&full_prefix) {
            let (k_full, v) = res.map_err(|e| anyhow!("mdb.iter_prefix_rev failed: {e}"))?;
            let rel = &k_full[self.mdb.prefix().len()..];
            entries.push((rel.to_vec(), v));
        }
        Ok(GetIterPrefixRevResult { entries })
    }

    pub fn get_iter_from(&self, params: GetIterFromParams) -> Result<GetIterFromResult> {
        let mut entries = Vec::new();
        for res in self.mdb.iter_from(&params.start) {
            let (k_full, v) = res.map_err(|e| anyhow!("mdb.iter_from failed: {e}"))?;
            let rel = &k_full[self.mdb.prefix().len()..];
            entries.push((rel.to_vec(), v));
        }
        Ok(GetIterFromResult { entries })
    }

    pub fn set_raw_value(&self, params: SetRawValueParams) -> Result<()> {
        self.mdb
            .put(&params.key, &params.value)
            .map_err(|e| anyhow!("mdb.put failed: {e}"))
    }

    pub fn set_batch(&self, params: SetBatchParams) -> Result<()> {
        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &params.deletes {
                    wb.delete(key);
                }
                for (key, value) in &params.puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }

    pub fn get_index_height(&self, _params: GetIndexHeightParams) -> Result<GetIndexHeightResult> {
        let table = self.table();
        let Some(bytes) = table.INDEX_HEIGHT.get()? else {
            return Ok(GetIndexHeightResult { height: None });
        };
        if bytes.len() != 4 {
            return Err(anyhow!("invalid /index_height length {}", bytes.len()));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(GetIndexHeightResult { height: Some(u32::from_le_bytes(arr)) })
    }

    pub fn set_index_height(&self, params: SetIndexHeightParams) -> Result<()> {
        let table = self.table();
        table.INDEX_HEIGHT.put(&params.height.to_le_bytes())
    }

    pub fn get_reserves_snapshot(
        &self,
        _params: GetReservesSnapshotParams,
    ) -> Result<GetReservesSnapshotResult> {
        let table = self.table();
        let snapshot = match table.RESERVES_SNAPSHOT.get()? {
            Some(bytes) => decode_reserves_snapshot(&bytes).ok(),
            None => None,
        };
        Ok(GetReservesSnapshotResult { snapshot })
    }

    pub fn set_reserves_snapshot(
        &self,
        params: SetReservesSnapshotParams,
    ) -> Result<()> {
        let table = self.table();
        let encoded = encode_reserves_snapshot(&params.snapshot)?;
        table.RESERVES_SNAPSHOT.put(&encoded)
    }

    pub fn get_canonical_pools(
        &self,
        params: GetCanonicalPoolsParams,
    ) -> Result<GetCanonicalPoolsResult> {
        let table = self.table();
        let pools = self
            .get_raw_value(GetRawValueParams { key: table.canonical_pool_key(&params.token) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_canonical_pools(&raw).ok())
            .unwrap_or_default();
        Ok(GetCanonicalPoolsResult { pools })
    }

    pub fn get_latest_token_usd_close(
        &self,
        params: GetLatestTokenUsdCloseParams,
    ) -> Result<GetLatestTokenUsdCloseResult> {
        let table = self.table();
        let prefix = table.token_usd_candle_ns_prefix(&params.token, params.timeframe);
        let entries = self
            .get_iter_prefix_rev(GetIterPrefixRevParams { prefix })
            .ok()
            .map(|resp| resp.entries)
            .unwrap_or_default();
        let close = entries
            .into_iter()
            .next()
            .and_then(|(_k, v)| decode_candle_v1(&v).ok())
            .map(|c| c.close);
        Ok(GetLatestTokenUsdCloseResult { close })
    }

    pub fn get_token_metrics(
        &self,
        params: GetTokenMetricsParams,
    ) -> Result<GetTokenMetricsResult> {
        let table = self.table();
        let metrics = self
            .get_raw_value(GetRawValueParams { key: table.token_metrics_key(&params.token) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_token_metrics(&raw).ok())
            .unwrap_or_default();
        Ok(GetTokenMetricsResult { metrics })
    }

    pub fn get_pool_ids_by_name_prefix(
        &self,
        params: GetPoolIdsByNamePrefixParams,
    ) -> Result<GetPoolIdsByNamePrefixResult> {
        let table = self.table();
        let keys = match self.get_scan_prefix(GetScanPrefixParams {
            prefix: table.pool_name_index_prefix(&params.prefix),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut ids = Vec::new();
        let mut seen = HashSet::new();
        for key in keys {
            if let Some((_name, id)) = table.parse_pool_name_index_key(&key) {
                if seen.insert(id) {
                    ids.push(id);
                }
            }
        }
        Ok(GetPoolIdsByNamePrefixResult { ids })
    }

    pub fn get_amm_factories(
        &self,
        _params: GetAmmFactoriesParams,
    ) -> Result<GetAmmFactoriesResult> {
        let table = self.table();
        let keys = match self.get_scan_prefix(GetScanPrefixParams {
            prefix: table.AMM_FACTORIES.key().to_vec(),
        }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut ids = Vec::new();
        for key in keys {
            if let Some(id) = parse_alkane_id_from_prefixed_key(table.AMM_FACTORIES.key(), &key) {
                ids.push(id);
            }
        }
        Ok(GetAmmFactoriesResult { factories: ids })
    }

    pub fn get_factory_pools(
        &self,
        params: GetFactoryPoolsParams,
    ) -> Result<GetFactoryPoolsResult> {
        let table = self.table();
        let prefix = table.factory_pools_prefix(&params.factory);
        let keys = match self.get_scan_prefix(GetScanPrefixParams { prefix: prefix.clone() }) {
            Ok(v) => v.keys,
            Err(_) => Vec::new(),
        };
        let mut pools = Vec::new();
        for key in keys {
            if let Some(id) = parse_alkane_id_from_prefixed_key(prefix.as_slice(), &key) {
                pools.push(id);
            }
        }
        Ok(GetFactoryPoolsResult { pools })
    }

    pub fn get_pool_defs(&self, params: GetPoolDefsParams) -> Result<GetPoolDefsResult> {
        let table = self.table();
        let defs = self
            .get_raw_value(GetRawValueParams { key: table.pools_key(&params.pool) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| SchemaMarketDefs::try_from_slice(&raw).ok());
        Ok(GetPoolDefsResult { defs })
    }

    pub fn get_pool_factory(
        &self,
        params: GetPoolFactoryParams,
    ) -> Result<GetPoolFactoryResult> {
        let table = self.table();
        let factory = self
            .get_raw_value(GetRawValueParams { key: table.pool_factory_key(&params.pool) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_alkane_id_be(&raw));
        Ok(GetPoolFactoryResult { factory })
    }

    pub fn get_pool_metrics(
        &self,
        params: GetPoolMetricsParams,
    ) -> Result<GetPoolMetricsResult> {
        let table = self.table();
        let metrics = self
            .get_raw_value(GetRawValueParams { key: table.pool_metrics_key(&params.pool) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_pool_metrics(&raw).ok())
            .unwrap_or_default();
        Ok(GetPoolMetricsResult { metrics })
    }

    pub fn get_pool_creation_info(
        &self,
        params: GetPoolCreationInfoParams,
    ) -> Result<GetPoolCreationInfoResult> {
        let table = self.table();
        let info = self
            .get_raw_value(GetRawValueParams { key: table.pool_creation_info_key(&params.pool) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_pool_creation_info(&raw).ok());
        Ok(GetPoolCreationInfoResult { info })
    }

    pub fn get_pool_lp_supply_latest(
        &self,
        params: GetPoolLpSupplyLatestParams,
    ) -> Result<GetPoolLpSupplyLatestResult> {
        let table = self.table();
        let supply = self
            .get_raw_value(GetRawValueParams { key: table.pool_lp_supply_latest_key(&params.pool) })
            .ok()
            .and_then(|resp| resp.value)
            .and_then(|raw| decode_u128_value(&raw).ok())
            .unwrap_or(0);
        Ok(GetPoolLpSupplyLatestResult { supply })
    }

    pub fn get_pool_activity_entries(
        &self,
        params: GetPoolActivityEntriesParams,
    ) -> Result<GetPoolActivityEntriesResult> {
        let table = self.table();
        let prefix = table.activity_ns_prefix(&params.pool);
        let entries = match self.get_iter_prefix_rev(GetIterPrefixRevParams { prefix }) {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut total = 0usize;
        let mut out: Vec<SchemaActivityV1> = Vec::new();
        let mut seen = 0usize;
        for (_k, v) in entries {
            let entry = match decode_activity_v1(&v) {
                Ok(e) => e,
                Err(_) => continue,
            };
            if let Some(ref kinds) = params.kinds {
                if !kinds.contains(&entry.kind) {
                    continue;
                }
            }
            if let Some(want) = params.successful {
                if want && !entry.success {
                    continue;
                }
            }
            total += 1;
            if seen < params.offset {
                seen += 1;
                continue;
            }
            if out.len() < params.limit {
                out.push(entry);
            }
            if out.len() >= params.limit && params.offset + out.len() >= total && !params.include_total {
                break;
            }
        }

        Ok(GetPoolActivityEntriesResult { entries: out, total })
    }

    pub fn get_activity_entry(
        &self,
        params: GetActivityEntryParams,
    ) -> Result<GetActivityEntryResult> {
        let table = self.table();
        let key = table.activity_key(&params.pool, params.ts, params.seq);
        let entry = self
            .get_raw_value(GetRawValueParams { key })?
            .value
            .and_then(|raw| decode_activity_v1(&raw).ok());
        Ok(GetActivityEntryResult { entry })
    }

    pub fn get_token_swaps_page(
        &self,
        params: GetTokenSwapsPageParams,
    ) -> Result<GetTokenSwapsPageResult> {
        let table = self.table();
        let prefix = table.token_swaps_prefix(&params.token);
        let entries = match self.get_iter_prefix_rev(GetIterPrefixRevParams { prefix: prefix.clone() })
        {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<TokenSwapEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            let mut parts = rest.splitn(2, |b| *b == b'/');
            let ts_seq = parts.next().unwrap_or(&[]);
            let pool_bytes = parts.next().unwrap_or(&[]);
            let (ts, seq) = match parse_ts_seq_from_key(ts_seq) {
                Some(v) => v,
                None => continue,
            };
            let pool = match decode_alkane_id_be(pool_bytes) {
                Some(p) => p,
                None => continue,
            };
            parsed.push(TokenSwapEntry { ts, seq, pool });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetTokenSwapsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_pool_creations_page(
        &self,
        params: GetPoolCreationsPageParams,
    ) -> Result<GetPoolCreationsPageResult> {
        let table = self.table();
        let prefix = table.pool_creations_prefix();
        let entries = match self.get_iter_prefix_rev(GetIterPrefixRevParams { prefix: prefix.clone() })
        {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<PoolCreationEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            let mut parts = rest.splitn(2, |b| *b == b'/');
            let ts_seq = parts.next().unwrap_or(&[]);
            let pool_bytes = parts.next().unwrap_or(&[]);
            let (ts, seq) = match parse_ts_seq_from_key(ts_seq) {
                Some(v) => v,
                None => continue,
            };
            let pool = match decode_alkane_id_be(pool_bytes) {
                Some(p) => p,
                None => continue,
            };
            parsed.push(PoolCreationEntry { ts, seq, pool });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetPoolCreationsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_address_pool_swaps_page(
        &self,
        params: GetAddressPoolSwapsPageParams,
    ) -> Result<GetAddressPoolSwapsPageResult> {
        let table = self.table();
        let prefix = table.address_pool_swaps_prefix(&params.address_spk, &params.pool);
        let entries = match self.get_iter_prefix_rev(GetIterPrefixRevParams { prefix: prefix.clone() })
        {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<AddressPoolSwapEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            let rest = &k[prefix.len()..];
            let (ts, seq) = match parse_ts_seq_from_tail_be(rest) {
                Some(v) => v,
                None => continue,
            };
            parsed.push(AddressPoolSwapEntry { ts, seq });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetAddressPoolSwapsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_address_token_swaps_page(
        &self,
        params: GetAddressTokenSwapsPageParams,
    ) -> Result<GetAddressTokenSwapsPageResult> {
        let table = self.table();
        let prefix = table.address_token_swaps_prefix(&params.address_spk, &params.token);
        let entries = match self.get_iter_prefix_rev(GetIterPrefixRevParams { prefix: prefix.clone() })
        {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };

        let mut parsed: Vec<AddressTokenSwapEntry> = Vec::new();
        for (k, _v) in entries {
            if !k.starts_with(&prefix) {
                continue;
            }
            if k.len() < prefix.len() + 24 {
                continue;
            }
            let pool_bytes = &k[k.len() - 12..];
            let ts_seq_bytes = &k[k.len() - 24..k.len() - 12];
            let (ts, seq) = match parse_ts_seq_from_tail_be(ts_seq_bytes) {
                Some(v) => v,
                None => continue,
            };
            let pool = match decode_alkane_id_be(pool_bytes) {
                Some(p) => p,
                None => continue,
            };
            parsed.push(AddressTokenSwapEntry { ts, seq, pool });
        }

        let total = parsed.len();
        let offset = params.offset.min(total);
        let end = (offset + params.limit).min(total);
        let page = if offset >= total { &[] } else { &parsed[offset..end] };

        Ok(GetAddressTokenSwapsPageResult { entries: page.to_vec(), total })
    }

    pub fn get_address_pool_creations_page(
        &self,
        params: GetAddressPoolCreationsPageParams,
    ) -> Result<GetAddressPoolEventsPageResult> {
        let table = self.table();
        let prefix = table.address_pool_creations_prefix(&params.address_spk);
        read_address_pool_events(self, prefix, params.offset, params.limit)
    }

    pub fn get_address_pool_mints_page(
        &self,
        params: GetAddressPoolMintsPageParams,
    ) -> Result<GetAddressPoolEventsPageResult> {
        let table = self.table();
        let prefix = table.address_pool_mints_prefix(&params.address_spk);
        read_address_pool_events(self, prefix, params.offset, params.limit)
    }

    pub fn get_address_pool_burns_page(
        &self,
        params: GetAddressPoolBurnsPageParams,
    ) -> Result<GetAddressPoolEventsPageResult> {
        let table = self.table();
        let prefix = table.address_pool_burns_prefix(&params.address_spk);
        read_address_pool_events(self, prefix, params.offset, params.limit)
    }

    pub fn get_tvl_versioned_at_or_before(
        &self,
        params: GetTvlVersionedAtOrBeforeParams,
    ) -> Result<GetTvlVersionedAtOrBeforeResult> {
        let table = self.table();
        let prefix = table.tvl_versioned_prefix(&params.pool);
        let entries = match self.get_iter_prefix_rev(GetIterPrefixRevParams { prefix }) {
            Ok(v) => v.entries,
            Err(_) => Vec::new(),
        };
        let mut value = None;
        for (k, v) in entries {
            let height = parse_height_from_tvl_key(&k)?;
            if height <= params.height {
                value = decode_u128_value(&v).ok();
                break;
            }
        }
        Ok(GetTvlVersionedAtOrBeforeResult { value })
    }

    pub fn get_canonical_pool_prices(
        &self,
        params: GetCanonicalPoolPricesParams,
    ) -> Result<GetCanonicalPoolPricesResult> {
        let mut frbtc_price = 0u128;
        let mut busd_price = 0u128;
        let mut unit_map: HashMap<SchemaAlkaneId, CanonicalQuoteUnit> = HashMap::new();
        for cq in canonical_quotes(get_network()) {
            unit_map.insert(cq.id, cq.unit);
        }
        let pools = self.get_canonical_pools(GetCanonicalPoolsParams { token: params.token })?.pools;
        for entry in pools {
            let unit = match unit_map.get(&entry.quote_id) {
                Some(u) => *u,
                None => continue,
            };
            let res = read_candles_v1(
                self,
                entry.pool_id,
                Timeframe::M10,
                1,
                params.now_ts,
                PriceSide::Base,
            )
            .ok();
            let close = res
                .and_then(|slice| slice.candles_newest_first.first().copied())
                .map(|c| c.close)
                .unwrap_or(0);
            match unit {
                CanonicalQuoteUnit::Btc => frbtc_price = close,
                CanonicalQuoteUnit::Usd => busd_price = close,
            }
        }
        Ok(GetCanonicalPoolPricesResult { frbtc_price, busd_price })
    }

    pub fn rpc_get_candles(&self, params: RpcGetCandlesParams) -> Result<RpcGetCandlesResult> {
        let tf = params
            .timeframe
            .as_deref()
            .and_then(parse_timeframe)
            .unwrap_or(Timeframe::H1);

        let legacy_size = params.size.map(|n| n as usize);
        let limit = params.limit.map(|n| n as usize).or(legacy_size).unwrap_or(120);
        let page = params.page.map(|n| n as usize).unwrap_or(1);

        let side = params
            .side
            .as_deref()
            .and_then(parse_price_side)
            .unwrap_or(PriceSide::Base);

        let now = params.now.unwrap_or_else(now_ts);

        let pool_raw = match params.pool.as_deref() {
            Some(p) => p,
            None => {
                return Ok(RpcGetCandlesResult {
                    value: json!({
                        "ok": false,
                        "error": "missing_or_invalid_pool",
                        "hint": "pool should be a string like \"2:68441\" or \"2:0-usd\""
                    }),
                });
            }
        };

        let (pool, is_usd) = if let Some(stripped) = pool_raw.strip_suffix("-usd") {
            match parse_id_from_str(stripped) {
                Some(p) => (p, true),
                None => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\" or \"2:0-usd\""
                        }),
                    });
                }
            }
        } else {
            match parse_id_from_str(pool_raw) {
                Some(p) => (p, false),
                None => {
                    return Ok(RpcGetCandlesResult {
                        value: json!({
                            "ok": false,
                            "error": "missing_or_invalid_pool",
                            "hint": "pool should be a string like \"2:68441\" or \"2:0-usd\""
                        }),
                    });
                }
            }
        };

        let slice = if is_usd {
            read_token_usd_candles_v1(self, pool, tf, now)
        } else {
            read_candles_v1(self, pool, tf, /*unused*/ limit, now, side)
        };

        match slice {
            Ok(slice) => {
                let total = slice.candles_newest_first.len();
                let dur = tf.duration_secs();
                let newest_ts = slice.newest_ts;

                let offset = limit.saturating_mul(page.saturating_sub(1));
                let end = (offset + limit).min(total);
                let page_slice = if offset >= total {
                    &[][..]
                } else {
                    &slice.candles_newest_first[offset..end]
                };

                let arr: Vec<Value> = page_slice
                    .iter()
                    .enumerate()
                    .map(|(i, c)| {
                        let global_idx = offset + i;
                        let ts = newest_ts.saturating_sub((global_idx as u64) * dur);
                        json!({
                            "ts":     ts,
                            "open":   scale_u128(c.open),
                            "high":   scale_u128(c.high),
                            "low":    scale_u128(c.low),
                            "close":  scale_u128(c.close),
                            "volume": scale_u128(c.volume),
                        })
                    })
                    .collect();

                Ok(RpcGetCandlesResult {
                    value: json!({
                        "ok": true,
                        "pool": if is_usd {
                            format!("{}-usd", id_str(&pool))
                        } else {
                            id_str(&pool)
                        },
                        "timeframe": tf.code(),
                        "side": if is_usd { "base" } else { match side { PriceSide::Base => "base", PriceSide::Quote => "quote" } },
                        "page": page,
                        "limit": limit,
                        "total": total,
                        "has_more": end < total,
                        "candles": arr
                    }),
                })
            }
            Err(e) => Ok(RpcGetCandlesResult {
                value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
            }),
        }
    }

    pub fn rpc_get_activity(&self, params: RpcGetActivityParams) -> Result<RpcGetActivityResult> {
        let limit = params.limit.map(|n| n as usize).unwrap_or(50);
        let page = params.page.map(|n| n as usize).unwrap_or(1);

        let side = params
            .side
            .as_deref()
            .and_then(parse_price_side)
            .unwrap_or(PriceSide::Base);

        let filter_side = parse_side_filter_str(params.filter_side.as_deref());
        let activity_type = parse_activity_type_str(params.activity_type.as_deref());

        let sort_token: Option<String> = params.sort.clone();
        let dir = parse_sort_dir_str(params.dir.as_deref());
        let (sort_key, sort_code) = map_sort(side, sort_token.as_deref());

        let pool = match params.pool.as_deref().and_then(parse_id_from_str) {
            Some(p) => p,
            None => {
                return Ok(RpcGetActivityResult {
                    value: json!({
                        "ok": false,
                        "error": "missing_or_invalid_pool",
                        "hint": "pool should be a string like \"2:68441\""
                    }),
                });
            }
        };

        if sort_token.is_some()
            || !matches!(filter_side, ActivitySideFilter::All)
            || !matches!(activity_type, ActivityFilter::All)
        {
            match read_activity_for_pool_sorted(
                self,
                pool,
                page,
                limit,
                side,
                sort_key,
                dir,
                filter_side,
                activity_type,
            ) {
                Ok(ActivityPage { activity, total }) => Ok(RpcGetActivityResult {
                    value: json!({
                        "ok": true,
                        "pool": id_str(&pool),
                        "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                        "filter_side": match filter_side {
                            ActivitySideFilter::All => "all",
                            ActivitySideFilter::Buy => "buy",
                            ActivitySideFilter::Sell => "sell"
                        },
                        "activity_type": match activity_type {
                            ActivityFilter::All => "all",
                            ActivityFilter::Trades => "trades",
                            ActivityFilter::Events => "events",
                        },
                        "sort": sort_code,
                        "dir": match dir { SortDir::Asc => "asc", SortDir::Desc => "desc" },
                        "page": page,
                        "limit": limit,
                        "total": total,
                        "has_more": page.saturating_mul(limit) < total,
                        "activity": activity
                    }),
                }),
                Err(e) => Ok(RpcGetActivityResult {
                    value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
                }),
            }
        } else {
            match read_activity_for_pool(self, pool, page, limit, side, activity_type) {
                Ok(ActivityPage { activity, total }) => Ok(RpcGetActivityResult {
                    value: json!({
                        "ok": true,
                        "pool": id_str(&pool),
                        "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
                        "filter_side": "all",
                        "activity_type": "all",
                        "sort": "ts",
                        "dir": "desc",
                        "page": page,
                        "limit": limit,
                        "total": total,
                        "has_more": page.saturating_mul(limit) < total,
                        "activity": activity
                    }),
                }),
                Err(e) => Ok(RpcGetActivityResult {
                    value: json!({ "ok": false, "error": format!("read_failed: {e}") }),
                }),
            }
        }
    }

    pub fn rpc_get_pools(&self, params: RpcGetPoolsParams) -> Result<RpcGetPoolsResult> {
        let live_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = match fetch_all_pools(self) {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcGetPoolsResult {
                    value: json!({
                        "ok": false,
                        "error": "live_fetch_failed",
                        "hint": "could not load live reserves"
                    }),
                });
            }
        };

        let mut rows: Vec<(SchemaAlkaneId, SchemaPoolSnapshot)> = live_map.into_iter().collect();
        rows.sort_by(|(a, _), (b, _)| a.block.cmp(&b.block).then(a.tx.cmp(&b.tx)));
        let total = rows.len();

        let limit = params
            .limit
            .map(|n| n as usize)
            .unwrap_or(total.max(1))
            .clamp(1, 20_000);
        let page = params.page.map(|n| n as usize).unwrap_or(1).max(1);

        let offset = limit.saturating_mul(page.saturating_sub(1));
        let end = (offset + limit).min(total);
        let window = if offset >= total { &[][..] } else { &rows[offset..end] };
        let has_more = end < total;

        let mut pools_obj: Map<String, Value> = Map::with_capacity(window.len());
        for (pool, snap) in window.iter() {
            pools_obj.insert(
                format!("{}:{}", pool.block, pool.tx),
                json!({
                    "base":          format!("{}:{}", snap.base_id.block,  snap.base_id.tx),
                    "quote":         format!("{}:{}", snap.quote_id.block, snap.quote_id.tx),
                    "base_reserve":  snap.base_reserve.to_string(),
                    "quote_reserve": snap.quote_reserve.to_string(),
                    "source": "live",
                }),
            );
        }

        Ok(RpcGetPoolsResult {
            value: json!({
                "ok": true,
                "page": page,
                "limit": limit,
                "total": total,
                "has_more": has_more,
                "pools": Value::Object(pools_obj)
            }),
        })
    }

    pub fn rpc_find_best_swap_path(
        &self,
        params: RpcFindBestSwapPathParams,
    ) -> Result<RpcFindBestSwapPathResult> {
        let snapshot_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = match fetch_all_pools(self) {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({
                        "ok": false,
                        "error": "no_liquidity",
                        "hint": "live reserves unavailable"
                    }),
                });
            }
        };

        if snapshot_map.is_empty() {
            return Ok(RpcFindBestSwapPathResult {
                value: json!({
                    "ok": false,
                    "error": "no_liquidity",
                    "hint": "live reserves map is empty"
                }),
            });
        }

        let mode = params
            .mode
            .as_deref()
            .unwrap_or("exact_in")
            .to_ascii_lowercase();

        let token_in = match params.token_in.as_deref().and_then(parse_id_from_str) {
            Some(t) => t,
            None => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_token_in"}),
                });
            }
        };
        let token_out = match params.token_out.as_deref().and_then(parse_id_from_str) {
            Some(t) => t,
            None => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_token_out"}),
                });
            }
        };

        let fee_bps = params.fee_bps.map(|n| n as u32).unwrap_or(DEFAULT_FEE_BPS);
        let max_hops = params
            .max_hops
            .map(|n| n as usize)
            .unwrap_or(3)
            .max(1)
            .min(6);

        let plan = match mode.as_str() {
            "exact_in" => {
                let amount_in = match parse_u128_arg(params.amount_in.as_ref()) {
                    Some(v) => v,
                    None => {
                        return Ok(RpcFindBestSwapPathResult {
                            value: json!({"ok": false, "error": "missing_or_invalid_amount_in"}),
                        });
                    }
                };
                let min_out = parse_u128_arg(params.amount_out_min.as_ref()).unwrap_or(0u128);

                if params.fee_bps.is_some() {
                    plan_swap_exact_tokens_for_tokens(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_in,
                        min_out,
                        fee_bps,
                        max_hops,
                    )
                } else {
                    plan_exact_in_default_fee(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_in,
                        min_out,
                        max_hops,
                    )
                }
            }
            "exact_out" => {
                let amount_out = match parse_u128_arg(params.amount_out.as_ref()) {
                    Some(v) => v,
                    None => {
                        return Ok(RpcFindBestSwapPathResult {
                            value: json!({"ok": false, "error": "missing_or_invalid_amount_out"}),
                        });
                    }
                };
                let in_max = parse_u128_arg(params.amount_in_max.as_ref()).unwrap_or(u128::MAX);

                if params.fee_bps.is_some() {
                    plan_swap_tokens_for_exact_tokens(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_out,
                        in_max,
                        fee_bps,
                        max_hops,
                    )
                } else {
                    plan_exact_out_default_fee(
                        &snapshot_map,
                        token_in,
                        token_out,
                        amount_out,
                        in_max,
                        max_hops,
                    )
                }
            }
            "implicit" => {
                let available_in = match parse_u128_arg(
                    params.amount_in.as_ref().or(params.available_in.as_ref()),
                ) {
                    Some(v) => v,
                    None => {
                        return Ok(RpcFindBestSwapPathResult {
                            value: json!({"ok": false, "error": "missing_or_invalid_amount_in"}),
                        });
                    }
                };
                let min_out = parse_u128_arg(params.amount_out_min.as_ref()).unwrap_or(0u128);

                if params.fee_bps.is_some() {
                    plan_swap_exact_tokens_for_tokens_implicit(
                        &snapshot_map,
                        token_in,
                        token_out,
                        available_in,
                        min_out,
                        fee_bps,
                        max_hops,
                    )
                } else {
                    plan_implicit_default_fee(
                        &snapshot_map,
                        token_in,
                        token_out,
                        available_in,
                        min_out,
                        max_hops,
                    )
                }
            }
            _ => {
                return Ok(RpcFindBestSwapPathResult {
                    value: json!({
                        "ok": false,
                        "error": "invalid_mode",
                        "hint": "use exact_in | exact_out | implicit"
                    }),
                });
            }
        };

        match plan {
            Some(pq) => {
                let hops: Vec<Value> = pq
                    .hops
                    .iter()
                    .map(|h| {
                        json!({
                            "pool":       id_str(&h.pool),
                            "token_in":   id_str(&h.token_in),
                            "token_out":  id_str(&h.token_out),
                            "amount_in":  h.amount_in.to_string(),
                            "amount_out": h.amount_out.to_string(),
                        })
                    })
                    .collect();

                Ok(RpcFindBestSwapPathResult {
                    value: json!({
                        "ok": true,
                        "mode": mode,
                        "token_in":  id_str(&token_in),
                        "token_out": id_str(&token_out),
                        "fee_bps": fee_bps,
                        "max_hops": max_hops,
                        "amount_in":  pq.amount_in.to_string(),
                        "amount_out": pq.amount_out.to_string(),
                        "hops": hops
                    }),
                })
            }
            None => Ok(RpcFindBestSwapPathResult {
                value: json!({"ok": false, "error": "no_path_found"}),
            }),
        }
    }

    pub fn rpc_get_best_mev_swap(
        &self,
        params: RpcGetBestMevSwapParams,
    ) -> Result<RpcGetBestMevSwapResult> {
        let snapshot_map: HashMap<SchemaAlkaneId, SchemaPoolSnapshot> = match fetch_all_pools(self) {
            Ok(m) => m,
            Err(_) => {
                return Ok(RpcGetBestMevSwapResult {
                    value: json!({
                        "ok": false,
                        "error": "no_liquidity",
                        "hint": "live reserves unavailable"
                    }),
                });
            }
        };

        if snapshot_map.is_empty() {
            return Ok(RpcGetBestMevSwapResult {
                value: json!({
                    "ok": false,
                    "error": "no_liquidity",
                    "hint": "live reserves map is empty"
                }),
            });
        }

        let token = match params.token.as_deref().and_then(parse_id_from_str) {
            Some(t) => t,
            None => {
                return Ok(RpcGetBestMevSwapResult {
                    value: json!({"ok": false, "error": "missing_or_invalid_token"}),
                });
            }
        };
        let fee_bps = params.fee_bps.map(|n| n as u32).unwrap_or(DEFAULT_FEE_BPS);
        let max_hops = params
            .max_hops
            .map(|n| n as usize)
            .unwrap_or(3)
            .clamp(2, 6);

        match plan_best_mev_swap(&snapshot_map, token, fee_bps, max_hops) {
            Some(pq) => {
                let hops: Vec<Value> = pq
                    .hops
                    .iter()
                    .map(|h| {
                        json!({
                            "pool":       id_str(&h.pool),
                            "token_in":   id_str(&h.token_in),
                            "token_out":  id_str(&h.token_out),
                            "amount_in":  h.amount_in.to_string(),
                            "amount_out": h.amount_out.to_string(),
                        })
                    })
                    .collect();

                Ok(RpcGetBestMevSwapResult {
                    value: json!({
                        "ok": true,
                        "token":   id_str(&token),
                        "fee_bps": fee_bps,
                        "max_hops": max_hops,
                        "amount_in":  pq.amount_in.to_string(),
                        "amount_out": pq.amount_out.to_string(),
                        "profit": (pq.amount_out as i128 - pq.amount_in as i128).to_string(),
                        "hops": hops
                    }),
                })
            }
            None => Ok(RpcGetBestMevSwapResult {
                value: json!({"ok": false, "error": "no_profitable_cycle"}),
            }),
        }
    }

    pub fn rpc_ping(&self, _params: RpcPingParams) -> Result<RpcPingResult> {
        Ok(RpcPingResult { value: Value::String("pong".to_string()) })
    }
}

pub struct GetRawValueParams {
    pub key: Vec<u8>,
}

pub struct GetRawValueResult {
    pub value: Option<Vec<u8>>,
}

pub struct GetMultiValuesParams {
    pub keys: Vec<Vec<u8>>,
}

pub struct GetMultiValuesResult {
    pub values: Vec<Option<Vec<u8>>>,
}

pub struct GetScanPrefixParams {
    pub prefix: Vec<u8>,
}

pub struct GetScanPrefixResult {
    pub keys: Vec<Vec<u8>>,
}

pub struct GetIterPrefixRevParams {
    pub prefix: Vec<u8>,
}

pub struct GetIterPrefixRevResult {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct GetIterFromParams {
    pub start: Vec<u8>,
}

pub struct GetIterFromResult {
    pub entries: Vec<(Vec<u8>, Vec<u8>)>,
}

pub struct SetRawValueParams {
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct SetBatchParams {
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<Vec<u8>>,
}

pub struct GetIndexHeightParams;

pub struct GetIndexHeightResult {
    pub height: Option<u32>,
}

pub struct SetIndexHeightParams {
    pub height: u32,
}

pub struct GetReservesSnapshotParams;

pub struct GetReservesSnapshotResult {
    pub snapshot: Option<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>>,
}

pub struct GetCanonicalPoolsParams {
    pub token: SchemaAlkaneId,
}

pub struct GetCanonicalPoolsResult {
    pub pools: Vec<SchemaCanonicalPoolEntry>,
}

pub struct GetLatestTokenUsdCloseParams {
    pub token: SchemaAlkaneId,
    pub timeframe: Timeframe,
}

pub struct GetLatestTokenUsdCloseResult {
    pub close: Option<u128>,
}

pub struct GetTokenMetricsParams {
    pub token: SchemaAlkaneId,
}

pub struct GetTokenMetricsResult {
    pub metrics: SchemaTokenMetricsV1,
}

pub struct GetPoolIdsByNamePrefixParams {
    pub prefix: String,
}

pub struct GetPoolIdsByNamePrefixResult {
    pub ids: Vec<SchemaAlkaneId>,
}

pub struct GetAmmFactoriesParams;

pub struct GetAmmFactoriesResult {
    pub factories: Vec<SchemaAlkaneId>,
}

pub struct GetFactoryPoolsParams {
    pub factory: SchemaAlkaneId,
}

pub struct GetFactoryPoolsResult {
    pub pools: Vec<SchemaAlkaneId>,
}

pub struct GetPoolDefsParams {
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolDefsResult {
    pub defs: Option<SchemaMarketDefs>,
}

pub struct GetPoolFactoryParams {
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolFactoryResult {
    pub factory: Option<SchemaAlkaneId>,
}

pub struct GetPoolMetricsParams {
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolMetricsResult {
    pub metrics: SchemaPoolMetricsV1,
}

pub struct GetPoolCreationInfoParams {
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolCreationInfoResult {
    pub info: Option<SchemaPoolCreationInfoV1>,
}

pub struct GetPoolLpSupplyLatestParams {
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolLpSupplyLatestResult {
    pub supply: u128,
}

pub struct GetPoolActivityEntriesParams {
    pub pool: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
    pub kinds: Option<Vec<ActivityKind>>,
    pub successful: Option<bool>,
    pub include_total: bool,
}

pub struct GetPoolActivityEntriesResult {
    pub entries: Vec<SchemaActivityV1>,
    pub total: usize,
}

pub struct GetActivityEntryParams {
    pub pool: SchemaAlkaneId,
    pub ts: u64,
    pub seq: u32,
}

pub struct GetActivityEntryResult {
    pub entry: Option<SchemaActivityV1>,
}

#[derive(Clone, Debug)]
pub struct TokenSwapEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetTokenSwapsPageParams {
    pub token: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetTokenSwapsPageResult {
    pub entries: Vec<TokenSwapEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct PoolCreationEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetPoolCreationsPageParams {
    pub offset: usize,
    pub limit: usize,
}

pub struct GetPoolCreationsPageResult {
    pub entries: Vec<PoolCreationEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AddressPoolSwapEntry {
    pub ts: u64,
    pub seq: u32,
}

pub struct GetAddressPoolSwapsPageParams {
    pub address_spk: Vec<u8>,
    pub pool: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolSwapsPageResult {
    pub entries: Vec<AddressPoolSwapEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AddressTokenSwapEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetAddressTokenSwapsPageParams {
    pub address_spk: Vec<u8>,
    pub token: SchemaAlkaneId,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressTokenSwapsPageResult {
    pub entries: Vec<AddressTokenSwapEntry>,
    pub total: usize,
}

#[derive(Clone, Debug)]
pub struct AddressPoolEventEntry {
    pub ts: u64,
    pub seq: u32,
    pub pool: SchemaAlkaneId,
}

pub struct GetAddressPoolCreationsPageParams {
    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolMintsPageParams {
    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolBurnsPageParams {
    pub address_spk: Vec<u8>,
    pub offset: usize,
    pub limit: usize,
}

pub struct GetAddressPoolEventsPageResult {
    pub entries: Vec<AddressPoolEventEntry>,
    pub total: usize,
}

pub struct GetTvlVersionedAtOrBeforeParams {
    pub pool: SchemaAlkaneId,
    pub height: u32,
}

pub struct GetTvlVersionedAtOrBeforeResult {
    pub value: Option<u128>,
}

pub struct GetCanonicalPoolPricesParams {
    pub token: SchemaAlkaneId,
    pub now_ts: u64,
}

pub struct GetCanonicalPoolPricesResult {
    pub frbtc_price: u128,
    pub busd_price: u128,
}

pub struct SetReservesSnapshotParams {
    pub snapshot: HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
}

pub struct RpcGetCandlesParams {
    pub pool: Option<String>,
    pub timeframe: Option<String>,
    pub limit: Option<u64>,
    pub size: Option<u64>,
    pub page: Option<u64>,
    pub side: Option<String>,
    pub now: Option<u64>,
}

pub struct RpcGetCandlesResult {
    pub value: Value,
}

pub struct RpcGetActivityParams {
    pub pool: Option<String>,
    pub limit: Option<u64>,
    pub page: Option<u64>,
    pub side: Option<String>,
    pub filter_side: Option<String>,
    pub activity_type: Option<String>,
    pub sort: Option<String>,
    pub dir: Option<String>,
}

pub struct RpcGetActivityResult {
    pub value: Value,
}

pub struct RpcGetPoolsParams {
    pub page: Option<u64>,
    pub limit: Option<u64>,
}

pub struct RpcGetPoolsResult {
    pub value: Value,
}

pub struct RpcFindBestSwapPathParams {
    pub mode: Option<String>,
    pub token_in: Option<String>,
    pub token_out: Option<String>,
    pub fee_bps: Option<u64>,
    pub max_hops: Option<u64>,
    pub amount_in: Option<Value>,
    pub amount_out_min: Option<Value>,
    pub amount_out: Option<Value>,
    pub amount_in_max: Option<Value>,
    pub available_in: Option<Value>,
}

pub struct RpcFindBestSwapPathResult {
    pub value: Value,
}

pub struct RpcGetBestMevSwapParams {
    pub token: Option<String>,
    pub fee_bps: Option<u64>,
    pub max_hops: Option<u64>,
}

pub struct RpcGetBestMevSwapResult {
    pub value: Value,
}

pub struct RpcPingParams;

pub struct RpcPingResult {
    pub value: Value,
}

/// Hex without "0x", lowercase
#[inline]
pub fn to_hex_no0x<T: Into<u128>>(x: T) -> String {
    format!("{:x}", x.into())
}

/// Deserialize helper
pub fn decode_full_candle_v1(bytes: &[u8]) -> anyhow::Result<SchemaFullCandleV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaFullCandleV1::try_from_slice(bytes)?)
}

/// Encode helper
pub fn encode_full_candle_v1(v: &SchemaFullCandleV1) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(64);
    v.serialize(&mut out)?;
    Ok(out)
}

pub fn decode_candle_v1(bytes: &[u8]) -> anyhow::Result<SchemaCandleV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaCandleV1::try_from_slice(bytes)?)
}

pub fn encode_candle_v1(v: &SchemaCandleV1) -> anyhow::Result<Vec<u8>> {
    let mut out = Vec::with_capacity(40);
    v.serialize(&mut out)?;
    Ok(out)
}

pub fn decode_canonical_pools(bytes: &[u8]) -> anyhow::Result<Vec<SchemaCanonicalPoolEntry>> {
    use borsh::BorshDeserialize;
    Ok(Vec::<SchemaCanonicalPoolEntry>::try_from_slice(bytes)?)
}

pub fn encode_canonical_pools(
    entries: &[SchemaCanonicalPoolEntry],
) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(entries)?)
}

pub fn decode_token_metrics(bytes: &[u8]) -> anyhow::Result<SchemaTokenMetricsV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaTokenMetricsV1::try_from_slice(bytes)?)
}

pub fn encode_token_metrics(v: &SchemaTokenMetricsV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_pool_metrics(bytes: &[u8]) -> anyhow::Result<SchemaPoolMetricsV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaPoolMetricsV1::try_from_slice(bytes)?)
}

pub fn encode_pool_metrics(v: &SchemaPoolMetricsV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_pool_creation_info(bytes: &[u8]) -> anyhow::Result<SchemaPoolCreationInfoV1> {
    use borsh::BorshDeserialize;
    Ok(SchemaPoolCreationInfoV1::try_from_slice(bytes)?)
}

pub fn encode_pool_creation_info(v: &SchemaPoolCreationInfoV1) -> anyhow::Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_u128_value(bytes: &[u8]) -> anyhow::Result<u128> {
    if bytes.len() != 16 {
        return Err(anyhow!("invalid u128 length {}", bytes.len()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(&bytes[..16]);
    Ok(u128::from_le_bytes(arr))
}

pub fn encode_u128_value(value: u128) -> anyhow::Result<Vec<u8>> {
    Ok(value.to_le_bytes().to_vec())
}

// Encode Snapshot -> BORSH (deterministic order via BTreeMap)
pub fn encode_reserves_snapshot(
    map: &HashMap<SchemaAlkaneId, SchemaPoolSnapshot>,
) -> Result<Vec<u8>> {
    let ordered: BTreeMap<SchemaAlkaneId, SchemaPoolSnapshot> =
        map.iter().map(|(k, v)| (*k, v.clone())).collect();
    let snap = SchemaReservesSnapshot { entries: ordered };
    Ok(borsh::to_vec(&snap)?)
}

// Decode BORSH -> Snapshot
pub fn decode_reserves_snapshot(
    bytes: &[u8],
) -> Result<HashMap<SchemaAlkaneId, SchemaPoolSnapshot>> {
    let snap = SchemaReservesSnapshot::try_from_slice(bytes)?;
    Ok(snap.entries.into_iter().collect())
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn parse_id_from_str(s: &str) -> Option<SchemaAlkaneId> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let parse_u32 = |s: &str| {
        if let Some(x) = s.strip_prefix("0x") {
            u32::from_str_radix(x, 16).ok()
        } else {
            s.parse::<u32>().ok()
        }
    };
    let parse_u64 = |s: &str| {
        if let Some(x) = s.strip_prefix("0x") {
            u64::from_str_radix(x, 16).ok()
        } else {
            s.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(parts[0])?, tx: parse_u64(parts[1])? })
}

fn decode_alkane_id_be(bytes: &[u8]) -> Option<SchemaAlkaneId> {
    if bytes.len() != 12 {
        return None;
    }
    let mut block_arr = [0u8; 4];
    block_arr.copy_from_slice(&bytes[..4]);
    let mut tx_arr = [0u8; 8];
    tx_arr.copy_from_slice(&bytes[4..12]);
    Some(SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) })
}

fn parse_alkane_id_from_prefixed_key(prefix: &[u8], key: &[u8]) -> Option<SchemaAlkaneId> {
    if !key.starts_with(prefix) {
        return None;
    }
    let rest = &key[prefix.len()..];
    decode_alkane_id_be(rest)
}

fn parse_height_from_tvl_key(key: &[u8]) -> Result<u32> {
    if key.len() < 4 {
        return Err(anyhow!("tvlVersioned key too short"));
    }
    let height_bytes = &key[key.len() - 4..];
    let mut arr = [0u8; 4];
    arr.copy_from_slice(height_bytes);
    Ok(u32::from_be_bytes(arr))
}

fn read_token_usd_candles_v1(
    provider: &AmmDataProvider,
    token: SchemaAlkaneId,
    tf: Timeframe,
    now_ts: u64,
) -> Result<CandleSlice> {
    let dur = tf.duration_secs();
    let table = provider.table();
    let logical = table.token_usd_candle_ns_prefix(&token, tf);
    let mut per_bucket: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    for (k, v) in provider
        .get_iter_prefix_rev(GetIterPrefixRevParams { prefix: logical })?
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

    if per_bucket.is_empty() {
        return Ok(CandleSlice { candles_newest_first: vec![], newest_ts: 0 });
    }

    let start_bucket = *per_bucket.keys().next().unwrap();
    let newest_bucket_with_data = *per_bucket.keys().last().unwrap();
    let newest_bucket_now = (now_ts / dur) * dur;

    let mut last_close: u128 = 0;
    let mut have_prev: bool = false;
    let mut forward: BTreeMap<u64, SchemaCandleV1> = BTreeMap::new();
    let mut bts = start_bucket;

    while bts <= newest_bucket_with_data {
        if let Some(candle) = per_bucket.get(&bts) {
            let mut c = *candle;
            if have_prev {
                c.open = last_close;
                if c.open > c.high {
                    c.high = c.open;
                }
                if c.open < c.low {
                    c.low = c.open;
                }
            }
            last_close = c.close;
            have_prev = true;
            forward.insert(bts, c);
        } else {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            have_prev = true;
            forward.insert(bts, c);
        }
        bts = match bts.checked_add(dur) {
            Some(n) => n,
            None => break,
        };
    }

    if newest_bucket_now > newest_bucket_with_data {
        let mut t = newest_bucket_with_data.saturating_add(dur);
        while t <= newest_bucket_now {
            let c = SchemaCandleV1 {
                open: last_close,
                high: last_close,
                low: last_close,
                close: last_close,
                volume: 0,
            };
            forward.insert(t, c);
            t = match t.checked_add(dur) {
                Some(n) => n,
                None => break,
            };
        }
    }

    let newest_first: Vec<SchemaCandleV1> =
        forward.into_iter().rev().map(|(_ts, c)| c).collect();

    Ok(CandleSlice { candles_newest_first: newest_first, newest_ts: newest_bucket_now })
}

fn parse_timeframe(s: &str) -> Option<Timeframe> {
    match s {
        "10m" | "m10" => Some(Timeframe::M10),
        "1h" | "h1" => Some(Timeframe::H1),
        "1d" | "d1" => Some(Timeframe::D1),
        "1w" | "w1" => Some(Timeframe::W1),
        "1M" | "m1" => Some(Timeframe::M1),
        _ => None,
    }
}

fn parse_price_side(s: &str) -> Option<PriceSide> {
    match s.to_ascii_lowercase().as_str() {
        "base" | "b" => Some(PriceSide::Base),
        "quote" | "q" => Some(PriceSide::Quote),
        _ => None,
    }
}

fn parse_side_filter_str(s: Option<&str>) -> ActivitySideFilter {
    if let Some(s) = s {
        return match s.to_ascii_lowercase().as_str() {
            "buy" | "b" => ActivitySideFilter::Buy,
            "sell" | "s" => ActivitySideFilter::Sell,
            "all" | "a" | "" => ActivitySideFilter::All,
            _ => ActivitySideFilter::All,
        };
    }
    ActivitySideFilter::All
}

fn parse_activity_type_str(s: Option<&str>) -> ActivityFilter {
    if let Some(s) = s {
        return match s.to_ascii_lowercase().as_str() {
            "trades" | "trade" => ActivityFilter::Trades,
            "events" | "event" => ActivityFilter::Events,
            "all" | "" => ActivityFilter::All,
            _ => ActivityFilter::All,
        };
    }
    ActivityFilter::All
}

fn parse_sort_dir_str(s: Option<&str>) -> SortDir {
    if let Some(s) = s {
        match s.to_ascii_lowercase().as_str() {
            "asc" | "ascending" => return SortDir::Asc,
            "desc" | "descending" => return SortDir::Desc,
            _ => {}
        }
    }
    SortDir::Desc
}

fn norm_token(s: &str) -> Option<&'static str> {
    match s.to_ascii_lowercase().as_str() {
        "ts" | "time" | "timestamp" => Some("ts"),
        "amt" | "amount" => Some("amount"),
        "side" | "s" => Some("side"),
        "absb" | "amount_base" | "base_amount" => Some("absb"),
        "absq" | "amount_quote" | "quote_amount" => Some("absq"),
        _ => None,
    }
}

fn map_sort(side: PriceSide, token: Option<&str>) -> (ActivitySortKey, &'static str) {
    if let Some(tok) = token.and_then(norm_token) {
        return match tok {
            "ts" => (ActivitySortKey::Timestamp, "ts"),
            "amount" => match side {
                PriceSide::Base => (ActivitySortKey::AmountBaseAbs, "absb"),
                PriceSide::Quote => (ActivitySortKey::AmountQuoteAbs, "absq"),
            },
            "side" => match side {
                PriceSide::Base => (ActivitySortKey::SideBaseTs, "sb_ts"),
                PriceSide::Quote => (ActivitySortKey::SideQuoteTs, "sq_ts"),
            },
            "absb" => (ActivitySortKey::AmountBaseAbs, "absb"),
            "absq" => (ActivitySortKey::AmountQuoteAbs, "absq"),
            _ => (ActivitySortKey::Timestamp, "ts"),
        };
    }
    (ActivitySortKey::Timestamp, "ts")
}

fn parse_u128_arg(v: Option<&Value>) -> Option<u128> {
    match v {
        Some(Value::String(s)) => s.parse::<u128>().ok(),
        Some(Value::Number(n)) => n.as_u64().map(|x| x as u128),
        _ => None,
    }
}

fn id_str(id: &SchemaAlkaneId) -> String {
    format!("{}:{}", id.block, id.tx)
}

fn scale_u128(x: u128) -> f64 {
    (x as f64) / (PRICE_SCALE as f64)
}
