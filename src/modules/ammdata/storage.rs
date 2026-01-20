use super::schemas::{SchemaPoolSnapshot, SchemaReservesSnapshot, Timeframe};
use crate::modules::ammdata::consts::{KEY_INDEX_HEIGHT, PRICE_SCALE};
use crate::modules::ammdata::schemas::SchemaFullCandleV1;
use crate::modules::ammdata::utils::activity::{
    ActivityFilter, ActivityPage, ActivitySideFilter, ActivitySortKey, SortDir,
    read_activity_for_pool, read_activity_for_pool_sorted,
};
use crate::modules::ammdata::utils::candles::{PriceSide, read_candles_v1};
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
use std::collections::{BTreeMap, HashMap};
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
    // Activity logs + secondary indexes for sort/paging.
    pub ACTIVITY: MdbPointer<'a>,
    pub ACTIVITY_INDEX: MdbPointer<'a>,
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
            ACTIVITY: root.keyword("activity:v1:"),
            ACTIVITY_INDEX: root.keyword("activity:idx:"),
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

        let pool = match params.pool.as_deref().and_then(parse_id_from_str) {
            Some(p) => p,
            None => {
                return Ok(RpcGetCandlesResult {
                    value: json!({
                        "ok": false,
                        "error": "missing_or_invalid_pool",
                        "hint": "pool should be a string like \"2:68441\""
                    }),
                });
            }
        };

        match read_candles_v1(self, pool, tf, /*unused*/ limit, now, side) {
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
                        "pool": id_str(&pool),
                        "timeframe": tf.code(),
                        "side": match side { PriceSide::Base => "base", PriceSide::Quote => "quote" },
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
