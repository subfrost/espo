use crate::config::get_network;
use crate::modules::ammdata::consts::{CanonicalQuoteUnit, PRICE_SCALE, canonical_quotes};
use crate::modules::ammdata::schemas::SchemaTokenMetricsV1;
use crate::modules::ammdata::storage::{
    AmmDataProvider, GetIterPrefixRevParams as AmmGetIterPrefixRevParams, GetRawValueParams,
    decode_canonical_pools, decode_candle_v1, decode_token_metrics,
};
use crate::modules::ammdata::utils::candles::{PriceSide, read_candles_v1};
use crate::modules::ammdata::schemas::Timeframe;
use crate::modules::essentials::storage::{
    EssentialsProvider, GetCreationRecordsByIdParams,
    GetRawValueParams as EssentialsGetRawValueParams,
    decode_u128_value,
};
use crate::modules::essentials::utils::balances::get_balance_for_address;
use crate::modules::oylapi::config::OylApiConfig;
use crate::modules::oylapi::storage::{FormattedUtxo, get_address_utxos};
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use axum::{Json, Router, extract::State, routing::post};
use borsh::BorshDeserialize;
use serde::Deserialize;
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::net::SocketAddr;
use std::sync::Arc;
use tokio::net::TcpListener;
use bitcoin::Address;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct OylApiState {
    pub config: OylApiConfig,
    pub essentials: Arc<EssentialsProvider>,
    pub ammdata: Arc<AmmDataProvider>,
    pub http_client: reqwest::Client,
}

#[derive(Deserialize)]
struct AddressRequest {
    address: String,
}

#[derive(Deserialize)]
struct AmmUtxosRequest {
    address: String,
    #[serde(rename = "spendStrategy")]
    spend_strategy: Option<Value>,
}

#[derive(Deserialize)]
struct GetAlkanesRequest {
    limit: u64,
    offset: Option<u64>,
    sort_by: Option<String>,
    order: Option<String>,
    #[serde(rename = "searchQuery")]
    search_query: Option<String>,
}

#[derive(Deserialize)]
struct SearchRequest {
    #[serde(rename = "searchQuery")]
    search_query: String,
}

pub fn router(state: OylApiState) -> Router {
    Router::new()
        .route("/get-alkanes-by-address", post(get_alkanes_by_address))
        .route("/get-alkanes-utxo", post(get_alkanes_utxo))
        .route("/get-amm-utxos", post(get_amm_utxos))
        .route("/get-alkanes", post(get_alkanes))
        .route("/global-alkanes-search", post(global_alkanes_search))
        .with_state(state)
}

pub async fn run(addr: SocketAddr, state: OylApiState) -> anyhow::Result<()> {
    let app = router(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn get_alkanes_by_address(
    State(state): State<OylApiState>,
    Json(req): Json<AddressRequest>,
) -> Json<Value> {
    let Some(address) = normalize_address(&req.address) else {
        return Json(error_response(400, "invalid_address"));
    };

    let balances = match get_balance_for_address(&state.essentials, &address) {
        Ok(v) => v,
        Err(_) => HashMap::new(),
    };
    if balances.is_empty() {
        return Json(json!({ "statusCode": 200, "data": [] }));
    }

    let ids: Vec<SchemaAlkaneId> = balances.keys().copied().collect();
    let records = match state
        .essentials
        .get_creation_records_by_id(GetCreationRecordsByIdParams { alkanes: ids })
    {
        Ok(r) => r.records,
        Err(_) => Vec::new(),
    };
    let mut rec_map: HashMap<SchemaAlkaneId, _> = HashMap::new();
    for rec in records.into_iter().flatten() {
        rec_map.insert(rec.alkane, rec);
    }

    let quote_units = canonical_quote_units();
    let now_ts = now_ts();
    let mut out: Vec<Value> = Vec::new();

    for (alkane, balance) in balances {
        let rec = rec_map.get(&alkane);
        let name = rec.and_then(|r| r.names.first()).cloned().unwrap_or_default();
        let symbol = rec.and_then(|r| r.symbols.first()).cloned().unwrap_or_default();

        let (frbtc_price, busd_price) =
            canonical_pool_prices(&state, &alkane, &quote_units, now_ts);
        let price_usd = latest_token_usd_close(&state, &alkane).unwrap_or(0);
        let image = format!("{}/{}:{}.png", state.config.alkane_icon_cdn, alkane.block, alkane.tx);

        out.push(json!({
            "name": name,
            "symbol": symbol,
            "balance": balance.to_string(),
            "alkaneId": alkane_id_json(&alkane),
            "floorPrice": price_usd.to_string(),
            "frbtcPoolPriceInSats": frbtc_price.to_string(),
            "busdPoolPriceInUsd": busd_price.to_string(),
            "priceUsd": price_usd.to_string(),
            "priceInSatoshi": frbtc_price.to_string(),
            "tokenImage": image,
            "idClubMarketplace": false,
        }));
    }

    Json(json!({ "statusCode": 200, "data": out }))
}

async fn get_alkanes_utxo(
    State(state): State<OylApiState>,
    Json(req): Json<AddressRequest>,
) -> Json<Value> {
    let Some(address) = normalize_address(&req.address) else {
        return Json(error_response(400, "invalid_address"));
    };
    let utxos = match get_address_utxos(
        &state.essentials,
        &state.http_client,
        &address,
        state.config.ord_endpoint.as_deref(),
    )
    .await
    {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };
    Json(json!({ "statusCode": 200, "data": utxos }))
}

async fn get_amm_utxos(
    State(state): State<OylApiState>,
    Json(req): Json<AmmUtxosRequest>,
) -> Json<Value> {
    let Some(address) = normalize_address(&req.address) else {
        return Json(error_response(400, "invalid_address"));
    };

    let mut utxos = match get_address_utxos(
        &state.essentials,
        &state.http_client,
        &address,
        state.config.ord_endpoint.as_deref(),
    )
    .await
    {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };

    utxos.retain(|u| !u.alkanes.is_empty() || is_clean_btc(u));

    if should_sort_greatest_to_least(req.spend_strategy) {
        utxos.sort_by(|a, b| b.satoshis.cmp(&a.satoshis));
    }

    Json(json!({ "statusCode": 200, "data": { "utxos": utxos } }))
}

async fn get_alkanes(
    State(state): State<OylApiState>,
    Json(req): Json<GetAlkanesRequest>,
) -> Json<Value> {
    if req.limit == 0 {
        return Json(error_response(400, "limit_required"));
    }
    let limit = req.limit as usize;
    let offset = req.offset.unwrap_or(0) as usize;
    let sort_by = req.sort_by.unwrap_or_else(|| "volumeAllTime".to_string());
    let order = req.order.unwrap_or_else(|| "desc".to_string());

    let records = match fetch_records_for_query(&state, req.search_query.as_deref()) {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };
    let total = records.len();

    let mut items = records
        .into_iter()
        .map(|rec| {
            let metrics = token_metrics(&state, &rec.alkane);
            let holders = holders_count(&state, &rec.alkane);
            (rec, metrics, holders)
        })
        .collect::<Vec<_>>();

    sort_records(&mut items, &sort_by, &order);

    let slice = items
        .into_iter()
        .skip(offset)
        .take(limit)
        .collect::<Vec<_>>();

    let mut tokens = Vec::new();
    for (rec, metrics, holders) in slice {
        if let Ok(token) = build_alkane_token(&state, rec, metrics, holders) {
            tokens.push(token);
        }
    }

    Json(json!({
        "statusCode": 200,
        "data": {
            "tokens": tokens,
            "total": total,
            "count": tokens.len(),
            "offset": offset,
            "limit": limit,
        }
    }))
}

async fn global_alkanes_search(
    State(state): State<OylApiState>,
    Json(req): Json<SearchRequest>,
) -> Json<Value> {
    let tokens = fetch_records_for_query(&state, Some(&req.search_query))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|rec| {
            let metrics = token_metrics(&state, &rec.alkane);
            let holders = holders_count(&state, &rec.alkane);
            build_alkane_token(&state, rec, metrics, holders).ok()
        })
        .collect::<Vec<_>>();

    let pools = search_pools(&state, &req.search_query).unwrap_or_default();

    Json(json!({ "statusCode": 200, "data": { "tokens": tokens, "pools": pools } }))
}

fn normalize_address(address: &str) -> Option<String> {
    let network = get_network();
    Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .map(|a| a.to_string())
}

fn error_response(code: u16, msg: &str) -> Value {
    json!({ "statusCode": code, "error": msg })
}

fn now_ts() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap_or_default().as_secs()
}

fn alkane_id_json(alkane: &SchemaAlkaneId) -> Value {
    json!({ "block": alkane.block.to_string(), "tx": alkane.tx.to_string() })
}

fn parse_alkane_id_str(raw: &str) -> Option<SchemaAlkaneId> {
    let mut parts = raw.split(':');
    let block = parts.next()?.parse::<u32>().ok()?;
    let tx = parts.next()?.parse::<u64>().ok()?;
    Some(SchemaAlkaneId { block, tx })
}

fn normalize_query(raw: &str) -> String {
    raw.trim().to_ascii_lowercase()
}

fn is_clean_btc(utxo: &FormattedUtxo) -> bool {
    if !utxo.inscriptions.is_empty() {
        return false;
    }
    if let Value::Object(map) = &utxo.runes {
        map.is_empty()
    } else {
        utxo.runes.is_null()
    }
}

fn should_sort_greatest_to_least(spend_strategy: Option<Value>) -> bool {
    let Some(mut value) = spend_strategy else {
        return false;
    };
    if let Value::String(ref s) = value {
        if let Ok(parsed) = serde_json::from_str::<Value>(s) {
            value = parsed;
        }
    }
    if let Value::Object(map) = value {
        map.get("utxoSortGreatestToLeast")
            .and_then(|v| v.as_bool())
            .unwrap_or(false)
    } else {
        false
    }
}

fn canonical_quote_units() -> HashMap<SchemaAlkaneId, CanonicalQuoteUnit> {
    let mut map = HashMap::new();
    for cq in canonical_quotes(get_network()) {
        map.insert(cq.id, cq.unit);
    }
    map
}

fn latest_token_usd_close(state: &OylApiState, token: &SchemaAlkaneId) -> Option<u128> {
    let table = state.ammdata.table();
    let prefix = table.token_usd_candle_ns_prefix(token, Timeframe::M10);
    let entries = state
        .ammdata
        .get_iter_prefix_rev(AmmGetIterPrefixRevParams { prefix })
        .ok()?
        .entries;
    let (_k, v) = entries.into_iter().next()?;
    decode_candle_v1(&v).ok().map(|c| c.close)
}

fn canonical_pool_prices(
    state: &OylApiState,
    token: &SchemaAlkaneId,
    quote_units: &HashMap<SchemaAlkaneId, CanonicalQuoteUnit>,
    now_ts: u64,
) -> (u128, u128) {
    let mut frbtc_price = 0u128;
    let mut busd_price = 0u128;
    let table = state.ammdata.table();
    let key = table.canonical_pool_key(token);
    let pools = state
        .ammdata
        .get_raw_value(GetRawValueParams { key })
        .ok()
        .and_then(|resp| resp.value)
        .and_then(|raw| decode_canonical_pools(&raw).ok())
        .unwrap_or_default();
    for entry in pools {
        let unit = match quote_units.get(&entry.quote_id) {
            Some(u) => *u,
            None => continue,
        };
        let res = read_candles_v1(
            &state.ammdata,
            entry.pool_id,
            Timeframe::M10,
            1,
            now_ts,
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
    (frbtc_price, busd_price)
}

fn token_metrics(state: &OylApiState, token: &SchemaAlkaneId) -> SchemaTokenMetricsV1 {
    let table = state.ammdata.table();
    let key = table.token_metrics_key(token);
    let metrics = state
        .ammdata
        .get_raw_value(GetRawValueParams { key })
        .ok()
        .and_then(|resp| resp.value)
        .and_then(|raw| decode_token_metrics(&raw).ok())
        .unwrap_or_default();
    SchemaTokenMetricsV1 {
        change_1d: if metrics.change_1d.is_empty() { "0".to_string() } else { metrics.change_1d },
        change_7d: if metrics.change_7d.is_empty() { "0".to_string() } else { metrics.change_7d },
        change_30d: if metrics.change_30d.is_empty() { "0".to_string() } else { metrics.change_30d },
        change_all_time: if metrics.change_all_time.is_empty() { "0".to_string() } else { metrics.change_all_time },
        ..metrics
    }
}

fn holders_count(state: &OylApiState, token: &SchemaAlkaneId) -> u64 {
    let table = state.essentials.table();
    let key = table.holders_count_key(token);
    state
        .essentials
        .get_raw_value(EssentialsGetRawValueParams { key })
        .ok()
        .and_then(|resp| resp.value)
        .and_then(|raw| {
            crate::modules::essentials::storage::HoldersCountEntry::try_from_slice(&raw).ok()
        })
        .map(|hc| hc.count)
        .unwrap_or(0)
}

fn fetch_records_for_query(
    state: &OylApiState,
    query: Option<&str>,
) -> Result<Vec<crate::modules::essentials::utils::inspections::AlkaneCreationRecord>> {
    let table = state.essentials.table();
    let mut ids: HashSet<SchemaAlkaneId> = HashSet::new();

    if let Some(q) = query {
        let q = q.trim();
        if q.is_empty() {
            return Ok(Vec::new());
        }
        if let Some(id) = parse_alkane_id_str(q) {
            ids.insert(id);
        } else {
            let norm = normalize_query(q);
            let name_prefix = table.alkane_name_index_prefix(&norm);
            for key in state
                .essentials
                .get_scan_prefix(crate::modules::essentials::storage::GetScanPrefixParams {
                    prefix: name_prefix,
                })?
                .keys
            {
                if let Some((_name, id)) = table.parse_alkane_name_index_key(&key) {
                    ids.insert(id);
                }
            }
            let symbol_prefix = table.alkane_symbol_index_prefix(&norm);
            for key in state
                .essentials
                .get_scan_prefix(crate::modules::essentials::storage::GetScanPrefixParams {
                    prefix: symbol_prefix,
                })?
                .keys
            {
                if let Some((_sym, id)) = table.parse_alkane_symbol_index_key(&key) {
                    ids.insert(id);
                }
            }
        }
    } else {
        let entries = state
            .essentials
            .get_iter_prefix_rev(crate::modules::essentials::storage::GetIterPrefixRevParams {
                prefix: table.alkane_creation_ordered_prefix(),
            })?
            .entries;
        for (_k, v) in entries {
            if let Ok(rec) = crate::modules::essentials::storage::decode_creation_record(&v) {
                ids.insert(rec.alkane);
            }
        }
    }

    if ids.is_empty() {
        return Ok(Vec::new());
    }

    let recs = state
        .essentials
        .get_creation_records_by_id(GetCreationRecordsByIdParams {
            alkanes: ids.iter().copied().collect(),
        })?
        .records;
    Ok(recs.into_iter().flatten().collect())
}

fn sort_records(
    items: &mut Vec<(
        crate::modules::essentials::utils::inspections::AlkaneCreationRecord,
        SchemaTokenMetricsV1,
        u64,
    )>,
    sort_by: &str,
    order: &str,
) {
    let desc = order.eq_ignore_ascii_case("desc");
    let cmp_f64 = |a: f64, b: f64| {
        if desc { b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal) }
        else { a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal) }
    };
    let cmp_u128 = |a: u128, b: u128| if desc { b.cmp(&a) } else { a.cmp(&b) };
    let cmp_u64 = |a: u64, b: u64| if desc { b.cmp(&a) } else { a.cmp(&b) };

    items.sort_by(|a, b| match sort_by {
        "holders" => cmp_u64(a.2, b.2),
        "price" => cmp_u128(a.1.price_usd, b.1.price_usd),
        "marketcap" => cmp_u128(a.1.marketcap_usd, b.1.marketcap_usd),
        "volume1d" => cmp_u128(a.1.volume_1d, b.1.volume_1d),
        "volume7d" => cmp_u128(a.1.volume_7d, b.1.volume_7d),
        "volume30d" => cmp_u128(a.1.volume_30d, b.1.volume_30d),
        "volumeAllTime" => cmp_u128(a.1.volume_all_time, b.1.volume_all_time),
        "change1d" => cmp_f64(a.1.change_1d.parse::<f64>().unwrap_or(0.0), b.1.change_1d.parse::<f64>().unwrap_or(0.0)),
        "change7d" => cmp_f64(a.1.change_7d.parse::<f64>().unwrap_or(0.0), b.1.change_7d.parse::<f64>().unwrap_or(0.0)),
        "change30d" => cmp_f64(a.1.change_30d.parse::<f64>().unwrap_or(0.0), b.1.change_30d.parse::<f64>().unwrap_or(0.0)),
        "changeAllTime" => cmp_f64(a.1.change_all_time.parse::<f64>().unwrap_or(0.0), b.1.change_all_time.parse::<f64>().unwrap_or(0.0)),
        _ => cmp_u128(a.1.volume_all_time, b.1.volume_all_time),
    });
}

fn build_alkane_token(
    state: &OylApiState,
    rec: crate::modules::essentials::utils::inspections::AlkaneCreationRecord,
    metrics: SchemaTokenMetricsV1,
    holders: u64,
) -> Result<Value> {
    let supply = latest_circulating_supply(state, &rec.alkane);
    let minted = latest_total_minted(state, &rec.alkane);
    let max_supply = rec.cap.saturating_mul(rec.mint_amount);
    let mint_active = max_supply > minted;
    let percentage_minted = if max_supply == 0 {
        0
    } else {
        minted.saturating_mul(100) / max_supply
    };

    let quote_units = canonical_quote_units();
    let now_ts = now_ts();
    let (frbtc_price, busd_price) =
        canonical_pool_prices(state, &rec.alkane, &quote_units, now_ts);
    let price_usd = latest_token_usd_close(state, &rec.alkane).unwrap_or(0);

    let has_busd = busd_price > 0;
    let has_frbtc = frbtc_price > 0;
    let frbtc_fdv = if has_frbtc {
        frbtc_price.saturating_mul(supply) / PRICE_SCALE
    } else {
        0
    };
    let busd_fdv = if has_busd { metrics.fdv_usd } else { 0 };
    let frbtc_mcap = if has_frbtc {
        frbtc_price.saturating_mul(supply) / PRICE_SCALE
    } else {
        0
    };
    let busd_mcap = if has_busd { metrics.marketcap_usd } else { 0 };

    Ok(json!({
        "id": alkane_id_json(&rec.alkane),
        "alkaneId": alkane_id_json(&rec.alkane),
        "name": rec.names.first().cloned().unwrap_or_default(),
        "symbol": rec.symbols.first().cloned().unwrap_or_default(),
        "totalSupply": supply.to_string(),
        "cap": rec.cap.to_string(),
        "minted": minted.to_string(),
        "mintActive": mint_active,
        "percentageMinted": percentage_minted.to_string(),
        "mintAmount": rec.mint_amount.to_string(),
        "image": format!("{}/{}:{}.png", state.config.alkane_icon_cdn, rec.alkane.block, rec.alkane.tx),
        "frbtcPoolPriceInSats": frbtc_price.to_string(),
        "busdPoolPriceInUsd": busd_price.to_string(),
        "maxSupply": max_supply.to_string(),
        "floorPrice": "0",
        "fdv": metrics.fdv_usd.to_string(),
        "holders": holders,
        "marketcap": metrics.marketcap_usd.to_string(),
        "idClubMarketplace": false,
        "busdPoolFdvInUsd": busd_fdv.to_string(),
        "frbtcPoolFdvInSats": frbtc_fdv.to_string(),
        "priceUsd": price_usd.to_string(),
        "fdvUsd": metrics.fdv_usd.to_string(),
        "busdPoolMarketcapInUsd": busd_mcap.to_string(),
        "frbtcPoolMarketcapInSats": frbtc_mcap.to_string(),
        "tokenPoolsVolume1dInUsd": metrics.volume_1d.to_string(),
        "tokenPoolsVolume30dInUsd": metrics.volume_30d.to_string(),
        "tokenPoolsVolume7dInUsd": metrics.volume_7d.to_string(),
        "tokenPoolsVolumeAllTimeInUsd": metrics.volume_all_time.to_string(),
        "tokenVolume1d": metrics.volume_1d.to_string(),
        "tokenVolume30d": metrics.volume_30d.to_string(),
        "tokenVolume7d": metrics.volume_7d.to_string(),
        "tokenVolumeAllTime": metrics.volume_all_time.to_string(),
        "priceChange24h": metrics.change_1d,
        "priceChange7d": metrics.change_7d,
        "priceChange30d": metrics.change_30d,
        "priceChangeAllTime": metrics.change_all_time,
    }))
}

fn latest_circulating_supply(state: &OylApiState, alkane: &SchemaAlkaneId) -> u128 {
    let table = state.essentials.table();
    let key = table.circulating_supply_latest_key(alkane);
    state
        .essentials
        .get_raw_value(EssentialsGetRawValueParams { key })
        .ok()
        .and_then(|resp| resp.value)
        .and_then(|raw| decode_u128_value(&raw).ok())
        .unwrap_or(0)
}

fn latest_total_minted(state: &OylApiState, alkane: &SchemaAlkaneId) -> u128 {
    let table = state.essentials.table();
    let key = table.total_minted_latest_key(alkane);
    state
        .essentials
        .get_raw_value(EssentialsGetRawValueParams { key })
        .ok()
        .and_then(|resp| resp.value)
        .and_then(|raw| decode_u128_value(&raw).ok())
        .unwrap_or(0)
}

fn search_pools(state: &OylApiState, query: &str) -> Result<Vec<Value>> {
    let table = state.ammdata.table();
    let mut pool_ids: HashSet<SchemaAlkaneId> = HashSet::new();
    if let Some(id) = parse_alkane_id_str(query) {
        pool_ids.insert(id);
    }
    let norm = normalize_query(query);
    let prefix = table.pool_name_index_prefix(&norm);
    for key in state
        .ammdata
        .get_scan_prefix(crate::modules::ammdata::storage::GetScanPrefixParams { prefix })?
        .keys
    {
        if let Some(pool) = parse_pool_name_index_key(&table, &key) {
            pool_ids.insert(pool);
        }
    }

    if pool_ids.is_empty() {
        return Ok(Vec::new());
    }

    let snapshot = state
        .ammdata
        .get_raw_value(GetRawValueParams { key: table.reserves_snapshot_key() })
        .ok()
        .and_then(|resp| resp.value)
        .and_then(|raw| crate::modules::ammdata::storage::decode_reserves_snapshot(&raw).ok())
        .unwrap_or_default();

    let mut out = Vec::new();
    for pool in pool_ids {
        let Some(snap) = snapshot.get(&pool) else { continue };
        let base = snap.base_id;
        let quote = snap.quote_id;
        let base_label = alkane_label(state, &base);
        let quote_label = alkane_label(state, &quote);
        let pool_name = format!("{base_label} / {quote_label}");
        out.push(json!({
            "poolId": alkane_id_json(&pool),
            "token0": alkane_id_json(&base),
            "token1": alkane_id_json(&quote),
            "token0Amount": snap.base_reserve.to_string(),
            "token1Amount": snap.quote_reserve.to_string(),
            "poolName": pool_name,
            "totalSupply": "0",
        }));
    }

    Ok(out)
}

fn parse_pool_name_index_key(
    table: &crate::modules::ammdata::storage::AmmDataTable<'_>,
    key: &[u8],
) -> Option<SchemaAlkaneId> {
    let prefix = table.POOL_NAME_INDEX.key();
    if !key.starts_with(prefix) {
        return None;
    }
    let rest = &key[prefix.len()..];
    let split = rest.iter().rposition(|b| *b == b'/')?;
    let id_bytes = &rest[split + 1..];
    if id_bytes.len() != 12 {
        return None;
    }
    let mut block_arr = [0u8; 4];
    block_arr.copy_from_slice(&id_bytes[..4]);
    let mut tx_arr = [0u8; 8];
    tx_arr.copy_from_slice(&id_bytes[4..12]);
    Some(SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) })
}

fn alkane_label(state: &OylApiState, id: &SchemaAlkaneId) -> String {
    let rec = state
        .essentials
        .get_creation_record(crate::modules::essentials::storage::GetCreationRecordParams {
            alkane: *id,
        })
        .ok()
        .and_then(|resp| resp.record);
    rec.and_then(|r| r.symbols.first().cloned().or_else(|| r.names.first().cloned()))
        .unwrap_or_else(|| format!("{}:{}", id.block, id.tx))
}
