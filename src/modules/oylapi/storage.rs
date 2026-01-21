use crate::config::{get_config, get_electrum_like, get_network};
use crate::modules::ammdata::consts::PRICE_SCALE;
use crate::modules::ammdata::schemas::{SchemaTokenMetricsV1, Timeframe};
use crate::modules::ammdata::storage::{
    AmmDataProvider, GetCanonicalPoolPricesParams, GetFactoryPoolsParams, GetLatestTokenUsdCloseParams,
    GetPoolCreationInfoParams, GetPoolDefsParams, GetPoolFactoryParams,
    GetPoolIdsByNamePrefixParams, GetPoolLpSupplyLatestParams, GetPoolMetricsParams,
    GetReservesSnapshotParams, GetTokenMetricsParams,
};
use crate::modules::essentials::storage::{
    EssentialsProvider, GetAlkaneIdsByNamePrefixParams, GetAlkaneIdsBySymbolPrefixParams,
    GetCreationRecordParams, GetCreationRecordsByIdParams, GetCreationRecordsOrderedParams,
    GetHoldersCountParams, GetLatestCirculatingSupplyParams, GetLatestTotalMintedParams,
    spk_to_address_str,
};
use crate::modules::essentials::utils::balances::{
    get_alkane_balances, get_balance_for_address, get_outpoint_balances_with_spent_batch,
};
use crate::modules::oylapi::config::OylApiConfig;
use crate::modules::oylapi::ordinals::{OrdOutput, fetch_ord_outputs};
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::{Address, Txid, hashes::Hash as _};
use bitcoin::hashes::sha256;
use bitcoin::script::ScriptBuf;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Deserialize)]
struct EsploraUtxoStatus {
    confirmed: bool,
    block_height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EsploraUtxo {
    txid: String,
    vout: u32,
    value: u64,
    status: EsploraUtxoStatus,
}

#[derive(Debug, Serialize)]
pub struct AlkanesUtxoEntry {
    pub value: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FormattedUtxo {
    pub tx_id: String,
    pub output_index: u32,
    pub satoshis: u64,
    pub script_pk: String,
    pub address: String,
    pub inscriptions: Vec<String>,
    pub runes: Value,
    pub alkanes: HashMap<String, AlkanesUtxoEntry>,
    pub confirmations: u64,
    pub indexed: bool,
}

#[derive(Clone)]
pub struct OylApiState {
    pub config: OylApiConfig,
    pub essentials: Arc<EssentialsProvider>,
    pub ammdata: Arc<AmmDataProvider>,
    pub http_client: Client,
}

pub struct GetAlkanesParams {
    pub limit: u64,
    pub offset: Option<u64>,
    pub sort_by: Option<String>,
    pub order: Option<String>,
    pub search_query: Option<String>,
}

pub async fn get_alkanes_by_address(state: &OylApiState, address: &str) -> Value {
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };

    let balances = match get_balance_for_address(&state.essentials, &address) {
        Ok(v) => v,
        Err(_) => HashMap::new(),
    };
    if balances.is_empty() {
        return json!({ "statusCode": 200, "data": [] });
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

    let now_ts = now_ts();
    let mut out: Vec<Value> = Vec::new();

    for (alkane, balance) in balances {
        let rec = rec_map.get(&alkane);
        let name = rec.and_then(|r| r.names.first()).cloned().unwrap_or_default();
        let symbol = rec.and_then(|r| r.symbols.first()).cloned().unwrap_or_default();

        let (frbtc_price, busd_price) = canonical_pool_prices(state, &alkane, now_ts);
        let price_usd = latest_token_usd_close(state, &alkane).unwrap_or(0);
        let image = format!(
            "{}/{}:{}.png",
            state.config.alkane_icon_cdn, alkane.block, alkane.tx
        );

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

    json!({ "statusCode": 200, "data": out })
}

pub async fn get_alkanes_utxo(state: &OylApiState, address: &str) -> Value {
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
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
    json!({ "statusCode": 200, "data": utxos })
}

pub async fn get_amm_utxos(
    state: &OylApiState,
    address: &str,
    spend_strategy: Option<Value>,
) -> Value {
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
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

    if should_sort_greatest_to_least(spend_strategy) {
        utxos.sort_by(|a, b| b.satoshis.cmp(&a.satoshis));
    }

    json!({ "statusCode": 200, "data": { "utxos": utxos } })
}

pub async fn get_alkanes(state: &OylApiState, params: GetAlkanesParams) -> Value {
    if params.limit == 0 {
        return error_response(400, "limit_required");
    }
    let limit = params.limit as usize;
    let offset = params.offset.unwrap_or(0) as usize;
    let sort_by = params.sort_by.unwrap_or_else(|| "volumeAllTime".to_string());
    let order = params.order.unwrap_or_else(|| "desc".to_string());

    let records = match fetch_records_for_query(state, params.search_query.as_deref()) {
        Ok(v) => v,
        Err(_) => Vec::new(),
    };
    let total = records.len();

    let mut items = records
        .into_iter()
        .map(|rec| {
            let metrics = token_metrics(state, &rec.alkane);
            let holders = holders_count(state, &rec.alkane);
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
        if let Ok(token) = build_alkane_token(state, rec, metrics, holders) {
            tokens.push(token);
        }
    }

    json!({
        "statusCode": 200,
        "data": {
            "tokens": tokens,
            "total": total,
            "count": tokens.len(),
            "offset": offset,
            "limit": limit,
        }
    })
}

pub async fn get_global_alkanes_search(state: &OylApiState, query: &str) -> Value {
    let tokens = fetch_records_for_query(state, Some(query))
        .unwrap_or_default()
        .into_iter()
        .filter_map(|rec| {
            let metrics = token_metrics(state, &rec.alkane);
            let holders = holders_count(state, &rec.alkane);
            build_alkane_token(state, rec, metrics, holders).ok()
        })
        .collect::<Vec<_>>();

    let pools = search_pools(state, query).unwrap_or_default();

    json!({ "statusCode": 200, "data": { "tokens": tokens, "pools": pools } })
}

pub async fn get_alkane_details(state: &OylApiState, block: &str, tx: &str) -> Value {
    let Some(alkane) = parse_alkane_id_fields(block, tx) else {
        return error_response(400, "invalid_alkane_id");
    };

    let rec = state
        .essentials
        .get_creation_record(GetCreationRecordParams { alkane })
        .ok()
        .and_then(|resp| resp.record);
    let Some(rec) = rec else {
        return error_response(404, "alkane_not_found");
    };

    let metrics = token_metrics(state, &alkane);
    let holders = holders_count(state, &alkane);
    let mut token = match build_alkane_token(state, rec, metrics, holders) {
        Ok(v) => v,
        Err(_) => return error_response(500, "build_failed"),
    };

    let supply = latest_circulating_supply(state, &alkane);
    let now_ts = now_ts();
    let (frbtc_price, _busd_price) = canonical_pool_prices(state, &alkane, now_ts);
    let token_image = format!(
        "{}/{}:{}.png",
        state.config.alkane_icon_cdn, alkane.block, alkane.tx
    );

    if let Value::Object(ref mut map) = token {
        map.insert("decimals".to_string(), json!(8));
        map.insert("supply".to_string(), json!(supply.to_string()));
        map.insert("priceInSatoshi".to_string(), json!(frbtc_price.to_string()));
        map.insert("tokenImage".to_string(), json!(token_image));
    }

    json!({ "statusCode": 200, "data": token })
}

pub async fn get_pools(
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    limit: Option<u64>,
    offset: Option<u64>,
) -> Value {
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };

    let pools = state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { factory })
        .map(|res| res.pools)
        .unwrap_or_default();
    let total = pools.len();
    let offset = offset.unwrap_or(0) as usize;
    let limit_usize = limit.map(|l| l as usize);

    let slice: Vec<Value> = pools
        .into_iter()
        .skip(offset)
        .take(limit_usize.unwrap_or(total))
        .map(|id| alkane_id_json(&id))
        .collect();

    json!({
        "statusCode": 200,
        "data": slice,
        "total": total,
        "offset": offset,
        "limit": limit,
    })
}

pub async fn get_pool_details(
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    pool_block: &str,
    pool_tx: &str,
) -> Value {
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };
    let Some(pool) = parse_alkane_id_fields(pool_block, pool_tx) else {
        return error_response(400, "invalid_pool_id");
    };

    let details = match build_pool_details(state, Some(factory), pool) {
        Ok(Some(d)) => Some(d.value),
        Ok(None) => None,
        Err(_) => None,
    };

    json!({ "statusCode": 200, "data": details })
}

pub async fn get_address_positions(
    state: &OylApiState,
    address: &str,
    factory_block: &str,
    factory_tx: &str,
) -> Value {
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };
    let Some(address) = normalize_address(address) else {
        return error_response(400, "invalid_address");
    };

    let balances = match get_balance_for_address(&state.essentials, &address) {
        Ok(v) => v,
        Err(_) => HashMap::new(),
    };
    if balances.is_empty() {
        return json!({ "statusCode": 200, "data": [] });
    }

    let pool_ids = state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { factory })
        .map(|res| res.pools)
        .unwrap_or_default();
    let pool_set: HashSet<SchemaAlkaneId> = pool_ids.into_iter().collect();

    let mut positions: Vec<(Value, u128)> = Vec::new();

    for (pool_id, balance) in balances {
        if balance == 0 || !pool_set.contains(&pool_id) {
            continue;
        }
        let Some(details) = build_pool_details(state, Some(factory), pool_id).ok().flatten() else {
            continue;
        };

        let lp_supply = details.lp_supply;
        let share_value = if lp_supply == 0 {
            0
        } else {
            balance
        };

        let token0_value_usd = if lp_supply == 0 {
            0
        } else {
            details
                .token0_tvl_usd
                .saturating_mul(share_value)
                .saturating_div(lp_supply)
        };
        let token1_value_usd = if lp_supply == 0 {
            0
        } else {
            details
                .token1_tvl_usd
                .saturating_mul(share_value)
                .saturating_div(lp_supply)
        };
        let token0_value_sats = if lp_supply == 0 {
            0
        } else {
            details
                .token0_tvl_sats
                .saturating_mul(share_value)
                .saturating_div(lp_supply)
        };
        let token1_value_sats = if lp_supply == 0 {
            0
        } else {
            details
                .token1_tvl_sats
                .saturating_mul(share_value)
                .saturating_div(lp_supply)
        };

        let total_value_usd = token0_value_usd.saturating_add(token1_value_usd);
        let total_value_sats = token0_value_sats.saturating_add(token1_value_sats);

        let mut value = details.value;
        if let Value::Object(ref mut map) = value {
            map.insert("balance".to_string(), json!(balance.to_string()));
            map.insert(
                "token0ValueInSats".to_string(),
                json!(token0_value_sats.to_string()),
            );
            map.insert(
                "token1ValueInSats".to_string(),
                json!(token1_value_sats.to_string()),
            );
            map.insert(
                "token0ValueInUsd".to_string(),
                json!(token0_value_usd.to_string()),
            );
            map.insert(
                "token1ValueInUsd".to_string(),
                json!(token1_value_usd.to_string()),
            );
            map.insert(
                "totalValueInSats".to_string(),
                json!(total_value_sats.to_string()),
            );
            map.insert(
                "totalValueInUsd".to_string(),
                json!(total_value_usd.to_string()),
            );
        }

        positions.push((value, total_value_usd));
    }

    positions.sort_by(|a, b| b.1.cmp(&a.1));
    let data: Vec<Value> = positions.into_iter().map(|(v, _)| v).collect();

    json!({ "statusCode": 200, "data": data })
}

pub async fn get_all_pools_details(
    state: &OylApiState,
    factory_block: &str,
    factory_tx: &str,
    limit: Option<u64>,
    offset: Option<u64>,
    sort_by: Option<String>,
    order: Option<String>,
    address: Option<String>,
    search_query: Option<String>,
) -> Value {
    let Some(factory) = parse_alkane_id_fields(factory_block, factory_tx) else {
        return error_response(400, "invalid_factory_id");
    };

    let mut pools = state
        .ammdata
        .get_factory_pools(GetFactoryPoolsParams { factory })
        .map(|res| res.pools)
        .unwrap_or_default();

    if let Some(query) = search_query.as_deref().map(str::trim).filter(|q| !q.is_empty()) {
        if let Some(id) = parse_alkane_id_str(query) {
            let mut filtered = Vec::new();
            for pool in pools.into_iter() {
                if pool == id {
                    filtered.push(pool);
                    continue;
                }
                if let Ok(resp) = state.ammdata.get_pool_defs(GetPoolDefsParams { pool }) {
                    if let Some(defs) = resp.defs {
                        if defs.base_alkane_id == id || defs.quote_alkane_id == id {
                            filtered.push(pool);
                        }
                    }
                }
            }
            pools = filtered;
        } else {
            let norm = normalize_query(query);
            let ids = state
                .ammdata
                .get_pool_ids_by_name_prefix(GetPoolIdsByNamePrefixParams { prefix: norm })
                .map(|res| res.ids)
                .unwrap_or_default();
            let id_set: HashSet<SchemaAlkaneId> = ids.into_iter().collect();
            pools = pools.into_iter().filter(|p| id_set.contains(p)).collect();
        }
    }

    if let Some(addr) = address {
        let Some(addr) = normalize_address(&addr) else {
            return error_response(400, "invalid_address");
        };
        let balances = match get_balance_for_address(&state.essentials, &addr) {
            Ok(v) => v,
            Err(_) => HashMap::new(),
        };
        pools = pools
            .into_iter()
            .filter(|p| balances.get(p).copied().unwrap_or(0) > 0)
            .collect();
    }

    let mut computed: Vec<PoolDetailsComputed> = Vec::new();
    for pool in pools.into_iter() {
        if let Ok(Some(details)) = build_pool_details(state, Some(factory), pool) {
            computed.push(details);
        }
    }

    if computed.is_empty() {
        return json!({
            "statusCode": 200,
            "data": {
                "count": 0,
                "pools": [],
                "total": 0,
                "offset": offset.unwrap_or(0),
                "limit": limit,
                "largestPool": Value::Null,
                "trendingPools": Value::Null,
                "totalTvl": "0",
                "totalPoolVolume24hChange": "0.00",
                "totalPoolVolume24h": "0",
            }
        });
    }

    let mut total_tvl: u128 = 0;
    let mut total_volume_1d: u128 = 0;
    let mut largest: Option<PoolDetailsComputed> = None;
    let mut trending: Option<PoolDetailsComputed> = None;

    for details in &computed {
        total_tvl = total_tvl.saturating_add(details.pool_tvl_usd);
        total_volume_1d = total_volume_1d.saturating_add(details.pool_volume_1d_usd);

        if largest
            .as_ref()
            .map(|d| details.pool_tvl_usd > d.pool_tvl_usd)
            .unwrap_or(true)
        {
            largest = Some(details.clone());
        }
        if trending
            .as_ref()
            .map(|d| details.tvl_change_24h > d.tvl_change_24h)
            .unwrap_or(true)
        {
            trending = Some(details.clone());
        }
    }

    let sort_by = sort_by.unwrap_or_else(|| "tvl".to_string());
    let order = order.unwrap_or_else(|| "desc".to_string());
    let desc = order.eq_ignore_ascii_case("desc");

    computed.sort_by(|a, b| {
        let (va, vb) = match sort_by.as_str() {
            "volume1d" => (a.pool_volume_1d_usd as f64, b.pool_volume_1d_usd as f64),
            "volume30d" => (a.pool_volume_30d_usd as f64, b.pool_volume_30d_usd as f64),
            "apr" => (a.pool_apr, b.pool_apr),
            "tvlChange" => (a.tvl_change_24h, b.tvl_change_24h),
            _ => (a.pool_tvl_usd as f64, b.pool_tvl_usd as f64),
        };
        if desc {
            vb.partial_cmp(&va).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            va.partial_cmp(&vb).unwrap_or(std::cmp::Ordering::Equal)
        }
    });

    let total = computed.len();
    let offset = offset.unwrap_or(0) as usize;
    let limit_usize = limit.map(|l| l as usize);
    let paginated = computed
        .iter()
        .skip(offset)
        .take(limit_usize.unwrap_or(total))
        .map(|d| d.value.clone())
        .collect::<Vec<_>>();

    let largest_pool = largest.map(|d| {
        let mut val = d.value.clone();
        if let Value::Object(ref mut map) = val {
            map.insert("tvl".to_string(), json!(d.pool_tvl_usd.to_string()));
        }
        val
    });

    let trending_pool = trending.map(|d| {
        let mut val = d.value.clone();
        if let Value::Object(ref mut map) = val {
            map.insert(
                "trend".to_string(),
                json!(d.tvl_change_24h),
            );
        }
        json!({ "1d": val })
    });

    json!({
        "statusCode": 200,
        "data": {
            "count": paginated.len(),
            "pools": paginated,
            "total": total,
            "offset": offset,
            "limit": limit,
            "largestPool": largest_pool,
            "trendingPools": trending_pool,
            "totalTvl": total_tvl.to_string(),
            "totalPoolVolume24hChange": "0.00",
            "totalPoolVolume24h": total_volume_1d.to_string(),
        }
    })
}

pub async fn get_address_utxos(
    essentials: &EssentialsProvider,
    client: &Client,
    address: &str,
    ord_endpoint: Option<&str>,
) -> Result<Vec<FormattedUtxo>> {
    let network = get_network();
    let addr = Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .ok_or_else(|| anyhow!("invalid address"))?;
    let address_str = addr.to_string();
    let script_pubkey = addr.script_pubkey();
    let script_pk_hex = hex::encode(script_pubkey.as_bytes());

    let script_hash = script_hash_hex(&script_pubkey);
    let electrs_url = get_config()
        .electrs_esplora_url
        .clone()
        .ok_or_else(|| anyhow!("electrs_esplora_url missing"))?;

    let utxos = fetch_scripthash_utxos(client, &electrs_url, &script_hash).await?;
    if utxos.is_empty() {
        return Ok(Vec::new());
    }

    let tip_height = get_electrum_like().tip_height().unwrap_or(0) as u64;

    let mut outpoints: Vec<(Txid, u32)> = Vec::with_capacity(utxos.len());
    let mut outpoint_strs: Vec<String> = Vec::with_capacity(utxos.len());
    for utxo in &utxos {
        let txid = Txid::from_str(&utxo.txid)?;
        outpoints.push((txid, utxo.vout));
        outpoint_strs.push(format!("{}:{}", utxo.txid, utxo.vout));
    }

    let balances_by_outpoint = match get_outpoint_balances_with_spent_batch(essentials, &outpoints) {
        Ok(map) => map,
        Err(_) => HashMap::new(),
    };

    let mut alkane_ids: HashSet<SchemaAlkaneId> = HashSet::new();
    for lookup in balances_by_outpoint.values() {
        for be in &lookup.balances {
            alkane_ids.insert(be.alkane);
        }
    }

    let mut names: HashMap<SchemaAlkaneId, (String, String)> = HashMap::new();
    if !alkane_ids.is_empty() {
        let alkanes: Vec<SchemaAlkaneId> = alkane_ids.iter().copied().collect();
        let records = essentials
            .get_creation_records_by_id(GetCreationRecordsByIdParams { alkanes })?
            .records;
        for rec in records.into_iter().flatten() {
            let name = rec.names.first().cloned().unwrap_or_default();
            let symbol = rec.symbols.first().cloned().unwrap_or_default();
            names.insert(rec.alkane, (name, symbol));
        }
    }

    let ord_outputs = if let Some(endpoint) = ord_endpoint {
        fetch_ord_outputs(client, endpoint, &outpoint_strs).await.unwrap_or_default()
    } else {
        HashMap::new()
    };

    let mut formatted: Vec<FormattedUtxo> = Vec::with_capacity(utxos.len());
    for (idx, utxo) in utxos.iter().enumerate() {
        let txid = Txid::from_str(&utxo.txid)?;
        let out_key = (txid, utxo.vout);
        let lookup = balances_by_outpoint.get(&out_key);
        let balances = lookup.map(|l| l.balances.clone()).unwrap_or_default();

        let mut alkanes_map: HashMap<String, AlkanesUtxoEntry> = HashMap::new();
        for be in balances {
            let id_str = format!("{}:{}", be.alkane.block, be.alkane.tx);
            let (name, symbol) = names.get(&be.alkane).cloned().unwrap_or_default();
            alkanes_map.insert(
                id_str,
                AlkanesUtxoEntry { value: be.amount.to_string(), name, symbol },
            );
        }

        let ord = ord_outputs
            .get(&outpoint_strs[idx])
            .cloned()
            .unwrap_or_else(OrdOutput::default);

        let confirmations = if utxo.status.confirmed {
            utxo.status
                .block_height
                .and_then(|h| tip_height.checked_sub(h).map(|d| d + 1))
                .unwrap_or(0)
        } else {
            0
        };

        formatted.push(FormattedUtxo {
            tx_id: utxo.txid.clone(),
            output_index: utxo.vout,
            satoshis: utxo.value,
            script_pk: script_pk_hex.clone(),
            address: address_str.clone(),
            inscriptions: ord.inscriptions,
            runes: ord.runes,
            alkanes: alkanes_map,
            confirmations,
            indexed: true,
        });
    }

    Ok(formatted)
}

fn script_hash_hex(script_pubkey: &ScriptBuf) -> String {
    let hash = sha256::Hash::hash(script_pubkey.as_bytes());
    let mut bytes = hash.to_byte_array();
    bytes.reverse();
    hex::encode(bytes)
}

async fn fetch_scripthash_utxos(
    client: &Client,
    base_url: &str,
    script_hash: &str,
) -> Result<Vec<EsploraUtxo>> {
    let base = base_url.trim_end_matches('/');
    let url = format!("{base}/scripthash/{script_hash}/utxo");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow!("esplora scripthash utxo request failed: {e}"))?;
    let resp = resp
        .error_for_status()
        .map_err(|e| anyhow!("esplora scripthash utxo status error: {e}"))?;
    let body = resp
        .json::<Vec<EsploraUtxo>>()
        .await
        .map_err(|e| anyhow!("esplora scripthash utxo decode failed: {e}"))?;
    Ok(body)
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
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

fn parse_alkane_id_fields(block: &str, tx: &str) -> Option<SchemaAlkaneId> {
    let block = block.parse::<u32>().ok()?;
    let tx = tx.parse::<u64>().ok()?;
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

fn canonical_pool_prices(
    state: &OylApiState,
    token: &SchemaAlkaneId,
    now_ts: u64,
) -> (u128, u128) {
    state
        .ammdata
        .get_canonical_pool_prices(GetCanonicalPoolPricesParams {
            token: *token,
            now_ts,
        })
        .map(|res| (res.frbtc_price, res.busd_price))
        .unwrap_or((0, 0))
}

fn latest_token_usd_close(state: &OylApiState, token: &SchemaAlkaneId) -> Option<u128> {
    state
        .ammdata
        .get_latest_token_usd_close(GetLatestTokenUsdCloseParams {
            token: *token,
            timeframe: Timeframe::M10,
        })
        .ok()
        .and_then(|res| res.close)
}

fn token_metrics(state: &OylApiState, token: &SchemaAlkaneId) -> SchemaTokenMetricsV1 {
    let mut metrics = state
        .ammdata
        .get_token_metrics(GetTokenMetricsParams { token: *token })
        .ok()
        .map(|res| res.metrics)
        .unwrap_or_default();
    if metrics.change_1d.is_empty() {
        metrics.change_1d = "0".to_string();
    }
    if metrics.change_7d.is_empty() {
        metrics.change_7d = "0".to_string();
    }
    if metrics.change_30d.is_empty() {
        metrics.change_30d = "0".to_string();
    }
    if metrics.change_all_time.is_empty() {
        metrics.change_all_time = "0".to_string();
    }
    metrics
}

fn holders_count(state: &OylApiState, token: &SchemaAlkaneId) -> u64 {
    state
        .essentials
        .get_holders_count(GetHoldersCountParams { alkane: *token })
        .ok()
        .map(|res| res.count)
        .unwrap_or(0)
}

fn latest_circulating_supply(state: &OylApiState, token: &SchemaAlkaneId) -> u128 {
    state
        .essentials
        .get_latest_circulating_supply(GetLatestCirculatingSupplyParams { alkane: *token })
        .ok()
        .map(|res| res.supply)
        .unwrap_or(0)
}

fn latest_total_minted(state: &OylApiState, token: &SchemaAlkaneId) -> u128 {
    state
        .essentials
        .get_latest_total_minted(GetLatestTotalMintedParams { alkane: *token })
        .ok()
        .map(|res| res.total_minted)
        .unwrap_or(0)
}

fn fetch_records_for_query(
    state: &OylApiState,
    query: Option<&str>,
) -> Result<Vec<crate::modules::essentials::utils::inspections::AlkaneCreationRecord>> {
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
            let by_name = state
                .essentials
                .get_alkane_ids_by_name_prefix(GetAlkaneIdsByNamePrefixParams {
                    prefix: norm.clone(),
                })?
                .ids;
            let by_symbol = state
                .essentials
                .get_alkane_ids_by_symbol_prefix(GetAlkaneIdsBySymbolPrefixParams {
                    prefix: norm,
                })?
                .ids;
            for id in by_name.into_iter().chain(by_symbol.into_iter()) {
                ids.insert(id);
            }
        }
    } else {
        return Ok(
            state
                .essentials
                .get_creation_records_ordered(GetCreationRecordsOrderedParams)?
                .records,
        );
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
        if desc {
            b.partial_cmp(&a).unwrap_or(std::cmp::Ordering::Equal)
        } else {
            a.partial_cmp(&b).unwrap_or(std::cmp::Ordering::Equal)
        }
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

    let now_ts = now_ts();
    let (frbtc_price, busd_price) = canonical_pool_prices(state, &rec.alkane, now_ts);
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

#[derive(Clone)]
struct PoolDetailsComputed {
    value: Value,
    token0_tvl_usd: u128,
    token1_tvl_usd: u128,
    token0_tvl_sats: u128,
    token1_tvl_sats: u128,
    pool_tvl_usd: u128,
    pool_volume_1d_usd: u128,
    pool_volume_30d_usd: u128,
    pool_apr: f64,
    tvl_change_24h: f64,
    lp_supply: u128,
}

fn build_pool_details(
    state: &OylApiState,
    factory: Option<SchemaAlkaneId>,
    pool: SchemaAlkaneId,
) -> Result<Option<PoolDetailsComputed>> {
    if let Some(factory_id) = factory {
        let factory_match = state
            .ammdata
            .get_pool_factory(GetPoolFactoryParams { pool })
            .ok()
            .and_then(|res| res.factory);
        if let Some(found) = factory_match {
            if found != factory_id {
                return Ok(None);
            }
        } else {
            let pools = state
                .ammdata
                .get_factory_pools(GetFactoryPoolsParams { factory: factory_id })
                .map(|res| res.pools)
                .unwrap_or_default();
            if !pools.contains(&pool) {
                return Ok(None);
            }
        }
    }

    let defs = state
        .ammdata
        .get_pool_defs(GetPoolDefsParams { pool })
        .ok()
        .and_then(|res| res.defs);
    let Some(defs) = defs else {
        return Ok(None);
    };

    let token0 = defs.base_alkane_id;
    let token1 = defs.quote_alkane_id;

    let mut balances = get_alkane_balances(&state.essentials, &pool).unwrap_or_default();
    let token0_amount = balances.remove(&token0).unwrap_or(0);
    let token1_amount = balances.remove(&token1).unwrap_or(0);

    let token0_label = alkane_label(state, &token0);
    let token1_label = alkane_label(state, &token1);
    let pool_name = format!("{token0_label} / {token1_label}");

    let mut metrics = state
        .ammdata
        .get_pool_metrics(GetPoolMetricsParams { pool })
        .ok()
        .map(|res| res.metrics)
        .unwrap_or_default();
    if metrics.tvl_change_24h.is_empty() {
        metrics.tvl_change_24h = "0".to_string();
    }
    if metrics.tvl_change_7d.is_empty() {
        metrics.tvl_change_7d = "0".to_string();
    }
    if metrics.pool_apr.is_empty() {
        metrics.pool_apr = "0".to_string();
    }

    let token0_price_usd = token_metrics(state, &token0).price_usd;
    let token1_price_usd = token_metrics(state, &token1).price_usd;
    let now_ts = now_ts();
    let (token0_price_sats, _) = canonical_pool_prices(state, &token0, now_ts);
    let (token1_price_sats, _) = canonical_pool_prices(state, &token1, now_ts);

    let token0_tvl_usd = token0_amount.saturating_mul(token0_price_usd) / PRICE_SCALE;
    let token1_tvl_usd = token1_amount.saturating_mul(token1_price_usd) / PRICE_SCALE;
    let token0_tvl_sats = token0_amount.saturating_mul(token0_price_sats) / PRICE_SCALE;
    let token1_tvl_sats = token1_amount.saturating_mul(token1_price_sats) / PRICE_SCALE;

    let pool_tvl_usd = token0_tvl_usd.saturating_add(token1_tvl_usd);
    let pool_tvl_sats = token0_tvl_sats.saturating_add(token1_tvl_sats);

    let mut lp_supply = state
        .ammdata
        .get_pool_lp_supply_latest(GetPoolLpSupplyLatestParams { pool })
        .ok()
        .map(|res| res.supply)
        .unwrap_or(0);
    if lp_supply == 0 {
        lp_supply = latest_circulating_supply(state, &pool);
    }

    let lp_value_sats = if lp_supply == 0 {
        0
    } else {
        pool_tvl_sats.saturating_div(lp_supply)
    };
    let lp_value_usd = if lp_supply == 0 {
        0
    } else {
        pool_tvl_usd.saturating_div(lp_supply)
    };

    let creation = state
        .ammdata
        .get_pool_creation_info(GetPoolCreationInfoParams { pool })
        .ok()
        .and_then(|res| res.info);
    let (creator_address, creation_height, initial_token0_amount, initial_token1_amount) =
        if let Some(info) = creation {
            let creator = if info.creator_spk.is_empty() {
                None
            } else {
                let spk = ScriptBuf::from(info.creator_spk.clone());
                spk_to_address_str(&spk, get_network())
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

    let value = json!({
        "token0": alkane_id_json(&token0),
        "token1": alkane_id_json(&token1),
        "token0Amount": token0_amount.to_string(),
        "token1Amount": token1_amount.to_string(),
        "tokenSupply": lp_supply.to_string(),
        "poolName": pool_name,
        "poolId": alkane_id_json(&pool),
        "token0TvlInSats": token0_tvl_sats.to_string(),
        "token0TvlInUsd": token0_tvl_usd.to_string(),
        "token1TvlInSats": token1_tvl_sats.to_string(),
        "token1TvlInUsd": token1_tvl_usd.to_string(),
        "poolVolume30dInSats": metrics.pool_volume_30d_sats.to_string(),
        "poolVolume1dInSats": metrics.pool_volume_1d_sats.to_string(),
        "poolVolume30dInUsd": metrics.pool_volume_30d_usd.to_string(),
        "poolVolume1dInUsd": metrics.pool_volume_1d_usd.to_string(),
        "token0Volume30d": metrics.token0_volume_30d.to_string(),
        "token1Volume30d": metrics.token1_volume_30d.to_string(),
        "token0Volume1d": metrics.token0_volume_1d.to_string(),
        "token1Volume1d": metrics.token1_volume_1d.to_string(),
        "lPTokenValueInSats": lp_value_sats.to_string(),
        "lPTokenValueInUsd": lp_value_usd.to_string(),
        "poolTvlInSats": pool_tvl_sats.to_string(),
        "poolTvlInUsd": pool_tvl_usd.to_string(),
        "tvlChange24h": metrics.tvl_change_24h.clone(),
        "tvlChange7d": metrics.tvl_change_7d.clone(),
        "totalSupply": lp_supply.to_string(),
        "poolApr": metrics.pool_apr.clone(),
        "initialToken0Amount": initial_token0_amount.to_string(),
        "initialToken1Amount": initial_token1_amount.to_string(),
        "creatorAddress": creator_address,
        "creationBlockHeight": creation_height,
        "tvl": pool_tvl_usd.to_string(),
        "volume1d": metrics.pool_volume_1d_usd.to_string(),
        "volume7d": "0",
        "volume30d": metrics.pool_volume_30d_usd.to_string(),
        "volumeAllTime": "0",
        "apr": metrics.pool_apr.clone(),
        "tvlChange": metrics.tvl_change_24h.clone(),
    });

    Ok(Some(PoolDetailsComputed {
        value,
        token0_tvl_usd,
        token1_tvl_usd,
        token0_tvl_sats,
        token1_tvl_sats,
        pool_tvl_usd,
        pool_volume_1d_usd: metrics.pool_volume_1d_usd,
        pool_volume_30d_usd: metrics.pool_volume_30d_usd,
        pool_apr: metrics.pool_apr.parse::<f64>().unwrap_or(0.0),
        tvl_change_24h: metrics.tvl_change_24h.parse::<f64>().unwrap_or(0.0),
        lp_supply,
    }))
}

fn search_pools(state: &OylApiState, query: &str) -> Result<Vec<Value>> {
    let mut pool_ids: HashSet<SchemaAlkaneId> = HashSet::new();
    if let Some(id) = parse_alkane_id_str(query) {
        pool_ids.insert(id);
    }
    let norm = normalize_query(query);
    let matches = state
        .ammdata
        .get_pool_ids_by_name_prefix(GetPoolIdsByNamePrefixParams { prefix: norm })?
        .ids;
    for id in matches {
        pool_ids.insert(id);
    }

    if pool_ids.is_empty() {
        return Ok(Vec::new());
    }

    let snapshot = state
        .ammdata
        .get_reserves_snapshot(GetReservesSnapshotParams)?
        .snapshot
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
