use crate::modules::oylapi::storage::{
    GetAlkanesParams, OylApiState, get_address_positions, get_alkane_details, get_alkanes,
    get_alkanes_by_address, get_alkanes_utxo, get_all_pools_details, get_amm_utxos,
    get_global_alkanes_search, get_pool_details, get_pools,
};
use axum::{Json, Router, extract::State, routing::post};
use serde::Deserialize;
use serde_json::Value;
use std::net::SocketAddr;
use tokio::net::TcpListener;

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

#[derive(Deserialize)]
struct AlkaneIdRequest {
    block: String,
    tx: String,
}

#[derive(Deserialize)]
struct AlkaneDetailsRequest {
    #[serde(rename = "alkaneId")]
    alkane_id: AlkaneIdRequest,
}

#[derive(Deserialize)]
struct GetPoolsRequest {
    #[serde(rename = "factoryId")]
    factory_id: AlkaneIdRequest,
    limit: Option<u64>,
    offset: Option<u64>,
}

#[derive(Deserialize)]
struct PoolDetailsRequest {
    #[serde(rename = "factoryId")]
    factory_id: AlkaneIdRequest,
    #[serde(rename = "poolId")]
    pool_id: AlkaneIdRequest,
}

#[derive(Deserialize)]
struct AddressPositionsRequest {
    address: String,
    #[serde(rename = "factoryId")]
    factory_id: AlkaneIdRequest,
}

#[derive(Deserialize)]
struct GetAllPoolsDetailsRequest {
    #[serde(rename = "factoryId")]
    factory_id: AlkaneIdRequest,
    limit: Option<u64>,
    offset: Option<u64>,
    sort_by: Option<String>,
    order: Option<String>,
    address: Option<String>,
    #[serde(rename = "searchQuery")]
    search_query: Option<String>,
}

pub fn router(state: OylApiState) -> Router {
    Router::new()
        .route("/get-alkanes-by-address", post(get_alkanes_by_address_handler))
        .route("/get-alkanes-utxo", post(get_alkanes_utxo_handler))
        .route("/get-amm-utxos", post(get_amm_utxos_handler))
        .route("/get-alkanes", post(get_alkanes_handler))
        .route("/global-alkanes-search", post(global_alkanes_search_handler))
        .route("/get-alkane-details", post(get_alkane_details_handler))
        .route("/get-pools", post(get_pools_handler))
        .route("/get-pool-details", post(get_pool_details_handler))
        .route("/address-positions", post(get_address_positions_handler))
        .route("/get-all-pools-details", post(get_all_pools_details_handler))
        .with_state(state)
}

pub async fn run(addr: SocketAddr, state: OylApiState) -> anyhow::Result<()> {
    let app = router(state);
    let listener = TcpListener::bind(addr).await?;
    axum::serve(listener, app.into_make_service()).await?;
    Ok(())
}

async fn get_alkanes_by_address_handler(
    State(state): State<OylApiState>,
    Json(req): Json<AddressRequest>,
) -> Json<Value> {
    Json(get_alkanes_by_address(&state, &req.address).await)
}

async fn get_alkanes_utxo_handler(
    State(state): State<OylApiState>,
    Json(req): Json<AddressRequest>,
) -> Json<Value> {
    Json(get_alkanes_utxo(&state, &req.address).await)
}

async fn get_amm_utxos_handler(
    State(state): State<OylApiState>,
    Json(req): Json<AmmUtxosRequest>,
) -> Json<Value> {
    Json(get_amm_utxos(&state, &req.address, req.spend_strategy).await)
}

async fn get_alkanes_handler(
    State(state): State<OylApiState>,
    Json(req): Json<GetAlkanesRequest>,
) -> Json<Value> {
    let params = GetAlkanesParams {
        limit: req.limit,
        offset: req.offset,
        sort_by: req.sort_by,
        order: req.order,
        search_query: req.search_query,
    };
    Json(get_alkanes(&state, params).await)
}

async fn global_alkanes_search_handler(
    State(state): State<OylApiState>,
    Json(req): Json<SearchRequest>,
) -> Json<Value> {
    Json(get_global_alkanes_search(&state, &req.search_query).await)
}

async fn get_alkane_details_handler(
    State(state): State<OylApiState>,
    Json(req): Json<AlkaneDetailsRequest>,
) -> Json<Value> {
    Json(
        get_alkane_details(&state, &req.alkane_id.block, &req.alkane_id.tx).await,
    )
}

async fn get_pools_handler(
    State(state): State<OylApiState>,
    Json(req): Json<GetPoolsRequest>,
) -> Json<Value> {
    Json(
        get_pools(
            &state,
            &req.factory_id.block,
            &req.factory_id.tx,
            req.limit,
            req.offset,
        )
        .await,
    )
}

async fn get_pool_details_handler(
    State(state): State<OylApiState>,
    Json(req): Json<PoolDetailsRequest>,
) -> Json<Value> {
    Json(
        get_pool_details(
            &state,
            &req.factory_id.block,
            &req.factory_id.tx,
            &req.pool_id.block,
            &req.pool_id.tx,
        )
        .await,
    )
}

async fn get_address_positions_handler(
    State(state): State<OylApiState>,
    Json(req): Json<AddressPositionsRequest>,
) -> Json<Value> {
    Json(
        get_address_positions(
            &state,
            &req.address,
            &req.factory_id.block,
            &req.factory_id.tx,
        )
        .await,
    )
}

async fn get_all_pools_details_handler(
    State(state): State<OylApiState>,
    Json(req): Json<GetAllPoolsDetailsRequest>,
) -> Json<Value> {
    Json(
        get_all_pools_details(
            &state,
            &req.factory_id.block,
            &req.factory_id.tx,
            req.limit,
            req.offset,
            req.sort_by,
            req.order,
            req.address,
            req.search_query,
        )
        .await,
    )
}
