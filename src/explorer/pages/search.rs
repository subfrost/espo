use axum::extract::Query;
use axum::response::{IntoResponse, Redirect, Response};
use bitcoincore_rpc::RpcApi;
use serde::Deserialize;
use std::str::FromStr;

use crate::config::{get_base_path, get_bitcoind_rpc_client, get_network};
use bitcoin::Address;

/// Helper to prefix a path with base_path
fn prefixed(path: &str) -> String {
    let base_path = get_base_path();
    if base_path.is_empty() {
        path.to_string()
    } else if path.starts_with('/') {
        format!("{}{}", base_path, path)
    } else {
        format!("{}/{}", base_path, path)
    }
}

#[derive(Deserialize)]
pub struct SearchQuery {
    pub q: Option<String>,
}

pub async fn search(Query(q): Query<SearchQuery>) -> Response {
    let Some(mut query) = q.q else {
        return Redirect::to(&prefixed("/")).into_response();
    };
    query = query.trim().to_string();
    if query.is_empty() {
        return Redirect::to(&prefixed("/")).into_response();
    }

    if let Ok(h) = query.parse::<u64>() {
        return Redirect::to(&prefixed(&format!("/block/{h}"))).into_response();
    }

    if let Some(alk) = parse_alkane_id(&query) {
        return Redirect::to(&prefixed(&format!("/alkane/{}:{}", alk.block, alk.tx))).into_response();
    }

    if let Ok(addr) = Address::from_str(&query) {
        if let Ok(addr) = addr.require_network(get_network()) {
            return Redirect::to(&prefixed(&format!("/address/{addr}"))).into_response();
        }
    }

    if query.len() == 64 && query.chars().all(|c| c.is_ascii_hexdigit()) {
        match bitcoincore_rpc::bitcoin::BlockHash::from_str(&query) {
            Ok(hash) => match get_bitcoind_rpc_client().get_block_header_info(&hash) {
                Ok(info) => {
                    return Redirect::to(&prefixed(&format!("/block/{}", info.height))).into_response();
                }
                Err(_) => {}
            },
            Err(_) => {}
        }

        return Redirect::to(&prefixed(&format!("/tx/{query}"))).into_response();
    }

    Redirect::to(&prefixed("/")).into_response()
}

fn parse_alkane_id(s: &str) -> Option<crate::schemas::SchemaAlkaneId> {
    let (a, b) = s.split_once(':')?;
    let block = parse_u32_any(a)?;
    let tx = parse_u64_any(b)?;
    Some(crate::schemas::SchemaAlkaneId { block, tx })
}

fn parse_u32_any(s: &str) -> Option<u32> {
    let t = s.trim();
    if let Some(h) = t.strip_prefix("0x") {
        u32::from_str_radix(h, 16).ok()
    } else {
        t.parse().ok()
    }
}

fn parse_u64_any(s: &str) -> Option<u64> {
    let t = s.trim();
    if let Some(h) = t.strip_prefix("0x") {
        u64::from_str_radix(h, 16).ok()
    } else {
        t.parse().ok()
    }
}
