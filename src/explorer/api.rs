use axum::Json;
use axum::extract::Query;
use serde::Deserialize;
use serde::Serialize;

use crate::config::{get_espo_next_height, get_metashrew_rpc_url, get_network};
use crate::explorer::components::tx_view::{AlkaneMetaCache, alkane_meta};
use crate::explorer::consts::{alkane_contract_name_overrides, alkane_name_overrides};
use crate::explorer::paths::explorer_path;
use crate::modules::essentials::storage::{
    HoldersCountEntry, alkane_holders_ordered_prefix, alkane_name_index_prefix,
    block_summary_key, get_cached_block_summary, holders_count_key, load_creation_record,
    parse_alkane_holders_ordered_key, parse_alkane_name_index_key, BlockSummary,
};
use crate::modules::essentials::utils::names::normalize_alkane_name;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_support::cellpack::Cellpack;
use alkanes_support::id::AlkaneId as SupportAlkaneId;
use alkanes_support::proto::alkanes::{
    AlkaneId as ProtoAlkaneId, MessageContextParcel, SimulateResponse as SimulateProto,
};
use anyhow::Context;
use bitcoincore_rpc::bitcoin::Network;
use bitcoin::blockdata::block::Header;
use bitcoin::consensus::encode::deserialize;
use bitcoin::consensus::Encodable;
use bitcoin::locktime::absolute::LockTime;
use bitcoin::transaction::Version;
use bitcoin::secp256k1::{Secp256k1, XOnlyPublicKey};
use bitcoin::{Address, Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut};
use borsh::BorshDeserialize;
use ordinals::Runestone;
use prost::Message;
use protorune_support::protostone::{Protostone, Protostones};
use reqwest::Client;
use serde_json::json;
use std::collections::{HashMap, HashSet};
use std::io::Cursor;
use std::str::FromStr;

#[derive(Deserialize)]
pub struct CarouselQuery {
    pub center: Option<u64>,
    pub radius: Option<u64>,
}

#[derive(Serialize)]
pub struct CarouselBlock {
    pub height: u64,
    pub traces: usize,
    pub time: Option<u32>,
}

#[derive(Serialize)]
pub struct CarouselResponse {
    pub espo_tip: u64,
    pub blocks: Vec<CarouselBlock>,
}

#[derive(Deserialize)]
pub struct SearchGuessQuery {
    pub q: Option<String>,
}

#[derive(Serialize)]
pub struct SearchGuessItem {
    pub label: String,
    pub value: String,
    pub href: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icon_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fallback_letter: Option<String>,
}

#[derive(Serialize)]
pub struct SearchGuessGroup {
    pub kind: String,
    pub title: String,
    pub items: Vec<SearchGuessItem>,
}

#[derive(Serialize)]
pub struct SearchGuessResponse {
    pub query: String,
    pub groups: Vec<SearchGuessGroup>,
}

pub async fn carousel_blocks(Query(q): Query<CarouselQuery>) -> Json<CarouselResponse> {
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let center = q.center.unwrap_or(espo_tip).min(espo_tip);
    let radius = q.radius.unwrap_or(8).min(50); // guardrail

    let start = center.saturating_sub(radius);
    let end = (center + radius).min(espo_tip);

    let essentials_mdb = Mdb::from_db(crate::config::get_espo_db(), b"essentials:");
    let mut blocks: Vec<CarouselBlock> = Vec::with_capacity((end - start + 1) as usize);

    for h in start..=end {
        let summary = get_cached_block_summary(h as u32).or_else(|| {
            essentials_mdb
                .get(&block_summary_key(h as u32))
                .ok()
                .flatten()
                .and_then(|b| BlockSummary::try_from_slice(&b).ok())
        });

        let (traces, time) = if let Some(summary) = summary {
            let time = deserialize::<Header>(&summary.header)
                .ok()
                .map(|hdr| hdr.time as u32);
            (summary.trace_count as usize, time)
        } else {
            (0, None)
        };

        blocks.push(CarouselBlock { height: h, traces, time });
    }

    Json(CarouselResponse { espo_tip, blocks })
}

pub async fn search_guess(Query(q): Query<SearchGuessQuery>) -> Json<SearchGuessResponse> {
    let query = q.q.unwrap_or_default().trim().to_string();
    if query.is_empty() {
        return Json(SearchGuessResponse { query, groups: Vec::new() });
    }

    let essentials_mdb = Mdb::from_db(crate::config::get_espo_db(), b"essentials:");
    let mut meta_cache: AlkaneMetaCache = HashMap::new();
    let mut seen_alkanes: HashSet<SchemaAlkaneId> = HashSet::new();
    let mut blocks: Vec<SearchGuessItem> = Vec::new();
    struct RankedAlkaneItem {
        item: SearchGuessItem,
        holders: u64,
    }

    let mut alkanes: Vec<RankedAlkaneItem> = Vec::new();
    let mut txid: Vec<SearchGuessItem> = Vec::new();
    let mut addresses: Vec<SearchGuessItem> = Vec::new();

    fn holders_for(essentials_mdb: &Mdb, alk: &SchemaAlkaneId) -> u64 {
        essentials_mdb
            .get(&holders_count_key(alk))
            .ok()
            .flatten()
            .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
            .map(|hc| hc.count)
            .unwrap_or(0)
    }

    fn push_alkane_item(
        seen_alkanes: &mut HashSet<SchemaAlkaneId>,
        alkanes: &mut Vec<RankedAlkaneItem>,
        meta_cache: &mut AlkaneMetaCache,
        essentials_mdb: &Mdb,
        alk: &SchemaAlkaneId,
        holders_hint: Option<u64>,
    ) -> bool {
        if !seen_alkanes.insert(*alk) {
            return false;
        }
        let holders = holders_hint.unwrap_or_else(|| holders_for(essentials_mdb, alk));
        let meta = alkane_meta(alk, meta_cache, essentials_mdb);
        let id = format!("{}:{}", alk.block, alk.tx);
        let known = meta.name.known;
        let label = if known { meta.name.value.clone() } else { id.clone() };
        let icon_url =
            if !meta.icon_url.trim().is_empty() { Some(meta.icon_url.clone()) } else { None };
        alkanes.push(RankedAlkaneItem {
            item: SearchGuessItem {
                label,
                value: id.clone(),
                href: Some(explorer_path(&format!("/alkane/{id}"))),
                icon_url,
                fallback_letter: Some(meta.name.fallback_letter().to_string()),
            },
            holders,
        });
        true
    }

    fn push_override_alkane(
        seen_alkanes: &mut HashSet<SchemaAlkaneId>,
        alkanes: &mut Vec<RankedAlkaneItem>,
        meta_cache: &mut AlkaneMetaCache,
        essentials_mdb: &Mdb,
        id_s: &str,
        name: &str,
    ) {
        if let Some(alk) = parse_alkane_id(id_s) {
            if !seen_alkanes.insert(alk) {
                return;
            }
            let meta = alkane_meta(&alk, meta_cache, essentials_mdb);
            let icon_url = if !meta.icon_url.trim().is_empty() {
                Some(meta.icon_url.clone())
            } else {
                None
            };
            let holders = holders_for(essentials_mdb, &alk);
            alkanes.push(RankedAlkaneItem {
                item: SearchGuessItem {
                    label: name.to_string(),
                    value: id_s.to_string(),
                    href: Some(explorer_path(&format!("/alkane/{id_s}"))),
                    icon_url,
                    fallback_letter: Some(
                        name.chars()
                            .find(|c| !c.is_whitespace())
                            .map(|c| c.to_ascii_uppercase())
                            .unwrap_or('?')
                            .to_string(),
                    ),
                },
                holders,
            });
        }
    }

    if let Some(query_norm) = normalize_alkane_name(&query) {
        let mut matches = 0usize;
        let prefix_full = essentials_mdb.prefixed(alkane_holders_ordered_prefix());
        let it = essentials_mdb.iter_prefix_rev(&prefix_full);
        for res in it {
            let Ok((k, _)) = res else { continue };
            let rel = &k[essentials_mdb.prefix().len()..];
            let Some((holders, alk)) = parse_alkane_holders_ordered_key(rel) else { continue };
            let Some(rec) = load_creation_record(&essentials_mdb, &alk).ok().flatten() else {
                continue;
            };
            let matches_name = rec
                .names
                .iter()
                .filter_map(|name| normalize_alkane_name(name))
                .any(|name| name.starts_with(&query_norm));
            if !matches_name {
                continue;
            }
            if push_alkane_item(
                &mut seen_alkanes,
                &mut alkanes,
                &mut meta_cache,
                &essentials_mdb,
                &alk,
                Some(holders),
            ) {
                matches += 1;
                if matches >= 5 {
                    break;
                }
            }
        }
        if matches < 5 {
            let prefix = alkane_name_index_prefix(&query_norm);
            for res in essentials_mdb.iter_from(&prefix) {
                let Ok((k, _)) = res else { continue };
                let rel = &k[essentials_mdb.prefix().len()..];
                if !rel.starts_with(&prefix) {
                    break;
                }
                let Some((_name, alk)) = parse_alkane_name_index_key(rel) else { continue };
                if push_alkane_item(
                    &mut seen_alkanes,
                    &mut alkanes,
                    &mut meta_cache,
                    &essentials_mdb,
                    &alk,
                    None,
                ) {
                    matches += 1;
                    if matches >= 5 {
                        break;
                    }
                }
            }
        }
    }

    if !query.is_empty() {
        let query_lower = query.to_ascii_lowercase();
        for (id_s, name, _sym) in alkane_name_overrides() {
            if name.to_ascii_lowercase().contains(&query_lower) {
                push_override_alkane(
                    &mut seen_alkanes,
                    &mut alkanes,
                    &mut meta_cache,
                    &essentials_mdb,
                    id_s,
                    name,
                );
            }
        }
        for (id_s, name) in alkane_contract_name_overrides() {
            if name.to_ascii_lowercase().contains(&query_lower) {
                push_override_alkane(
                    &mut seen_alkanes,
                    &mut alkanes,
                    &mut meta_cache,
                    &essentials_mdb,
                    id_s,
                    name,
                );
            }
        }
    }

    if let Ok(height) = query.parse::<u64>() {
        let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
        let href = if height <= espo_tip {
            Some(explorer_path(&format!("/block/{height}")))
        } else {
            None
        };
        blocks.push(SearchGuessItem {
            label: format!("#{height}"),
            value: height.to_string(),
            href,
            icon_url: None,
            fallback_letter: None,
        });

        if height <= u32::MAX as u64 {
            let alk = SchemaAlkaneId { block: height as u32, tx: 0 };
            let _ = push_alkane_item(
                &mut seen_alkanes,
                &mut alkanes,
                &mut meta_cache,
                &essentials_mdb,
                &alk,
                None,
            );
        }
    }

    if let Some(alk) = parse_alkane_id(&query) {
        let _ = push_alkane_item(
            &mut seen_alkanes,
            &mut alkanes,
            &mut meta_cache,
            &essentials_mdb,
            &alk,
            None,
        );
    }

    if let Ok(addr) = Address::from_str(&query) {
        if let Ok(addr) = addr.require_network(get_network()) {
            let addr_str = addr.to_string();
            let label = if addr_str.len() > 24 {
                format!(
                    "{}...{}",
                    &addr_str[..8],
                    &addr_str[addr_str.len().saturating_sub(6)..]
                )
            } else {
                addr_str.clone()
            };
            addresses.push(SearchGuessItem {
                label,
                value: addr_str.clone(),
                href: Some(explorer_path(&format!("/address/{addr_str}"))),
                icon_url: None,
                fallback_letter: None,
            });
        }
    }

    if query.chars().all(|c| c.is_ascii_hexdigit()) {
        let normalized = query.to_lowercase();
        if normalized.len() <= 64 {
            let label = if normalized.len() > 16 {
                format!(
                    "{}...{}",
                    &normalized[..8],
                    &normalized[normalized.len().saturating_sub(6)..]
                )
            } else {
                normalized.clone()
            };
            let href = if normalized.len() == 64 {
                Some(explorer_path(&format!("/tx/{normalized}")))
            } else {
                None
            };
            txid.push(SearchGuessItem {
                label,
                value: normalized,
                href,
                icon_url: None,
                fallback_letter: None,
            });
        }
    }

    let mut groups = Vec::new();
    if !blocks.is_empty() {
        groups.push(SearchGuessGroup {
            kind: "blocks".to_string(),
            title: "Blocks".to_string(),
            items: blocks,
        });
    }
    if !alkanes.is_empty() {
        alkanes.sort_by(|a, b| {
            b.holders
                .cmp(&a.holders)
                .then_with(|| a.item.label.cmp(&b.item.label))
        });
        let alkanes: Vec<SearchGuessItem> = alkanes.into_iter().map(|item| item.item).collect();
        groups.push(SearchGuessGroup {
            kind: "alkanes".to_string(),
            title: "Alkanes".to_string(),
            items: alkanes,
        });
    }
    if !txid.is_empty() {
        groups.push(SearchGuessGroup {
            kind: "transactions".to_string(),
            title: "Transactions".to_string(),
            items: txid,
        });
    }
    if !addresses.is_empty() {
        groups.push(SearchGuessGroup {
            kind: "addresses".to_string(),
            title: "Addresses".to_string(),
            items: addresses,
        });
    }

    Json(SearchGuessResponse { query, groups })
}

#[derive(Deserialize)]
pub struct SimulateRequest {
    pub alkane: String,
    pub opcode: u128,
    pub returns: Option<String>,
}

#[derive(Serialize)]
pub struct SimulateResponse {
    pub ok: bool,
    pub status: Option<String>,
    pub data: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alkanes: Option<Vec<SearchGuessItem>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alkanes_overflow: Option<usize>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub addresses: Option<Vec<SearchGuessItem>>,
    pub error: Option<String>,
}

pub async fn simulate_contract(Json(req): Json<SimulateRequest>) -> Json<SimulateResponse> {
    let Some(alk) = parse_alkane_id(&req.alkane) else {
        return Json(SimulateResponse {
            ok: false,
            status: None,
            data: None,
            alkanes: None,
            alkanes_overflow: None,
            addresses: None,
            error: Some("invalid_alkane_id".to_string()),
        });
    };

    let cellpack = Cellpack {
        target: SupportAlkaneId { block: alk.block as u128, tx: alk.tx as u128 },
        inputs: vec![req.opcode],
    };
    let calldata = cellpack.encipher();
    let protostone = Protostone {
        burn: None,
        message: calldata.clone(),
        edicts: Vec::new(),
        refund: None,
        pointer: Some(0),
        from: None,
        protocol_tag: 1,
    };
    let protocol_values = match vec![protostone].encipher() {
        Ok(v) => v,
        Err(e) => {
            return Json(SimulateResponse {
                ok: false,
                status: None,
                data: None,
                alkanes: None,
                alkanes_overflow: None,
                addresses: None,
                error: Some(format!("protostone_encode_failed: {e}")),
            });
        }
    };
    let runestone =
        Runestone { protocol: Some(protocol_values), pointer: Some(0), ..Default::default() };
    let runestone_script = runestone.encipher();

    let dummy_tx = Transaction {
        version: Version::TWO,
        lock_time: LockTime::ZERO,
        input: vec![TxIn {
            previous_output: OutPoint::null(),
            script_sig: ScriptBuf::new(),
            sequence: Sequence::MAX,
            witness: bitcoin::Witness::new(),
        }],
        output: vec![
            TxOut { value: Amount::from_sat(0), script_pubkey: ScriptBuf::new() },
            TxOut { value: Amount::from_sat(0), script_pubkey: runestone_script },
        ],
    };

    let mut tx_bytes = Vec::new();
    if let Err(e) = dummy_tx.consensus_encode(&mut tx_bytes) {
        return Json(SimulateResponse {
            ok: false,
            status: None,
            data: None,
            alkanes: None,
            alkanes_overflow: None,
            addresses: None,
            error: Some(format!("tx_encode_failed: {e}")),
        });
    }

    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let parcel = MessageContextParcel {
        alkanes: vec![],
        transaction: tx_bytes,
        block: vec![],
        height: espo_tip,
        txindex: 0,
        calldata,
        vout: 0,
        pointer: 0,
        refund_pointer: 0,
    };

    let mut parcel_bytes = Vec::new();
    if let Err(e) = parcel.encode(&mut parcel_bytes) {
        return Json(SimulateResponse {
            ok: false,
            status: None,
            data: None,
            alkanes: None,
            alkanes_overflow: None,
            addresses: None,
            error: Some(format!("parcel_encode_failed: {e}")),
        });
    }

    let body = json!({
        "jsonrpc": "2.0",
        "id": format!("simulate:{}:{}", alk.block, alk.tx),
        "method": "metashrew_view",
        "params": [
            "simulate",
            format!("0x{}", hex::encode(parcel_bytes)),
            espo_tip,
        ],
    });

    let client = Client::new();
    let resp_json: serde_json::Value =
        match client.post(get_metashrew_rpc_url()).json(&body).send().await {
            Ok(resp) => match resp.error_for_status() {
                Ok(ok) => match ok.json().await {
                    Ok(v) => v,
                    Err(e) => {
                        return Json(SimulateResponse {
                            ok: false,
                            status: None,
                            data: None,
                            alkanes: None,
                            alkanes_overflow: None,
                            addresses: None,
                            error: Some(format!("response_decode_failed: {e}")),
                        });
                    }
                },
                Err(e) => {
                    return Json(SimulateResponse {
                        ok: false,
                        status: None,
                        data: None,
                        alkanes: None,
                        alkanes_overflow: None,
                        addresses: None,
                        error: Some(format!("metashrew_http_error: {e}")),
                    });
                }
            },
            Err(e) => {
                return Json(SimulateResponse {
                    ok: false,
                    status: None,
                    data: None,
                    alkanes: None,
                    alkanes_overflow: None,
                    addresses: None,
                    error: Some(format!("metashrew_request_failed: {e}")),
                });
            }
        };

    let result_hex = resp_json.get("result").and_then(|v| v.as_str()).unwrap_or("");
    if result_hex.is_empty() {
        return Json(SimulateResponse {
            ok: false,
            status: None,
            data: None,
            alkanes: None,
            alkanes_overflow: None,
            addresses: None,
            error: Some("metashrew_empty_result".to_string()),
        });
    }

    let result_hex = result_hex.strip_prefix("0x").unwrap_or(result_hex);
    let bytes = match hex::decode(result_hex) {
        Ok(b) => b,
        Err(e) => {
            return Json(SimulateResponse {
                ok: false,
                status: None,
                data: None,
                alkanes: None,
                alkanes_overflow: None,
                addresses: None,
                error: Some(format!("result_decode_failed: {e}")),
            });
        }
    };
    let sim = match SimulateProto::decode(bytes.as_slice()).context("simulate response decode") {
        Ok(s) => s,
        Err(e) => {
            return Json(SimulateResponse {
                ok: false,
                status: None,
                data: None,
                alkanes: None,
                alkanes_overflow: None,
                addresses: None,
                error: Some(format!("simulate_decode_failed: {e}")),
            });
        }
    };

    let (status, data, alkanes, alkanes_overflow, addresses) = if !sim.error.is_empty() {
        ("failure".to_string(), sim.error, None, None, None)
    } else if let Some(exec) = sim.execution {
        let returns_norm = normalize_returns(req.returns.as_deref());
        let formatted = format_simulation_data(&exec.data, &returns_norm);
        let essentials_mdb = Mdb::from_db(crate::config::get_espo_db(), b"essentials:");
        let mut meta_cache: AlkaneMetaCache = HashMap::new();
        let cards = decode_alkane_cards(&exec.data, &mut meta_cache, &essentials_mdb);
        let (alkanes, alkanes_overflow) = match cards {
            Some(batch) => (Some(batch.items), batch.overflow),
            None => (None, None),
        };
        let addresses = if should_decode_taproot(&returns_norm) {
            decode_address_cards(&exec.data, get_network())
        } else {
            None
        };
        ("success".to_string(), formatted, alkanes, alkanes_overflow, addresses)
    } else {
        ("success".to_string(), "0x".to_string(), None, None, None)
    };

    Json(SimulateResponse {
        ok: true,
        status: Some(status),
        data: Some(data),
        alkanes,
        alkanes_overflow,
        addresses,
        error: None,
    })
}

fn normalize_returns(returns: Option<&str>) -> String {
    returns
        .map(|r| r.chars().filter(|c| !c.is_whitespace()).collect::<String>().to_lowercase())
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| "void".to_string())
}

fn should_decode_taproot(returns_norm: &str) -> bool {
    matches!(returns_norm, "void" | "vec<u8>")
}

fn format_simulation_data(bytes: &[u8], normalized: &str) -> String {
    match normalized {
        "string" => decode_utf8(bytes)
            .or_else(|| decode_u128_value(bytes))
            .unwrap_or_else(|| hex_string(bytes)),
        "u128" => decode_u128(bytes).map(|v| v.to_string()).unwrap_or_else(|| hex_string(bytes)),
        "tuple<u128,u128>" | "(u128,u128)" | "u128,u128" => decode_u128_tuple(bytes)
            .map(|(a, b)| format!("({a}, {b})"))
            .unwrap_or_else(|| hex_string(bytes)),
        "vec<u8>" => decode_u128_vec(bytes).unwrap_or_else(|| hex_string(bytes)),
        "void" => decode_void(bytes),
        _ => decode_void(bytes),
    }
}

fn decode_void(bytes: &[u8]) -> String {
    if let Some(text) = decode_utf8(bytes) {
        return text;
    }
    if let Some(num) = decode_u128(bytes) {
        return num.to_string();
    }
    hex_string(bytes)
}

fn decode_u128_value(bytes: &[u8]) -> Option<String> {
    decode_u128(bytes).map(|num| num.to_string())
}

fn decode_u128_vec(bytes: &[u8]) -> Option<String> {
    if let Some(value) = decode_u128_value(bytes) {
        return Some(value);
    }
    let payload = strip_len_prefix(bytes)?;
    decode_u128_value(payload)
}

fn strip_len_prefix(bytes: &[u8]) -> Option<&[u8]> {
    if bytes.len() >= 5 {
        let mut len_bytes = [0u8; 4];
        len_bytes.copy_from_slice(&bytes[..4]);
        let len = u32::from_le_bytes(len_bytes) as usize;
        if len + 4 == bytes.len() {
            return Some(&bytes[4..]);
        }
    }
    if !bytes.is_empty() {
        let len = bytes[0] as usize;
        if len + 1 == bytes.len() {
            return Some(&bytes[1..]);
        }
    }
    None
}

fn strip_u128_prefix(bytes: &[u8]) -> Option<(usize, &[u8])> {
    if bytes.len() <= 16 {
        return None;
    }
    let remaining = bytes.len() - 16;
    if remaining % 32 != 0 {
        return None;
    }
    let mut count_bytes = [0u8; 16];
    count_bytes.copy_from_slice(&bytes[..16]);
    let count_u128 = u128::from_le_bytes(count_bytes);
    let count = usize::try_from(count_u128).ok()?;
    if count == 0 || count != (remaining / 32) {
        return None;
    }
    Some((count, &bytes[16..]))
}

const MAX_ALKANE_BLOCK: u128 = 6;
const MAX_ALKANE_DISPLAY: usize = 200;
const MAX_ALKANE_SCAN: usize = 5000;

fn decode_address_cards(bytes: &[u8], network: Network) -> Option<Vec<SearchGuessItem>> {
    let address = decode_taproot_address(bytes, network)?;
    let href = explorer_path(&format!("/address/{address}"));
    Some(vec![SearchGuessItem {
        label: address.clone(),
        value: address.clone(),
        href: Some(href),
        icon_url: None,
        fallback_letter: None,
    }])
}

fn decode_taproot_address(
    bytes: &[u8],
    network: Network,
) -> Option<String> {
    let payload = if bytes.len() == 32 {
        Some(bytes)
    } else {
        strip_len_prefix(bytes).filter(|p| p.len() == 32)
    }?;
    let key = XOnlyPublicKey::from_slice(payload).ok()?;
    let secp = Secp256k1::verification_only();
    Some(Address::p2tr(&secp, key, None, network).to_string())
}

struct AlkaneDecodeResult {
    ids: Vec<SchemaAlkaneId>,
    total: usize,
}

struct AlkaneCardBatch {
    items: Vec<SearchGuessItem>,
    overflow: Option<usize>,
}

fn decode_alkane_cards(
    bytes: &[u8],
    meta_cache: &mut AlkaneMetaCache,
    essentials_mdb: &Mdb,
) -> Option<AlkaneCardBatch> {
    let decoded = decode_alkane_ids(bytes)?;
    let mut seen: HashSet<SchemaAlkaneId> = HashSet::new();
    let mut items: Vec<SearchGuessItem> = Vec::new();
    for id in decoded.ids {
        if !seen.insert(id) {
            continue;
        }
        let meta = alkane_meta(&id, meta_cache, essentials_mdb);
        let id_s = format!("{}:{}", id.block, id.tx);
        let label = if meta.name.known { meta.name.value.clone() } else { id_s.clone() };
        let icon_url =
            if !meta.icon_url.trim().is_empty() { Some(meta.icon_url.clone()) } else { None };
        items.push(SearchGuessItem {
            label,
            value: id_s.clone(),
            href: Some(explorer_path(&format!("/alkane/{id_s}"))),
            icon_url,
            fallback_letter: Some(meta.name.fallback_letter().to_string()),
        });
    }
    if items.is_empty() {
        None
    } else {
        let overflow = decoded.total.saturating_sub(MAX_ALKANE_DISPLAY);
        Some(AlkaneCardBatch { items, overflow: if overflow > 0 { Some(overflow) } else { None } })
    }
}

fn decode_alkane_ids(bytes: &[u8]) -> Option<AlkaneDecodeResult> {
    decode_support_alkane_ids(bytes)
        .or_else(|| strip_len_prefix(bytes).and_then(decode_support_alkane_ids))
        .or_else(|| strip_u128_prefix(bytes).and_then(|(count, payload)| {
            decode_support_alkane_ids_prefixed(payload, count)
        }))
        .or_else(|| decode_proto_alkane_id(bytes).map(|id| AlkaneDecodeResult { ids: vec![id], total: 1 }))
        .or_else(|| strip_len_prefix(bytes).and_then(|payload| {
            decode_proto_alkane_id(payload)
                .map(|id| AlkaneDecodeResult { ids: vec![id], total: 1 })
        }))
        .or_else(|| strip_u128_prefix(bytes).and_then(|(_, payload)| {
            decode_proto_alkane_id(payload)
                .map(|id| AlkaneDecodeResult { ids: vec![id], total: 1 })
        }))
}

fn decode_support_alkane_ids_prefixed(bytes: &[u8], total: usize) -> Option<AlkaneDecodeResult> {
    if bytes.is_empty() {
        return None;
    }
    let mut cursor = Cursor::new(bytes.to_vec());
    let mut ids: Vec<SchemaAlkaneId> = Vec::new();
    let max_read = total.min(MAX_ALKANE_DISPLAY);
    for _ in 0..max_read {
        let parsed = SupportAlkaneId::parse(&mut cursor).ok()?;
        let schema = schema_from_support_id(parsed)?;
        ids.push(schema);
    }
    if ids.is_empty() { None } else { Some(AlkaneDecodeResult { ids, total }) }
}

fn decode_support_alkane_ids(bytes: &[u8]) -> Option<AlkaneDecodeResult> {
    if bytes.is_empty() {
        return None;
    }
    let mut cursor = Cursor::new(bytes.to_vec());
    let mut ids: Vec<SchemaAlkaneId> = Vec::new();
    let mut total = 0usize;
    while (cursor.position() as usize) < bytes.len() {
        if total >= MAX_ALKANE_SCAN {
            return None;
        }
        let parsed = SupportAlkaneId::parse(&mut cursor).ok()?;
        let schema = schema_from_support_id(parsed)?;
        total += 1;
        if ids.len() < MAX_ALKANE_DISPLAY {
            ids.push(schema);
        }
    }
    if ids.is_empty() { None } else { Some(AlkaneDecodeResult { ids, total }) }
}

fn decode_proto_alkane_id(bytes: &[u8]) -> Option<SchemaAlkaneId> {
    let parsed = ProtoAlkaneId::decode(bytes).ok()?;
    let schema: SchemaAlkaneId = parsed.try_into().ok()?;
    validate_schema_alkane(schema)
}

fn schema_from_support_id(id: SupportAlkaneId) -> Option<SchemaAlkaneId> {
    if id.block > MAX_ALKANE_BLOCK {
        return None;
    }
    let block = u32::try_from(id.block).ok()?;
    let tx = u64::try_from(id.tx).ok()?;
    validate_schema_alkane(SchemaAlkaneId { block, tx })
}

fn validate_schema_alkane(id: SchemaAlkaneId) -> Option<SchemaAlkaneId> {
    if (id.block as u128) <= MAX_ALKANE_BLOCK {
        Some(id)
    } else {
        None
    }
}

fn decode_utf8(bytes: &[u8]) -> Option<String> {
    if bytes.is_empty() {
        return None;
    }
    let text = String::from_utf8(bytes.to_vec()).ok()?;
    let trimmed = text.trim_matches('\u{0}').to_string();
    if trimmed.is_empty() {
        return None;
    }
    if trimmed.chars().any(|c| c.is_control() && !matches!(c, '\n' | '\r' | '\t')) {
        return None;
    }
    Some(trimmed)
}

fn decode_u128(bytes: &[u8]) -> Option<u128> {
    if bytes.len() != 16 {
        return None;
    }
    let mut buf = [0u8; 16];
    buf.copy_from_slice(bytes);
    Some(u128::from_le_bytes(buf))
}

fn decode_u128_tuple(bytes: &[u8]) -> Option<(u128, u128)> {
    if bytes.len() != 32 {
        return None;
    }
    let mut a = [0u8; 16];
    let mut b = [0u8; 16];
    a.copy_from_slice(&bytes[..16]);
    b.copy_from_slice(&bytes[16..]);
    Some((u128::from_le_bytes(a), u128::from_le_bytes(b)))
}

fn hex_string(bytes: &[u8]) -> String {
    format!("0x{}", hex::encode(bytes))
}

fn parse_alkane_id(s: &str) -> Option<SchemaAlkaneId> {
    let (a, b) = s.split_once(':')?;
    let block = parse_u32_any(a)?;
    let tx = parse_u64_any(b)?;
    Some(SchemaAlkaneId { block, tx })
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
