use axum::extract::Query;
use axum::Json;
use serde::Deserialize;
use serde::Serialize;

use crate::config::{get_bitcoind_rpc_client, get_espo_next_height, get_metashrew_rpc_url};
use crate::modules::essentials::storage::trace_count_key;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use alkanes_support::cellpack::Cellpack;
use alkanes_support::id::AlkaneId;
use alkanes_support::proto::alkanes::MessageContextParcel;
use alkanes_support::proto::alkanes::SimulateResponse as SimulateProto;
use anyhow::Context;
use bitcoincore_rpc::RpcApi;
use bitcoin::consensus::Encodable;
use bitcoin::locktime::absolute::LockTime;
use bitcoin::transaction::Version;
use bitcoin::{Amount, OutPoint, ScriptBuf, Sequence, Transaction, TxIn, TxOut};
use ordinals::Runestone;
use protorune_support::protostone::{Protostone, Protostones};
use prost::Message;
use reqwest::Client;
use serde_json::json;

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

pub async fn carousel_blocks(Query(q): Query<CarouselQuery>) -> Json<CarouselResponse> {
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let center = q.center.unwrap_or(espo_tip).min(espo_tip);
    let radius = q.radius.unwrap_or(8).min(50); // guardrail

    let start = center.saturating_sub(radius);
    let end = (center + radius).min(espo_tip);

    let rpc = get_bitcoind_rpc_client();
    let essentials_mdb = Mdb::from_db(crate::config::get_espo_db(), b"essentials:");
    let mut blocks: Vec<CarouselBlock> = Vec::with_capacity((end - start + 1) as usize);

    for h in start..=end {
        let block_hash = match rpc.get_block_hash(h) {
            Ok(bh) => bh,
            Err(_) => continue,
        };

        let header_info = rpc.get_block_header_info(&block_hash).ok();
        let time = header_info.as_ref().map(|hi| hi.time as u32);

        let traces = essentials_mdb
            .get(&trace_count_key(h as u32))
            .ok()
            .flatten()
            .and_then(|b| {
                if b.len() == 4 {
                    let mut arr = [0u8; 4];
                    arr.copy_from_slice(&b);
                    Some(u32::from_le_bytes(arr) as usize)
                } else {
                    None
                }
            })
            .unwrap_or(0);

        blocks.push(CarouselBlock { height: h, traces, time });
    }

    Json(CarouselResponse { espo_tip, blocks })
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
    pub error: Option<String>,
}

pub async fn simulate_contract(Json(req): Json<SimulateRequest>) -> Json<SimulateResponse> {
    let Some(alk) = parse_alkane_id(&req.alkane) else {
        return Json(SimulateResponse {
            ok: false,
            status: None,
            data: None,
            error: Some("invalid_alkane_id".to_string()),
        });
    };

    let cellpack = Cellpack {
        target: AlkaneId { block: alk.block as u128, tx: alk.tx as u128 },
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
                error: Some(format!("protostone_encode_failed: {e}")),
            });
        }
    };
    let runestone = Runestone { protocol: Some(protocol_values), pointer: Some(0), ..Default::default() };
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
            error: Some(format!("tx_encode_failed: {e}")),
        });
    }

    let parcel = MessageContextParcel {
        alkanes: vec![],
        transaction: tx_bytes,
        block: vec![],
        height: get_espo_next_height() as u64,
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
            "latest",
        ],
    });

    let client = Client::new();
    let resp_json: serde_json::Value = match client.post(get_metashrew_rpc_url()).json(&body).send().await {
        Ok(resp) => match resp.error_for_status() {
            Ok(ok) => match ok.json().await {
                Ok(v) => v,
                Err(e) => {
                    return Json(SimulateResponse {
                        ok: false,
                        status: None,
                        data: None,
                        error: Some(format!("response_decode_failed: {e}")),
                    });
                }
            },
            Err(e) => {
                return Json(SimulateResponse {
                    ok: false,
                    status: None,
                    data: None,
                    error: Some(format!("metashrew_http_error: {e}")),
                });
            }
        },
        Err(e) => {
            return Json(SimulateResponse {
                ok: false,
                status: None,
                data: None,
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
                error: Some(format!("simulate_decode_failed: {e}")),
            });
        }
    };

    let (status, data) = if !sim.error.is_empty() {
        ("failure".to_string(), sim.error)
    } else if let Some(exec) = sim.execution {
        (
            "success".to_string(),
            format_simulation_data(&exec.data, req.returns.as_deref()),
        )
    } else {
        ("success".to_string(), "0x".to_string())
    };

    Json(SimulateResponse {
        ok: true,
        status: Some(status),
        data: Some(data),
        error: None,
    })
}

fn format_simulation_data(bytes: &[u8], returns: Option<&str>) -> String {
    let normalized = returns
        .map(|r| r.chars().filter(|c| !c.is_whitespace()).collect::<String>().to_lowercase())
        .filter(|r| !r.is_empty())
        .unwrap_or_else(|| "void".to_string());

    match normalized.as_str() {
        "string" => decode_utf8(bytes).unwrap_or_else(|| hex_string(bytes)),
        "u128" => decode_u128(bytes)
            .map(|v| v.to_string())
            .unwrap_or_else(|| hex_string(bytes)),
        "vec<u8>" => hex_string(bytes),
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
