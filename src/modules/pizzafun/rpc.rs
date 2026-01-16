use crate::config::get_last_safe_tip;
use crate::modules::defs::RpcNsRegistrar;
use crate::schemas::SchemaAlkaneId;
use serde_json::{Value, json};
use std::sync::Arc;

use super::main::{SeriesEntry, SharedSeriesIndex};

#[inline]
fn log_rpc(method: &str, msg: &str) {
    eprintln!("[RPC::PIZZAFUN] {method} - {msg}");
}

fn parse_alkane_id(s: &str) -> Option<SchemaAlkaneId> {
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

fn normalize_series_id(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

fn confirmations_for(creation_height: u32) -> u32 {
    get_last_safe_tip()
        .map(|tip| tip.saturating_sub(creation_height))
        .unwrap_or(0)
}

fn entry_to_json(entry: &SeriesEntry) -> Value {
    json!({
        "series_id": entry.series_id.clone(),
        "alkane_id": format!("{}:{}", entry.alkane_id.block, entry.alkane_id.tx),
        "confirmations": confirmations_for(entry.creation_height),
    })
}

pub fn register_rpc(reg: RpcNsRegistrar, shared_index: SharedSeriesIndex) {
    eprintln!("[RPC::PIZZAFUN] registering RPC handlers...");

    /* -------- get_series_id_from_alkane_id -------- */
    {
        let reg_one = reg.clone();
        let idx_one = Arc::clone(&shared_index);
        tokio::spawn(async move {
            reg_one
                .register("get_series_id_from_alkane_id", move |_cx, payload| {
                    let idx = Arc::clone(&idx_one);
                    async move {
                        let alk = match payload
                            .get("alkane_id")
                            .and_then(|v| v.as_str())
                            .and_then(parse_alkane_id)
                        {
                            Some(a) => a,
                            None => {
                                log_rpc("get_series_id_from_alkane_id", "missing_or_invalid_alkane_id");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_alkane_id",
                                    "hint": "provide alkane_id as \"<block>:<tx>\" (hex ok)"
                                });
                            }
                        };

                        let current = idx.read().expect("series index lock poisoned").clone();
                        let Some(entry) = current.alkane_to_series.get(&alk) else {
                            return json!({"ok": false, "error": "not_found"});
                        };

                        let mut out = entry_to_json(entry);
                        if let Value::Object(ref mut map) = out {
                            map.insert("ok".to_string(), Value::Bool(true));
                        }
                        out
                    }
                })
                .await;
        });
    }

    /* -------- get_series_ids_from_alkane_ids -------- */
    {
        let reg_batch = reg.clone();
        let idx_batch = Arc::clone(&shared_index);
        tokio::spawn(async move {
            reg_batch
                .register("get_series_ids_from_alkane_ids", move |_cx, payload| {
                    let idx = Arc::clone(&idx_batch);
                    async move {
                        let ids = match payload.get("alkane_ids").and_then(|v| v.as_array()) {
                            Some(v) => v,
                            None => {
                                log_rpc("get_series_ids_from_alkane_ids", "missing_or_invalid_alkane_ids");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_alkane_ids",
                                    "hint": "provide alkane_ids as an array of \"<block>:<tx>\""
                                });
                            }
                        };

                        let current = idx.read().expect("series index lock poisoned").clone();
                        let mut out: Vec<Value> = Vec::with_capacity(ids.len());

                        for raw in ids {
                            let Some(s) = raw.as_str() else {
                                out.push(Value::Null);
                                continue;
                            };
                            let Some(alk) = parse_alkane_id(s) else {
                                out.push(Value::Null);
                                continue;
                            };
                            match current.alkane_to_series.get(&alk) {
                                Some(entry) => out.push(entry_to_json(entry)),
                                None => out.push(Value::Null),
                            }
                        }

                        json!({
                            "ok": true,
                            "items": out,
                        })
                    }
                })
                .await;
        });
    }

    /* -------- get_alkane_id_from_series_id -------- */
    {
        let reg_one = reg.clone();
        let idx_one = Arc::clone(&shared_index);
        tokio::spawn(async move {
            reg_one
                .register("get_alkane_id_from_series_id", move |_cx, payload| {
                    let idx = Arc::clone(&idx_one);
                    async move {
                        let series_id = match payload
                            .get("series_id")
                            .and_then(|v| v.as_str())
                            .and_then(normalize_series_id)
                        {
                            Some(s) => s,
                            None => {
                                log_rpc("get_alkane_id_from_series_id", "missing_or_invalid_series_id");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_series_id"
                                });
                            }
                        };

                        let current = idx.read().expect("series index lock poisoned").clone();
                        let Some(entry) = current.series_to_alkane.get(&series_id) else {
                            return json!({"ok": false, "error": "not_found"});
                        };

                        let mut out = entry_to_json(entry);
                        if let Value::Object(ref mut map) = out {
                            map.insert("ok".to_string(), Value::Bool(true));
                        }
                        out
                    }
                })
                .await;
        });
    }

    /* -------- get_alkane_ids_from_series_ids -------- */
    {
        let reg_batch = reg.clone();
        let idx_batch = Arc::clone(&shared_index);
        tokio::spawn(async move {
            reg_batch
                .register("get_alkane_ids_from_series_ids", move |_cx, payload| {
                    let idx = Arc::clone(&idx_batch);
                    async move {
                        let ids = match payload.get("series_ids").and_then(|v| v.as_array()) {
                            Some(v) => v,
                            None => {
                                log_rpc("get_alkane_ids_from_series_ids", "missing_or_invalid_series_ids");
                                return json!({
                                    "ok": false,
                                    "error": "missing_or_invalid_series_ids",
                                    "hint": "provide series_ids as an array of strings"
                                });
                            }
                        };

                        let current = idx.read().expect("series index lock poisoned").clone();
                        let mut out: Vec<Value> = Vec::with_capacity(ids.len());

                        for raw in ids {
                            let Some(s) = raw.as_str() else {
                                out.push(Value::Null);
                                continue;
                            };
                            let Some(series_id) = normalize_series_id(s) else {
                                out.push(Value::Null);
                                continue;
                            };
                            match current.series_to_alkane.get(&series_id) {
                                Some(entry) => out.push(entry_to_json(entry)),
                                None => out.push(Value::Null),
                            }
                        }

                        json!({
                            "ok": true,
                            "items": out,
                        })
                    }
                })
                .await;
        });
    }
}
