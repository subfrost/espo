use crate::modules::defs::RpcNsRegistrar;
use super::config::get_fujin_config;
use super::schemas::*;
use super::storage::{FujinProvider, GetIndexHeightParams};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::sync::Arc;

pub fn register_rpc(reg: &RpcNsRegistrar, provider: Arc<FujinProvider>) {
    eprintln!("[RPC::FUJIN] registering RPC handlers…");

    // fujin.ping
    {
        let reg = reg.clone();
        tokio::spawn(async move {
            reg.register("ping", move |_cx, _payload| async move {
                json!("pong")
            })
            .await;
        });
    }

    // fujin.get_markets
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_markets", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    let limit = payload
                        .get("limit")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(50)
                        .clamp(1, 200) as usize;
                    let page = payload
                        .get("page")
                        .and_then(|v| v.as_u64())
                        .unwrap_or(0) as usize;

                    match get_markets_impl(&p, page, limit) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_pool
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_pool", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_pool_impl(&p, &payload) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_vault
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_vault", move |_cx, _payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match p.get_vault_state() {
                        Ok(Some(vs)) => vault_state_json(&vs),
                        Ok(None) => json!({"ok": true, "vault": null}),
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_activity
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_activity", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_activity_impl(&p, &payload) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_settlement_history
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_settlement_history", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_settlement_history_impl(&p, &payload) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_activity_by_address
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_activity_by_address", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_activity_by_address_impl(&p, &payload) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_difficulty
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_difficulty", move |_cx, _payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_difficulty_impl(&p) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_balances
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_balances", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_balances_impl(&p, &payload) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_pool_reserves
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_pool_reserves", move |_cx, payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_pool_reserves_impl(&p, &payload) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }

    // fujin.get_block_height
    {
        let reg = reg.clone();
        let p = Arc::clone(&provider);
        tokio::spawn(async move {
            let handler_p = Arc::clone(&p);
            reg.register("get_block_height", move |_cx, _payload| {
                let p = Arc::clone(&handler_p);
                async move {
                    match get_block_height_impl(&p) {
                        Ok(v) => v,
                        Err(e) => json!({"ok": false, "error": format!("{e}")}),
                    }
                }
            })
            .await;
        });
    }
}

// ── Implementation helpers ──

fn get_markets_impl(p: &FujinProvider, page: usize, limit: usize) -> anyhow::Result<Value> {
    let snapshot = p.get_snapshot()?;
    let Some(snap) = snapshot else {
        return Ok(json!({"ok": true, "markets": [], "total": 0}));
    };

    let total = snap.epochs.len();
    let offset = page * limit;
    let markets: Vec<Value> = snap
        .epochs
        .iter()
        .rev() // newest first
        .skip(offset)
        .take(limit)
        .map(|epoch_info| {
            let pool_state = snap
                .pool_states
                .iter()
                .find(|ps| ps.epoch == epoch_info.epoch);
            epoch_market_json(epoch_info, pool_state)
        })
        .collect();

    Ok(json!({
        "ok": true,
        "markets": markets,
        "total": total,
    }))
}

fn get_pool_impl(p: &FujinProvider, payload: &Value) -> anyhow::Result<Value> {
    // Accept either { pool: "block:tx" } or { epoch: number }
    if let Some(epoch_val) = payload.get("epoch") {
        let epoch = epoch_val.as_u64().unwrap_or(0) as u128;
        let info = p.get_epoch_info(epoch)?;
        let Some(info) = info else {
            return Ok(json!({"ok": false, "error": "epoch_not_found"}));
        };
        let state = p.get_pool_state(&info.pool_id)?;
        return Ok(json!({
            "ok": true,
            "epoch": epoch_info_json(&info),
            "pool": state.map(|s| pool_state_json(&s)),
        }));
    }

    if let Some(pool_str) = payload.get("pool").and_then(|v| v.as_str()) {
        let Some(pool_id) = parse_alkane_id_str(pool_str) else {
            return Ok(json!({"ok": false, "error": "invalid_pool_id"}));
        };
        let state = p.get_pool_state(&pool_id)?;
        // Find epoch info by scanning epochs
        let epochs = p.get_epoch_list()?;
        let mut epoch_info_val = None;
        for e in &epochs {
            if let Ok(Some(info)) = p.get_epoch_info(*e) {
                if info.pool_id == pool_id {
                    epoch_info_val = Some(epoch_info_json(&info));
                    break;
                }
            }
        }
        return Ok(json!({
            "ok": true,
            "epoch": epoch_info_val,
            "pool": state.map(|s| pool_state_json(&s)),
        }));
    }

    Ok(json!({"ok": false, "error": "must provide pool or epoch param"}))
}

fn get_activity_impl(p: &FujinProvider, payload: &Value) -> anyhow::Result<Value> {
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(50)
        .clamp(1, 200) as usize;
    let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let offset = page * limit;

    let table = p.table();

    let prefix = if let Some(pool_str) = payload.get("pool").and_then(|v| v.as_str()) {
        let Some(pool_id) = parse_alkane_id_str(pool_str) else {
            return Ok(json!({"ok": false, "error": "invalid_pool_id"}));
        };
        table.activity_pool_prefix(&pool_id)
    } else {
        table.ACTIVITY_ALL.key().to_vec()
    };

    let (entries, total) = p.get_activity_iter(&prefix, offset, limit)?;

    let items: Vec<Value> = entries.iter().map(activity_json).collect();
    Ok(json!({"ok": true, "items": items, "total": total}))
}

fn get_settlement_history_impl(p: &FujinProvider, payload: &Value) -> anyhow::Result<Value> {
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(50)
        .clamp(1, 200) as usize;
    let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(0) as usize;

    let epochs = p.get_epoch_list()?;
    let mut settlements = Vec::new();
    for e in epochs.iter().rev() {
        if let Ok(Some(s)) = p.get_settlement(*e) {
            settlements.push(s);
        }
    }

    let total = settlements.len();
    let offset = page * limit;
    let items: Vec<Value> = settlements
        .into_iter()
        .skip(offset)
        .take(limit)
        .map(|s| settlement_json(&s))
        .collect();

    Ok(json!({"ok": true, "items": items, "total": total}))
}

fn get_activity_by_address_impl(p: &FujinProvider, payload: &Value) -> anyhow::Result<Value> {
    let Some(address) = payload.get("address").and_then(|v| v.as_str()) else {
        return Ok(json!({"ok": false, "error": "invalid_address"}));
    };
    let Some(spk) = address_to_spk(address) else {
        return Ok(json!({"ok": false, "error": "invalid_address"}));
    };
    let limit = payload
        .get("limit")
        .and_then(|v| v.as_u64())
        .unwrap_or(50)
        .clamp(1, 200) as usize;
    let page = payload.get("page").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
    let offset = page * limit;

    let table = p.table();
    let prefix = table.activity_addr_prefix(&spk);
    let (entries, total) = p.get_activity_iter(&prefix, offset, limit)?;

    let items: Vec<Value> = entries.iter().map(activity_json).collect();
    Ok(json!({"ok": true, "items": items, "total": total}))
}

// ── Difficulty helpers ──

const RETARGET_INTERVAL: u32 = 2016;
const EXPECTED_BLOCK_TIME: f64 = 600.0;

fn nbits_to_difficulty(bits: u32) -> f64 {
    let exponent = (bits >> 24) & 0xff;
    let mantissa = (bits & 0x00ffffff) as f64;
    if mantissa == 0.0 {
        return 0.0;
    }
    let diff1_target = 0x00ffff_u64 as f64 * 256.0f64.powi(0x1d - 3);
    let target = mantissa * 256.0f64.powi(exponent as i32 - 3);
    diff1_target / target
}

fn parse_header_time(header: &[u8]) -> Option<u32> {
    if header.len() < 80 {
        return None;
    }
    Some(u32::from_le_bytes([header[68], header[69], header[70], header[71]]))
}

fn parse_header_bits(header: &[u8]) -> Option<u32> {
    if header.len() < 80 {
        return None;
    }
    Some(u32::from_le_bytes([header[72], header[73], header[74], header[75]]))
}

fn get_difficulty_impl(p: &FujinProvider) -> anyhow::Result<Value> {
    let current_height = match p.get_index_height(GetIndexHeightParams)? {
        super::storage::GetIndexHeightResult { height: Some(h) } => h,
        _ => return Ok(json!({"ok": false, "error": "index_height_not_available"})),
    };

    let current_header = match p.get_essentials_block_summary(current_height)? {
        Some(h) => h,
        None => return Ok(json!({"ok": false, "error": "block_header_not_found"})),
    };

    let current_time = parse_header_time(&current_header)
        .ok_or_else(|| anyhow::anyhow!("invalid header length"))? as u64;
    let current_bits = parse_header_bits(&current_header)
        .ok_or_else(|| anyhow::anyhow!("invalid header length"))?;

    let period_start = (current_height / RETARGET_INTERVAL) * RETARGET_INTERVAL;
    let blocks_since = current_height - period_start;
    let next_retarget = period_start + RETARGET_INTERVAL;
    let blocks_until = next_retarget - current_height;
    let progress_pct = blocks_since as f64 / RETARGET_INTERVAL as f64 * 100.0;

    let difficulty = nbits_to_difficulty(current_bits);

    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    // Compute estimated change and remaining time if we have enough blocks
    let (estimated_change, remaining_time, avg_block_time) = if blocks_since >= 10 {
        match p.get_essentials_block_summary(period_start)? {
            Some(period_header) => {
                let period_start_time = parse_header_time(&period_header)
                    .ok_or_else(|| anyhow::anyhow!("invalid period start header"))? as f64;
                let actual_time = current_time as f64 - period_start_time;
                let avg_bt = actual_time / blocks_since as f64;
                let projected_period_time = avg_bt * RETARGET_INTERVAL as f64;
                let expected_period_time = RETARGET_INTERVAL as f64 * EXPECTED_BLOCK_TIME;
                let est_change = (expected_period_time / projected_period_time - 1.0) * 100.0;
                let rem_time = blocks_until as f64 * avg_bt * 1000.0;
                (Some(est_change), Some(rem_time as u64), Some(avg_bt))
            }
            None => (None, None, None),
        }
    } else {
        (None, None, None)
    };

    let estimated_retarget_date = remaining_time
        .map(|rt| now_ms + rt)
        .unwrap_or(0);

    Ok(json!({
        "ok": true,
        "currentDifficulty": difficulty,
        "currentHeight": current_height,
        "nextRetargetHeight": next_retarget,
        "blocksUntilRetarget": blocks_until,
        "estimatedRetargetDate": estimated_retarget_date,
        "estimatedChange": estimated_change,
        "progressPercent": progress_pct,
        "remainingTime": remaining_time,
        "averageBlockTime": avg_block_time,
    }))
}

fn get_balances_impl(p: &FujinProvider, payload: &Value) -> anyhow::Result<Value> {
    let Some(address) = payload.get("address").and_then(|v| v.as_str()) else {
        return Ok(json!({"ok": false, "error": "missing_address"}));
    };

    let result = p.get_address_balances(address)?;
    let balances_map: HashMap<String, String> = result
        .get("balances")
        .and_then(|v| v.as_object())
        .map(|obj| {
            obj.iter()
                .map(|(k, v)| (k.clone(), v.as_str().unwrap_or("0").to_string()))
                .collect()
        })
        .unwrap_or_default();

    // DIESEL is always 2:0
    let diesel = balances_map.get("2:0").cloned().unwrap_or_else(|| "0".to_string());

    // Optional token IDs from params
    let long_bal = payload
        .get("long")
        .and_then(|v| v.as_str())
        .and_then(|id| balances_map.get(id))
        .cloned()
        .unwrap_or_else(|| "0".to_string());

    let short_bal = payload
        .get("short")
        .and_then(|v| v.as_str())
        .and_then(|id| balances_map.get(id))
        .cloned()
        .unwrap_or_else(|| "0".to_string());

    let lp_bal = payload
        .get("pool")
        .and_then(|v| v.as_str())
        .and_then(|id| balances_map.get(id))
        .cloned()
        .unwrap_or_else(|| "0".to_string());

    // Vault shares from config
    let config = get_fujin_config();
    let vault_id_str = format!("{}:{}", config.vault_id.block, config.vault_id.tx);
    let vault_shares = balances_map
        .get(&vault_id_str)
        .cloned()
        .unwrap_or_else(|| "0".to_string());

    // Build allBalances array
    let all_balances: Vec<Value> = balances_map
        .iter()
        .map(|(id, balance)| json!({"id": id, "balance": balance}))
        .collect();

    Ok(json!({
        "ok": true,
        "diesel": diesel,
        "long": long_bal,
        "short": short_bal,
        "lp": lp_bal,
        "vaultShares": vault_shares,
        "allBalances": all_balances,
    }))
}

fn get_pool_reserves_impl(p: &FujinProvider, payload: &Value) -> anyhow::Result<Value> {
    let Some(pool_str) = payload.get("pool").and_then(|v| v.as_str()) else {
        return Ok(json!({"ok": false, "error": "missing_pool_param"}));
    };
    let Some(pool_id) = parse_alkane_id_str(pool_str) else {
        return Ok(json!({"ok": false, "error": "invalid_pool_id"}));
    };

    // Read on-chain storage keys from pool contract
    let diesel_locked = p
        .read_alkane_storage(pool_id.clone(), b"/diesel")?
        .map(|b| {
            if b.len() >= 16 {
                u128::from_le_bytes(b[..16].try_into().unwrap())
            } else {
                0u128
            }
        })
        .unwrap_or(0);

    let end_height = p
        .read_alkane_storage(pool_id.clone(), b"/event/end_height")?
        .map(|b| {
            if b.len() >= 16 {
                u128::from_le_bytes(b[..16].try_into().unwrap())
            } else {
                0u128
            }
        })
        .unwrap_or(0);

    let settled_raw = p
        .read_alkane_storage(pool_id.clone(), b"/event/settled")?
        .map(|b| {
            if b.len() >= 16 {
                u128::from_le_bytes(b[..16].try_into().unwrap())
            } else {
                0u128
            }
        })
        .unwrap_or(0);
    let settled = settled_raw != 0;

    let long_payout_q64 = p
        .read_alkane_storage(pool_id.clone(), b"/event/long_payout")?
        .map(|b| {
            if b.len() >= 16 {
                u128::from_le_bytes(b[..16].try_into().unwrap())
            } else {
                0u128
            }
        })
        .unwrap_or(0);

    let short_payout_q64 = p
        .read_alkane_storage(pool_id.clone(), b"/event/short_payout")?
        .map(|b| {
            if b.len() >= 16 {
                u128::from_le_bytes(b[..16].try_into().unwrap())
            } else {
                0u128
            }
        })
        .unwrap_or(0);

    let total_fee_per_1000 = p
        .read_alkane_storage(pool_id.clone(), b"/totalfeeper1000")?
        .map(|b| {
            if b.len() >= 16 {
                u128::from_le_bytes(b[..16].try_into().unwrap())
            } else {
                0u128
            }
        })
        .unwrap_or(0);

    // Get reserves from snapshot
    let (reserve_long, reserve_short, lp_total_supply) = match p.get_snapshot()? {
        Some(snap) => {
            let pool_state = snap
                .pool_states
                .iter()
                .find(|ps| {
                    // Match by looking up the epoch info
                    snap.epochs.iter().any(|e| {
                        e.pool_id.block == pool_id.block
                            && e.pool_id.tx == pool_id.tx
                            && e.epoch == ps.epoch
                    })
                });
            match pool_state {
                Some(ps) => (
                    ps.reserve_long.to_string(),
                    ps.reserve_short.to_string(),
                    ps.lp_total_supply.to_string(),
                ),
                None => ("0".to_string(), "0".to_string(), "0".to_string()),
            }
        }
        None => ("0".to_string(), "0".to_string(), "0".to_string()),
    };

    Ok(json!({
        "ok": true,
        "reserve_long": reserve_long,
        "reserve_short": reserve_short,
        "diesel_locked": diesel_locked.to_string(),
        "lp_total_supply": lp_total_supply,
        "total_fee_per_1000": total_fee_per_1000.to_string(),
        "end_height": end_height.to_string(),
        "settled": settled,
        "long_payout_q64": long_payout_q64.to_string(),
        "short_payout_q64": short_payout_q64.to_string(),
    }))
}

fn get_block_height_impl(p: &FujinProvider) -> anyhow::Result<Value> {
    let height = match p.get_snapshot()? {
        Some(snap) => snap.last_height,
        None => 0,
    };
    Ok(json!({"ok": true, "height": height}))
}

// ── JSON serializers ──

fn epoch_market_json(info: &SchemaEpochInfo, pool_state: Option<&SchemaPoolState>) -> Value {
    let mut v = epoch_info_json(info);
    if let Some(ps) = pool_state {
        v.as_object_mut().unwrap().insert("pool".to_string(), pool_state_json(ps));
        v.as_object_mut().unwrap().insert(
            "start_difficulty".to_string(),
            json!(nbits_to_difficulty(ps.start_bits)),
        );
    }
    v
}

fn epoch_info_json(info: &SchemaEpochInfo) -> Value {
    json!({
        "epoch": info.epoch.to_string(),
        "pool_id": format!("{}:{}", info.pool_id.block, info.pool_id.tx),
        "long_id": format!("{}:{}", info.long_id.block, info.long_id.tx),
        "short_id": format!("{}:{}", info.short_id.block, info.short_id.tx),
        "creation_height": info.creation_height,
        "creation_ts": info.creation_ts,
    })
}

fn pool_state_json(ps: &SchemaPoolState) -> Value {
    json!({
        "epoch": ps.epoch.to_string(),
        "reserve_long": ps.reserve_long.to_string(),
        "reserve_short": ps.reserve_short.to_string(),
        "diesel_locked": ps.diesel_locked.to_string(),
        "total_fee_per_1000": ps.total_fee_per_1000.to_string(),
        "lp_total_supply": ps.lp_total_supply.to_string(),
        "start_bits": ps.start_bits,
        "end_height": ps.end_height.to_string(),
        "settled": ps.settled,
        "long_payout_q64": ps.long_payout_q64.to_string(),
        "short_payout_q64": ps.short_payout_q64.to_string(),
        "long_price_scaled": ps.long_price_scaled.to_string(),
        "short_price_scaled": ps.short_price_scaled.to_string(),
        "blocks_remaining": ps.blocks_remaining,
    })
}

fn vault_state_json(vs: &SchemaVaultState) -> Value {
    json!({
        "ok": true,
        "vault": {
            "factory_id": format!("{}:{}", vs.factory_id.block, vs.factory_id.tx),
            "pool_id": format!("{}:{}", vs.pool_id.block, vs.pool_id.tx),
            "lp_balance": vs.lp_balance.to_string(),
            "total_supply": vs.total_supply.to_string(),
            "share_price_scaled": vs.share_price_scaled.to_string(),
        }
    })
}

fn activity_json(a: &SchemaFujinActivityV1) -> Value {
    let mut txid = a.txid;
    txid.reverse();
    json!({
        "txid": hex::encode(txid),
        "timestamp": a.timestamp,
        "kind": a.kind.as_str(),
        "pool_id": format!("{}:{}", a.pool_id.block, a.pool_id.tx),
        "epoch": a.epoch.to_string(),
        "long_delta": a.long_delta.to_string(),
        "short_delta": a.short_delta.to_string(),
        "diesel_delta": a.diesel_delta.to_string(),
        "lp_delta": a.lp_delta.to_string(),
        "address_spk": hex::encode(&a.address_spk),
        "success": a.success,
    })
}

fn settlement_json(s: &SchemaSettlementV1) -> Value {
    json!({
        "epoch": s.epoch.to_string(),
        "pool_id": format!("{}:{}", s.pool_id.block, s.pool_id.tx),
        "start_bits": s.start_bits,
        "end_bits": s.end_bits,
        "long_payout_q64": s.long_payout_q64.to_string(),
        "short_payout_q64": s.short_payout_q64.to_string(),
        "settled_height": s.settled_height,
        "difficulty_change_pct": s.difficulty_change_pct,
    })
}

fn parse_alkane_id_str(s: &str) -> Option<crate::schemas::SchemaAlkaneId> {
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return None;
    }
    let block: u32 = parts[0].parse().ok()?;
    let tx: u64 = parts[1].parse().ok()?;
    Some(crate::schemas::SchemaAlkaneId { block, tx })
}

fn address_to_spk(address: &str) -> Option<Vec<u8>> {
    use bitcoin::Address;
    use crate::config::get_network;
    use std::str::FromStr;

    let network = get_network();
    Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .map(|a| a.script_pubkey().into_bytes())
}
