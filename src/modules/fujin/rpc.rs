use crate::modules::defs::RpcNsRegistrar;
use super::schemas::*;
use super::storage::FujinProvider;
use serde_json::{Value, json};
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

// ── JSON serializers ──

fn epoch_market_json(info: &SchemaEpochInfo, pool_state: Option<&SchemaPoolState>) -> Value {
    let mut v = epoch_info_json(info);
    if let Some(ps) = pool_state {
        v.as_object_mut().unwrap().insert("pool".to_string(), pool_state_json(ps));
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
