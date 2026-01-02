use axum::extract::State;
use axum::response::Html;
use bitcoin::{Network, Txid};
use bitcoincore_rpc::RpcApi;
use borsh::BorshDeserialize;
use maud::{Markup, html};
use std::collections::HashSet;

use crate::alkanes::trace::{EspoTrace, get_espo_block};
use crate::config::{get_bitcoind_rpc_client, get_espo_next_height};
use crate::consts::alkanes_genesis_block;
use crate::explorer::components::block_carousel::block_carousel;
use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::icon_right;
use crate::explorer::components::table::{AlkaneTableRow, alkanes_table};
use crate::explorer::components::tx_view::{alkane_icon_url, render_trace_summaries};
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::storage::{
    HoldersCountEntry, alkane_creation_ordered_prefix, decode_creation_record, holders_count_key,
};
use crate::runtime::mempool::{decode_seen_key, get_mempool_mdb, get_tx_from_mempool};

struct AlkaneTxRow {
    txid: Txid,
    trace: EspoTrace,
}

fn load_newest_alkanes(mdb: &crate::runtime::mdb::Mdb, limit: usize) -> Vec<AlkaneTableRow> {
    let mut rows: Vec<AlkaneTableRow> = Vec::new();
    if limit == 0 {
        return rows;
    }

    let prefix_full = mdb.prefixed(alkane_creation_ordered_prefix());
    let it = mdb.iter_prefix_rev(&prefix_full);
    for res in it {
        if rows.len() >= limit {
            break;
        }
        let Ok((_k, v)) = res else { continue };
        let Ok(rec) = decode_creation_record(&v) else { continue };

        let id = format!("{}:{}", rec.alkane.block, rec.alkane.tx);
        let name = rec
            .names
            .first()
            .map(|s| s.to_string())
            .filter(|s| !s.trim().is_empty())
            .unwrap_or_else(|| "Unnamed".to_string());
        let holders = mdb
            .get(&holders_count_key(&rec.alkane))
            .ok()
            .flatten()
            .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
            .map(|hc| hc.count)
            .unwrap_or(0);
        let icon_url = alkane_icon_url(&rec.alkane, mdb);
        let fallback = if name == "Unnamed" {
            '?'
        } else {
            name.chars()
                .find(|c| !c.is_whitespace())
                .map(|c| c.to_ascii_uppercase())
                .unwrap_or('?')
        };
        let creation_txid = hex::encode(rec.txid);

        rows.push(AlkaneTableRow {
            id,
            name,
            holders,
            icon_url,
            fallback,
            creation_height: rec.creation_height,
            creation_txid,
        });
    }

    rows
}

fn load_latest_alkane_txs(espo_tip: u64, network: Network, limit: usize) -> Vec<AlkaneTxRow> {
    let mut out: Vec<AlkaneTxRow> = Vec::new();
    if limit == 0 {
        return out;
    }

    let mut seen: HashSet<Txid> = HashSet::new();
    let mdb = get_mempool_mdb();
    let pref = mdb.prefixed(b"seen/");
    for res in mdb.iter_prefix_rev(&pref) {
        if out.len() >= limit {
            break;
        }
        let Ok((k_full, _)) = res else { continue };
        let rel = &k_full[mdb.prefix().len()..];
        let Some((_, txid)) = decode_seen_key(rel) else { continue };
        if !seen.insert(txid) {
            continue;
        }
        if let Some(entry) = get_tx_from_mempool(&txid) {
            if let Some(traces) = entry.traces.as_ref().filter(|t| !t.is_empty()) {
                if let Some(first) = traces.first().cloned() {
                    out.push(AlkaneTxRow { txid, trace: first });
                }
            }
        }
    }

    if out.len() >= limit || espo_tip == 0 {
        return out;
    }

    let genesis = alkanes_genesis_block(network) as u64;
    let mut height = espo_tip;
    loop {
        if out.len() >= limit || height < genesis {
            break;
        }
        if let Ok(block) = get_espo_block(height, espo_tip) {
            for tx in block.transactions {
                if out.len() >= limit {
                    break;
                }
                let Some(traces) = tx.traces.as_ref().filter(|t| !t.is_empty()) else { continue };
                let txid = tx.transaction.compute_txid();
                if !seen.insert(txid) {
                    continue;
                }
                if let Some(first) = traces.first().cloned() {
                    out.push(AlkaneTxRow { txid, trace: first });
                }
            }
        }
        if height == genesis {
            break;
        }
        height = height.saturating_sub(1);
    }

    out
}

pub async fn home_page(State(state): State<ExplorerState>) -> Html<String> {
    let rpc = get_bitcoind_rpc_client();
    let tip = rpc.get_blockchain_info().map(|i| i.blocks).unwrap_or(0);
    let espo_tip = get_espo_next_height().saturating_sub(1) as u64;
    let latest_height = espo_tip.min(tip);
    let newest_alkanes = load_newest_alkanes(&state.essentials_mdb, 10);
    let latest_alkane_txs = load_latest_alkane_txs(espo_tip, state.network, 4);
    let latest_block_link = format!("/block/{espo_tip}?traces=1");
    let alkanes_link = "/alkanes";

    let newest_alkanes_table: Markup = if newest_alkanes.is_empty() {
        html! { p class="muted" { "No alkanes found." } }
    } else {
        alkanes_table(&newest_alkanes, false, false, true)
    };

    let latest_txs_table: Markup = if latest_alkane_txs.is_empty() {
        html! { p class="muted" { "No alkane transactions found." } }
    } else {
        html! {
            table class="table holders_table home-table" {
                tbody {
                    @for row in latest_alkane_txs {
                        tr {
                            td class="tx-trace-cell" {
                                div class="tx-trace-header" {
                                    a class="link mono tx-trace-id" href=(format!("/tx/{}", row.txid)) { (row.txid) }
                                }
                                (render_trace_summaries(std::slice::from_ref(&row.trace), &state.essentials_mdb))
                            }
                        }
                    }
                }
            }
        }
    };

    layout(
        "Blocks",
        html! {
            div class="block-hero full-bleed" {
                (block_carousel(Some(latest_height), espo_tip))
            }

            div class="home-table-intro" {
                h2 class="home-table-title" {
                    "Explore "
                    span class="home-table-accent" { "programmable" }
                    " Bitcoin"
                }
            }
            div class="grid2 home-table-grid" {
                div class="home-table-block" {
                    div class="home-table-header" {
                            h2 class="h2" { "Latest Traces" }
                        a class="home-table-link" href=(latest_block_link) {
                            "View more alkane txs"
                            (icon_right())
                        }
                    }
                    div class="home-table-card" {
                        (latest_txs_table)
                    }
                }
                div class="home-table-block" {
                    div class="home-table-header" {
                        h2 class="h2" { "Newest alkanes" }
                        a class="home-table-link" href=(alkanes_link) {
                            "View more alkanes"
                            (icon_right())
                        }
                    }
                    div class="home-table-card" {
                        (newest_alkanes_table)
                    }
                }
            }
        },
    )
}
