use axum::extract::{Query, State};
use axum::response::Html;
use borsh::BorshDeserialize;
use maud::{Markup, html};
use serde::Deserialize;

use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::{
    icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::table::{AlkaneTableRow, alkanes_table};
use crate::explorer::components::tx_view::alkane_icon_url;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::storage::{
    HoldersCountEntry, alkane_creation_count_key, alkane_creation_ordered_prefix,
    decode_creation_record, holders_count_key,
};

#[derive(Deserialize)]
pub struct PageQuery {
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

pub async fn alkanes_page(
    State(state): State<ExplorerState>,
    Query(q): Query<PageQuery>,
) -> Html<String> {
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(50).clamp(1, 50);
    let offset = limit.saturating_mul(page.saturating_sub(1));

    let total: u64 = state
        .essentials_mdb
        .get(alkane_creation_count_key())
        .ok()
        .flatten()
        .and_then(|b| {
            if b.len() == 8 {
                let mut arr = [0u8; 8];
                arr.copy_from_slice(&b);
                Some(u64::from_le_bytes(arr))
            } else {
                None
            }
        })
        .unwrap_or(0);

    let mut rows: Vec<AlkaneTableRow> = Vec::new();
    let prefix_full = state.essentials_mdb.prefixed(alkane_creation_ordered_prefix());
    let it = state.essentials_mdb.iter_prefix_rev(&prefix_full);
    let mut seen: usize = 0;
    for res in it {
        let Ok((_k, v)) = res else { continue };
        if seen < offset {
            seen += 1;
            continue;
        }
        if rows.len() >= limit {
            break;
        }
        match decode_creation_record(&v) {
            Ok(rec) => {
                let id = format!("{}:{}", rec.alkane.block, rec.alkane.tx);
                let name = rec
                    .names
                    .first()
                    .map(|s| s.to_string())
                    .filter(|s| !s.trim().is_empty())
                    .unwrap_or_else(|| "Unnamed".to_string());
                let holders = state
                    .essentials_mdb
                    .get(&holders_count_key(&rec.alkane))
                    .ok()
                    .flatten()
                    .and_then(|b| HoldersCountEntry::try_from_slice(&b).ok())
                    .map(|hc| hc.count)
                    .unwrap_or(0);

                let icon_url = alkane_icon_url(&rec.alkane, &state.essentials_mdb);
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
            Err(_) => continue,
        }
        seen += 1;
    }

    let display_start = if total > 0 && offset < total as usize { (offset + 1) as u64 } else { 0 };
    let display_end = (offset as u64 + rows.len() as u64).min(total);
    let has_prev = page > 1;
    let has_next = (offset as u64 + rows.len() as u64) < total;
    let last_page = if total > 0 { ((total + limit as u64 - 1) / limit as u64).max(1) } else { 1 };
    let show_creation_block = has_prev || has_next;

    let table: Markup = if rows.is_empty() {
        html! { p class="muted" { "No alkanes found." } }
    } else {
        html! { div class="alkanes-card" { (alkanes_table(&rows, true, show_creation_block, true)) } }
    };

    layout(
        "Alkanes",
        html! {
            div class="row" {
                h1 class="h1" { "All Alkanes" }
            }
            (table)
            div class="pager" {
                @if has_prev {
                    a class="pill iconbtn" href=(format!("/alkanes?page=1&limit={limit}")) aria-label="First page" { (icon_skip_left()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                }
                @if has_prev {
                    a class="pill iconbtn" href=(format!("/alkanes?page={}&limit={limit}", page - 1)) aria-label="Previous page" { (icon_left()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_left()) }
                }
                span class="pager-meta muted" { "Showing "
                    (display_start)
                    @if total > 0 {
                        "-"
                        (display_end)
                    }
                    " / "
                    (total)
                }
                @if has_next {
                    a class="pill iconbtn" href=(format!("/alkanes?page={}&limit={limit}", page + 1)) aria-label="Next page" { (icon_right()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                }
                @if has_next {
                    a class="pill iconbtn" href=(format!("/alkanes?page={}&limit={limit}", last_page)) aria-label="Last page" { (icon_skip_right()) }
                } @else {
                    span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                }
            }
        },
    )
}
