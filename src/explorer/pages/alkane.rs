use axum::extract::{Path, Query, State};
use axum::response::Html;
use hex;
use maud::{Markup, PreEscaped, html};
use serde::Deserialize;

use crate::explorer::components::alk_balances::render_alkane_balance_cards;
use crate::explorer::components::header::header_scripts;
use crate::explorer::components::layout::layout;
use crate::explorer::components::svg_assets::{
    icon_caret_right, icon_left, icon_right, icon_skip_left, icon_skip_right,
};
use crate::explorer::components::table::holders_table;
use crate::explorer::components::tx_view::{
    AlkaneMetaCache, alkane_icon_url_unfiltered, alkane_meta, icon_bg_style,
};
use crate::explorer::pages::common::fmt_alkane_amount;
use crate::explorer::pages::state::ExplorerState;
use crate::modules::essentials::storage::{BalanceEntry, HolderId, load_creation_record};
use crate::modules::essentials::utils::balances::{get_alkane_balances, get_holders_for_alkane};
use crate::modules::essentials::utils::inspections::{StoredInspectionMethod, load_inspection};

const ADDR_SUFFIX_LEN: usize = 8;

#[derive(Deserialize)]
pub struct PageQuery {
    pub tab: Option<String>,
    pub page: Option<usize>,
    pub limit: Option<usize>,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum AlkaneTab {
    Holders,
    Inspect,
}

impl AlkaneTab {
    fn from_query(raw: Option<&str>) -> Self {
        match raw {
            Some("inspect") => AlkaneTab::Inspect,
            _ => AlkaneTab::Holders,
        }
    }
}

pub async fn alkane_page(
    State(state): State<ExplorerState>,
    Path(alkane_raw): Path<String>,
    Query(q): Query<PageQuery>,
) -> Html<String> {
    let Some(alk) = parse_alkane_id(&alkane_raw) else {
        return layout(
            "Alkane",
            html! { p class="error" { "Invalid alkane id; expected \"<block>:<tx>\"." } },
        );
    };

    let tab = AlkaneTab::from_query(q.tab.as_deref());
    let page = q.page.unwrap_or(1).max(1);
    let limit = q.limit.unwrap_or(50).clamp(1, 200);
    let alk_str = format!("{}:{}", alk.block, alk.tx);
    let mut kv_cache: AlkaneMetaCache = Default::default();
    let meta = alkane_meta(&alk, &mut kv_cache, &state.essentials_mdb);
    let display_name = meta.name.value.clone();
    let fallback_letter = meta.name.fallback_letter();
    let hero_icon_url = alkane_icon_url_unfiltered(&alk, &state.essentials_mdb);
    let page_title = if meta.name.known && display_name != alk_str {
        format!("Alkane {display_name} ({alk_str})")
    } else {
        format!("Alkane {alk_str}")
    };

    let creation_record = load_creation_record(&state.essentials_mdb, &alk).ok().flatten();
    let creation_ts = creation_record.as_ref().map(|r| r.creation_timestamp as u64);
    let creation_height = creation_record.as_ref().map(|r| r.creation_height);
    let creation_txid = creation_record.as_ref().map(|r| hex::encode(r.txid));

    let balances_map = get_alkane_balances(&state.essentials_mdb, &alk).unwrap_or_default();
    let mut balance_entries: Vec<BalanceEntry> = balances_map
        .into_iter()
        .map(|(alk, amt)| BalanceEntry { alkane: alk, amount: amt })
        .collect();
    balance_entries.sort_by(|a, b| {
        a.alkane.block.cmp(&b.alkane.block).then_with(|| a.alkane.tx.cmp(&b.alkane.tx))
    });

    let (total, circulating_supply, holders) =
        get_holders_for_alkane(&state.essentials_mdb, alk, page, limit).unwrap_or((
            0,
            0,
            Vec::new(),
        ));
    let off = limit.saturating_mul(page.saturating_sub(1));
    let holders_len = holders.len();
    let has_prev = page > 1;
    let has_next = off + holders_len < total;
    let display_start = if total > 0 && off < total { off + 1 } else { 0 };
    let display_end = (off + holders_len).min(total);
    let last_page = if total > 0 { (total + limit - 1) / limit } else { 1 };
    let icon_url = meta.icon_url.clone();
    let coin_label = meta.name.value.clone();
    let holders_count = total;

    let inspection = creation_record.as_ref().and_then(|r| r.inspection.as_ref());
    let (view_methods, write_methods) = split_methods(inspection);
    let (factory_name, factory_id_str) =
        resolve_factory_contract(inspection, &alk, &mut kv_cache, &state.essentials_mdb);

    let rows: Vec<Vec<Markup>> = holders
        .into_iter()
        .enumerate()
        .map(|(idx, h)| {
            let rank = off + idx + 1;
            let holder_cell = match h.holder {
                HolderId::Address(addr) => {
                    let (addr_prefix, addr_suffix) = addr_prefix_suffix(&addr);
                    html! {
                        a class="link mono addr-inline" href=(format!("/address/{}", addr)) {
                            span class="addr-rank" { (format!("{rank}.")) }
                            span class="addr-prefix" { (addr_prefix) }
                            span class="addr-suffix" { (addr_suffix) }
                        }
                    }
                }
                HolderId::Alkane(id) => {
                    let id_str = format!("{}:{}", id.block, id.tx);
                    let h_meta = alkane_meta(&id, &mut kv_cache, &state.essentials_mdb);
                    let h_fallback_letter = h_meta.name.fallback_letter();
                    html! {
                        a class="link mono addr-inline" href=(format!("/alkane/{id_str}")) {
                            span class="addr-rank" { (format!("{rank}.")) }
                            div class="alk-icon-wrap" aria-hidden="true" {
                                span class="alk-icon-img" style=(icon_bg_style(&h_meta.icon_url)) {}
                                span class="alk-icon-letter" { (h_fallback_letter) }
                            }
                            span class="addr-prefix" { (h_meta.name.value.clone()) }
                            span class="addr-suffix mono" { (format!(" ({id_str})")) }
                        }
                    }
                }
            };
            vec![
                holder_cell,
                html! {
                    div class="alk-line" {
                        div class="alk-icon-wrap" aria-hidden="true" {
                            span class="alk-icon-img" style=(icon_bg_style(&icon_url)) {}
                            span class="alk-icon-letter" { (fallback_letter) }
                        }
                        span class="alk-amt mono" { (fmt_alkane_amount(h.amount)) }
                        a class="alk-sym link mono" href=(format!("/alkane/{alk_str}")) { (coin_label.clone()) }
                    }
                },
            ]
        })
        .collect();

    let table_markup = if rows.is_empty() {
        html! { div class="alkane-panel" { p class="muted" { "No holders." } } }
    } else {
        html! {
            div class="alkane-panel alkane-holders-card" {
                (holders_table(&["Holder", "Balance"], rows))
            }
        }
    };

    let balances_markup = if balance_entries.is_empty() {
        html! { p class="muted" { "No alkanes tracked for this alkane." } }
    } else {
        render_alkane_balance_cards(&balance_entries, &state.essentials_mdb)
    };

    layout(
        &page_title,
        html! {
            div class="alkane-page" {
                div class="alkane-hero-card" {
                    div class="alk-icon-wrap alk-icon-lg" aria-hidden="true" {
                        span class="alk-icon-img" style=(icon_bg_style(&hero_icon_url)) {}
                        span class="alk-icon-letter" { (fallback_letter) }
                    }
                    div class="alkane-hero-text" {
                        span class="alkane-tag" { "TOKEN" }
                        h1 class="alkane-hero-title" { (display_name.clone()) }
                        span class="alkane-hero-id mono" { (alk_str.clone()) }
                    }
                }

                section class="alkane-section" {
                    h2 class="section-title" { "Overview" }
                    div class="alkane-overview-card" {
                        div class="alkane-stat" {
                            span class="alkane-stat-label" { "Symbol" }
                            div class="alkane-stat-line" {
                                span class="alkane-stat-value" { (meta.symbol.clone()) }
                            }
                        }
                        div class="alkane-stat" {
                            span class="alkane-stat-label" { "Circulating supply" }
                            div class="alkane-stat-line" {
                                span class="alkane-stat-value" { (fmt_alkane_amount(circulating_supply)) }
                                span class="alkane-stat-sub" { "(with 8 decimals)" }
                            }
                        }
                        div class="alkane-stat" {
                            span class="alkane-stat-label" { "Holders" }
                            div class="alkane-stat-line" {
                                span class="alkane-stat-value" { (holders_count) }
                            }
                        }
                        div class="alkane-stat" {
                            span class="alkane-stat-label" { "Deploy date" }
                            @if let Some(ts) = creation_ts {
                                div class="alkane-stat-line" data-ts-group="" {
                                    span class="alkane-stat-value" data-header-ts=(ts) { (ts) }
                                    span class="alkane-stat-sub" data-header-ts-rel { "" }
                                }
                            } @else {
                                div class="alkane-stat-line" {
                                    span class="alkane-stat-value muted" { "Unknown" }
                                }
                            }
                        }
                        div class="alkane-stat" {
                            span class="alkane-stat-label" { "Deploy transaction" }
                            @if let Some(txid) = creation_txid.as_ref() {
                                div class="alkane-stat-line" {
                                    a class="alkane-stat-value link mono" href=(format!("/tx/{txid}")) { (short_hex(txid)) }
                                }
                            } @else {
                                div class="alkane-stat-line" {
                                    span class="alkane-stat-value muted" { "Unknown" }
                                }
                            }
                        }
                        div class="alkane-stat" {
                            span class="alkane-stat-label" { "Deploy block" }
                            @if let Some(h) = creation_height {
                                div class="alkane-stat-line" {
                                    a class="alkane-stat-value link" href=(format!("/block/{h}")) { (h) }
                                }
                            } @else {
                                div class="alkane-stat-line" {
                                    span class="alkane-stat-value muted" { "Unknown" }
                                }
                            }
                        }
                    }
                }

                section class="alkane-section" {
                    h2 class="section-title" { "Alkane Balances" }
                    (balances_markup)
                }

                section class="alkane-section" {
                    div class="alkane-tabs" {
                        div class="alkane-tab-list" {
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::Holders { " active" } else { "" }))
                                href=(format!("/alkane/{alk_str}?page={page}&limit={limit}")) { "Holders" }
                            a class=(format!("alkane-tab{}", if tab == AlkaneTab::Inspect { " active" } else { "" }))
                                href=(format!("/alkane/{alk_str}?tab=inspect&page={page}&limit={limit}")) { "Inspect contract" }
                        }
                        div class="alkane-tab-panel" {
                            @if tab == AlkaneTab::Holders {
                                (table_markup)
                                div class="pager" {
                                    @if has_prev {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page=1&limit={limit}")) aria-label="First page" {
                                            (icon_skip_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_left()) }
                                    }
                                    @if has_prev {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page={}&limit={limit}", page - 1)) aria-label="Previous page" {
                                            (icon_left())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_left()) }
                                    }
                                    span class="pager-meta muted" { "Showing "
                                        (if total > 0 { display_start } else { 0 })
                                        @if total > 0 {
                                            "-"
                                            (display_end)
                                        }
                                        " / "
                                        (total)
                                    }
                                    @if has_next {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page={}&limit={limit}", page + 1)) aria-label="Next page" {
                                            (icon_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_right()) }
                                    }
                                    @if has_next {
                                        a class="pill iconbtn" href=(format!("/alkane/{alk_str}?page={}&limit={limit}", last_page)) aria-label="Last page" {
                                            (icon_skip_right())
                                        }
                                    } @else {
                                        span class="pill disabled iconbtn" aria-hidden="true" { (icon_skip_right()) }
                                    }
                                }
                            } @else {
                                div class="alkane-inspect-card" data-alkane-inspect="" data-alkane-id=(alk_str.clone()) {
                                    div class="alkane-inspect-header" {
                                        span class="alkane-inspect-name" { (factory_name.clone()) }
                                        span class="alkane-inspect-id mono" { (factory_id_str.clone()) }
                                    }
                                    @if view_methods.is_empty() && write_methods.is_empty() {
                                        p class="muted" { "No contract methods found." }
                                    } @else {
                                        div class="alkane-method-group" {
                                            h3 class="alkane-method-title" { "Read methods:" }
                                            @if view_methods.is_empty() {
                                                p class="muted" { "No read methods." }
                                            } @else {
                                                @for method in &view_methods {
                                                    details class="opret-toggle alkane-method-toggle" data-alkane-method=(method.name.clone()) data-alkane-opcode=(method.opcode) data-alkane-returns=(method.returns.clone()) data-alkane-view="1" {
                                                        summary class="opret-toggle-summary" {
                                                            span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                                                            span class="opret-toggle-label" { (method.name.clone()) }
                                                            span class="trace-opcode" { (format!("opcode {}", method.opcode)) }
                                                        }
                                                        div class="opret-toggle-body" {
                                                            div class="alkane-method-result" data-sim-result="" data-status="idle" {
                                                                span class="alkane-method-label" { "Result:" }
                                                                span class="alkane-method-value" data-sim-value="" { "—" }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                        div class="alkane-method-group" {
                                            h3 class="alkane-method-title" { "Write methods:" }
                                            @if write_methods.is_empty() {
                                                p class="muted" { "No write methods." }
                                            } @else {
                                                @for method in &write_methods {
                                                    details class="opret-toggle alkane-method-toggle" data-alkane-method=(method.name.clone()) data-alkane-opcode=(method.opcode) data-alkane-returns=(method.returns.clone()) data-alkane-view="0" {
                                                        summary class="opret-toggle-summary" {
                                                            span class="opret-toggle-caret" aria-hidden="true" { (icon_caret_right()) }
                                                            span class="opret-toggle-label" { (method.name.clone()) }
                                                            span class="trace-opcode" { (format!("opcode {}", method.opcode)) }
                                                        }
                                                        div class="opret-toggle-body" {
                                                            div class="alkane-method-result" data-sim-result="" data-status="idle" {
                                                                span class="alkane-method-label" { "Result:" }
                                                                span class="alkane-method-value muted" data-sim-value="" data-default-text="Providing inputs to simulate methods is not currently supported on espo" {
                                                                    "Providing inputs to write methods is not currently supported on Espo"
                                                                }
                                                            }
                                                            button class="alkane-method-btn" type="button" { "Simulate anyways" }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            (header_scripts())
            @if tab == AlkaneTab::Inspect {
                (inspect_scripts())
            }
        },
    )
}

fn short_hex(s: &str) -> String {
    const KEEP: usize = 6;
    if s.len() <= KEEP * 2 {
        return s.to_string();
    }
    format!("{}...{}", &s[..KEEP], &s[s.len() - KEEP..])
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

fn addr_prefix_suffix(addr: &str) -> (String, String) {
    let suffix_len = addr.len().min(ADDR_SUFFIX_LEN);
    let split_at = addr.len().saturating_sub(suffix_len);
    let prefix = addr[..split_at].to_string();
    let suffix = addr[split_at..].to_string();
    (prefix, suffix)
}

fn split_methods(
    inspection: Option<&crate::modules::essentials::utils::inspections::StoredInspectionResult>,
) -> (Vec<StoredInspectionMethod>, Vec<StoredInspectionMethod>) {
    let mut view = Vec::new();
    let mut write = Vec::new();
    if let Some(meta) = inspection.and_then(|i| i.metadata.as_ref()) {
        for method in &meta.methods {
            if method.name.starts_with("get_") {
                view.push(method.clone());
            } else {
                write.push(method.clone());
            }
        }
    }
    (view, write)
}

fn resolve_factory_contract(
    inspection: Option<&crate::modules::essentials::utils::inspections::StoredInspectionResult>,
    alkane: &crate::schemas::SchemaAlkaneId,
    meta_cache: &mut AlkaneMetaCache,
    essentials_mdb: &crate::runtime::mdb::Mdb,
) -> (String, String) {
    let factory_id = inspection.and_then(|i| i.factory_alkane).unwrap_or(*alkane);
    let factory_name = load_inspection(essentials_mdb, &factory_id)
        .ok()
        .flatten()
        .and_then(|i| i.metadata.as_ref().map(|m| m.name.trim().to_string()))
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| {
            let meta = alkane_meta(&factory_id, meta_cache, essentials_mdb);
            meta.name.value.clone()
        });
    let factory_id_str = format!("{}:{}", factory_id.block, factory_id.tx);
    (factory_name, factory_id_str)
}

fn inspect_scripts() -> Markup {
    let script = r#"
<script>
(() => {
  const root = document.querySelector('[data-alkane-inspect]');
  if (!root) return;
  const alkaneId = root.dataset.alkaneId || '';
  if (!alkaneId) return;
  const writeDefault = 'Providing inputs to simulate methods is not currently supported on espo';

  const toggles = root.querySelectorAll('[data-alkane-method]');
  const resetWrite = (details) => {
    const button = details.querySelector('.alkane-method-btn');
    if (button) {
      button.style.display = '';
    }
    const resultWrap = details.querySelector('[data-sim-result]');
    const valueNode = details.querySelector('[data-sim-value]');
    if (resultWrap && valueNode) {
      resultWrap.dataset.status = 'idle';
      valueNode.textContent = valueNode.dataset.defaultText || writeDefault;
    }
  };

  const runSim = async (details) => {
    if (!details || details.dataset.loading === '1') return;
    const opcode = details.dataset.alkaneOpcode || details.dataset.opcode;
    const returnsType = details.dataset.alkaneReturns || '';
    if (!opcode) return;
    const resultWrap = details.querySelector('[data-sim-result]');
    const valueNode = details.querySelector('[data-sim-value]');
    if (!resultWrap || !valueNode) return;

    details.dataset.loading = '1';
    resultWrap.dataset.status = 'loading';
    valueNode.textContent = 'Loading...';

    try {
      const payload = { alkane: alkaneId, opcode: Number(opcode) };
      if (returnsType) {
        payload.returns = returnsType;
      }
      const res = await fetch('/api/alkane/simulate', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify(payload)
      });
      const data = await res.json();
      if (!data || !data.ok) {
        const msg = data && data.error ? data.error : 'Simulation failed';
        resultWrap.dataset.status = 'failure';
        valueNode.textContent = msg;
      } else {
        const status = data.status || 'success';
        resultWrap.dataset.status = status;
        valueNode.textContent = data.data || 'No data';
      }
    } catch (_) {
      resultWrap.dataset.status = 'failure';
      valueNode.textContent = 'Simulation failed';
    } finally {
      details.dataset.loading = '0';
    }
  };

  toggles.forEach((details) => {
    details.addEventListener('toggle', async () => {
      if (!details.open) {
        if (details.dataset.alkaneView !== '1') {
          resetWrite(details);
        }
        return;
      }
      if (details.dataset.alkaneView !== '1') return;
      await runSim(details);
    });
  });

  const buttons = root.querySelectorAll('.alkane-method-btn');
  buttons.forEach((button) => {
    button.addEventListener('click', async (event) => {
      event.preventDefault();
      const details = button.closest('details');
      if (!details) return;
      details.open = true;
      button.style.display = 'none';
      await runSim(details);
    });
  });
})();
</script>
"#;
    PreEscaped(script.to_string())
}
