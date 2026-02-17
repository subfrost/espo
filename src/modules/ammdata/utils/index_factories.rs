use crate::runtime::state_at::StateAt;
use crate::alkanes::trace::{EspoBlock, EspoSandshrewLikeTraceEvent};
use crate::modules::ammdata::storage::{AmmDataProvider, GetAmmFactoriesParams};
use crate::modules::essentials::storage::{
    EssentialsProvider, GetCreationRecordParams, GetCreationRecordsOrderedPageParams,
};
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};

pub fn prepare_factories(
    block: &EspoBlock,
    provider: &AmmDataProvider,
    essentials: &EssentialsProvider,
    bootstrap_essentials: &EssentialsProvider,
    creation_count: u64,
    factory_bootstrap_creation_count: &AtomicU64,
) -> Result<(HashSet<SchemaAlkaneId>, Vec<(Vec<u8>, Vec<u8>)>)> {
    let table = provider.table();
    let mut amm_factories: HashSet<SchemaAlkaneId> = provider
        .get_amm_factories(GetAmmFactoriesParams {
            blockhash: StateAt::Block(block.block_header.block_hash()),
        })
        .map(|res| res.factories.into_iter().collect())
        .unwrap_or_default();
    let mut amm_factory_writes: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();

    let last_bootstrap = factory_bootstrap_creation_count.load(Ordering::Relaxed);
    if creation_count > last_bootstrap {
        let mut offset = last_bootstrap;
        let mut scanned = 0usize;
        let mut discovered = 0usize;
        let mut inferred = 0usize;
        let mut complete = true;
        while offset < creation_count {
            let remaining = creation_count.saturating_sub(offset);
            let limit = remaining.min(2048);
            let page = match bootstrap_essentials.get_creation_records_ordered_page(
                GetCreationRecordsOrderedPageParams {
            blockhash: StateAt::Latest, offset, limit, desc: false },
            ) {
                Ok(page) => page,
                Err(_) => {
                    complete = false;
                    break;
                }
            };
            if page.records.is_empty() {
                complete = false;
                break;
            }
            let page_len = page.records.len() as u64;
            for rec in page.records {
                scanned = scanned.saturating_add(1);
                if amm_factories.contains(&rec.alkane) {
                    continue;
                }
                let mut is_factory = rec
                    .inspection
                    .as_ref()
                    .map(crate::modules::ammdata::inspection_is_amm_factory)
                    .unwrap_or(false);
                if !is_factory {
                    if let Some(proxy_target) = crate::modules::ammdata::lookup_proxy_target(
                        bootstrap_essentials,
                        rec.alkane,
                    ) {
                        if let Ok(resp) = bootstrap_essentials
                            .get_creation_record(GetCreationRecordParams {
            blockhash: StateAt::Latest, alkane: proxy_target })
                        {
                            if let Some(rec) = resp.record {
                                if let Some(inspection) = rec.inspection.as_ref() {
                                    if crate::modules::ammdata::inspection_is_amm_factory(
                                        inspection,
                                    ) {
                                        is_factory = true;
                                    }
                                }
                            }
                        }
                    }
                }
                if is_factory {
                    amm_factories.insert(rec.alkane);
                    amm_factory_writes.push((table.amm_factory_key(&rec.alkane), Vec::new()));
                    discovered += 1;
                }

                if let Some(factory_id) =
                    rec.inspection.as_ref().and_then(|inspection| inspection.factory_alkane)
                {
                    if !amm_factories.contains(&factory_id) {
                        amm_factories.insert(factory_id);
                        amm_factory_writes.push((table.amm_factory_key(&factory_id), Vec::new()));
                        inferred += 1;
                    }
                }
            }
            offset = offset.saturating_add(page_len);
        }
        eprintln!(
            "[AMMDATA] factory bootstrap scanned {} creation records, discovered {} factories, inferred {} from LP records",
            scanned, discovered, inferred
        );
        if complete && offset >= creation_count {
            factory_bootstrap_creation_count.store(creation_count, Ordering::Relaxed);
        }
    }

    let mut created_alkanes: Vec<SchemaAlkaneId> = Vec::new();
    for tx in &block.transactions {
        if let Some(traces) = &tx.traces {
            for trace in traces {
                for ev in &trace.sandshrew_trace.events {
                    if let EspoSandshrewLikeTraceEvent::Create(c) = ev {
                        if let (Some(block), Some(tx)) = (
                            crate::modules::ammdata::parse_hex_u32(&c.block),
                            crate::modules::ammdata::parse_hex_u64(&c.tx),
                        ) {
                            created_alkanes.push(SchemaAlkaneId { block, tx });
                        }
                    }
                }
            }
        }
    }

    for alk in created_alkanes {
        if amm_factories.contains(&alk) {
            continue;
        }
        let mut is_factory = false;
        if let Ok(resp) = essentials.get_creation_record(GetCreationRecordParams {
            blockhash: StateAt::Latest, alkane: alk }) {
            if let Some(rec) = resp.record {
                if let Some(inspection) = rec.inspection.as_ref() {
                    if crate::modules::ammdata::inspection_is_amm_factory(inspection) {
                        is_factory = true;
                    }
                }
            }
        }
        if !is_factory {
            if let Some(proxy_target) =
                crate::modules::ammdata::lookup_proxy_target(essentials, alk)
            {
                if let Ok(resp) =
                    essentials.get_creation_record(GetCreationRecordParams {
            blockhash: StateAt::Latest, alkane: proxy_target })
                {
                    if let Some(rec) = resp.record {
                        if let Some(inspection) = rec.inspection.as_ref() {
                            if crate::modules::ammdata::inspection_is_amm_factory(inspection) {
                                is_factory = true;
                            }
                        }
                    }
                }
            }
        }
        if is_factory {
            amm_factories.insert(alk);
            amm_factory_writes.push((table.amm_factory_key(&alk), Vec::new()));
        }
    }

    Ok((amm_factories, amm_factory_writes))
}
