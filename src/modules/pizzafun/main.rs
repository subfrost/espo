use crate::alkanes::trace::EspoBlock;
use crate::config::{debug_enabled, get_espo_db, get_last_safe_tip};
use crate::debug;
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    decode_creation_record, EssentialsProvider, GetIndexHeightParams as EssentialsGetIndexHeightParams,
    GetIterFromParams,
};
use crate::modules::essentials::utils::names::normalize_alkane_name;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

use super::consts::PRIORITY_SERIES_ALKANES;
use super::rpc;
use super::storage::{
    GetIndexHeightParams as PizzafunGetIndexHeightParams, PizzafunProvider, SeriesEntry,
};

fn parse_alkane_id_str(s: &str) -> Option<SchemaAlkaneId> {
    let (block_raw, tx_raw) = s.split_once(':')?;
    let parse_u32 = |v: &str| {
        if let Some(hex) = v.strip_prefix("0x") {
            u32::from_str_radix(hex, 16).ok()
        } else {
            v.parse::<u32>().ok()
        }
    };
    let parse_u64 = |v: &str| {
        if let Some(hex) = v.strip_prefix("0x") {
            u64::from_str_radix(hex, 16).ok()
        } else {
            v.parse::<u64>().ok()
        }
    };
    Some(SchemaAlkaneId { block: parse_u32(block_raw)?, tx: parse_u64(tx_raw)? })
}

pub struct Pizzafun {
    essentials_provider: Option<Arc<EssentialsProvider>>,
    provider: Option<Arc<PizzafunProvider>>,
    index_height: Arc<RwLock<Option<u32>>>,
}

impl Pizzafun {
    pub fn new() -> Self {
        Self {
            essentials_provider: None,
            provider: None,
            index_height: Arc::new(RwLock::new(None)),
        }
    }

    #[inline]
    fn essentials_provider(&self) -> &EssentialsProvider {
        self.essentials_provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    #[inline]
    fn provider(&self) -> &PizzafunProvider {
        self.provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    fn load_essentials_index_height(&self) -> Option<u32> {
        let resp = self
            .essentials_provider()
            .get_index_height(EssentialsGetIndexHeightParams)
            .ok()?;
        resp.height
    }

    fn load_index_height(&self) -> Option<u32> {
        if let Ok(resp) = self.provider().get_index_height(PizzafunGetIndexHeightParams) {
            if resp.height.is_some() {
                return resp.height;
            }
        }
        self.load_essentials_index_height()
    }

    fn build_series_entries(essentials_provider: &EssentialsProvider) -> Vec<SeriesEntry> {
        let mut records = Vec::new();
        let prefix = b"/alkanes/creation/id/".to_vec();
        let entries = match essentials_provider.get_iter_from(GetIterFromParams { start: prefix.clone() }) {
            Ok(resp) => resp.entries,
            Err(e) => {
                eprintln!("[PIZZAFUN] iter_from failed: {e}");
                Vec::new()
            }
        };
        for (rel, v) in entries {
            if !rel.starts_with(&prefix) {
                break;
            }
            match decode_creation_record(&v) {
                Ok(rec) => records.push(rec),
                Err(e) => {
                    eprintln!("[PIZZAFUN] decode creation record failed: {e}");
                }
            }
        }

        records.sort_by(|a, b| {
            (
                a.creation_height,
                a.tx_index_in_block,
                a.alkane.block,
                a.alkane.tx,
            )
                .cmp(&(
                    b.creation_height,
                    b.tx_index_in_block,
                    b.alkane.block,
                    b.alkane.tx,
                ))
        });

        struct PendingSeriesEntry {
            alkane_id: SchemaAlkaneId,
            creation_height: u32,
            order_idx: usize,
        }

        let mut priority_index: HashMap<SchemaAlkaneId, usize> = HashMap::new();
        for (idx, raw) in PRIORITY_SERIES_ALKANES.iter().enumerate() {
            if let Some(id) = parse_alkane_id_str(raw) {
                priority_index.entry(id).or_insert(idx);
            }
        }

        let mut entries_by_name: HashMap<String, Vec<PendingSeriesEntry>> = HashMap::new();
        for (order_idx, rec) in records.into_iter().enumerate() {
            let Some(raw_name) = rec.names.first() else { continue };
            let Some(name_norm) = normalize_alkane_name(raw_name) else { continue };
            entries_by_name
                .entry(name_norm)
                .or_default()
                .push(PendingSeriesEntry {
                    alkane_id: rec.alkane,
                    creation_height: rec.creation_height,
                    order_idx,
                });
        }

        let mut out: Vec<SeriesEntry> = Vec::new();

        for (name, mut entries) in entries_by_name {
            entries.sort_by(|a, b| {
                let a_pri = priority_index.get(&a.alkane_id);
                let b_pri = priority_index.get(&b.alkane_id);
                match (a_pri, b_pri) {
                    (Some(ai), Some(bi)) => ai.cmp(bi).then_with(|| a.order_idx.cmp(&b.order_idx)),
                    (Some(_), None) => Ordering::Less,
                    (None, Some(_)) => Ordering::Greater,
                    (None, None) => a.order_idx.cmp(&b.order_idx),
                }
            });

            for (idx, entry) in entries.into_iter().enumerate() {
                let series_id = if idx == 0 {
                    name.clone()
                } else {
                    format!("{}-{}", name, idx)
                };
                let entry = SeriesEntry {
                    series_id: series_id.clone(),
                    alkane_id: entry.alkane_id,
                    creation_height: entry.creation_height,
                };
                out.push(entry);
            }
        }

        out
    }

    fn rebuild_series_index(&self, reason: &str, height: u32) -> Result<()> {
        let essentials_provider = self.essentials_provider();
        eprintln!("[PIZZAFUN] rebuilding series index ({reason})...");
        let entries = Self::build_series_entries(essentials_provider);
        let entry_count = entries.len();
        self.provider().replace_series_entries(&entries, height)?;
        *self.index_height.write().expect("pizzafun index height lock poisoned") = Some(height);
        eprintln!("[PIZZAFUN] series index ready: {} entries", entry_count);
        Ok(())
    }
}

impl Default for Pizzafun {
    fn default() -> Self {
        Self::new()
    }
}

impl EspoModule for Pizzafun {
    fn get_name(&self) -> &'static str {
        "pizzafun"
    }

    fn set_mdb(&mut self, mdb: Arc<Mdb>) {
        let essentials_mdb = Mdb::from_db(get_espo_db(), b"essentials:");
        let essentials_provider = Arc::new(EssentialsProvider::new(Arc::new(essentials_mdb)));
        self.essentials_provider = Some(essentials_provider);
        self.provider = Some(Arc::new(PizzafunProvider::new(mdb)));

        *self.index_height.write().unwrap() = self.load_index_height();
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        crate::modules::essentials::consts::essentials_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        let debug = debug_enabled();
        let module = self.get_name();

        let timer = debug::start_if(debug);
        let tip = get_last_safe_tip();
        debug::log_elapsed(module, "read_safe_tip", timer);

        let timer = debug::start_if(debug);
        let last_indexed = *self.index_height.read().unwrap();
        let should_reload = match tip {
            Some(tip) => block.height >= tip && last_indexed.map_or(true, |h| h < tip),
            None => false,
        };
        debug::log_elapsed(module, "evaluate_reload", timer);

        let timer = debug::start_if(debug);
        if should_reload {
            let target = tip.unwrap_or(block.height);
            self.rebuild_series_index("safe_tip", target)?;
        }
        debug::log_elapsed(module, "reload_series", timer);

        let timer = debug::start_if(debug);
        self.provider().set_index_height(super::storage::SetIndexHeightParams {
            height: block.height,
        })?;
        *self.index_height.write().expect("pizzafun index height lock poisoned") =
            Some(block.height);
        debug::log_elapsed(module, "store_height", timer);

        let timer = debug::start_if(debug);
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            block.height,
            t0.elapsed()
        );
        debug::log_elapsed(module, "finalize", timer);
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        *self.index_height.read().unwrap()
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        let provider = self.provider.as_ref().expect("ModuleRegistry must call set_mdb()");
        rpc::register_rpc(reg.clone(), Arc::clone(provider));
    }
}
