use crate::alkanes::trace::EspoBlock;
use crate::config::{get_espo_db, get_last_safe_tip};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::storage::{
    decode_creation_record, EssentialsProvider, GetIndexHeightParams, GetIterFromParams,
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

#[derive(Clone, Debug)]
pub(crate) struct SeriesEntry {
    pub series_id: String,
    pub alkane_id: SchemaAlkaneId,
    pub creation_height: u32,
}

#[derive(Clone, Debug, Default)]
pub(crate) struct SeriesIndex {
    pub series_to_alkane: HashMap<String, SeriesEntry>,
    pub alkane_to_series: HashMap<SchemaAlkaneId, SeriesEntry>,
}

pub(crate) type SharedSeriesIndex = Arc<RwLock<Arc<SeriesIndex>>>;

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
    index_height: Arc<RwLock<Option<u32>>>,
    series_index: SharedSeriesIndex,
}

impl Pizzafun {
    pub fn new() -> Self {
        Self {
            essentials_provider: None,
            index_height: Arc::new(RwLock::new(None)),
            series_index: Arc::new(RwLock::new(Arc::new(SeriesIndex::default()))),
        }
    }

    #[inline]
    fn essentials_provider(&self) -> &EssentialsProvider {
        self.essentials_provider
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    fn load_essentials_index_height(&self) -> Option<u32> {
        let resp = self
            .essentials_provider()
            .get_index_height(GetIndexHeightParams)
            .ok()?;
        resp.height
    }

    fn build_series_index(essentials_provider: &EssentialsProvider) -> SeriesIndex {
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

        let mut series_to_alkane: HashMap<String, SeriesEntry> = HashMap::new();
        let mut alkane_to_series: HashMap<SchemaAlkaneId, SeriesEntry> = HashMap::new();

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
                series_to_alkane.insert(series_id, entry.clone());
                alkane_to_series.insert(entry.alkane_id, entry);
            }
        }

        SeriesIndex { series_to_alkane, alkane_to_series }
    }

    fn reload_series_index(&self, reason: &str) {
        let essentials_provider = self.essentials_provider();
        eprintln!("[PIZZAFUN] rebuilding series index ({reason})...");
        let index = Self::build_series_index(essentials_provider);
        let entry_count = index.series_to_alkane.len();
        let mut guard = self.series_index.write().expect("series index lock poisoned");
        *guard = Arc::new(index);
        eprintln!("[PIZZAFUN] series index ready: {} entries", entry_count);
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

    fn set_mdb(&mut self, _mdb: Arc<Mdb>) {
        let essentials_mdb = Mdb::from_db(get_espo_db(), b"essentials:");
        let provider = Arc::new(EssentialsProvider::new(Arc::new(essentials_mdb)));
        self.essentials_provider = Some(provider);

        *self.index_height.write().unwrap() = self.load_essentials_index_height();
        self.reload_series_index("startup");
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        crate::modules::essentials::consts::essentials_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        let t0 = std::time::Instant::now();
        if let Some(tip) = get_last_safe_tip() {
            if block.height >= tip {
                self.reload_series_index("safe_tip");
            }
        }
        *self.index_height.write().unwrap() = Some(block.height);
        eprintln!(
            "[indexer] module={} height={} index_block done in {:?}",
            self.get_name(),
            block.height,
            t0.elapsed()
        );
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        get_last_safe_tip().or_else(|| *self.index_height.read().unwrap())
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        rpc::register_rpc(reg.clone(), Arc::clone(&self.series_index));
    }
}
