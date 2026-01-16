use crate::alkanes::trace::EspoBlock;
use crate::config::{get_espo_db, get_last_safe_tip};
use crate::modules::defs::{EspoModule, RpcNsRegistrar};
use crate::modules::essentials::main::Essentials;
use crate::modules::essentials::storage::decode_creation_record;
use crate::modules::essentials::utils::names::normalize_alkane_name;
use crate::runtime::mdb::Mdb;
use crate::schemas::SchemaAlkaneId;
use anyhow::Result;
use bitcoin::Network;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

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

pub struct Pizzafun {
    essentials_mdb: Option<Arc<Mdb>>,
    index_height: Arc<RwLock<Option<u32>>>,
    series_index: SharedSeriesIndex,
}

impl Pizzafun {
    pub fn new() -> Self {
        Self {
            essentials_mdb: None,
            index_height: Arc::new(RwLock::new(None)),
            series_index: Arc::new(RwLock::new(Arc::new(SeriesIndex::default()))),
        }
    }

    #[inline]
    fn essentials_mdb(&self) -> &Mdb {
        self.essentials_mdb
            .as_ref()
            .expect("ModuleRegistry must call set_mdb()")
            .as_ref()
    }

    fn load_essentials_index_height(&self) -> Option<u32> {
        let bytes = self
            .essentials_mdb()
            .get(Essentials::k_index_height())
            .ok()
            .flatten()?;
        if bytes.len() != 4 {
            return None;
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Some(u32::from_le_bytes(arr))
    }

    fn build_series_index(essentials_mdb: &Mdb) -> SeriesIndex {
        let mut records = Vec::new();
        let prefix = b"/alkanes/creation/id/";
        for res in essentials_mdb.iter_from(prefix) {
            let Ok((k_full, v)) = res else { continue };
            let rel = &k_full[essentials_mdb.prefix().len()..];
            if !rel.starts_with(prefix) {
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

        let mut name_counts: HashMap<String, u32> = HashMap::new();
        let mut series_to_alkane: HashMap<String, SeriesEntry> = HashMap::new();
        let mut alkane_to_series: HashMap<SchemaAlkaneId, SeriesEntry> = HashMap::new();

        for rec in records {
            let Some(raw_name) = rec.names.first() else { continue };
            let Some(name_norm) = normalize_alkane_name(raw_name) else { continue };
            let idx = name_counts.entry(name_norm.clone()).or_insert(0);
            let series_id = if *idx == 0 {
                name_norm.clone()
            } else {
                format!("{}-{}", name_norm, idx)
            };
            *idx = idx.saturating_add(1);

            let entry = SeriesEntry {
                series_id: series_id.clone(),
                alkane_id: rec.alkane,
                creation_height: rec.creation_height,
            };
            series_to_alkane.insert(series_id, entry.clone());
            alkane_to_series.insert(rec.alkane, entry);
        }

        SeriesIndex { series_to_alkane, alkane_to_series }
    }

    fn reload_series_index(&self, reason: &str) {
        let essentials_mdb = self.essentials_mdb();
        eprintln!("[PIZZAFUN] rebuilding series index ({reason})...");
        let index = Self::build_series_index(essentials_mdb);
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
        self.essentials_mdb = Some(Arc::new(essentials_mdb));

        *self.index_height.write().unwrap() = self.load_essentials_index_height();
        self.reload_series_index("startup");
    }

    fn get_genesis_block(&self, network: Network) -> u32 {
        crate::modules::essentials::consts::essentials_genesis_block(network)
    }

    fn index_block(&self, block: EspoBlock) -> Result<()> {
        if let Some(tip) = get_last_safe_tip() {
            if block.height >= tip {
                self.reload_series_index("safe_tip");
            }
        }
        *self.index_height.write().unwrap() = Some(block.height);
        Ok(())
    }

    fn get_index_height(&self) -> Option<u32> {
        get_last_safe_tip().or_else(|| *self.index_height.read().unwrap())
    }

    fn register_rpc(&self, reg: &RpcNsRegistrar) {
        rpc::register_rpc(reg.clone(), Arc::clone(&self.series_index));
    }
}
