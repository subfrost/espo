use crate::config::get_chunk_size;
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::runtime::pointers::{KvPointer, ListPointer};
use crate::runtime::tree_db::get_global_tree_db;
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::BlockHash;
use borsh::{BorshDeserialize, BorshSerialize};
use std::collections::HashSet;
use std::sync::Arc;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SeriesEntry {
    pub series_id: String,
    pub alkane_id: SchemaAlkaneId,
    pub creation_height: u32,
}

pub fn normalize_series_id(s: &str) -> Option<String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return None;
    }
    Some(trimmed.to_ascii_lowercase())
}

pub fn series_id_base_from_name(name_norm: &str) -> Option<String> {
    let trimmed = name_norm.trim();
    if trimmed.is_empty() {
        return None;
    }
    let lowered = trimmed.to_ascii_lowercase();
    Some(lowered.split_whitespace().collect::<Vec<_>>().join("-"))
}

fn series_id_matches_name(series_id: &str, name_norm: &str) -> bool {
    if series_id == name_norm {
        return true;
    }
    if let Some(rest) = series_id.strip_prefix(name_norm) {
        if let Some(num) = rest.strip_prefix('-') {
            return !num.is_empty() && num.chars().all(|c| c.is_ascii_digit());
        }
    }
    false
}

#[allow(non_snake_case)]
#[derive(Clone)]
pub struct PizzafunTable<'a> {
    pub ROOT: KvPointer<'a>,
    pub INDEX_HEIGHT: KvPointer<'a>,
    pub SERIES_BY_ID: KvPointer<'a>,
    pub SERIES_BY_ALKANE: KvPointer<'a>,
    pub SERIES_ALL: ListPointer<'a>,
}

impl<'a> PizzafunTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = KvPointer::root(mdb);
        Self {
            INDEX_HEIGHT: root.keyword("/index_height"),
            SERIES_BY_ID: root.keyword("/series/by_id/"),
            SERIES_BY_ALKANE: root.keyword("/series/by_alkane/"),
            SERIES_ALL: root.list_keyword("/series/all/v2/"),
            ROOT: root,
        }
    }

    pub fn series_by_id_key(&self, series_id: &str) -> Vec<u8> {
        self.SERIES_BY_ID.select(series_id.as_bytes()).key().to_vec()
    }

    pub fn series_by_id_prefix(&self) -> Vec<u8> {
        self.SERIES_BY_ID.key().to_vec()
    }

    pub fn series_by_alkane_key(&self, alkane: &SchemaAlkaneId) -> Vec<u8> {
        let mut suffix = Vec::with_capacity(12);
        suffix.extend_from_slice(&alkane.block.to_be_bytes());
        suffix.extend_from_slice(&alkane.tx.to_be_bytes());
        self.SERIES_BY_ALKANE.select(&suffix).key().to_vec()
    }

    pub fn series_by_alkane_prefix(&self) -> Vec<u8> {
        self.SERIES_BY_ALKANE.key().to_vec()
    }

    pub fn series_all_prefix(&self) -> Vec<u8> {
        self.SERIES_ALL.key().to_vec()
    }

    pub fn series_all_length_key(&self) -> Vec<u8> {
        let mut key = self.series_all_prefix();
        key.extend_from_slice(b"length");
        key
    }

    pub fn series_all_chunk_key(&self, chunk_id: u64) -> Vec<u8> {
        let mut key = self.series_all_prefix();
        key.extend_from_slice(b"chunk/");
        key.extend_from_slice(&chunk_id.to_be_bytes());
        key
    }
}

pub struct GetIndexHeightParams;

pub struct GetIndexHeightResult {
    pub height: Option<u32>,
}

pub struct SetIndexHeightParams {
    pub height: u32,
}

fn encode_u64_le(value: u64) -> [u8; 8] {
    value.to_le_bytes()
}

fn decode_u64_le(bytes: &[u8]) -> Option<u64> {
    if bytes.len() != 8 {
        return None;
    }
    let mut arr = [0u8; 8];
    arr.copy_from_slice(bytes);
    Some(u64::from_le_bytes(arr))
}

fn encode_series_id_chunk(chunk: &[String]) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(&chunk.to_vec())?)
}

fn decode_series_id_chunk(bytes: &[u8]) -> Result<Vec<String>> {
    Ok(Vec::<String>::try_from_slice(bytes)?)
}

#[derive(Clone)]
pub struct PizzafunProvider {
    mdb: Arc<Mdb>,
    view_blockhash: Option<BlockHash>,
}

impl PizzafunProvider {
    pub fn new(mdb: Arc<Mdb>) -> Self {
        Self { mdb, view_blockhash: None }
    }

    pub fn with_view_blockhash(&self, blockhash: Option<BlockHash>) -> Self {
        Self { mdb: Arc::clone(&self.mdb), view_blockhash: blockhash }
    }

    pub fn with_height(&self, height: Option<u64>, height_present: bool) -> Result<Self> {
        if !height_present {
            return Ok(self.with_view_blockhash(None));
        }
        let Some(height) = height else {
            return Err(anyhow!("missing_or_invalid_height"));
        };
        let height_u32 = u32::try_from(height).map_err(|_| anyhow!("height_out_of_range"))?;
        let Some(tree) = get_global_tree_db() else {
            return Err(anyhow!("versioned_tree_unavailable"));
        };
        let Some(blockhash) =
            tree.blockhash_for_height(height_u32).map_err(|e| anyhow!("tree lookup failed: {e}"))?
        else {
            return Err(anyhow!("height_not_indexed"));
        };
        Ok(self.with_view_blockhash(Some(blockhash)))
    }

    pub fn table(&self) -> PizzafunTable<'_> {
        PizzafunTable::new(self.mdb.as_ref())
    }

    pub fn mdb(&self) -> &Mdb {
        self.mdb.as_ref()
    }

    fn raw_get(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        match self.view_blockhash {
            Some(blockhash) => {
                self.mdb.get_at_blockhash(&blockhash, key).map_err(|e| anyhow!("mdb.get_at_blockhash failed: {e}"))
            }
            None => self.mdb.get(key).map_err(|e| anyhow!("mdb.get failed: {e}")),
        }
    }

    fn raw_multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        match self.view_blockhash {
            Some(_blockhash) => {
                let mut out = Vec::with_capacity(keys.len());
                for key in keys {
                    out.push(self.raw_get(key)?);
                }
                Ok(out)
            }
            None => self.mdb.multi_get(keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}")),
        }
    }

    fn read_u64_len(&self, key: &[u8]) -> Result<u64> {
        Ok(self.raw_get(key)?.and_then(|v| decode_u64_le(&v)).unwrap_or(0))
    }

    fn read_series_ids_all(&self) -> Result<Vec<String>> {
        let table = self.table();
        let len = self.read_u64_len(&table.series_all_length_key())? as usize;
        if len == 0 {
            return Ok(Vec::new());
        }
        let chunk_size = get_chunk_size() as usize;
        let chunk_count = len.div_ceil(chunk_size);
        let mut keys = Vec::with_capacity(chunk_count);
        for chunk_id in 0..chunk_count {
            keys.push(table.series_all_chunk_key(chunk_id as u64));
        }
        let values = self.raw_multi_get(&keys)?;
        let mut out = Vec::with_capacity(len);
        for raw in values {
            let Some(bytes) = raw else { continue };
            out.extend(decode_series_id_chunk(&bytes)?);
        }
        if out.len() > len {
            out.truncate(len);
        }
        Ok(out)
    }

    fn build_series_id_list_rewrite(
        &self,
        ids: &[String],
    ) -> Result<(Vec<(Vec<u8>, Vec<u8>)>, Vec<Vec<u8>>)> {
        let table = self.table();
        let chunk_size = get_chunk_size() as usize;
        let old_len = self
            .mdb
            .get(&table.series_all_length_key())
            .map_err(|e| anyhow!("mdb.get failed: {e}"))?
            .and_then(|v| decode_u64_le(&v))
            .unwrap_or(0) as usize;

        let old_chunk_count = if old_len == 0 { 0 } else { old_len.div_ceil(chunk_size) };
        let new_chunk_count = if ids.is_empty() { 0 } else { ids.len().div_ceil(chunk_size) };

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
        for chunk_id in 0..new_chunk_count {
            let start = chunk_id * chunk_size;
            let end = (start + chunk_size).min(ids.len());
            puts.push((
                table.series_all_chunk_key(chunk_id as u64),
                encode_series_id_chunk(&ids[start..end])?,
            ));
        }
        puts.push((table.series_all_length_key(), encode_u64_le(ids.len() as u64).to_vec()));

        let mut deletes = Vec::new();
        for chunk_id in new_chunk_count..old_chunk_count {
            deletes.push(table.series_all_chunk_key(chunk_id as u64));
        }

        Ok((puts, deletes))
    }

    pub fn get_index_height(&self, _params: GetIndexHeightParams) -> Result<GetIndexHeightResult> {
        crate::debug_timer_log!("pizzafun.get_index_height");
        let table = self.table();
        let Some(bytes) = self.raw_get(table.INDEX_HEIGHT.key())? else {
            return Ok(GetIndexHeightResult { height: None });
        };
        if bytes.len() != 4 {
            return Err(anyhow!("[PIZZAFUN] invalid /index_height length {}", bytes.len()));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(GetIndexHeightResult { height: Some(u32::from_le_bytes(arr)) })
    }

    pub fn set_index_height(&self, params: SetIndexHeightParams) -> Result<()> {
        crate::debug_timer_log!("pizzafun.set_index_height");
        let table = self.table();
        table.INDEX_HEIGHT.put(&params.height.to_le_bytes())
    }

    pub fn get_series_by_id(&self, series_id: &str) -> Result<Option<SeriesEntry>> {
        let table = self.table();
        let key = table.series_by_id_key(series_id);
        let Some(bytes) = self.raw_get(&key)? else {
            return Ok(None);
        };
        Ok(Some(SeriesEntry::try_from_slice(&bytes)?))
    }

    pub fn get_series_by_ids(&self, series_ids: &[String]) -> Result<Vec<Option<SeriesEntry>>> {
        let table = self.table();
        let keys: Vec<Vec<u8>> = series_ids.iter().map(|s| table.series_by_id_key(s)).collect();
        let raw = self.raw_multi_get(&keys)?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            match item {
                Some(bytes) => out.push(Some(SeriesEntry::try_from_slice(&bytes)?)),
                None => out.push(None),
            }
        }
        Ok(out)
    }

    pub fn get_series_by_alkane(&self, alkane: &SchemaAlkaneId) -> Result<Option<SeriesEntry>> {
        let table = self.table();
        let key = table.series_by_alkane_key(alkane);
        let Some(bytes) = self.raw_get(&key)? else {
            return Ok(None);
        };
        Ok(Some(SeriesEntry::try_from_slice(&bytes)?))
    }

    pub fn get_series_by_alkanes(
        &self,
        alkanes: &[SchemaAlkaneId],
    ) -> Result<Vec<Option<SeriesEntry>>> {
        let table = self.table();
        let keys: Vec<Vec<u8>> = alkanes.iter().map(|a| table.series_by_alkane_key(a)).collect();
        let raw = self.raw_multi_get(&keys)?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            match item {
                Some(bytes) => out.push(Some(SeriesEntry::try_from_slice(&bytes)?)),
                None => out.push(None),
            }
        }
        Ok(out)
    }

    pub fn get_series_entries_by_name(&self, name_norm: &str) -> Result<Vec<SeriesEntry>> {
        let table = self.table();
        let mut lookup_names: Vec<String> = vec![name_norm.to_string()];
        if let Some(series_base) = series_id_base_from_name(name_norm) {
            if series_base != name_norm {
                lookup_names.push(series_base);
            }
        }

        let all_ids = self.read_series_ids_all()?;
        let mut filtered_ids: Vec<String> = Vec::new();
        let mut seen_ids: HashSet<String> = HashSet::new();
        for series_id in all_ids {
            if lookup_names.iter().any(|name| series_id_matches_name(&series_id, name))
                && seen_ids.insert(series_id.clone())
            {
                filtered_ids.push(series_id);
            }
        }

        if filtered_ids.is_empty() {
            return Ok(Vec::new());
        }

        let keys: Vec<Vec<u8>> = filtered_ids.iter().map(|id| table.series_by_id_key(id)).collect();
        let raw = self.raw_multi_get(&keys)?;
        let mut out = Vec::with_capacity(raw.len());
        for item in raw {
            if let Some(bytes) = item {
                out.push(SeriesEntry::try_from_slice(&bytes)?);
            }
        }
        Ok(out)
    }

    pub fn update_series_for_name(
        &self,
        existing: &[SeriesEntry],
        updated: &[SeriesEntry],
    ) -> Result<()> {
        let table = self.table();
        let mut series_ids = self.read_series_ids_all()?;
        let existing_ids: HashSet<String> = existing.iter().map(|e| e.series_id.clone()).collect();
        series_ids.retain(|id| !existing_ids.contains(id));
        let mut seen_ids: HashSet<String> = series_ids.iter().cloned().collect();
        for entry in updated {
            if seen_ids.insert(entry.series_id.clone()) {
                series_ids.push(entry.series_id.clone());
            }
        }
        let (list_puts, list_deletes) = self.build_series_id_list_rewrite(&series_ids)?;

        let mut deletes: Vec<Vec<u8>> = Vec::with_capacity(existing.len() * 2);
        for entry in existing {
            deletes.push(table.series_by_id_key(&entry.series_id));
            deletes.push(table.series_by_alkane_key(&entry.alkane_id));
        }
        deletes.extend(list_deletes);

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(updated.len() * 2 + list_puts.len());
        for entry in updated {
            let encoded = borsh::to_vec(entry)?;
            puts.push((table.series_by_id_key(&entry.series_id), encoded.clone()));
            puts.push((table.series_by_alkane_key(&entry.alkane_id), encoded));
        }
        puts.extend(list_puts);

        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &deletes {
                    wb.delete(key);
                }
                for (key, value) in &puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }

    pub fn replace_series_entries(&self, entries: &[SeriesEntry], height: u32) -> Result<()> {
        let table = self.table();
        let existing_ids = self.read_series_ids_all()?;
        let mut deletes: Vec<Vec<u8>> = Vec::new();
        if !existing_ids.is_empty() {
            let existing_rows = self.get_series_by_ids(&existing_ids)?;
            for (idx, maybe_entry) in existing_rows.into_iter().enumerate() {
                let Some(entry) = maybe_entry else { continue };
                if let Some(series_id) = existing_ids.get(idx) {
                    deletes.push(table.series_by_id_key(series_id));
                } else {
                    deletes.push(table.series_by_id_key(&entry.series_id));
                }
                deletes.push(table.series_by_alkane_key(&entry.alkane_id));
            }
        }

        let new_ids: Vec<String> = entries.iter().map(|e| e.series_id.clone()).collect();
        let (list_puts, list_deletes) = self.build_series_id_list_rewrite(&new_ids)?;
        deletes.extend(list_deletes);

        let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::with_capacity(entries.len() * 2 + list_puts.len());
        for entry in entries {
            let encoded = borsh::to_vec(entry)?;
            puts.push((table.series_by_id_key(&entry.series_id), encoded.clone()));
            puts.push((table.series_by_alkane_key(&entry.alkane_id), encoded));
        }
        puts.extend(list_puts);

        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &deletes {
                    wb.delete(key);
                }
                for (key, value) in &puts {
                    wb.put(key, value);
                }
                wb.put(table.INDEX_HEIGHT.key(), &height.to_le_bytes());
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }
}

#[cfg(test)]
mod tests {
    use super::{normalize_series_id, series_id_base_from_name, series_id_matches_name};

    #[test]
    fn normalize_series_id_preserves_spacing_for_backcompat() {
        assert_eq!(normalize_series_id("  Love Bomb  ").as_deref(), Some("love bomb"));
        assert_eq!(normalize_series_id("   "), None);
    }

    #[test]
    fn series_id_base_from_name_matches_expected_slug() {
        assert_eq!(series_id_base_from_name("love bomb").as_deref(), Some("love-bomb"));
    }

    #[test]
    fn series_id_matching_supports_base_and_numbered_suffix() {
        assert!(series_id_matches_name("love-bomb", "love-bomb"));
        assert!(series_id_matches_name("love-bomb-2", "love-bomb"));
        assert!(!series_id_matches_name("love-bomb-two", "love-bomb"));
        assert!(!series_id_matches_name("love-bomb-2a", "love-bomb"));
    }
}
