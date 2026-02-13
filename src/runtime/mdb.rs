use bitcoin::BlockHash;
use rocksdb::{
    BlockBasedOptions, Cache, DB, Direction, Error as RocksError, IteratorMode, Options,
    ReadOptions, WriteBatch,
};
use std::collections::{HashMap, HashSet};
use std::{path::Path, sync::Arc};

use crate::runtime::aof::AofManager;
use crate::runtime::tree_db::{get_global_tree_db, is_tree_internal_key};

/// ===== Cache / open-time tuning =====
/// How big you want the LRU block cache (data + index/filter when enabled).
pub const ROCKS_BLOCK_CACHE_BYTES: usize = 1 << 30; // 1 GiB

/// Warm the block cache for this namespace on open (iterate all keys once).
pub const WARM_CACHE_ON_OPEN: bool = true;

/// Bloom filter bits/key (helps point lookups).
pub const BLOOM_BITS_PER_KEY: f64 = 10.0;

#[derive(Clone)]
pub struct Mdb {
    db: Arc<DB>,
    prefix: Vec<u8>,
    namespace_label: String,
    aof: Option<Arc<AofManager>>,
    versioned: bool,
}

impl Mdb {
    fn should_enable_versioned_namespace(prefix: &[u8]) -> bool {
        matches!(prefix, b"essentials:" | b"ammdata:" | b"subfrost:" | b"pizzafun:" | b"oylapi:")
    }

    fn from_parts(
        db: Arc<DB>,
        prefix: impl AsRef<[u8]>,
        aof: Option<Arc<AofManager>>,
        label: Option<String>,
        versioned: bool,
    ) -> Self {
        let prefix_vec = prefix.as_ref().to_vec();
        let namespace_label = label.unwrap_or_else(|| {
            String::from_utf8(prefix_vec.clone()).unwrap_or_else(|_| hex::encode(&prefix_vec))
        });
        Self { db, prefix: prefix_vec, namespace_label, aof, versioned }
    }

    pub fn from_db(db: Arc<DB>, prefix: impl AsRef<[u8]>) -> Self {
        // Back-compat constructor (no custom options)
        let p = prefix.as_ref().to_vec();
        let versioned = Self::should_enable_versioned_namespace(&p);
        Self::from_parts(db, p, None, None, versioned)
    }

    pub fn from_db_with_aof(
        db: Arc<DB>,
        prefix: impl AsRef<[u8]>,
        aof: Option<Arc<AofManager>>,
        label: Option<String>,
    ) -> Self {
        Self::from_parts(db, prefix, aof, label, true)
    }

    pub fn open(path: impl AsRef<Path>, prefix: impl AsRef<[u8]>) -> Result<Self, RocksError> {
        // ---- Block cache + table options ----
        let cache = Cache::new_lru_cache(ROCKS_BLOCK_CACHE_BYTES);

        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        // Put index + filter in the cache (hot metadata)
        table.set_cache_index_and_filter_blocks(true);
        // Pin L0 index/filter in cache (fastest for recent data)
        table.set_pin_l0_filter_and_index_blocks_in_cache(true);
        // Bloom filter (not whole-key)
        table.set_bloom_filter(BLOOM_BITS_PER_KEY, false);

        let mut opts = Options::default();
        opts.create_if_missing(true);
        // Keep readers open (avoid fd thrash)
        opts.set_max_open_files(-1);
        opts.set_block_based_table_factory(&table);

        let db = DB::open(&opts, path)?;

        let p = prefix.as_ref().to_vec();
        let versioned = Self::should_enable_versioned_namespace(&p);
        let mdb = Self::from_parts(Arc::new(db), p, None, None, versioned);
        if WARM_CACHE_ON_OPEN {
            let _ = mdb.warm_up_namespace(); // best-effort
        }
        Ok(mdb)
    }

    pub fn open_read_only(
        path: impl AsRef<Path>,
        prefix: impl AsRef<[u8]>,
        error_if_log_file_exist: bool,
    ) -> Result<Self, RocksError> {
        let cache = Cache::new_lru_cache(ROCKS_BLOCK_CACHE_BYTES);

        let mut table = BlockBasedOptions::default();
        table.set_block_cache(&cache);
        table.set_cache_index_and_filter_blocks(true);
        table.set_pin_l0_filter_and_index_blocks_in_cache(true);
        table.set_bloom_filter(BLOOM_BITS_PER_KEY, false);

        let mut opts = Options::default();
        opts.set_block_based_table_factory(&table);

        let db = DB::open_for_read_only(&opts, path, error_if_log_file_exist)?;
        let p = prefix.as_ref().to_vec();
        let versioned = Self::should_enable_versioned_namespace(&p);
        let mdb = Self::from_parts(Arc::new(db), p, None, None, versioned);
        if WARM_CACHE_ON_OPEN {
            let _ = mdb.warm_up_namespace();
        }
        Ok(mdb)
    }

    /// Walk the namespace once to populate the block cache.
    /// Returns the number of KV pairs touched.
    pub fn warm_up_namespace(&self) -> Result<usize, RocksError> {
        if self.versioned_manager().is_some() {
            return Ok(0);
        }
        let ns = self.prefix.clone();

        let mut ro = ReadOptions::default();
        ro.fill_cache(true); // populate block cache on read

        // Start at the namespace prefix and scan forward until it stops matching.
        let it = self.db.iterator_opt(IteratorMode::From(&ns, Direction::Forward), ro);

        let mut count = 0usize;
        for res in it {
            let (k, _v) = res?;
            if !k.starts_with(&ns) {
                break;
            }
            count += 1;
        }
        Ok(count)
    }

    #[inline]
    pub fn prefixed(&self, k: &[u8]) -> Vec<u8> {
        let mut out = Vec::with_capacity(self.prefix.len() + k.len());
        out.extend_from_slice(&self.prefix);
        out.extend_from_slice(k);
        out
    }

    pub fn get(&self, k: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let full = self.prefixed(k);
        if let Some(tree) = self.versioned_manager() {
            if is_tree_internal_key(&full) {
                return self.db.get(full);
            }
            return tree.get(&full);
        }
        self.db.get(full)
    }

    pub fn get_at_blockhash(
        &self,
        block_hash: &BlockHash,
        k: &[u8],
    ) -> Result<Option<Vec<u8>>, RocksError> {
        let full = self.prefixed(k);
        if let Some(tree) = self.versioned_manager() {
            if let Some(root) = tree.root_for_blockhash(block_hash)? {
                return tree.get_at_root(root, &full);
            }
            return Ok(None);
        }
        self.db.get(full)
    }

    pub fn scan_prefix_entries(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let ns_prefix = self.prefixed(prefix);
        if let Some(tree) = self.versioned_manager() {
            let entries = tree.collect_prefixed_entries(&ns_prefix)?;
            let mut out = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                if key.starts_with(&self.prefix) {
                    out.push((key[self.prefix.len()..].to_vec(), value));
                }
            }
            return Ok(out);
        }

        let mut out = Vec::new();
        for res in self.db.iterator(IteratorMode::From(&ns_prefix, Direction::Forward)) {
            let (key, value) = res?;
            if !key.starts_with(&ns_prefix) {
                break;
            }
            if key.starts_with(&self.prefix) {
                out.push((key[self.prefix.len()..].to_vec(), value.to_vec()));
            }
        }
        Ok(out)
    }

    pub fn scan_prefix_entries_at_blockhash(
        &self,
        block_hash: &BlockHash,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let ns_prefix = self.prefixed(prefix);
        if let Some(tree) = self.versioned_manager() {
            let Some(root) = tree.root_for_blockhash(block_hash)? else {
                return Ok(Vec::new());
            };
            let entries = tree.collect_prefixed_entries_at_root(root, &ns_prefix)?;
            let mut out = Vec::with_capacity(entries.len());
            for (key, value) in entries {
                if key.starts_with(&self.prefix) {
                    out.push((key[self.prefix.len()..].to_vec(), value));
                }
            }
            return Ok(out);
        }
        self.scan_prefix_entries(prefix)
    }

    pub fn scan_prefix_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, RocksError> {
        let ns_prefix = self.prefixed(prefix);
        if let Some(tree) = self.versioned_manager() {
            let keys = tree.collect_prefixed_keys(&ns_prefix)?;
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                if key.starts_with(&self.prefix) {
                    out.push(key[self.prefix.len()..].to_vec());
                }
            }
            return Ok(out);
        }

        let mut out = Vec::new();
        for res in self.db.iterator(IteratorMode::From(&ns_prefix, Direction::Forward)) {
            let (key, _value) = res?;
            if !key.starts_with(&ns_prefix) {
                break;
            }
            if key.starts_with(&self.prefix) {
                out.push(key[self.prefix.len()..].to_vec());
            }
        }
        Ok(out)
    }

    pub fn scan_prefix_keys_at_blockhash(
        &self,
        block_hash: &BlockHash,
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksError> {
        let ns_prefix = self.prefixed(prefix);
        if let Some(tree) = self.versioned_manager() {
            let Some(root) = tree.root_for_blockhash(block_hash)? else {
                return Ok(Vec::new());
            };
            let keys = tree.collect_prefixed_keys_at_root(root, &ns_prefix)?;
            let mut out = Vec::with_capacity(keys.len());
            for key in keys {
                if key.starts_with(&self.prefix) {
                    out.push(key[self.prefix.len()..].to_vec());
                }
            }
            return Ok(out);
        }
        self.scan_prefix_keys(prefix)
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, RocksError> {
        if let Some(tree) = self.versioned_manager() {
            let prefixed: Vec<Vec<u8>> = keys.iter().map(|k| self.prefixed(k)).collect();
            return tree.multi_get(&prefixed);
        }
        // Apply DB prefix to each RELATIVE key
        let prefixed: Vec<Vec<u8>> = keys.iter().map(|k| self.prefixed(k)).collect();

        // rocksdb::DB::multi_get returns Vec<Result<Option<DBPinnableSlice>, Error>>
        let results = self.db.multi_get(prefixed);

        // Map to Result<Vec<Option<Vec<u8>>>, Error>, preserving order
        let mut out = Vec::with_capacity(results.len());
        for r in results {
            match r {
                Ok(Some(slice)) => out.push(Some(slice.to_vec())),
                Ok(None) => out.push(None),
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    pub fn put(&self, k: &[u8], v: &[u8]) -> Result<(), RocksError> {
        let prefixed = self.prefixed(k);
        if let Some(tree) = self.versioned_manager() {
            if is_tree_internal_key(&prefixed) {
                return self.db.put(prefixed, v);
            }
            return tree.put(&prefixed, v);
        }
        let prev = if self.aof.is_some() { self.db.get(&prefixed)? } else { None };
        self.db.put(&prefixed, v)?;
        if let Some(aof) = &self.aof {
            aof.record_put(&self.namespace_label, prefixed, prev.map(|p| p.to_vec()), v.to_vec());
        }
        Ok(())
    }

    pub fn delete(&self, k: &[u8]) -> Result<(), RocksError> {
        let prefixed = self.prefixed(k);
        if let Some(tree) = self.versioned_manager() {
            if is_tree_internal_key(&prefixed) {
                return self.db.delete(prefixed);
            }
            return tree.delete(&prefixed);
        }
        let prev = if self.aof.is_some() { self.db.get(&prefixed)? } else { None };
        self.db.delete(&prefixed)?;
        if let Some(aof) = &self.aof {
            aof.record_delete(&self.namespace_label, prefixed, prev.map(|p| p.to_vec()));
        }
        Ok(())
    }

    pub fn bulk_write<F>(&self, build: F) -> Result<(), RocksError>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        if let Some(tree) = self.versioned_manager() {
            let mut versioned_changes: Vec<VersionedChange> = Vec::new();
            {
                let mut mb = MdbBatch {
                    mdb: self,
                    wb: None,
                    pending_ops: None,
                    versioned_changes: Some(&mut versioned_changes),
                };
                build(&mut mb);
            }
            let ops: Vec<(Vec<u8>, Option<Vec<u8>>)> =
                versioned_changes.into_iter().map(|c| (c.key, c.value)).collect();
            return tree.apply_batch(&ops);
        }

        let mut wb = WriteBatch::default();
        let mut pending_ops: Vec<PendingChange> = Vec::new();
        {
            let mut mb = MdbBatch {
                mdb: self,
                wb: Some(&mut wb),
                pending_ops: self.aof.as_ref().map(|_| &mut pending_ops),
                versioned_changes: None,
            };
            build(&mut mb);
        }
        let prev_map = if self.aof.is_some() && !pending_ops.is_empty() {
            self.load_previous_values(&pending_ops)?
        } else {
            HashMap::new()
        };

        self.db.write(wb)?;

        if let Some(aof) = &self.aof {
            for op in pending_ops {
                let prev = prev_map.get(&op.key).cloned().unwrap_or(None);
                match op.value {
                    Some(v) => aof.record_put(&self.namespace_label, op.key, prev, v),
                    None => aof.record_delete(&self.namespace_label, op.key, prev),
                }
            }
        }
        Ok(())
    }

    /// Iterate forward over raw DB starting from namespaced key `start` (inclusive).
    pub fn iter_from(
        &self,
        start: &[u8],
    ) -> Box<dyn Iterator<Item = Result<(Vec<u8>, Vec<u8>), RocksError>> + '_> {
        if let Some(tree) = self.versioned_manager() {
            let start_full = self.prefixed(start);
            let mut entries =
                tree.collect_prefixed_entries(self.prefix()).unwrap_or_else(|_| Vec::new());
            entries.retain(|(k, _)| k >= &start_full);
            return Box::new(entries.into_iter().map(Ok));
        }
        let ns_start = self.prefixed(start);
        Box::new(
            self.db
                .iterator(IteratorMode::From(&ns_start, Direction::Forward))
                .map(|res| res.map(|(k, v)| (k.to_vec(), v.to_vec()))),
        )
    }

    #[inline]
    pub fn inner_db(&self) -> &DB {
        &self.db
    }

    #[inline]
    pub fn prefix(&self) -> &[u8] {
        &self.prefix
    }

    fn load_previous_values(
        &self,
        pending_ops: &[PendingChange],
    ) -> Result<HashMap<Vec<u8>, Option<Vec<u8>>>, RocksError> {
        let mut unique: Vec<Vec<u8>> = Vec::new();
        let mut seen: HashSet<Vec<u8>> = HashSet::new();

        for op in pending_ops {
            if seen.insert(op.key.clone()) {
                unique.push(op.key.clone());
            }
        }

        let mut out = HashMap::new();
        if unique.is_empty() {
            return Ok(out);
        }

        let results = self.db.multi_get(unique.clone());
        for (idx, res) in results.into_iter().enumerate() {
            match res {
                Ok(Some(slice)) => {
                    out.insert(unique[idx].clone(), Some(slice.to_vec()));
                }
                Ok(None) => {
                    out.insert(unique[idx].clone(), None);
                }
                Err(e) => return Err(e),
            }
        }
        Ok(out)
    }

    fn versioned_manager(&self) -> Option<Arc<crate::runtime::tree_db::VersionedTreeDb>> {
        if !self.versioned {
            return None;
        }
        get_global_tree_db()
    }
}

struct PendingChange {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

pub struct MdbBatch<'a> {
    mdb: &'a Mdb,
    wb: Option<&'a mut WriteBatch>,
    pending_ops: Option<&'a mut Vec<PendingChange>>,
    versioned_changes: Option<&'a mut Vec<VersionedChange>>,
}

struct VersionedChange {
    key: Vec<u8>,
    value: Option<Vec<u8>>,
}

impl<'a> MdbBatch<'a> {
    #[inline]
    pub fn put(&mut self, k: &[u8], v: &[u8]) {
        let key = self.mdb.prefixed(k);
        if let Some(buf) = self.versioned_changes.as_mut() {
            buf.push(VersionedChange { key, value: Some(v.to_vec()) });
            return;
        }
        if let Some(buf) = self.pending_ops.as_mut() {
            buf.push(PendingChange { key: key.clone(), value: Some(v.to_vec()) });
        }
        if let Some(wb) = self.wb.as_mut() {
            wb.put(key, v);
        }
    }
    #[inline]
    pub fn delete(&mut self, k: &[u8]) {
        let key = self.mdb.prefixed(k);
        if let Some(buf) = self.versioned_changes.as_mut() {
            buf.push(VersionedChange { key, value: None });
            return;
        }
        if let Some(buf) = self.pending_ops.as_mut() {
            buf.push(PendingChange { key: key.clone(), value: None });
        }
        if let Some(wb) = self.wb.as_mut() {
            wb.delete(key);
        }
    }
}
