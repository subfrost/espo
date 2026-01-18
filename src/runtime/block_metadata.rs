use anyhow::{anyhow, Result};
use rocksdb::DB;
use std::sync::Arc;

/// Tracks block hashes for reorg detection following metashrew's pattern
///
/// Storage keys:
/// - /__INTERNAL/height → current_height_le4
/// - /__INTERNAL/height-to-hash/{height} → block_hash_hex
///
/// This enables:
/// - Metashrew-style reorg detection by walking backwards comparing hashes
/// - Efficient lookup of historical block hashes
/// - Tracking of current indexed height
pub struct BlockMetadata {
    db: Arc<DB>,
}

impl BlockMetadata {
    const HEIGHT_KEY: &'static [u8] = b"/__INTERNAL/height";
    const HASH_PREFIX: &'static [u8] = b"/__INTERNAL/height-to-hash/";

    pub fn new(db: Arc<DB>) -> Self {
        Self { db }
    }

    /// Get the current indexed height
    pub fn get_indexed_height(&self) -> Result<Option<u32>> {
        match self.db.get(Self::HEIGHT_KEY)? {
            Some(bytes) if bytes.len() == 4 => {
                Ok(Some(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]])))
            }
            Some(_) => Err(anyhow!("invalid height value")),
            None => Ok(None),
        }
    }

    /// Set the current indexed height
    pub fn set_indexed_height(&self, height: u32) -> Result<()> {
        self.db.put(Self::HEIGHT_KEY, &height.to_le_bytes())?;
        Ok(())
    }

    /// Store a block hash at the given height
    pub fn store_block_hash(&self, height: u32, block_hash: &str) -> Result<()> {
        let key = self.make_hash_key(height);
        self.db.put(&key, block_hash.as_bytes())?;
        Ok(())
    }

    /// Get the block hash at the given height
    pub fn get_block_hash(&self, height: u32) -> Result<Option<String>> {
        let key = self.make_hash_key(height);
        match self.db.get(&key)? {
            Some(bytes) => {
                let hash = String::from_utf8(bytes)
                    .map_err(|e| anyhow!("invalid block hash UTF-8: {}", e))?;
                Ok(Some(hash))
            }
            None => Ok(None),
        }
    }

    /// Delete block hashes from the given height onwards (for rollback)
    pub fn delete_hashes_from(&self, height: u32) -> Result<()> {
        const MAX_HEIGHT: u32 = 10_000_000;

        for h in height..MAX_HEIGHT {
            let key = self.make_hash_key(h);
            match self.db.get(&key)? {
                Some(_) => {
                    self.db.delete(&key)?;
                }
                None => {
                    break;
                }
            }
        }
        Ok(())
    }

    /// Build the storage key for a block hash at a given height
    fn make_hash_key(&self, height: u32) -> Vec<u8> {
        let mut key = Self::HASH_PREFIX.to_vec();
        key.extend_from_slice(&height.to_le_bytes());
        key
    }

    /// Walk backwards from current height comparing block hashes with the remote chain
    /// Returns the height of the common ancestor, or None if no reorg detected
    pub fn detect_reorg_height<F>(
        &self,
        current_height: u32,
        max_depth: u32,
        mut get_remote_hash: F,
    ) -> Result<Option<u32>>
    where
        F: FnMut(u32) -> Result<Option<String>>,
    {
        if current_height == 0 {
            return Ok(None);
        }

        let min_height = current_height.saturating_sub(max_depth);

        for check_height in (min_height..current_height).rev() {
            let local_hash = self.get_block_hash(check_height)?;
            let remote_hash = get_remote_hash(check_height)?;

            match (local_hash, remote_hash) {
                (Some(local), Some(remote)) => {
                    if local == remote {
                        if check_height == current_height - 1 {
                            return Ok(None);
                        } else {
                            return Ok(Some(check_height));
                        }
                    }
                }
                (None, _) | (_, None) => {
                    return Err(anyhow!(
                        "missing block hash at height {} during reorg detection",
                        check_height
                    ));
                }
            }
        }

        Err(anyhow!(
            "reorg exceeds maximum depth of {} blocks",
            max_depth
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rocksdb::Options;
    use tempfile::TempDir;

    fn create_test_db() -> (Arc<DB>, TempDir) {
        let temp_dir = TempDir::new().unwrap();
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = DB::open(&opts, temp_dir.path()).unwrap();
        (Arc::new(db), temp_dir)
    }

    #[test]
    fn test_height_tracking() {
        let (db, _temp) = create_test_db();
        let meta = BlockMetadata::new(db);

        assert_eq!(meta.get_indexed_height().unwrap(), None);

        meta.set_indexed_height(100).unwrap();
        assert_eq!(meta.get_indexed_height().unwrap(), Some(100));

        meta.set_indexed_height(200).unwrap();
        assert_eq!(meta.get_indexed_height().unwrap(), Some(200));
    }

    #[test]
    fn test_block_hash_storage() {
        let (db, _temp) = create_test_db();
        let meta = BlockMetadata::new(db);

        meta.store_block_hash(100, "hash100").unwrap();
        meta.store_block_hash(101, "hash101").unwrap();

        assert_eq!(meta.get_block_hash(100).unwrap(), Some("hash100".to_string()));
        assert_eq!(meta.get_block_hash(101).unwrap(), Some("hash101".to_string()));
        assert_eq!(meta.get_block_hash(102).unwrap(), None);
    }

    #[test]
    fn test_reorg_detection() {
        let (db, _temp) = create_test_db();
        let meta = BlockMetadata::new(db);

        meta.store_block_hash(100, "hash100").unwrap();
        meta.store_block_hash(101, "hash101").unwrap();
        meta.store_block_hash(102, "hash102").unwrap();
        meta.store_block_hash(103, "hash103_old").unwrap();

        let get_remote = |height: u32| -> Result<Option<String>> {
            Ok(Some(match height {
                100 => "hash100".to_string(),
                101 => "hash101".to_string(),
                102 => "hash102_new".to_string(),
                103 => "hash103_new".to_string(),
                _ => "unknown".to_string(),
            }))
        };

        let result = meta.detect_reorg_height(104, 100, get_remote).unwrap();
        assert_eq!(result, Some(101));
    }

    #[test]
    fn test_no_reorg() {
        let (db, _temp) = create_test_db();
        let meta = BlockMetadata::new(db);

        meta.store_block_hash(100, "hash100").unwrap();
        meta.store_block_hash(101, "hash101").unwrap();

        let get_remote = |height: u32| -> Result<Option<String>> {
            Ok(Some(match height {
                100 => "hash100".to_string(),
                101 => "hash101".to_string(),
                _ => "unknown".to_string(),
            }))
        };

        let result = meta.detect_reorg_height(102, 100, get_remote).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_delete_hashes_from() {
        let (db, _temp) = create_test_db();
        let meta = BlockMetadata::new(db);

        meta.store_block_hash(100, "hash100").unwrap();
        meta.store_block_hash(101, "hash101").unwrap();
        meta.store_block_hash(102, "hash102").unwrap();
        meta.store_block_hash(103, "hash103").unwrap();

        meta.delete_hashes_from(102).unwrap();

        assert_eq!(meta.get_block_hash(100).unwrap(), Some("hash100".to_string()));
        assert_eq!(meta.get_block_hash(101).unwrap(), Some("hash101".to_string()));
        assert_eq!(meta.get_block_hash(102).unwrap(), None);
        assert_eq!(meta.get_block_hash(103).unwrap(), None);
    }
}
