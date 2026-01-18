use anyhow::{anyhow, Result};
use rocksdb::{DB, IteratorMode, Direction};
use std::sync::Arc;

/// Height-indexed append-only storage trait following metashrew's pattern
///
/// Storage schema:
/// - {key}/length → u32_le (number of updates)
/// - {key}/0 → "height0:value_hex"
/// - {key}/1 → "height1:value_hex"
/// - ...
///
/// This enables:
/// - Unlimited historical queries via binary search
/// - Efficient rollback by deleting data from target height onwards
/// - Strict mode validation with historical balance lookups
pub trait HeightIndexedStorage: Send + Sync {
    /// Store a versioned value at the given height
    fn put(&self, key: &[u8], value: &[u8], height: u32) -> Result<()>;

    /// Get the value at or before the specified height
    fn get_at_height(&self, key: &[u8], height: u32) -> Result<Option<Vec<u8>>>;

    /// Get the most recent value (at current height)
    fn get_current(&self, key: &[u8]) -> Result<Option<Vec<u8>>>;

    /// Rollback all keys to the specified height (delete all data > height)
    fn rollback_to_height(&self, height: u32) -> Result<()>;

    /// Get all historical versions of a key
    fn get_history(&self, key: &[u8]) -> Result<Vec<(u32, Vec<u8>)>>;
}

/// RocksDB-backed implementation of height-indexed storage
pub struct RocksHeightIndexedStorage {
    db: Arc<DB>,
    prefix: Vec<u8>,
}

impl RocksHeightIndexedStorage {
    pub fn new(db: Arc<DB>, prefix: impl AsRef<[u8]>) -> Self {
        Self {
            db,
            prefix: prefix.as_ref().to_vec(),
        }
    }

    /// Build a prefixed key
    fn make_key(&self, key: &[u8], suffix: &[u8]) -> Vec<u8> {
        let mut result = Vec::with_capacity(self.prefix.len() + key.len() + suffix.len() + 1);
        result.extend_from_slice(&self.prefix);
        if !self.prefix.is_empty() {
            result.push(b':');
        }
        result.extend_from_slice(key);
        result.extend_from_slice(suffix);
        result
    }

    /// Get the current update count for a key
    fn get_length(&self, key: &[u8]) -> Result<u32> {
        let length_key = self.make_key(key, b"/length");
        match self.db.get(&length_key)? {
            Some(bytes) if bytes.len() == 4 => {
                Ok(u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
            }
            Some(_) => Err(anyhow!("invalid length value")),
            None => Ok(0),
        }
    }

    /// Set the update count for a key
    fn set_length(&self, key: &[u8], length: u32) -> Result<()> {
        let length_key = self.make_key(key, b"/length");
        self.db.put(&length_key, &length.to_le_bytes())?;
        Ok(())
    }

    /// Parse a versioned value: "height:value_hex" → (height, value)
    fn parse_versioned(&self, data: &[u8]) -> Result<(u32, Vec<u8>)> {
        let s = std::str::from_utf8(data)?;
        let colon_pos = s.find(':')
            .ok_or_else(|| anyhow!("invalid versioned format: missing colon"))?;

        let height: u32 = s[..colon_pos].parse()
            .map_err(|e| anyhow!("invalid height: {}", e))?;
        let value_hex = &s[colon_pos + 1..];
        let value = hex::decode(value_hex)
            .map_err(|e| anyhow!("invalid hex value: {}", e))?;

        Ok((height, value))
    }

    /// Encode a versioned value: (height, value) → "height:value_hex"
    fn encode_versioned(&self, height: u32, value: &[u8]) -> String {
        format!("{}:{}", height, hex::encode(value))
    }

    /// Binary search through versioned values to find the highest height ≤ target
    fn binary_search_height(&self, key: &[u8], target_height: u32, length: u32) -> Result<Option<(u32, Vec<u8>)>> {
        if length == 0 {
            return Ok(None);
        }

        let mut left = 0;
        let mut right = length - 1;
        let mut best: Option<(u32, Vec<u8>)> = None;

        while left <= right {
            let mid = left + (right - left) / 2;
            let idx_key = self.make_key(key, &format!("/{}", mid).into_bytes());

            match self.db.get(&idx_key)? {
                Some(data) => {
                    let (height, value) = self.parse_versioned(&data)?;

                    if height <= target_height {
                        best = Some((height, value));
                        if mid == u32::MAX || mid == right {
                            break;
                        }
                        left = mid + 1;
                    } else {
                        if mid == 0 {
                            break;
                        }
                        right = mid - 1;
                    }
                }
                None => {
                    return Err(anyhow!("missing index entry at {}", mid));
                }
            }
        }

        Ok(best)
    }
}

impl HeightIndexedStorage for RocksHeightIndexedStorage {
    fn put(&self, key: &[u8], value: &[u8], height: u32) -> Result<()> {
        let length = self.get_length(key)?;

        let idx_key = self.make_key(key, &format!("/{}", length).into_bytes());
        let versioned = self.encode_versioned(height, value);
        self.db.put(&idx_key, versioned.as_bytes())?;

        self.set_length(key, length + 1)?;
        Ok(())
    }

    fn get_at_height(&self, key: &[u8], height: u32) -> Result<Option<Vec<u8>>> {
        let length = self.get_length(key)?;
        if length == 0 {
            return Ok(None);
        }

        match self.binary_search_height(key, height, length)? {
            Some((_, value)) => Ok(Some(value)),
            None => Ok(None),
        }
    }

    fn get_current(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let length = self.get_length(key)?;
        if length == 0 {
            return Ok(None);
        }

        let idx_key = self.make_key(key, &format!("/{}", length - 1).into_bytes());
        match self.db.get(&idx_key)? {
            Some(data) => {
                let (_, value) = self.parse_versioned(&data)?;
                Ok(Some(value))
            }
            None => Ok(None),
        }
    }

    fn rollback_to_height(&self, height: u32) -> Result<()> {
        let prefix_with_colon = if self.prefix.is_empty() {
            Vec::new()
        } else {
            let mut v = self.prefix.clone();
            v.push(b':');
            v
        };

        let mut keys_to_process = Vec::new();

        {
            let iter = self.db.iterator(IteratorMode::From(&prefix_with_colon, Direction::Forward));

            for item in iter {
                let (k, _) = item?;

                if !k.starts_with(&prefix_with_colon) {
                    break;
                }

                if k.ends_with(b"/length") {
                    let key_part = &k[prefix_with_colon.len()..k.len() - 7];
                    keys_to_process.push(key_part.to_vec());
                }
            }
        }

        for key in keys_to_process {
            let length = self.get_length(&key)?;
            if length == 0 {
                continue;
            }

            let cutoff_result = self.binary_search_height(&key, height, length)?;
            let cutoff_idx = match cutoff_result {
                Some(_) => {
                    let mut idx = 0;
                    for i in 0..length {
                        let idx_key = self.make_key(&key, &format!("/{}", i).into_bytes());
                        if let Some(data) = self.db.get(&idx_key)? {
                            let (h, _) = self.parse_versioned(&data)?;
                            if h <= height {
                                idx = i + 1;
                            } else {
                                break;
                            }
                        }
                    }
                    idx
                }
                None => 0,
            };

            for i in cutoff_idx..length {
                let idx_key = self.make_key(&key, &format!("/{}", i).into_bytes());
                self.db.delete(&idx_key)?;
            }

            if cutoff_idx > 0 {
                self.set_length(&key, cutoff_idx)?;
            } else {
                let length_key = self.make_key(&key, b"/length");
                self.db.delete(&length_key)?;
            }
        }

        Ok(())
    }

    fn get_history(&self, key: &[u8]) -> Result<Vec<(u32, Vec<u8>)>> {
        let length = self.get_length(key)?;
        let mut history = Vec::new();

        for i in 0..length {
            let idx_key = self.make_key(key, &format!("/{}", i).into_bytes());
            if let Some(data) = self.db.get(&idx_key)? {
                let (height, value) = self.parse_versioned(&data)?;
                history.push((height, value));
            }
        }

        Ok(history)
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
    fn test_basic_put_get() {
        let (db, _temp) = create_test_db();
        let storage = RocksHeightIndexedStorage::new(db, b"test");

        storage.put(b"key1", b"value1", 100).unwrap();
        storage.put(b"key1", b"value2", 200).unwrap();

        assert_eq!(storage.get_at_height(b"key1", 150).unwrap(), Some(b"value1".to_vec()));
        assert_eq!(storage.get_at_height(b"key1", 200).unwrap(), Some(b"value2".to_vec()));
        assert_eq!(storage.get_current(b"key1").unwrap(), Some(b"value2".to_vec()));
    }

    #[test]
    fn test_rollback() {
        let (db, _temp) = create_test_db();
        let storage = RocksHeightIndexedStorage::new(db, b"test");

        storage.put(b"key1", b"value1", 100).unwrap();
        storage.put(b"key1", b"value2", 200).unwrap();
        storage.put(b"key1", b"value3", 300).unwrap();

        storage.rollback_to_height(200).unwrap();

        assert_eq!(storage.get_current(b"key1").unwrap(), Some(b"value2".to_vec()));
        assert_eq!(storage.get_at_height(b"key1", 300).unwrap(), Some(b"value2".to_vec()));
    }
}
