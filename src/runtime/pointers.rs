use crate::runtime::mdb::{Mdb, MdbBatch};
use anyhow::{Result, anyhow};

pub trait KeyValuePointer {
    fn key(&self) -> &[u8];
    fn get(&self) -> Result<Option<Vec<u8>>>;
    fn put(&self, value: &[u8]) -> Result<()>;
    fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>>;
    fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>);
}

pub trait ListBasedPointer {
    fn key(&self) -> &[u8];
    fn get(&self) -> Result<Option<Vec<u8>>>;
    fn put(&self, value: &[u8]) -> Result<()>;
    fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>>;
    fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>);
}

#[derive(Clone)]
pub struct KvPointer<'a> {
    mdb: &'a Mdb,
    key: Vec<u8>,
}

impl<'a> KvPointer<'a> {
    pub fn root(mdb: &'a Mdb) -> Self {
        Self { mdb, key: Vec::new() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn keyword(&self, suffix: &str) -> Self {
        self.select(suffix.as_bytes())
    }

    pub fn select(&self, suffix: &[u8]) -> Self {
        let mut key = self.key.clone();
        key.extend_from_slice(suffix);
        Self { mdb: self.mdb, key }
    }

    pub fn list_keyword(&self, suffix: &str) -> ListPointer<'a> {
        self.list_select(suffix.as_bytes())
    }

    pub fn list_select(&self, suffix: &[u8]) -> ListPointer<'a> {
        let mut key = self.key.clone();
        key.extend_from_slice(suffix);
        ListPointer { mdb: self.mdb, key }
    }

    pub fn get(&self) -> Result<Option<Vec<u8>>> {
        self.mdb.get(&self.key).map_err(|e| anyhow!("mdb.get failed: {e}"))
    }

    pub fn put(&self, value: &[u8]) -> Result<()> {
        self.mdb.put(&self.key, value).map_err(|e| anyhow!("mdb.put failed: {e}"))
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let full_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| {
                let mut key = self.key.clone();
                key.extend_from_slice(k);
                key
            })
            .collect();
        self.mdb.multi_get(&full_keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}"))
    }

    pub fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        self.mdb.bulk_write(build).map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }
}

impl<'a> KeyValuePointer for KvPointer<'a> {
    fn key(&self) -> &[u8] {
        self.key()
    }

    fn get(&self) -> Result<Option<Vec<u8>>> {
        self.get()
    }

    fn put(&self, value: &[u8]) -> Result<()> {
        self.put(value)
    }

    fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.multi_get(keys)
    }

    fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        self.bulk_write(build)
    }
}

#[derive(Clone)]
pub struct ListPointer<'a> {
    mdb: &'a Mdb,
    key: Vec<u8>,
}

impl<'a> ListPointer<'a> {
    pub fn root(mdb: &'a Mdb) -> Self {
        Self { mdb, key: Vec::new() }
    }

    pub fn key(&self) -> &[u8] {
        &self.key
    }

    pub fn keyword(&self, suffix: &str) -> Self {
        self.select(suffix.as_bytes())
    }

    pub fn select(&self, suffix: &[u8]) -> Self {
        let mut key = self.key.clone();
        key.extend_from_slice(suffix);
        Self { mdb: self.mdb, key }
    }

    pub fn kv_keyword(&self, suffix: &str) -> KvPointer<'a> {
        self.kv_select(suffix.as_bytes())
    }

    pub fn kv_select(&self, suffix: &[u8]) -> KvPointer<'a> {
        let mut key = self.key.clone();
        key.extend_from_slice(suffix);
        KvPointer { mdb: self.mdb, key }
    }

    pub fn get(&self) -> Result<Option<Vec<u8>>> {
        self.mdb.get(&self.key).map_err(|e| anyhow!("mdb.get failed: {e}"))
    }

    pub fn put(&self, value: &[u8]) -> Result<()> {
        self.mdb.put(&self.key, value).map_err(|e| anyhow!("mdb.put failed: {e}"))
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        let full_keys: Vec<Vec<u8>> = keys
            .iter()
            .map(|k| {
                let mut key = self.key.clone();
                key.extend_from_slice(k);
                key
            })
            .collect();
        self.mdb.multi_get(&full_keys).map_err(|e| anyhow!("mdb.multi_get failed: {e}"))
    }

    pub fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        self.mdb.bulk_write(build).map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }
}

impl<'a> ListBasedPointer for ListPointer<'a> {
    fn key(&self) -> &[u8] {
        self.key()
    }

    fn get(&self) -> Result<Option<Vec<u8>>> {
        self.get()
    }

    fn put(&self, value: &[u8]) -> Result<()> {
        self.put(value)
    }

    fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>> {
        self.multi_get(keys)
    }

    fn bulk_write<F>(&self, build: F) -> Result<()>
    where
        F: FnOnce(&mut MdbBatch<'_>),
    {
        self.bulk_write(build)
    }
}
