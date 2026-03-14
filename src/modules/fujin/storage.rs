use super::consts::KEY_INDEX_HEIGHT;
use super::schemas::*;
use crate::modules::essentials::storage::{
    EssentialsProvider, GetAlkaneStorageValueParams,
    GetBlockSummaryParams, RpcGetAddressBalancesParams,
    GetIndexHeightParams as EssentialsGetIndexHeightParams,
};
use crate::runtime::mdb::{Mdb, MdbBatch};
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use borsh::BorshDeserialize;
use std::sync::Arc;

// ── MdbPointer (reuse pattern from subfrost) ──

#[derive(Clone)]
pub struct MdbPointer<'a> {
    mdb: &'a Mdb,
    key: Vec<u8>,
}

impl<'a> MdbPointer<'a> {
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

    pub fn get(&self) -> Result<Option<Vec<u8>>> {
        self.mdb.get(&self.key).map_err(|e| anyhow!("mdb.get failed: {e}"))
    }

    pub fn put(&self, value: &[u8]) -> Result<()> {
        self.mdb.put(&self.key, value).map_err(|e| anyhow!("mdb.put failed: {e}"))
    }

    pub fn scan_prefix(&self) -> Result<Vec<Vec<u8>>> {
        self.mdb.scan_prefix(&self.key).map_err(|e| anyhow!("mdb.scan_prefix failed: {e}"))
    }
}

// ── FujinTable ──

#[allow(non_snake_case)]
#[derive(Clone)]
pub struct FujinTable<'a> {
    pub ROOT: MdbPointer<'a>,
    pub INDEX_HEIGHT: MdbPointer<'a>,
    pub FACTORY: MdbPointer<'a>,
    pub VAULT: MdbPointer<'a>,
    pub EPOCH: MdbPointer<'a>,
    pub EPOCH_LIST: MdbPointer<'a>,
    pub POOL_STATE: MdbPointer<'a>,
    pub SNAPSHOT: MdbPointer<'a>,
    pub VAULT_STATE: MdbPointer<'a>,
    pub SETTLEMENT: MdbPointer<'a>,
    pub ACTIVITY: MdbPointer<'a>,
    pub ACTIVITY_ALL: MdbPointer<'a>,
    pub ACTIVITY_ADDR: MdbPointer<'a>,
}

impl<'a> FujinTable<'a> {
    pub fn new(mdb: &'a Mdb) -> Self {
        let root = MdbPointer::root(mdb);
        FujinTable {
            ROOT: root.clone(),
            INDEX_HEIGHT: root.select(KEY_INDEX_HEIGHT),
            FACTORY: root.keyword("/factory"),
            VAULT: root.keyword("/vault"),
            EPOCH: root.keyword("/epoch/v1/"),
            EPOCH_LIST: root.keyword("/epoch_list/v1"),
            POOL_STATE: root.keyword("/pool_state/v1/"),
            SNAPSHOT: root.keyword("/snapshot/v1"),
            VAULT_STATE: root.keyword("/vault_state/v1"),
            SETTLEMENT: root.keyword("/settlement/v1/"),
            ACTIVITY: root.keyword("/activity/v1/"),
            ACTIVITY_ALL: root.keyword("/activity_all/v1/"),
            ACTIVITY_ADDR: root.keyword("/activity_addr/v1/"),
        }
    }

    // ── Key builders ──

    pub fn epoch_key(&self, epoch: u128) -> Vec<u8> {
        let mut k = self.EPOCH.key().to_vec();
        k.extend_from_slice(&epoch.to_be_bytes());
        k
    }

    pub fn pool_state_key(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.POOL_STATE.key().to_vec();
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.push(b':');
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k
    }

    pub fn settlement_key(&self, epoch: u128) -> Vec<u8> {
        let mut k = self.SETTLEMENT.key().to_vec();
        k.extend_from_slice(&epoch.to_be_bytes());
        k
    }

    pub fn activity_pool_key(&self, pool: &SchemaAlkaneId, ts: u64, seq: u32) -> Vec<u8> {
        let mut k = self.ACTIVITY.key().to_vec();
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.push(b':');
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k.push(b'/');
        k.extend_from_slice(&ts.to_be_bytes());
        k.push(b':');
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    pub fn activity_pool_prefix(&self, pool: &SchemaAlkaneId) -> Vec<u8> {
        let mut k = self.ACTIVITY.key().to_vec();
        k.extend_from_slice(&pool.block.to_be_bytes());
        k.push(b':');
        k.extend_from_slice(&pool.tx.to_be_bytes());
        k.push(b'/');
        k
    }

    pub fn activity_all_key(&self, ts: u64, seq: u32) -> Vec<u8> {
        let mut k = self.ACTIVITY_ALL.key().to_vec();
        k.extend_from_slice(&ts.to_be_bytes());
        k.push(b':');
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    pub fn activity_addr_key(&self, spk: &[u8], ts: u64, seq: u32) -> Vec<u8> {
        let mut k = self.ACTIVITY_ADDR.key().to_vec();
        let len = spk.len().min(u16::MAX as usize) as u16;
        k.extend_from_slice(&len.to_be_bytes());
        k.extend_from_slice(&spk[..len as usize]);
        k.push(b'/');
        k.extend_from_slice(&ts.to_be_bytes());
        k.push(b':');
        k.extend_from_slice(&seq.to_be_bytes());
        k
    }

    pub fn activity_addr_prefix(&self, spk: &[u8]) -> Vec<u8> {
        let mut k = self.ACTIVITY_ADDR.key().to_vec();
        let len = spk.len().min(u16::MAX as usize) as u16;
        k.extend_from_slice(&len.to_be_bytes());
        k.extend_from_slice(&spk[..len as usize]);
        k.push(b'/');
        k
    }
}

// ── Param / Result structs ──

pub struct GetIndexHeightParams;
pub struct GetIndexHeightResult {
    pub height: Option<u32>,
}
pub struct SetIndexHeightParams {
    pub height: u32,
}
pub struct SetBatchParams {
    pub puts: Vec<(Vec<u8>, Vec<u8>)>,
    pub deletes: Vec<Vec<u8>>,
}

// ── FujinProvider ──

#[derive(Clone)]
pub struct FujinProvider {
    mdb: Arc<Mdb>,
    essentials: Arc<EssentialsProvider>,
}

impl FujinProvider {
    pub fn new(mdb: Arc<Mdb>, essentials: Arc<EssentialsProvider>) -> Self {
        Self { mdb, essentials }
    }

    pub fn table(&self) -> FujinTable<'_> {
        FujinTable::new(self.mdb.as_ref())
    }

    pub fn essentials(&self) -> &EssentialsProvider {
        &self.essentials
    }

    // ── Index height ──

    pub fn get_index_height(&self, _params: GetIndexHeightParams) -> Result<GetIndexHeightResult> {
        let table = self.table();
        let Some(bytes) = table.INDEX_HEIGHT.get()? else {
            return Ok(GetIndexHeightResult { height: None });
        };
        if bytes.len() != 4 {
            return Err(anyhow!("invalid /index_height length {}", bytes.len()));
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(&bytes);
        Ok(GetIndexHeightResult { height: Some(u32::from_le_bytes(arr)) })
    }

    pub fn set_index_height(&self, params: SetIndexHeightParams) -> Result<()> {
        let table = self.table();
        table.INDEX_HEIGHT.put(&params.height.to_le_bytes())
    }

    // ── Batch write ──

    pub fn set_batch(&self, params: SetBatchParams) -> Result<()> {
        self.mdb
            .bulk_write(|wb: &mut MdbBatch<'_>| {
                for key in &params.deletes {
                    wb.delete(key);
                }
                for (key, value) in &params.puts {
                    wb.put(key, value);
                }
            })
            .map_err(|e| anyhow!("mdb.bulk_write failed: {e}"))
    }

    // ── Epoch storage ──

    pub fn get_epoch_info(&self, epoch: u128) -> Result<Option<SchemaEpochInfo>> {
        let table = self.table();
        let key = table.epoch_key(epoch);
        match self.mdb.get(&key).map_err(|e| anyhow!("mdb.get: {e}"))? {
            Some(bytes) => Ok(Some(SchemaEpochInfo::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    pub fn get_epoch_list(&self) -> Result<Vec<u128>> {
        let table = self.table();
        match table.EPOCH_LIST.get()? {
            Some(bytes) => Ok(Vec::<u128>::try_from_slice(&bytes)?),
            None => Ok(Vec::new()),
        }
    }

    // ── Pool state ──

    pub fn get_pool_state(&self, pool: &SchemaAlkaneId) -> Result<Option<SchemaPoolState>> {
        let table = self.table();
        let key = table.pool_state_key(pool);
        match self.mdb.get(&key).map_err(|e| anyhow!("mdb.get: {e}"))? {
            Some(bytes) => Ok(Some(SchemaPoolState::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    // ── Snapshot ──

    pub fn get_snapshot(&self) -> Result<Option<SchemaFujinSnapshot>> {
        let table = self.table();
        match table.SNAPSHOT.get()? {
            Some(bytes) => Ok(Some(SchemaFujinSnapshot::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    // ── Vault state ──

    pub fn get_vault_state(&self) -> Result<Option<SchemaVaultState>> {
        let table = self.table();
        match table.VAULT_STATE.get()? {
            Some(bytes) => Ok(Some(SchemaVaultState::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    // ── Settlement history ──

    pub fn get_settlement(&self, epoch: u128) -> Result<Option<SchemaSettlementV1>> {
        let table = self.table();
        let key = table.settlement_key(epoch);
        match self.mdb.get(&key).map_err(|e| anyhow!("mdb.get: {e}"))? {
            Some(bytes) => Ok(Some(SchemaSettlementV1::try_from_slice(&bytes)?)),
            None => Ok(None),
        }
    }

    // ── Activity (iter prefix reverse) ──

    pub fn get_activity_iter(
        &self,
        prefix: &[u8],
        offset: usize,
        limit: usize,
    ) -> Result<(Vec<SchemaFujinActivityV1>, usize)> {
        let full_prefix = self.mdb.prefixed(prefix);
        let mut total = 0usize;
        let mut out = Vec::new();
        let mut seen = 0usize;
        for res in self.mdb.iter_prefix_rev(&full_prefix) {
            let (_k, v) = res.map_err(|e| anyhow!("iter_prefix_rev: {e}"))?;
            let entry = match SchemaFujinActivityV1::try_from_slice(&v) {
                Ok(e) => e,
                Err(_) => continue,
            };
            total += 1;
            if seen < offset {
                seen += 1;
                continue;
            }
            if out.len() < limit {
                out.push(entry);
            }
        }
        Ok((out, total))
    }

    // ── Essentials cross-read: alkane storage ──

    pub fn read_alkane_storage(
        &self,
        alkane: SchemaAlkaneId,
        key: &[u8],
    ) -> Result<Option<Vec<u8>>> {
        let result = self.essentials.get_alkane_storage_value(GetAlkaneStorageValueParams {
            alkane,
            key: key.to_vec(),
        })?;
        Ok(result.value)
    }

    // ── Essentials cross-read: address balances ──

    pub fn get_address_balances(&self, address: &str) -> Result<serde_json::Value> {
        let result = self.essentials.rpc_get_address_balances(
            RpcGetAddressBalancesParams {
                address: Some(address.to_string()),
                include_outpoints: Some(false),
            },
        )?;
        Ok(result.value)
    }

    // ── Essentials cross-read: block summary (header bytes) ──

    pub fn get_essentials_block_summary(&self, height: u32) -> Result<Option<Vec<u8>>> {
        let result = self.essentials.get_block_summary(GetBlockSummaryParams { height })?;
        Ok(result.summary.map(|s| s.header))
    }

    // ── Essentials cross-read: index height ──

    pub fn get_essentials_index_height(&self) -> Result<Option<u32>> {
        let result = self.essentials.get_index_height(EssentialsGetIndexHeightParams)?;
        Ok(result.height)
    }
}
