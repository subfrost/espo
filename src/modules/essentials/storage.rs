use crate::alkanes::trace::EspoSandshrewLikeTrace;
use crate::modules::essentials::utils::inspections::AlkaneCreationRecord;
use crate::modules::essentials::utils::balances::SignedU128;
use crate::runtime::mdb::Mdb;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use bitcoin::{Address, Network, ScriptBuf};
use borsh::{BorshDeserialize, BorshSerialize};

use anyhow::Result;
use std::collections::{BTreeMap, HashMap, VecDeque};
use std::sync::{Arc, OnceLock, RwLock};

/// Identifier for a holder: either a Bitcoin address or another Alkane.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, BorshSerialize, BorshDeserialize)]
pub enum HolderId {
    Address(String),
    Alkane(SchemaAlkaneId),
}

/// Entry in holders index (holder id + amount for one alkane)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HolderEntry {
    pub holder: HolderId,
    pub amount: u128,
}

/// One alkane balance record inside a single outpoint (BORSH)
#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct BalanceEntry {
    pub alkane: SchemaAlkaneId,
    pub amount: u128,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AlkaneBalanceTxEntry {
    pub txid: [u8; 32],
    pub height: u32,
    pub outflow: BTreeMap<SchemaAlkaneId, SignedU128>,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct AlkaneTxSummary {
    pub txid: [u8; 32],
    pub traces: Vec<EspoSandshrewLikeTrace>,
    pub outflows: Vec<AlkaneBalanceTxEntry>,
    pub height: u32,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct HoldersCountEntry {
    pub count: u64,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct BlockSummary {
    pub trace_count: u32,
    pub header: Vec<u8>,
}

const BLOCK_SUMMARY_CACHE_CAP: usize = 100;

struct BlockSummaryCache {
    order: VecDeque<u32>,
    map: HashMap<u32, BlockSummary>,
}

impl BlockSummaryCache {
    fn insert(&mut self, height: u32, summary: BlockSummary) {
        if self.map.contains_key(&height) {
            self.order.retain(|h| *h != height);
        }
        self.map.insert(height, summary);
        self.order.push_back(height);
        while self.order.len() > BLOCK_SUMMARY_CACHE_CAP {
            if let Some(oldest) = self.order.pop_front() {
                self.map.remove(&oldest);
            }
        }
    }

    fn get(&self, height: u32) -> Option<BlockSummary> {
        self.map.get(&height).cloned()
    }
}

static BLOCK_SUMMARY_CACHE: OnceLock<Arc<RwLock<BlockSummaryCache>>> = OnceLock::new();

fn block_summary_cache() -> &'static Arc<RwLock<BlockSummaryCache>> {
    BLOCK_SUMMARY_CACHE.get_or_init(|| {
        Arc::new(RwLock::new(BlockSummaryCache {
            order: VecDeque::new(),
            map: HashMap::new(),
        }))
    })
}

pub fn cache_block_summary(height: u32, summary: BlockSummary) {
    if let Ok(mut cache) = block_summary_cache().write() {
        cache.insert(height, summary);
    }
}

pub fn get_cached_block_summary(height: u32) -> Option<BlockSummary> {
    block_summary_cache()
        .read()
        .ok()
        .and_then(|cache| cache.get(height))
}

pub fn preload_block_summary_cache(mdb: &Mdb) -> usize {
    let prefix = block_summary_prefix();
    let prefix_full = mdb.prefixed(prefix);
    let mut loaded = 0usize;

    for res in mdb.iter_prefix_rev(&prefix_full) {
        if loaded >= BLOCK_SUMMARY_CACHE_CAP {
            break;
        }
        let Ok((k, v)) = res else { continue };
        let rel = &k[mdb.prefix().len()..];
        if !rel.starts_with(prefix) {
            break;
        }
        let height_bytes = &rel[prefix.len()..];
        if height_bytes.len() != 4 {
            continue;
        }
        let mut arr = [0u8; 4];
        arr.copy_from_slice(height_bytes);
        let height = u32::from_be_bytes(arr);
        if let Ok(summary) = BlockSummary::try_from_slice(&v) {
            cache_block_summary(height, summary);
            loaded += 1;
        }
    }

    loaded
}

/// Creation metadata for an alkane.
#[derive(Clone, Debug, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
pub struct AlkaneInfo {
    pub creation_txid: [u8; 32],
    pub creation_height: u32,
    pub creation_timestamp: u32,
}

pub fn addr_spk_key(addr: &str) -> Vec<u8> {
    let mut k = b"/addr_spk/".to_vec();
    k.extend_from_slice(addr.as_bytes());
    k
}

// /balances/{address}/{borsh(EspoOutpoint)}
pub fn balances_key(address: &str, outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/balances/".to_vec();
    k.extend_from_slice(address.as_bytes());
    k.push(b'/');
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /holders/{alkane block:u32be}{tx:u64be}
pub fn holders_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut k = b"/holders/".to_vec();
    k.extend_from_slice(&alkane.block.to_be_bytes());
    k.extend_from_slice(&alkane.tx.to_be_bytes());
    k
}
// /holders/count/{alkane block:u32be}{tx:u64be}
pub fn holders_count_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/holders/count/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkane_balance_txs/{alkane block:u32be}{tx:u64be}
pub fn alkane_balance_txs_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_balance_txs/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkane_balance_txs_by_height/{height_be}
pub fn alkane_balance_txs_by_height_key(height: u32) -> Vec<u8> {
    let mut key = b"/alkane_balance_txs_by_height/".to_vec();
    key.extend_from_slice(&height.to_be_bytes());
    key
}

// /alkane_balance_txs_by_token/{owner block:u32be}{owner tx:u64be}/{token block:u32be}{token tx:u64be}
pub fn alkane_balance_txs_by_token_key(owner: &SchemaAlkaneId, token: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_balance_txs_by_token/".to_vec();
    key.extend_from_slice(&owner.block.to_be_bytes());
    key.extend_from_slice(&owner.tx.to_be_bytes());
    key.push(b'/');
    key.extend_from_slice(&token.block.to_be_bytes());
    key.extend_from_slice(&token.tx.to_be_bytes());
    key
}

// /alkane_balances/{owner block:u32be}{tx:u64be}
pub fn alkane_balances_key(owner: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_balances/".to_vec();
    key.extend_from_slice(&owner.block.to_be_bytes());
    key.extend_from_slice(&owner.tx.to_be_bytes());
    key
}
// /alkane_info/{alkane block:u32be}{tx:u64be}
pub fn alkane_info_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkane_info/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkanes/name/{name}/{alkane block:u32be}{tx:u64be}
pub fn alkane_name_index_key(name: &str, alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkanes/name/".to_vec();
    key.extend_from_slice(name.as_bytes());
    key.push(b'/');
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

pub fn alkane_name_index_prefix(name_prefix: &str) -> Vec<u8> {
    let mut key = b"/alkanes/name/".to_vec();
    key.extend_from_slice(name_prefix.as_bytes());
    key
}

pub fn parse_alkane_name_index_key(key: &[u8]) -> Option<(String, SchemaAlkaneId)> {
    let prefix = b"/alkanes/name/";
    if !key.starts_with(prefix) {
        return None;
    }
    let rest = &key[prefix.len()..];
    let split = rest.iter().rposition(|b| *b == b'/')?;
    let name_bytes = &rest[..split];
    let id_bytes = &rest[split + 1..];
    if id_bytes.len() != 12 {
        return None;
    }
    let mut block_arr = [0u8; 4];
    block_arr.copy_from_slice(&id_bytes[..4]);
    let mut tx_arr = [0u8; 8];
    tx_arr.copy_from_slice(&id_bytes[4..12]);
    let name = String::from_utf8(name_bytes.to_vec()).ok()?;
    Some((
        name,
        SchemaAlkaneId { block: u32::from_be_bytes(block_arr), tx: u64::from_be_bytes(tx_arr) },
    ))
}
// /alkanes/creation/id/{alkane block:u32be}{tx:u64be}
pub fn alkane_creation_by_id_key(alkane: &SchemaAlkaneId) -> Vec<u8> {
    let mut key = b"/alkanes/creation/id/".to_vec();
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

// /alkanes/creation/ordered/{ts_be(4)}{height_be(4)}{tx_index_be(4)}{alk_block_be(4)}{alk_tx_be(8)}
pub fn alkane_creation_ordered_key(
    timestamp: u32,
    height: u32,
    tx_index: u32,
    alkane: &SchemaAlkaneId,
) -> Vec<u8> {
    let mut key = b"/alkanes/creation/ordered/".to_vec();
    key.extend_from_slice(&timestamp.to_be_bytes());
    key.extend_from_slice(&height.to_be_bytes());
    key.extend_from_slice(&tx_index.to_be_bytes());
    key.extend_from_slice(&alkane.block.to_be_bytes());
    key.extend_from_slice(&alkane.tx.to_be_bytes());
    key
}

pub fn alkane_creation_ordered_prefix() -> &'static [u8] {
    b"/alkanes/creation/ordered/"
}

pub fn alkane_creation_count_key() -> &'static [u8] {
    b"/alkanes/creation/count"
}

pub fn alkane_tx_summary_key(txid: &[u8; 32]) -> Vec<u8> {
    let mut k = b"/alkane_tx_summary/".to_vec();
    k.extend_from_slice(txid);
    k
}

pub fn alkane_block_txid_key(height: u64, idx: u64) -> Vec<u8> {
    let mut k = b"/alkane_block/".to_vec();
    k.extend_from_slice(&height.to_be_bytes());
    k.push(b'/');
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

pub fn alkane_block_len_key(height: u64) -> Vec<u8> {
    let mut k = b"/alkane_block/".to_vec();
    k.extend_from_slice(&height.to_be_bytes());
    k.extend_from_slice(b"/length");
    k
}

pub fn alkane_address_txid_key(addr: &str, idx: u64) -> Vec<u8> {
    let mut k = b"/alkane_addr/".to_vec();
    k.extend_from_slice(addr.as_bytes());
    k.push(b'/');
    k.extend_from_slice(&idx.to_be_bytes());
    k
}

pub fn alkane_address_len_key(addr: &str) -> Vec<u8> {
    let mut k = b"/alkane_addr/".to_vec();
    k.extend_from_slice(addr.as_bytes());
    k.extend_from_slice(b"/length");
    k
}

pub fn alkane_latest_traces_key() -> &'static [u8] {
    b"/alkane_latest_traces"
}
// /outpoint_addr/{borsh(EspoOutpoint)} -> address (utf8)
pub fn outpoint_addr_key(outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/outpoint_addr/".to_vec();
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /utxo_spk/{borsh(EspoOutpoint)} -> ScriptPubKey (raw bytes)
pub fn utxo_spk_key(outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/utxo_spk/".to_vec();
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /outpoint_balances/{borsh(EspoOutpoint)} -> Vec<BalanceEntry>
pub fn outpoint_balances_key(outp: &EspoOutpoint) -> Result<Vec<u8>> {
    let mut k = b"/outpoint_balances/".to_vec();
    k.extend_from_slice(&borsh::to_vec(outp)?);
    Ok(k)
}

// /block_summary/{height_be}
pub fn block_summary_key(height: u32) -> Vec<u8> {
    let mut k = b"/block_summary/".to_vec();
    k.extend_from_slice(&height.to_be_bytes());
    k
}

pub fn block_summary_prefix() -> &'static [u8] {
    b"/block_summary/"
}

#[derive(BorshSerialize)]
struct OutpointPrefix {
    txid: Vec<u8>,
    vout: u32,
}

/// Prefix for matching any serialization of an outpoint (with or without tx_spent)
pub fn outpoint_balances_prefix(txid: &[u8], vout: u32) -> Result<Vec<u8>> {
    let mut k = b"/outpoint_balances/".to_vec();
    k.extend_from_slice(&borsh::to_vec(&OutpointPrefix { txid: txid.to_vec(), vout })?);
    Ok(k)
}

/// Helper to build an outpoint with optional spending txid for lookups.
pub fn mk_outpoint(txid: Vec<u8>, vout: u32, tx_spent: Option<Vec<u8>>) -> EspoOutpoint {
    EspoOutpoint { txid, vout, tx_spent }
}

pub fn spk_to_address_str(spk: &ScriptBuf, net: Network) -> Option<String> {
    Address::from_script(spk.as_script(), net).ok().map(|a| a.to_string())
}

pub fn encode_vec<T: BorshSerialize>(v: &Vec<T>) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(v)?)
}

pub fn decode_balances_vec(bytes: &[u8]) -> Result<Vec<BalanceEntry>> {
    Ok(Vec::<BalanceEntry>::try_from_slice(bytes)?)
}

pub fn decode_alkane_balance_tx_entries(bytes: &[u8]) -> Result<Vec<AlkaneBalanceTxEntry>> {
    if let Ok(parsed) = Vec::<AlkaneBalanceTxEntry>::try_from_slice(bytes) {
        return Ok(parsed);
    }

    #[derive(BorshDeserialize)]
    struct LegacyAlkaneBalanceTxEntry {
        txid: [u8; 32],
        outflow: BTreeMap<SchemaAlkaneId, SignedU128>,
    }

    if let Ok(legacy) = Vec::<LegacyAlkaneBalanceTxEntry>::try_from_slice(bytes) {
        return Ok(legacy
            .into_iter()
            .map(|entry| AlkaneBalanceTxEntry {
                txid: entry.txid,
                height: 0,
                outflow: entry.outflow,
            })
            .collect());
    }

    let legacy: Vec<[u8; 32]> = Vec::<[u8; 32]>::try_from_slice(bytes)?;
    Ok(legacy
        .into_iter()
        .map(|txid| AlkaneBalanceTxEntry {
            txid,
            height: 0,
            outflow: BTreeMap::new(),
        })
        .collect())
}

pub fn decode_holders_vec(bytes: &[u8]) -> Result<Vec<HolderEntry>> {
    if let Ok(parsed) = Vec::<HolderEntry>::try_from_slice(bytes) {
        return Ok(parsed);
    }

    #[derive(BorshDeserialize)]
    struct LegacyHolderEntry {
        address: String,
        amount: u128,
    }

    let legacy: Vec<LegacyHolderEntry> = Vec::<LegacyHolderEntry>::try_from_slice(bytes)?;
    Ok(legacy
        .into_iter()
        .map(|h| HolderEntry { holder: HolderId::Address(h.address), amount: h.amount })
        .collect())
}

pub fn encode_alkane_info(info: &AlkaneInfo) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(info)?)
}

pub fn decode_alkane_info(bytes: &[u8]) -> Result<AlkaneInfo> {
    Ok(AlkaneInfo::try_from_slice(bytes)?)
}

pub fn encode_creation_record(record: &AlkaneCreationRecord) -> Result<Vec<u8>> {
    Ok(borsh::to_vec(record)?)
}

pub fn decode_creation_record(bytes: &[u8]) -> Result<AlkaneCreationRecord> {
    // Try new schema first; fall back to legacy Option name/symbol layout.
    if let Ok(rec) = AlkaneCreationRecord::try_from_slice(bytes) {
        return Ok(rec);
    }

    #[derive(BorshDeserialize)]
    struct LegacyCreationRecord {
        alkane: SchemaAlkaneId,
        txid: [u8; 32],
        creation_height: u32,
        creation_timestamp: u32,
        tx_index_in_block: u32,
        inspection: Option<crate::modules::essentials::utils::inspections::StoredInspectionResult>,
        name: Option<String>,
        symbol: Option<String>,
    }

    let legacy = LegacyCreationRecord::try_from_slice(bytes)?;
    let mut names = Vec::new();
    let mut symbols = Vec::new();
    if let Some(n) = legacy.name {
        names.push(n);
    }
    if let Some(s) = legacy.symbol {
        symbols.push(s);
    }
    Ok(AlkaneCreationRecord {
        alkane: legacy.alkane,
        txid: legacy.txid,
        creation_height: legacy.creation_height,
        creation_timestamp: legacy.creation_timestamp,
        tx_index_in_block: legacy.tx_index_in_block,
        inspection: legacy.inspection,
        names,
        symbols,
    })
}

pub fn load_creation_record(
    mdb: &crate::runtime::mdb::Mdb,
    alkane: &SchemaAlkaneId,
) -> Result<Option<AlkaneCreationRecord>> {
    let key = alkane_creation_by_id_key(alkane);
    if let Some(bytes) = mdb.get(&key)? {
        let record = decode_creation_record(&bytes)?;
        Ok(Some(record))
    } else {
        Ok(None)
    }
}

pub fn get_holders_count_encoded(count: u64) -> Result<Vec<u8>> {
    let count_value = HoldersCountEntry { count };

    Ok(borsh::to_vec(&count_value)?)
}

pub fn get_holders_values_encoded(holders: Vec<HolderEntry>) -> Result<(Vec<u8>, Vec<u8>)> {
    Ok((encode_vec(&holders)?, get_holders_count_encoded(holders.len().try_into()?)?))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn alkane_info_round_trip() {
        let info = AlkaneInfo {
            creation_txid: [7u8; 32],
            creation_height: 42,
            creation_timestamp: 1_700_000_000,
        };

        let encoded = encode_alkane_info(&info).expect("encode");
        let decoded = decode_alkane_info(&encoded).expect("decode");
        assert_eq!(info, decoded);
    }

    #[test]
    fn creation_record_round_trip() {
        let rec = AlkaneCreationRecord {
            alkane: SchemaAlkaneId { block: 5, tx: 10 },
            txid: [9u8; 32],
            creation_height: 123,
            creation_timestamp: 99,
            tx_index_in_block: 3,
            inspection: None,
            names: vec!["demo".to_string(), "demo2".to_string()],
            symbols: vec!["DMO".to_string()],
        };

        let encoded = encode_creation_record(&rec).expect("encode");
        let decoded = decode_creation_record(&encoded).expect("decode");
        assert_eq!(rec, decoded);
    }
}
