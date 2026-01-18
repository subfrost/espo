use crate::alkanes::trace::PartialEspoTrace;
use crate::config::{get_metashrew_sdb, is_strict_mode};
use crate::runtime::sdb::SDB;
use crate::schemas::SchemaAlkaneId;
use alkanes_cli_common::alkanes_pb::{AlkanesTrace, AlkanesTraceEvent};
use alkanes_support::gz;
use alkanes_support::id::AlkaneId as SupportAlkaneId;
use anyhow::{Context, Result, anyhow};
use bitcoin::OutPoint;
use bitcoin::Txid;
use bitcoin::consensus::encode::serialize;
use bitcoin::hashes::Hash;
use prost::Message;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicU32, Ordering};

fn try_decode_trace_prost(raw: &[u8]) -> Option<AlkanesTrace> {
    AlkanesTrace::decode(raw).ok().or_else(|| {
        if raw.len() >= 4 { AlkanesTrace::decode(&raw[..raw.len() - 4]).ok() } else { None }
    })
}

fn try_decode_trace_event_prost(raw: &[u8]) -> Option<AlkanesTraceEvent> {
    AlkanesTraceEvent::decode(raw).ok().or_else(|| {
        if raw.len() >= 4 { AlkanesTraceEvent::decode(&raw[..raw.len() - 4]).ok() } else { None }
    })
}

/// Traces can be stored as raw protobuf bytes or as UTF-8 "height:HEX" blobs.
/// This helper handles both by decoding any hex payload and stripping the
/// optional 4-byte trailer some entries carry.
pub fn decode_trace_blob(bytes: &[u8]) -> Option<AlkanesTrace> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Some((_block, hex_part)) = s.split_once(':') {
            if let Ok(decoded) = hex::decode(hex_part) {
                if let Some(trace) = try_decode_trace_prost(&decoded) {
                    return Some(trace);
                }
            }
        }
    }

    try_decode_trace_prost(bytes)
}

pub fn decode_trace_event_blob(bytes: &[u8]) -> Option<AlkanesTraceEvent> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Some((_block, hex_part)) = s.split_once(':') {
            if let Ok(decoded) = hex::decode(hex_part) {
                if let Some(event) = try_decode_trace_event_prost(&decoded) {
                    return Some(event);
                }
            }
        }
    }

    try_decode_trace_event_prost(bytes)
}

fn parse_ascii_or_le_u64(bytes: &[u8]) -> Option<u64> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Ok(v) = s.parse::<u64>() {
            return Some(v);
        }
        if let Some((_height, hex_part)) = s.split_once(':') {
            if let Ok(decoded) = hex::decode(hex_part) {
                return parse_ascii_or_le_u64(&decoded);
            }
        }
    }

    match bytes.len() {
        4 => {
            let mut arr = [0u8; 4];
            arr.copy_from_slice(bytes);
            Some(u32::from_le_bytes(arr) as u64)
        }
        8 => {
            let mut arr = [0u8; 8];
            arr.copy_from_slice(bytes);
            Some(u64::from_le_bytes(arr))
        }
        _ => None,
    }
}

fn parse_ascii_or_le_usize(bytes: &[u8]) -> Option<usize> {
    let v = parse_ascii_or_le_u64(bytes)?;
    if v > usize::MAX as u64 { None } else { Some(v as usize) }
}

fn decode_u128_le(bytes: &[u8]) -> Result<u128> {
    if bytes.len() != 16 {
        return Err(anyhow!("expected 16 bytes for u128, got {}", bytes.len()));
    }
    let mut arr = [0u8; 16];
    arr.copy_from_slice(bytes);
    Ok(u128::from_le_bytes(arr))
}

fn decode_alkane_id_le(bytes: &[u8]) -> Result<SupportAlkaneId> {
    if bytes.len() != 32 {
        return Err(anyhow!("expected 32 bytes for AlkaneId, got {}", bytes.len()));
    }
    let mut block = [0u8; 16];
    let mut tx = [0u8; 16];
    block.copy_from_slice(&bytes[..16]);
    tx.copy_from_slice(&bytes[16..]);
    Ok(SupportAlkaneId { block: u128::from_le_bytes(block), tx: u128::from_le_bytes(tx) })
}

fn encode_alkane_id_le(id: &SupportAlkaneId) -> [u8; 32] {
    let mut out = [0u8; 32];
    out[..16].copy_from_slice(&id.block.to_le_bytes());
    out[16..].copy_from_slice(&id.tx.to_le_bytes());
    out
}

fn decode_versioned_payload(bytes: &[u8]) -> Result<Vec<u8>> {
    if let Ok(s) = std::str::from_utf8(bytes) {
        if let Some((_height, hex_part)) = s.split_once(':') {
            let raw = hex::decode(hex_part)
                .map_err(|e| anyhow!("hex decode versioned payload '{hex_part}': {e}"))?;
            return Ok(raw);
        }
    }
    Ok(bytes.to_vec())
}

struct VersionedPointer<'a> {
    sdb: &'a SDB,
    label: Option<&'a str>,
    base: Vec<u8>,
}

impl<'a> VersionedPointer<'a> {
    fn new(sdb: &'a SDB, base: Vec<u8>) -> Self {
        Self { sdb, label: None, base }
    }

    fn with_label(mut self, label: Option<&'a str>) -> Self {
        self.label = label;
        self
    }

    fn get(&self) -> Result<Option<Vec<u8>>> {
        self.get_with_depth(&self.base, 0)
    }

    fn len(&self) -> Result<Option<usize>> {
        self.length_with_depth(&self.base, 0)
    }

    fn get_index(&self, idx: u64) -> Result<Option<Vec<u8>>> {
        let mut key = Vec::with_capacity(self.base.len() + 1 + 20);
        key.extend_from_slice(&self.base);
        key.push(b'/');
        key.extend_from_slice(idx.to_string().as_bytes());
        self.get_with_depth(&key, 0)
    }

    fn get_with_depth(&self, base: &[u8], depth: u8) -> Result<Option<Vec<u8>>> {
        if depth > 2 {
            return Ok(None);
        }

        if let Some(bytes) = self.get_key(base)? {
            return Ok(Some(bytes));
        }

        let len = match self.length_with_depth(base, depth)? {
            Some(len) => len,
            None => return Ok(None),
        };
        if len == 0 {
            return Ok(None);
        }
        let idx = len.saturating_sub(1);

        let mut key = Vec::with_capacity(base.len() + 1 + 20);
        key.extend_from_slice(base);
        key.push(b'/');
        key.extend_from_slice(idx.to_string().as_bytes());

        if let Some(bytes) = self.get_key(&key)? {
            return Ok(Some(bytes));
        }
        if depth >= 2 {
            return Ok(None);
        }
        self.get_with_depth(&key, depth + 1)
    }

    fn length_with_depth(&self, base: &[u8], depth: u8) -> Result<Option<usize>> {
        let mut length_key = Vec::with_capacity(base.len() + 7);
        length_key.extend_from_slice(base);
        length_key.extend_from_slice(b"/length");

        if let Some(bytes) = self.get_key(&length_key)? {
            return Ok(parse_ascii_or_le_usize(&bytes));
        }
        if depth >= 2 {
            return Ok(None);
        }
        let bytes = self.get_with_depth(&length_key, depth + 1)?;
        Ok(bytes.as_deref().and_then(parse_ascii_or_le_usize))
    }

    fn get_key(&self, key: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(label) = self.label else {
            return self.sdb.get(key);
        };

        let mut with = Vec::with_capacity(label.len() + 3 + key.len());
        with.extend_from_slice(label.as_bytes());
        with.extend_from_slice(b"://");
        with.extend_from_slice(key);
        self.sdb.get(with)
    }
}

pub struct MetashrewAdapter {
    label: Option<String>,
}

pub trait FromLeBytes<const N: usize>: Sized {
    fn from_le_bytes(bytes: [u8; N]) -> Self;
}

impl FromLeBytes<4> for u32 {
    fn from_le_bytes(bytes: [u8; 4]) -> Self {
        u32::from_le_bytes(bytes)
    }
}

impl FromLeBytes<8> for u64 {
    fn from_le_bytes(bytes: [u8; 8]) -> Self {
        u64::from_le_bytes(bytes)
    }
}

impl FromLeBytes<16> for u128 {
    fn from_le_bytes(bytes: [u8; 16]) -> Self {
        u128::from_le_bytes(bytes)
    }
}

impl MetashrewAdapter {
    pub fn new(label: Option<String>) -> MetashrewAdapter {
        let norm_label = label.and_then(|s| {
            let trimmed = s.trim().to_string();
            if trimmed.is_empty() { None } else { Some(trimmed) }
        });
        MetashrewAdapter { label: norm_label }
    }

    fn next_prefix(&self, mut p: Vec<u8>) -> Option<Vec<u8>> {
        for i in (0..p.len()).rev() {
            if p[i] != 0xff {
                p[i] += 1;
                p.truncate(i + 1);
                return Some(p);
            }
        }
        None
    }

    fn apply_label(&self, key: Vec<u8>) -> Vec<u8> {
        let suffix = b"://";

        match &self.label {
            Some(label) => {
                let mut result: Vec<u8> = vec![];
                result.extend(label.as_str().as_bytes());
                result.extend(suffix);
                result.extend(key);
                result
            }
            None => key.clone(),
        }
    }

    fn versioned_pointer<'a>(&'a self, sdb: &'a SDB, base: Vec<u8>) -> VersionedPointer<'a> {
        VersionedPointer::new(sdb, base).with_label(self.label.as_deref())
    }

    fn outpoint_balance_prefix(&self, txid: &Txid, vout: u32) -> Vec<u8> {
        let op = OutPoint::new(*txid, vout);
        let outpoint = serialize(&op);

        let mut base = b"/runes/proto/1/byoutpoint/".to_vec();
        base.extend_from_slice(&outpoint);
        base
    }

    fn read_uint_key<const N: usize, T>(&self, key: Vec<u8>) -> Result<T>
    where
        T: FromLeBytes<N>,
    {
        let metashrew_sdb = get_metashrew_sdb();

        let bytes = metashrew_sdb
            .get(key)?
            .ok_or_else(|| anyhow!("ESPO ERROR: failed to find metashrew key"))?;

        let arr: [u8; N] = bytes
            .as_slice()
            .try_into()
            .map_err(|_| anyhow!("ESPO ERROR: Expected {} bytes, got {}", N, bytes.len()))?;

        Ok(T::from_le_bytes(arr))
    }

    fn load_wasm_inner(
        &self,
        db: &SDB,
        block: u128,
        tx: u128,
        seen: &mut HashSet<(u128, u128)>,
        hops: usize,
    ) -> Result<Option<(Vec<u8>, SupportAlkaneId)>> {
        const MAX_HOPS: usize = 64;
        if hops > MAX_HOPS {
            return Err(anyhow!("alias chain too deep (possible cycle)"));
        }
        if !seen.insert((block, tx)) {
            return Err(anyhow!("alias cycle detected at ({block}, {tx})"));
        }

        // Build candidate keys (LE only, with optional /0..3 suffixes)
        let mut base_le = b"/alkanes/".to_vec();
        base_le.extend_from_slice(&block.to_le_bytes());
        base_le.extend_from_slice(&tx.to_le_bytes());

        let mut candidate_keys: Vec<Vec<u8>> = Vec::new();
        candidate_keys.push(base_le.clone());
        for idx in 0u8..=3u8 {
            let mut k = base_le.clone();
            k.push(b'/');
            k.push(b'0' + idx);
            candidate_keys.push(k);
        }

        let mut last_err: Option<anyhow::Error> = None;
        for key in candidate_keys {
            if let Some(raw) = self.versioned_pointer(db, key).get()? {
                if raw.is_empty() {
                    continue;
                }

                // direct alias payload
                if raw.len() == 32 {
                    let alias = SupportAlkaneId::try_from(raw.clone())
                        .map_err(|e| anyhow!("decode alkane alias for ({block}, {tx}): {e}"))?;
                    return self.load_wasm_inner(db, alias.block, alias.tx, seen, hops + 1);
                }

                // pointer string like "height:HEX"
                let mut payload = raw.to_vec();
                if let Ok(s) = std::str::from_utf8(&raw) {
                    if let Some((_h, hex_part)) = s.split_once(':') {
                        if let Ok(decoded) = hex::decode(hex_part) {
                            payload = decoded;
                        }
                    }
                }

                if payload.len() == 32 {
                    if let Ok(alias) = SupportAlkaneId::try_from(payload.clone()) {
                        return self.load_wasm_inner(db, alias.block, alias.tx, seen, hops + 1);
                    }
                }

                match gz::decompress(payload.clone()) {
                    Ok(bytes) => {
                        return Ok(Some((bytes, SupportAlkaneId { block, tx })));
                    }
                    Err(e) => {
                        last_err = Some(anyhow!(e));
                        continue;
                    }
                }
            }
        }

        if let Some(e) = last_err {
            return Err(anyhow!("decompress alkane wasm payload from metashrew: {e}"));
        }

        Ok(None)
    }

    pub fn get_alkane_wasm_bytes_with_db(
        &self,
        db: &SDB,
        alkane: &SchemaAlkaneId,
    ) -> Result<Option<(Vec<u8>, SchemaAlkaneId)>> {
        let mut seen = HashSet::new();
        let res =
            self.load_wasm_inner(db, alkane.block as u128, alkane.tx as u128, &mut seen, 0)?;
        if let Some((bytes, sid)) = res {
            let block: u32 = sid
                .block
                .try_into()
                .map_err(|_| anyhow!("factory alkane block does not fit into u32"))?;
            let tx: u64 = sid
                .tx
                .try_into()
                .map_err(|_| anyhow!("factory alkane tx does not fit into u64"))?;
            Ok(Some((bytes, SchemaAlkaneId { block, tx })))
        } else {
            Ok(None)
        }
    }

    pub fn get_alkane_wasm_bytes(
        &self,
        alkane: &SchemaAlkaneId,
    ) -> Result<Option<(Vec<u8>, SchemaAlkaneId)>> {
        let db = get_metashrew_sdb();
        db.catch_up_now().context("metashrew catch_up before wasm fetch")?;
        self.get_alkane_wasm_bytes_with_db(db.as_ref(), alkane)
    }

    pub fn get_alkanes_tip_height(&self) -> Result<u32> {
        static LAST_LOGGED_HEIGHT: AtomicU32 = AtomicU32::new(u32::MAX);
        let height_key = b"__INTERNAL/height".to_vec();

        match self.read_uint_key::<4, u32>(height_key) {
            Ok(height) => {
                let prev = LAST_LOGGED_HEIGHT.load(Ordering::Relaxed);
                if prev != height {
                    eprintln!("[metashrew] indexed height: {}", height);
                    LAST_LOGGED_HEIGHT.store(height, Ordering::Relaxed);
                }
                Ok(height)
            }
            Err(_) => Ok(0),
        }
    }

    /// Fetch all traces for a txid directly from the secondary DB, without needing block height.
    pub fn traces_for_tx(&self, txid: &Txid) -> Result<Vec<PartialEspoTrace>> {
        let db = get_metashrew_sdb();
        self.traces_for_tx_with_db(db.as_ref(), txid)
    }

    pub fn traces_for_tx_with_db(&self, db: &SDB, txid: &Txid) -> Result<Vec<PartialEspoTrace>> {
        db.catch_up_now().context("metashrew catch_up before scanning traces_for_tx")?;

        let tx_be = txid.to_byte_array().to_vec();
        let mut tx_le = tx_be.clone();
        tx_le.reverse();

        let mut traces_by_outpoint: HashMap<Vec<u8>, HashMap<u64, AlkanesTrace>> = HashMap::new();
        let mut events_by_outpoint: HashMap<
            Vec<u8>,
            HashMap<u64, BTreeMap<u64, AlkanesTraceEvent>>,
        > = HashMap::new();

        let mut scan_prefix = |tx_bytes: &[u8], tx_bytes_are_be: bool| -> Result<()> {
            let mut prefix = b"/trace/".to_vec();
            prefix.extend_from_slice(tx_bytes);
            let prefix = self.apply_label(prefix);

            let mut ro = ReadOptions::default();
            if let Some(ub) = self.next_prefix(prefix.clone()) {
                ro.set_iterate_upper_bound(ub);
            }
            ro.set_total_order_seek(true);

            let mut tx_outpoint = tx_bytes.to_vec();
            if tx_bytes_are_be {
                tx_outpoint.reverse();
            }

            let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
            while let Some(Ok((k, v))) = it.next() {
                if !k.starts_with(&prefix) {
                    break;
                }

                let suffix = &k[prefix.len()..];
                if suffix.len() < 4 {
                    continue;
                }
                let vout_le = &suffix[..4];
                let rest = &suffix[4..];

                let mut outpoint = Vec::with_capacity(tx_outpoint.len() + 4);
                outpoint.extend_from_slice(&tx_outpoint);
                outpoint.extend_from_slice(vout_le);

                if rest.is_empty() {
                    if let Some(trace) = decode_trace_blob(&v) {
                        traces_by_outpoint.entry(outpoint).or_default().insert(0, trace);
                    }
                    continue;
                }

                if rest[0] != b'/' {
                    continue;
                }
                let remainder = &rest[1..];
                if remainder.is_empty() {
                    continue;
                }
                if remainder == b"length" {
                    continue;
                }

                let segments: Vec<&[u8]> = remainder.split(|b| *b == b'/').collect();
                if segments.is_empty() || segments.iter().any(|s| *s == b"length") {
                    continue;
                }

                if segments.len() == 1 {
                    let idx = parse_ascii_or_le_u64(segments[0]).unwrap_or(0);
                    if let Some(event) = decode_trace_event_blob(&v) {
                        let events = events_by_outpoint
                            .entry(outpoint)
                            .or_default()
                            .entry(0)
                            .or_insert_with(BTreeMap::new);
                        events.entry(idx).or_insert(event);
                        continue;
                    }
                    if let Some(trace) = decode_trace_blob(&v) {
                        traces_by_outpoint.entry(outpoint).or_default().insert(idx, trace);
                    }
                    continue;
                }

                let trace_idx = parse_ascii_or_le_u64(segments[0]).unwrap_or(0);
                let Some(event_idx) = parse_ascii_or_le_u64(segments[1]) else {
                    continue;
                };

                if let Some(event) = decode_trace_event_blob(&v) {
                    let events = events_by_outpoint
                        .entry(outpoint)
                        .or_default()
                        .entry(trace_idx)
                        .or_insert_with(BTreeMap::new);
                    events.entry(event_idx).or_insert(event);
                    continue;
                }

                if let Some(trace) = decode_trace_blob(&v) {
                    traces_by_outpoint.entry(outpoint).or_default().insert(trace_idx, trace);
                }
            }

            Ok(())
        };

        scan_prefix(&tx_be, true)?;
        if tx_le != tx_be {
            scan_prefix(&tx_le, false)?;
        }

        for (outpoint, trace_map) in events_by_outpoint {
            let traces_entry = traces_by_outpoint.entry(outpoint).or_default();
            for (trace_idx, events) in trace_map {
                if events.is_empty() {
                    continue;
                }
                traces_entry.entry(trace_idx).or_insert_with(|| {
                    let evs = events.into_iter().map(|(_, ev)| ev).collect();
                    AlkanesTrace { events: evs }
                });
            }
        }

        let mut out: Vec<PartialEspoTrace> = Vec::new();
        for (outpoint, traces) in traces_by_outpoint {
            if outpoint.len() < 36 {
                continue;
            }
            let mut trace_key = b"/trace/".to_vec();
            trace_key.extend_from_slice(&outpoint);
            let trace = self
                .versioned_pointer(db, trace_key)
                .get()?
                .and_then(|v| decode_trace_blob(&v))
                .or_else(|| traces.keys().max().and_then(|idx| traces.get(idx)).cloned())
                .or_else(|| traces.values().next().cloned());
            if let Some(trace) = trace {
                out.push(PartialEspoTrace { protobuf_trace: trace, outpoint });
            }
        }

        Ok(out)
    }

    pub fn get_reserves_for_alkane_with_db(
        &self,
        db: &SDB,
        who_alkane: &SchemaAlkaneId,
        what_alkane: &SchemaAlkaneId,
        height: Option<u64>,
    ) -> Result<Option<u128>> {
        let u128_to_le16 = |x: u128| x.to_le_bytes();

        let enc_alkaneid_raw32 = |id: &SchemaAlkaneId| {
            let mut out = [0u8; 32];
            out[..16].copy_from_slice(&u128_to_le16(id.block as u128));
            out[16..].copy_from_slice(&u128_to_le16(id.tx as u128));
            out
        };

        let balance_prefix = |what: &SchemaAlkaneId, who: &SchemaAlkaneId| {
            let what32 = enc_alkaneid_raw32(what);
            let who32 = enc_alkaneid_raw32(who);
            let mut v = Vec::with_capacity(9 + 32 + 10 + 32);
            v.extend_from_slice(b"/alkanes/");
            v.extend_from_slice(&what32);
            v.extend_from_slice(b"/balances/");
            v.extend_from_slice(&who32);
            v
        };

        let prefix = balance_prefix(what_alkane, who_alkane);
        let pointer = self.versioned_pointer(db, prefix.clone());

        let parse_entry = |entry_bytes: &[u8]| -> Result<(u64, u128)> {
            let entry_str = std::str::from_utf8(entry_bytes)
                .map_err(|e| anyhow!("utf8 decode balance entry: {e}"))?;
            let (height_str, hex_part) =
                entry_str.split_once(':').ok_or_else(|| anyhow!("balance entry missing ':'"))?;

            let updated_height: u64 = height_str
                .parse()
                .map_err(|e| anyhow!("parse balance height '{height_str}': {e}"))?;

            let raw_balance = hex::decode(hex_part)
                .map_err(|e| anyhow!("hex decode balance payload '{hex_part}': {e}"))?;
            if raw_balance.len() != 16 {
                return Err(anyhow!(
                    "balance payload length {}, expected 16 bytes",
                    raw_balance.len()
                ));
            }
            let mut bal_bytes = [0u8; 16];
            bal_bytes.copy_from_slice(&raw_balance);

            Ok((updated_height, u128::from_le_bytes(bal_bytes)))
        };

        let read_entry_at = |idx: u64| -> Result<Option<(u64, u128)>> {
            let entry_bytes = match pointer.get_index(idx)? {
                Some(bytes) => bytes,
                None => return Ok(None),
            };
            parse_entry(&entry_bytes).map(Some)
        };

        let Some(target_height) = height else {
            let entry_bytes = match pointer.get()? {
                Some(bytes) => bytes,
                None => return Ok(None),
            };
            let (_height, bal) = parse_entry(&entry_bytes)?;
            return Ok(Some(bal));
        };

        let Some(last_idx) =
            pointer.len()?.and_then(|len| len.checked_sub(1)).map(|len| len as u64)
        else {
            return Ok(None);
        };

        if let Some(entry_bytes) = pointer.get()? {
            let (latest_height, latest_bal) = parse_entry(&entry_bytes)?;
            if latest_height <= target_height {
                return Ok(Some(latest_bal));
            }
        }

        let mut low = 0u64;
        let mut high = last_idx;
        let mut best: Option<(u64, u128)> = None;

        while low <= high {
            let mid = low + (high - low) / 2;
            let Some((entry_height, entry_balance)) = read_entry_at(mid)? else {
                return Ok(None);
            };

            if entry_height <= target_height {
                best = Some((entry_height, entry_balance));
                if mid == u64::MAX {
                    break;
                }
                low = mid + 1;
            } else {
                if mid == 0 {
                    break;
                }
                high = mid - 1;
            }
        }

        Ok(best.map(|(_, bal)| bal))
    }

    pub fn get_reserves_for_alkane(
        &self,
        who_alkane: &SchemaAlkaneId,
        what_alkane: &SchemaAlkaneId,
        height: Option<u64>,
    ) -> Result<Option<u128>> {
        let db = get_metashrew_sdb();
        self.get_reserves_for_alkane_with_db(db.as_ref(), who_alkane, what_alkane, height)
    }

    pub fn get_outpoint_alkane_balances_with_db(
        &self,
        db: &SDB,
        txid: &Txid,
        vout: u32,
    ) -> Result<Vec<(SupportAlkaneId, u128)>> {
        let base = self.outpoint_balance_prefix(txid, vout);

        let mut runes_base = base.clone();
        runes_base.extend_from_slice(b"/runes");
        let mut balances_base = base.clone();
        balances_base.extend_from_slice(b"/balances");

        let runes_base_ptr = self.versioned_pointer(db, runes_base.clone());
        let balances_base_ptr = self.versioned_pointer(db, balances_base.clone());

        let runes_versions = runes_base_ptr.len()?.unwrap_or(0);
        let balances_versions = balances_base_ptr.len()?.unwrap_or(0);

        if runes_versions == 0 || balances_versions == 0 {
            let mut runes_length_key = runes_base.clone();
            runes_length_key.extend_from_slice(b"/length");
            let mut balances_length_key = balances_base.clone();
            balances_length_key.extend_from_slice(b"/length");

            return Ok(Vec::new());
        }

        let runes_version_idx = runes_versions.saturating_sub(1);
        let balances_version_idx = balances_versions.saturating_sub(1);

        let mut runes_list_base = runes_base.clone();
        runes_list_base.push(b'/');
        runes_list_base.extend_from_slice(runes_version_idx.to_string().as_bytes());

        let mut balances_list_base = balances_base.clone();
        balances_list_base.push(b'/');
        balances_list_base.extend_from_slice(balances_version_idx.to_string().as_bytes());

        let runes_ptr = self.versioned_pointer(db, runes_list_base);
        let balances_ptr = self.versioned_pointer(db, balances_list_base);

        let runes_len = runes_ptr.len()?.unwrap_or(0);
        let balances_len = balances_ptr.len()?.unwrap_or(0);
        if runes_len == 0 || balances_len == 0 {
            return Ok(Vec::new());
        }
        if balances_len < runes_len {
            return Err(anyhow!(
                "outpoint balance array missing balances: runes_len={} balances_len={}",
                runes_len,
                balances_len
            ));
        }
        if balances_len > runes_len {
            eprintln!(
                "[metashrew] outpoint balance arrays: extra balances ignored (runes_len={} balances_len={})",
                runes_len, balances_len
            );
        }

        let mut out = Vec::with_capacity(runes_len);
        for idx in 0..runes_len {
            let rune_bytes =
                runes_ptr.get_index(idx as u64)?.ok_or_else(|| anyhow!("missing runes/{idx}"))?;
            let balance_bytes = balances_ptr
                .get_index(idx as u64)?
                .ok_or_else(|| anyhow!("missing balances/{idx}"))?;

            let rune_payload = decode_versioned_payload(&rune_bytes)?;
            let balance_payload = decode_versioned_payload(&balance_bytes)?;

            let id = decode_alkane_id_le(&rune_payload)?;
            let balance = decode_u128_le(&balance_payload)?;
            out.push((id, balance));
        }

        Ok(out)
    }

    pub fn get_outpoint_alkane_balances(
        &self,
        txid: &Txid,
        vout: u32,
    ) -> Result<Vec<(SupportAlkaneId, u128)>> {
        let db = get_metashrew_sdb();
        db.catch_up_now().context("metashrew catch_up before outpoint balances")?;
        self.get_outpoint_alkane_balances_with_db(db.as_ref(), txid, vout)
    }

    pub fn get_outpoint_alkane_balance_for_id_with_db(
        &self,
        db: &SDB,
        txid: &Txid,
        vout: u32,
        id: &SupportAlkaneId,
    ) -> Result<Option<u128>> {
        let mut key = self.outpoint_balance_prefix(txid, vout);
        key.extend_from_slice(b"/id_to_balance/");
        key.extend_from_slice(&encode_alkane_id_le(id));

        let pointer = self.versioned_pointer(db, key);
        let Some(bytes) = pointer.get()? else {
            return Ok(None);
        };
        let payload = decode_versioned_payload(&bytes)?;
        Ok(Some(decode_u128_le(&payload)?))
    }

    pub fn traces_for_block_as_prost(&self, block: u64) -> Result<Vec<PartialEspoTrace>> {
        let db = get_metashrew_sdb();
        self.traces_for_block_as_prost_with_db(db.as_ref(), block)
    }

    pub fn traces_for_block_as_prost_with_db(
        &self,
        db: &SDB,
        block: u64,
    ) -> Result<Vec<PartialEspoTrace>> {
        // Ensure the secondary view is fresh before scanning traces.
        db.catch_up_now().context("metashrew catch_up before scanning traces")?;
        let mut base = b"/trace/".to_vec();
        base.extend_from_slice(&block.to_le_bytes());

        let list_len = self.versioned_pointer(db, base.clone()).len()?.unwrap_or(0);

        let mut missing_pointer_values = 0usize;
        let mut missing_trace_blobs = 0usize;
        let mut bad_pointers = 0usize;
        let mut final_traces: Vec<PartialEspoTrace> = Vec::new();
        let mut seen_outpoints: HashSet<Vec<u8>> = HashSet::new();

        for idx in 0..list_len {
            let mut pointer_key = Vec::with_capacity(base.len() + 1 + 20);
            pointer_key.extend_from_slice(&base);
            pointer_key.push(b'/');
            pointer_key.extend_from_slice(idx.to_string().as_bytes());

            let pointer_value = match self.versioned_pointer(db, pointer_key).get()? {
                Some(bytes) => bytes,
                None => {
                    missing_pointer_values = missing_pointer_values.saturating_add(1);
                    continue;
                }
            };

            let outpoint_bytes = if let Ok(s) = std::str::from_utf8(&pointer_value) {
                if let Some((_block_str, hex_part)) = s.split_once(':') {
                    hex::decode(hex_part).ok()
                } else {
                    None
                }
            } else {
                None
            }
            .or_else(|| {
                if pointer_value.len() >= 36 { Some(pointer_value[..36].to_vec()) } else { None }
            });

            let Some(mut outpoint) = outpoint_bytes else {
                bad_pointers = bad_pointers.saturating_add(1);
                continue;
            };
            if outpoint.len() > 36 {
                outpoint.truncate(36);
            }
            if !seen_outpoints.insert(outpoint.clone()) {
                continue;
            }

            let mut trace_key = Vec::with_capacity(7 + outpoint.len());
            trace_key.extend_from_slice(b"/trace/");
            trace_key.extend_from_slice(&outpoint);
            match self.versioned_pointer(db, trace_key).get()? {
                Some(bytes) => {
                    if let Some(protobuf_trace) = decode_trace_blob(&bytes) {
                        final_traces.push(PartialEspoTrace { protobuf_trace, outpoint });
                    } else {
                        missing_trace_blobs = missing_trace_blobs.saturating_add(1);
                    }
                }
                None => {
                    missing_trace_blobs = missing_trace_blobs.saturating_add(1);
                }
            }
        }
        if final_traces.is_empty() {
            eprintln!("[metashrew] block {block}: pointers={list_len} traces=0");
        }

        if missing_trace_blobs > 0 || missing_pointer_values > 0 || bad_pointers > 0 {
            let mut reason = String::new();
            if missing_trace_blobs > 0 {
                reason.push_str(&format!("missing_trace_blobs={missing_trace_blobs}"));
            }
            if missing_pointer_values > 0 {
                if !reason.is_empty() {
                    reason.push_str(", ");
                }
                reason.push_str(&format!("missing_pointers={missing_pointer_values}"));
            }
            if bad_pointers > 0 {
                if !reason.is_empty() {
                    reason.push_str(", ");
                }
                reason.push_str(&format!("bad_pointers={bad_pointers}"));
            }
            let warning =
                format!("[metashrew] warn: block {block}: trace index looks incomplete ({reason})");
            eprintln!("{warning}");
            if is_strict_mode() {
                panic!("{warning}");
            }
        }

        Ok(final_traces)
    }
}
