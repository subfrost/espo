use crate::alkanes::trace::PartialEspoTrace;
use crate::config::{get_block_source, get_metashrew_sdb, is_debug_mode};
use crate::core::blockfetcher::BlockSource;
use crate::schemas::SchemaAlkaneId;
use alkanes_cli_common::alkanes_pb::{AlkanesTrace, AlkanesTraceEvent};
use alkanes_support::gz;
use alkanes_support::id::AlkaneId as SupportAlkaneId;
use anyhow::{Context, Result, anyhow};
use bitcoin::{OutPoint, Transaction};
use bitcoin::Txid;
use bitcoin::hashes::Hash;
use ordinals::{Artifact, Runestone};
use prost::Message;
use protorune_support::protostone::Protostone;
use metashrew_support::utils::consensus_encode;
use rocksdb::{Direction, IteratorMode, ReadOptions};
use std::collections::{HashMap, HashSet};

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
        db: &rocksdb::DB,
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

        // Build candidate keys (LE + BE, with optional /0..3 suffixes)
        let mut base_le = b"/alkanes/".to_vec();
        base_le.extend_from_slice(&block.to_le_bytes());
        base_le.extend_from_slice(&tx.to_le_bytes());
        let mut base_be = b"/alkanes/".to_vec();
        base_be.extend_from_slice(&block.to_be_bytes());
        base_be.extend_from_slice(&tx.to_be_bytes());

        let mut candidate_keys: Vec<Vec<u8>> = Vec::new();
        for base in [base_le, base_be] {
            candidate_keys.push(self.apply_label(base.clone()));
            for idx in 0u8..=3u8 {
                let mut k = base.clone();
                k.push(b'/');
                k.push(b'0' + idx);
                candidate_keys.push(self.apply_label(k));
            }
        }

        let mut last_err: Option<anyhow::Error> = None;
        for key in candidate_keys {
            if let Some(raw) = db.get(&key)? {
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
        db: &rocksdb::DB,
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
        self.get_alkane_wasm_bytes_with_db(db.as_db(), alkane)
    }
    pub fn get_alkanes_tip_height(&self) -> Result<u32> {
        let tip_height_prefix: Vec<u8> = self.apply_label(b"/__INTERNAL/tip-height".to_vec());

        self.read_uint_key::<4, u32>(tip_height_prefix)
    }

    /// Fetch all traces for a txid directly from the secondary DB, without needing block height.
    pub fn traces_for_tx(&self, txid: &Txid) -> Result<Vec<PartialEspoTrace>> {
        let db = get_metashrew_sdb();
        db.catch_up_now().context("metashrew catch_up before scanning traces_for_tx")?;

        let tx_le = txid.to_byte_array().to_vec();

        let parse_ascii_len = |bytes: &[u8]| -> Option<usize> {
            std::str::from_utf8(bytes).ok()?.parse::<usize>().ok()
        };

        let mut prefix = b"/trace/".to_vec();
        prefix.extend_from_slice(&tx_le);
        let prefix = self.apply_label(prefix);

        let mut ro = ReadOptions::default();
        if let Some(ub) = self.next_prefix(prefix.clone()) {
            ro.set_iterate_upper_bound(ub);
        }
        ro.set_total_order_seek(true);

        let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
        let mut outpoint_lengths: HashMap<Vec<u8>, usize> = HashMap::new();

        while let Some(Ok((k, v))) = it.next() {
            if !k.starts_with(&prefix) {
                break;
            }

            let suffix = &k[prefix.len()..];
            if suffix.len() < 5 || suffix[4] != b'/' {
                continue;
            }
            let tail = &suffix[5..];
            if tail != b"length" {
                continue;
            }

            let mut outpoint = Vec::with_capacity(36);
            outpoint.extend_from_slice(&tx_le);
            outpoint.extend_from_slice(&suffix[..4]);

            if let Some(len) = parse_ascii_len(&v) {
                outpoint_lengths.insert(outpoint, len);
            }
        }

        let mut out: Vec<PartialEspoTrace> = Vec::with_capacity(outpoint_lengths.len());
        for (outpoint, len) in outpoint_lengths {
            if len == 0 {
                continue;
            }
            let idx = len.saturating_sub(1);
            let mut key = Vec::with_capacity(7 + outpoint.len() + 1 + 20);
            key.extend_from_slice(b"/trace/");
            key.extend_from_slice(&outpoint);
            key.push(b'/');
            key.extend_from_slice(idx.to_string().as_bytes());
            let key = self.apply_label(key);

            let Some(bytes) = db.get(&key)? else { continue };
            let Some(trace) = decode_trace_blob(&bytes) else { continue };
            out.push(PartialEspoTrace { protobuf_trace: trace, outpoint });
        }

        Ok(out)
    }

    pub fn get_reserves_for_alkane_with_db(
        &self,
        db: &rocksdb::DB,
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
            self.apply_label(v)
        };

        let prefix = balance_prefix(what_alkane, who_alkane);

        let mut length_key = prefix.clone();
        length_key.extend_from_slice(b"/length");

        let length_bytes = match db.get(&length_key)? {
            Some(bytes) => bytes,
            None => return Ok(None),
        };

        let length_str = std::str::from_utf8(&length_bytes)
            .map_err(|e| anyhow!("utf8 decode balances length: {e}"))?;
        let length: u64 = length_str
            .parse()
            .map_err(|e| anyhow!("parse balances length '{length_str}': {e}"))?;

        let Some(last_idx) = length.checked_sub(1) else { return Ok(None) };

        let entry_key_for_index = |idx: u64| {
            let mut entry_key = prefix.clone();
            entry_key.push(b'/');
            entry_key.extend_from_slice(idx.to_string().as_bytes());
            entry_key
        };

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
            let entry_key = entry_key_for_index(idx);
            let entry_bytes = match db.get(&entry_key)? {
                Some(bytes) => bytes,
                None => return Ok(None),
            };
            parse_entry(&entry_bytes).map(Some)
        };

        let Some(target_height) = height else {
            return read_entry_at(last_idx).map(|opt| opt.map(|(_, bal)| bal));
        };

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
        self.get_reserves_for_alkane_with_db(db.as_db(), who_alkane, what_alkane, height)
    }

    pub fn traces_for_block_as_prost(&self, block: u64) -> Result<Vec<PartialEspoTrace>> {
        let trace_block_prefix = |blk: u64| {
            let mut v = Vec::with_capacity(7 + 8 + 1);
            v.extend_from_slice(b"/trace/");
            v.extend_from_slice(&blk.to_le_bytes());
            v.push(b'/');
            self.apply_label(v)
        };

        let next_prefix = |mut p: Vec<u8>| -> Option<Vec<u8>> {
            for i in (0..p.len()).rev() {
                if p[i] != 0xff {
                    p[i] += 1;
                    p.truncate(i + 1);
                    return Some(p);
                }
            }
            None
        };

        let parse_ascii_len = |bytes: &[u8]| -> Option<usize> {
            std::str::from_utf8(bytes).ok()?.parse::<usize>().ok()
        };

        let db = get_metashrew_sdb();
        // Ensure the secondary view is fresh before scanning traces.
        db.catch_up_now().context("metashrew catch_up before scanning traces")?;
        let prefix = trace_block_prefix(block);

        let mut ro = ReadOptions::default();
        if let Some(ub) = next_prefix(prefix.clone()) {
            ro.set_iterate_upper_bound(ub);
        }
        ro.set_total_order_seek(true);

        let mut it = db.iterator_opt(IteratorMode::From(&prefix, Direction::Forward), ro);
        let mut pointer_lengths: HashMap<Vec<u8>, usize> = HashMap::new();
        let mut pointer_idxs_seen: HashSet<Vec<u8>> = HashSet::new();
        let mut bad_lengths = 0usize;
        let mut legacy_pointer_values: HashMap<Vec<u8>, Vec<u8>> = HashMap::new();

        while let Some(Ok((k, v))) = it.next() {
            if !k.starts_with(&prefix) {
                break;
            }

            let suffix = &k[prefix.len()..];
            if suffix == b"length" {
                continue;
            }

            // Keys under /trace/<height>/ are per-index pointers, and each pointer
            // is versioned with /length and /<ver> (reorg-safe).
            let parts: Vec<&[u8]> = suffix.split(|b| *b == b'/').collect();
            if parts.is_empty() {
                continue;
            }

            let idx = parts[0];
            pointer_idxs_seen.insert(idx.to_vec());
            if parts.len() == 1 {
                // Legacy layout: /trace/<height>/<idx> => pointer value
                legacy_pointer_values.insert(idx.to_vec(), v.to_vec());
                continue;
            }
            if parts.len() == 2 && parts[1] == b"length" {
                match parse_ascii_len(&v) {
                    Some(len) => {
                        pointer_lengths.insert(idx.to_vec(), len);
                    }
                    None => {
                        bad_lengths = bad_lengths.saturating_add(1);
                    }
                }
                continue;
            }
        }

        let mut missing_pointer_values = 0usize;
        let mut missing_trace_blobs = 0usize;
        let mut final_traces: Vec<PartialEspoTrace> = Vec::new();
        let mut seen_outpoints: HashSet<Vec<u8>> = HashSet::new();

        for (idx, len) in &pointer_lengths {
            if *len == 0 {
                continue;
            }
            // /trace/<height>/<idx>/length is a count; latest pointer lives at len-1.
            let ver = len.saturating_sub(1);
            let mut pointer_key = Vec::with_capacity(prefix.len() + idx.len() + 1 + 20);
            pointer_key.extend_from_slice(&prefix);
            pointer_key.extend_from_slice(idx);
            pointer_key.push(b'/');
            pointer_key.extend_from_slice(ver.to_string().as_bytes());

            let pointer_value = match db.get(&pointer_key)? {
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
                if pointer_value.len() >= 36 {
                    Some(pointer_value[..36].to_vec())
                } else {
                    None
                }
            });

            let Some(mut outpoint) = outpoint_bytes else {
                missing_pointer_values = missing_pointer_values.saturating_add(1);
                continue;
            };
            if outpoint.len() > 36 {
                outpoint.truncate(36);
            }
            if !seen_outpoints.insert(outpoint.clone()) {
                continue;
            }

            // /trace/<outpoint>/length => latest trace version for this outpoint.
            let mut trace_len_key = Vec::with_capacity(7 + outpoint.len() + 7);
            trace_len_key.extend_from_slice(b"/trace/");
            trace_len_key.extend_from_slice(&outpoint);
            trace_len_key.extend_from_slice(b"/length");
            let trace_len_key = self.apply_label(trace_len_key);
            let trace_len = db
                .get(&trace_len_key)?
                .and_then(|v| parse_ascii_len(&v))
                .unwrap_or(0);
            if trace_len == 0 {
                missing_trace_blobs = missing_trace_blobs.saturating_add(1);
                continue;
            }
            let trace_idx = trace_len.saturating_sub(1);
            let mut trace_key = Vec::with_capacity(7 + outpoint.len() + 1 + 20);
            trace_key.extend_from_slice(b"/trace/");
            trace_key.extend_from_slice(&outpoint);
            trace_key.push(b'/');
            trace_key.extend_from_slice(trace_idx.to_string().as_bytes());
            let trace_key = self.apply_label(trace_key);

            match db.get(&trace_key)? {
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

        for (idx, pointer_value) in legacy_pointer_values {
            if pointer_lengths.contains_key(&idx) {
                continue;
            }
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
                if pointer_value.len() >= 36 {
                    Some(pointer_value[..36].to_vec())
                } else {
                    None
                }
            });

            let Some(mut outpoint) = outpoint_bytes else {
                missing_pointer_values = missing_pointer_values.saturating_add(1);
                continue;
            };
            if outpoint.len() > 36 {
                outpoint.truncate(36);
            }
            if !seen_outpoints.insert(outpoint.clone()) {
                continue;
            }

            let mut trace_len_key = Vec::with_capacity(7 + outpoint.len() + 7);
            trace_len_key.extend_from_slice(b"/trace/");
            trace_len_key.extend_from_slice(&outpoint);
            trace_len_key.extend_from_slice(b"/length");
            let trace_len_key = self.apply_label(trace_len_key);
            let trace_len = db
                .get(&trace_len_key)?
                .and_then(|v| parse_ascii_len(&v))
                .unwrap_or(0);
            if trace_len == 0 {
                missing_trace_blobs = missing_trace_blobs.saturating_add(1);
                continue;
            }
            let trace_idx = trace_len.saturating_sub(1);
            let mut trace_key = Vec::with_capacity(7 + outpoint.len() + 1 + 20);
            trace_key.extend_from_slice(b"/trace/");
            trace_key.extend_from_slice(&outpoint);
            trace_key.push(b'/');
            trace_key.extend_from_slice(trace_idx.to_string().as_bytes());
            let trace_key = self.apply_label(trace_key);

            match db.get(&trace_key)? {
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

        let pointer_len = pointer_lengths.len();
        if final_traces.is_empty() {
            eprintln!(
                "[metashrew] block {block}: pointers={} traces=0",
                pointer_len
            );
        }

        let missing_lengths = pointer_idxs_seen.len().saturating_sub(pointer_len);
        let needs_fallback = missing_trace_blobs > 0
            || missing_pointer_values > 0
            || missing_lengths > 0
            || bad_lengths > 0;

        if needs_fallback {
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
            if missing_lengths > 0 {
                if !reason.is_empty() {
                    reason.push_str(", ");
                }
                reason.push_str(&format!("missing_lengths={missing_lengths}"));
            }
            if bad_lengths > 0 {
                if !reason.is_empty() {
                    reason.push_str(", ");
                }
                reason.push_str(&format!("bad_lengths={bad_lengths}"));
            }
            eprintln!(
                "[metashrew] block {block}: trace index looks incomplete ({reason}); recomputing shadow vouts"
            );

            let block_source = get_block_source();
            let h32: u32 = block
                .try_into()
                .context("block height does not fit into u32 for trace fallback")?;
            let full_block = match block_source.get_block_by_height(h32, h32) {
                Ok(b) => b,
                Err(e) => {
                    eprintln!(
                        "[metashrew] block {block}: fallback failed to fetch block ({e:?}); using indexed traces"
                    );
                    return Ok(final_traces);
                }
            };

            let mut fallback_traces: Vec<PartialEspoTrace> = Vec::new();
            let mut expected_outpoints = 0usize;
            let mut missing_keys = 0usize;
            let mut decode_failures = 0usize;
            let mut parse_failures = 0usize;

            let mut protostone_count = |tx: &Transaction| -> usize {
                let runestone = match Runestone::decipher(tx) {
                    Some(Artifact::Runestone(r)) => r,
                    _ => return 0,
                };
                match Protostone::from_runestone(&runestone) {
                    Ok(protos) => protos.len(),
                    Err(e) => {
                        parse_failures = parse_failures.saturating_add(1);
                        if is_debug_mode() {
                            eprintln!(
                                "[metashrew] block {block}: protostone parse failed txid={} err={e:#}",
                                tx.compute_txid()
                            );
                        }
                        0
                    }
                }
            };

            for tx in &full_block.txdata {
                let count = protostone_count(tx);
                if count == 0 {
                    continue;
                }
                let n_outputs = tx.output.len() as u32;
                let shadow_base = n_outputs.saturating_add(1);
                let txid = tx.compute_txid();
                let outpoint_bytes = |vout: u32| -> Result<Vec<u8>> {
                    let op = OutPoint::new(txid, vout);
                    consensus_encode(&op).map_err(|e| anyhow!("encode outpoint: {e}"))
                };

                for i in 0..count {
                    let vout = shadow_base + i as u32;
                    expected_outpoints = expected_outpoints.saturating_add(1);

                    let outpoint = outpoint_bytes(vout)?;
                    let mut key_base = Vec::with_capacity(7 + outpoint.len());
                    key_base.extend_from_slice(b"/trace/");
                    key_base.extend_from_slice(&outpoint);
                    let key_base = self.apply_label(key_base);

                    let mut candidates: Vec<(u32, AlkanesTrace)> = Vec::new();
                    let mut length_key = key_base.clone();
                    length_key.extend_from_slice(b"/length");
                    let latest_idx = db
                        .get(&length_key)?
                        .and_then(|v| parse_ascii_len(&v))
                        .and_then(|len| len.checked_sub(1));
                    if let Some(idx) = latest_idx {
                        let mut key = key_base.clone();
                        key.push(b'/');
                        key.extend_from_slice(idx.to_string().as_bytes());
                        match db.get(&key)? {
                            Some(bytes) => {
                                if let Some(trace) = decode_trace_blob(&bytes) {
                                    candidates.push((idx as u32, trace));
                                } else {
                                    decode_failures = decode_failures.saturating_add(1);
                                }
                            }
                            None => {}
                        }
                    }

                    let mut uniq: HashMap<Vec<u8>, (u32, AlkanesTrace)> = HashMap::new();
                    for (idx, trace) in candidates {
                        let mut buf = Vec::with_capacity(trace.encoded_len());
                        if trace.encode(&mut buf).is_err() {
                            decode_failures = decode_failures.saturating_add(1);
                            continue;
                        }
                        uniq.entry(buf)
                            .or_insert_with(|| (idx, trace));
                    }

                    if uniq.is_empty() {
                        missing_keys = missing_keys.saturating_add(1);
                        continue;
                    }
                    let mut uniq_traces: Vec<(u32, AlkanesTrace)> =
                        uniq.into_values().collect();
                    uniq_traces.sort_by_key(|(idx, _)| *idx);
                    for (_idx, trace) in uniq_traces {
                        fallback_traces.push(PartialEspoTrace {
                            protobuf_trace: trace,
                            outpoint: outpoint.clone(),
                        });
                    }
                }
            }

            eprintln!(
                "[metashrew] block {block}: fallback traces={} expected_outpoints={} missing_keys={} decode_failures={} parse_failures={}",
                fallback_traces.len(),
                expected_outpoints,
                missing_keys,
                decode_failures,
                parse_failures
            );

            if fallback_traces.is_empty() {
                return Ok(final_traces);
            }

            return Ok(fallback_traces);
        }

        Ok(final_traces)
    }
}
