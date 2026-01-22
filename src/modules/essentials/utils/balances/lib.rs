use super::defs::{EspoTraceType, SignedU128, SignedU128MapExt};
use super::utils::{
    Unallocated, compute_nets, is_op_return, parse_protostones, parse_short_id,
    schema_id_from_parts, transfers_to_sheet, tx_has_op_return, u128_to_u32,
};
use crate::alkanes::trace::{
    EspoBlock, EspoHostFunctionValues, EspoSandshrewLikeTrace, EspoSandshrewLikeTraceEvent,
    EspoSandshrewLikeTraceStatus, EspoTrace,
};
use crate::config::{
    get_electrum_like, get_metashrew, get_metashrew_sdb, get_network, is_strict_mode,
};
use crate::modules::essentials::storage::get_holders_values_encoded;
use crate::modules::essentials::storage::{
    AlkaneBalanceTxEntry, AlkaneTxSummary, BalanceEntry, HolderEntry, HolderId,
    decode_alkane_balance_tx_entries, decode_balances_vec, decode_holders_vec, decode_u128_value,
    encode_u128_value, encode_vec,
    mk_outpoint, spk_to_address_str, build_alkane_balances_key,
};
use crate::modules::essentials::storage::{
    EssentialsProvider, GetMultiValuesParams, GetRawValueParams, GetScanPrefixParams, SetBatchParams,
};
use crate::runtime::mdb::Mdb;
use crate::schemas::{EspoOutpoint, SchemaAlkaneId};
use anyhow::{Context, Result, anyhow};
use bitcoin::block::Header;
use bitcoin::consensus::encode::deserialize;
use bitcoin::{ScriptBuf, Transaction, Txid, hashes::Hash};
use borsh::BorshDeserialize;
use protorune_support::protostone::{Protostone, ProtostoneEdict};
use std::collections::{BTreeMap, HashMap, HashSet};
use std::time::Instant;

fn clean_espo_sandshrew_like_trace(
    trace: &EspoSandshrewLikeTrace,
    host_function_values: &EspoHostFunctionValues,
) -> Option<EspoSandshrewLikeTrace> {
    let mut invokes = 0usize;
    let mut returns = 0usize;
    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(_) => invokes += 1,
            EspoSandshrewLikeTraceEvent::Return(_) => returns += 1,
            EspoSandshrewLikeTraceEvent::Create(_) => {}
        }
    }

    if invokes == returns {
        return Some(trace.clone());
    }
    if returns < invokes {
        return None;
    }

    let (header, coinbase, diesel, fee) = host_function_values;
    let host_values: [&[u8]; 4] = [header, coinbase, diesel, fee];
    let mismatch = returns.saturating_sub(invokes);

    let decode_data = |data: &str| -> Option<Vec<u8>> {
        let trimmed = data.strip_prefix("0x").unwrap_or(data);
        if trimmed.is_empty() {
            return Some(Vec::new());
        }
        hex::decode(trimmed).ok()
    };

    let host_match = |data_bytes: &[u8]| -> bool {
        for host_bytes in host_values.iter() {
            if data_bytes == *host_bytes {
                return true;
            }
        }
        false
    };

    let fuzzy_host_match = |data_bytes: &[u8]| -> bool {
        if data_bytes.len() == 80 && deserialize::<Header>(data_bytes).is_ok() {
            return true;
        }
        if let Ok(tx) = deserialize::<Transaction>(data_bytes) {
            if tx.is_coinbase() {
                return true;
            }
        }
        false
    };

    let attempt_clean = |allow_fuzzy: bool| -> Option<EspoSandshrewLikeTrace> {
        let mut remove_indices: HashSet<usize> = HashSet::new();
        let mut candidate_stack: Vec<usize> = Vec::new();
        let mut total_candidates = 0usize;
        let mut depth: isize = 0;

        for (idx, ev) in trace.events.iter().enumerate() {
            match ev {
                EspoSandshrewLikeTraceEvent::Invoke(_) => {
                    depth += 1;
                }
                EspoSandshrewLikeTraceEvent::Return(ret) => {
                    let mut is_candidate = false;
                    if ret.status == EspoSandshrewLikeTraceStatus::Success
                        && ret.response.alkanes.is_empty()
                        && ret.response.storage.is_empty()
                    {
                        if let Some(data_bytes) = decode_data(&ret.response.data) {
                            if host_match(&data_bytes) {
                                is_candidate = true;
                            } else if allow_fuzzy && fuzzy_host_match(&data_bytes) {
                                is_candidate = true;
                            }
                        }
                    }
                    if is_candidate {
                        total_candidates += 1;
                        candidate_stack.push(idx);
                    }

                    depth -= 1;
                    if depth < 0 {
                        let Some(remove_idx) = candidate_stack.pop() else {
                            return None;
                        };
                        remove_indices.insert(remove_idx);
                        depth += 1;
                    }
                }
                EspoSandshrewLikeTraceEvent::Create(_) => {}
            }
        }

        if total_candidates < mismatch || remove_indices.len() != mismatch {
            return None;
        }

        let mut cleaned_events =
            Vec::with_capacity(trace.events.len().saturating_sub(remove_indices.len()));
        for (idx, ev) in trace.events.iter().enumerate() {
            if !remove_indices.contains(&idx) {
                cleaned_events.push(ev.clone());
            }
        }

        let mut cleaned_invokes = 0usize;
        let mut cleaned_returns = 0usize;
        let mut cleaned_depth: isize = 0;
        for ev in &cleaned_events {
            match ev {
                EspoSandshrewLikeTraceEvent::Invoke(_) => {
                    cleaned_invokes += 1;
                    cleaned_depth += 1;
                }
                EspoSandshrewLikeTraceEvent::Return(_) => {
                    cleaned_returns += 1;
                    cleaned_depth -= 1;
                    if cleaned_depth < 0 {
                        return None;
                    }
                }
                EspoSandshrewLikeTraceEvent::Create(_) => {}
            }
        }
        if cleaned_invokes != cleaned_returns || cleaned_depth != 0 {
            return None;
        }

        Some(EspoSandshrewLikeTrace { outpoint: trace.outpoint.clone(), events: cleaned_events })
    };

    attempt_clean(false).or_else(|| attempt_clean(true))
}

pub(crate) fn accumulate_alkane_balance_deltas(
    trace: &EspoSandshrewLikeTrace,
    _txid: &Txid,
    host_function_values: &EspoHostFunctionValues,
) -> (bool, HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>) {
    let Some(trace) = clean_espo_sandshrew_like_trace(trace, host_function_values) else {
        if is_strict_mode() {
            eprintln!(
                "[balances][strict] dropped trace: failed to clean sandshrew-like events (txid={})",
                _txid
            );
        }
        return (false, HashMap::new());
    };
    if std::env::var_os("ESPO_LOG_HOST_FUNCTION_VALUES").is_some() {
        let (header, coinbase, diesel, fee) = host_function_values;
        eprintln!(
            "[balances] host_function_values header={} coinbase={} diesel={} fee={}",
            hex::encode(header),
            hex::encode(coinbase),
            hex::encode(diesel),
            hex::encode(fee),
        );
    }

    // We treat the trace as a call stack (invoke ... return), and only apply balance
    // changes when a frame returns successfully. This lets us drop an entire subtree
    // of effects if a parent frame fails or is static (reverts all children).
    //
    // Rules implemented:
    // - Normal calls: incoming credits go to `myself`, outgoing debits come from `myself`.
    // - Delegate calls: still credit `myself` for incoming, but the "parent" for both
    //   incoming and outgoing is the nearest NORMAL ancestor frame (skip delegates).
    // - Static calls: ignored completely (no effects, children ignored).
    // - Create events: ignored.
    // - Returned alkanes pay to the nearest normal parent (never to a delegate).
    // - We allow negative deltas here; final balance checks happen later.
    // - Self-token deltas are kept for outflow reporting; balances/holders ignore them later.

    #[derive(Copy, Clone, Eq, PartialEq, Debug)]
    enum FrameKind {
        Normal,
        Delegate,
        Static,
    }

    #[derive(Clone)]
    struct Frame {
        kind: FrameKind,
        owner: SchemaAlkaneId,
        incoming: BTreeMap<SchemaAlkaneId, u128>,
        parent_normal: Option<SchemaAlkaneId>,
        deltas: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
    }

    // Find the nearest NORMAL frame in the current stack (delegates/statics are skipped).
    fn nearest_normal_owner(stack: &[Frame]) -> Option<SchemaAlkaneId> {
        stack.iter().rev().find_map(|frame| {
            if matches!(frame.kind, FrameKind::Normal) {
                Some(frame.owner)
            } else {
                None
            }
        })
    }

    // Add a signed delta for a (owner, token) pair.
    // Self-token deltas are kept for outflow reporting; balances filter them later.
    fn add_delta(
        outflows: &mut HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
        owner: SchemaAlkaneId,
        token: SchemaAlkaneId,
        delta: SignedU128,
    ) {
        if delta.is_zero() {
            return;
        }
        let remove = {
            let entry = outflows.entry(owner).or_default();
            entry.add_signed(token, delta);
            entry.is_empty()
        };
        if remove {
            outflows.remove(&owner);
        }
    }

    // Apply a transfer (amount of token) from -> to into a delta map.
    fn apply_transfer(
        outflows: &mut HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
        from: Option<SchemaAlkaneId>,
        to: Option<SchemaAlkaneId>,
        token: SchemaAlkaneId,
        amount: u128,
    ) {
        if amount == 0 {
            return;
        }
        if let Some(owner) = from {
            add_delta(outflows, owner, token, SignedU128::negative(amount));
        }
        if let Some(owner) = to {
            add_delta(outflows, owner, token, SignedU128::positive(amount));
        }
    }

    // Merge a child's delta map into its parent (used to drop effects on failure/static).
    fn merge_deltas(
        target: &mut HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
        child: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>>,
    ) {
        for (owner, per_token) in child {
            if per_token.is_empty() {
                continue;
            }
            let remove = {
                let entry = target.entry(owner).or_default();
                for (token, delta) in per_token {
                    entry.add_signed(token, delta);
                }
                entry.is_empty()
            };
            if remove {
                target.remove(&owner);
            }
        }
    }

    let mut outflows: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>> =
        HashMap::new();
    let mut stack: Vec<Frame> = Vec::new();
    let mut root_reverted = false;

    for ev in &trace.events {
        match ev {
            EspoSandshrewLikeTraceEvent::Invoke(inv) => {
                // Determine call kind and the nearest normal parent BEFORE pushing the frame.
                let kind = match inv.typ.to_ascii_lowercase().as_str() {
                    "delegatecall" => FrameKind::Delegate,
                    "staticcall" => FrameKind::Static,
                    _ => FrameKind::Normal,
                };
                let Some(owner) = parse_short_id(&inv.context.myself) else { continue };
                let parent_normal = nearest_normal_owner(&stack);

                // Static calls are ignored, but we still push a frame to keep stack depth.
                let incoming = if matches!(kind, FrameKind::Static) {
                    BTreeMap::new()
                } else {
                    transfers_to_sheet(&inv.context.incoming_alkanes)
                };

                stack.push(Frame {
                    kind,
                    owner,
                    incoming,
                    parent_normal,
                    deltas: HashMap::new(),
                });
            }
            EspoSandshrewLikeTraceEvent::Return(ret) => {
                let Some(mut frame) = stack.pop() else {
                    // Mismatched return: treat as a reverted root.
                    root_reverted = true;
                    continue;
                };

                // Failed frames (and their children) are ignored.
                if ret.status == EspoSandshrewLikeTraceStatus::Failure {
                    if stack.is_empty() && matches!(frame.kind, FrameKind::Normal) {
                        root_reverted = true;
                    }
                    continue;
                }

                // Static calls are ignored completely (including children).
                if matches!(frame.kind, FrameKind::Static) {
                    continue;
                }

                // Incoming: transfer from nearest normal parent -> this frame's owner.
                for (token, amount) in &frame.incoming {
                    apply_transfer(
                        &mut frame.deltas,
                        frame.parent_normal,
                        Some(frame.owner),
                        *token,
                        *amount,
                    );
                }

                // Outgoing: transfer from this frame's owner -> nearest normal parent.
                let outgoing = transfers_to_sheet(&ret.response.alkanes);
                for (token, amount) in &outgoing {
                    apply_transfer(
                        &mut frame.deltas,
                        Some(frame.owner),
                        frame.parent_normal,
                        *token,
                        *amount,
                    );
                }

                // Merge this frame's (successful) subtree effects upward.
                if let Some(parent) = stack.last_mut() {
                    merge_deltas(&mut parent.deltas, frame.deltas);
                } else {
                    merge_deltas(&mut outflows, frame.deltas);
                }
            }
            EspoSandshrewLikeTraceEvent::Create(_) => {
                // Create events are ignored per rules.
            }
        }
    }

    if root_reverted || !stack.is_empty() {
        return (false, HashMap::new());
    }

    (true, outflows)
}

/* -------------------------- Edicts + routing (multi-protostone, per your rules) -------------------------- */

/// Whether `vout` is a valid, spendable, non-OP_RETURN output index for this tx.
fn is_valid_spend_vout(tx: &Transaction, vout: u32) -> bool {
    let i = vout as usize;
    i < tx.output.len() && !is_op_return(&tx.output[i].script_pubkey)
}

fn apply_transfers_multi(
    tx: &Transaction,
    protostones: &[Protostone],
    traces_for_tx: &[EspoTrace],
    mut seed_unalloc: Unallocated, // VIN balances only
) -> Result<HashMap<u32, Vec<BalanceEntry>>> {
    let mut out_map: HashMap<u32, Vec<BalanceEntry>> = HashMap::new();

    let n_outputs: u32 = tx.output.len() as u32;
    let multicast_index: u32 = n_outputs; // runes multicast
    let shadow_base: u32 = n_outputs.saturating_add(1);
    let shadow_end: u32 = shadow_base + protostones.len() as u32 - 1;

    // Spendable (non-OP_RETURN)
    let spendable_vouts: Vec<u32> = tx
        .output
        .iter()
        .enumerate()
        .filter_map(|(i, o)| if is_op_return(&o.script_pubkey) { None } else { Some(i as u32) })
        .collect();

    // Map shadow index -> trace (prefer match by Invoke.vout; fallback by order)
    let mut trace_by_shadow: HashMap<u32, &EspoSandshrewLikeTrace> = HashMap::new();

    for t in traces_for_tx {
        // prefer the vout recorded in the first Invoke; else use the outpoint's vout
        let mut vout_opt: Option<u32> = None;
        for ev in &t.sandshrew_trace.events {
            if let EspoSandshrewLikeTraceEvent::Invoke(inv) = ev {
                vout_opt = Some(inv.context.vout);
                break;
            }
        }
        let vout = vout_opt.unwrap_or(t.outpoint.vout);

        // only keep traces that actually point into this tx's shadow range
        if vout >= shadow_base && vout <= shadow_end {
            trace_by_shadow.insert(vout, &t.sandshrew_trace);
        }
    }

    // Sheet incoming routed explicitly to protostone[i] (from previous pointers/edicts/refunds)
    let mut incoming_shadow: Vec<BTreeMap<SchemaAlkaneId, u128>> =
        vec![BTreeMap::new(); protostones.len()];

    // helpers
    fn push_to_vout(
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        vout: u32,
        delta: &BTreeMap<SchemaAlkaneId, u128>,
    ) {
        if delta.is_empty() {
            return;
        }
        let e = out_map.entry(vout).or_default();
        for (rid, &amt) in delta {
            if amt > 0 {
                e.push(BalanceEntry { alkane: *rid, amount: amt });
            }
        }
    }

    fn route_delta(
        target: u32,
        delta: &BTreeMap<SchemaAlkaneId, u128>,
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        incoming_shadow: &mut [BTreeMap<SchemaAlkaneId, u128>],
        tx: &Transaction,
        spendable_vouts: &[u32],
        n_outputs: u32,
        multicast_index: u32,
        shadow_base: u32,
        shadow_end: u32,
    ) {
        if delta.is_empty() {
            return;
        }

        if target == multicast_index {
            if spendable_vouts.is_empty() {
                return;
            }
            let m = spendable_vouts.len() as u128;
            for (rid, &total_amt) in delta.iter() {
                if total_amt == 0 {
                    continue;
                }
                let per = total_amt / m;
                let rem = (total_amt % m) as usize;
                for (i, out_i) in spendable_vouts.iter().enumerate() {
                    let mut amt = per;
                    if i < rem {
                        amt = amt.saturating_add(1);
                    }
                    if amt == 0 {
                        continue;
                    }
                    out_map
                        .entry(*out_i)
                        .or_default()
                        .push(BalanceEntry { alkane: *rid, amount: amt });
                }
            }
            return;
        }

        if target < n_outputs {
            if !is_valid_spend_vout(tx, target) {
                return;
            }
            push_to_vout(out_map, target, delta);
            return;
        }

        if target >= shadow_base && target <= shadow_end {
            let idx = (target - shadow_base) as usize;
            let sheet = &mut incoming_shadow[idx];
            for (rid, &amt) in delta {
                if amt == 0 {
                    continue;
                }
                *sheet.entry(*rid).or_default() =
                    sheet.get(rid).copied().unwrap_or(0).saturating_add(amt);
            }
            return;
        }
        // else burn by omission
    }

    fn apply_single_edict(
        sheet: &mut BTreeMap<SchemaAlkaneId, u128>,
        ed: &ProtostoneEdict,
        out_map: &mut HashMap<u32, Vec<BalanceEntry>>,
        incoming_shadow: &mut [BTreeMap<SchemaAlkaneId, u128>],
        tx: &Transaction,
        spendable_vouts: &[u32],
        n_outputs: u32,
        multicast_index: u32,
        shadow_base: u32,
        shadow_end: u32,
    ) -> Result<()> {
        // guard
        if ed.id.block == 0 && ed.id.tx > 0 {
            return Ok(());
        }
        let out_idx = u128_to_u32(ed.output)?;
        let rid = schema_id_from_parts(ed.id.block, ed.id.tx)?;

        // ---- SPECIAL: multicast target (output == n_outputs) ----
        if out_idx == multicast_index {
            if spendable_vouts.is_empty() {
                return Ok(());
            }

            // how much is available on the sheet for this rune
            let entry = sheet.entry(rid).or_default();
            let have = *entry;
            if have == 0 {
                return Ok(());
            }

            if ed.amount == 0 {
                // even split of ALL available (what you already had working)
                let mut delta = BTreeMap::new();
                delta.insert(rid, have);
                // zero it out from the sheet before routing
                *entry = 0;
                sheet.remove(&rid);

                route_delta(
                    out_idx,
                    &delta,
                    out_map,
                    incoming_shadow,
                    tx,
                    spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                );
            } else {
                // amount > 0 → treat ed.amount as PER-VOUT CAP, and use ALL available
                let mut remaining = have;
                let mut used: u128 = 0;

                for v in spendable_vouts {
                    if remaining == 0 {
                        break;
                    }
                    let give = remaining.min(ed.amount);
                    if give == 0 {
                        break;
                    }
                    out_map.entry(*v).or_default().push(BalanceEntry { alkane: rid, amount: give });
                    remaining = remaining.saturating_sub(give);
                    used = used.saturating_add(give);
                }

                // subtract only what we actually allocated; leave any leftover on the sheet
                *entry = entry.saturating_sub(used);
                if *entry == 0 {
                    sheet.remove(&rid);
                }
            }

            return Ok(());
        }

        // ---- normal (non-multicast) targets: original behavior ----
        let have = sheet.get(&rid).copied().unwrap_or(0);
        let need = if ed.amount == 0 { have } else { ed.amount.min(have) };
        if need == 0 {
            return Ok(());
        }

        // take from sheet
        let entry = sheet.entry(rid).or_default();
        let take = (*entry).min(need);
        *entry = entry.saturating_sub(take);
        if *entry == 0 {
            sheet.remove(&rid);
        }
        if take == 0 {
            return Ok(());
        }

        // route normally
        let mut delta = BTreeMap::new();
        delta.insert(rid, take);
        route_delta(
            out_idx,
            &delta,
            out_map,
            incoming_shadow,
            tx,
            spendable_vouts,
            n_outputs,
            multicast_index,
            shadow_base,
            shadow_end,
        );
        Ok(())
    }

    // process in order
    for (i, ps) in protostones.iter().enumerate() {
        let shadow_vout = shadow_base + i as u32;

        // sheet starts with explicitly routed incoming to this shadow.
        let mut sheet: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

        // merge routed-in firstx
        for (rid, amt) in std::mem::take(&mut incoming_shadow[i]) {
            if amt == 0 {
                continue;
            }
            *sheet.entry(rid).or_default() =
                sheet.get(&rid).copied().unwrap_or(0).saturating_add(amt);
        }

        // if there is a trace for this protostone, compute net_out and status
        let (net_in, net_out, status) = match trace_by_shadow.get(&shadow_vout) {
            Some(trace) => compute_nets(trace),
            None => (None, None, EspoTraceType::NOTRACE),
        };
        // Metashrew can omit incoming_alkanes on reverted traces; fall back to NOTRACE
        // so original VIN balances are still available for edicts/pointers.
        let revert_missing_incoming = status == EspoTraceType::REVERT
            && net_in.as_ref().map_or(true, |m| m.is_empty());
        let status = if revert_missing_incoming {
            EspoTraceType::NOTRACE
        } else {
            status
        };

        // On success, consume incoming amounts so only returned/minted balances remain.
        if status == EspoTraceType::SUCCESS {
            if let Some(ref net_in_map) = net_in {
                for (rid, amt) in net_in_map {
                    if *amt == 0 {
                        continue;
                    }
                    let entry = sheet.entry(*rid).or_default();
                    *entry = entry.saturating_sub(*amt);
                    if *entry == 0 {
                        sheet.remove(rid);
                    }
                }
            }
        }

        // add net_out to sheet
        if status == EspoTraceType::SUCCESS {
            if let Some(ref net_out_map) = net_out {
                for (rid, amt) in net_out_map {
                    if *amt == 0 {
                        continue;
                    }
                    *sheet.entry(*rid).or_default() =
                        sheet.get(rid).copied().unwrap_or(0).saturating_add(*amt);
                }
            }
        }
        // merge VIN balances ONLY into protostone 0’s sheet
        if i == 0 && status == EspoTraceType::NOTRACE {
            for (rid, amt) in seed_unalloc.drain_all() {
                if amt == 0 {
                    continue;
                }
                *sheet.entry(rid).or_default() =
                    sheet.get(&rid).copied().unwrap_or(0).saturating_add(amt);
            }
        }

        // If we have a status and it is Failure → refund net_in (only), skip edicts.
        if status == EspoTraceType::REVERT {
            if let Some(ref net_in_map) = net_in {
                if let Some(refund_ptr) = ps.refund {
                    route_delta(
                        refund_ptr,
                        &net_in_map,
                        &mut out_map,
                        &mut incoming_shadow,
                        tx,
                        &spendable_vouts,
                        n_outputs,
                        multicast_index,
                        shadow_base,
                        shadow_end,
                    );
                }
                // if no refund pointer → burn (do nothing)
            }
            // Skip edicts on failure
            continue;
        }

        // Success path (or no status info): apply edicts against the current sheet
        if !ps.edicts.is_empty() {
            for ed in &ps.edicts {
                if let Err(e) = apply_single_edict(
                    &mut sheet,
                    ed,
                    &mut out_map,
                    &mut incoming_shadow,
                    tx,
                    &spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                ) {
                    eprintln!("[ESSENTIALS::balances] WARN edict apply failed: {e:?}");
                }
            }
        }

        // leftovers after edicts:
        if !sheet.is_empty() {
            if let Some(ptr) = ps.pointer {
                route_delta(
                    ptr,
                    &sheet,
                    &mut out_map,
                    &mut incoming_shadow,
                    tx,
                    &spendable_vouts,
                    n_outputs,
                    multicast_index,
                    shadow_base,
                    shadow_end,
                );
            } else {
                // per your note: do NOT auto-chain; send to first non-OP_RETURN vout
                if let Some(v) = spendable_vouts.first().copied() {
                    push_to_vout(&mut out_map, v, &sheet);
                }
                // else burn by omission
            }
        }
    }

    Ok(out_map)
}

/* -------------------------- Holders helpers -------------------------- */

fn holder_order_key(id: &HolderId) -> String {
    match id {
        HolderId::Address(a) => format!("addr:{a}"),
        HolderId::Alkane(id) => format!("alkane:{:010}:{:020}", id.block, id.tx),
    }
}

fn apply_holders_delta(
    mut holders: Vec<HolderEntry>,
    holder: &HolderId,
    delta: SignedU128,
) -> Vec<HolderEntry> {
    let idx = holders.iter().position(|h| h.holder == *holder);
    let (is_negative, amount) = delta.as_parts();
    if amount == 0 {
        return holders;
    }
    if !is_negative {
        if let Some(i) = idx {
            holders[i].amount = holders[i].amount.saturating_add(amount);
        } else {
            holders.push(HolderEntry { holder: holder.clone(), amount });
        }
    } else if let Some(i) = idx {
        let cur = holders[i].amount;
        if amount > cur {
            panic!(
                "[balances] negative holder balance detected (holder={:?}, cur={}, sub={})",
                holders[i].holder, cur, amount
            );
        }
        let after = cur - amount;
        if after == 0 {
            holders.swap_remove(i);
        } else {
            holders[i].amount = after;
        }
    }
    holders.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => holder_order_key(&a.holder).cmp(&holder_order_key(&b.holder)),
        o => o,
    });
    holders
}

/* ===========================================================
Public API
=========================================================== */

#[allow(unused_assignments)]
pub fn bulk_update_balances_for_block(
    provider: &EssentialsProvider,
    block: &EspoBlock,
) -> Result<()> {
    let network = get_network();
    let table = provider.table();

    eprintln!("[balances] >>> begin block #{} (txs={})", block.height, block.transactions.len());

    // --------- stats ----------
    let mut stat_outpoints_marked_spent: usize = 0;
    let mut stat_outpoints_written: usize = 0;
    let mut stat_minus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut stat_plus_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut minted_delta_by_alk: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
    let mut alkane_tx_summaries: Vec<AlkaneTxSummary> = Vec::new();
    let mut alkane_block_txids: Vec<[u8; 32]> = Vec::new();
    let mut alkane_address_txids: HashMap<String, Vec<[u8; 32]>> = HashMap::new();
    let mut latest_trace_txids: Vec<[u8; 32]> = Vec::new();

    let push_balance_tx_entry = |map: &mut HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>>,
                                 alk: SchemaAlkaneId,
                                 entry: AlkaneBalanceTxEntry| {
        let entries = map.entry(alk).or_default();
        if let Some(existing) = entries.iter_mut().find(|e| e.txid == entry.txid) {
            if existing.outflow.is_empty() && !entry.outflow.is_empty() {
                existing.outflow = entry.outflow;
            }
            if existing.height == 0 && entry.height != 0 {
                existing.height = entry.height;
            }
            return;
        }
        entries.push(entry);
    };
    let push_balance_tx_entry_pair =
        |map: &mut HashMap<(SchemaAlkaneId, SchemaAlkaneId), Vec<AlkaneBalanceTxEntry>>,
         owner: SchemaAlkaneId,
         token: SchemaAlkaneId,
         entry: AlkaneBalanceTxEntry| {
            let entries = map.entry((owner, token)).or_default();
            if let Some(existing) = entries.iter_mut().find(|e| e.txid == entry.txid) {
                if existing.outflow.is_empty() && !entry.outflow.is_empty() {
                    existing.outflow = entry.outflow;
                }
                if existing.height == 0 && entry.height != 0 {
                    existing.height = entry.height;
                }
                return;
            }
            entries.push(entry);
        };

    // holders_delta[alk][addr] = SignedU128 delta
    let mut holders_delta: HashMap<SchemaAlkaneId, BTreeMap<HolderId, SignedU128>> = HashMap::new();
    let mut alkane_balance_delta: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>> =
        HashMap::new();
    let mut alkane_balance_tx_entries: HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>> =
        HashMap::new();
    let mut alkane_balance_tx_entries_by_token: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId),
        Vec<AlkaneBalanceTxEntry>,
    > = HashMap::new();
    let mut alkane_balance_delta_src: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId),
        AlkaneBalanceTxEntry,
    > = HashMap::new();

    // Records for inputs spent in this block (for persistence w/ tx_spent)
    #[derive(Clone)]
    struct SpentOutpointRecord {
        outpoint: EspoOutpoint,      // original outpoint (tx_spent = None)
        addr: Option<String>,        // resolved address
        balances: Vec<BalanceEntry>, // balances stored on the outpoint
        spk: Option<ScriptBuf>,      // script (for reverse index)
        spent_by: Vec<u8>,           // BE txid of spending tx
    }
    let mut spent_outpoints: HashMap<String, SpentOutpointRecord> = HashMap::new();

    // Ephemeral state for CPFP within the same block
    let mut ephem_outpoint_balances: HashMap<String, Vec<BalanceEntry>> = HashMap::new();
    let mut ephem_outpoint_addr: HashMap<String, String> = HashMap::new();
    let mut ephem_outpoint_spk: HashMap<String, ScriptBuf> = HashMap::new();
    let mut ephem_outpoint_struct: HashMap<String, EspoOutpoint> = HashMap::new();
    let mut consumed_ephem_outpoints: HashMap<String, Vec<u8>> = HashMap::new(); // outpoint_str -> spender txid

    // ---------- Pass A: collect block-created outpoints & external inputs ----------
    let mut block_created_outs: HashSet<String> = HashSet::new();
    for atx in &block.transactions {
        let tx = &atx.transaction;
        if !tx_has_op_return(tx) {
            continue; // no OP_RETURN → no Alkanes activity on its outputs
        }
        let txid = tx.compute_txid();
        for (vout, _o) in tx.output.iter().enumerate() {
            let op = mk_outpoint(txid.as_byte_array().to_vec(), vout as u32, None);
            block_created_outs.insert(op.as_outpoint_string());
        }
    }

    // Collect all non-ephemeral vins across the block (dedup)
    let mut external_inputs_vec: Vec<EspoOutpoint> = Vec::new();
    let mut external_inputs_set: HashSet<(Vec<u8>, u32)> = HashSet::new();

    for atx in &block.transactions {
        for input in &atx.transaction.input {
            let op = mk_outpoint(
                input.previous_output.txid.as_byte_array().to_vec(),
                input.previous_output.vout,
                None,
            );
            let in_str = op.as_outpoint_string();
            if !block_created_outs.contains(&in_str) {
                let key = (op.txid.clone(), op.vout);
                if external_inputs_set.insert(key) {
                    external_inputs_vec.push(op);
                }
            }
        }
    }

    // ---------- Pass B: fetch external inputs (batch read) ----------
    let mut balances_by_outpoint: HashMap<(Vec<u8>, u32), Vec<BalanceEntry>> = HashMap::new();
    let mut addr_by_outpoint: HashMap<(Vec<u8>, u32), String> = HashMap::new();
    let mut spk_by_outpoint: HashMap<(Vec<u8>, u32), ScriptBuf> = HashMap::new();

    if !external_inputs_vec.is_empty() {
        let mut k_balances: Vec<Vec<u8>> = Vec::with_capacity(external_inputs_vec.len());
        let mut k_addr: Vec<Vec<u8>> = Vec::with_capacity(external_inputs_vec.len());
        let mut k_spk: Vec<Vec<u8>> = Vec::with_capacity(external_inputs_vec.len());

        for op in &external_inputs_vec {
            k_balances.push(table.outpoint_balances_key(op)?);
            k_addr.push(table.outpoint_addr_key(op)?);
            k_spk.push(table.utxo_spk_key(op)?);
        }

        let v_balances = provider
            .get_multi_values(GetMultiValuesParams { keys: k_balances })?
            .values;
        let v_addr = provider
            .get_multi_values(GetMultiValuesParams { keys: k_addr })?
            .values;
        let v_spk = provider
            .get_multi_values(GetMultiValuesParams { keys: k_spk })?
            .values;

        for (i, op) in external_inputs_vec.iter().enumerate() {
            let key = (op.txid.clone(), op.vout);

            if let Some(bytes) = &v_balances[i] {
                if let Ok(bals) = decode_balances_vec(bytes) {
                    if !bals.is_empty() {
                        balances_by_outpoint.insert(key.clone(), bals);
                    }
                }
            }
            if let Some(addr_bytes) = &v_addr[i] {
                if let Ok(s) = std::str::from_utf8(addr_bytes) {
                    addr_by_outpoint.insert(key.clone(), s.to_string());
                }
            }
            if let Some(spk_bytes) = &v_spk[i] {
                spk_by_outpoint.insert(key, ScriptBuf::from(spk_bytes.clone()));
            }
        }
    }

    let mut block_tx_index: HashMap<Txid, usize> = HashMap::new();
    for (idx, atx) in block.transactions.iter().enumerate() {
        block_tx_index.insert(atx.transaction.compute_txid(), idx);
    }

    let mut trace_prevout_txids: Vec<Txid> = Vec::new();
    let mut trace_prevout_set: HashSet<Txid> = HashSet::new();
    for atx in &block.transactions {
        let has_traces = atx.traces.as_ref().map_or(false, |t| !t.is_empty());
        if !has_traces {
            continue;
        }
        for input in &atx.transaction.input {
            if input.previous_output.is_null() {
                continue;
            }
            let prev_txid = input.previous_output.txid;
            if block_tx_index.contains_key(&prev_txid) {
                continue;
            }
            if trace_prevout_set.insert(prev_txid) {
                trace_prevout_txids.push(prev_txid);
            }
        }
    }

    // TODO: extend prevout fallback to all alkane txs (not just traced) for full address coverage.
    let mut trace_prev_tx_map: HashMap<Txid, Transaction> = HashMap::new();
    if !trace_prevout_txids.is_empty() {
        let electrum_like = get_electrum_like();
        let start = Instant::now();
        let raw_prev = electrum_like
            .batch_transaction_get_raw(&trace_prevout_txids)
            .unwrap_or_default();
        eprintln!(
            "[balances] traced prevout fetch: block={} prevouts={} elapsed_ms={}",
            block.height,
            trace_prevout_txids.len(),
            start.elapsed().as_millis()
        );
        for (i, raw_prev) in raw_prev.into_iter().enumerate() {
            if raw_prev.is_empty() {
                continue;
            }
            if let Ok(prev_tx) = deserialize::<Transaction>(&raw_prev) {
                trace_prev_tx_map.insert(trace_prevout_txids[i], prev_tx);
            }
        }
    }

    // ---------- Main per-tx loop ----------
    for atx in &block.transactions {
        let tx = &atx.transaction;
        let txid = tx.compute_txid();
        let txid_bytes = txid.to_byte_array();
        let mut tx_addrs: HashSet<String> = HashSet::new();
        let mut has_alkane_vin = false;
        let has_traces = atx.traces.as_ref().map_or(false, |t| !t.is_empty());
        let mut holder_alkanes_changed: HashSet<SchemaAlkaneId> = HashSet::new();
        let mut local_alkane_delta: HashMap<SchemaAlkaneId, BTreeMap<SchemaAlkaneId, SignedU128>> =
            HashMap::new();

        let mut add_holder_delta =
            |alk: SchemaAlkaneId,
             holder: HolderId,
             delta: SignedU128,
             holder_changed: &mut HashSet<SchemaAlkaneId>| {
                if delta.is_zero() {
                    return;
                }
                if let HolderId::Alkane(a) = holder {
                    holder_changed.insert(a);
                }
                let entry = holders_delta.entry(alk).or_default();
                let slot = entry.entry(holder.clone()).or_insert_with(SignedU128::zero);
                *slot += delta;
                if slot.is_zero() {
                    entry.remove(&holder);
                }
            };

        // Seed from VIN balances only
        let mut seed_unalloc = Unallocated::default();

        // Gather ephemerals for this tx & apply; for externals, use prefetched maps
        for input in &tx.input {
            let in_op = mk_outpoint(
                input.previous_output.txid.as_byte_array().to_vec(),
                input.previous_output.vout,
                None,
            );
            let in_key = (in_op.txid.clone(), in_op.vout);
            let in_str = in_op.as_outpoint_string();

            if !input.previous_output.is_null() {
                let mut input_addr: Option<String> = None;
                if let Some(idx) = block_tx_index.get(&input.previous_output.txid) {
                    if let Some(prev_out) =
                        block.transactions[*idx].transaction.output.get(input.previous_output.vout as usize)
                    {
                        input_addr = spk_to_address_str(&prev_out.script_pubkey, network);
                    }
                }
                if input_addr.is_none() {
                    if let Some(addr) = addr_by_outpoint.get(&in_key) {
                        input_addr = Some(addr.clone());
                    } else if let Some(spk) = spk_by_outpoint.get(&in_key) {
                        input_addr = spk_to_address_str(spk, network);
                    }
                }
                if input_addr.is_none() && has_traces {
                    if let Some(prev_tx) = trace_prev_tx_map.get(&input.previous_output.txid) {
                        if let Some(prev_out) =
                            prev_tx.output.get(input.previous_output.vout as usize)
                        {
                            input_addr = spk_to_address_str(&prev_out.script_pubkey, network);
                        }
                    }
                }
                if let Some(addr) = input_addr {
                    tx_addrs.insert(addr);
                }
            }

            // 1) Ephemeral? (created earlier in this same block)
            if let Some(bals) = ephem_outpoint_balances.get(&in_str) {
                consumed_ephem_outpoints.insert(in_str.clone(), txid.as_byte_array().to_vec());
                has_alkane_vin = true;

                if let Some(addr) = ephem_outpoint_addr.get(&in_str) {
                    tx_addrs.insert(addr.clone());
                    for be in bals {
                        add_holder_delta(
                            be.alkane,
                            HolderId::Address(addr.clone()),
                            SignedU128::negative(be.amount),
                            &mut holder_alkanes_changed,
                        );
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
                    }
                    // we only track addr-row deletes for DB-resident rows; ephemerals were not persisted yet
                }
                for be in bals {
                    seed_unalloc.add(be.alkane, be.amount);
                }
                // record for persistence as spent
                let rec = SpentOutpointRecord {
                    outpoint: in_op.clone(),
                    addr: ephem_outpoint_addr.get(&in_str).cloned(),
                    balances: bals.clone(),
                    spk: ephem_outpoint_spk.get(&in_str).cloned(),
                    spent_by: txid.to_byte_array().to_vec(),
                };
                spent_outpoints.entry(in_str.clone()).or_insert(rec);
                continue;
            }

            // 2) External input: resolve from prefetched maps (no DB calls here)
            if let Some(bals) = balances_by_outpoint.get(&in_key).cloned() {
                has_alkane_vin = true;
                // resolve address: /outpoint_addr first, else /utxo_spk → address
                let mut resolved_addr = addr_by_outpoint.get(&in_key).cloned();
                if resolved_addr.is_none() {
                    if let Some(spk) = spk_by_outpoint.get(&in_key) {
                        resolved_addr = spk_to_address_str(spk, network);
                    }
                }

                if let Some(ref addr) = resolved_addr {
                    tx_addrs.insert(addr.clone());
                    // holders-- and mark legacy addr-row delete
                    for be in &bals {
                        add_holder_delta(
                            be.alkane,
                            HolderId::Address(addr.clone()),
                            SignedU128::negative(be.amount),
                            &mut holder_alkanes_changed,
                        );
                        *stat_minus_by_alk.entry(be.alkane).or_default() = stat_minus_by_alk
                            .get(&be.alkane)
                            .copied()
                            .unwrap_or(0)
                            .saturating_add(be.amount);
                    }
                }

                for be in &bals {
                    seed_unalloc.add(be.alkane, be.amount);
                }

                // record for persistence with spend metadata
                let rec = SpentOutpointRecord {
                    outpoint: in_op.clone(),
                    addr: resolved_addr.clone(),
                    balances: bals.clone(),
                    spk: spk_by_outpoint.get(&in_key).cloned(),
                    spent_by: txid.to_byte_array().to_vec(),
                };
                spent_outpoints.entry(in_str.clone()).or_insert(rec);
            }
            // else: no balances row → nothing to do for this vin
        }

        // apply transfers with your semantics
        let traces_for_tx: Vec<EspoTrace> = atx.traces.clone().unwrap_or_default();
        if !traces_for_tx.is_empty() {
            for t in &traces_for_tx {
                let (ok, deltas) = accumulate_alkane_balance_deltas(
                    &t.sandshrew_trace,
                    &txid,
                    &block.host_function_values,
                );
                if !ok {
                    local_alkane_delta.clear();
                    holder_alkanes_changed.clear();
                    break;
                }
                for (owner, per_token) in deltas {
                    let entry = local_alkane_delta.entry(owner).or_default();
                    for (tok, delta) in per_token {
                        let slot = entry.entry(tok).or_insert_with(SignedU128::zero);
                        *slot += delta;
                        if slot.is_zero() {
                            entry.remove(&tok);
                        }
                    }
                }
            }
        }

        let allocations = if tx_has_op_return(tx) {
            let protostones = parse_protostones(tx)?;
            // apply transfers only when there’s a proto/runestone carrier
            apply_transfers_multi(tx, &protostones, &traces_for_tx, seed_unalloc)?
        } else {
            // No OP_RETURN → no Alkanes allocations (but we already did VIN cleanup/holders--)
            HashMap::<u32, Vec<BalanceEntry>>::new()
        };
        // record outputs ephemerally (for same-block spends)
        for (vout_idx, entries_for_vout) in allocations {
            if entries_for_vout.is_empty() || vout_idx as usize >= tx.output.len() {
                continue;
            }
            let output = &tx.output[vout_idx as usize];
            if is_op_return(&output.script_pubkey) {
                continue;
            }

            if let Some(address_str) = spk_to_address_str(&output.script_pubkey, network) {
                tx_addrs.insert(address_str.clone());
                // Combine duplicates
                let mut amounts_by_alkane: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();
                for entry in entries_for_vout {
                    *amounts_by_alkane.entry(entry.alkane).or_default() = amounts_by_alkane
                        .get(&entry.alkane)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(entry.amount);
                }

                let balances_for_outpoint: Vec<BalanceEntry> = amounts_by_alkane
                    .iter()
                    .map(|(alkane_id, amount)| BalanceEntry { alkane: *alkane_id, amount: *amount })
                    .collect();

                let created_outpoint = mk_outpoint(txid.as_byte_array().to_vec(), vout_idx, None);
                let outpoint_str = created_outpoint.as_outpoint_string();

                // cache for same-block spends
                ephem_outpoint_balances.insert(outpoint_str.clone(), balances_for_outpoint.clone());
                ephem_outpoint_addr.insert(outpoint_str.clone(), address_str.clone());
                ephem_outpoint_spk.insert(outpoint_str.clone(), output.script_pubkey.clone());
                ephem_outpoint_struct.insert(outpoint_str.clone(), created_outpoint.clone());

                // holders++ stats
                for (alkane_id, delta_amount) in amounts_by_alkane {
                    add_holder_delta(
                        alkane_id,
                        HolderId::Address(address_str.clone()),
                        SignedU128::positive(delta_amount),
                        &mut holder_alkanes_changed,
                    );
                    *stat_plus_by_alk.entry(alkane_id).or_default() = stat_plus_by_alk
                        .get(&alkane_id)
                        .copied()
                        .unwrap_or(0)
                        .saturating_add(delta_amount);
                }

                stat_outpoints_written += 1;
            }
        }
        for (holder_alk, per_token) in &local_alkane_delta {
            let entry_outflow = AlkaneBalanceTxEntry {
                txid: txid_bytes,
                height: block.height,
                outflow: per_token.clone(),
            };
            for (token, delta) in per_token {
                if delta.is_zero() {
                    continue;
                }
                alkane_balance_delta_src.insert((*holder_alk, *token), entry_outflow.clone());
                push_balance_tx_entry_pair(
                    &mut alkane_balance_tx_entries_by_token,
                    *holder_alk,
                    *token,
                    entry_outflow.clone(),
                );
                if *token == *holder_alk {
                    // Keep self-token outflows for summaries/ammdata, but don't persist balances.
                    let (is_negative, mag) = delta.as_parts();
                    if is_negative && mag > 0 {
                        *minted_delta_by_alk.entry(*token).or_default() =
                            minted_delta_by_alk.get(token).copied().unwrap_or(0).saturating_add(mag);
                    }
                    continue;
                }
                add_holder_delta(
                    *token,
                    HolderId::Alkane(*holder_alk),
                    *delta,
                    &mut holder_alkanes_changed,
                );
                let (is_negative, mag) = delta.as_parts();
                if is_negative {
                    *stat_minus_by_alk.entry(*token).or_default() =
                        stat_minus_by_alk.get(token).copied().unwrap_or(0).saturating_add(mag);
                } else {
                    *stat_plus_by_alk.entry(*token).or_default() =
                        stat_plus_by_alk.get(token).copied().unwrap_or(0).saturating_add(mag);
                }
                let entry = alkane_balance_delta.entry(*holder_alk).or_default();
                let slot = entry.entry(*token).or_insert_with(SignedU128::zero);
                *slot += *delta;
                if slot.is_zero() {
                    entry.remove(token);
                }
            }
        }

        for owner in &holder_alkanes_changed {
            let outflow = local_alkane_delta.get(owner).cloned().unwrap_or_else(BTreeMap::new);
            let entry = AlkaneBalanceTxEntry { txid: txid_bytes, height: block.height, outflow };
            push_balance_tx_entry(&mut alkane_balance_tx_entries, *owner, entry);
        }

        let is_alkane_tx = has_alkane_vin || has_traces;
        if is_alkane_tx {
            for output in &tx.output {
                if is_op_return(&output.script_pubkey) {
                    continue;
                }
                if let Some(addr) = spk_to_address_str(&output.script_pubkey, network) {
                    tx_addrs.insert(addr);
                }
            }

            let mut outflows: Vec<AlkaneBalanceTxEntry> = Vec::new();
            for (_owner, per_token) in &local_alkane_delta {
                if per_token.is_empty() {
                    continue;
                }
                outflows.push(AlkaneBalanceTxEntry {
                    txid: txid_bytes,
                    height: block.height,
                    outflow: per_token.clone(),
                });
            }

            let traces: Vec<EspoSandshrewLikeTrace> = if has_traces {
                atx.traces
                    .as_ref()
                    .map(|list| list.iter().map(|t| t.sandshrew_trace.clone()).collect())
                    .unwrap_or_default()
            } else {
                Vec::new()
            };

            alkane_tx_summaries.push(AlkaneTxSummary {
                txid: txid_bytes,
                traces,
                outflows,
                height: block.height,
            });
            alkane_block_txids.push(txid_bytes);
            for addr in &tx_addrs {
                alkane_address_txids.entry(addr.clone()).or_default().push(txid_bytes);
            }
            if has_traces {
                latest_trace_txids.push(txid_bytes);
            }
        }
    }

    // Ensure txid indexes are recorded for every alkane/token delta we are about to persist.
    for ((owner, token), entry) in &alkane_balance_delta_src {
        push_balance_tx_entry(&mut alkane_balance_tx_entries, *owner, entry.clone());
        push_balance_tx_entry_pair(
            &mut alkane_balance_tx_entries_by_token,
            *owner,
            *token,
            entry.clone(),
        );
    }

    // Accumulate alkane holder deltas (alkane -> token) and prepare rows for persistence.
    let mut alkane_balances_rows: HashMap<SchemaAlkaneId, Vec<BalanceEntry>> = HashMap::new();
    if !alkane_balance_delta.is_empty() {
        let mut owners: Vec<SchemaAlkaneId> = alkane_balance_delta.keys().copied().collect();
        owners.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(owners.len());
        for owner in &owners {
            keys.push(table.alkane_balances_key(owner));
        }
        let existing = provider
            .get_multi_values(GetMultiValuesParams { keys })?
            .values;

        for (idx, owner) in owners.iter().enumerate() {
            let mut amounts: BTreeMap<SchemaAlkaneId, u128> = BTreeMap::new();

            if let Some(bytes) = &existing.get(idx).and_then(|v| v.as_ref()) {
                if let Ok(entries) = decode_balances_vec(bytes) {
                    for be in entries {
                        if be.amount == 0 {
                            continue;
                        }
                        amounts.insert(be.alkane, be.amount);
                    }
                }
            }

            if let Some(delta_map) = alkane_balance_delta.get(owner) {
                for (token, delta) in delta_map {
                    let (is_negative, mag) = delta.as_parts();
                    if mag == 0 {
                        continue;
                    }
                    let cur = amounts.get(token).copied().unwrap_or(0);
                    let updated = if !is_negative {
                        cur.saturating_add(mag)
                    } else {
                        if mag > cur {
                            let txid_str = alkane_balance_delta_src
                                .get(&(*owner, *token))
                                .map(|entry| Txid::from_byte_array(entry.txid))
                                .map(|t| t.to_string())
                                .unwrap_or_else(|| "unknown".to_string());
                            panic!(
                                "[balances] negative alkane balance detected (txid={}, owner={}:{}, token={}:{}, cur={}, sub={})",
                                txid_str, owner.block, owner.tx, token.block, token.tx, cur, mag
                            );
                        }
                        cur - mag
                    };
                    if updated == 0 {
                        amounts.remove(token);
                    } else {
                        amounts.insert(*token, updated);
                    }
                }
            }

            let mut vec_entries: Vec<BalanceEntry> = amounts
                .into_iter()
                .map(|(alkane, amount)| BalanceEntry { alkane, amount })
                .collect();
            vec_entries
                .sort_by(|a, b| b.amount.cmp(&a.amount).then_with(|| a.alkane.cmp(&b.alkane)));
            alkane_balances_rows.insert(*owner, vec_entries);
        }
    }

    // Build balance-change tx rows (merge existing + new)
    let mut alkane_balance_txs_rows: HashMap<SchemaAlkaneId, Vec<AlkaneBalanceTxEntry>> =
        HashMap::new();
    let mut alkane_balance_txs_by_token_rows: HashMap<
        (SchemaAlkaneId, SchemaAlkaneId),
        Vec<AlkaneBalanceTxEntry>,
    > = HashMap::new();

    fn merge_tx_entries(
        existing: &mut Vec<AlkaneBalanceTxEntry>,
        new_entries: &[AlkaneBalanceTxEntry],
    ) {
        let mut seen: HashMap<[u8; 32], usize> = HashMap::new();
        for (idx, entry) in existing.iter().enumerate() {
            seen.insert(entry.txid, idx);
        }
        for entry in new_entries {
            if let Some(idx) = seen.get(&entry.txid).copied() {
                if existing[idx].outflow.is_empty() && !entry.outflow.is_empty() {
                    existing[idx].outflow = entry.outflow.clone();
                }
                if existing[idx].height == 0 && entry.height != 0 {
                    existing[idx].height = entry.height;
                }
                continue;
            }
            seen.insert(entry.txid, existing.len());
            existing.push(entry.clone());
        }
    }

    if !alkane_balance_tx_entries.is_empty() {
        let mut tokens: Vec<SchemaAlkaneId> = alkane_balance_tx_entries.keys().copied().collect();
        tokens.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(tokens.len());
        for tok in &tokens {
            keys.push(table.alkane_balance_txs_key(tok));
        }
        let existing = provider
            .get_multi_values(GetMultiValuesParams { keys })?
            .values;

        for (idx, tok) in tokens.iter().enumerate() {
            let mut merged: Vec<AlkaneBalanceTxEntry> = Vec::new();
            if let Some(bytes) = &existing.get(idx).and_then(|v| v.as_ref()) {
                if let Ok(cur) = decode_alkane_balance_tx_entries(bytes) {
                    merged = cur;
                }
            }

            if let Some(new) = alkane_balance_tx_entries.get(tok) {
                merge_tx_entries(&mut merged, new);
            }

            if !merged.is_empty() {
                alkane_balance_txs_rows.insert(*tok, merged);
            }
        }
    }
    if !alkane_balance_tx_entries_by_token.is_empty() {
        let mut pairs: Vec<(SchemaAlkaneId, SchemaAlkaneId)> =
            alkane_balance_tx_entries_by_token.keys().copied().collect();
        pairs.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(pairs.len());
        for (owner, token) in &pairs {
            keys.push(table.alkane_balance_txs_by_token_key(owner, token));
        }
        let existing = provider
            .get_multi_values(GetMultiValuesParams { keys })?
            .values;

        for (idx, pair) in pairs.iter().enumerate() {
            let mut merged: Vec<AlkaneBalanceTxEntry> = Vec::new();
            if let Some(bytes) = &existing.get(idx).and_then(|v| v.as_ref()) {
                if let Ok(cur) = decode_alkane_balance_tx_entries(bytes) {
                    merged = cur;
                }
            }

            if let Some(new) = alkane_balance_tx_entries_by_token.get(pair) {
                merge_tx_entries(&mut merged, new);
            }

            if !merged.is_empty() {
                alkane_balance_txs_by_token_rows.insert(*pair, merged);
            }
        }
    }

    let mut alkane_balance_txs_by_height_row: BTreeMap<
        SchemaAlkaneId,
        Vec<AlkaneBalanceTxEntry>,
    > = BTreeMap::new();
    if !alkane_balance_tx_entries.is_empty() {
        for (alkane, entries) in &alkane_balance_tx_entries {
            if entries.is_empty() {
                continue;
            }
            alkane_balance_txs_by_height_row.insert(*alkane, entries.clone());
        }
    }

    let mut address_offsets: HashMap<String, u64> = HashMap::new();
    if !alkane_address_txids.is_empty() {
        let mut addrs: Vec<String> = alkane_address_txids.keys().cloned().collect();
        addrs.sort();
        let mut keys: Vec<Vec<u8>> = Vec::with_capacity(addrs.len());
        for addr in &addrs {
            keys.push(table.alkane_address_len_key(addr));
        }
        let existing = provider
            .get_multi_values(GetMultiValuesParams { keys })?
            .values;
        for (idx, addr) in addrs.iter().enumerate() {
            let len = existing
                .get(idx)
                .and_then(|v| v.as_ref())
                .and_then(|b| {
                    if b.len() == 8 {
                        let mut arr = [0u8; 8];
                        arr.copy_from_slice(b);
                        Some(u64::from_le_bytes(arr))
                    } else {
                        None
                    }
                })
                .unwrap_or(0);
            address_offsets.insert(addr.clone(), len);
        }
    }

    let mut latest_traces: Vec<[u8; 32]> = provider
        .get_raw_value(GetRawValueParams {
            key: table.alkane_latest_traces_key(),
        })?
        .value
        .and_then(|b| Vec::<[u8; 32]>::try_from_slice(&b).ok())
        .unwrap_or_default();
    if !latest_trace_txids.is_empty() {
        for txid in latest_trace_txids {
            latest_traces.insert(0, txid);
        }
        if latest_traces.len() > 20 {
            latest_traces.truncate(20);
        }
    }

    // logging metric
    stat_outpoints_marked_spent = spent_outpoints.len();

    // Build unified rows (new outputs + spent inputs)
    struct NewRow {
        outpoint: EspoOutpoint,
        addr: String,
        enc_balances: Vec<u8>,
        uspk_val: Option<Vec<u8>>, // spk bytes
    }
    let mut new_rows: Vec<NewRow> = Vec::new();

    // map outpoint string -> row data
    let mut row_map: HashMap<String, NewRow> = HashMap::new();

    // Persist block-created outputs (mark as spent if consumed within same block)
    for (out_str, vec_out) in &ephem_outpoint_balances {
        let addr = match ephem_outpoint_addr.get(out_str) {
            Some(a) => a.clone(),
            None => continue,
        };
        let mut op = match ephem_outpoint_struct.get(out_str) {
            Some(o) => o.clone(),
            None => continue,
        };

        if let Some(spender) = consumed_ephem_outpoints.get(out_str) {
            op.tx_spent = Some(spender.clone());
        }

        let enc_balances = encode_vec(vec_out)?;
        let uspk_val = ephem_outpoint_spk.get(out_str).map(|spk| spk.as_bytes().to_vec());

        row_map.insert(out_str.clone(), NewRow { outpoint: op, addr, enc_balances, uspk_val });
    }

    // Persist external inputs (spent) and any ephemerals consumed in-block
    for (out_str, rec) in &spent_outpoints {
        let addr = match &rec.addr {
            Some(a) => a.clone(),
            None => continue,
        };
        let mut op = rec.outpoint.clone();
        op.tx_spent = Some(rec.spent_by.clone());
        let enc_balances = encode_vec(&rec.balances)?;
        let uspk_val = rec.spk.as_ref().map(|spk| spk.as_bytes().to_vec());

        row_map
            .entry(out_str.clone())
            .and_modify(|row| {
                row.outpoint.tx_spent = Some(rec.spent_by.clone());
                if row.uspk_val.is_none() {
                    row.uspk_val = uspk_val.clone();
                }
            })
            .or_insert(NewRow { outpoint: op, addr, enc_balances, uspk_val });
    }

    for (_, row) in row_map {
        new_rows.push(row);
    }

    // --- Cleanup keys for outpoints that were spent (remove unspent variants) ---
    let mut del_keys_outpoint_balances: Vec<Vec<u8>> = Vec::new();
    let mut del_keys_outpoint_addr: Vec<Vec<u8>> = Vec::new();
    let mut del_keys_utxo_spk: Vec<Vec<u8>> = Vec::new();
    let mut del_keys_addr_balances: Vec<Vec<u8>> = Vec::new();

    for row in &new_rows {
        if row.outpoint.tx_spent.is_some() {
            let unspent = mk_outpoint(row.outpoint.txid.clone(), row.outpoint.vout, None);

            del_keys_outpoint_balances.push(table.outpoint_balances_key(&unspent)?);
            del_keys_outpoint_addr.push(table.outpoint_addr_key(&unspent)?);
            del_keys_utxo_spk.push(table.utxo_spk_key(&unspent)?);
            del_keys_addr_balances.push(table.balances_key(&row.addr, &unspent)?);
        }
    }

    // ---- single write-batch ----
    let mut puts: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
    let mut deletes: Vec<Vec<u8>> = Vec::new();

    // A) Address-scoped deletes
    deletes.extend(del_keys_addr_balances);

    // B) Reverse-index cleanup
    deletes.extend(del_keys_outpoint_balances);
    deletes.extend(del_keys_outpoint_addr);
    deletes.extend(del_keys_utxo_spk);

    // C) Persist new outputs (unspent + spent with tx_spent metadata)
    for row in &new_rows {
        let bkey = match table.balances_key(&row.addr, &row.outpoint) {
            Ok(k) => k,
            Err(_) => continue,
        };
        let obkey = match table.outpoint_balances_key(&row.outpoint) {
            Ok(k) => k,
            Err(_) => continue,
        };
        let oaddr_key = match table.outpoint_addr_key(&row.outpoint) {
            Ok(k) => k,
            Err(_) => continue,
        };
        let uspk_key = match table.utxo_spk_key(&row.outpoint) {
            Ok(k) => k,
            Err(_) => continue,
        };

        puts.push((bkey, row.enc_balances.clone()));
        puts.push((obkey, row.enc_balances.clone()));
        puts.push((oaddr_key, row.addr.as_bytes().to_vec()));
        if let Some(ref spk_bytes) = row.uspk_val {
            puts.push((uspk_key, spk_bytes.clone()));
            puts.push((table.addr_spk_key(&row.addr), spk_bytes.clone()));
        }
    }

    // C2) Persist alkane balance index (alkane -> Vec<BalanceEntry>)
    for (owner, entries) in alkane_balances_rows.iter() {
        let key = table.alkane_balances_key(owner);
        if entries.is_empty() {
            deletes.push(key);
            continue;
        }
        if let Ok(buf) = encode_vec(entries) {
            puts.push((key, buf));
        }
    }

    // C3) Persist alkane balance change txids
    for (alk, txids) in alkane_balance_txs_rows.iter() {
        let key = table.alkane_balance_txs_key(alk);
        if txids.is_empty() {
            deletes.push(key);
            continue;
        }
        if let Ok(buf) = encode_vec(txids) {
            puts.push((key, buf));
        }
    }
    // C3b) Persist alkane balance change txids by token
    for ((owner, token), txids) in alkane_balance_txs_by_token_rows.iter() {
        let key = table.alkane_balance_txs_by_token_key(owner, token);
        if txids.is_empty() {
            deletes.push(key);
            continue;
        }
        if let Ok(buf) = encode_vec(txids) {
            puts.push((key, buf));
        }
    }

    // C3c) Persist alkane balance change txids by height
    let height_key = table.alkane_balance_txs_by_height_key(block.height);
    if alkane_balance_txs_by_height_row.is_empty() {
        deletes.push(height_key);
    } else if let Ok(buf) = borsh::to_vec(&alkane_balance_txs_by_height_row) {
        puts.push((height_key, buf));
    }

    // C4) Persist alkane tx summaries + block/address indexes
    for summary in &alkane_tx_summaries {
        if let Ok(buf) = borsh::to_vec(summary) {
            puts.push((table.alkane_tx_summary_key(&summary.txid), buf));
        }
    }

    let block_len = alkane_block_txids.len() as u64;
    puts.push((table.alkane_block_len_key(block.height as u64), block_len.to_le_bytes().to_vec()));
    for (idx, txid_bytes) in alkane_block_txids.iter().enumerate() {
        puts.push((table.alkane_block_txid_key(block.height as u64, idx as u64), txid_bytes.to_vec()));
    }

    for (addr, txids) in alkane_address_txids.iter() {
        let start = address_offsets.get(addr).copied().unwrap_or(0);
        for (i, txid_bytes) in txids.iter().enumerate() {
            let idx = start + i as u64;
            puts.push((table.alkane_address_txid_key(addr, idx), txid_bytes.to_vec()));
        }
        let new_len = start + txids.len() as u64;
        puts.push((table.alkane_address_len_key(addr), new_len.to_le_bytes().to_vec()));
    }

    if let Ok(buf) = borsh::to_vec(&latest_traces) {
        if latest_traces.is_empty() {
            deletes.push(table.alkane_latest_traces_key());
        } else {
            puts.push((table.alkane_latest_traces_key(), buf));
        }
    }

    // D) Holders deltas
    for (alkane, per_holder) in holders_delta.iter() {
        let holders_key = table.holders_key(alkane);
        let holders_count_key = table.holders_count_key(alkane);

        let current_holders = provider
            .get_raw_value(GetRawValueParams { key: holders_key.clone() })?
            .value;
        let mut vec_holders: Vec<HolderEntry> = match current_holders {
            Some(bytes) => decode_holders_vec(&bytes).unwrap_or_default(),
            None => Vec::new(),
        };
        let prev_count = vec_holders.len() as u64;
        for (holder, delta) in per_holder {
            vec_holders = apply_holders_delta(vec_holders, holder, *delta);
        }
        let new_count = vec_holders.len() as u64;
        let new_index_key = table.alkane_holders_ordered_key(new_count, alkane);
        if prev_count != new_count {
            let prev_index_key = table.alkane_holders_ordered_key(prev_count, alkane);
            deletes.push(prev_index_key);
        }
        puts.push((new_index_key, Vec::new()));

        let supply: u128 = vec_holders.iter().map(|h| h.amount).sum();
        let supply_latest_key = table.circulating_supply_latest_key(alkane);
        let prev_supply = provider
            .get_raw_value(GetRawValueParams { key: supply_latest_key.clone() })?
            .value
            .and_then(|v| decode_u128_value(&v).ok())
            .unwrap_or(0);
        if supply != prev_supply {
            let encoded = encode_u128_value(supply)?;
            puts.push((table.circulating_supply_key(alkane, block.height), encoded.clone()));
            puts.push((supply_latest_key, encoded));
        }

        if vec_holders.is_empty() {
            deletes.push(holders_key);
        } else if let Ok((encoded_holders_vec, encoded_holders_count_vec)) =
            get_holders_values_encoded(vec_holders)
        {
            puts.push((holders_key, encoded_holders_vec));
            puts.push((holders_count_key, encoded_holders_count_vec));
        }
    }

    for (alkane, delta) in minted_delta_by_alk.iter() {
        if *delta == 0 {
            continue;
        }
        let latest_key = table.total_minted_latest_key(alkane);
        let prev_total = provider
            .get_raw_value(GetRawValueParams { key: latest_key.clone() })?
            .value
            .and_then(|v| decode_u128_value(&v).ok())
            .unwrap_or(0);
        let new_total = prev_total.saturating_add(*delta);
        let encoded = encode_u128_value(new_total)?;
        puts.push((table.total_minted_key(alkane, block.height), encoded.clone()));
        puts.push((latest_key, encoded));
    }

    if is_strict_mode() {
        let metashrew = get_metashrew();
        let height_u64 = block.height as u64;
        let metashrew_sdb = get_metashrew_sdb();
        metashrew_sdb
            .catch_up_now()
            .context("metashrew catch_up before strict checks")?;
        let sdb = metashrew_sdb.as_ref();

        let balances_from_rows = |owner: &SchemaAlkaneId| -> HashMap<SchemaAlkaneId, u128> {
            let entries = alkane_balances_rows.get(owner).unwrap_or_else(|| {
                panic!(
                    "[balances][strict] missing prewrite balances (owner={}:{})",
                    owner.block, owner.tx
                )
            });
            let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
            for entry in entries {
                if entry.amount == 0 {
                    continue;
                }
                *agg.entry(entry.alkane).or_default() =
                    agg.get(&entry.alkane).copied().unwrap_or(0).saturating_add(entry.amount);
            }
            if let Some(self_balance) = lookup_self_balance(owner) {
                if self_balance == 0 {
                    agg.remove(owner);
                } else {
                    agg.insert(*owner, self_balance);
                }
            }
            agg
        };

        let mut local_cache: HashMap<SchemaAlkaneId, HashMap<SchemaAlkaneId, u128>> =
            HashMap::new();
        let mut balance_mismatches: Vec<(SchemaAlkaneId, SchemaAlkaneId, u128, u128)> = Vec::new();

        let mut changed_pairs: Vec<(SchemaAlkaneId, SchemaAlkaneId)> = Vec::new();
        for (owner, per_token) in &alkane_balance_delta {
            for (token, delta) in per_token {
                if delta.is_zero() {
                    continue;
                }
                changed_pairs.push((*owner, *token));
            }
        }
        changed_pairs.sort();
        changed_pairs.dedup();

        for (owner, token) in changed_pairs {
            if !local_cache.contains_key(&owner) {
                let balances = balances_from_rows(&owner);
                local_cache.insert(owner, balances);
            }
            let local_balance = local_cache
                .get(&owner)
                .and_then(|m| m.get(&token).copied())
                .unwrap_or(0);

            let metashrew_balance = match metashrew.get_reserves_for_alkane_with_db(
                sdb,
                &owner,
                &token,
                Some(height_u64),
            ) {
                Ok(Some(bal)) => bal,
                Ok(None) => 0,
                Err(e) => {
                    panic!(
                        "[balances][strict] metashrew lookup failed (owner={}:{}, token={}:{}, height={}): {e:?}",
                        owner.block, owner.tx, token.block, token.tx, height_u64
                    );
                }
            };

            if local_balance != metashrew_balance {
                balance_mismatches.push((owner, token, local_balance, metashrew_balance));
            }
        }

        let to_balance_map = |entries: &[BalanceEntry]| -> BTreeMap<SchemaAlkaneId, u128> {
            let mut out = BTreeMap::new();
            for entry in entries {
                if entry.amount == 0 {
                    continue;
                }
                *out.entry(entry.alkane).or_default() = out
                    .get(&entry.alkane)
                    .copied()
                    .unwrap_or(0u128)
                    .saturating_add(entry.amount);
            }
            out
        };
        let fmt_sheet = |sheet: &BTreeMap<SchemaAlkaneId, u128>| -> String {
            if sheet.is_empty() {
                return "empty".to_string();
            }
            sheet
                .iter()
                .map(|(id, amt)| format!("{}:{}={}", id.block, id.tx, amt))
                .collect::<Vec<_>>()
                .join(", ")
        };
        let parse_txid = |txid_bytes: &[u8]| -> Result<Txid> {
            if txid_bytes.len() != 32 {
                return Err(anyhow!("invalid txid length {}", txid_bytes.len()));
            }
            let mut arr = [0u8; 32];
            arr.copy_from_slice(txid_bytes);
            Ok(Txid::from_byte_array(arr))
        };

        struct UtxoMismatch {
            outpoint: EspoOutpoint,
            addr: String,
            local: BTreeMap<SchemaAlkaneId, u128>,
            metashrew: BTreeMap<SchemaAlkaneId, u128>,
        }
        let mut utxo_mismatches: Vec<UtxoMismatch> = Vec::new();

        for row in &new_rows {
            if row.outpoint.tx_spent.is_some() {
                continue;
            }
            let txid = parse_txid(&row.outpoint.txid).unwrap_or_else(|e| {
                panic!(
                    "[balances][strict] invalid outpoint txid bytes ({}:{}): {e}",
                    row.outpoint.as_outpoint_string(),
                    row.outpoint.vout
                )
            });
            let local_entries = decode_balances_vec(&row.enc_balances).unwrap_or_else(|e| {
                panic!(
                    "[balances][strict] decode outpoint balances failed ({}:{}): {e:?}",
                    row.outpoint.as_outpoint_string(),
                    row.outpoint.vout
                )
            });
            let local_map = to_balance_map(&local_entries);

            let meta_entries = metashrew
                .get_outpoint_alkane_balances_with_db(sdb, &txid, row.outpoint.vout)
                .unwrap_or_else(|e| {
                    panic!(
                        "[balances][strict] metashrew outpoint lookup failed ({}:{}): {e:?}",
                        row.outpoint.as_outpoint_string(),
                        row.outpoint.vout
                    )
                });
            let mut meta_map = BTreeMap::new();
            for (id, amount) in meta_entries {
                if amount == 0 {
                    continue;
                }
                let schema = schema_id_from_parts(id.block, id.tx).unwrap_or_else(|e| {
                    panic!(
                        "[balances][strict] invalid metashrew alkane id ({}:{}): {e:?}",
                        id.block, id.tx
                    )
                });
                *meta_map.entry(schema).or_default() = meta_map
                    .get(&schema)
                    .copied()
                    .unwrap_or(0u128)
                    .saturating_add(amount);
            }

            if local_map != meta_map {
                utxo_mismatches.push(UtxoMismatch {
                    outpoint: row.outpoint.clone(),
                    addr: row.addr.clone(),
                    local: local_map,
                    metashrew: meta_map,
                });
            }
        }

        if !balance_mismatches.is_empty() || !utxo_mismatches.is_empty() {
            for (owner, token, local_balance, metashrew_balance) in &balance_mismatches {
                eprintln!(
                    "[balances][strict] mismatch height={} owner={}:{} token={}:{} local={} metashrew={}",
                    height_u64,
                    owner.block,
                    owner.tx,
                    token.block,
                    token.tx,
                    local_balance,
                    metashrew_balance
                );

                let mut txids: Vec<String> = alkane_balance_tx_entries_by_token
                    .get(&(*owner, *token))
                    .map(|entries| {
                        entries
                            .iter()
                            .map(|entry| Txid::from_byte_array(entry.txid).to_string())
                            .collect()
                    })
                    .unwrap_or_default();
                txids.sort();
                txids.dedup();

                if txids.is_empty() {
                    eprintln!(
                        "[balances][strict] balance-change txids: none (owner={}:{}, token={}:{})",
                        owner.block, owner.tx, token.block, token.tx
                    );
                } else {
                    eprintln!(
                        "[balances][strict] balance-change txids: {}",
                        txids.join(",")
                    );
                }
            }

            for mismatch in &utxo_mismatches {
                eprintln!(
                    "[balances][strict] utxo mismatch outpoint={} addr={} local=[{}] metashrew=[{}]",
                    mismatch.outpoint.as_outpoint_string(),
                    mismatch.addr,
                    fmt_sheet(&mismatch.local),
                    fmt_sheet(&mismatch.metashrew)
                );
            }

            panic!(
                "[balances][strict] metashrew mismatch at height {} (alkanes={} utxos={})",
                height_u64,
                balance_mismatches.len(),
                utxo_mismatches.len()
            );
        }
    }

    provider.set_batch(SetBatchParams { puts, deletes })?;

    let minus_total: u128 = stat_minus_by_alk.values().copied().sum();
    let plus_total: u128 = stat_plus_by_alk.values().copied().sum();

    eprintln!(
        "[balances] block #{}, txs={}, outpoints_written={}, outpoints_marked_spent={}, alkanes_added={}, alkanes_removed={}, unique_add={}, unique_remove={}",
        block.height,
        block.transactions.len(),
        stat_outpoints_written,
        stat_outpoints_marked_spent,
        plus_total,
        minus_total,
        stat_plus_by_alk.len(),
        stat_minus_by_alk.len()
    );
    eprintln!("[balances] <<< end   block #{}", block.height);

    Ok(())
}

fn lookup_self_balance(alk: &SchemaAlkaneId) -> Option<u128> {
    match get_metashrew().get_reserves_for_alkane(alk, alk, None) {
        Ok(val) => val,
        Err(e) => {
            eprintln!(
                "[balances] WARN: self-balance lookup failed for {}:{} ({e:?})",
                alk.block, alk.tx
            );
            None
        }
    }
}

pub fn get_balance_for_address(
    provider: &EssentialsProvider,
    address: &str,
) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let mut prefix = b"/balances/".to_vec();
    prefix.extend_from_slice(address.as_bytes());
    prefix.push(b'/');

    let keys = provider
        .get_scan_prefix(GetScanPrefixParams { prefix: prefix.clone() })?
        .keys;
    let vals = provider
        .get_multi_values(GetMultiValuesParams { keys: keys.clone() })?
        .values;

    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();
    for (k, v) in keys.iter().zip(vals) {
        if let Some(bytes) = v {
            // decode outpoint from key to determine spend status
            if k.len() <= prefix.len() {
                continue;
            }
            let Ok(op) = EspoOutpoint::try_from_slice(&k[prefix.len()..]) else { continue };
            if op.tx_spent.is_some() {
                continue; // skip spent outpoints when computing live balance
            }
            if let Ok(bals) = decode_balances_vec(&bytes) {
                for be in bals {
                    *agg.entry(be.alkane).or_default() =
                        agg.get(&be.alkane).copied().unwrap_or(0).saturating_add(be.amount);
                }
            }
        }
    }
    Ok(agg)
}

pub fn get_alkane_balances(
    provider: &EssentialsProvider,
    owner: &SchemaAlkaneId,
) -> Result<HashMap<SchemaAlkaneId, u128>> {
    let table = provider.table();
    let key = table.alkane_balances_key(owner);
    let mut agg: HashMap<SchemaAlkaneId, u128> = HashMap::new();

    if let Some(bytes) = provider.get_raw_value(GetRawValueParams { key })?.value {
        if let Ok(bals) = decode_balances_vec(&bytes) {
            for be in bals {
                if be.amount == 0 {
                    continue;
                }
                *agg.entry(be.alkane).or_default() =
                    agg.get(&be.alkane).copied().unwrap_or(0).saturating_add(be.amount);
            }
        }
    }

    if let Some(self_balance) = lookup_self_balance(owner) {
        if self_balance == 0 {
            agg.remove(owner);
        } else {
            agg.insert(*owner, self_balance);
        }
    }

    Ok(agg)
}

/// Get balance at a specific height (for strict mode validation)
pub fn get_alkane_balance_at_height(
    mdb: &Mdb,
    owner: &SchemaAlkaneId,
    token: &SchemaAlkaneId,
    height: u32,
) -> Result<u128> {
    if !mdb.has_height_indexed() {
        return Err(anyhow!("height-indexed storage not enabled"));
    }

    let key = build_alkane_balances_key(owner);
    if let Some(bytes) = mdb.get_at_height(&key, height)? {
        if let Ok(bals) = decode_balances_vec(&bytes) {
            for be in bals {
                if be.alkane == *token {
                    return Ok(be.amount);
                }
            }
        }
    }

    if owner == token {
        if let Some(self_balance) = lookup_self_balance(owner) {
            return Ok(self_balance);
        }
    }

    Ok(0)
}

/// Validate balance history in strict mode with historical lookups
///
/// This function validates that an owner's balance for a specific token at a given height
/// matches the expected balance. This is useful for strict mode validation where we want
/// to ensure consistency with historical data.
pub fn validate_balance_history_strict(
    mdb: &Mdb,
    owner: &SchemaAlkaneId,
    token: &SchemaAlkaneId,
    expected_balance: u128,
    height: u32,
) -> Result<()> {
    if !is_strict_mode() {
        return Ok(());
    }

    if !mdb.has_height_indexed() {
        eprintln!(
            "[balances][strict] height-indexed storage not enabled; skipping historical validation"
        );
        return Ok(());
    }

    let stored_balance = get_alkane_balance_at_height(mdb, owner, token, height)?;

    if stored_balance != expected_balance {
        return Err(anyhow!(
            "strict mode: balance mismatch at height {} for owner {}:{} token {}:{} - expected {} but found {}",
            height,
            owner.block,
            owner.tx,
            token.block,
            token.tx,
            expected_balance,
            stored_balance
        ));
    }

    Ok(())
}

#[derive(Default, Clone, Debug)]
pub struct OutpointLookup {
    pub balances: Vec<BalanceEntry>,
    pub spent_by: Option<Txid>,
}

pub fn get_outpoint_balances(
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
) -> Result<Vec<BalanceEntry>> {
    let table = provider.table();
    let pref = table.outpoint_balances_prefix(txid.as_byte_array().as_slice(), vout)?;
    let keys = provider
        .get_scan_prefix(GetScanPrefixParams { prefix: pref.clone() })?
        .keys;
    if keys.is_empty() {
        return Ok(Vec::new());
    }
    let vals = provider
        .get_multi_values(GetMultiValuesParams { keys: keys.clone() })?
        .values;
    for (_k, v) in keys.into_iter().zip(vals) {
        if let Some(bytes) = v {
            if let Ok(bals) = decode_balances_vec(&bytes) {
                return Ok(bals);
            }
        }
    }
    Ok(Vec::new())
}

pub fn get_outpoint_balances_with_spent(
    provider: &EssentialsProvider,
    txid: &Txid,
    vout: u32,
) -> Result<OutpointLookup> {
    const OUTPOINT_BALANCES_PREFIX: &[u8] = b"/outpoint_balances/";

    let table = provider.table();
    let pref = table.outpoint_balances_prefix(txid.as_byte_array().as_slice(), vout)?;
    let keys = provider
        .get_scan_prefix(GetScanPrefixParams { prefix: pref.clone() })?
        .keys;
    if keys.is_empty() {
        return Ok(OutpointLookup::default());
    }

    let vals = provider
        .get_multi_values(GetMultiValuesParams { keys: keys.clone() })?
        .values;
    let mut fallback: Option<OutpointLookup> = None;

    for (k, v) in keys.into_iter().zip(vals) {
        let Some(bytes) = v else { continue };

        let spent_by = if k.len() > OUTPOINT_BALANCES_PREFIX.len() {
            EspoOutpoint::try_from_slice(&k[OUTPOINT_BALANCES_PREFIX.len()..])
                .ok()
                .and_then(|op| op.tx_spent.and_then(|t| Txid::from_slice(&t).ok()))
        } else {
            None
        };

        if let Ok(balances) = decode_balances_vec(&bytes) {
            let lookup = OutpointLookup { balances, spent_by };
            if lookup.spent_by.is_some() {
                return Ok(lookup);
            }
            if fallback.is_none() {
                fallback = Some(lookup);
            }
        }
    }

    Ok(fallback.unwrap_or_default())
}

pub fn get_outpoint_balances_with_spent_batch(
    provider: &EssentialsProvider,
    outpoints: &[(Txid, u32)],
) -> Result<HashMap<(Txid, u32), OutpointLookup>> {
    const OUTPOINT_BALANCES_PREFIX: &[u8] = b"/outpoint_balances/";

    let mut key_map: Vec<(Vec<u8>, Txid, u32)> = Vec::new();
    let table = provider.table();
    for (txid, vout) in outpoints {
        let pref = table.outpoint_balances_prefix(txid.as_byte_array().as_slice(), *vout)?;
        let keys = provider
            .get_scan_prefix(GetScanPrefixParams { prefix: pref.clone() })?
            .keys;
        for k in keys {
            key_map.push((k, *txid, *vout));
        }
    }

    let mut out: HashMap<(Txid, u32), OutpointLookup> = HashMap::new();
    if key_map.is_empty() {
        return Ok(out);
    }

    let keys: Vec<Vec<u8>> = key_map.iter().map(|(k, _, _)| k.clone()).collect();
    let vals = provider
        .get_multi_values(GetMultiValuesParams { keys: keys.clone() })?
        .values;

    for ((k, txid, vout), val) in key_map.into_iter().zip(vals.into_iter()) {
        let Some(bytes) = val else { continue };
        let spent_by = if k.len() > OUTPOINT_BALANCES_PREFIX.len() {
            EspoOutpoint::try_from_slice(&k[OUTPOINT_BALANCES_PREFIX.len()..])
                .ok()
                .and_then(|op| op.tx_spent.and_then(|t| Txid::from_slice(&t).ok()))
        } else {
            None
        };
        if let Ok(balances) = decode_balances_vec(&bytes) {
            let entry = OutpointLookup { balances, spent_by };
            let slot = out.entry((txid, vout)).or_insert_with(OutpointLookup::default);
            if entry.spent_by.is_some() || slot.balances.is_empty() {
                *slot = entry;
            }
        }
    }

    for (txid, vout) in outpoints {
        out.entry((*txid, *vout)).or_insert_with(OutpointLookup::default);
    }

    Ok(out)
}

pub fn get_holders_for_alkane(
    provider: &EssentialsProvider,
    alk: SchemaAlkaneId,
    page: usize,
    limit: usize,
) -> Result<(usize /*total*/, u128 /*supply*/, Vec<HolderEntry>)> {
    let table = provider.table();
    let key = table.holders_key(&alk);
    let cur = provider.get_raw_value(GetRawValueParams { key })?.value;
    let mut all = match cur {
        Some(bytes) => decode_holders_vec(&bytes).unwrap_or_default(),
        None => Vec::new(),
    };
    if let Some(self_balance) = lookup_self_balance(&alk) {
        if self_balance > 0 {
            if let Some(existing) = all.iter_mut().find(|h| h.holder == HolderId::Alkane(alk)) {
                existing.amount = self_balance;
            } else {
                all.push(HolderEntry { holder: HolderId::Alkane(alk), amount: self_balance });
            }
        } else {
            all.retain(|h| h.holder != HolderId::Alkane(alk));
        }
    }

    all.sort_by(|a, b| match b.amount.cmp(&a.amount) {
        std::cmp::Ordering::Equal => holder_order_key(&a.holder).cmp(&holder_order_key(&b.holder)),
        o => o,
    });
    let total = all.len();
    let supply: u128 = all.iter().map(|h| h.amount).sum();
    let p = page.max(1);
    let l = limit.max(1);
    let off = l.saturating_mul(p - 1);
    let end = (off + l).min(total);
    let slice = if off >= total { vec![] } else { all[off..end].to_vec() };
    Ok((total, supply, slice))
}

pub fn get_scriptpubkey_for_address(
    provider: &EssentialsProvider,
    addr: &str,
) -> Result<Option<ScriptBuf>> {
    let table = provider.table();
    let key = table.addr_spk_key(addr);
    let v = provider.get_raw_value(GetRawValueParams { key })?.value;
    Ok(v.map(ScriptBuf::from))
}
