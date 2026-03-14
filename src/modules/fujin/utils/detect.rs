use crate::alkanes::trace::{
    EspoSandshrewLikeTraceInvokeData, EspoSandshrewLikeTraceShortId,
    EspoSandshrewLikeTraceTransfer,
};
use crate::schemas::SchemaAlkaneId;

use super::super::schemas::FujinActivityKind;

/// Which Fujin contract was invoked.
#[derive(Clone, Debug)]
pub enum FujinTarget {
    Factory,
    Pool(SchemaAlkaneId),
    Vault,
    Zap,
}

/// A detected Fujin invocation with its opcode and target.
#[derive(Clone, Debug)]
pub struct FujinInvocation {
    pub target: FujinTarget,
    pub opcode: u64,
    pub incoming_alkanes: Vec<(SchemaAlkaneId, u128)>,
}

/// Known contract IDs to match against.
pub struct FujinContracts {
    pub factory_id: SchemaAlkaneId,
    pub vault_id: SchemaAlkaneId,
    pub zap_id: SchemaAlkaneId,
    pub known_pools: Vec<SchemaAlkaneId>,
}

impl FujinContracts {
    pub fn match_invoke(&self, invoke: &EspoSandshrewLikeTraceInvokeData) -> Option<FujinInvocation> {
        let myself = parse_trace_id(&invoke.context.myself)?;
        let opcode = parse_opcode(&invoke.context.inputs)?;

        let target = if myself == self.factory_id {
            FujinTarget::Factory
        } else if myself == self.vault_id {
            FujinTarget::Vault
        } else if myself == self.zap_id {
            FujinTarget::Zap
        } else if self.known_pools.contains(&myself) {
            FujinTarget::Pool(myself)
        } else {
            return None;
        };

        let incoming_alkanes = parse_incoming_alkanes(&invoke.context.incoming_alkanes);

        Some(FujinInvocation { target, opcode, incoming_alkanes })
    }
}

/// Map (target, opcode) → FujinActivityKind.
pub fn classify_activity(target: &FujinTarget, opcode: u64) -> Option<FujinActivityKind> {
    match target {
        FujinTarget::Pool(_) => match opcode {
            1 => Some(FujinActivityKind::AddLiquidity),
            2 => Some(FujinActivityKind::WithdrawAndBurn),
            3 => Some(FujinActivityKind::Swap),
            11 => Some(FujinActivityKind::MintPair),
            12 => Some(FujinActivityKind::BurnPair),
            13 => Some(FujinActivityKind::StartEvent),
            14 => Some(FujinActivityKind::Redeem),
            _ => None,
        },
        FujinTarget::Vault => match opcode {
            1 => Some(FujinActivityKind::VaultDeposit),
            2 => Some(FujinActivityKind::VaultWithdraw),
            3 => Some(FujinActivityKind::VaultRollover),
            4 => Some(FujinActivityKind::VaultWithdrawDiesel),
            _ => None,
        },
        FujinTarget::Zap => match opcode {
            1 => Some(FujinActivityKind::ZapDieselToLp),
            4 => Some(FujinActivityKind::ZapDieselToLong),
            5 => Some(FujinActivityKind::ZapDieselToShort),
            6 => Some(FujinActivityKind::UnzapLongToDiesel),
            7 => Some(FujinActivityKind::UnzapShortToDiesel),
            _ => None,
        },
        FujinTarget::Factory => None,
    }
}

// ── Helpers ──

pub fn parse_trace_id(id: &EspoSandshrewLikeTraceShortId) -> Option<SchemaAlkaneId> {
    let block = parse_hex_u32(&id.block)?;
    let tx = parse_hex_u64(&id.tx)?;
    Some(SchemaAlkaneId { block, tx })
}

fn parse_opcode(inputs: &[String]) -> Option<u64> {
    inputs.first().and_then(|s| parse_hex_u64(s))
}

fn parse_incoming_alkanes(
    transfers: &[EspoSandshrewLikeTraceTransfer],
) -> Vec<(SchemaAlkaneId, u128)> {
    let mut out = Vec::new();
    for t in transfers {
        let Some(id) = parse_trace_id(&t.id) else { continue };
        let Some(value) = parse_hex_u128(&t.value) else { continue };
        out.push((id, value));
    }
    out
}

pub fn parse_hex_u32(s: &str) -> Option<u32> {
    s.strip_prefix("0x")
        .and_then(|h| u32::from_str_radix(h, 16).ok())
        .or_else(|| s.parse::<u32>().ok())
}

pub fn parse_hex_u64(s: &str) -> Option<u64> {
    s.strip_prefix("0x")
        .and_then(|h| u64::from_str_radix(h, 16).ok())
        .or_else(|| s.parse::<u64>().ok())
}

pub fn parse_hex_u128(s: &str) -> Option<u128> {
    s.strip_prefix("0x")
        .and_then(|h| u128::from_str_radix(h, 16).ok())
        .or_else(|| s.parse::<u128>().ok())
}

/// Extract total amount for a specific alkane from a list of transfers.
pub fn extract_amount_for_alkane(
    transfers: &[EspoSandshrewLikeTraceTransfer],
    target: SchemaAlkaneId,
) -> Option<u128> {
    let mut found = false;
    let mut total: u128 = 0;
    for t in transfers {
        let Some(id) = parse_trace_id(&t.id) else { continue };
        if id != target {
            continue;
        }
        let Some(value) = parse_hex_u128(&t.value) else { continue };
        found = true;
        total = total.saturating_add(value);
    }
    if found { Some(total) } else { None }
}
