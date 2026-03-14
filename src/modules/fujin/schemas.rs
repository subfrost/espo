use borsh::{BorshDeserialize, BorshSerialize};
use crate::schemas::SchemaAlkaneId;

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SchemaEpochInfo {
    pub epoch: u128,
    pub pool_id: SchemaAlkaneId,
    pub long_id: SchemaAlkaneId,
    pub short_id: SchemaAlkaneId,
    pub creation_height: u32,
    pub creation_ts: u64,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct SchemaPoolState {
    pub epoch: u128,
    pub reserve_long: u128,
    pub reserve_short: u128,
    pub diesel_locked: u128,
    pub total_fee_per_1000: u128,
    pub lp_total_supply: u128,
    pub start_bits: u32,
    pub end_height: u128,
    pub settled: bool,
    pub long_payout_q64: u128,
    pub short_payout_q64: u128,
    pub long_price_scaled: u128,
    pub short_price_scaled: u128,
    pub blocks_remaining: u64,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct SchemaVaultState {
    pub factory_id: SchemaAlkaneId,
    pub pool_id: SchemaAlkaneId,
    pub lp_balance: u128,
    pub total_supply: u128,
    pub share_price_scaled: u128,
}

#[derive(Clone, Debug, Default, BorshSerialize, BorshDeserialize)]
pub struct SchemaFujinSnapshot {
    pub epochs: Vec<SchemaEpochInfo>,
    pub pool_states: Vec<SchemaPoolState>,
    pub vault_state: SchemaVaultState,
    pub last_height: u32,
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SchemaSettlementV1 {
    pub epoch: u128,
    pub pool_id: SchemaAlkaneId,
    pub start_bits: u32,
    pub end_bits: u32,
    pub long_payout_q64: u128,
    pub short_payout_q64: u128,
    pub settled_height: u32,
    pub difficulty_change_pct: i64,
}

#[derive(Clone, Debug, Copy, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
#[borsh(use_discriminant = true)]
#[repr(u8)]
pub enum FujinActivityKind {
    Swap = 0,
    AddLiquidity = 1,
    WithdrawAndBurn = 2,
    MintPair = 3,
    BurnPair = 4,
    StartEvent = 5,
    Redeem = 6,
    VaultDeposit = 7,
    VaultWithdraw = 8,
    VaultRollover = 9,
    VaultWithdrawDiesel = 10,
    ZapDieselToLp = 11,
    ZapDieselToLong = 12,
    ZapDieselToShort = 13,
    UnzapLongToDiesel = 14,
    UnzapShortToDiesel = 15,
}

impl FujinActivityKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Swap => "swap",
            Self::AddLiquidity => "add_liquidity",
            Self::WithdrawAndBurn => "withdraw_and_burn",
            Self::MintPair => "mint_pair",
            Self::BurnPair => "burn_pair",
            Self::StartEvent => "start_event",
            Self::Redeem => "redeem",
            Self::VaultDeposit => "vault_deposit",
            Self::VaultWithdraw => "vault_withdraw",
            Self::VaultRollover => "vault_rollover",
            Self::VaultWithdrawDiesel => "vault_withdraw_diesel",
            Self::ZapDieselToLp => "zap_diesel_to_lp",
            Self::ZapDieselToLong => "zap_diesel_to_long",
            Self::ZapDieselToShort => "zap_diesel_to_short",
            Self::UnzapLongToDiesel => "unzap_long_to_diesel",
            Self::UnzapShortToDiesel => "unzap_short_to_diesel",
        }
    }
}

#[derive(Clone, Debug, BorshSerialize, BorshDeserialize)]
pub struct SchemaFujinActivityV1 {
    pub timestamp: u64,
    pub txid: [u8; 32],
    pub kind: FujinActivityKind,
    pub pool_id: SchemaAlkaneId,
    pub epoch: u128,
    pub long_delta: u128,
    pub short_delta: u128,
    pub diesel_delta: u128,
    pub lp_delta: u128,
    pub address_spk: Vec<u8>,
    pub success: bool,
}
