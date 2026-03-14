use bitcoin::Network;

pub const KEY_INDEX_HEIGHT: &[u8] = b"/index_height";

/// Difficulty adjustment epoch length in blocks.
pub const EPOCH_LENGTH: u128 = 2016;

/// Scale factor for price display (1e8).
pub const PRICE_SCALE: u128 = 100_000_000;

/// Q64 fixed-point: 1.0 = 1 << 64.
pub const ONE_Q64: u128 = 1u128 << 64;

/// Genesis block for the fujin module per network.
pub fn fujin_genesis_block(network: Network) -> u32 {
    match network {
        Network::Bitcoin => 880_000,
        _ => 0,
    }
}
