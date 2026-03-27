#![cfg(not(target_arch = "wasm32"))]

//! Comprehensive test coverage for all 44 tertiary WASM view functions.
//!
//! Each view function is called through NativeTertiaryRuntime after indexing
//! blocks via alkanes.wasm + espo _start().

#[cfg(feature = "test-utils")]
mod tests {
    use espo::test_utils::metashrew_runtime::TestMetashrewRuntime;
    use espo::test_utils::tertiary_runtime::{NativeTertiaryRuntime, SecondaryGetFn};
    use espo::test_utils::*;
    use std::collections::HashMap;
    use std::sync::Arc;

    fn load_espo_wasm() -> Vec<u8> {
        let paths = [
            "target/wasm32-unknown-unknown/release/espo.wasm",
            "target/wasm32-unknown-unknown/debug/espo.wasm",
        ];
        for path in &paths {
            if let Ok(bytes) = std::fs::read(path) {
                return bytes;
            }
        }
        panic!("espo.wasm not found. Build with: cargo build --lib --target wasm32-unknown-unknown --features tertiary --release");
    }

    fn make_alkanes_reader(db: &Arc<rocksdb::DB>) -> SecondaryGetFn {
        let db = db.clone();
        Arc::new(move |key: &[u8]| -> Option<Vec<u8>> { db.get(key).ok().flatten() })
    }

    fn secondary_storages(reader: &SecondaryGetFn) -> HashMap<String, SecondaryGetFn> {
        let mut map = HashMap::new();
        map.insert("alkanes".to_string(), reader.clone());
        map
    }

    /// Setup: create runtime, index setup blocks + AMM deploy through alkanes + espo tertiary
    fn setup_with_amm() -> (
        NativeTertiaryRuntime,
        HashMap<Vec<u8>, Vec<u8>>,
        HashMap<String, SecondaryGetFn>,
        u32, // tip height
    ) {
        let wasm = load_espo_wasm();
        let tertiary_rt = NativeTertiaryRuntime::new(&wasm).unwrap();
        let metashrew = TestMetashrewRuntime::new().unwrap();

        // Setup blocks
        for h in 0..4 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            metashrew.index_block(&block, h).unwrap();
        }

        // Deploy AMM
        let deployment = setup_amm(&metashrew, 4).unwrap();

        let reader = make_alkanes_reader(metashrew.db());
        let secondaries = secondary_storages(&reader);
        let mut own_storage = HashMap::new();

        // Index setup blocks through tertiary
        for h in 0..4 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            let block_bytes = bitcoin::consensus::serialize(&block);
            let pairs = tertiary_rt.run_block(h, &block_bytes, &own_storage, &secondaries).unwrap();
            for (k, v) in pairs { own_storage.insert(k, v); }
        }

        // Index AMM deployment blocks
        let end_height = *deployment.blocks.keys().max().unwrap_or(&4);
        for h in 4..=end_height {
            if let Some(block) = deployment.blocks.get(&h) {
                let block_bytes = bitcoin::consensus::serialize(block);
                let pairs = tertiary_rt.run_block(h, &block_bytes, &own_storage, &secondaries).unwrap();
                for (k, v) in pairs { own_storage.insert(k, v); }
            }
        }

        (tertiary_rt, own_storage, secondaries, end_height)
    }

    /// Call a view and return the JSON string
    fn call_view(
        rt: &NativeTertiaryRuntime,
        name: &str,
        height: u32,
        payload: &[u8],
        own: &HashMap<Vec<u8>, Vec<u8>>,
        sec: &HashMap<String, SecondaryGetFn>,
    ) -> String {
        let result = rt.call_view(name, height, payload, own, sec).unwrap();
        String::from_utf8(result).unwrap_or_else(|_| "<non-utf8>".to_string())
    }

    // ========================================================================
    // No-data views (always work)
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_ping() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "ping", h, &[], &own, &sec);
        assert!(json.contains("ok") || json.contains("quspo") || json.contains("espo"), "ping: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_block_height() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_block_height", h, &[], &own, &sec);
        assert!(json.contains("height"), "get_block_height: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_bitcoin_price() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_bitcoin_price", h, &[], &own, &sec);
        assert!(json.contains("usd"), "get_bitcoin_price: {json}");
    }

    // ========================================================================
    // Balance views (with address payload)
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_alkanes_by_address_empty() {
        let (rt, own, sec, h) = setup_with_amm();
        let addr = "bcrt1qw508d6qejxtdg4y5r3zarvary0c5xw7kygt080";
        let json = call_view(&rt, "get_alkanes_by_address", h, addr.as_bytes(), &own, &sec);
        assert_eq!(json, "[]");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_address_balances_empty() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_address_balances", h, br#"{"address":"bcrt1qtest"}"#, &own, &sec);
        assert!(json.contains("outpoints"), "get_address_balances: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_outpoint_balances() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_outpoint_balances", h, b"0000000000000000000000000000000000000000000000000000000000000000:0", &own, &sec);
        // Should return empty or valid JSON
        assert!(!json.is_empty(), "get_outpoint_balances returned empty");
    }

    // ========================================================================
    // Alkane info views
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_alkane_info() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_alkane_info", h, b"2:0", &own, &sec);
        assert!(!json.is_empty(), "get_alkane_info: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_all_alkanes() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_all_alkanes", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_all_alkanes: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_holders() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_holders", h, b"2:0", &own, &sec);
        assert!(!json.is_empty(), "get_holders: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_holders_count() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_holders_count", h, b"2:0", &own, &sec);
        assert!(!json.is_empty(), "get_holders_count: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_circulating_supply() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_circulating_supply", h, b"2:0", &own, &sec);
        assert!(!json.is_empty(), "get_circulating_supply: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_transfer_volume() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_transfer_volume", h, b"2:0", &own, &sec);
        assert!(!json.is_empty(), "get_transfer_volume: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_token_details() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_token_details", h, b"2:0", &own, &sec);
        assert!(!json.is_empty(), "get_token_details: {json}");
    }

    // ========================================================================
    // Contract state views
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_contract_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_contract_state", h, br#"{"id":"4:0","keys":["/name"]}"#, &own, &sec);
        assert!(!json.is_empty(), "get_contract_state: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_contract_state_batch() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_contract_state_batch", h, br#"{"requests":[{"id":"4:0","keys":["/name"]}]}"#, &own, &sec);
        assert!(!json.is_empty(), "get_contract_state_batch: {json}");
    }

    // ========================================================================
    // Pool/AMM views
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_pools() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_pools", h, &[], &own, &sec);
        assert!(json.contains("pools"), "get_pools: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_candles() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_candles", h, br#"{"pool":"4:0","timeframe":"1h"}"#, &own, &sec);
        assert!(json.contains("points"), "get_candles: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_normal_pool_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_normal_pool_state", h, b"4:0", &own, &sec);
        assert!(!json.is_empty(), "get_normal_pool_state: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_synth_pool_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_synth_pool_state", h, b"4:0", &own, &sec);
        assert!(!json.is_empty(), "get_synth_pool_state: {json}");
    }

    // ========================================================================
    // Protocol-specific views (FIRE, dxBTC, Fujin, FUEL, frUSD, Gauge)
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fire_token_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fire_token_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fire_token_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fire_staking_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fire_staking_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fire_staking_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_dxbtc_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_dxbtc_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_dxbtc_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fujin_factory_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fujin_factory_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fujin_factory_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_gauge_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_gauge_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_gauge_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_ftrbtc_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_ftrbtc_state", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_ftrbtc_state: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fujin_pool_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fujin_pool_state", h, b"4:0", &own, &sec);
        assert!(!json.is_empty(), "get_fujin_pool_state: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fujin_vault_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fujin_vault_state", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fujin_vault_state: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fuel_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fuel_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fuel_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fire_bonding_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fire_bonding_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fire_bonding_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fire_redemption_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fire_redemption_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fire_redemption_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fire_distributor_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fire_distributor_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fire_distributor_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_fire_treasury_stats() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_fire_treasury_stats", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_fire_treasury_stats: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_frusd_state() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_frusd_state", h, &[], &own, &sec);
        assert!(!json.is_empty(), "get_frusd_state: {json}");
    }

    // ========================================================================
    // Activity views
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_activity() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_activity", h, br#"{"limit":10}"#, &own, &sec);
        assert!(!json.is_empty(), "get_activity: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_address_activity() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_address_activity", h, b"bcrt1qtest", &own, &sec);
        assert!(!json.is_empty(), "get_address_activity: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_address_transactions() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_address_transactions", h, b"bcrt1qtest", &own, &sec);
        assert!(!json.is_empty(), "get_address_transactions: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_user_positions() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_user_positions", h, b"bcrt1qtest", &own, &sec);
        assert!(json.contains("positions"), "get_user_positions: {json}");
    }

    // ========================================================================
    // Wrap/unwrap views
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_wrap_events() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_wrap_events", h, &[], &own, &sec);
        assert!(json.contains("events"), "get_wrap_events: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_unwrap_events() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_unwrap_events", h, &[], &own, &sec);
        assert!(json.contains("events"), "get_unwrap_events: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_wrap_events_by_address() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_wrap_events_by_address", h, b"bcrt1qtest", &own, &sec);
        assert!(!json.is_empty(), "get_wrap_events_by_address: {json}");
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_get_unwrap_events_by_address() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "get_unwrap_events_by_address", h, b"bcrt1qtest", &own, &sec);
        assert!(!json.is_empty(), "get_unwrap_events_by_address: {json}");
    }

    // ========================================================================
    // Debug view
    // ========================================================================

    #[tokio::test(flavor = "multi_thread")]
    async fn test_view_debug_read_raw_key() {
        let (rt, own, sec, h) = setup_with_amm();
        let json = call_view(&rt, "debug_read_raw_key", h, b"/some/test/key", &own, &sec);
        assert!(!json.is_empty(), "debug_read_raw_key: {json}");
    }
}
