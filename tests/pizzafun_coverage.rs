#![cfg(not(target_arch = "wasm32"))]

//! Integration tests for the pizzafun module.
//!
//! Exercises series assignment from creation records.

#[cfg(feature = "test-utils")]
mod tests {
    use alkanes_support::cellpack::Cellpack;
    use alkanes_support::id::AlkaneId;
    use anyhow::Result;
    use espo::modules::defs::EspoModule;
    use espo::modules::essentials::main::Essentials;
    use espo::modules::pizzafun::main::Pizzafun;
    use espo::runtime::mdb::Mdb;
    use espo::test_utils::*;
    use rocksdb::{DB, Options};
    use std::sync::Arc;

    fn init_test_config() -> Result<()> {
        let temp_dir = tempfile::tempdir()?;
        let base = temp_dir.path();
        let db_path = base.join("espo_db");
        let metashrew_db = base.join("metashrew_db");
        let blocks_dir = base.join("blocks");
        std::fs::create_dir_all(&db_path)?;
        std::fs::create_dir_all(&metashrew_db)?;
        std::fs::create_dir_all(&blocks_dir)?;
        let mut opts = Options::default();
        opts.create_if_missing(true);
        DB::open(&opts, &db_path)?;
        DB::open(&opts, &metashrew_db)?;
        let config = espo::config::AppConfig {
            readonly_metashrew_db_dir: metashrew_db.to_str().unwrap().to_string(),
            electrum_rpc_url: None,
            metashrew_rpc_url: "http://127.0.0.1:9999".into(),
            electrs_esplora_url: Some("http://127.0.0.1:3000".into()),
            bitcoind_rpc_url: "http://127.0.0.1:8332".into(),
            bitcoind_rpc_user: "test".into(),
            bitcoind_rpc_pass: "test".into(),
            bitcoind_blocks_dir: blocks_dir.to_str().unwrap().to_string(),
            reset_mempool_on_startup: false,
            view_only: true,
            db_path: db_path.to_str().unwrap().to_string(),
            enable_aof: false,
            sdb_poll_ms: 100,
            indexer_block_delay_ms: 0,
            port: 9090,
            explorer_host: None,
            explorer_base_path: "/".into(),
            network: bitcoin::Network::Regtest,
            metashrew_db_label: None,
            strict_mode: None,
            debug: false,
            debug_ignore_ms: 0,
            debug_backup: None,
            safe_tip_hook_script: None,
            block_source_mode: espo::core::blockfetcher::BlockFetchMode::Auto,
            simulate_reorg: false,
            explorer_networks: None,
            modules: std::collections::HashMap::new(),
        };
        std::mem::forget(temp_dir);
        let _ = espo::config::init_config_from(config);
        Ok(())
    }

    fn create_modules() -> Result<(Essentials, Pizzafun, Arc<DB>, tempfile::TempDir)> {
        let temp_dir = tempfile::tempdir()?;
        let db_path = temp_dir.path().join("espo_db");
        let mut opts = Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, &db_path)?);

        let ess_mdb = Arc::new(Mdb::from_db(db.clone(), b"essentials:"));
        let mut essentials = Essentials::new();
        essentials.set_mdb(ess_mdb);

        let pf_mdb = Arc::new(Mdb::from_db(db.clone(), b"pizzafun:"));
        let mut pizzafun = Pizzafun::new();
        pizzafun.set_mdb(pf_mdb);

        Ok((essentials, pizzafun, db, temp_dir))
    }

    fn deploy_token(
        metashrew: &TestMetashrewRuntime,
        essentials: &Essentials,
        pizzafun: &Pizzafun,
        height: u32,
        supply: u128,
    ) -> Result<bitcoin::Block> {
        let wasm = alkanes::precompiled::alkanes_std_owned_token_build::get_bytes();
        let cellpacks = vec![BinaryAndCellpack {
            binary: wasm,
            cellpack: Cellpack {
                target: AlkaneId { block: 1, tx: 0 },
                inputs: vec![0, 1, supply],
            },
        }];
        let block = init_with_cellpack_pairs(cellpacks);
        metashrew.index_block(&block, height)?;
        let traces = metashrew.get_traces_for_block(height)?;
        let espo_block = build_espo_block(height, &block, traces)?;
        essentials.index_block(espo_block.clone())?;
        pizzafun.index_block(espo_block)?;
        Ok(block)
    }

    // ========================================================================

    #[test]
    fn test_pizzafun_indexes_setup_blocks() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();
        init_test_config()?;

        let metashrew = TestMetashrewRuntime::new()?;
        let (essentials, pizzafun, _db, _tmp) = create_modules()?;

        for h in 0..5 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            metashrew.index_block(&block, h)?;
            let traces = metashrew.get_traces_for_block(h)?;
            let espo_block = build_espo_block(h, &block, traces)?;
            essentials.index_block(espo_block.clone())?;
            pizzafun.index_block(espo_block)?;
        }

        assert_eq!(pizzafun.get_index_height(), Some(4));
        println!("[PIZZAFUN] 5 setup blocks indexed OK");
        Ok(())
    }

    #[test]
    fn test_pizzafun_with_token_deploys() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();
        init_test_config()?;

        let metashrew = TestMetashrewRuntime::new()?;
        let (essentials, pizzafun, _db, _tmp) = create_modules()?;

        // Setup blocks
        for h in 0..4 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            metashrew.index_block(&block, h)?;
            let traces = metashrew.get_traces_for_block(h)?;
            let espo_block = build_espo_block(h, &block, traces)?;
            essentials.index_block(espo_block.clone())?;
            pizzafun.index_block(espo_block)?;
        }

        // Deploy tokens - pizzafun should process creation records
        deploy_token(&metashrew, &essentials, &pizzafun, 4, 100)?;
        deploy_token(&metashrew, &essentials, &pizzafun, 5, 200)?;

        assert_eq!(pizzafun.get_index_height(), Some(5));
        println!("[PIZZAFUN] Token deploys indexed OK");
        Ok(())
    }

    #[test]
    fn test_pizzafun_with_amm_deployment() -> Result<()> {
        let rt = tokio::runtime::Runtime::new()?;
        let _guard = rt.enter();
        init_test_config()?;

        let metashrew = TestMetashrewRuntime::new()?;
        let (essentials, pizzafun, _db, _tmp) = create_modules()?;

        // Setup blocks
        for h in 0..4 {
            let block = protorune::test_helpers::create_block_with_coinbase_tx(h);
            metashrew.index_block(&block, h)?;
            let traces = metashrew.get_traces_for_block(h)?;
            let espo_block = build_espo_block(h, &block, traces)?;
            essentials.index_block(espo_block.clone())?;
            pizzafun.index_block(espo_block)?;
        }

        // Deploy AMM (multiple contracts with names)
        let deployment = setup_amm(&metashrew, 4)?;
        for h in 4..=9 {
            if let Some(block) = deployment.blocks.get(&h) {
                let traces = metashrew.get_traces_for_block(h)?;
                let espo_block = build_espo_block(h, block, traces)?;
                essentials.index_block(espo_block.clone())?;
                pizzafun.index_block(espo_block)?;
            }
        }

        let end = *deployment.blocks.keys().max().unwrap_or(&4);
        assert_eq!(pizzafun.get_index_height(), Some(end));
        println!("[PIZZAFUN] AMM deployment indexed OK");
        Ok(())
    }

    #[test]
    fn test_pizzafun_genesis_block() {
        let pizzafun = Pizzafun::new();
        assert_eq!(pizzafun.get_genesis_block(bitcoin::Network::Regtest), 0);
    }
}
