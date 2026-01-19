#![cfg(not(target_arch = "wasm32"))]

// Basic test harness demonstrating ESPO infrastructure testing
// This tests the components without requiring full metashrew runtime

mod common;

use espo::test_utils::{ChainBuilder, MockBitcoinNode, TestConfigBuilder};
use espo::runtime::block_metadata::BlockMetadata;
use espo::runtime::height_indexed_storage::{HeightIndexedStorage, RocksHeightIndexedStorage};
use rocksdb::{DB, Options};
use std::sync::Arc;

#[test]
fn test_block_metadata_integration() {
    // Create a test database
    let (config, _temp_dirs) = TestConfigBuilder::new()
        .with_height_indexed(true)
        .build();

    let espo_path = std::path::Path::new(&config.db_path).join("test_metadata");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(DB::open(&opts, espo_path).unwrap());

    // Initialize block metadata
    let metadata = BlockMetadata::new(db);

    // Generate a test chain
    let blocks = ChainBuilder::new()
        .add_blocks(10)
        .build();

    // Index blocks
    for (height, block) in blocks.iter().enumerate() {
        let hash = block.block_hash().to_string();
        metadata.store_block_hash(height as u32, &hash).unwrap();
        metadata.set_indexed_height(height as u32).unwrap();
    }

    // Verify indexed data
    assert_eq!(metadata.get_indexed_height().unwrap(), Some(10));

    // Verify block hashes
    for (height, block) in blocks.iter().enumerate() {
        let expected_hash = block.block_hash().to_string();
        let stored_hash = metadata.get_block_hash(height as u32).unwrap().unwrap();
        assert_eq!(stored_hash, expected_hash);
    }

    // Verify we can delete hashes from a certain height
    metadata.delete_hashes_from(5).unwrap();

    // Hashes 5-10 should be gone
    for height in 5..=10 {
        assert_eq!(metadata.get_block_hash(height).unwrap(), None);
    }

    // Hashes 0-4 should still exist
    for height in 0..5 {
        assert!(metadata.get_block_hash(height).unwrap().is_some());
    }
}

#[test]
fn test_height_indexed_storage_integration() {
    // Create a test database
    let (config, _temp_dirs) = TestConfigBuilder::new()
        .with_height_indexed(true)
        .build();

    let test_path = std::path::Path::new(&config.db_path).join("test_height_indexed");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(DB::open(&opts, test_path).unwrap());

    // Create height-indexed storage
    let storage = RocksHeightIndexedStorage::new(db, b"test_balances");

    // Simulate balance updates over time
    storage.put(b"user1:token1", b"100", 10).unwrap();
    storage.put(b"user1:token1", b"200", 20).unwrap();
    storage.put(b"user1:token1", b"300", 30).unwrap();

    // Query historical balances
    assert_eq!(
        storage.get_at_height(b"user1:token1", 15).unwrap(),
        Some(b"100".to_vec())
    );
    assert_eq!(
        storage.get_at_height(b"user1:token1", 25).unwrap(),
        Some(b"200".to_vec())
    );
    assert_eq!(
        storage.get_current(b"user1:token1").unwrap(),
        Some(b"300".to_vec())
    );

    // Test rollback
    storage.rollback_to_height(20).unwrap();
    assert_eq!(
        storage.get_current(b"user1:token1").unwrap(),
        Some(b"200".to_vec())
    );
}

#[test]
fn test_mock_node_with_metadata() {
    let mut node = MockBitcoinNode::new();

    // Build initial chain
    let blocks = ChainBuilder::new()
        .add_blocks(20)
        .build();

    node.set_chain(blocks.clone());

    // Create metadata store
    let (config, _temp_dirs) = TestConfigBuilder::new().build();
    let test_path = std::path::Path::new(&config.db_path).join("test_mock_metadata");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(DB::open(&opts, test_path).unwrap());
    let metadata = BlockMetadata::new(db);

    // Index all blocks
    for (height, block) in blocks.iter().enumerate() {
        let hash = block.block_hash().to_string();
        metadata.store_block_hash(height as u32, &hash).unwrap();
        metadata.set_indexed_height(height as u32).unwrap();
    }

    // Simulate a reorg by applying new blocks
    let fork_blocks = ChainBuilder::new()
        .add_blocks(15)
        .fork(5)
        .with_salt(1)
        .add_blocks(8)
        .build();

    let new_blocks: Vec<_> = fork_blocks.into_iter().skip(15).collect();
    node.apply_reorg(15, new_blocks.clone());

    // Verify the reorg was applied correctly
    assert!(node.get_tip_height() >= 15);

    // The node should now have different blocks from height 15 onwards
    let original_hash = blocks[15].block_hash();
    let new_hash = node.get_block_by_height(15).unwrap().block_hash();
    assert_ne!(original_hash, new_hash, "Block hash should differ after reorg");
}

#[test]
fn test_multi_user_balances_over_time() {
    let (config, _temp_dirs) = TestConfigBuilder::new()
        .with_height_indexed(true)
        .build();

    let test_path = std::path::Path::new(&config.db_path).join("test_multi_user");
    let mut opts = Options::default();
    opts.create_if_missing(true);
    let db = Arc::new(DB::open(&opts, test_path).unwrap());

    let storage = RocksHeightIndexedStorage::new(db, b"balances");

    // Simulate multiple users and tokens over time
    let users = vec!["alice", "bob", "charlie"];
    let tokens = vec!["token_a", "token_b"];

    let mut height = 100u32;
    for user in &users {
        for token in &tokens {
            let key = format!("{}:{}", user, token);
            let value = format!("balance_{}_{}_{}", user, token, height);

            storage.put(key.as_bytes(), value.as_bytes(), height).unwrap();
            height += 10;
        }
    }

    // Query balances at specific heights
    let alice_token_a_key = b"alice:token_a";
    let alice_balance_early = storage
        .get_at_height(alice_token_a_key, 105)
        .unwrap()
        .unwrap();
    assert_eq!(
        alice_balance_early,
        b"balance_alice_token_a_100".to_vec()
    );

    // Query current state
    let bob_token_b_key = b"bob:token_b";
    let bob_current = storage.get_current(bob_token_b_key).unwrap().unwrap();
    assert!(String::from_utf8(bob_current).unwrap().contains("bob_token_b"));
}
