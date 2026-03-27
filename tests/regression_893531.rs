#![cfg(not(target_arch = "wasm32"))]

//! Regression test for production crash at block 893531.
//!
//! The crash: "negative holder balance detected (alkane=2:16,
//! holder=Address("bc1pcdmxr..."), cur=0, sub=1000000000000)"
//!
//! This test loads the actual mainnet block 893531 (3922 txs) and
//! examines the transaction that triggers the underflow.

#[cfg(feature = "test-utils")]
mod tests {
    use anyhow::Result;
    use bitcoin::consensus::deserialize;

    /// Load block 893531 from hex file
    fn load_block_893531() -> Result<bitcoin::Block> {
        let hex_str = std::fs::read_to_string("test_data/block_893531.hex")
            .map_err(|e| anyhow::anyhow!("Missing test_data/block_893531.hex: {e}"))?;
        let bytes = hex::decode(hex_str.trim())?;
        let block: bitcoin::Block = deserialize(&bytes)?;
        Ok(block)
    }

    #[test]
    fn test_load_and_inspect_block_893531() -> Result<()> {
        let block = load_block_893531()?;
        println!("[893531] Block loaded: {} txs", block.txdata.len());

        // Find transactions involving the crash address
        let crash_addr = "bc1pcdmxr982z6jxnwk9uevj6q5s85dn495myhn2kamxgffpy4l2ygnqfkqs8p";
        let mut relevant_txs = Vec::new();

        for (tx_idx, tx) in block.txdata.iter().enumerate() {
            // Check outputs for the crash address
            for (vout, output) in tx.output.iter().enumerate() {
                if let Some(addr) = espo::modules::essentials::storage::spk_to_address_str(
                    &output.script_pubkey,
                    bitcoin::Network::Bitcoin,
                ) {
                    if addr == crash_addr {
                        relevant_txs.push((tx_idx, tx.compute_txid(), "output", vout));
                    }
                }
            }

            // Check if any input spends from known outpoints
            // (We'd need the UTXO set to know which inputs relate to the crash address)
        }

        println!("[893531] Transactions with crash address in outputs:");
        for (idx, txid, direction, vout) in &relevant_txs {
            println!("  tx#{idx} {txid} {direction} vout={vout}");
        }

        // Count protorune transactions (have OP_RETURN with runestone)
        let mut protorune_count = 0;
        for tx in &block.txdata {
            for output in &tx.output {
                if output.script_pubkey.is_op_return() {
                    protorune_count += 1;
                    break;
                }
            }
        }
        println!("[893531] {} txs with OP_RETURN", protorune_count);

        // Find the specific tx that has alkane 2:16 activity
        // In protorune, the alkane ID is encoded in the runestone protocol field
        // For now, just log stats
        // Detailed inspection of tx#764
        if relevant_txs.len() > 0 {
            let tx = &block.txdata[relevant_txs[0].0];
            let txid = tx.compute_txid();
            println!("\n[893531] === TX #{} ({txid}) ===", relevant_txs[0].0);
            println!("  Inputs: {}", tx.input.len());
            for (i, input) in tx.input.iter().enumerate() {
                println!("    vin[{i}]: {}:{}", input.previous_output.txid, input.previous_output.vout);
            }
            println!("  Outputs: {}", tx.output.len());
            for (i, output) in tx.output.iter().enumerate() {
                let addr = espo::modules::essentials::storage::spk_to_address_str(
                    &output.script_pubkey, bitcoin::Network::Bitcoin,
                ).unwrap_or_else(|| "<unresolvable>".to_string());
                let is_opr = output.script_pubkey.is_op_return();
                println!("    vout[{i}]: {} sats, addr={}{}", output.value.to_sat(), addr, if is_opr { " [OP_RETURN]" } else { "" });
            }
        }

        // Also find ALL txs that involve the crash address in inputs (spending FROM it)
        // This requires checking if any input's previous_output points to a tx in THIS block
        // that sends to the crash address
        println!("\n[893531] Checking for same-block spend chains involving crash address...");
        let crash_txid = relevant_txs.get(0).map(|r| r.1);
        if let Some(ctxid) = crash_txid {
            for (tx_idx, tx) in block.txdata.iter().enumerate() {
                for input in &tx.input {
                    if input.previous_output.txid == ctxid {
                        println!("  tx#{tx_idx} ({}) spends {}:{}",
                            tx.compute_txid(), ctxid, input.previous_output.vout);
                    }
                }
            }
        }

        println!("[893531] Block inspection complete");
        Ok(())
    }

    /// Scan the block for transactions where outputs have unresolvable scripts
    /// AND inputs that might have alkane balances (protorune TXs)
    #[test]
    fn test_find_unresolvable_output_txs() -> Result<()> {
        let block = load_block_893531()?;

        let mut suspicious = Vec::new();

        for (tx_idx, tx) in block.txdata.iter().enumerate() {
            // Skip coinbase
            if tx.is_coinbase() {
                continue;
            }

            let has_op_return = tx.output.iter().any(|o| o.script_pubkey.is_op_return());
            if !has_op_return {
                continue;
            }

            // Check if any non-OP_RETURN output has an unresolvable address
            let mut has_unresolvable = false;
            let mut has_resolvable = false;
            for output in &tx.output {
                if output.script_pubkey.is_op_return() {
                    continue;
                }
                match espo::modules::essentials::storage::spk_to_address_str(
                    &output.script_pubkey,
                    bitcoin::Network::Bitcoin,
                ) {
                    Some(_) => has_resolvable = true,
                    None => has_unresolvable = true,
                }
            }

            if has_unresolvable && has_resolvable {
                // This TX has BOTH resolvable and unresolvable outputs
                // If tokens route from a resolvable VIN to an unresolvable VOUT,
                // the holder delta goes negative
                suspicious.push((tx_idx, tx.compute_txid(), tx.output.len()));
            }
        }

        println!(
            "[893531] {} protorune txs with mixed resolvable/unresolvable outputs",
            suspicious.len()
        );
        for (idx, txid, n_outputs) in &suspicious {
            println!("  tx#{idx} {txid} ({n_outputs} outputs)");
        }

        Ok(())
    }
}
