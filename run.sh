cargo run --release -- \
  --readonly-metashrew-db-dir /data/.metashrew/v9-chk/.metashrew-v9 \
  --port 5778 \
  --electrs-esplora-url http://127.0.0.1:4332 \
  --bitcoind-rpc-url http://127.0.0.1:8332 \
  --bitcoind-rpc-user admin \
  --bitcoind-rpc-pass admin \
  --network mainnet \
  --block-source-mode rpc \
  --metashrew-rpc-url http://127.0.0.1:7046 \
  --explorer-host 0.0.0.0:5779 \
  --enable-aof \
  --debug
# Add --view-only to disable indexing/mempool and serve existing data only.
