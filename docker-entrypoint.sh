#!/bin/bash
set -e

# Default values
PORT="${PORT:-5778}"
BLOCK_SOURCE_MODE="${BLOCK_SOURCE_MODE:-rpc}"

# Build command arguments
ARGS=""

# Required arguments
if [ -n "$READONLY_METASHREW_DB_DIR" ]; then
    ARGS="$ARGS --readonly-metashrew-db-dir $READONLY_METASHREW_DB_DIR"
fi

if [ -n "$METASHREW_RPC_URL" ]; then
    ARGS="$ARGS --metashrew-rpc-url $METASHREW_RPC_URL"
fi

ARGS="$ARGS --port $PORT"

if [ -n "$ELECTRS_ESPLORA_URL" ]; then
    ARGS="$ARGS --electrs-esplora-url $ELECTRS_ESPLORA_URL"
fi

if [ -n "$BITCOIND_RPC_URL" ]; then
    ARGS="$ARGS --bitcoind-rpc-url $BITCOIND_RPC_URL"
fi

if [ -n "$BITCOIND_RPC_USER" ]; then
    ARGS="$ARGS --bitcoind-rpc-user $BITCOIND_RPC_USER"
fi

if [ -n "$BITCOIND_RPC_PASS" ]; then
    ARGS="$ARGS --bitcoind-rpc-pass $BITCOIND_RPC_PASS"
fi

if [ -n "$NETWORK" ]; then
    ARGS="$ARGS --network $NETWORK"
fi

ARGS="$ARGS --block-source-mode $BLOCK_SOURCE_MODE"

if [ -n "$EXPLORER_HOST" ]; then
    ARGS="$ARGS --explorer-host $EXPLORER_HOST"
fi

if [ -n "$BASE_PATH" ]; then
    ARGS="$ARGS --base-path $BASE_PATH"
fi

if [ -n "$DB_PATH" ]; then
    ARGS="$ARGS --db-path $DB_PATH"
fi

echo "Starting espo with args: $ARGS"
exec /usr/local/bin/espo $ARGS
