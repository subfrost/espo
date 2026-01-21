use crate::config::{get_config, get_electrum_like, get_network};
use crate::modules::essentials::storage::{EssentialsProvider, GetCreationRecordsByIdParams};
use crate::modules::essentials::utils::balances::get_outpoint_balances_with_spent_batch;
use crate::modules::oylapi::ordinals::{OrdOutput, fetch_ord_outputs};
use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use bitcoin::{Address, Txid, hashes::Hash as _};
use bitcoin::hashes::sha256;
use bitcoin::script::ScriptBuf;
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::{HashMap, HashSet};
use std::str::FromStr;

#[derive(Debug, Deserialize)]
struct EsploraUtxoStatus {
    confirmed: bool,
    block_height: Option<u64>,
}

#[derive(Debug, Deserialize)]
struct EsploraUtxo {
    txid: String,
    vout: u32,
    value: u64,
    status: EsploraUtxoStatus,
}

#[derive(Debug, Serialize)]
pub struct AlkanesUtxoEntry {
    pub value: String,
    pub name: String,
    pub symbol: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct FormattedUtxo {
    pub tx_id: String,
    pub output_index: u32,
    pub satoshis: u64,
    pub script_pk: String,
    pub address: String,
    pub inscriptions: Vec<String>,
    pub runes: Value,
    pub alkanes: HashMap<String, AlkanesUtxoEntry>,
    pub confirmations: u64,
    pub indexed: bool,
}

pub async fn get_address_utxos(
    essentials: &EssentialsProvider,
    client: &Client,
    address: &str,
    ord_endpoint: Option<&str>,
) -> Result<Vec<FormattedUtxo>> {
    let network = get_network();
    let addr = Address::from_str(address)
        .ok()
        .and_then(|a| a.require_network(network).ok())
        .ok_or_else(|| anyhow!("invalid address"))?;
    let address_str = addr.to_string();
    let script_pubkey = addr.script_pubkey();
    let script_pk_hex = hex::encode(script_pubkey.as_bytes());

    let script_hash = script_hash_hex(&script_pubkey);
    let electrs_url = get_config()
        .electrs_esplora_url
        .clone()
        .ok_or_else(|| anyhow!("electrs_esplora_url missing"))?;

    let utxos = fetch_scripthash_utxos(client, &electrs_url, &script_hash).await?;
    if utxos.is_empty() {
        return Ok(Vec::new());
    }

    let tip_height = get_electrum_like().tip_height().unwrap_or(0) as u64;

    let mut outpoints: Vec<(Txid, u32)> = Vec::with_capacity(utxos.len());
    let mut outpoint_strs: Vec<String> = Vec::with_capacity(utxos.len());
    for utxo in &utxos {
        let txid = Txid::from_str(&utxo.txid)?;
        outpoints.push((txid, utxo.vout));
        outpoint_strs.push(format!("{}:{}", utxo.txid, utxo.vout));
    }

    let balances_by_outpoint = match get_outpoint_balances_with_spent_batch(essentials, &outpoints) {
        Ok(map) => map,
        Err(_) => HashMap::new(),
    };

    let mut alkane_ids: HashSet<SchemaAlkaneId> = HashSet::new();
    for lookup in balances_by_outpoint.values() {
        for be in &lookup.balances {
            alkane_ids.insert(be.alkane);
        }
    }

    let mut names: HashMap<SchemaAlkaneId, (String, String)> = HashMap::new();
    if !alkane_ids.is_empty() {
        let alkanes: Vec<SchemaAlkaneId> = alkane_ids.iter().copied().collect();
        let records = essentials
            .get_creation_records_by_id(GetCreationRecordsByIdParams { alkanes })?
            .records;
        for rec in records.into_iter().flatten() {
            let name = rec.names.first().cloned().unwrap_or_default();
            let symbol = rec.symbols.first().cloned().unwrap_or_default();
            names.insert(rec.alkane, (name, symbol));
        }
    }

    let ord_outputs = if let Some(endpoint) = ord_endpoint {
        fetch_ord_outputs(client, endpoint, &outpoint_strs).await.unwrap_or_default()
    } else {
        HashMap::new()
    };

    let mut formatted: Vec<FormattedUtxo> = Vec::with_capacity(utxos.len());
    for (idx, utxo) in utxos.iter().enumerate() {
        let txid = Txid::from_str(&utxo.txid)?;
        let out_key = (txid, utxo.vout);
        let lookup = balances_by_outpoint.get(&out_key);
        let balances = lookup.map(|l| l.balances.clone()).unwrap_or_default();

        let mut alkanes_map: HashMap<String, AlkanesUtxoEntry> = HashMap::new();
        for be in balances {
            let id_str = format!("{}:{}", be.alkane.block, be.alkane.tx);
            let (name, symbol) = names.get(&be.alkane).cloned().unwrap_or_default();
            alkanes_map.insert(
                id_str,
                AlkanesUtxoEntry { value: be.amount.to_string(), name, symbol },
            );
        }

        let ord = ord_outputs
            .get(&outpoint_strs[idx])
            .cloned()
            .unwrap_or_else(OrdOutput::default);

        let confirmations = if utxo.status.confirmed {
            utxo.status
                .block_height
                .and_then(|h| tip_height.checked_sub(h).map(|d| d + 1))
                .unwrap_or(0)
        } else {
            0
        };

        formatted.push(FormattedUtxo {
            tx_id: utxo.txid.clone(),
            output_index: utxo.vout,
            satoshis: utxo.value,
            script_pk: script_pk_hex.clone(),
            address: address_str.clone(),
            inscriptions: ord.inscriptions,
            runes: ord.runes,
            alkanes: alkanes_map,
            confirmations,
            indexed: true,
        });
    }

    Ok(formatted)
}

fn script_hash_hex(script_pubkey: &ScriptBuf) -> String {
    let hash = sha256::Hash::hash(script_pubkey.as_bytes());
    let mut bytes = hash.to_byte_array();
    bytes.reverse();
    hex::encode(bytes)
}

async fn fetch_scripthash_utxos(
    client: &Client,
    base_url: &str,
    script_hash: &str,
) -> Result<Vec<EsploraUtxo>> {
    let base = base_url.trim_end_matches('/');
    let url = format!("{base}/scripthash/{script_hash}/utxo");
    let resp = client
        .get(&url)
        .send()
        .await
        .map_err(|e| anyhow!("esplora scripthash utxo request failed: {e}"))?;
    let resp = resp
        .error_for_status()
        .map_err(|e| anyhow!("esplora scripthash utxo status error: {e}"))?;
    let body = resp
        .json::<Vec<EsploraUtxo>>()
        .await
        .map_err(|e| anyhow!("esplora scripthash utxo decode failed: {e}"))?;
    Ok(body)
}
