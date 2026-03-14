use crate::schemas::SchemaAlkaneId;
use anyhow::{Result, anyhow};
use serde_json::Value;
use std::sync::OnceLock;

#[derive(Clone, Debug)]
pub struct FujinConfig {
    pub factory_id: SchemaAlkaneId,
    pub vault_id: SchemaAlkaneId,
    pub zap_id: SchemaAlkaneId,
}

static FUJIN_CONFIG: OnceLock<FujinConfig> = OnceLock::new();

pub fn get_fujin_config() -> &'static FujinConfig {
    FUJIN_CONFIG.get().expect("fujin config not initialized")
}

impl FujinConfig {
    pub fn spec() -> &'static str {
        r#"{ "factory_id": "4:900008", "vault_id": "4:900010", "zap_id": "4:900009" }"#
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("fujin config must be an object; expected: {}", Self::spec()))?;

        let factory_id = parse_alkane_id(obj, "factory_id")?;
        let vault_id = parse_alkane_id(obj, "vault_id")?;
        let zap_id = parse_alkane_id(obj, "zap_id")?;

        let cfg = Self { factory_id, vault_id, zap_id };
        let _ = FUJIN_CONFIG.set(cfg.clone());
        Ok(cfg)
    }
}

fn parse_alkane_id(
    obj: &serde_json::Map<String, Value>,
    key: &str,
) -> Result<SchemaAlkaneId> {
    let s = obj
        .get(key)
        .ok_or_else(|| anyhow!("fujin.{key} missing"))?
        .as_str()
        .ok_or_else(|| anyhow!("fujin.{key} must be a string like \"4:900008\""))?;
    let parts: Vec<&str> = s.split(':').collect();
    if parts.len() != 2 {
        return Err(anyhow!("fujin.{key} must be \"block:tx\", got \"{s}\""));
    }
    let block: u32 = parts[0]
        .parse()
        .map_err(|_| anyhow!("fujin.{key} block not a number: {}", parts[0]))?;
    let tx: u64 = parts[1]
        .parse()
        .map_err(|_| anyhow!("fujin.{key} tx not a number: {}", parts[1]))?;
    Ok(SchemaAlkaneId { block, tx })
}
