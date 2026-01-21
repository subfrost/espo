use anyhow::{Result, anyhow};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct AmmDataConfig {
    pub eth_rpc: String,
    pub eth_call_throttle_ms: u64,
}

impl AmmDataConfig {
    pub fn spec() -> &'static str {
        "{ \"eth_rpc\": \"<url>\", \"eth_call_throttle\": <ms> }"
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("ammdata config must be an object; expected: {}", Self::spec()))?;
        let eth_rpc_val = obj
            .get("eth_rpc")
            .ok_or_else(|| anyhow!("ammdata.eth_rpc missing; expected: {}", Self::spec()))?;
        let eth_call_throttle_val = obj
            .get("eth_call_throttle")
            .ok_or_else(|| anyhow!("ammdata.eth_call_throttle missing; expected: {}", Self::spec()))?;
        let eth_rpc = eth_rpc_val
            .as_str()
            .ok_or_else(|| anyhow!("ammdata.eth_rpc must be a string; expected: {}", Self::spec()))?
            .trim()
            .to_string();
        if eth_rpc.is_empty() {
            anyhow::bail!("ammdata.eth_rpc must be set; expected: {}", Self::spec());
        }
        let eth_call_throttle_ms = eth_call_throttle_val
            .as_u64()
            .ok_or_else(|| {
                anyhow!("ammdata.eth_call_throttle must be a non-negative integer; expected: {}", Self::spec())
            })?;

        let parsed = reqwest::Url::parse(&eth_rpc)
            .map_err(|e| anyhow!("ammdata.eth_rpc must be an absolute URL (http/https): {e}"))?;
        if parsed.scheme() != "http" && parsed.scheme() != "https" {
            anyhow::bail!(
                "ammdata.eth_rpc must be an http/https URL; got scheme '{}'",
                parsed.scheme()
            );
        }

        Ok(Self { eth_rpc, eth_call_throttle_ms })
    }

    pub fn load_from_global_config() -> Result<Self> {
        let value = crate::config::get_module_config("ammdata")
            .ok_or_else(|| {
                anyhow!(
                    "No config defined for ammdata module, but ammdata module was loaded and defines a config. Expected: {}",
                    Self::spec()
                )
            })?;
        Self::from_value(value)
    }
}
