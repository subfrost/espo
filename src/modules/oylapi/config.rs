use anyhow::{Result, anyhow};
use serde_json::Value;

#[derive(Clone, Debug)]
pub struct BtcUsdPriceSourceConfig {
    pub endpoint: String,
    pub field: String,
    pub refresh_interval_secs: u64,
}

#[derive(Clone, Debug)]
pub struct OylApiConfig {
    pub host: String,
    pub port: u16,
    pub alkane_icon_cdn: String,
    pub btc_usd_price_source: BtcUsdPriceSourceConfig,
    pub ord_endpoint: Option<String>,
}

impl OylApiConfig {
    pub fn spec() -> &'static str {
        "{ \"host\": \"<host>\", \"port\": <port>, \"alkane_icon_cdn\": \"<url>\", \"btc_usd_price_source\": { \"endpoint\": \"<url>\", \"field\": \"<field>\", \"refresh_interval\": <seconds> }, \"ord_endpoint\": \"<optional url>\" }"
    }

    pub fn from_value(value: &Value) -> Result<Self> {
        let obj = value
            .as_object()
            .ok_or_else(|| anyhow!("oylapi config must be an object; expected: {}", Self::spec()))?;

        let host = obj
            .get("host")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("oylapi.host missing; expected: {}", Self::spec()))?;

        let port = obj
            .get("port")
            .and_then(|v| v.as_u64())
            .and_then(|v| u16::try_from(v).ok())
            .ok_or_else(|| anyhow!("oylapi.port missing/invalid; expected: {}", Self::spec()))?;

        let alkane_icon_cdn = obj
            .get("alkane_icon_cdn")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("oylapi.alkane_icon_cdn missing; expected: {}", Self::spec()))?;

        let btc_usd_price_source = obj
            .get("btc_usd_price_source")
            .ok_or_else(|| anyhow!("oylapi.btc_usd_price_source missing; expected: {}", Self::spec()))?;
        let btc_obj = btc_usd_price_source.as_object().ok_or_else(|| {
            anyhow!("oylapi.btc_usd_price_source must be an object; expected: {}", Self::spec())
        })?;
        let endpoint = btc_obj
            .get("endpoint")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("oylapi.btc_usd_price_source.endpoint missing; expected: {}", Self::spec()))?;
        let field = btc_obj
            .get("field")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .ok_or_else(|| anyhow!("oylapi.btc_usd_price_source.field missing; expected: {}", Self::spec()))?;
        let refresh_interval_secs = btc_obj
            .get("refresh_interval")
            .and_then(|v| v.as_u64())
            .ok_or_else(|| {
                anyhow!(
                    "oylapi.btc_usd_price_source.refresh_interval missing/invalid; expected: {}",
                    Self::spec()
                )
            })?;

        let ord_endpoint = obj
            .get("ord_endpoint")
            .and_then(|v| v.as_str())
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty());

        Ok(Self {
            host,
            port,
            alkane_icon_cdn,
            btc_usd_price_source: BtcUsdPriceSourceConfig {
                endpoint,
                field,
                refresh_interval_secs,
            },
            ord_endpoint,
        })
    }
}
