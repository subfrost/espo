use crate::config::{get_base_path, get_network};
use crate::runtime::mdb::Mdb;
use bitcoin::Network;

#[derive(Clone)]
pub struct ExplorerState {
    pub essentials_mdb: Mdb,
    pub network: Network,
    pub base_path: String,
}

impl ExplorerState {
    pub fn new() -> Self {
        let essentials_mdb = Mdb::from_db(crate::config::get_espo_db(), b"essentials:");
        let network = get_network();
        let base_path = get_base_path().to_string();
        Self { essentials_mdb, network, base_path }
    }

    /// Generate a URL with the base path prefix
    pub fn url(&self, path: &str) -> String {
        if self.base_path.is_empty() {
            path.to_string()
        } else if path.starts_with('/') {
            format!("{}{}", self.base_path, path)
        } else {
            format!("{}/{}", self.base_path, path)
        }
    }
}
