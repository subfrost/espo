use crate::config::get_explorer_base_path;

pub fn explorer_base_path() -> &'static str {
    get_explorer_base_path()
}

pub fn explorer_path(path: &str) -> String {
    let base = explorer_base_path();
    let resolved = if path.is_empty() {
        "/".to_string()
    } else if path.starts_with('/') {
        path.to_string()
    } else {
        format!("/{path}")
    };
    if base == "/" {
        return resolved;
    }
    if resolved == "/" {
        return base.to_string();
    }
    format!("{base}{resolved}")
}
