use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use maud::{DOCTYPE, Markup, PreEscaped, html};

use crate::config::{get_explorer_networks, get_network};
use crate::explorer::components::dropdown::{DropdownItem, DropdownProps, dropdown};
use crate::explorer::components::footer::footer;
use crate::explorer::components::svg_assets::{
    dots, icon_btc, icon_search, icon_signet, icon_testnet, logo_espo,
};
use crate::explorer::paths::{explorer_base_path, explorer_path};

const STYLE_CSS: &str = include_str!("../assets/style.css");
const SEARCH_DEBOUNCE_MS: u64 = 300;
const NAV_SCRIPT: &str = r#"
<script>
(() => {
  const nav = document.querySelector('[data-nav-menu]');
  if (!nav) return;
  const toggles = nav.querySelectorAll('[data-menu-toggle]');
  const menu = nav.querySelector('[data-menu]');
  const topbar = document.querySelector('[data-topbar]');
  const mobileSearch = document.querySelector('[data-search-mobile]');
  const closeSearch = () => {
    if (topbar) {
      topbar.dataset.searchOpen = '';
    }
    if (mobileSearch) {
      mobileSearch.setAttribute('aria-hidden', 'true');
    }
    document.querySelectorAll('[data-search-results]').forEach((node) => {
      node.dataset.open = '';
      node.setAttribute('aria-hidden', 'true');
    });
  };
  const closeDropdowns = () => {
    document.querySelectorAll('[data-dropdown]').forEach((node) => {
      node.dataset.open = '';
      const toggle = node.querySelector('[data-dropdown-toggle]');
      const panel = node.querySelector('.dropdown-panel');
      if (toggle) {
        toggle.setAttribute('aria-expanded', 'false');
      }
      if (panel) {
        panel.setAttribute('aria-hidden', 'true');
      }
    });
  };
  if (!menu || toggles.length === 0) return;
  const closeMenu = () => {
    nav.dataset.menuOpen = '';
    menu.setAttribute('aria-hidden', 'true');
    toggles.forEach((btn) => btn.setAttribute('aria-expanded', 'false'));
  };
  const openMenu = () => {
    closeSearch();
    nav.dataset.menuOpen = '1';
    menu.setAttribute('aria-hidden', 'false');
    toggles.forEach((btn) => btn.setAttribute('aria-expanded', 'true'));
  };
  const toggleMenu = () => {
    closeDropdowns();
    if (nav.dataset.menuOpen === '1') {
      closeMenu();
    } else {
      openMenu();
    }
  };
  toggles.forEach((btn) => {
    btn.addEventListener('click', (event) => {
      event.stopPropagation();
      toggleMenu();
    });
  });
  document.addEventListener('click', (event) => {
    if (!nav.contains(event.target)) {
      closeMenu();
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeMenu();
    }
  });
})();
</script>
"#;

pub async fn style() -> impl IntoResponse {
    (StatusCode::OK, [(CONTENT_TYPE, "text/css; charset=utf-8")], STYLE_CSS)
}

pub fn layout(title: &str, content: Markup) -> Html<String> {
    let base_path_js = format!("{:?}", explorer_base_path());
    let network_dropdown = network_dropdown();
    let markup = html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                link rel="stylesheet" href=(explorer_path("/static/style.css"));
            }
            body {
                header class="topbar" data-topbar="" {
                    div class="app" {
                        nav class="nav" data-nav-menu="" {
                            div class="brand-group" {
                                a class="brand" href=(explorer_path("/")) {
                                    (logo_espo())
                                    span class="brand-text" { "Espo" }
                                }
                                @if let Some(dropdown) = network_dropdown {
                                    (dropdown)
                                }
                            }
                            div class="nav-search hero-search" data-search="" {
                                form class="hero-search-form" method="get" action=(explorer_path("/search")) autocomplete="off" data-search-form="" {
                                    div class="hero-search-input" {
                                        span class="hero-search-icon" aria-hidden="true" { (icon_search()) }
                                        input class="hero-search-field" type="text" name="q" placeholder="Search blocks, alkanes, transactions, addresses" data-search-input="" aria-label="Search blocks, alkanes, transactions, addresses" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false";
                                        button class="hero-search-submit" type="submit" aria-label="Search" { (icon_search()) }
                                    }
                                }
                                div class="search-results" data-search-results="" aria-hidden="true" {
                                    div class="search-results-body" data-search-results-body="" {}
                                }
                            }
                            div class="navlinks-container" {
                                a class="navlink" href=(explorer_path("/")) { "Blocks" }
                                a class="navlink" href=(explorer_path("/alkanes")) { "Alkanes" }
                            }
                            div class="nav-actions" {
                                button class="nav-icon-btn nav-search-toggle" type="button" aria-label="Search" data-search-toggle="" {
                                    (icon_search())
                                }
                                button class="nav-menu-toggle nav-menu-toggle-dots" type="button" aria-label="Open menu" data-menu-toggle="" aria-expanded="false" {
                                    (dots())
                                }
                            }
                            div class="nav-menu" data-menu="" aria-hidden="true" {
                                a class="nav-menu-link" href=(explorer_path("/")) { "Blocks" }
                                a class="nav-menu-link" href=(explorer_path("/alkanes")) { "Alkanes" }
                            }
                        }
                    }
                    div class="nav-search-mobile" data-search-mobile="" aria-hidden="true" {
                        div class="app" {
                            div class="nav-search hero-search" data-search="" {
                                form class="hero-search-form" method="get" action=(explorer_path("/search")) autocomplete="off" data-search-form="" {
                                    div class="hero-search-input" {
                                        span class="hero-search-icon" aria-hidden="true" { (icon_search()) }
                                        input class="hero-search-field" type="text" name="q" placeholder="Search blocks, alkanes, transactions, addresses" data-search-input="" aria-label="Search blocks, alkanes, transactions, addresses" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false";
                                        button class="hero-search-submit" type="submit" aria-label="Search" { (icon_search()) }
                                    }
                                }
                                div class="search-results" data-search-results="" aria-hidden="true" {
                                    div class="search-results-body" data-search-results-body="" {}
                                }
                            }
                        }
                    }
                }
                main class="app" {
                    (content)
                }
                (footer())
                (search_scripts(&base_path_js))
                (dropdown_scripts())
                (PreEscaped(NAV_SCRIPT))
            }
        }
    };
    Html(markup.into_string())
}

fn network_dropdown() -> Option<Markup> {
    let networks = get_explorer_networks()?;
    if networks.is_empty() {
        return None;
    }

    let mut entries: Vec<(&'static str, &'static str, String)> = Vec::new();
    if let Some(url) = networks.mainnet.as_ref() {
        entries.push(("mainnet", "Mainnet", url.clone()));
    }
    if let Some(url) = networks.signet.as_ref() {
        entries.push(("signet", "Signet", url.clone()));
    }
    if let Some(url) = networks.testnet3.as_ref() {
        entries.push(("testnet3", "Testnet3", url.clone()));
    }
    if let Some(url) = networks.testnet4.as_ref() {
        entries.push(("testnet4", "Testnet4", url.clone()));
    }
    if let Some(url) = networks.regtest.as_ref() {
        entries.push(("regtest", "Regtest", url.clone()));
    }
    if entries.is_empty() {
        return None;
    }

    let current_key = network_key(get_network());
    let selected_key = entries
        .iter()
        .find(|(key, _, _)| *key == current_key)
        .map(|(key, _, _)| *key)
        .unwrap_or(entries[0].0);

    let items = entries
        .iter()
        .map(|(key, label, url)| DropdownItem {
            label: (*label).to_string(),
            href: url.clone(),
            icon: Some(network_icon(key)),
            selected: *key == selected_key,
        })
        .collect();

    Some(dropdown(DropdownProps {
        label: None,
        selected_icon: Some(network_icon(selected_key)),
        items,
        aria_label: Some("Network".to_string()),
    }))
}

fn network_key(network: bitcoin::Network) -> &'static str {
    match network {
        bitcoin::Network::Bitcoin => "mainnet",
        bitcoin::Network::Regtest => "regtest",
        bitcoin::Network::Signet => "signet",
        _ => {
            let tag = network.to_string();
            if tag == "testnet4" { "testnet4" } else { "testnet3" }
        }
    }
}

fn network_icon(key: &str) -> Markup {
    match key {
        "mainnet" => icon_btc(),
        "signet" => icon_signet(),
        _ => icon_testnet(),
    }
}

fn search_scripts(base_path_js: &str) -> Markup {
    let script = format!(
        r#"
<script>
(() => {{
  const SEARCH_DEBOUNCE_MS = {SEARCH_DEBOUNCE_MS};
  const basePath = {base_path_js};
  const basePrefix = basePath === '/' ? '' : basePath;

  const initSearch = (root) => {{
    const form = root.querySelector('[data-search-form]');
    const input = root.querySelector('[data-search-input]');
    const results = root.querySelector('[data-search-results]');
    const resultsBody = root.querySelector('[data-search-results-body]');
    if (!form || !input || !results || !resultsBody) return;

    let debounceId = null;
    let abortController = null;
    let lastQuery = '';

    const closeResults = () => {{
      results.dataset.open = '';
      results.setAttribute('aria-hidden', 'true');
    }};

    const openResults = () => {{
      results.dataset.open = '1';
      results.setAttribute('aria-hidden', 'false');
    }};

    const clearResults = () => {{
      resultsBody.innerHTML = '';
      closeResults();
    }};

  const iconTextFor = (kind) => {{
    if (kind === 'blocks') return '#';
    if (kind === 'addresses') return '@';
    return 'Tx';
  }};

  const buildIcon = (group, item) => {{
    if (group.kind === 'alkanes') {{
      const wrap = document.createElement('span');
      wrap.className = 'alk-icon-wrap search-alk-icon';
      const img = document.createElement('span');
      img.className = 'alk-icon-img';
      if (item.icon_url) {{
        img.style.backgroundImage = `url("${{item.icon_url}}")`;
      }}
      const letter = document.createElement('span');
      letter.className = 'alk-icon-letter';
      letter.textContent = item.fallback_letter || '?';
      wrap.appendChild(img);
      wrap.appendChild(letter);
      return wrap;
    }}
    const icon = document.createElement('span');
    icon.className = 'search-result-icon';
    icon.textContent = iconTextFor(group.kind);
    return icon;
  }};

    const renderResults = (groups) => {{
      resultsBody.innerHTML = '';
      if (!Array.isArray(groups) || groups.length === 0) {{
        closeResults();
        return;
      }}
      groups.forEach((group) => {{
        const section = document.createElement('div');
        section.className = 'search-results-section';
        section.dataset.kind = group.kind || '';

        const title = document.createElement('div');
        title.className = 'search-results-title';
        title.textContent = group.title || '';

        const items = document.createElement('div');
        items.className = 'search-results-items';

        (group.items || []).forEach((item) => {{
          const hasHref = Boolean(item.href);
          const entry = document.createElement(hasHref ? 'a' : 'div');
          entry.className = 'search-result';
          if (hasHref) {{
            entry.setAttribute('href', item.href);
          }} else {{
            entry.dataset.disabled = '1';
          }}
        const icon = buildIcon(group, item);
          const label = document.createElement('span');
          label.className = 'search-result-label';
          label.textContent = item.label || '';
          entry.appendChild(icon);
          entry.appendChild(label);
          items.appendChild(entry);
        }});

        section.appendChild(title);
        section.appendChild(items);
        resultsBody.appendChild(section);
      }});
      openResults();
    }};

    const fetchResults = (value) => {{
      if (abortController) abortController.abort();
      abortController = new AbortController();
      fetch(`${{basePrefix}}/api/search/guess?q=${{encodeURIComponent(value)}}`, {{ signal: abortController.signal }})
        .then((res) => (res.ok ? res.json() : null))
        .then((data) => {{
          if (!data) {{
            clearResults();
            return;
          }}
          renderResults(data.groups || []);
        }})
        .catch((err) => {{
          if (err && err.name === 'AbortError') return;
          clearResults();
        }});
    }};

    const queueFetch = () => {{
      const value = input.value.trim();
      if (!value) {{
        clearResults();
        lastQuery = '';
        return;
      }}
      if (value === lastQuery) return;
      lastQuery = value;
      if (debounceId) window.clearTimeout(debounceId);
      debounceId = window.setTimeout(() => fetchResults(value), SEARCH_DEBOUNCE_MS);
    }};

    input.addEventListener('input', queueFetch);
    input.addEventListener('focus', () => {{
      if (resultsBody.children.length > 0) {{
        openResults();
      }}
    }});
    form.addEventListener('submit', (event) => {{
      const value = input.value.trim();
      if (!value) {{
        event.preventDefault();
        clearResults();
        return;
      }}
      const firstAlkane = resultsBody.querySelector(
        '.search-results-section[data-kind="alkanes"] .search-result[href]'
      );
      if (firstAlkane) {{
        const href = firstAlkane.getAttribute('href');
        if (href) {{
          event.preventDefault();
          window.location.assign(href);
          return;
        }}
      }}
    }});
    document.addEventListener('click', (event) => {{
      if (!root.contains(event.target)) {{
        closeResults();
      }}
    }});
  }};

  document.querySelectorAll('[data-search]').forEach(initSearch);

  const topbar = document.querySelector('[data-topbar]');
  const mobileWrap = document.querySelector('[data-search-mobile]');
  const toggle = document.querySelector('[data-search-toggle]');
  const nav = document.querySelector('[data-nav-menu]');
  const menu = nav ? nav.querySelector('[data-menu]') : null;
  const menuToggles = nav ? nav.querySelectorAll('[data-menu-toggle]') : [];

  const closeMenu = () => {{
    if (!nav || !menu) return;
    nav.dataset.menuOpen = '';
    menu.setAttribute('aria-hidden', 'true');
    menuToggles.forEach((btn) => btn.setAttribute('aria-expanded', 'false'));
  }};

  if (topbar && mobileWrap && toggle) {{
    const mobileInput = mobileWrap.querySelector('[data-search-input]');
    const closeMobile = () => {{
      topbar.dataset.searchOpen = '';
      mobileWrap.setAttribute('aria-hidden', 'true');
    }};
    const openMobile = () => {{
      closeMenu();
      topbar.dataset.searchOpen = '1';
      mobileWrap.setAttribute('aria-hidden', 'false');
      if (mobileInput) mobileInput.focus();
    }};
    const toggleMobile = () => {{
      if (topbar.dataset.searchOpen === '1') {{
        closeMobile();
      }} else {{
        openMobile();
      }}
    }};
    toggle.addEventListener('click', (event) => {{
      event.stopPropagation();
      toggleMobile();
    }});
    document.addEventListener('click', (event) => {{
      if (!topbar.contains(event.target)) {{
        closeMobile();
      }}
    }});
    document.addEventListener('keydown', (event) => {{
      if (event.key === 'Escape') {{
        closeMobile();
      }}
    }});
  }}
}})();
</script>
"#
    );
    PreEscaped(script)
}

fn dropdown_scripts() -> Markup {
    let script = r#"
<script>
(() => {
  const nav = document.querySelector('[data-nav-menu]');
  const menu = nav ? nav.querySelector('[data-menu]') : null;
  const menuToggles = nav ? nav.querySelectorAll('[data-menu-toggle]') : [];
  const closeMenu = () => {
    if (!nav || !menu) return;
    nav.dataset.menuOpen = '';
    menu.setAttribute('aria-hidden', 'true');
    menuToggles.forEach((btn) => btn.setAttribute('aria-expanded', 'false'));
  };
  const dropdowns = Array.from(document.querySelectorAll('[data-dropdown]'));
  const setOpen = (node, open) => {
    node.dataset.open = open ? '1' : '';
    const toggle = node.querySelector('[data-dropdown-toggle]');
    const panel = node.querySelector('.dropdown-panel');
    if (toggle) {
      toggle.setAttribute('aria-expanded', open ? 'true' : 'false');
    }
    if (panel) {
      panel.setAttribute('aria-hidden', open ? 'false' : 'true');
    }
  };
  const closeAll = () => {
    dropdowns.forEach((node) => setOpen(node, false));
  };
  dropdowns.forEach((node) => {
    const toggle = node.querySelector('[data-dropdown-toggle]');
    if (!toggle) {
      return;
    }
    let touchHandled = false;
    const toggleOpen = (event) => {
      event.preventDefault();
      event.stopPropagation();
      const isOpen = node.dataset.open === '1';
      closeAll();
      closeMenu();
      if (!isOpen) {
        setOpen(node, true);
      }
    };
    toggle.addEventListener('touchstart', (event) => {
      touchHandled = true;
      toggleOpen(event);
    }, { passive: false });
    toggle.addEventListener('click', (event) => {
      if (touchHandled) {
        touchHandled = false;
        return;
      }
      toggleOpen(event);
    });
  });
  document.addEventListener('click', (event) => {
    if (event.target.closest && event.target.closest('[data-dropdown]')) {
      return;
    }
    closeAll();
  });
  document.addEventListener('touchstart', (event) => {
    if (event.target.closest && event.target.closest('[data-dropdown]')) {
      return;
    }
    closeAll();
  }, { passive: true });
  document.addEventListener('click', (event) => {
    const item = event.target.closest && event.target.closest('[data-dropdown] a');
    if (item) {
      closeAll();
    }
  });
  document.addEventListener('keydown', (event) => {
    if (event.key === 'Escape') {
      closeAll();
    }
  });
})();
</script>
"#;
    PreEscaped(script.to_string())
}
