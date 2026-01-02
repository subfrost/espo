use axum::http::StatusCode;
use axum::http::header::CONTENT_TYPE;
use axum::response::{Html, IntoResponse};
use maud::{DOCTYPE, Markup, PreEscaped, html};

use crate::explorer::components::footer::footer;
use crate::explorer::components::svg_assets::{dots, icon_search, logo_espo};

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
    let markup = html! {
        (DOCTYPE)
        html lang="en" {
            head {
                meta charset="utf-8";
                meta name="viewport" content="width=device-width, initial-scale=1";
                title { (title) }
                link rel="stylesheet" href="/static/style.css";
            }
            body {
                header class="topbar" data-topbar="" {
                    div class="app" {
                        nav class="nav" data-nav-menu="" {
                            a class="brand" href="/" {
                                (logo_espo())
                                span class="brand-text" { "Espo" }
                            }
                            div class="nav-search hero-search" data-search="" {
                                form class="hero-search-form" method="get" action="/search" autocomplete="off" data-search-form="" {
                                    div class="hero-search-input" {
                                        span class="hero-search-icon" aria-hidden="true" { (icon_search()) }
                                        input class="hero-search-field" type="text" name="q" placeholder="Search blocks, alkanes, transactions" data-search-input="" aria-label="Search blocks, alkanes, transactions" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false";
                                        button class="hero-search-submit" type="submit" aria-label="Search" { (icon_search()) }
                                    }
                                }
                                div class="search-results" data-search-results="" aria-hidden="true" {
                                    div class="search-results-body" data-search-results-body="" {}
                                }
                            }
                            div class="navlinks-container" {
                                a class="navlink" href="/" { "Blocks" }
                                a class="navlink" href="/alkanes" { "Alkanes" }
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
                                a class="nav-menu-link" href="/" { "Blocks" }
                                a class="nav-menu-link" href="/alkanes" { "Alkanes" }
                            }
                        }
                    }
                    div class="nav-search-mobile" data-search-mobile="" aria-hidden="true" {
                        div class="app" {
                            div class="nav-search hero-search" data-search="" {
                                form class="hero-search-form" method="get" action="/search" autocomplete="off" data-search-form="" {
                                    div class="hero-search-input" {
                                        span class="hero-search-icon" aria-hidden="true" { (icon_search()) }
                                        input class="hero-search-field" type="text" name="q" placeholder="Search blocks, alkanes, transactions" data-search-input="" aria-label="Search blocks, alkanes, transactions" autocomplete="off" autocorrect="off" autocapitalize="off" spellcheck="false";
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
                (search_scripts())
                (PreEscaped(NAV_SCRIPT))
            }
        }
    };
    Html(markup.into_string())
}

fn search_scripts() -> Markup {
    let script = format!(
        r#"
<script>
(() => {{
  const SEARCH_DEBOUNCE_MS = {SEARCH_DEBOUNCE_MS};

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
      fetch(`/api/search/guess?q=${{encodeURIComponent(value)}}`, {{ signal: abortController.signal }})
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
      if (!input.value.trim()) {{
        event.preventDefault();
        clearResults();
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
