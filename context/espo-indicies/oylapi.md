# oylapi indices

DB namespace prefix: `oylapi:`

Assumption: this refactor will ship with a mandatory full reindex. No backwards compatibility, in-place migrations, or mixed-schema reads are required.

The `oylapi` module does not currently write/read any keys under its own `oylapi:` namespace.

What it does instead:
- It constructs providers over other module namespaces (`essentials:`, `ammdata:`, `subfrost:`) and serves HTTP/RPC off those indices.
- `set_mdb` ignores the injected module `Mdb` and creates new `Mdb` instances pointing at the other namespaces.

Compatible with the refactor rules: N/A (no indices in this module namespace)

Integration notes:
- If you want `oylapi` to own persistent indices/caches in the future, keep them strictly list-based or key-based and avoid aggregate `Vec/Map` payloads.
