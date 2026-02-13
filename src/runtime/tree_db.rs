use bitcoin::BlockHash;
use bitcoin::hashes::{Hash as _, sha256};
use borsh::{BorshDeserialize, BorshSerialize};
use rocksdb::{DB, Error as RocksError, WriteBatch};
use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, OnceLock, RwLock};

const ROOT_PREFIX: &[u8] = b"__espo_tree:";
const NODE_PREFIX: &[u8] = b"__espo_tree:node:";
const META_ACTIVE_ROOT: &[u8] = b"__espo_tree:meta:active_root";
const META_ACTIVE_BLOCK: &[u8] = b"__espo_tree:meta:active_block";
const META_PINNED_ROOT: &[u8] = b"__espo_tree:meta:pinned_root";
const META_PIN_UNTIL_HEIGHT: &[u8] = b"__espo_tree:meta:pin_until_height";
const BLOCK_ROOT_PREFIX: &[u8] = b"__espo_tree:block:";
const HEIGHT_BLOCK_PREFIX: &[u8] = b"__espo_tree:height:";

#[derive(Clone, PartialEq, Eq, Default, BorshSerialize, BorshDeserialize)]
struct TrieNode {
    value: Option<Vec<u8>>,
    children: BTreeMap<u8, TrieEdge>,
}

#[derive(Clone, PartialEq, Eq, BorshSerialize, BorshDeserialize)]
struct TrieEdge {
    label: Vec<u8>,
    child: [u8; 32],
}

#[derive(Clone, PartialEq, Eq, Default, BorshSerialize, BorshDeserialize)]
struct LegacyTrieNode {
    value: Option<Vec<u8>>,
    children: BTreeMap<u8, [u8; 32]>,
}

#[derive(Clone, Copy)]
struct PrefixCursor {
    node_id: [u8; 32],
    key_buf_len: usize,
}

fn common_prefix_len(a: &[u8], b: &[u8]) -> usize {
    let mut i = 0usize;
    let limit = a.len().min(b.len());
    while i < limit && a[i] == b[i] {
        i += 1;
    }
    i
}

#[derive(Clone, Copy)]
struct BlockContext {
    height: u32,
    block_hash: [u8; 32],
    working_root: [u8; 32],
}

struct TreeState {
    active_root: [u8; 32],
    active_block: Option<[u8; 32]>,
    pinned_root: Option<[u8; 32]>,
    pin_until_height: Option<u32>,
    current_block: Option<BlockContext>,
}

impl Default for TreeState {
    fn default() -> Self {
        Self {
            active_root: empty_root_id(),
            active_block: None,
            pinned_root: None,
            pin_until_height: None,
            current_block: None,
        }
    }
}

pub struct VersionedTreeDb {
    db: Arc<DB>,
    state: RwLock<TreeState>,
}

static TREE_DB: OnceLock<Arc<VersionedTreeDb>> = OnceLock::new();

pub fn init_global_tree_db(db: Arc<DB>) -> Result<(), RocksError> {
    let tree = Arc::new(VersionedTreeDb::new(db)?);
    let _ = TREE_DB.set(tree);
    Ok(())
}

pub fn get_global_tree_db() -> Option<Arc<VersionedTreeDb>> {
    TREE_DB.get().cloned()
}

fn empty_root_id() -> [u8; 32] {
    let empty = TrieNode::default();
    hash_node(&empty)
}

fn hash_node(node: &TrieNode) -> [u8; 32] {
    let encoded = borsh::to_vec(node).expect("trie node serialization must succeed");
    sha256::Hash::hash(&encoded).to_byte_array()
}

fn node_key(id: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(NODE_PREFIX.len() + id.len());
    out.extend_from_slice(NODE_PREFIX);
    out.extend_from_slice(id);
    out
}

fn block_root_key(hash: &[u8; 32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(BLOCK_ROOT_PREFIX.len() + hash.len());
    out.extend_from_slice(BLOCK_ROOT_PREFIX);
    out.extend_from_slice(hash);
    out
}

fn height_block_key(height: u32) -> Vec<u8> {
    let mut out = Vec::with_capacity(HEIGHT_BLOCK_PREFIX.len() + 4);
    out.extend_from_slice(HEIGHT_BLOCK_PREFIX);
    out.extend_from_slice(&height.to_be_bytes());
    out
}

impl VersionedTreeDb {
    pub fn new(db: Arc<DB>) -> Result<Self, RocksError> {
        let empty = TrieNode::default();
        let empty_id = hash_node(&empty);
        db.put(node_key(&empty_id), borsh::to_vec(&empty).expect("empty trie serialization"))?;

        let mut state = TreeState::default();

        if let Some(bytes) = db.get(META_ACTIVE_ROOT)? {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                state.active_root = arr;
            }
        }
        if let Some(bytes) = db.get(META_ACTIVE_BLOCK)? {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                state.active_block = Some(arr);
            }
        }
        if let Some(bytes) = db.get(META_PINNED_ROOT)? {
            if bytes.len() == 32 {
                let mut arr = [0u8; 32];
                arr.copy_from_slice(&bytes);
                state.pinned_root = Some(arr);
            }
        }
        if let Some(bytes) = db.get(META_PIN_UNTIL_HEIGHT)? {
            if bytes.len() == 4 {
                let mut arr = [0u8; 4];
                arr.copy_from_slice(&bytes);
                state.pin_until_height = Some(u32::from_be_bytes(arr));
            }
        }

        Ok(Self { db, state: RwLock::new(state) })
    }

    pub fn begin_block(
        &self,
        height: u32,
        block_hash: &BlockHash,
        parent_hash: &BlockHash,
    ) -> Result<(), RocksError> {
        let parent_arr = parent_hash.to_byte_array();
        let base_root = self.root_for_blockhash_bytes(&parent_arr)?.unwrap_or_else(empty_root_id);

        let mut st = self.state.write().expect("tree state poisoned");
        st.current_block = Some(BlockContext {
            height,
            block_hash: block_hash.to_byte_array(),
            working_root: base_root,
        });
        Ok(())
    }

    pub fn finish_block(&self) -> Result<(), RocksError> {
        let mut st = self.state.write().expect("tree state poisoned");
        let Some(ctx) = st.current_block.take() else {
            return Ok(());
        };

        let mut wb = WriteBatch::default();
        wb.put(block_root_key(&ctx.block_hash), ctx.working_root);
        wb.put(height_block_key(ctx.height), ctx.block_hash);

        let mut should_switch_active = true;
        if let Some(until_height) = st.pin_until_height {
            if ctx.height <= until_height {
                should_switch_active = false;
            } else {
                st.pin_until_height = None;
                st.pinned_root = None;
                wb.delete(META_PIN_UNTIL_HEIGHT);
                wb.delete(META_PINNED_ROOT);
            }
        }

        if should_switch_active {
            st.active_root = ctx.working_root;
            st.active_block = Some(ctx.block_hash);
            wb.put(META_ACTIVE_ROOT, ctx.working_root);
            wb.put(META_ACTIVE_BLOCK, ctx.block_hash);
        } else {
            wb.put(META_ACTIVE_ROOT, st.active_root);
            if let Some(active_block) = st.active_block {
                wb.put(META_ACTIVE_BLOCK, active_block);
            }
        }

        self.db.write(wb)
    }

    pub fn pin_active_root_until_height(&self, until_height: u32) -> Result<(), RocksError> {
        let mut st = self.state.write().expect("tree state poisoned");
        st.pinned_root = Some(st.active_root);
        st.pin_until_height = Some(until_height);

        let mut wb = WriteBatch::default();
        wb.put(META_PINNED_ROOT, st.active_root);
        wb.put(META_PIN_UNTIL_HEIGHT, until_height.to_be_bytes());
        self.db.write(wb)
    }

    pub fn blockhash_for_height(&self, height: u32) -> Result<Option<BlockHash>, RocksError> {
        let Some(bytes) = self.db.get(height_block_key(height))? else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Ok(None);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(BlockHash::from_byte_array(arr)))
    }

    pub fn active_root(&self) -> [u8; 32] {
        let st = self.state.read().expect("tree state poisoned");
        st.pinned_root.unwrap_or(st.active_root)
    }

    pub fn root_for_blockhash(
        &self,
        block_hash: &BlockHash,
    ) -> Result<Option<[u8; 32]>, RocksError> {
        self.root_for_blockhash_bytes(&block_hash.to_byte_array())
    }

    fn root_for_blockhash_bytes(
        &self,
        block_hash: &[u8; 32],
    ) -> Result<Option<[u8; 32]>, RocksError> {
        let Some(bytes) = self.db.get(block_root_key(block_hash))? else {
            return Ok(None);
        };
        if bytes.len() != 32 {
            return Ok(None);
        }
        let mut arr = [0u8; 32];
        arr.copy_from_slice(&bytes);
        Ok(Some(arr))
    }

    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let root = self.active_root();
        self.get_at_root(root, key)
    }

    pub fn get_at_root(&self, root: [u8; 32], key: &[u8]) -> Result<Option<Vec<u8>>, RocksError> {
        let mut current = root;
        let mut depth = 0usize;
        while depth < key.len() {
            let node = self.load_node(&current)?;
            let edge = match node.children.get(&key[depth]) {
                Some(edge) => edge,
                None => return Ok(None),
            };
            if !key[depth..].starts_with(&edge.label) {
                return Ok(None);
            }
            depth += edge.label.len();
            current = edge.child;
        }
        let node = self.load_node(&current)?;
        Ok(node.value)
    }

    pub fn multi_get(&self, keys: &[Vec<u8>]) -> Result<Vec<Option<Vec<u8>>>, RocksError> {
        let root = self.active_root();
        keys.iter().map(|k| self.get_at_root(root, k)).collect()
    }

    pub fn put(&self, key: &[u8], value: &[u8]) -> Result<(), RocksError> {
        self.apply_mutation(key, Some(value.to_vec()))
    }

    pub fn delete(&self, key: &[u8]) -> Result<(), RocksError> {
        self.apply_mutation(key, None)
    }

    pub fn apply_batch(&self, changes: &[(Vec<u8>, Option<Vec<u8>>)]) -> Result<(), RocksError> {
        if changes.is_empty() {
            return Ok(());
        }
        let mut st = self.state.write().expect("tree state poisoned");
        let mut cache: HashMap<[u8; 32], TrieNode> = HashMap::new();
        if let Some(ctx) = st.current_block.as_mut() {
            let mut root = ctx.working_root;
            for (k, v) in changes {
                root = self.apply_single(root, k, v.clone(), &mut cache)?;
            }
            ctx.working_root = root;
            return Ok(());
        }

        let mut root = st.active_root;
        for (k, v) in changes {
            root = self.apply_single(root, k, v.clone(), &mut cache)?;
        }
        st.active_root = root;

        let mut wb = WriteBatch::default();
        wb.put(META_ACTIVE_ROOT, root);
        if let Some(active_block) = st.active_block {
            wb.put(META_ACTIVE_BLOCK, active_block);
        }
        self.db.write(wb)
    }

    pub fn collect_prefixed_keys(&self, prefix: &[u8]) -> Result<Vec<Vec<u8>>, RocksError> {
        let root = self.active_root();
        self.collect_prefixed_keys_at_root(root, prefix)
    }

    pub fn collect_prefixed_keys_at_root(
        &self,
        root: [u8; 32],
        prefix: &[u8],
    ) -> Result<Vec<Vec<u8>>, RocksError> {
        let entries = self.collect_prefixed_entries_at_root(root, prefix)?;
        Ok(entries.into_iter().map(|(k, _)| k).collect())
    }

    pub fn collect_prefixed_entries(
        &self,
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let root = self.active_root();
        self.collect_prefixed_entries_at_root(root, prefix)
    }

    pub fn collect_prefixed_entries_at_root(
        &self,
        root: [u8; 32],
        prefix: &[u8],
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>, RocksError> {
        let mut key = Vec::new();
        let Some(cursor) = self.find_prefix_cursor(root, prefix, &mut key)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        key.truncate(cursor.key_buf_len);
        self.collect_entries(cursor.node_id, &mut key, &mut out)?;
        Ok(out)
    }

    fn apply_mutation(&self, key: &[u8], value: Option<Vec<u8>>) -> Result<(), RocksError> {
        let mut st = self.state.write().expect("tree state poisoned");
        let mut cache: HashMap<[u8; 32], TrieNode> = HashMap::new();
        if let Some(ctx) = st.current_block.as_mut() {
            ctx.working_root = self.apply_single(ctx.working_root, key, value, &mut cache)?;
            return Ok(());
        }

        st.active_root = self.apply_single(st.active_root, key, value, &mut cache)?;
        let mut wb = WriteBatch::default();
        wb.put(META_ACTIVE_ROOT, st.active_root);
        if let Some(active_block) = st.active_block {
            wb.put(META_ACTIVE_BLOCK, active_block);
        }
        self.db.write(wb)
    }

    fn apply_single(
        &self,
        root: [u8; 32],
        key: &[u8],
        value: Option<Vec<u8>>,
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<[u8; 32], RocksError> {
        let updated = self.update_node(root, key, 0, value.as_deref(), cache)?;
        Ok(updated.unwrap_or_else(empty_root_id))
    }

    fn update_node(
        &self,
        node_id: [u8; 32],
        key: &[u8],
        depth: usize,
        value: Option<&[u8]>,
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<Option<[u8; 32]>, RocksError> {
        let before = self.load_node_cached(node_id, cache)?;
        let mut node = before.clone();

        if depth == key.len() {
            node.value = value.map(|v| v.to_vec());
        } else {
            let remaining = &key[depth..];
            let edge_key = remaining[0];
            match node.children.get(&edge_key).cloned() {
                None => {
                    if let Some(v) = value {
                        let edge = self.build_edge(remaining, v, cache)?;
                        node.children.insert(edge_key, edge);
                    } else {
                        return Ok(Some(node_id));
                    }
                }
                Some(existing_edge) => {
                    let common = common_prefix_len(&existing_edge.label, remaining);
                    if common == existing_edge.label.len() {
                        let next = self.update_node(
                            existing_edge.child,
                            key,
                            depth + common,
                            value,
                            cache,
                        )?;
                        match next {
                            Some(id) => {
                                let compact =
                                    self.compact_edge(existing_edge.label.clone(), id, cache)?;
                                node.children.insert(edge_key, compact);
                            }
                            None => {
                                node.children.remove(&edge_key);
                            }
                        }
                    } else if value.is_some() {
                        let mut split = TrieNode::default();
                        let old_suffix = existing_edge.label[common..].to_vec();
                        let old_first = old_suffix[0];
                        split.children.insert(
                            old_first,
                            TrieEdge { label: old_suffix, child: existing_edge.child },
                        );

                        let new_suffix = &remaining[common..];
                        if new_suffix.is_empty() {
                            split.value = value.map(|v| v.to_vec());
                        } else {
                            let new_edge = self.build_edge(
                                new_suffix,
                                value.expect("checked is_some"),
                                cache,
                            )?;
                            split.children.insert(new_edge.label[0], new_edge);
                        }

                        let split_id = self.store_node_cached(&split, cache)?;
                        let compact = self.compact_edge(
                            existing_edge.label[..common].to_vec(),
                            split_id,
                            cache,
                        )?;
                        node.children.insert(edge_key, compact);
                    } else {
                        return Ok(Some(node_id));
                    }
                }
            }
        }

        if node.value.is_none() && node.children.is_empty() {
            return Ok(None);
        }
        if node == before {
            return Ok(Some(node_id));
        }

        let id = self.store_node_cached(&node, cache)?;
        Ok(Some(id))
    }

    fn build_edge(
        &self,
        suffix: &[u8],
        value: &[u8],
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<TrieEdge, RocksError> {
        debug_assert!(!suffix.is_empty());
        let leaf = TrieNode { value: Some(value.to_vec()), children: BTreeMap::new() };
        let child = self.store_node_cached(&leaf, cache)?;
        Ok(TrieEdge { label: suffix.to_vec(), child })
    }

    fn compact_edge(
        &self,
        mut label: Vec<u8>,
        mut child: [u8; 32],
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<TrieEdge, RocksError> {
        loop {
            let next = self.load_node_cached(child, cache)?;
            if next.value.is_some() || next.children.len() != 1 {
                break;
            }
            let (_, edge) = next.children.iter().next().expect("checked len");
            label.extend_from_slice(&edge.label);
            child = edge.child;
        }
        Ok(TrieEdge { label, child })
    }

    fn find_prefix_cursor(
        &self,
        root: [u8; 32],
        prefix: &[u8],
        key_buf: &mut Vec<u8>,
    ) -> Result<Option<PrefixCursor>, RocksError> {
        let mut current = root;
        let mut depth = 0usize;
        while depth < prefix.len() {
            let node = self.load_node(&current)?;
            let edge = match node.children.get(&prefix[depth]) {
                Some(edge) => edge,
                None => return Ok(None),
            };
            let common = common_prefix_len(&edge.label, &prefix[depth..]);
            if common == 0 {
                return Ok(None);
            }

            if common < edge.label.len() {
                if depth + common == prefix.len() {
                    key_buf.extend_from_slice(&edge.label);
                    return Ok(Some(PrefixCursor {
                        node_id: edge.child,
                        key_buf_len: key_buf.len(),
                    }));
                }
                return Ok(None);
            }

            key_buf.extend_from_slice(&edge.label);
            depth += common;
            current = edge.child;
        }
        Ok(Some(PrefixCursor { node_id: current, key_buf_len: key_buf.len() }))
    }

    fn collect_entries(
        &self,
        node_id: [u8; 32],
        key_buf: &mut Vec<u8>,
        out: &mut Vec<(Vec<u8>, Vec<u8>)>,
    ) -> Result<(), RocksError> {
        let node = self.load_node(&node_id)?;
        if let Some(v) = node.value {
            out.push((key_buf.clone(), v));
        }
        for (_, edge) in node.children {
            key_buf.extend_from_slice(&edge.label);
            self.collect_entries(edge.child, key_buf, out)?;
            key_buf.truncate(key_buf.len() - edge.label.len());
        }
        Ok(())
    }

    fn load_node(&self, id: &[u8; 32]) -> Result<TrieNode, RocksError> {
        let key = node_key(id);
        let Some(bytes) = self.db.get(key)? else {
            return Ok(TrieNode::default());
        };
        if let Ok(node) = TrieNode::try_from_slice(&bytes) {
            return Ok(node);
        }

        // Backward compatibility for legacy one-byte-per-edge trie encoding.
        if let Ok(old) = LegacyTrieNode::try_from_slice(&bytes) {
            let mut children = BTreeMap::new();
            for (k, child) in old.children {
                children.insert(k, TrieEdge { label: vec![k], child });
            }
            return Ok(TrieNode { value: old.value, children });
        }

        Ok(TrieNode::default())
    }

    fn load_node_cached(
        &self,
        id: [u8; 32],
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<TrieNode, RocksError> {
        if let Some(node) = cache.get(&id) {
            return Ok(node.clone());
        }
        let node = self.load_node(&id)?;
        cache.insert(id, node.clone());
        Ok(node)
    }

    fn store_node_cached(
        &self,
        node: &TrieNode,
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<[u8; 32], RocksError> {
        let id = hash_node(node);
        let key = node_key(&id);
        self.db.put(key, borsh::to_vec(node).expect("trie node serialization"))?;
        cache.insert(id, node.clone());
        Ok(id)
    }
}

pub fn is_tree_internal_key(full_key: &[u8]) -> bool {
    full_key.starts_with(ROOT_PREFIX)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_tree() -> (TempDir, VersionedTreeDb) {
        let dir = TempDir::new().expect("tempdir");
        let mut opts = rocksdb::Options::default();
        opts.create_if_missing(true);
        let db = Arc::new(DB::open(&opts, dir.path()).expect("rocksdb open"));
        let tree = VersionedTreeDb::new(db).expect("tree init");
        (dir, tree)
    }

    #[test]
    fn prefix_lookup_matches_mid_edge() {
        let (_dir, tree) = new_tree();
        let key = b"essentials:/address/v2/some-very-long-address/outpoint/txid:vout";
        let value = b"v1".to_vec();
        tree.apply_batch(&[(key.to_vec(), Some(value.clone()))]).expect("apply");

        let prefix = b"essentials:/address/v2/some-very-long-address/outpoint/";
        let entries = tree.collect_prefixed_entries(prefix).expect("collect");
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].0, key.to_vec());
        assert_eq!(entries[0].1, value);
    }

    #[test]
    fn split_and_delete_keep_sibling() {
        let (_dir, tree) = new_tree();
        let k1 = b"abcde".to_vec();
        let k2 = b"abc".to_vec();
        tree.apply_batch(&[(k1.clone(), Some(vec![1]))]).expect("put k1");
        tree.apply_batch(&[(k2.clone(), Some(vec![2]))]).expect("put k2");

        assert_eq!(tree.get(&k1).expect("get k1"), Some(vec![1]));
        assert_eq!(tree.get(&k2).expect("get k2"), Some(vec![2]));

        tree.apply_batch(&[(k2.clone(), None)]).expect("delete k2");
        assert_eq!(tree.get(&k2).expect("get k2 after delete"), None);
        assert_eq!(tree.get(&k1).expect("get k1 after delete"), Some(vec![1]));
    }

    #[test]
    fn collect_entries_order_is_lexicographic() {
        let (_dir, tree) = new_tree();
        let keys = vec![b"a/2".to_vec(), b"a/10".to_vec(), b"a/1".to_vec(), b"a/z".to_vec()];
        let mut changes = Vec::new();
        for (i, k) in keys.iter().enumerate() {
            changes.push((k.clone(), Some(vec![i as u8])));
        }
        tree.apply_batch(&changes).expect("apply");

        let entries = tree.collect_prefixed_entries(b"a/").expect("collect");
        let observed: Vec<Vec<u8>> = entries.into_iter().map(|(k, _)| k).collect();
        let mut sorted = observed.clone();
        sorted.sort();
        assert_eq!(observed, sorted);
    }

    #[test]
    fn legacy_node_encoding_is_readable() {
        let (_dir, tree) = new_tree();

        let old_leaf = LegacyTrieNode { value: Some(vec![0x99]), children: BTreeMap::new() };
        let old_leaf_id =
            sha256::Hash::hash(&borsh::to_vec(&old_leaf).expect("serialize legacy leaf"))
                .to_byte_array();
        tree.db
            .put(node_key(&old_leaf_id), borsh::to_vec(&old_leaf).expect("serialize legacy leaf"))
            .expect("put legacy leaf");

        let mut root_children = BTreeMap::new();
        root_children.insert(b'k', old_leaf_id);
        let old_root = LegacyTrieNode { value: None, children: root_children };
        let old_root_id =
            sha256::Hash::hash(&borsh::to_vec(&old_root).expect("serialize legacy root"))
                .to_byte_array();
        tree.db
            .put(node_key(&old_root_id), borsh::to_vec(&old_root).expect("serialize legacy root"))
            .expect("put legacy root");

        let got = tree.get_at_root(old_root_id, b"k").expect("get_at_root legacy");
        assert_eq!(got, Some(vec![0x99]));
    }

    #[test]
    fn block_roots_preserve_historical_reads() {
        let (_dir, tree) = new_tree();
        let key = b"essentials:/k";

        let genesis = BlockHash::from_byte_array([0u8; 32]);
        let h1 = BlockHash::from_byte_array([1u8; 32]);
        let h2 = BlockHash::from_byte_array([2u8; 32]);

        tree.begin_block(1, &h1, &genesis).expect("begin block 1");
        tree.apply_batch(&[(key.to_vec(), Some(vec![1]))]).expect("apply block 1");
        tree.finish_block().expect("finish block 1");

        tree.begin_block(2, &h2, &h1).expect("begin block 2");
        tree.apply_batch(&[(key.to_vec(), Some(vec![2]))]).expect("apply block 2");
        tree.finish_block().expect("finish block 2");

        let r1 = tree.root_for_blockhash(&h1).expect("root h1").expect("root exists");
        let r2 = tree.root_for_blockhash(&h2).expect("root h2").expect("root exists");
        assert_eq!(tree.get_at_root(r1, key).expect("get h1"), Some(vec![1]));
        assert_eq!(tree.get_at_root(r2, key).expect("get h2"), Some(vec![2]));
    }
}
