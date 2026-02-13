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
    children: BTreeMap<u8, [u8; 32]>,
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
        for b in key {
            let node = self.load_node(&current)?;
            let Some(next) = node.children.get(b) else {
                return Ok(None);
            };
            current = *next;
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
        let Some(prefix_node) = self.find_prefix_node(root, prefix)? else {
            return Ok(Vec::new());
        };
        let mut out = Vec::new();
        let mut key = prefix.to_vec();
        self.collect_entries(prefix_node, &mut key, &mut out)?;
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
            let edge = key[depth];
            let next = if let Some(child_id) = node.children.get(&edge).copied() {
                self.update_node(child_id, key, depth + 1, value, cache)?
            } else if value.is_some() {
                self.build_path(key, depth + 1, value.expect("checked is_some"), cache)?
            } else {
                None
            };

            match next {
                Some(id) => {
                    node.children.insert(edge, id);
                }
                None => {
                    node.children.remove(&edge);
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

    fn build_path(
        &self,
        key: &[u8],
        depth: usize,
        value: &[u8],
        cache: &mut HashMap<[u8; 32], TrieNode>,
    ) -> Result<Option<[u8; 32]>, RocksError> {
        let mut node = TrieNode::default();
        if depth == key.len() {
            node.value = Some(value.to_vec());
            return Ok(Some(self.store_node_cached(&node, cache)?));
        }

        let edge = key[depth];
        if let Some(child) = self.build_path(key, depth + 1, value, cache)? {
            node.children.insert(edge, child);
        }
        Ok(Some(self.store_node_cached(&node, cache)?))
    }

    fn find_prefix_node(
        &self,
        root: [u8; 32],
        prefix: &[u8],
    ) -> Result<Option<[u8; 32]>, RocksError> {
        let mut current = root;
        for b in prefix {
            let node = self.load_node(&current)?;
            let Some(next) = node.children.get(b).copied() else {
                return Ok(None);
            };
            current = next;
        }
        Ok(Some(current))
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
        for (b, child_id) in node.children {
            key_buf.push(b);
            self.collect_entries(child_id, key_buf, out)?;
            key_buf.pop();
        }
        Ok(())
    }

    fn load_node(&self, id: &[u8; 32]) -> Result<TrieNode, RocksError> {
        let key = node_key(id);
        let Some(bytes) = self.db.get(key)? else {
            return Ok(TrieNode::default());
        };
        Ok(TrieNode::try_from_slice(&bytes).unwrap_or_default())
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
