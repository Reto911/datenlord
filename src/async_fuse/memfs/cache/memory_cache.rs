//! The in-memory cache.

use std::collections::HashMap as StdHashMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use tokio::sync::RwLock;

use super::policy::EvictPolicy;
use super::{Block, BlockCoordinate, BlockId, Storage};
use crate::async_fuse::fuse::protocol::INum;

/// The file-level cache.
type FileCache = Arc<RwLock<StdHashMap<BlockId, Block>>>;

/// Merge the content from `src` to `dst`. This will set `dst` to be dirty.
fn merge_two_blocks(src: &Block, dst: &mut Block) {
    dst.set_dirty();
    dst.update(src);
}

/// A builder to configure and build a `MemoryCache`.
pub struct MemoryCacheBuilder<P, S> {
    /// The `policy` used by the built `MemoryCache`
    policy: P,
    /// The `backend` of the built `MemoryCache`
    backend: S,
    /// The size of blocks
    block_size: usize,
    /// A flag that if the built `MemoryCache` runs in writing-through policy
    write_through: bool,
    /// The soft limit for the write back task. The value of `(3, 5)` meaning that
    /// the soft limit is `3/5` of the capacity of policy.
    limit: (usize, usize),
    /// The interval of the write back task
    interval: Duration,
}

impl<P, S> MemoryCacheBuilder<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync + 'static,
    S: Storage + Send + Sync + 'static,
{
    /// Create a builder.
    pub fn new(policy: P, backend: S, block_size: usize) -> Self {
        Self {
            policy,
            backend,
            block_size,
            write_through: true,
            limit: (3, 5),
            interval: Duration::from_millis(100),
        }
    }

    /// Set write policy. Write through is enabled by default.
    pub fn write_through(mut self, write_through: bool) -> Self {
        self.write_through = write_through;
        self
    }

    /// Set the soft limit for the write back task.
    ///
    /// Both `(0, x)` and `(x, 0)` patterns are illegal and will be ignored.
    pub fn limit(mut self, limit: (usize, usize)) -> Self {
        if limit.0 == 0 || limit.1 == 0 {
            return self;
        }

        self.limit = limit;
        self
    }

    /// Set the interval for the write back task
    pub fn interval(mut self, interval: Duration) -> Self {
        self.interval = interval;
        self
    }

    /// Builds a `MemoryCache`. Make sure that this method is called in `tokio` runtime.
    pub fn build(self) -> Arc<MemoryCache<P, S>> {
        let MemoryCacheBuilder {
            policy,
            backend,
            block_size,
            ..
        } = self;

        Arc::new(MemoryCache {
            map: HashMap::new(),
            policy,
            backend,
            block_size,
        })
    }
}

/// The in-memory cache, implemented with lockfree hashmaps.
#[derive(Debug)]
pub struct MemoryCache<P, S> {
    /// The inner map where the cached blocks stored
    map: HashMap<INum, FileCache>,
    /// The evict policy
    policy: P,
    /// The backend storage
    backend: S,
    /// The block size
    block_size: usize,
}

impl<P, S> MemoryCache<P, S> {
    /// The limit of retrying to insert a block into the cache.
    const INSERT_RETRY_LIMMIT: usize = 10;

    /// Get the file-level cache `HashMap`.
    fn get_file_cache(&self, ino: INum) -> Option<FileCache> {
        let guard = pin();
        self.map.get(&ino, &guard).cloned()
    }

    /// Get a block from the in-memory cache without fetch it from backend.
    ///
    /// Notice that this method will also not change the position of the block
    /// in the policy.
    async fn get_block_from_cache(&self, ino: INum, block_id: usize) -> Option<Block> {
        let block = if let Some(file_cache) = self.get_file_cache(ino) {
            file_cache.read().await.get(&block_id).cloned()
        } else {
            None
        };

        block
    }

    /// Update a block into cache in place.
    /// Return the block if success (the destination existing in cache),
    /// otherwise returns `None`.
    async fn update_block(&self, ino: INum, block_id: usize, src: &Block) -> Option<Block> {
        let res = if let Some(file_cache) = self.get_file_cache(ino) {
            let mut file_cache = file_cache.write().await;
            if let Some(block) = file_cache.get_mut(&block_id) {
                merge_two_blocks(src, block);
                Some(block.clone())
            } else {
                None
            }
        } else {
            None
        };
        res
    }

    /// Try to evict a block from the cache to backend, if needed.
    async fn evict(&self)
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let evicted = self.policy.evict();
        // There is still a gap between the removal from policy and the lock on
        // file-level cache The evicted block may be modified and inserted to
        // the cache again during the gap This may cause the block evicted to
        // the backend incorrectly, but will not cause incorrect data fetched by
        // the user. TODO: Maybe it's better to lock the `policy` via a `Mutex`.
        if let Some(BlockCoordinate(ino, block_id)) = evicted {
            if let Some(file_cache) = self.get_file_cache(ino) {
                let mut file_cache = file_cache.write().await;

                // With the file-level write lock protected, there is no gap
                // between the removal of evicted block and writing to the backend
                if let Some(evicted) = file_cache.remove(&block_id) {
                    // A dirty block in cache must come from the backend (as it's already dirty when
                    // in backend), or be inserted into cache by storing, which
                    // will be written-through to the backend. That means, if a
                    // block in cache is dirty, it must have been shown in the backend.
                    // Therefore, we can just drop it when evicting.
                    if !evicted.dirty() {
                        self.backend.store(ino, block_id, evicted).await;
                    }
                }
            }
        }
    }

    /// Write a block into the cache without writing through.
    ///
    /// A block may be evicted to the backend.
    async fn write_block_into_cache(&self, ino: INum, block_id: usize, block: Block)
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let mut retry_times = 0;

        let mut file_cache = loop {
            // TODO: returns error instead of `panic`.
            assert!(
                retry_times < Self::INSERT_RETRY_LIMMIT,
                "Gave up retrying to insert a block into the cache."
            );

            let file_cache = {
                let guard = pin();
                Arc::clone(self.map.get_or_insert(ino, Arc::default(), &guard)).write_owned()
            }
            .await;

            let success = self.policy.try_put(BlockCoordinate(ino, block_id));

            if success {
                break file_cache;
            }
            drop(file_cache);
            self.evict().await;
            retry_times = retry_times.overflow_add(1);
        };

        file_cache.insert(block_id, block);
    }
}

#[async_trait]
impl<P, S> Storage for MemoryCache<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    async fn load_from_self(&self, ino: INum, block_id: usize) -> Option<Block> {
        let res = self.get_block_from_cache(ino, block_id).await;
        if res.is_some() {
            self.policy.touch(&BlockCoordinate(ino, block_id));
        }
        res
    }

    async fn load_from_backend(&self, ino: INum, block_id: usize) -> Option<Block> {
        self.backend.load(ino, block_id).await
    }

    async fn cache_block_from_backend(&self, ino: INum, block_id: usize, block: Block) {
        self.write_block_into_cache(ino, block_id, block).await;
    }

    async fn store(&self, ino: INum, block_id: usize, input: Block) {
        let start_offset = input.start();
        let end_offset = input.end();

        // TODO: Return error instead of panic.
        assert!(end_offset <= self.block_size, "out of range");

        // If the writing block is the whole block, then there is no need to fetch a
        // block from cache or backend, as the block in storage will be
        // overwritten directly.
        if start_offset == 0 && end_offset == self.block_size {
            self.write_block_into_cache(ino, block_id, input.clone())
                .await;
            self.backend.store(ino, block_id, input).await;
            return;
        }

        let dirty_block = if let Some(inserted) = self.update_block(ino, block_id, &input).await {
            self.policy.touch(&BlockCoordinate(ino, block_id));
            inserted
        } else {
            let mut to_be_inserted = self.backend.load(ino, block_id).await.unwrap_or_else(|| {
                // Create a new block for write, despite the offset is larger than file size.
                Block::new_zeroed(self.block_size)
            });
            merge_two_blocks(&input, &mut to_be_inserted);
            self.write_block_into_cache(ino, block_id, to_be_inserted.clone())
                .await;

            to_be_inserted
        };

        self.backend.store(ino, block_id, dirty_block).await;
    }

    async fn remove(&self, ino: INum) {
        self.map.remove(&ino);
        self.backend.remove(ino).await;
    }

    async fn invalidate(&self, ino: INum) {
        self.map.remove(&ino);
        self.backend.invalidate(ino).await;
    }

    async fn flush(&self, ino: INum) {
        self.backend.flush(ino).await;
    }

    async fn flush_all(&self) {
        // The `InMemoryCache` cannot iterate its `HashMap`, thus it cannot `flush_all`
        // on all the file cache. As it runs with a "write through" policy, the
        // backend takes the responsibility to do the actual `flush_all`.
        self.backend.flush_all().await;
    }

    async fn truncate(&self, ino: INum, from_block: usize, to_block: usize, fill_start: usize) {
        debug_assert!(from_block >= to_block);

        {
            if let Some(file_cache) = self.get_file_cache(ino) {
                let mut file_cache = file_cache.write().await;
                for block_id in to_block..from_block {
                    file_cache.remove(&block_id);
                }

                if to_block > 0 && fill_start < self.block_size {
                    let fill_block_id = to_block.overflow_sub(1);
                    if let Some(block) = file_cache.get_mut(&fill_block_id) {
                        if fill_start >= block.end() {
                            return;
                        }

                        block.set_start(block.start().min(fill_start));

                        let fill_len = block.end().overflow_sub(fill_start);
                        let fill_content = vec![0; fill_len];
                        let write_start = fill_start.overflow_sub(block.start());

                        block
                            .make_mut_slice()
                            .get_mut(write_start..)
                            .unwrap_or_else(|| {
                                unreachable!("`fill_start` is checked to be less than block size.")
                            })
                            .copy_from_slice(&fill_content);

                        block.set_dirty();
                    }
                }
            };
        }

        self.backend
            .truncate(ino, from_block, to_block, fill_start)
            .await;
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used)]
mod tests {
    use std::sync::Arc;

    use super::{Block, BlockCoordinate, MemoryCache, MemoryCacheBuilder, Storage};
    use crate::async_fuse::memfs::cache::mock::MemoryStorage;
    use crate::async_fuse::memfs::cache::policy::LruPolicy;

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const CACHE_CAPACITY_IN_BLOCKS: usize = 4;

    type MemoryCacheType = MemoryCache<LruPolicy<BlockCoordinate>, Arc<MemoryStorage>>;

    fn prepare_empty_storage() -> (Arc<MemoryStorage>, Arc<MemoryCacheType>) {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache =
            MemoryCacheBuilder::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES).build();

        (backend, cache)
    }

    #[tokio::test]
    async fn test_write_whole_block() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        let loaded_from_cache = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded_from_cache.as_slice(), BLOCK_CONTENT);

        // test write through
        let loaded_from_backend = backend.load(ino, block_id).await.unwrap();
        assert_eq!(loaded_from_backend.as_slice(), loaded_from_cache.as_slice());
    }

    #[tokio::test]
    async fn test_overwrite() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 7, b"foo ");
        cache.store(ino, block_id, block).await;
        let loaded = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo foo ");

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 4, b"bar xxx ");
        cache.store(ino, block_id, block).await;
        let loaded = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded.as_slice(), b"bar foo ");
    }

    #[tokio::test]
    async fn test_load_inexist_block() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let block = cache.load(ino, 1).await;
        assert!(block.is_none());

        let block = cache.load(1, block_id).await;
        assert!(block.is_none());
    }

    #[tokio::test]
    async fn test_append() {
        let ino = 0;

        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, 0, block).await;

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 0, 4, b"xxx foo ");
        cache.store(ino, 1, block).await;

        let loaded = cache.load(ino, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), b"xxx \0\0\0\0");
    }

    #[tokio::test]
    async fn test_remove() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        cache.remove(ino).await;
        assert!(!backend.contains(ino, block_id));

        let loaded = cache.load(ino, block_id).await;
        assert!(loaded.is_none());
    }

    /// Prepare a backend and cache for test eviction.
    ///
    /// The backend does not contain any block.
    /// The cache contains block `[0, 4)` for file `ino=0`. All the blocks are
    /// not dirty.
    async fn prepare_data_for_evict() -> (Arc<MemoryStorage>, Arc<MemoryCacheType>) {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache =
            MemoryCacheBuilder::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES).build();

        // Fill the backend
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            backend.store(0, block_id, block).await;
        }

        // Warn up the data
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            cache.load(0, block_id).await;
        }

        // Clear the backend
        backend.remove(0).await;

        (backend, cache)
    }

    #[tokio::test]
    async fn test_evict() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Insert a block, and (0, 0) will be evicted
        assert!(!backend.contains(0, 0));
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        let loaded = backend.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_touch_by_load() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Touch (0, 0) by loading
        let _: Option<Block> = cache.load(0, 0).await;

        // LRU in cache: (0, 1) -> (0, 2) -> (0, 3) -> (0, 0)
        // Insert a block, and (0, 1) will be evicted
        assert!(!backend.contains(0, 1));
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        let loaded = backend.load(0, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_touch_by_store() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Touch (0, 0) by storing
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(0, 0, block).await;

        // LRU in cache: (0, 1) -> (0, 2) -> (0, 3) -> (0, 0)
        // Insert a block, and (0, 1) will be evicted
        assert!(!backend.contains(0, 1));
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        let loaded = backend.load(0, 1).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_evict_dirty_block() {
        let (backend, cache) = prepare_empty_storage();

        // Fill the cache
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let mut block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
            block.set_dirty(); // This will be done by `StorageManager` in productive env
            cache.store(0, block_id, block).await;
        }

        // Clear the backend
        backend.remove(0).await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // All of them are dirty
        // Insert a block, and (0, 0) will be evicted.
        // Because it's dirty, it will be dropped directly
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(1, 0, block).await;
        assert!(!backend.contains(0, 0));
    }

    #[tokio::test]
    async fn test_flush() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        cache.flush(ino).await;
        assert!(backend.flushed(ino));

        cache.flush_all().await;
        assert!(backend.flushed(ino));
    }

    #[tokio::test]
    async fn test_load_from_backend() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);
        cache.store(ino, block_id, block).await;

        cache.invalidate(ino).await;
        let loaded_from_cache = cache.get_block_from_cache(ino, block_id).await;
        assert!(loaded_from_cache.is_none());

        let loaded = cache.load(ino, block_id).await.unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_write_missing_block_in_middle() {
        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice_with_range(BLOCK_SIZE_IN_BYTES, 4, 7, &BLOCK_CONTENT[4..7]);
        cache.store(0, 0, block).await;
        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, b"\0\0\0\0bar\0");
        let loaded = cache.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), block.as_slice());
    }

    #[tokio::test]
    async fn test_truncate() {
        let ino = 0;
        let from_block = 8;
        let to_block = 4;

        let (backend, cache) = prepare_empty_storage();

        for block_id in 0..from_block {
            cache
                .store(ino, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
        }

        cache
            .truncate(ino, from_block, to_block, BLOCK_SIZE_IN_BYTES)
            .await;
        for block_id in to_block..from_block {
            assert!(!backend.contains(0, block_id));
            let block = cache.get_block_from_cache(ino, block_id).await;
            assert!(block.is_none());
        }
    }

    #[tokio::test]
    async fn test_truncate_may_fill() {
        let ino = 0;
        let from_block = 8;
        let to_block = 4;

        let (_, cache) = prepare_empty_storage();

        for block_id in 0..from_block {
            cache
                .store(0, block_id, Block::new_zeroed(BLOCK_SIZE_IN_BYTES))
                .await;
        }

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        cache.store(ino, 3, block).await;
        cache.truncate(ino, from_block, to_block, 4).await;

        let loaded = cache.load(ino, 3).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    async fn test_truncate_in_the_same_block() {
        let (_, cache) = prepare_empty_storage();

        let block = Block::from_slice(BLOCK_SIZE_IN_BYTES, BLOCK_CONTENT);

        cache.store(0, 0, block).await;
        cache.truncate(0, 1, 1, 4).await;

        let loaded = cache.load(0, 0).await.unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    #[should_panic(expected = "out of range")]
    async fn test_write_out_of_range() {
        let (_, cache) = prepare_empty_storage();

        let block = Block::new_zeroed(16);
        cache.store(0, 0, block).await;
    }
}