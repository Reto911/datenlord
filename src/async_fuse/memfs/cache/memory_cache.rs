//! The in-memory cache.

use async_trait::async_trait;
use clippy_utilities::OverflowArithmetic;
use lockfree_cuckoohash::{pin, LockFreeCuckooHash as HashMap};
use parking_lot::RwLock;
use std::collections::HashMap as StdHashMap;

use super::policy::EvictPolicy;
use super::{Block, BlockCoordinate, IoBlock, Storage};
use crate::async_fuse::fuse::protocol::INum;

/// Merge the content from `src` to `dst`. This will set `dst` to be dirty.
fn merge_two_blocks(src: &IoBlock, dst: &mut Block) {
    let start_offset = src.offset();
    let end_offset = src.end();
    let len_dst = dst.len();

    dst.set_dirty();

    dst.make_mut()
        .get_mut(start_offset..end_offset)
        .unwrap_or_else(|| {
            unreachable!(
                "Slice {}..{} in block is out of range of {}.",
                start_offset, end_offset, len_dst
            )
        })
        .copy_from_slice(src.as_slice());
}

/// The in-memory cache, implemented with lockfree hashmaps.
#[derive(Debug)]
pub struct InMemoryCache<P, S> {
    /// The inner map where the cached blocks stored
    map: HashMap<INum, RwLock<StdHashMap<usize, Block>>>,
    /// The evict policy
    policy: P,
    /// The backend storage
    backend: S,
    /// The block size
    block_size: usize,
}

impl<P, S> InMemoryCache<P, S> {
    /// Create a new `InMemoryCache` with specified `policy`, `backend` and `block_size`.
    pub fn new(policy: P, backend: S, block_size: usize) -> Self {
        InMemoryCache {
            map: HashMap::new(),
            policy,
            backend,
            block_size,
        }
    }

    /// Get a block from the in-memory cache without fetch it from backend.
    fn get_block_from_cache(&self, ino: INum, block_id: usize) -> Option<Block> {
        let guard = pin();
        let block = self
            .map
            .get(&ino, &guard)
            .and_then(|file_cache| file_cache.read().get(&block_id).cloned());
        block
    }

    /// Merge a block into cache in place.
    /// Return the block if success (the to-be-merged block existing in cache), otherwise returns `None`.
    fn merge_block_in_place(&self, ino: INum, block_id: usize, src: &IoBlock) -> Option<Block> {
        let guard = pin();

        let res = if let Some(mut lock) = self.map.get(&ino, &guard).map(RwLock::write) {
            if let Some(block) = lock.get_mut(&block_id) {
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

    /// Write a block into the cache without writing through.
    ///
    /// If an old block is to be evicted, returns it with its coordinate.
    fn write_block_into_cache(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> Option<(BlockCoordinate, Block)>
    where
        P: EvictPolicy<BlockCoordinate>,
    {
        let mut empty_file_cache = None;
        let to_be_evicted = self.policy.put(BlockCoordinate(ino, block_id));
        let guard = pin();

        // Try to get the evicted block
        let evicted = to_be_evicted.and_then(|coord| {
            self.map
                .get(&coord.0, &guard)
                .and_then(|file_cache| {
                    let mut cache_lock = file_cache.write();
                    let evicted = cache_lock.remove(&coord.1);
                    if cache_lock.is_empty() {
                        empty_file_cache = Some(coord.0);
                    }
                    evicted
                })
                .map(|evicted| (coord, evicted))
        });

        // Remove empty file cache
        if let Some(empty_cache_ino) = empty_file_cache {
            self.map.remove(&empty_cache_ino);
        }

        // Insert the written block into cache
        self.map
            .get_or_insert(ino, RwLock::new(StdHashMap::new()), &guard)
            .write()
            .insert(block_id, block);
        evicted
    }

    /// Write a block into the cache and evict a block to backend (if needed).
    async fn write_and_evict(&self, ino: INum, block_id: usize, block: Block)
    where
        P: EvictPolicy<BlockCoordinate> + Send + Sync,
        S: Storage + Send + Sync,
    {
        let evicted = self.write_block_into_cache(ino, block_id, block);
        if let Some((BlockCoordinate(e_ino, e_block), evicted)) = evicted {
            self.on_evict(e_ino, e_block, evicted).await;
        }
    }
}

#[async_trait]
impl<P, S> Storage for InMemoryCache<P, S>
where
    P: EvictPolicy<BlockCoordinate> + Send + Sync,
    S: Storage + Send + Sync,
{
    async fn load_from_self(&self, ino: INum, block_id: usize) -> Option<Block> {
        let res = self.get_block_from_cache(ino, block_id);
        if res.is_some() {
            self.policy.touch(&BlockCoordinate(ino, block_id));
        }
        res
    }

    /// Loads a block from the backend.
    ///
    /// If `backend` returns a `None`, this will create a new block with zeros filled,
    /// inserts it into the `backend`, and returns it to caller. That means, this method
    /// will never return `None`.
    async fn load_from_backend(&self, ino: INum, block_id: usize) -> Option<Block> {
        let from_backend = self.backend.load(ino, block_id).await;

        if from_backend.is_some() {
            from_backend
        } else {
            // If a fetching from backend still fails, create a new block filled with zeros,
            // then write it to cache and backend as stored.
            let mut zeroed_block = Block::new(self.block_size);

            // This block is to be written
            zeroed_block.set_dirty();

            self.backend
                .store(
                    ino,
                    block_id,
                    IoBlock::new(zeroed_block.clone(), 0, self.block_size),
                )
                .await;
            Some(zeroed_block)
        }
    }

    async fn cache_block_from_backend(
        &self,
        ino: INum,
        block_id: usize,
        block: Block,
    ) -> Option<(BlockCoordinate, Block)> {
        self.write_block_into_cache(ino, block_id, block)
    }

    async fn on_evict(&self, ino: INum, block_id: usize, evicted: Block) {
        // A dirty block in cache must come from the backend (as it's already dirty when in backend),
        // or be inserted into cache by storing, which will be written-through to the backend.
        // That means, if a block in cache is dirty, it must have been shown in the backend.
        // Therefore, we can just drop it when evicting.
        if evicted.dirty() {
            return;
        }
        self.backend
            .store(ino, block_id, IoBlock::new(evicted, 0, self.block_size))
            .await;
    }

    async fn store(&self, ino: INum, block_id: usize, mut block: IoBlock) {
        let start_offset = block.offset();
        let end_offset = block.end();

        // double check to ensure the block to be written is set to be dirty.
        block.set_dirty();

        // If the writing block is the whole block, then there is no need to fetch a block from cache or backend,
        // as the block in storage will be overwritten directly.
        if start_offset == 0 && end_offset == self.block_size {
            self.write_and_evict(ino, block_id, block.block().clone())
                .await;
            self.backend.store(ino, block_id, block).await;
            return;
        }

        if let Some(inserted) = self.merge_block_in_place(ino, block_id, &block) {
            // Cache hits and the merge can be merge in place,
            // write through and return.
            self.backend
                .store(ino, block_id, IoBlock::new(inserted, 0, self.block_size))
                .await;
            return;
        }

        let mut to_be_inserted = {
            let block_from_backend = self.backend.load(ino, block_id).await;
            block_from_backend.unwrap_or_else(|| {
                // Create a new block for write, despite the offset is larger than file size.
                Block::new(self.block_size)
            })
        };

        merge_two_blocks(&block, &mut to_be_inserted);

        self.write_and_evict(ino, block_id, to_be_inserted.clone())
            .await;

        self.backend
            .store(
                ino,
                block_id,
                IoBlock::new(to_be_inserted, 0, self.block_size),
            )
            .await;
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
        // The `InMemoryCache` cannot iterate its `HashMap`, thus it cannot `flush_all` on all the file cache.
        // As it runs with a "write through" policy, the backend takes the responsibility to do the actual `flush_all`.
        self.backend.flush_all().await;
    }

    async fn truncate(&self, ino: INum, from_block: usize, to_block: usize, fill_start: usize) {
        debug_assert!(from_block >= to_block);

        {
            let guard = pin();
            if let Some(mut file_cache) = self.map.get(&ino, &guard).map(|lock| lock.write()) {
                for block_id in to_block..from_block {
                    file_cache.remove(&block_id);
                }

                if to_block > 0 && fill_start < self.block_size {
                    let fill_block_id = to_block.overflow_sub(1);
                    if let Some(block) = file_cache.get_mut(&fill_block_id) {
                        let fill_len = self.block_size.overflow_sub(fill_start);
                        let fill_content = vec![0; fill_len];
                        block
                            .make_mut()
                            .get_mut(fill_start..)
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

    use super::{Block, BlockCoordinate, InMemoryCache, IoBlock, Storage};
    use crate::async_fuse::memfs::cache::{mock::MemoryStorage, policy::LruPolicy};

    const BLOCK_SIZE_IN_BYTES: usize = 8;
    const BLOCK_CONTENT: &[u8; BLOCK_SIZE_IN_BYTES] = b"foo bar ";
    const CACHE_CAPACITY_IN_BLOCKS: usize = 4;

    /// Create an `IoBlock`
    macro_rules! create_block {
        ($content:expr, $start:expr, $end:expr) => {{
            let mut block = crate::async_fuse::memfs::cache::Block::new(BLOCK_SIZE_IN_BYTES);
            block.make_mut().copy_from_slice(($content).as_slice());
            let io_block = crate::async_fuse::memfs::cache::IoBlock::new(block, $start, $end);
            io_block
        }};
        ($content:expr) => {
            create_block!($content, 0, BLOCK_SIZE_IN_BYTES)
        };
    }

    fn prepare_empty_storage() -> (
        Arc<MemoryStorage>,
        InMemoryCache<LruPolicy<BlockCoordinate>, Arc<MemoryStorage>>,
    ) {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        (backend, cache)
    }

    #[tokio::test]
    async fn test_write_whole_block() {
        let ino = 0;
        let block_id = 0;

        let (backend, cache) = prepare_empty_storage();

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block).await;

        let loaded_from_cache = cache.load(ino, block_id).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded_from_cache.as_slice(), BLOCK_CONTENT);

        // test write through
        let loaded_from_backend = backend
            .load(ino, block_id)
            .await
            .map(IoBlock::from)
            .unwrap();
        assert_eq!(loaded_from_backend.as_slice(), loaded_from_cache.as_slice());
    }

    #[tokio::test]
    async fn test_overwrite() {
        let ino = 0;
        let block_id = 0;

        let (_, cache) = prepare_empty_storage();

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block).await;

        let io_block = create_block!(b"xxx foo ", 4, 7);
        cache.store(ino, block_id, io_block).await;
        let loaded = cache.load(ino, block_id).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), b"foo foo ");

        let io_block = create_block!(b"bar xxx ", 0, 4);
        cache.store(ino, block_id, io_block).await;
        let loaded = cache.load(ino, block_id).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), b"bar foo ");
    }

    #[tokio::test]
    async fn test_load_inexist_block() {
        let ino = 0;
        let block_id = 0;
        let zeroed_block: IoBlock = Block::new(BLOCK_SIZE_IN_BYTES).into();

        let (_, cache) = prepare_empty_storage();

        let block = cache.load(ino, 1).await.map(IoBlock::from).unwrap();
        assert_eq!(block.as_slice(), zeroed_block.as_slice());

        let block = cache.load(1, block_id).await.map(IoBlock::from).unwrap();
        assert_eq!(block.as_slice(), zeroed_block.as_slice());
    }

    #[tokio::test]
    async fn test_append() {
        let ino = 0;

        let (_, cache) = prepare_empty_storage();

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, 0, io_block).await;

        let io_block = create_block!(b"xxx foo ", 0, 4);
        cache.store(ino, 1, io_block).await;

        let loaded = cache.load(ino, 1).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), b"xxx \0\0\0\0");
    }

    #[tokio::test]
    async fn test_remove() {
        let ino = 0;
        let block_id = 0;
        let zeroed_block: IoBlock = Block::new(BLOCK_SIZE_IN_BYTES).into();

        let (backend, cache) = prepare_empty_storage();

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block).await;

        cache.remove(ino).await;
        assert!(!backend.contains(ino, block_id));

        let loaded = cache.load(ino, block_id).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), zeroed_block.as_slice());
    }

    /// Prepare a backend and cache for test eviction.
    ///
    /// The backend does not contain any block.
    /// The cache contains block `[0, 4)` for file `ino=0`. All the blocks are not dirty.
    async fn prepare_data_for_evict() -> (
        Arc<MemoryStorage>,
        InMemoryCache<LruPolicy<BlockCoordinate>, Arc<MemoryStorage>>,
    ) {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        // Fill the backend
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let io_block = create_block!(BLOCK_CONTENT);
            backend.store(0, block_id, io_block).await;
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
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 0, io_block).await;
        let loaded = backend.load(0, 0).await.map(IoBlock::from).unwrap();
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
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 0, io_block).await;
        let loaded = backend.load(0, 1).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_touch_by_store() {
        let (backend, cache) = prepare_data_for_evict().await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // Touch (0, 0) by storing
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(0, 0, io_block).await;

        // LRU in cache: (0, 1) -> (0, 2) -> (0, 3) -> (0, 0)
        // Insert a block, and (0, 1) will be evicted
        assert!(!backend.contains(0, 1));
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 0, io_block).await;
        let loaded = backend.load(0, 1).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_evict_the_last_block_of_a_file() {
        let (_, cache) = prepare_data_for_evict().await;

        assert!(cache.map.contains_key(&0));

        // to evict all blocks of file 0
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let io_block = create_block!(BLOCK_CONTENT);
            cache.store(1, block_id, io_block).await;
        }

        assert!(!cache.map.contains_key(&0));
    }

    #[tokio::test]
    async fn test_evict_dirty_block() {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        // Fill the cache
        for block_id in 0..CACHE_CAPACITY_IN_BLOCKS {
            let io_block = create_block!(BLOCK_CONTENT);
            cache.store(0, block_id, io_block).await;
        }

        // Clear the backend
        backend.remove(0).await;

        // LRU in cache: (0, 0) -> (0, 1) -> (0, 2) -> (0, 3)
        // All of them are dirty
        // Insert a block, and (0, 0) will be evicted.
        // Because it's dirty, it will be dropped directly
        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(1, 0, io_block).await;
        assert!(!backend.contains(0, 0));
    }

    #[tokio::test]
    async fn test_flush() {
        let ino = 0;
        let block_id = 0;

        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block).await;

        cache.flush(ino).await;
        assert!(backend.flushed(ino));

        cache.flush_all().await;
        assert!(backend.flushed(ino));
    }

    #[tokio::test]
    async fn test_load_from_backend() {
        let ino = 0;
        let block_id = 0;

        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, Arc::clone(&backend), BLOCK_SIZE_IN_BYTES);

        let io_block = create_block!(BLOCK_CONTENT);
        cache.store(ino, block_id, io_block).await;

        cache.invalidate(ino).await;
        let loaded_from_cache = cache.get_block_from_cache(ino, block_id);
        assert!(loaded_from_cache.is_none());

        let loaded = cache.load(ino, block_id).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), BLOCK_CONTENT);
    }

    #[tokio::test]
    async fn test_write_missing_block_in_middle() {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, backend, BLOCK_SIZE_IN_BYTES);

        let io_block = create_block!(BLOCK_CONTENT, 4, 7);
        cache.store(0, 0, io_block).await;
        let io_block = create_block!(b"\0\0\0\0bar\0");
        let loaded = cache.load(0, 0).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), io_block.as_slice());
    }

    #[tokio::test]
    async fn test_truncate() {
        let ino = 0;
        let from_block = 8;
        let to_block = 4;

        let (backend, cache) = prepare_empty_storage();

        for block_id in 0..from_block {
            cache
                .store(0, block_id, Block::new(BLOCK_SIZE_IN_BYTES).into())
                .await;
        }

        cache
            .truncate(ino, from_block, to_block, BLOCK_SIZE_IN_BYTES)
            .await;
        for block_id in to_block..from_block {
            assert!(!backend.contains(0, block_id));
            let block = cache.get_block_from_cache(ino, block_id);
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
                .store(0, block_id, Block::new(BLOCK_SIZE_IN_BYTES).into())
                .await;
        }

        let block = create_block!(BLOCK_CONTENT);

        cache.store(ino, 3, block).await;
        cache.truncate(ino, from_block, to_block, 4).await;

        let loaded = cache.load(ino, 3).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    async fn test_truncate_in_the_same_block() {
        let (_, cache) = prepare_empty_storage();

        let io_block = create_block!(BLOCK_CONTENT);

        cache.store(0, 0, io_block).await;
        cache.truncate(0, 1, 1, 4).await;

        let loaded = cache.load(0, 0).await.map(IoBlock::from).unwrap();
        assert_eq!(loaded.as_slice(), b"foo \0\0\0\0");
    }

    #[tokio::test]
    #[should_panic(expected = "out of range")]
    async fn test_write_out_of_range() {
        let policy = LruPolicy::<BlockCoordinate>::new(CACHE_CAPACITY_IN_BLOCKS);
        let backend = Arc::new(MemoryStorage::new(BLOCK_SIZE_IN_BYTES));
        let cache = InMemoryCache::new(policy, backend, BLOCK_SIZE_IN_BYTES);

        let io_block = IoBlock::new(Block::new(16), 0, 16);
        cache.store(0, 0, io_block).await;
    }
}
