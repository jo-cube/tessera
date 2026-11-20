//! Payloads addressed by stable chunk indices. A snapshot shares the chunk
//! list; the writer copies only a shared, partially filled chunk before
//! appending. No references into a mutable chunk escape the owning table.
use std::sync::Arc;

use crate::chunk::{CHUNK_BYTES, Chunk, ChunkPool};

#[derive(Clone, Copy, Debug)]
pub(super) struct Location {
    allocation: usize,
    offset: u32,
    len: u32,
}

#[derive(Clone, Debug)]
struct Page {
    chunk: Chunk,
    used: usize,
}

#[derive(Clone, Debug)]
pub(super) struct Arena {
    pool: ChunkPool,
    pages: Vec<Arc<Page>>,
    active: Option<usize>,
    used: usize,
}

impl Arena {
    pub fn new(pool: &ChunkPool) -> Self {
        Self {
            pool: pool.clone(),
            pages: Vec::new(),
            active: None,
            used: 0,
        }
    }

    pub fn used(&self) -> usize {
        self.used
    }

    /// Used bytes of the partial chunk a snapshot would keep when it is copied.
    pub fn copy_charge(&self, len: usize) -> usize {
        self.active.map_or(0, |at| {
            let page = &self.pages[at];
            if len > 0 && len <= CHUNK_BYTES - page.used && Arc::strong_count(page) > 1 {
                page.used
            } else {
                0
            }
        })
    }

    /// Returns the location and the used bytes actually copied for snapshots.
    pub fn append(&mut self, parts: &[&[u8]]) -> (Location, usize) {
        let len = parts.iter().map(|part| part.len()).sum();
        if len == 0 {
            return (
                Location {
                    allocation: 0,
                    offset: 0,
                    len: 0,
                },
                0,
            );
        }
        let (allocation, offset, copied) = if len > CHUNK_BYTES {
            let allocation = self.pages.len();
            self.pages.push(Arc::new(Page {
                chunk: Chunk::exact_parts(parts),
                used: len,
            }));
            (allocation, 0, 0)
        } else {
            let at = match self.active {
                Some(at) if self.pages[at].used + len <= CHUNK_BYTES => at,
                _ => {
                    let at = self.pages.len();
                    self.pages.push(Arc::new(Page {
                        chunk: self.pool.take(),
                        used: 0,
                    }));
                    self.active = Some(at);
                    at
                }
            };
            let copied = self.copy_charge(len);
            let page = Arc::make_mut(&mut self.pages[at]);
            let offset = page.used;
            let mut end = offset;
            for part in parts {
                page.chunk.bytes_mut()[end..end + part.len()].copy_from_slice(part);
                end += part.len();
            }
            page.used += len;
            page.chunk.set_used(page.used);
            (at, offset, copied)
        };
        self.used += len;
        (
            Location {
                allocation,
                offset: offset as u32,
                len: len as u32,
            },
            copied,
        )
    }

    pub fn get(&self, location: Location) -> &[u8] {
        if location.len == 0 {
            return &[];
        }
        let offset = location.offset as usize;
        &self.pages[location.allocation].chunk.bytes()[offset..offset + location.len as usize]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_are_whole_and_accounting_is_used_bytes_including_garbage() {
        let pool = ChunkPool::new(2 * CHUNK_BYTES);
        let mut arena = Arena::new(&pool);
        let (first, _) = arena.append(&[b"a", &vec![1; CHUNK_BYTES - 10]]);
        let (second, _) = arena.append(&[b"b", &[2; 32]]);
        let (large, _) = arena.append(&[b"c", &vec![3; CHUNK_BYTES + 1]]);
        assert_ne!(first.allocation, second.allocation);
        assert_eq!(
            arena.pages[large.allocation].chunk.bytes().len(),
            CHUNK_BYTES + 2
        );
        assert_eq!(arena.used(), CHUNK_BYTES - 9 + 33 + CHUNK_BYTES + 2);
        assert_eq!(arena.get(first)[0], b'a');
        assert_eq!(
            arena.get(second),
            &b"b".iter().copied().chain([2; 32]).collect::<Vec<_>>()
        );
        assert_eq!(pool.stats().1, 2 * CHUNK_BYTES);
    }

    #[test]
    fn a_snapshot_copies_only_the_partial_chunk_and_keeps_its_bytes_alive() {
        let pool = ChunkPool::new(3 * CHUNK_BYTES);
        let mut arena = Arena::new(&pool);
        let (first, _) = arena.append(&[b"key", b"old"]);
        let snapshot = arena.clone();
        assert_eq!(arena.copy_charge(6), 6);
        let (_, copied) = arena.append(&[b"key", b"new"]);
        assert_eq!(copied, 6);
        assert_eq!(arena.used(), 12);
        assert_eq!(arena.get(first), b"keyold");
        assert_eq!(snapshot.get(first), b"keyold");
        assert_eq!(pool.stats().1, 2 * CHUNK_BYTES);
        drop(arena);
        assert_eq!(pool.stats().1, CHUNK_BYTES);
        assert_eq!(snapshot.get(first), b"keyold");
        drop(snapshot);
        assert_eq!(pool.stats().1, 0);
        assert_eq!(pool.stats().0, 2 * CHUNK_BYTES);
    }
}
