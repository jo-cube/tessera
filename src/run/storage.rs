//! Chunk-backed storage with one contiguous region per frame or dictionary.
//! Logical offsets map to shared allocations; exact tails avoid retaining
//! unused chunk capacity, while checkpoint writes stream the same run bytes.

use std::ops::Range;

use super::CorruptRun;
use super::reader::RunView;
use crate::chunk::{CHUNK_BYTES, Chunk, ChunkPool};

/// Consecutive logical bytes within one allocation. Adjacent frames coalesce;
/// a frame and a dictionary are always appended atomically, never split.
#[derive(Clone, Debug)]
pub(super) struct Piece {
    pub start: usize,
    pub allocation: usize,
    pub range: Range<usize>,
}

#[derive(Debug)]
pub(super) struct Packing {
    pool: ChunkPool,
    allocations: Vec<Chunk>,
    active: Option<(usize, usize)>,
    frames: Vec<Piece>,
    dictionaries: Vec<Piece>,
    pub frames_len: usize,
    pub dictionaries_len: usize,
}

impl Packing {
    pub fn new(pool: &ChunkPool) -> Self {
        Self {
            pool: pool.clone(),
            allocations: Vec::new(),
            active: None,
            frames: Vec::new(),
            dictionaries: Vec::new(),
            frames_len: 0,
            dictionaries_len: 0,
        }
    }

    fn append(&mut self, bytes: &[u8], dictionary: bool) {
        if bytes.is_empty() {
            return;
        }
        let (allocation, range) = if bytes.len() > CHUNK_BYTES {
            let at = self.allocations.len();
            self.allocations.push(Chunk::exact(bytes));
            (at, 0..bytes.len())
        } else {
            let (at, used) = match self.active {
                Some((at, used)) if used + bytes.len() <= CHUNK_BYTES => (at, used),
                _ => {
                    let at = self.allocations.len();
                    self.allocations.push(self.pool.take());
                    (at, 0)
                }
            };
            let range = used..used + bytes.len();
            self.allocations[at].bytes_mut()[range.clone()].copy_from_slice(bytes);
            self.allocations[at].set_used(range.end);
            self.active = Some((at, range.end));
            (at, range)
        };
        let (pieces, len) = if dictionary {
            (&mut self.dictionaries, &mut self.dictionaries_len)
        } else {
            (&mut self.frames, &mut self.frames_len)
        };
        if let Some(last) = pieces.last_mut()
            && last.allocation == allocation
            && last.range.end == range.start
        {
            last.range.end = range.end;
        } else {
            pieces.push(Piece {
                start: *len,
                allocation,
                range,
            });
        }
        *len += bytes.len();
    }

    pub fn compressed(&self, raw: usize, stored: usize, compressed: bool) {
        self.pool.compressed(raw, stored, compressed);
    }
    pub fn frame(&mut self, bytes: &[u8]) {
        self.append(bytes, false);
    }
    pub fn dictionary(&mut self, bytes: &[u8]) {
        self.append(bytes, true);
    }

    pub fn finish(mut self, tail: Box<[u8]>) -> Segmented {
        // Finished runs never pin a mostly empty final pool chunk. Piece
        // offsets stay valid because only unused bytes after the tail go away.
        if let Some((at, used)) = self.active
            && used < CHUNK_BYTES
        {
            self.allocations[at] = Chunk::exact(&self.allocations[at].bytes()[..used]);
        }
        Segmented {
            allocations: self.allocations.into_boxed_slice(),
            frames: self.frames.into_boxed_slice(),
            dictionaries: self.dictionaries.into_boxed_slice(),
            tail,
            frames_len: self.frames_len,
            dictionaries_len: self.dictionaries_len,
        }
    }
}

#[derive(Clone, Debug)]
pub(super) struct Segmented {
    pub allocations: Box<[Chunk]>,
    pub frames: Box<[Piece]>,
    pub dictionaries: Box<[Piece]>,
    /// Segment entries, block table, keys and footer, in their file order.
    pub tail: Box<[u8]>,
    pub frames_len: usize,
    pub dictionaries_len: usize,
}

impl Segmented {
    fn bytes(&self, piece: &Piece) -> &[u8] {
        &self.allocations[piece.allocation].bytes()[piece.range.clone()]
    }
}

/// Borrowed storage remains useful for validation and format tests; builders
/// and recovery fill chunks and retain only exact index/footer buffers beside them.
#[derive(Clone, Debug)]
pub(super) enum RunData<B> {
    #[cfg_attr(
        not(test),
        allow(dead_code, reason = "borrowed storage keeps format tests generic")
    )]
    Contiguous(B),
    Segmented(Segmented),
}
impl<B: AsRef<[u8]>> RunData<B> {
    pub fn key_data(&self) -> &[u8] {
        match self {
            Self::Contiguous(data) => data.as_ref(),
            Self::Segmented(data) => &data.tail,
        }
    }
    pub fn view(&self) -> Result<RunView<'_>, CorruptRun> {
        match self {
            Self::Contiguous(data) => RunView::open(data.as_ref()),
            Self::Segmented(data) => RunView::open_segmented(data),
        }
    }
    pub fn stored_len(&self) -> usize {
        match self {
            Self::Contiguous(data) => data.as_ref().len(),
            Self::Segmented(data) => data.frames_len + data.dictionaries_len + data.tail.len(),
        }
    }
    pub fn chunk_capacity(&self) -> usize {
        match self {
            Self::Contiguous(bytes) => bytes.as_ref().len(),
            Self::Segmented(data) => data
                .allocations
                .iter()
                .map(|chunk| chunk.bytes().len())
                .sum(),
        }
    }
    /// Metadata beside serialized bytes. Pool capacity, including padding and
    /// the free reserve, is reported separately by the engine's pool counters.
    pub fn held_bytes(&self) -> usize {
        match self {
            Self::Contiguous(_) => 0,
            Self::Segmented(data) => {
                size_of_val(&*data.allocations)
                    + size_of_val(&*data.frames)
                    + size_of_val(&*data.dictionaries)
            }
        }
    }
    /// File order: all frames, all dictionaries, then the index/footer.
    pub fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        let count = match self {
            Self::Contiguous(_) => 1,
            Self::Segmented(data) => data.frames.len() + data.dictionaries.len() + 1,
        };
        (0..count).map(move |at| match self {
            Self::Contiguous(data) => data.as_ref(),
            Self::Segmented(data) => {
                if at < data.frames.len() {
                    data.bytes(&data.frames[at])
                } else if let Some(piece) = data.dictionaries.get(at - data.frames.len()) {
                    data.bytes(piece)
                } else {
                    &data.tail
                }
            }
        })
    }
}

/// A logical region whose requested ranges must lie in a single chunk.
#[derive(Clone, Copy, Debug)]
pub(super) enum Region<'a> {
    Contiguous(&'a [u8]),
    Frames(&'a Segmented),
    Dictionaries(&'a Segmented),
}
impl<'a> Region<'a> {
    pub fn len(self) -> usize {
        match self {
            Self::Contiguous(bytes) => bytes.len(),
            Self::Frames(data) => data.frames_len,
            Self::Dictionaries(data) => data.dictionaries_len,
        }
    }
    pub fn is_empty(self) -> bool {
        self.len() == 0
    }
    pub fn get(self, range: Range<usize>) -> Option<&'a [u8]> {
        match self {
            Self::Contiguous(bytes) => bytes.get(range),
            Self::Frames(data) | Self::Dictionaries(data) => {
                let pieces = if matches!(self, Self::Frames(_)) {
                    &data.frames
                } else {
                    &data.dictionaries
                };
                let at = pieces
                    .partition_point(|p| p.start <= range.start)
                    .checked_sub(1)?;
                let piece = &pieces[at];
                data.bytes(piece).get(
                    range.start.checked_sub(piece.start)?..range.end.checked_sub(piece.start)?,
                )
            }
        }
    }
    pub fn stored_subslice(self, value: &[u8]) -> Option<&'a [u8]> {
        let within = |bytes: &'a [u8]| {
            let start = value.as_ptr().addr().checked_sub(bytes.as_ptr().addr())?;
            bytes.get(start..start.checked_add(value.len())?)
        };
        match self {
            Self::Contiguous(bytes) => within(bytes),
            Self::Frames(data) => data.frames.iter().find_map(|p| within(data.bytes(p))),
            Self::Dictionaries(data) => {
                data.dictionaries.iter().find_map(|p| within(data.bytes(p)))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_partial_tail_before_an_oversized_frame_is_exact_and_cloneable() {
        let pool = ChunkPool::new(CHUNK_BYTES);
        let mut packing = Packing::new(&pool);
        packing.frame(&[7; 13]);
        packing.frame(&vec![9; CHUNK_BYTES + 1]);
        let data = packing.finish(Box::new([]));
        assert_eq!(data.allocations[0].bytes(), &[7; 13]);
        assert_eq!(data.allocations[1].bytes().len(), CHUNK_BYTES + 1);
        assert_eq!(pool.stats(), (CHUNK_BYTES, 0, CHUNK_BYTES));
        let copy = data.clone();
        drop(data);
        assert_eq!(Region::Frames(&copy).get(0..13).unwrap(), &[7; 13]);
        assert_eq!(pool.stats(), (CHUNK_BYTES, 0, CHUNK_BYTES));
    }

    #[test]
    fn packing_shares_chunks_but_never_splits_a_frame_or_dictionary() {
        let pool = ChunkPool::new(CHUNK_BYTES);
        let mut packing = Packing::new(&pool);
        packing.frame(&vec![1; CHUNK_BYTES - 32]);
        packing.dictionary(&[2; 16]);
        // The next segment shares the first chunk's last sixteen bytes.
        packing.frame(&[3; 16]);
        packing.frame(&[4; 32]);
        packing.dictionary(&[5; 64]);
        let data = packing.finish(Box::new([]));
        assert_eq!(data.allocations.len(), 2);
        assert_eq!(data.allocations[0].bytes().len(), CHUNK_BYTES);
        assert_eq!(data.allocations[1].bytes().len(), 96);
        assert_eq!(pool.stats(), (CHUNK_BYTES, CHUNK_BYTES, CHUNK_BYTES));
        assert_eq!(data.frames[1].allocation, 0);
        assert_eq!(data.frames[2].allocation, 1);
        assert_eq!(
            Region::Frames(&data)
                .get(CHUNK_BYTES - 32..CHUNK_BYTES - 16)
                .unwrap(),
            &[3; 16]
        );
        assert!(
            Region::Frames(&data)
                .get(CHUNK_BYTES - 17..CHUNK_BYTES - 15)
                .is_none()
        );
        assert_eq!(Region::Dictionaries(&data).get(16..80).unwrap(), &[5; 64]);
        drop(data);
        assert_eq!(pool.stats().0, CHUNK_BYTES);
        assert_eq!(pool.stats().1, 0);
    }
}
