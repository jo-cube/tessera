//! Allocation-free reconstruction of a prefix-compressed index key.
use crate::block::{MAX_KEY_LEN, RESTART_INTERVAL};

/// Order-preserving head; equal heads still require a full bytewise comparison
/// (including short keys that differ only by trailing zero bytes).
pub(super) fn head(key: &[u8]) -> u64 {
    let mut bytes = [0; 8];
    let len = key.len().min(bytes.len());
    bytes[..len].copy_from_slice(&key[..len]);
    u64::from_be_bytes(bytes)
}

/// At most one borrowed suffix per entry since the last restart. Keeping the
/// pieces avoids a maximum-key-sized buffer or an allocation on point reads.
#[derive(Clone, Copy, Debug)]
pub(super) struct IndexKey<'a> {
    pieces: [&'a [u8]; RESTART_INTERVAL],
    count: usize,
    len: usize,
}
impl<'a> IndexKey<'a> {
    pub fn new() -> Self {
        Self {
            pieces: [&[]; RESTART_INTERVAL],
            count: 0,
            len: 0,
        }
    }
    pub fn append(&mut self, shared: usize, suffix: &'a [u8]) -> Option<()> {
        let len = shared.checked_add(suffix.len())?;
        if shared > self.len || len > MAX_KEY_LEN {
            return None;
        }
        let mut left = shared;
        let mut kept = 0;
        for at in 0..self.count {
            let n = left.min(self.pieces[at].len());
            if n == 0 {
                break;
            }
            self.pieces[kept] = &self.pieces[at][..n];
            kept += 1;
            left -= n;
        }
        if !suffix.is_empty() {
            *self.pieces.get_mut(kept)? = suffix;
            kept += 1;
        }
        self.count = kept;
        self.len = len;
        Some(())
    }
    pub fn len(self) -> usize {
        self.len
    }
    fn bytes(&self) -> impl Iterator<Item = u8> + '_ {
        self.pieces[..self.count]
            .iter()
            .flat_map(|piece| piece.iter().copied())
    }
    pub fn cmp(&self, other: &[u8]) -> std::cmp::Ordering {
        self.bytes().cmp(other.iter().copied())
    }
    pub fn to_vec(self) -> Vec<u8> {
        let mut key = Vec::with_capacity(self.len);
        for piece in &self.pieces[..self.count] {
            key.extend_from_slice(piece);
        }
        key
    }
}
