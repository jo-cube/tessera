//! Byte boundaries of Zstandard blocks and entropy descriptions (RFC 8878).
//! Tessera frames omit the magic, content size, dictionary ID and checksum.

pub enum Block<'a> {
    Raw(usize),
    Rle,
    Compressed(Sections<'a>),
}

pub struct Sections<'a> {
    pub literals: usize,
    pub literal_mode: usize,
    pub huffman: &'a [u8],
    pub sequences: usize,
    // Literal lengths, offsets, match lengths, in frame order.
    pub fse: Option<[Table<'a>; 3]>,
}

#[derive(Clone, Copy)]
pub struct Table<'a> {
    pub mode: usize,
    pub description: &'a [u8],
}

pub fn walk(frame: &[u8], mut visit: impl FnMut(Block<'_>)) -> Option<()> {
    if *frame.first()? != 0 {
        return None;
    }
    let mut rest = frame.get(2..)?;
    loop {
        let (h, body) = rest.split_first_chunk::<3>()?;
        let header = u32::from_le_bytes([h[0], h[1], h[2], 0]);
        let len = (header >> 3) as usize;
        let kind = (header >> 1) & 3;
        let stored = if kind == 1 { 1 } else { len };
        let payload = body.get(..stored)?;
        visit(match kind {
            0 => Block::Raw(stored),
            1 => Block::Rle,
            2 => Block::Compressed(sections(payload)?),
            _ => return None,
        });
        rest = &body[stored..];
        if header & 1 != 0 {
            return rest.is_empty().then_some(());
        }
    }
}

fn sections(block: &[u8]) -> Option<Sections<'_>> {
    let first = *block.first()?;
    let literal_mode = (first & 3) as usize;
    let size_format = (first >> 2) & 3;
    let (header_len, stored) = if literal_mode < 2 {
        let (len, shift) = match size_format {
            0 | 2 => (1, 3),
            1 => (2, 4),
            _ => (3, 4),
        };
        let regenerated = le(block.get(..len)?) >> shift;
        (len, if literal_mode == 1 { 1 } else { regenerated })
    } else {
        let (len, bits) = match size_format {
            0 | 1 => (3, 10),
            2 => (4, 14),
            _ => (5, 18),
        };
        (len, le(block.get(..len)?) >> (4 + bits))
    };
    let literals = header_len + stored;
    let payload = block.get(header_len..literals)?;
    let huffman = if literal_mode == 2 {
        let first = *payload.first()? as usize;
        let following = if first < 128 {
            first
        } else {
            (first - 127).div_ceil(2)
        };
        payload.get(..=following)?
    } else {
        &[]
    };
    let seq = block.get(literals..)?;
    let fse = sequence_tables(seq)?;
    Some(Sections {
        literals,
        literal_mode,
        huffman,
        sequences: seq.len(),
        fse,
    })
}

fn le(bytes: &[u8]) -> usize {
    let mut word = [0; 8];
    word[..bytes.len()].copy_from_slice(bytes);
    u64::from_le_bytes(word) as usize
}

fn sequence_tables(mut seq: &[u8]) -> Option<Option<[Table<'_>; 3]>> {
    let count_len = match *seq.first()? {
        0 => return (seq.len() == 1).then_some(None),
        1..=127 => 1,
        128..=254 => 2,
        255 => 3,
    };
    let modes = *seq.get(count_len)?;
    if modes & 3 != 0 {
        return None;
    }
    seq = &seq[count_len + 1..];
    let mut next = |shift, max_symbol, max_log| {
        let mode = usize::from((modes >> shift) & 3u8);
        let len = match mode {
            1 => 1,
            2 => fse_len(seq, max_symbol, max_log)?,
            _ => 0,
        };
        let (description, rest) = seq.split_at_checked(len)?;
        seq = rest;
        Some(Table { mode, description })
    };
    let tables = [next(6, 35, 9)?, next(4, 31, 8)?, next(2, 52, 9)?];
    (!seq.is_empty()).then_some(Some(tables))
}

struct Bits<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl Bits<'_> {
    fn read(&mut self, count: u32) -> Option<usize> {
        let mut value = 0;
        for bit in 0..count {
            let byte = *self.bytes.get(self.pos / 8)?;
            value |= usize::from((byte >> (self.pos % 8)) & 1) << bit;
            self.pos += 1;
        }
        Some(value)
    }
}

/// Read the forward, little-endian FSE probability header up to its byte
/// boundary. A probability of -1 consumes one slot, like a probability of 1.
pub fn fse_len(bytes: &[u8], max_symbol: usize, max_log: usize) -> Option<usize> {
    let mut bits = Bits { bytes, pos: 0 };
    let log = bits.read(4)? + 5;
    if log > max_log {
        return None;
    }
    let mut remaining = (1usize << log) + 1;
    let mut symbols = 0;
    let mut used = 0;
    while remaining > 1 {
        if symbols > max_symbol {
            return None;
        }
        let width = remaining.ilog2();
        let threshold = 1 << width;
        let cutoff = 2 * threshold - 1 - remaining;
        let low = bits.read(width)?;
        let count = if low < cutoff {
            low
        } else {
            let full = low + (bits.read(1)? << width);
            if full >= threshold {
                full - cutoff
            } else {
                full
            }
        };
        remaining = remaining.checked_sub(count.abs_diff(1))?;
        symbols += 1;
        if count == 1 {
            loop {
                let zeros = bits.read(2)?;
                symbols += zeros;
                if zeros != 3 {
                    break;
                }
            }
        } else {
            used += 1;
        }
    }
    (remaining == 1 && used >= 2).then_some(bits.pos.div_ceil(8))
}
