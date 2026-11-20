//! CURRENT and manifest encoding and decoding.

use super::*;

pub(super) fn encode_current(generation: u64) -> [u8; CURRENT_LEN] {
    let mut bytes = [0; CURRENT_LEN];
    bytes[..8].copy_from_slice(&CURRENT_MAGIC);
    bytes[8..16].copy_from_slice(&generation.to_le_bytes());
    let sum = crc(&[&bytes[..16]]);
    bytes[16..].copy_from_slice(&sum.to_le_bytes());
    bytes
}

pub(super) fn decode_current(bytes: &[u8]) -> Result<u64, Damage> {
    if bytes.len() != CURRENT_LEN {
        return Err(Damage::Length {
            expected: CURRENT_LEN as u64,
            found: bytes.len() as u64,
        });
    }
    if bytes[..8] != CURRENT_MAGIC {
        return Err(Damage::BadMagic);
    }
    if crc(&[&bytes[..16]]).to_le_bytes() != bytes[16..] {
        return Err(Damage::Checksum);
    }
    Ok(u64::from_le_bytes(bytes[8..16].try_into().unwrap()))
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) struct RunEntry {
    pub(super) id: u128,
    pub(super) level: usize,
    pub(super) flush: u64,
    pub(super) seq_override: Option<u64>,
    pub(super) len: u64,
    pub(super) run_len: u64,
    pub(super) sum: u32,
    pub(super) bounds: Option<(Vec<u8>, Vec<u8>)>,
}

#[derive(Debug, PartialEq, Eq)]
pub(super) struct Manifest {
    pub(super) generation: u64,
    pub(super) last_seq: u64,
    pub(super) records: u64,
    pub(super) next_run_id: u64,
    pub(super) previous: u64,
    pub(super) depth: usize,
    pub(super) metadata: Vec<u8>,
    pub(super) runs: Vec<RunEntry>,
}

impl Manifest {
    pub(super) fn encode(&self) -> Vec<u8> {
        let bounds_len: usize = self
            .runs
            .iter()
            .filter_map(|r| r.bounds.as_ref())
            .map(|(a, b)| a.len() + b.len())
            .sum();
        let mut bytes = Vec::with_capacity(
            MANIFEST_HEADER_LEN
                + self.runs.len() * MANIFEST_ENTRY_LEN
                + bounds_len
                + self.metadata.len()
                + 4,
        );
        bytes.extend_from_slice(&MANIFEST_MAGIC);
        bytes.extend_from_slice(&MANIFEST_VERSION.to_le_bytes());
        bytes.extend_from_slice(&0u32.to_le_bytes());
        for n in [
            self.generation,
            self.last_seq,
            self.records,
            self.next_run_id,
            self.previous,
        ] {
            bytes.extend_from_slice(&n.to_le_bytes());
        }
        bytes.extend_from_slice(&(self.depth as u32).to_le_bytes());
        bytes.extend_from_slice(&(self.metadata.len() as u64).to_le_bytes());
        bytes.extend_from_slice(&(self.runs.len() as u32).to_le_bytes());
        bytes.extend_from_slice(&(bounds_len as u64).to_le_bytes());
        for run in &self.runs {
            bytes.extend_from_slice(&run.id.to_le_bytes());
            bytes.extend_from_slice(&(run.level as u32).to_le_bytes());
            let flags =
                u32::from(run.seq_override.is_some()) | (u32::from(run.bounds.is_some()) << 1);
            bytes.extend_from_slice(&flags.to_le_bytes());
            bytes.extend_from_slice(&run.seq_override.unwrap_or(0).to_le_bytes());
            bytes.extend_from_slice(&run.flush.to_le_bytes());
            bytes.extend_from_slice(&run.len.to_le_bytes());
            bytes.extend_from_slice(&run.run_len.to_le_bytes());
            bytes.extend_from_slice(&run.sum.to_le_bytes());
            let (min, max) = run
                .bounds
                .as_ref()
                .map_or((0, 0), |(a, b)| (a.len(), b.len()));
            bytes.extend_from_slice(&(min as u32).to_le_bytes());
            bytes.extend_from_slice(&(max as u32).to_le_bytes());
        }
        for (min, max) in self.runs.iter().filter_map(|r| r.bounds.as_ref()) {
            bytes.extend_from_slice(min);
            bytes.extend_from_slice(max);
        }
        bytes.extend_from_slice(&self.metadata);
        let sum = crc(&[&bytes]);
        bytes.extend_from_slice(&sum.to_le_bytes());
        bytes
    }

    pub(super) fn decode(bytes: &[u8]) -> Result<Self, Damage> {
        let u32_at = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
        let u64_at = |at: usize| u64::from_le_bytes(bytes[at..at + 8].try_into().unwrap());
        let length = |expected| Damage::Length {
            expected,
            found: bytes.len() as u64,
        };
        if bytes.len() < MANIFEST_HEADER_LEN + 4 {
            return Err(length((MANIFEST_HEADER_LEN + 4) as u64));
        }
        if bytes[..8] != MANIFEST_MAGIC {
            return Err(Damage::BadMagic);
        }
        let (version, flags) = (u32_at(8), u32_at(12));
        if version != MANIFEST_VERSION || flags != 0 {
            return Err(Damage::Unsupported { version, flags });
        }
        let count = u32_at(68) as usize;
        let metadata_len = u64_at(60);
        let bounds_len = u64_at(72);
        if bounds_len > count as u64 * 2 * crate::block::MAX_KEY_LEN as u64 {
            return Err(Damage::Order);
        }
        if metadata_len > MAX_METADATA_BYTES as u64 {
            return Err(length(
                (MANIFEST_HEADER_LEN + MAX_METADATA_BYTES + 4) as u64,
            ));
        }
        let expected = MANIFEST_HEADER_LEN as u64
            + count as u64 * MANIFEST_ENTRY_LEN as u64
            + metadata_len
            + bounds_len
            + 4;
        if bytes.len() as u64 != expected {
            return Err(length(expected));
        }
        let body = bytes.len() - 4;
        if crc(&[&bytes[..body]]).to_le_bytes() != bytes[body..] {
            return Err(Damage::Checksum);
        }
        let depth = u32_at(56) as usize;
        let next_run_id = u64_at(40);
        if depth == 0
            || depth > count.saturating_add(64)
            || next_run_id == 0
            || u64_at(48) >= u64_at(16)
        {
            return Err(Damage::Order);
        }
        let mut runs: Vec<RunEntry> = Vec::with_capacity(count);
        let mut ids: HashMap<u128, Vec<usize>> = HashMap::with_capacity(count);
        let mut previous = (0, 0);
        let mut bounds_at = MANIFEST_HEADER_LEN + count * MANIFEST_ENTRY_LEN;
        let bounds_end = bounds_at + bounds_len as usize;
        for index in 0..count {
            let at = MANIFEST_HEADER_LEN + index * MANIFEST_ENTRY_LEN;
            let id = u128::from_le_bytes(bytes[at..at + 16].try_into().unwrap());
            let level = u32_at(at + 16) as usize;
            let flags = u32_at(at + 20);
            let seq = u64_at(at + 24);
            let flush = u64_at(at + 32);
            let len = u64_at(at + 40);
            let run_len = u64_at(at + 48);
            let (min_len, max_len) = (u32_at(at + 60) as usize, u32_at(at + 64) as usize);
            if min_len > crate::block::MAX_KEY_LEN
                || max_len > crate::block::MAX_KEY_LEN
                || min_len + max_len > bounds_end - bounds_at
                || (flags & 2 == 0 && min_len + max_len != 0)
            {
                return Err(Damage::Order);
            }
            let bounds = if flags & 2 != 0 {
                let min = bytes[bounds_at..bounds_at + min_len].to_vec();
                bounds_at += min_len;
                let max = bytes[bounds_at..bounds_at + max_len].to_vec();
                bounds_at += max_len;
                if min > max {
                    return Err(Damage::Order);
                }
                Some((min, max))
            } else {
                None
            };
            if run_len > u32::MAX as u64 {
                return Err(Damage::Length {
                    expected: u32::MAX as u64,
                    found: run_len,
                });
            }
            let max_len = run_len + crate::run::MAX_FILTER_BYTES as u64 + 4;
            if len > max_len {
                return Err(Damage::Length {
                    expected: max_len,
                    found: len,
                });
            }
            if id as u64 == 0
                || id as u64 >= next_run_id
                || level > depth
                || flags > 3
                || (flags & 1 == 0 && seq != 0)
                || (level > 0 && flush != 0)
                || (level, flush) < previous
                || flush == u64::MAX
                || run_len > len.saturating_sub(4)
                || len < 4
            {
                return Err(Damage::Order);
            }
            if let Some(previous_views) = ids.get(&id) {
                for &index in previous_views {
                    let previous = &runs[index];
                    if previous.len != len
                        || previous.run_len != run_len
                        || previous.sum != u32_at(at + 56)
                        || previous.seq_override != (flags & 1 != 0).then_some(seq)
                    {
                        return Err(Damage::Order);
                    }
                    match (&previous.bounds, &bounds) {
                        (Some((a, b)), Some((c, d))) if b < c || d < a => {}
                        _ => return Err(Damage::Order),
                    }
                }
            }
            ids.entry(id).or_default().push(runs.len());
            previous = (level, flush);
            runs.push(RunEntry {
                id,
                level,
                flush,
                seq_override: (flags & 1 != 0).then_some(seq),
                len,
                run_len,
                sum: u32_at(at + 56),
                bounds,
            });
        }
        if bounds_at != bounds_end {
            return Err(Damage::Order);
        }
        let metadata_at = bounds_end;
        Ok(Self {
            generation: u64_at(16),
            last_seq: u64_at(24),
            records: u64_at(32),
            next_run_id,
            previous: u64_at(48),
            depth,
            metadata: bytes[metadata_at..body].to_vec(),
            runs,
        })
    }
}
