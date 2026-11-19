//! Which blocks of a run are settled: every record has sequence 0 and a
//! value. A settled block can be copied forward by a merge into the bottom
//! level, and its record count lets the copy keep the output's count exact
//! without decoding it. Held beside the run's bytes, not part of the format.

/// Records per block, 0 for a block that is not settled. One byte per block
/// when every settled count fits, two otherwise.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum SettledCounts {
    Narrow(Box<[u8]>),
    Wide(Box<[u16]>),
}

impl SettledCounts {
    /// Counts from one entry per block, or `None` if no block is settled.
    /// A count too large for two bytes marks its block as not settled.
    pub(super) fn from_blocks(counts: &[u32]) -> Option<Self> {
        let max = counts
            .iter()
            .copied()
            .filter(|&n| n <= u32::from(u16::MAX))
            .max()?;
        if max == 0 {
            return None;
        }
        Some(if max <= u32::from(u8::MAX) {
            Self::Narrow(
                counts
                    .iter()
                    .map(|&n| u8::try_from(n).unwrap_or(0))
                    .collect(),
            )
        } else {
            Self::Wide(
                counts
                    .iter()
                    .map(|&n| u16::try_from(n).unwrap_or(0))
                    .collect(),
            )
        })
    }

    /// The records of `block` if it is settled, or 0.
    pub(super) fn records(&self, block: usize) -> usize {
        match self {
            Self::Narrow(counts) => counts.get(block).map_or(0, |&n| n.into()),
            Self::Wide(counts) => counts.get(block).map_or(0, |&n| n.into()),
        }
    }

    pub(super) fn held_bytes(&self) -> usize {
        match self {
            Self::Narrow(counts) => size_of_val(&**counts),
            Self::Wide(counts) => size_of_val(&**counts),
        }
    }
}
