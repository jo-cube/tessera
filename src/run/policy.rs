//! Compression effort and dictionary length, chosen by where output goes.
//! The decoder depends on neither, so runs built under any policy recover.

use super::codec::ZSTD_LEVEL;
use super::dictionary::{DICTIONARY_LEN, Length};

const NON_BOTTOM_LEVEL: i32 = ZSTD_LEVEL;
const BOTTOM_LEVEL: i32 = ZSTD_LEVEL;
const DICTIONARY: Length = Length::Fixed(DICTIONARY_LEN);

/// The Zstandard level for output into the bottom level or above it.
#[cfg(not(feature = "diagnostics"))]
pub(super) fn level(bottom: bool) -> i32 {
    if bottom {
        BOTTOM_LEVEL
    } else {
        NON_BOTTOM_LEVEL
    }
}

/// How long each segment's dictionary is.
#[cfg(not(feature = "diagnostics"))]
pub(super) fn dictionary() -> Length {
    DICTIONARY
}

#[cfg(feature = "diagnostics")]
mod overrides {
    use super::*;
    use std::sync::atomic::{AtomicI32, AtomicUsize, Ordering::Relaxed};

    static LEVELS: [AtomicI32; 2] = [
        AtomicI32::new(NON_BOTTOM_LEVEL),
        AtomicI32::new(BOTTOM_LEVEL),
    ];
    /// Zero selects [`Length::Adaptive`].
    static DICTIONARY_BYTES: AtomicUsize = AtomicUsize::new(match DICTIONARY {
        Length::Fixed(n) => n,
        Length::Adaptive => 0,
    });

    pub(in super::super) fn level(bottom: bool) -> i32 {
        LEVELS[usize::from(bottom)].load(Relaxed)
    }
    pub(in super::super) fn dictionary() -> Length {
        match DICTIONARY_BYTES.load(Relaxed) {
            0 => Length::Adaptive,
            n => Length::Fixed(n),
        }
    }
    /// Process-wide experiment policy: levels above and at the bottom, and
    /// a fixed dictionary length, or adaptive if `None`.
    pub(crate) fn set(non_bottom: i32, bottom: i32, dictionary: Option<usize>) {
        LEVELS[0].store(non_bottom, Relaxed);
        LEVELS[1].store(bottom, Relaxed);
        DICTIONARY_BYTES.store(dictionary.unwrap_or(0), Relaxed);
    }
}
#[cfg(feature = "diagnostics")]
pub(crate) use overrides::set;
#[cfg(feature = "diagnostics")]
pub(super) use overrides::{dictionary, level};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::run::{Codec, RunBuilder};

    #[test]
    fn every_output_uses_level_one_and_the_standard_dictionary() {
        assert_eq!(level(false), ZSTD_LEVEL);
        assert_eq!(level(true), ZSTD_LEVEL);
        assert_eq!(dictionary(), Length::Fixed(DICTIONARY_LEN));
        let mut builder = RunBuilder::new(Codec::Zstd);
        assert_eq!(builder.level(), level(false));
        builder.set_bottom(true);
        assert_eq!(builder.level(), level(true));
    }
}
