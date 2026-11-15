//! Physical storage behind the unchanged logical run format.
use std::ops::Range;

use super::CorruptRun;
use super::reader::RunView;

/// One exact allocation: encoded frames followed by their raw dictionary.
#[derive(Clone, Debug)]
pub(super) struct Segment {
    pub frame_start: usize,
    pub dictionary_start: usize,
    pub frames_len: usize,
    pub bytes: Box<[u8]>,
}
impl Segment {
    pub fn frames(&self) -> &[u8] {
        &self.bytes[..self.frames_len]
    }
    pub fn dictionary(&self) -> &[u8] {
        &self.bytes[self.frames_len..]
    }
}

#[derive(Clone, Debug)]
pub(super) struct Segmented {
    pub segments: Box<[Segment]>,
    /// Segment entries, block table, keys and footer, in their file order.
    pub tail: Box<[u8]>,
    pub frames_len: usize,
    pub dictionaries_len: usize,
}

/// Borrowed storage remains useful for validation and format tests; builders
/// and recovery use Segmented and never retain a contiguous run buffer.
#[derive(Clone, Debug)]
pub(super) enum RunData<B> {
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
    pub fn held_bytes(&self) -> usize {
        match self {
            Self::Contiguous(_) => 0,
            Self::Segmented(data) => size_of_val(&*data.segments),
        }
    }
    /// The v5 file layout: all frames, all dictionaries, then the index/footer.
    pub fn chunks(&self) -> impl Iterator<Item = &[u8]> {
        let count = match self {
            Self::Contiguous(_) => 1,
            Self::Segmented(data) => 2 * data.segments.len() + 1,
        };
        (0..count).map(move |at| match self {
            Self::Contiguous(data) => data.as_ref(),
            Self::Segmented(data) => {
                let n = data.segments.len();
                if at < n {
                    data.segments[at].frames()
                } else if at < 2 * n {
                    data.segments[at - n].dictionary()
                } else {
                    &data.tail
                }
            }
        })
    }
}

/// A logical region whose ranges must lie in a single physical segment.
/// Index entries already delimit frames, so readers need only this lookup.
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
                let frames = matches!(self, Self::Frames(_));
                let start = |s: &Segment| {
                    if frames {
                        s.frame_start
                    } else {
                        s.dictionary_start
                    }
                };
                let at = data
                    .segments
                    .partition_point(|s| start(s) <= range.start)
                    .checked_sub(1)?;
                let segment = &data.segments[at];
                let bytes = if frames {
                    segment.frames()
                } else {
                    segment.dictionary()
                };
                bytes.get(
                    range.start.checked_sub(start(segment))?
                        ..range.end.checked_sub(start(segment))?,
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
            Self::Frames(data) => data.segments.iter().find_map(|s| within(s.frames())),
            Self::Dictionaries(data) => data.segments.iter().find_map(|s| within(s.dictionary())),
        }
    }
}
