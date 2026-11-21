//! Diagnostics only: whether each bottom input block of a level-0 → bottom
//! merge could be copied verbatim into the output. It observes the merge
//! stream and the inputs' indexes and changes nothing the merge does.
//!
//! A block is copyable when no record of another input falls between its
//! first and last keys (so no other input's next key is at or below its last
//! key when the merge reaches it), every record it yields has sequence 0, it
//! holds no tombstone, and it does not straddle the edge of a builder's range.
use std::cell::RefCell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering::Relaxed};

use super::reader::RunView;

static ENABLED: AtomicBool = AtomicBool::new(false);
static SEGMENTS: Mutex<Vec<SegmentCopy>> = Mutex::new(Vec::new());
/// Raw bytes of the other inputs' blocks the audited merges decoded.
static OTHER_RAW: AtomicU64 = AtomicU64::new(0);

thread_local! {
    /// Which inputs of the next merge on this thread are bottom-level runs.
    static BOTTOM: RefCell<Option<Vec<bool>>> = const { RefCell::new(None) };
}

/// Turns the audit on for this process.
pub(crate) fn enable() {
    ENABLED.store(true, Relaxed);
}
pub(crate) fn enabled() -> bool {
    ENABLED.load(Relaxed)
}
pub(crate) fn set_bottom_inputs(mask: Option<Vec<bool>>) {
    BOTTOM.with(|bottom| *bottom.borrow_mut() = mask);
}
pub(crate) fn bottom_inputs() -> Option<Vec<bool>> {
    BOTTOM.with(|bottom| bottom.borrow().clone())
}
/// Drains the segments audited so far.
pub(crate) fn take() -> (Vec<SegmentCopy>, u64) {
    (
        std::mem::take(&mut *SEGMENTS.lock().unwrap_or_else(|e| e.into_inner())),
        OTHER_RAW.swap(0, Relaxed),
    )
}

/// The blocks of one bottom input segment that one merge read.
#[derive(Clone, Copy, Debug, Default)]
pub struct SegmentCopy {
    /// Blocks read.
    pub blocks: u64,
    /// Their raw bytes.
    pub raw: u64,
    /// Their stored frame bytes, including headers.
    pub stored: u64,
    /// Copyable blocks.
    pub copyable_blocks: u64,
    /// Their raw bytes.
    pub copyable_raw: u64,
    /// Their stored frame bytes.
    pub copyable_stored: u64,
    /// Blocks that fail only because a record keeps a nonzero sequence.
    pub only_seq: u64,
    /// Blocks that fail only because they hold a tombstone.
    pub only_tombstone: u64,
    /// Blocks that fail only on both of those.
    pub seq_and_tombstone: u64,
    /// Blocks holding a key range another input's record falls into.
    pub touched: u64,
    /// Blocks straddling a builder range edge.
    pub edge: u64,
}

#[derive(Default)]
struct Flags {
    touched: bool,
    tombstone: bool,
    seq: bool,
    edge: bool,
}

struct Bottom<'a> {
    view: RunView<'a>,
    block: usize,
    entered: bool,
    /// Last key of the entered block.
    last: Vec<u8>,
    flags: Flags,
    raw: u64,
    stored: u64,
    dictionary_ref: u32,
    segment: SegmentCopy,
    any_block: bool,
}

impl Bottom<'_> {
    fn close(&mut self) {
        let f = std::mem::take(&mut self.flags);
        let s = &mut self.segment;
        s.blocks += 1;
        s.raw += self.raw;
        s.stored += self.stored;
        let clean = !f.touched && !f.edge;
        match (clean, f.seq, f.tombstone) {
            (true, false, false) => {
                s.copyable_blocks += 1;
                s.copyable_raw += self.raw;
                s.copyable_stored += self.stored;
            }
            (true, true, false) => s.only_seq += 1,
            (true, false, true) => s.only_tombstone += 1,
            (true, true, true) => s.seq_and_tombstone += 1,
            _ => {}
        }
        s.touched += u64::from(f.touched);
        s.edge += u64::from(f.edge);
        self.entered = false;
    }

    fn flush_segment(&mut self, out: &mut Vec<SegmentCopy>) {
        if self.segment.blocks > 0 {
            out.push(std::mem::take(&mut self.segment));
        }
    }
}

/// Follows one merge's records.
pub(crate) struct CopyAudit<'a> {
    inputs: Vec<Option<Bottom<'a>>>,
    /// Each other input's view and the last key of its decoded block.
    others: Vec<(RunView<'a>, Option<Vec<u8>>)>,
    other_raw: u64,
    last_other: Option<Vec<u8>>,
    seeked: bool,
    done: Vec<SegmentCopy>,
}

impl<'a> CopyAudit<'a> {
    /// An audit if the audit is on and this thread was told which inputs
    /// are bottom runs.
    pub(crate) fn new(inputs: &[RunView<'a>], start: Option<&[u8]>) -> Option<Self> {
        if !enabled() {
            return None;
        }
        let mask = bottom_inputs()?;
        if mask.len() != inputs.len() || !mask.iter().any(|&b| b) || mask.iter().all(|&b| b) {
            return None;
        }
        Some(Self {
            inputs: inputs
                .iter()
                .zip(mask)
                .map(|(&view, bottom)| {
                    bottom.then(|| Bottom {
                        view,
                        block: 0,
                        entered: false,
                        last: Vec::new(),
                        flags: Flags::default(),
                        raw: 0,
                        stored: 0,
                        dictionary_ref: u32::MAX,
                        segment: SegmentCopy::default(),
                        any_block: false,
                    })
                })
                .collect(),
            others: inputs.iter().map(|&view| (view, None)).collect(),
            other_raw: 0,
            last_other: None,
            seeked: start.is_some(),
            done: Vec::new(),
        })
    }

    /// One record yielded by the merge, from input `input`.
    pub(crate) fn record(&mut self, input: usize, key: &[u8], seq: u64, tombstone: bool) {
        let seeked = self.seeked;
        match &mut self.inputs[input] {
            Some(b) => {
                if b.entered && b.last.as_slice() < key {
                    b.close();
                }
                if !b.entered {
                    let Ok(Some(block)) = b.view.locate(key) else {
                        return;
                    };
                    let (Ok(last), Ok(frame)) = (b.view.last_key(block), b.view.frame(block))
                    else {
                        return;
                    };
                    if frame.dictionary_ref != b.dictionary_ref {
                        b.flush_segment(&mut self.done);
                        b.dictionary_ref = frame.dictionary_ref;
                    }
                    b.block = block;
                    b.entered = true;
                    b.last = last.to_vec();
                    b.raw = frame.decoded_len as u64;
                    b.stored = b.view.block_bytes(block) as u64;
                    // A newer version of this key from another input came just before.
                    b.flags.touched = self.last_other.as_deref() == Some(key);
                    // The first block a seeked builder enters may begin before its range.
                    b.flags.edge = seeked && !b.any_block && block > 0;
                    b.any_block = true;
                }
                b.flags.tombstone |= tombstone;
                b.flags.seq |= seq != 0;
            }
            None => {
                let (view, last) = &mut self.others[input];
                if last.as_deref().is_none_or(|last| last < key)
                    && let Ok(Some(block)) = view.locate(key)
                    && let (Ok(end), Ok(frame)) = (view.last_key(block), view.frame(block))
                {
                    self.other_raw += frame.decoded_len as u64;
                    *last = Some(end.to_vec());
                }
                for b in self.inputs.iter_mut().flatten() {
                    if b.entered && key <= b.last.as_slice() {
                        b.flags.touched = true;
                    }
                }
                let other = self.last_other.get_or_insert_with(Vec::new);
                other.clear();
                other.extend_from_slice(key);
            }
        }
    }

    /// Ends the audit. Blocks reaching `end` straddle the builder's range.
    pub(crate) fn finish(mut self, end: Option<&[u8]>) {
        for b in self.inputs.iter_mut().flatten() {
            if b.entered {
                if end.is_some_and(|end| b.last.as_slice() >= end) {
                    b.flags.edge = true;
                }
                b.close();
            }
            b.flush_segment(&mut self.done);
        }
        OTHER_RAW.fetch_add(self.other_raw, Relaxed);
        SEGMENTS
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .append(&mut self.done);
    }
}
