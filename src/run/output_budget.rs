//! Reservation-backed limits on unpublished output. Builders charge compressed
//! frames and conservative index/ownership metadata before copying them to runs.
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering::Relaxed};

#[derive(Debug, Default)]
pub(crate) struct OutputCounters {
    current: AtomicUsize,
    peak: AtomicUsize,
}
impl OutputCounters {
    #[cfg(feature = "diagnostics")]
    pub(crate) fn snapshot(&self) -> (usize, usize) {
        (self.current.load(Relaxed), self.peak.load(Relaxed))
    }
}

#[derive(Debug)]
pub(crate) struct OutputBudget {
    counters: Arc<OutputCounters>,
    limit: usize,
    used: AtomicUsize,
    attempted: AtomicUsize,
}
impl OutputBudget {
    pub(crate) fn new(counters: &Arc<OutputCounters>, limit: usize) -> Arc<Self> {
        Arc::new(Self {
            counters: counters.clone(),
            limit,
            used: AtomicUsize::new(0),
            attempted: AtomicUsize::new(0),
        })
    }
    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
    pub(super) fn reserve(&self, bytes: usize) -> bool {
        // One builder owns a piece, so a load/store suffices for its local sum.
        let next = self.used.load(Relaxed).saturating_add(bytes);
        self.attempted.store(next, Relaxed);
        if next > self.limit {
            return false;
        }
        self.used.store(next, Relaxed);
        let current = self
            .counters
            .current
            .fetch_add(bytes, Relaxed)
            .saturating_add(bytes);
        self.counters.peak.fetch_max(current, Relaxed);
        true
    }
    pub(crate) fn refused_bytes(&self) -> usize {
        self.attempted.load(Relaxed)
    }
}
impl Drop for OutputBudget {
    fn drop(&mut self) {
        self.counters
            .current
            .fetch_sub(self.used.load(Relaxed), Relaxed);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::block::Value;
    use crate::run::{Codec, RunBuilder};

    #[test]
    fn bounded_builders_preserve_bytes_and_refuse_before_exceeding_output() {
        for codec in [Codec::None, Codec::Zstd] {
            let counters = Arc::new(OutputCounters::default());
            let budget = OutputBudget::new(&counters, 1 << 20);
            let mut plain = RunBuilder::new(codec);
            let mut bounded = RunBuilder::new(codec);
            plain.limit_filter_build_bytes(0);
            bounded.limit_filter_build_bytes(0);
            bounded.limit_output(budget.clone());
            for key in 0..1000u32 {
                plain
                    .add(&key.to_be_bytes(), 0, Value::Bytes(&[7; 128]))
                    .unwrap();
                bounded
                    .add(&key.to_be_bytes(), 0, Value::Bytes(&[7; 128]))
                    .unwrap();
            }
            let plain = plain.finish();
            let bounded = bounded.finish_checked().unwrap();
            assert_eq!(plain.as_bytes(), bounded.as_bytes());
            assert!(bounded.stored_len() + bounded.held_bytes() <= budget.used.load(Relaxed));
            assert!(counters.peak.load(Relaxed) <= budget.limit());
            drop(bounded);
            drop(budget);
            assert_eq!(counters.current.load(Relaxed), 0);
            let budget = OutputBudget::new(&counters, 256);
            let mut builder = RunBuilder::new(codec);
            builder.limit_filter_build_bytes(0);
            builder.limit_output(budget.clone());
            builder.add(b"key", 1, Value::Bytes(&[7; 8192])).unwrap();
            assert!(builder.finish_checked().is_err());
            assert!(budget.used.load(Relaxed) <= budget.limit());
            drop(budget);
            assert_eq!(counters.current.load(Relaxed), 0);
        }
    }

    #[test]
    fn output_budget_refusal_during_probe_stops_cleanly() {
        let counters = Arc::new(OutputCounters::default());
        // Tight enough to allow initial key reservation and one index entry,
        // then fail inside probe() while re-cutting the 64 KiB sample.
        let budget = OutputBudget::new(&counters, 256);
        let mut builder = RunBuilder::new(Codec::Zstd);
        builder.limit_filter_build_bytes(0);
        builder.limit_output(budget.clone());
        let value = vec![b'a'; 256];
        for key in 0..400u32 {
            builder
                .add(&key.to_be_bytes(), 0, Value::Bytes(&value))
                .unwrap();
        }
        assert!(builder.finish_checked().is_err());
        assert!(budget.used.load(Relaxed) <= budget.limit());
        drop(budget);
        assert_eq!(counters.current.load(Relaxed), 0);
    }
}
