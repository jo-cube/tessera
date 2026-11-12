//! Per-run Zstandard dictionaries: training one for a rewrite from samples of
//! the runs being rewritten, and the prepared forms that build and read runs.
//!
//! Every run compressed with a dictionary stores its own copy (see the
//! [format](super#format)). The runs one rewrite builds share a single
//! prepared decoding form, freed with the last of them; a recovered run
//! prepares its own. Nothing is registered or shared beyond that.

use std::fmt;
use std::num::NonZeroU32;
use std::sync::Arc;
use std::time::{Duration, Instant};

use zstd_safe::{CCtx, CDict, DDict};

use super::CorruptRun;
use super::codec::ZSTD_LEVEL;
use super::compact::CONTEXT_ALLOWANCE;
use super::reader::{ReadWorkspace, RunView};

/// Bytes of dictionary trained for a run.
pub const DICTIONARY_LEN: usize = 16 << 10;

/// The longest dictionary a run may store. Validation rejects a longer one
/// before allocating anything for it.
pub const MAX_DICTIONARY_LEN: usize = 64 << 10;

/// Decoded input sampled for training, at most: 100 times the dictionary, the
/// ratio measured in `docs/benchmarks.md` (5AD).
pub const SAMPLE_BUDGET: usize = 100 * DICTIONARY_LEN;

/// The least decoded input a rewrite must hold for a dictionary to be trained:
/// enough that the samples are at most a quarter of it.
pub const MIN_DICTIONARY_INPUT: usize = 4 * SAMPLE_BUDGET;

/// One sampled block in this many is held out of training, to estimate the
/// saving on blocks the dictionary has not seen.
const HELD_OUT_EVERY: usize = 8;

/// The least estimated saving, in percent of the plain compressed size, for
/// which a dictionary is used.
pub const MIN_SAVING_PERCENT: u32 = 10;

/// Bytes allowed for Zstandard's trainer (`ZDICT_trainFromBuffer`, fastCover
/// with its defaults), which allocates outside the Rust heap. In zstd 1.5.7
/// it holds a 4 MiB frequency table, and for each parameter trial in turn a
/// 4 MiB copy of it, a 2 MiB segment table, a dictionary buffer and the
/// context and dictionary used to score the trial: about 12 MiB. Not
/// measurable through the safe API, so this is a conservative bound.
pub const TRAINER_ALLOWANCE: usize = 16 << 20;

/// Bytes a rewrite needs beside its merge to train and use a dictionary: the
/// samples (two buffers, training and held out, each within the budget), the
/// trainer, a compression context for the estimate and the prepared
/// compression dictionary, which lives until the last output run is
/// finished.
pub const DICTIONARY_WORKSPACE: usize =
    2 * SAMPLE_BUDGET + TRAINER_ALLOWANCE + 2 * CONTEXT_ALLOWANCE;

/// The magic number that starts a Zstandard dictionary.
const ZSTD_DICTIONARY_MAGIC: [u8; 4] = 0xEC30_A437_u32.to_le_bytes();

/// Whether a rewrite's output was compressed with a trained dictionary, and if
/// not, why. Decided once, before any output is built.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DictionaryChoice {
    /// Dictionaries are not enabled, or the codec is not Zstandard.
    #[default]
    Off,
    /// The input decodes to less than 6.4 MiB; nothing was
    /// sampled.
    InputTooSmall,
    /// The workspace limit leaves less than about 21 MiB for training beside
    /// the merge; nothing was sampled.
    WorkspaceLimit,
    /// Zstandard could not train a usable dictionary from the samples.
    TrainingFailed,
    /// The held-out estimate saved less than 10%.
    NotBeneficial {
        /// The estimated saving against plain Zstandard at the same block
        /// length, in percent, negative if the dictionary cost bytes.
        saving_percent: i32,
    },
    /// Every output run was compressed with the dictionary.
    Used {
        /// The estimated saving against plain Zstandard at the same block
        /// length, in percent.
        saving_percent: i32,
    },
}

/// A run's dictionary, prepared for decoding: a copy of its bytes digested
/// once, read by every reader of the run at the same time.
pub(crate) struct RunDictionary {
    ddict: DDict<'static>,
    id: NonZeroU32,
}

impl RunDictionary {
    /// Prepares `bytes`, which must be a Zstandard dictionary with an ID.
    /// `None` means they are not one. Allocates about the dictionary's length
    /// again plus fixed tables (27 KiB measured).
    pub(super) fn prepare(bytes: &[u8]) -> Option<Self> {
        if bytes.len() > MAX_DICTIONARY_LEN || !bytes.starts_with(&ZSTD_DICTIONARY_MAGIC) {
            return None;
        }
        let id = zstd_safe::get_dict_id_from_dict(bytes)?;
        let ddict = DDict::try_create(bytes)?;
        (ddict.get_dict_id() == Some(id)).then_some(Self { ddict, id })
    }

    pub(super) fn ddict(&self) -> &DDict<'static> {
        &self.ddict
    }

    pub(super) fn id(&self) -> NonZeroU32 {
        self.id
    }

    /// The bytes Zstandard holds for the prepared form.
    pub(super) fn bytes(&self) -> usize {
        self.ddict.sizeof()
    }
}

impl fmt::Debug for RunDictionary {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RunDictionary")
            .field("id", &self.id)
            .field("bytes", &self.bytes())
            .finish()
    }
}

/// A dictionary trained for one rewrite, prepared for compressing and for
/// decoding. Every output run stores its own copy of the bytes; the runs of
/// one rewrite share one prepared decoding form, which is freed with the
/// last of them. Readers then keep one dictionary's tables warm for all of
/// them rather than switching between identical copies.
pub(crate) struct Trained {
    bytes: Vec<u8>,
    cdict: CDict<'static>,
    prepared: Arc<RunDictionary>,
}

impl Trained {
    pub(super) fn bytes(&self) -> &[u8] {
        &self.bytes
    }

    pub(super) fn prepared(&self) -> &Arc<RunDictionary> {
        &self.prepared
    }

    pub(super) fn cdict(&self) -> &CDict<'static> {
        &self.cdict
    }
}

impl fmt::Debug for Trained {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Trained")
            .field("len", &self.bytes.len())
            .finish_non_exhaustive()
    }
}

/// What [`choose`] decided.
#[derive(Debug, Default)]
pub(crate) struct Choice {
    /// The dictionary for every output run, if one is used.
    pub trained: Option<Arc<Trained>>,
    pub outcome: DictionaryChoice,
    /// Time spent sampling, training and estimating.
    pub time: Duration,
    /// Workspace this decision counted beside the merge: [`DICTIONARY_WORKSPACE`]
    /// if anything was sampled, otherwise 0.
    pub workspace_bytes: usize,
}

/// Decides whether the rewrite of `inputs` into blocks of `target_block_len`
/// uses a dictionary, and trains it if so, within `room` bytes of workspace.
///
/// The input's decoded length comes from frame headers. Below
/// [`MIN_DICTIONARY_INPUT`] or without room, nothing is decoded. Otherwise
/// blocks are taken evenly across the inputs, whole and decoded, up to
/// [`SAMPLE_BUDGET`]; one in [`HELD_OUT_EVERY`] is kept back and the others
/// train a [`DICTIONARY_LEN`] dictionary. The held-out blocks, cut into
/// pieces of the target length, are compressed with and without it, and the
/// dictionary is used if it saves at least [`MIN_SAVING_PERCENT`]. Training
/// is deterministic for the same inputs.
///
/// An error means an input frame is damaged.
pub(crate) fn choose(
    inputs: &[RunView<'_>],
    target_block_len: usize,
    room: usize,
) -> Result<Choice, CorruptRun> {
    let start = Instant::now();
    let decoded = decoded_len(inputs)?;
    let decided = |outcome, workspace_bytes| Choice {
        trained: None,
        outcome,
        time: start.elapsed(),
        workspace_bytes,
    };
    if decoded < MIN_DICTIONARY_INPUT {
        return Ok(decided(DictionaryChoice::InputTooSmall, 0));
    }
    if room < DICTIONARY_WORKSPACE {
        return Ok(decided(DictionaryChoice::WorkspaceLimit, 0));
    }

    let samples = sample(inputs, decoded)?;
    let Some(trained) = train(&samples.train, &samples.sizes) else {
        return Ok(decided(
            DictionaryChoice::TrainingFailed,
            DICTIONARY_WORKSPACE,
        ));
    };
    let Some(saving_percent) = estimate(&trained, &samples.held, target_block_len) else {
        return Ok(decided(
            DictionaryChoice::TrainingFailed,
            DICTIONARY_WORKSPACE,
        ));
    };
    drop(samples);
    if saving_percent < MIN_SAVING_PERCENT as i32 {
        let outcome = DictionaryChoice::NotBeneficial { saving_percent };
        return Ok(decided(outcome, DICTIONARY_WORKSPACE));
    }
    Ok(Choice {
        trained: Some(Arc::new(trained)),
        outcome: DictionaryChoice::Used { saving_percent },
        time: start.elapsed(),
        workspace_bytes: DICTIONARY_WORKSPACE,
    })
}

/// The decoded length of every block of `inputs`, from frame headers.
fn decoded_len(inputs: &[RunView<'_>]) -> Result<usize, CorruptRun> {
    let mut total = 0usize;
    for input in inputs {
        for block in 0..input.block_count() {
            total = total.saturating_add(input.frame(block)?.decoded_len);
        }
    }
    Ok(total)
}

struct Samples {
    /// Training blocks, back to back, with their lengths.
    train: Vec<u8>,
    sizes: Vec<usize>,
    held: Vec<u8>,
}

/// Takes whole decoded blocks evenly across `inputs`, together at most
/// [`SAMPLE_BUDGET`] bytes.
fn sample(inputs: &[RunView<'_>], decoded: usize) -> Result<Samples, CorruptRun> {
    // Every `step`-th block, counted across inputs, so that blocks of the
    // average length fill the budget.
    let blocks: usize = inputs.iter().map(|input| input.block_count()).sum();
    let step = (decoded / SAMPLE_BUDGET).clamp(1, blocks.max(1));
    let mut samples = Samples {
        train: Vec::with_capacity(SAMPLE_BUDGET),
        sizes: Vec::new(),
        held: Vec::with_capacity(SAMPLE_BUDGET / HELD_OUT_EVERY),
    };
    let (mut taken, mut count) = (0, 0);
    let mut workspace = ReadWorkspace::new();
    let mut at = 0;
    'inputs: for input in inputs {
        while at < input.block_count() {
            let frame = input.frame(at)?;
            at += step;
            if taken + frame.decoded_len > SAMPLE_BUDGET {
                break 'inputs;
            }
            let block = workspace.load(&frame, input.dictionary())?;
            taken += block.len();
            count += 1;
            if count % HELD_OUT_EVERY == 0 {
                samples.held.extend_from_slice(block);
            } else {
                samples.train.extend_from_slice(block);
                samples.sizes.push(block.len());
            }
        }
        at -= input.block_count();
    }
    Ok(samples)
}

/// Trains a dictionary and prepares it for compressing, or `None` if
/// Zstandard cannot.
pub(super) fn train(samples: &[u8], sizes: &[usize]) -> Option<Trained> {
    let mut bytes = Vec::with_capacity(DICTIONARY_LEN);
    zstd_safe::train_from_buffer(&mut bytes, samples, sizes).ok()?;
    // A dictionary this run could not later prepare is not used at all.
    let prepared = Arc::new(RunDictionary::prepare(&bytes)?);
    let cdict = CDict::try_create(&bytes, ZSTD_LEVEL)?;
    bytes.shrink_to_fit();
    Some(Trained {
        bytes,
        cdict,
        prepared,
    })
}

/// The saving of `trained` on the held-out blocks, cut into output-sized
/// pieces, in percent of their plain compressed size; `None` if nothing was
/// held out.
fn estimate(trained: &Trained, held: &[u8], target_block_len: usize) -> Option<i32> {
    const ROOM: &str = "output has the codec's worst-case length";

    let piece_len = target_block_len.max(1);
    let mut cctx = CCtx::create();
    let mut out = Vec::with_capacity(zstd_safe::compress_bound(piece_len));
    let (mut plain, mut with) = (0u64, 0u64);
    for piece in held.chunks(piece_len) {
        out.clear();
        plain += cctx.compress(&mut out, piece, ZSTD_LEVEL).expect(ROOM) as u64;
        out.clear();
        with += cctx
            .compress_using_cdict(&mut out, piece, trained.cdict())
            .expect(ROOM) as u64;
    }
    if plain == 0 {
        return None;
    }
    // Within ±100 for any real compressor output; clamped to stay an i32.
    let saving = 100 - (with.saturating_mul(100) / plain).min(1_000) as i64;
    Some(saving as i32)
}
