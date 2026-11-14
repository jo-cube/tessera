# Scalable capacity and range-local compaction (design, 5AR–5AS)

Status: design, with research measurements. Nothing here is implemented
beyond test-only research: `src/engine/capacity_tests.rs` (5AR: the
current policy, a continuum model, directory costs) and
`src/engine/leveling_tests.rs` (5AS: a discrete model of the leveled
policy with exact keys, versions, tombstones, snapshots and byte limits,
and engine tests over real runs). Sections 1–3 are 5AR's analysis;
sections 4 onward are the design as 5AS validated and corrected it. The
implementation plan is the last section.

Every number is marked as one of:

- **measured**: from a run of the engine or of production code in a bounded
  container, with the command that reproduces it;
- **model**: from the simulation in `capacity_tests::model`, which was first
  checked against the measured engine (below);
- **derived**: arithmetic from measured or modelled figures;
- **hypothesis**: a claim not yet tested.

Sizes are in *segments*: one `target_run_len` of logical input (key, value
and 16 bytes a record), 64 MiB by default.

## 1. Why the engine stops growing

The ceiling is not the run directory (5AQ made lookups and scans over
thousands of runs cheap) but the interaction of three rules:

1. Every flush and compaction output is cut at `target_run_len`. A run is at
   most one segment.
2. The selector only merges a stack deeper than `max_overlap`, or, when
   nothing overlaps, neighbouring *tail* runs (shorter than a segment) once
   there are more than `disjoint_runs_above` runs.
3. A flush is refused when it would publish more than `max_runs` (64) runs.

Merging two full, key-disjoint runs produces two segments of output, cut
again at `target_run_len`: the same two runs, rewritten. The selector
correctly never does it. So once about `max_runs` full disjoint runs exist
(about 60 segments, about 4 GiB of logical data at the defaults), every flush
is refused, `select` returns `None`, the worker has nothing to do, and every
write, deletes included, fails with `UpdateError::FlushRefused`. The refusal
is safe (nothing is lost, readers continue) but permanent until the limits
are raised. Raising `max_runs` alone does not help for long:

| History, reduced scale, no run limit (measured) | WA | Compactions | Runs | One selection |
|---|---|---|---|---|
| Ascending inserts | 2.91 | 720 | 400 | 363 µs |
| Uniform inserts | 42.65 | 3,714 | 404 | 531 µs |
| Uniform updates | 26.66 | 2,633 | 210 | 140 µs |

(`capacity_tests::write_amplification_without_a_run_limit`,
`CAPACITY_WRITES=200`: 64 KiB memtables and segments, limits scaled by the
same factor.) Write amplification grows roughly linearly with the number of
runs, because a flush that spans the key space overlaps every run and each
stack merge rewrites whole overlapping runs. The model, which reproduces
these three histories within 6% (WA 2.90 / 44.84 / 26.88; jobs 727 / 3,919 /
2,671), projects uniform-update WA of 21, 37, 67 and 122 at 100, 200, 400
and 800 segments, and uniform-insert WA of 14 to 81 (**model**). The
selector itself is quadratic: 2.5 ms at 1,024 runs, 41 ms at 4,096 and
665 ms at 16,384 (**measured**, `metadata_and_selection_at_thousands_of_runs`),
and it runs under the state's read lock.

So the fixed ceiling is an artifact of a flat, overlap-driven policy over
bounded runs; removing it requires a different compaction organization, not
a larger limit.

## 2. What each limit is

| Limit | Default | Class | Fate |
|---|---|---|---|
| `memtable_budget` | 64 MiB | physical resource | unchanged; hard admission on writes |
| `target_run_len` | 64 MiB | algorithmic safeguard: bounds a segment, so every job and every snapshot pin is bounded | unchanged; becomes the segment length |
| `max_runs` | 64 | **flat-run artifact** as a capacity limit; its useful side effect is bounding read fan-in | no longer the capacity limit; fan-in is bounded by level 0's limit and the level count (section 7) |
| `max_run_bytes` | 4 GiB | physical resource (stored bytes, filters, dictionaries) | unchanged; hard admission, with a compaction reserve (section 7) |
| `max_inputs`, `max_input_bytes`, `max_output_bytes` | 8, 256 MiB, 256 MiB | algorithmic safeguards that bound a job's temporary memory | unchanged; the selector must bound each job by its actual stored bytes: at the defaults, six segments fit only if they compress to under 43 MiB each (section 8) |
| `max_workspace_bytes` | 64 MiB | physical resource (decoders, contexts, dictionary training) | unchanged |
| `max_overlap` | 4 | algorithmic safeguard (read fan-in) | replaced by level 0's group limit |
| `disjoint_runs_above` | 16 | flat-run artifact | replaced by the small-neighbour rule (section 4) |
| `SnapshotLimits::max_live`, `max_retained_bytes` | 16, 1 GiB | physical resource | unchanged; bound snapshot exposure |
| Directory and run metadata | 8 B a run, 24 B a group, about 112 B of run structure (struct size, heap excluded) | bounded auxiliary overhead | uncharged, proportional to segments (section 7) |

## 3. Organizations compared

**A. Larger runs at deeper tiers** (segment length growing with depth).
Removes the count ceiling, but a deep run is arbitrarily large: a small
update to its range rewrites all of it, its output is held in memory until
publication, and a snapshot pins all of it. It violates the job bound the
milestone asks for. Rejected.

**B. Logical sorted runs of bounded physical segments, tiered.** A sorted
run is a key-ordered sequence of disjoint segments; several sorted runs
overlap. Tiered merging writes less (each record is rewritten about once
per tier, `K` tiers for `K·(N/s)^(1/K)` sorted runs; **derived**), but it
keeps up to one version of an updated key per sorted run, and in a
RAM-first engine space amplification is memory. Read fan-in is the number of sorted runs.
Range-partitioned tiered merging is possible but has no advantage over C
once space matters. The 5AQ directory already reads this shape efficiently.
Not the default.

**C. Leveled organization of bounded segments.** Level 0 holds flush
output and may overlap; each deeper level is a set of disjoint segments.
Jobs merge a range of one level into the overlapping segments of the next.

| Size (segments) | Policy | Uniform update WA | Uniform insert WA | Ascending WA | Space amp. (updates) | Largest job |
|---|---|---|---|---|---|---|
| 400 | current, no run limit | 67.3 | 44.5 | 2.90 | 1.03 | 5 |
| 400 | leveled, ratio 4, split flushes | 6.70 | 8.43 | 1.00 | 1.32 | 6 |
| 400 | leveled, ratio 8, split flushes | 8.12 | 10.86 | 1.00 | 1.14 | 10 |
| 1,600 | leveled, ratio 4, split flushes | 8.22 | 10.74 | 1.00 | 1.33 | 6 |
| 6,400 | leveled, ratio 4, split flushes | 9.75 | 13.07 | 1.00 | 1.33 | 6 |
| 6,400 | leveled, ratio 8, split flushes | 12.22 | 16.40 | 1.00 | 1.14 | 10 |
| 6,400 | leveled, ratio 4, whole level 0 | 9.82 | 13.66 | 1.00 | 1.33 | 18.6 |
| 6,400 | leveled, ratio 8, whole level 0 | 12.49 | 16.81 | 1.00 | 1.14 | 34.3 |

(**model**, `policies_as_the_database_grows`, `MODEL_RUNS=6400`.) This
continuum model clips runs at any key and has no input-count limit, so it
is optimistic: the 5AS discrete model, which merges whole segments within
the real limits, gives 1.2× (inserts) to 1.5× (overwrites) its WA at the
same size, and with today's 8 inputs a job about 3× (section 9). With
static level targets the same model gave update space amplification of 1.85
(ratio 4) and 1.36 (ratio 8); dynamic level sizing brings it to the
theoretical `1 + 1/r + 1/r² + …`. Largest job is input in segments.

**Recommendation: C, with four Tessera-specific simplifications**, which
together make it smaller than a conventional leveled LSM:

1. *Levels are scheduling metadata, not a correctness invariant.* Tessera
   reads by sequence number: the directory orders groups by their largest
   sequence, a lookup stops only at a group whose largest sequence is below
   the version found, and a tombstone survives a merge while any run outside
   the job overlaps it. None of this depends on which level a run is in. A
   misplaced run can cost work, never correctness. So levels are not
   persisted, and a job may take any subset of level 0.
2. *Dynamic level sizing* (targets set from the last level), for space.
3. *Flush splitting*: a flush is cut at the base level's segment
   boundaries, so a level-0 run overlaps few base segments and level 0 is
   merged a range at a time. 5AS bounds it (section 8) and finds it
   matters only when a job budget binds.
4. *Ratio 4.* Lower WA and half the job size of ratio 8, for 1.33 against
   1.14 space amplification under pure overwrites (1.00 under inserts).
   It keeps the low-WA emphasis; ratio 8 is the documented alternative for
   update-heavy, memory-tight deployments, and the only tunable added.

## 4. Level state and invariants

### Representation

`State` gains, beside the run list and the directory (both unchanged; the
directory is still rebuilt from the run set at every publication):

- `levels: Vec<Vec<SharedRun>>`. Level 0 is in publication order and its
  runs may overlap (the runs of one flush are disjoint among themselves).
  Levels 1..n are each sorted by smallest key.
- `level_bytes: Vec<usize>`: stored and held bytes per level, adjusted at
  every publication.
- `cursors: Vec<Vec<u8>>`: per level, the key after the last source taken.
  Any key is valid; past the last run it wraps to the first.

About 8 bytes a run and one cursor a level beyond today. Nothing is
persisted and the run and checkpoint formats are unchanged.

### Invariants after every publication

These are checked after every publication in the model (thousands per
run, never violated) and the engine test checks them after each job:

1. **Membership.** Every published run is in exactly one level, and the
   levels' union is the run list the directory is built from.
2. **Deeper levels.** Each level above 0 is sorted and strictly disjoint:
   each run's smallest key sorts above the previous run's largest. Every run
   has a record.
3. **Level 0 order.** Level 0 is in publication order.
4. **Accounting.** Each level's byte count equals the sum over its runs.
5. **Replacement.** A job's outputs cover exactly the union of its inputs'
   key ranges, take the place of its destination runs, and are disjoint
   from the rest of their level. Untouched runs stay the same shared run.
6. **Reads.** What any read returns depends only on the run set, never on
   level tags: the directory is built without them, lookups order by
   sequence, and tombstones survive while any run outside the job covers
   their key. (The model reads every key through all segments, ignoring
   levels, against a reference map: no loss, no resurrection, through
   overwrites, deletes, splits, moves and snapshots.)
7. **Retention.** A replaced run stays readable to every snapshot holding
   it and is counted as retained until the last one is dropped.

### Maintenance targets (may be exceeded for a while)

Level 0's flush groups at most the trigger (4); each middle level's bytes
at most its target; no two adjacent runs of a level both under half a
segment; the number of levels as dynamic sizing asks. A state exceeding a
target is valid; it only means work is due. Empty middle levels are valid
(they appear when a level is inserted and fill by trivial moves).

### Recovery

A checkpoint is one sorted scan cut into runs, so recovered runs are
disjoint: they become the last level and level 0 is empty (**measured**:
recovered runs sorted, disjoint, one directory group). Dynamic sizing then
inserts empty levels above it without rewriting anything (**model**: 799
recovered segments, 3 insertions, 0 bytes written; then legal jobs as soon
as writes arrive).

## 5. Job formation

Scores and level insertion are as in 5AR: level 0 by flush groups over the
trigger, level `i` by bytes over `max(base, last / ratio^(last−i))`, a new
level 1 when level 1's share would exceed `ratio · base`. Then:

- **Trivial move**: a source that overlaps nothing below is relabelled.
- **Level `i ≥ 1`**: the source at the cursor and the runs of `i + 1` it
  overlaps (binary search).
- **Level 0 (corrected in 5AS)**: the oldest run, then every other
  level-0 run overlapping the job's range, oldest first, each with the
  level-1 runs it adds, while the job fits. 5AR took only runs contained
  in the range; taking overlapping ones lets one job absorb several
  flushes, so a level-1 range is rewritten once per batch rather than once
  per flush. Any subset of level 0 is correct (invariant 6).
- **Fit**, from metadata known before merging: inputs at most
  `max_inputs`; stored and held input bytes at most `max_input_bytes` and,
  as the output estimate, at most `max_output_bytes`; the workspace
  estimate (per input a context and its largest block) at most
  `max_workspace_bytes`. The measured publication growth of a job (below)
  makes input bytes a safe output estimate.
- **Split fallback**: when a job does not fit, the source alone is
  rewritten, cut at destination boundaries into pieces whose own jobs fit
  now. It is taken only if that yields at least two pieces with records;
  otherwise the job is refused (`Job` refusal, section 7).
- **Small neighbours**: two adjacent runs of a level, both under half a
  segment, merge into one, only if the merged range overlaps at most
  `ratio` segments below. 5AS found a loop without that condition: the
  grandparent cut split the merged pair into two again, forever. The merge
  must reduce the run count or it is not taken.
- **Output cuts**: at `target_run_len`, and where an output into level
  `i` would overlap more than `ratio` segments of level `i + 1`. Cuts
  fall between complete keys (one version per key in a merge's output).

### Progress measure of a split

A split is successful maintenance only if the next job from its level
takes one of its pieces and fits. The model asserts this after every split
(2,000+ splits across the runs; no failure) and counts any split of a
piece (0 in every run). Why it terminates: for a level `i ≥ 1` source,
the bytes of level `i + 1` under a piece change only through that piece
(level `i` is disjoint), so a piece that fits stays fitting. For level 0,
the oldest run is always the next source, so its pieces are consumed
before anything else merges into their range. A piece overlapping one
destination always fits when the limits are valid (section 7), so
splitting cannot recurse.

## 6. Progress results

`every_admitted_state_progresses_or_refuses` (**model**, deterministic,
bounded to 200,000 steps each; all settle within a few thousand):

| Scenario | Outcome |
|---|---|
| Full level 0 (12 flushes published without compaction) over recovered data | idle in 10 steps (2 jobs, 6 moves) |
| Recovery into one level of 799 segments | 3 level insertions, no writes; idle |
| Writes after that recovery | idle; 376 jobs, 132 moves, no refusal |
| Level-0 runs over many destinations a sixteenth of a segment long, 8 inputs | idle; 134 splits (269 pieces), 0 resplits |
| The same with 2 inputs a job | idle; 744 splits (4,363 pieces), 0 resplits |
| Job budgets of 1.5 stored segments (invalid) | explicit `Job` refusal (1,009 times), then flushes refused for good after 4,005 writes |
| A snapshot retaining more than allowed | `Retained` refusal; writes refused once level 0 reaches its stop |
| The same after the snapshot ends | idle in 12 steps; flushes admitted |
| `max_run_bytes` reached by inserts | flushes refused after 97,622 of 200,000 writes (needs user action) |
| `max_run_bytes` with overwrites of the same size | all 200,000 writes; compaction reclaims |
| 318 small recovered segments | 238 neighbour merges; idle |

So every admitted state the model reaches either makes progress or ends in
one of three explicit refusals: `Retained` (until snapshots end), `Job`
(invalid limits), or a refused flush at `max_run_bytes` or the level-0
stop. Compaction does not always progress: snapshots and explicit limits
can stop it, and that is reported, not hidden.

## 7. Job geometry and byte limits

**Segment sizes (measured**, `segment_sizes_against_job_limits`, 6 MiB
per case at 1 MiB segments**):**

| Values | Zstd | LZ4 | Zstd + dictionary | Uncompressed |
|---|---|---|---|---|
| text-like (100 B) | 0.224 | 0.440 | 0.224 | 0.872 |
| JSON (~150 B) | 0.168 | 0.279 | 0.168 | 0.913 |
| random 1 KiB | 0.988 | 0.991 | 0.988 | 0.987 |
| tiny (4 B keys, no value) | 0.398 | 0.458 | 0.398 | 0.489 |

(stored plus held bytes per logical byte.) No segment stored more than its
logical length, so a segment cut at the target stores at most the target.
But a segment is cut *after* the record that reaches the target, so the
largest is the target plus one record: **measured** 2.46 targets after a
merge with a value of almost two targets; and two such segments in one job
were refused by a four-target output limit (4.9 MiB against 4 MiB), with
the engine unchanged.

At the defaults the largest record is about one memtable (64 MiB), so the
largest segment is about 128 MiB and two of them reach the 256 MiB input
limit exactly. Two changes make every job formable:

1. **Cut before an overshoot**: close a non-empty output before a record
   that would take it past the target. The largest segment becomes
   `max(target, largest record)`, 64 MiB at the defaults. Not a format
   change.
2. **Validation rule**: `max_input_bytes` and `max_output_bytes` at least
   twice that largest segment, and `max_inputs ≥ 2`. A pair (one source
   piece, one destination) then always fits, so the split fallback always
   terminates. At the defaults (256 MiB against 2 × 64 MiB) it holds.
   Configurations below it are rejected when built, not refused at run
   time.

**Jobs under the limits (model**, uniform overwrites, ratio 4 and 8, 32
inputs):

| Values | Budget | Ratio 4 WA | Splits | Largest job | Ratio 8 WA |
|---|---|---|---|---|---|
| text | default (≈19 compressed segments) | 6.68 | 0 | 27 inputs | 9.31 |
| text | 4 stored segments | 8.49 | 237 | 14 inputs | 20.19 |
| text | 2.2 stored segments (smallest valid) | 14.32 | 1,445 | 10 inputs | 24.33 |
| JSON | default | 6.73 | 0 | 27 inputs | 9.31 |
| incompressible | default (= 4 segments) | 8.92 | 264 | 15 inputs | 20.49 |
| incompressible | 2.2 stored segments | 14.35 | 1,456 | 10 inputs | 24.44 |

Every configuration completed with no `Job` refusal and every job within
its budget. Small budgets cost WA through splits, not correctness.

**The input count binds before the bytes do.** With today's
`max_inputs = 8`, a level-0 job cannot take several flushes and their
level-1 range at once (**model**, 400,000 writes):

| Workload | 8 inputs, ratio 4 | 32 inputs, ratio 4 | 8 inputs, ratio 8 | 32 inputs, ratio 8 |
|---|---|---|---|---|
| uniform overwrite | 19.84 | 9.27 | 41.69 | 15.18 |
| uniform insert | 17.26 | 11.55 | 26.24 | 14.16 |
| mixed with deletes | 21.75 | 10.43 | 43.51 | 15.17 |

32 inputs fit the default workspace: 32 contexts of 1 MiB plus blocks is
about 34 MiB, and a dictionary needs 21.2 MiB beside it, 55 MiB of the
64 MiB default (**derived** from `CONTEXT_ALLOWANCE` and
`DICTIONARY_WORKSPACE`). Ratio 8 needs more inputs than 32 to approach
ratio 4's WA and stays the memory-leaning alternative.

**When a job is refused, precisely**: (a) `Job`: one source piece and one
destination exceed the limits; impossible under the validation rule.
(b) A build failing on a limit (an estimate wrong, say a codec change
growing the output): the job is not retried unchanged; its source is split
instead. (c) `Retained`: snapshots retain more than `max_retained_bytes`.
(d) Publication beyond `max_run_bytes` that does not shrink it.

## 8. Flush splitting

Policy: a flush is cut at level 1's boundaries (read when the flush is
prepared; stale boundaries only cost efficiency) only where both sides keep
at least an eighth of a segment, and into at most 8 pieces per flushed
segment. Cuts fall between complete keys; an ideal boundary that would
leave a tiny piece is skipped, and the piece then overlaps two destinations.

`flush_splitting_fragmentation` (**model**, 200,000 writes, 32 inputs):

| Policy | Pieces per flush, max | Smallest piece | Level-0 runs, max (uniform insert / overwrite / dense boundaries) | WA uniform overwrite | WA uniform insert |
|---|---|---|---|---|---|
| none | 1 | — | 5 / 5 / 5 | 6.76 | 10.41 |
| unbounded | 32 / 15 / 264 | 131 B (one record) | 113 / 62 / 268 | 6.92 | 10.09 |
| 1/16, 16 | 7–8 | 4.1 KiB | 35 / 30 / 31 | 6.81 | 10.29 |
| **1/8, 8** | 4 | 8.1 KiB (S/8) | 20 / 17 / 16 | 6.68 | 10.30 |
| 1/4, 4 | 2 | 16.1 KiB | 10 / 10 / 10 | 6.75 | 10.44 |

Boundaries read 8 publications late changed WA by less than 0.5. Sequential
inserts produce one piece a flush whatever the policy (trivial moves, WA
1.00). Hot ranges: WA 1.54–1.55 for every policy.

Unbounded splitting multiplies level-0 runs up to 268 with one-record
pieces; the bounds keep pieces at least S/8, at most 4 per flush here and
level 0 at about 20 runs, at no WA cost. Splitting matters only when a job
budget binds: with incompressible values and the default byte limit, WA is
8.9 with splitting and 15.4 without, because unsplit level-0 runs need
merge-time splits (**model**). Chosen: 1/8, 8.

## 9. Workloads

`workloads_under_leveling` (**model**, 400,000 writes, 64 KiB segments,
text-like values; overwrites and the hot range over 100,000 keys; the
snapshot history holds a snapshot for 30 of every 40 flushes with at most
16 segments retained):

| Workload | Current WA | Leveled 4 WA | Space amp. (current / leveled 4) | Segments (current / leveled 4) | Level-0 runs max | Notes |
|---|---|---|---|---|---|---|
| uniform insert | 82.22 | 11.55 | 1.00 / 1.01 | 801 / 938 | 20 | |
| uniform overwrite | 41.16 | 9.27 | 1.04 / 1.32 | 213 / 310 | 19 | |
| ascending insert | 2.92 | 1.00 | 1.00 / 1.01 | 799 / 1,504 | 5 | all trivial moves |
| hot-range overwrite | 2.88 | 2.09 | 1.01 / 1.04 | 203 / 378 | 18 | |
| mixed with deletes | 36.43 | 10.43 | 1.02 / 1.25 | 247 / 367 | 15 | |
| sustained snapshots | 13.78 | 8.94 | 1.13 / 1.32 | 246 / 316 | 40 | 749 `Retained` refusals, 27 blocked flushes, each cleared within 21 steps |

The model flushes after 266 distinct keys (the measured flush size at this
scale; an overwrite in the memtable replaces the older version), sizes a
record at 131 logical bytes and 0.214 stored bytes per logical byte (the
measured ratio for these values), and adds 64 bytes per run. It has no
time: no concurrency between flushes and compaction, and maintenance runs
to completion after each flush.

Calibration (**model** against **measured**, `the_discrete_model_reproduces_the_measured_policy`,
200,000 writes): the current policy's WA 2.90 / 46.28 / 26.63 / 2.88
against the engine's 2.91 / 42.65 / 26.66 / 2.88 (ascending, uniform
insert, uniform overwrite, hot); runs 400 / 402 / 209 / 204 against 400 /
404 / 210 / —. Uniform inserts are 8.5% high, the rest within 0.4%.

Observations:

- WA falls 2–7× against the current policy everywhere except sequential
  inserts, which become free.
- Leveled keeps more, smaller runs: ascending inserts keep about two runs
  per segment (half-segment flushes moved down unmerged; the small-neighbour
  rule does not merge them because they are not both under half a
  segment). That is metadata, not WA.
- Space amplification under overwrites and deletes is 1.25–1.33 at ratio 4
  (1.09–1.14 at ratio 8): the main cost of leveling in a RAM-first engine.
- Under the snapshot history, compaction is refused while snapshots retain
  too much, flushes stop at level 0's limit (12 groups), and both clear by
  themselves when the snapshot ends.

## 10. Resource admission

Accounted separately:

| Class | Bound | Kind |
|---|---|---|
| Published runs (stored and held) | `max_run_bytes` | hard; flush admission |
| Output under construction | `max_output_bytes` (one job at a time) | hard per job; transient, not in `max_run_bytes` |
| Retired runs snapshots pin | `max_retained_bytes` | hard; compaction refused beyond it |
| Pending flushes | 2 × `memtable_budget` | hard; writes |
| Decoder and compaction workspaces | `max_workspace_bytes` per job; readers' own | hard per job (estimate) |
| Directory, level vectors, run structures | about 16 B a run, 24 B a group, 112 B a run struct | uncharged auxiliary; bounded by run count |

None of these bounds process RSS (allocator slack, output buffer doubling,
reader workspaces).

**Reserve.** Publication replaces inputs, so a job's effect on published
bytes is its output minus its input, not its output. Measured on real runs
(`a_leveled_schedule_over_recovered_runs_keeps_levels_valid`): at most
229 B per job, 76 B per output run (filters and indexes of tiny runs, one
codec, one workload). So a full-output reserve looks unnecessary. An
estimate of the growth is the most output runs a job can write times a
per-run overhead, plus a dictionary copy per run where dictionaries are
used: with 32 inputs, about 34 × (16 KiB + 4 KiB) ≈ 680 KiB (**derived**).
That is not a proven bound: re-encoding under another codec (after a
configuration change) or different block packing could grow a job by more.
Safe admission therefore uses the job's actual output, measured after the
build and before publication, rather than a fixed reserve: compaction that
shrinks published bytes is always admitted; growth is admitted within
`max_run_bytes`.

**Level-0 stop.** 12 flush groups: reached only under the snapshot
history (**model**); otherwise level 0 peaked at 5 groups.

**`max_runs`.** Not a capacity limit any more. Fragmentation is bounded by
construction (pieces at least S/8, neighbour merges), and the model's
largest run count was 1.5 times its settled count (uniform overwrites, 468
against 310), and sequential inserts keep about two runs per segment. Keep it as a metadata safety
valve scaled to memory, `max_run_bytes / 64 KiB` (65,536 at the default
4 GiB; a run costs about 150 B of uncharged metadata, so 10 MiB at the
valve), refused like a byte limit.

**Refusals:**

| Refusal | Recovers |
|---|---|
| Level-0 stop | automatically, as compaction drains level 0 |
| `Retained` | when the application drops its snapshots |
| Flush at `max_run_bytes` with reclaimable versions | automatically, as compaction reclaims |
| Flush at `max_run_bytes` full of live data | user action: `set_run_limits` (deletes are writes and are refused too) |
| `max_runs` valve | automatically by neighbour merges if fragmentation is the cause; otherwise user action |
| `Job` (limits below the validation rule) | never at run time: rejected when configured |

## 11. Capacity projections

Keys of 24 bytes; `c` is the stored fraction (0.25 close to the measured
0.17–0.22 on JSON and text; 1.0 incompressible). Segments include 1.33
space amplification under overwrites. WA from the 5AR continuum model
interpolated in log size, times the discrete model's correction at one size
(1.2 for inserts, 1.5 for overwrites, 32 inputs): **derived**, assuming the
correction holds at larger sizes (**hypothesis**).

| Records | Value | Logical | Segments (insert / update) | Stored, c = 0.25 / 1.0 | Levels below 0 | Run metadata | Filters | WA insert / update |
|---|---|---|---|---|---|---|---|---|
| 1 M | 100 B | 0.13 GiB | 2 / 3 | 0.03 / 0.13 GiB | 1 | < 1 KiB | 2 MiB | ≤ 7.4 / 7.8 |
| 1 M | 1 KiB | 1.0 GiB | 16 / 21 | 0.25 / 1.0 GiB | 2 | 3 KiB | 2 MiB | ≤ 7.4 / 7.8 |
| 1 M | 4 KiB | 3.9 GiB | 62 / 82 | 1.0 / 3.9 GiB | 3 | 10 KiB | 2 MiB | ≤ 7.4 / 7.8 |
| 10 M | 100 B | 1.3 GiB | 21 / 28 | 0.3 / 1.3 GiB | 3 | 3 KiB | 16 MiB | ≤ 7.4 / 7.8 |
| 10 M | 1 KiB | 9.9 GiB | 159 / 211 | 2.5 / 9.9 GiB | 4 | 26 KiB | 16 MiB | 8.2 / 8.7 |
| 10 M | 4 KiB | 38.5 GiB | 616 / 820 | 9.6 / 38.5 GiB | 5 | 102 KiB | 16 MiB | 11.1 / 10.9 |
| 100 M | 100 B | 13 GiB | 209 / 277 | 3.3 / 13 GiB | 4 | 35 KiB | 159 MiB | 8.8 / 9.1 |
| 100 M | 1 KiB | 99 GiB | 1,585 / 2,109 | 25 / 99 GiB | 6 | 264 KiB | 159 MiB | 12.9 / 12.5 |
| 100 M | 4 KiB | 385 GiB | 6,163 / 8,197 | 96 / 385 GiB | 7 | 1 MiB | 159 MiB | 15.7 / 14.7 |

Per row (**derived**): lookups visit one group per level plus level 0's
(5–11; up to the stop plus the levels under backpressure); scans hold about
109 KiB a group; a job reads at most `max_input_bytes` (256 MiB) with a
workspace of at most 55 MiB; a snapshot is exposed to at most one job's
input per compaction and to `max_retained_bytes` in all. Whether a row fits
is decided by `max_run_bytes`, which is memory. Nothing was run at these
sizes.

## 12. Selection complexity and publication

| Step | Current | Proposed |
|---|---|---|
| Choose what to compact | O(n²) | O(levels) scores |
| A job's inputs | — | O(log n + k) |
| Runs outside the job, for tombstones | O(n) | O(levels · log n + level-0 runs) |
| Publication | O(n log n) directory rebuild | the same, plus O(n) level splice |

The directory build is 35 µs at 1,024 runs and 2.1–4.0 ms at 65,536
(**measured**, 5AR). Publication is unchanged in kind: under the write
lock, inputs removed by pointer, outputs placed, directory rebuilt, inputs
retired. A trivial move only relabels. Snapshots hold their directory (8 B
a run). Nothing new is persisted; run formats v3/v4/v5 and the checkpoint
layout stay.

## 13. Changes from 5AR

1. **Inputs per job**: 32, not 8; with 8, leveled WA doubles and ratio 8
   needs a split for nearly every job.
2. **Level-0 jobs** take overlapping level-0 runs, not only contained ones.
3. **Small-neighbour merges** only within the grandparent bound, and only if
   the run count falls (a loop otherwise).
4. **Cut before an overshoot**, and a validation rule of two largest
   segments per job budget, so every job can be formed.
5. **Flush splitting** bounded to pieces of at least S/8, at most 8 per
   segment; useful only when budgets bind.
6. **Reserve**: not a full output; growth is checked on the built output (the ≈ 680 KiB estimate is not a bound).
7. **WA projections** 1.2–1.5× higher than 5AR's.

Unchanged: leveled organization of existing bounded runs, ratio 4 (ratio 8
as the alternative), dynamic level sizing, in-memory levels, sequence-based
reads, no new persistent state.

## 14. Remaining risks

- The WA and space figures are a model, calibrated against the engine only
  for the current policy; leveled WA has not been measured on the engine.
- Deletes at `max_run_bytes` are refused like any write.
- Checkpoints rewrite every live key; at tens of GiB that dominates writes.
- Ascending inserts keep about two runs per segment.
- Foreground flushes stall writers; nothing here prevents a flush worker
  (flush splitting tolerates stale boundaries).
- Directory rebuild is linear under the write lock.

## 15. Implementation plan

Steps, and what must land together:

1. **Level metadata and invariants**: `levels`, `level_bytes`, `cursors`
   in `State`; an invariant check run after every publication in debug and
   tests.
2. **Recovery and publication**: flushes append to level 0; compaction
   publication splices outputs into the destination level; recovery and
   checkpoint load put runs in the last level.
3. **Bounded selection**: scores, dynamic sizing, level insertion, level-0
   and level-`i` jobs, fit by bytes, inputs and workspace, per-level overlap
   query for the tombstone rule.
4. **Output splitting and moves**: compaction output cut at given keys
   (split fallback) and before an overshoot; grandparent cuts; trivial
   moves; small-neighbour merges.
5. **Resource admission**: level-0 stop; reserve; validation rule;
   `max_runs` as a valve; `max_inputs` default 32.
6. **Retirement**: `select` and its quadratic stacks, `max_overlap` and
   `disjoint_runs_above` (accepted and ignored for one release, then
   removed).

Steps 1–4 and the level-0 stop and `max_runs` default of step 5 must land
in one change: the old selector's outputs cannot keep levels disjoint, and
leveled compaction does not reduce the run count, so with `max_runs = 64`
it would refuse flushes. Flush splitting, the reserve and the retirement of
obsolete options can follow.

**Smallest first milestone (5AT): leveled compaction in production, without
flush splitting.** Steps 1–4, the level-0 stop, `max_runs` as a valve,
`max_inputs` 32 and the validation rule, replacing `select` in the same
change. Tested by: the invariants after every publication under the
existing concurrency and crash tests; reads against a model with level tags
shuffled; recovery into one level; the progress scenarios of section 6 on
the engine at reduced scale; WA at reduced scale against this model
(within its calibration error). Then 5AU: flush splitting, the reserve,
removal of the obsolete options, and reduced-scale benchmarks.
