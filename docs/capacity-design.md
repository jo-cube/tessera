# Scalable capacity and range-local compaction (design 5AR–5AS, implemented 5AT–5AU)

Status: implemented in 5AT; 5AU added flush splitting, the level-0 wait
and removed `disjoint_runs_above`. Section 16 records what 5AT built and
measured, section 17 what 5AU changed and measured.
Sections 1–3 are 5AR's analysis of the flat policy that 5AT replaced
(kept for its measurements; the 64-run ceiling they describe no longer
exists). Sections 4–15 are the design as 5AS validated and corrected it,
with its research in `src/engine/capacity_tests.rs` (5AR) and
`src/engine/leveling_tests.rs` (5AS, a discrete model of the policy).

Every number is marked as one of:

- **measured**: from a run of the engine or of production code in a bounded
  container, with the command that reproduces it;
- **model**: from the simulation in `capacity_tests::model`, which was first
  checked against the measured engine (below);
- **derived**: arithmetic from measured or modelled figures;
- **hypothesis**: a claim not yet tested.

Sizes are in *segments*: one `target_run_len` of logical input (key, value
and 16 bytes a record), 64 MiB by default.

## 1. Why the flat policy stopped growing (before 5AT)

The ceiling is not the run directory (5AQ made lookups and scans over
thousands of runs cheap) but the interaction of three rules:

1. Every flush and compaction output is cut at `target_run_len`. A run is at
   most one segment.
2. The selector only merges a stack deeper than `max_overlap`, or, when
   nothing overlaps, neighbouring *tail* runs (shorter than a segment) once
   there are more than `disjoint_runs_above` runs (an option removed in
   5AU).
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
| `disjoint_runs_above` | 16 | flat-run artifact | replaced by the small-neighbour rule (section 4); removed in 5AU |
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

- Leveled WA is measured on the engine only at reduced scale (200,000
  writes, 64 KiB runs; section 16); larger figures are the model's.
- Deletes at `max_run_bytes` are refused like any write.
- Checkpoints rewrite every live key; at tens of GiB that dominates writes.
- Ascending inserts keep about two runs per segment.
- Foreground flushes stall writers, and since 5AU a writer at the level-0
  stop also waits for the worker; nothing here prevents a flush worker
  (flush splitting tolerates stale boundaries).
- With the background worker, level 0 runs at its stop under sustained
  random writes, so writers stall for compaction (0.3–0.8 s per 200,000
  writes here); WA is then at or below the synchronous figure (section
  17).
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
   `disjoint_runs_above` (accepted and ignored in 5AT, removed in 5AU
   without an alias).

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

## 16. Implementation (5AT)

### What was built

- **Levels** (`src/engine/levels.rs`): in memory beside the run directory,
  rebuilt on recovery (every checkpointed run into level 1) and never
  persisted. Level 0 is in publication order and may overlap; deeper levels
  are sorted and disjoint. Reads still go through the run directory by
  sequence, so level tags cannot change a result. Every publication in a
  test or debug build checks membership, order, disjointness and byte
  totals (`Levels::check`).
- **Selection**: dynamic level sizing, insertion of a level while the last
  exceeds its target, scores compared as exact fractions with ties to the
  shallower level, round-robin cursors, binary-search overlap. A level-0
  job takes level-0 runs oldest first while the job fits, with the level-1
  runs they overlap. A level-`i` job takes one run and its overlap below.
  Then, in order: a trivial move when nothing below overlaps; a split of
  the source at the boundaries of the level below when the job would not
  fit; a merge of two small adjacent runs; otherwise a refusal naming the
  job (`CompactionError::JobLimit`).
- **Outputs** (`run::compact_with`): cut before a record that would
  overshoot the target (a record larger than the target gets a run of its
  own), at given keys (splits), and where the run would overlap more than
  `RATIO - 1` runs two levels down. A neighbour merge writes one run.
- **Publication**: unchanged admission (growth measured on the built
  output; shrinking always allowed), then `Levels::replace` validates
  every input's place before changing anything (`CompactionError::Stale`
  otherwise). Moves are applied as they are chosen, under the write lock.
- **Tombstones**: the "others" a tombstone must be kept for are found per
  level, level 0 by scan and deeper levels by binary search, instead of
  scanning every run. The rule itself is unchanged.
- **Limits**: `max_runs` is a safety valve (default 65,536); `max_run_bytes`
  is the physical limit. With a background worker, a flush is refused while
  level 0 holds 12 flushes (`RunLimitError::max_level0_flushes`). `max_inputs`
  defaults to 32; inputs, bytes and workspace are checked separately before
  anything is allocated. Validation requires byte limits of two runs and a
  workspace of two runs' blocks, so every job can be formed.

### Differences from the design and the model

| Item | Design / model | 5AT |
|---|---|---|
| Flush splitting | pieces at level-1 boundaries | not built: a flush is one or more target-length runs |
| Level-0 trigger | more than `max_overlap` flushes | at least `max_overlap` (1 now drains level 0 every flush) |
| Level-0 stop | always | only with a background worker; a manual engine is bounded by the run limits and the caller |
| Segment estimate (sizing, "small") | runs' stored length | `target_run_len` × the stored/logical ratio of flushes so far; after recovery, the largest run |
| Split estimate | each piece's own bytes | the whole source per piece: conservative, may refuse a split that would fit |
| Grandparent cut | by bytes overlapped | by runs overlapped (more than 3) |
| A split that drops nothing | n/a | refused when it would write one unchanged run; published when it dropped records, which is progress |
| Level-0 stop refusal | stall | `UpdateError::FlushRefused`; the caller waits for the worker and retries |

### Merge memory (measured)

`tests/compaction_memory.rs`, release, one merge of 8, 16 and 32 inputs
with 64 KiB blocks and `ZstdDictionary` (the trial chose plain blocks,
`NotBeneficial`, on these values). Rust heap counted by a global allocator;
Zstandard's decompression contexts are allocated by `malloc` and reported
by `MergeWorkspace::context_bytes`.

| Inputs | Input MiB | Output MiB | Estimate MiB | Peak heap MiB | Peak − output MiB | Contexts MiB |
|---|---|---|---|---|---|---|
| 8 | 2.0 | 1.6 | 30.8 | 2.6 | 1.0 | 0.7 |
| 16 | 4.1 | 3.2 | 39.3 | 4.5 | 1.3 | 1.5 |
| 32 | 8.2 | 7.0 | 56.3 | 9.4 | 2.4 | 2.9 |

The estimate for 32 inputs (56.3 MiB) is within the default workspace
limit (64 MiB), so the default of 32 needed no budget raised; the
estimate's per-input context allowance dominates it and is far above what
these merges used. These are single runs, not a bound on process RSS.

### Production comparison (measured, reduced scale)

`leveled_tests::production_policies_compared`, release, in the bounded
container: 200,000 writes of 15-byte keys and 100-byte text values,
64 KiB memtables and runs, limits of four runs in and out, every
compaction synchronous after each flush (so its time is the worker's CPU).
"Flat(64)" is the old selector with its old 64-run limit, "flat
unlimited" the same without a limit, "leveled" 5AT with its defaults so
scaled. WA = (flushed + compacted) / flushed; latencies are in-process
point lookups and prefix scans after the load.

| Workload | Policy | Writes done | WA | Stored written / logical | Space amp. | Runs | Levels | Compaction s | Point µs | Prefix µs |
|---|---|---|---|---|---|---|---|---|---|---|
| ascending | flat(64) | 32,280 (refused) | 2.75 | 0.546 | 0.98 | 64 | 1 | 0.0 | 1.9 | 2.2 |
| ascending | flat unlimited | 200,000 | 2.91 | 0.594 | 1.00 | 400 | 1 | 0.3 | 11.3 | 12.5 |
| ascending | leveled | 200,000 | 1.00 | 0.204 | 1.00 | 744 | 4 | 0.0 | 11.2 | 12.5 |
| uniform insert | flat(64) | 31,473 (refused) | 9.09 | 1.967 | 1.01 | 64 | 1 | 0.1 | 11.2 | 29.3 |
| uniform insert | flat unlimited | 200,000 | 42.12 | 9.392 | 1.05 | 404 | 1 | 4.3 | 11.6 | 32.8 |
| uniform insert | leveled | 200,000 | 11.42 | 2.547 | 1.05 | 414 | 4 | 0.8 | 11.5 | 36.1 |
| uniform overwrite | flat(64) | 32,280 (refused) | 2.75 | 0.546 | 0.98 | 64 | 1 | 0.0 | 7.1 | 8.1 |
| uniform overwrite | flat unlimited | 200,000 | 22.80 | 4.915 | 1.10 | 109 | 1 | 1.8 | 11.5 | 29.7 |
| uniform overwrite | leveled | 200,000 | 7.37 | 1.588 | 1.37 | 141 | 3 | 0.5 | 13.0 | 42.9 |
| hot range | flat(64) | 32,280 (refused) | 2.75 | 0.546 | 0.98 | 64 | 1 | 0.0 | 7.1 | 8.1 |
| hot range | flat unlimited | 200,000 | 2.33 | 0.432 | 1.01 | 108 | 1 | 0.1 | 11.3 | 23.7 |
| hot range | leveled | 200,000 | 1.68 | 0.312 | 1.04 | 187 | 3 | 0.1 | 11.2 | 13.6 |
| mixed with deletes | flat(64) | 99,522 (refused) | 10.93 | 2.828 | 1.09 | 64 | 1 | 0.4 | 11.5 | 30.5 |
| mixed with deletes | flat unlimited | 200,000 | 18.14 | 4.758 | 1.06 | 126 | 1 | 1.3 | 11.6 | 32.1 |
| mixed with deletes | leveled | 200,000 | 9.05 | 2.373 | 1.27 | 159 | 3 | 0.6 | 12.3 | 38.8 |

No compaction or flush was refused under leveling; level 0 never held
more than 4 flushes. Peak published runs and filters were 0.7–4.9 MiB and
the test process's RSS 8–19 MiB in every row. The flat(64) rows stop where the old
ceiling refused a flush, which is why their lookups look cheap: they hold
a fraction of the data.

Against the unlimited flat policy, leveling cut WA 2.9× (ascending),
3.7× (uniform insert), 3.1× (overwrite), 1.4× (hot range) and 2.0×
(mixed). It costs space on overwrites and deletes (1.37 against 1.10;
1.27 against 1.06), since older versions wait in deeper levels. Prefix
scans over the whole range are 10–44% slower for uniform inserts,
overwrites and mixed writes (more levels to merge) and faster for the hot
range; point lookups are within 13%.

### Against the model (derived)

The model at the same 200,000 writes (`leveling_tests::workloads_under_leveling`,
`LEVELING_WRITES=200`, policy "leveled 4, no flush split"):

| Workload | Model WA | Engine WA | Model space amp. | Engine space amp. | Model runs | Engine runs |
|---|---|---|---|---|---|---|
| ascending | 1.00 | 1.00 | 1.01 | 1.00 | 752 | 744 |
| uniform insert | 10.41 | 11.42 | 1.01 | 1.05 | 463 | 414 |
| uniform overwrite | 6.76 | 7.37 | 1.33 | 1.37 | 156 | 141 |
| hot range | 1.55 | 1.68 | 1.03 | 1.04 | 187 | 187 |
| mixed with deletes | 8.45 | 9.05 | 1.23 | 1.27 | 177 | 159 |

The engine writes 7–10% more than the model wherever compaction rewrites
anything. Likely causes, not separated: the grandparent cut by count
rather than bytes, the conservative segment estimate, the `>=` level-0
trigger, and the model's fixed compression ratio against real blocks.
Ascending inserts keep about two runs per segment, as the model predicts.

### Not done in 5AT

- Flush splitting, and a stall (rather than a refusal) at the level-0
  stop (both done in 5AU). Flushes still run on the writer that needs room.
- Removal of `disjoint_runs_above`, accepted and ignored (removed in 5AU).
- Checkpoints still rewrite every live key.
- No measurement beyond 200,000 writes at 64 KiB runs: nothing here
  verifies hundreds of millions of records.


## 17. Stabilization (5AU)

### Flush splitting (implemented)

The policy is section 8's: a flush is cut before the first record whose
key is at or after one of level 1's smallest keys, only when the piece so
far holds at least `target_run_len / 8` of estimated input, at least as
much remains, and the flushed segment has fewer than 8 pieces. Cuts fall
between keys in byte order (empty keys and `0xFF` keys included); a record
is never divided, so a record larger than a segment is one piece.

The boundaries are copied under the state lock when the flush begins
(`FlushCuts`), and the lock is released before encoding. A boundary that
moves before publication cannot harm correctness: level 0 accepts any
overlap, reads resolve by sequence number, and the publication path is the
unsplit one. A stale boundary only makes a piece overlap two level-1 runs.
No metadata is persisted: pieces are ordinary level-0 runs with one flush
tag, and recovery places runs as before.

The remaining-bytes estimate is exact for one memtable; for several it
counts every version, so it may overestimate what remains after
deduplication and let a cut leave a slightly short last piece.

### Level-0 backpressure (implemented)

A flush that would make level 0 hold more than its stop (12 flushes) is
handled by cause:

| Cause | What a writer sees |
|---|---|
| Temporary backlog, background worker running | waits, without the state lock, for the worker's pass; re-checks after each published step and resumes as soon as level 0 has room |
| Snapshots retain too much (`Retained`) | trivial moves still run (they retain nothing); merges wait for snapshots; a flush that still finds level 0 full is refused (`FlushRefused`, `RunLimitError::background` = `Failed(Retained)`) |
| Job cannot fit (`JobLimit`; `OutputLimit` if an output outgrows its estimate beyond the headroom, as a codec change can make it) | the pass fails; the waiting writer is woken and refused, `background` = `Failed(..)` naming the limit |
| `max_runs` / `max_run_bytes` exhausted | refused without waiting (a stored non-level-0 refusal skips the wait); `background` is `None` |
| Worker panicked or stopped | the waiter is woken (the worker notifies on exit) and refused, `background` = `Panicked(message)` |
| Manual mode (no worker) | no stop; only the run limits refuse; `Engine::compact` makes room |

The wait is one requested pass bounded by the worker's own progress: it
ends when level 0 has room, the requested pass finishes, or the worker is
no longer running. It is a condition-variable wait on the counters the
worker already maintains, with no timeouts or polling. A refused level-0
flush does not request another pass (the wait already did), so an
impossible job runs at most once per refused flush. A stored level-0
refusal is lifted as soon as level 0 has room, including after a move.

The refusal carries its cause: `RunLimitError::background:
Option<Box<BackgroundError>>`, set at the stop from the outcome of the
pass the flush waited for, read under the worker's lock in the same step
that ended the wait (`State::blocked`, cleared once level 0 has room). A
stored refusal returned to a later write carries that write's own wait.
`background_status` alone could not serve: its `last_failure` is the
latest pass's, which a pass started by another request may already have
replaced, so a caller could not tell a refusal that will repeat from one
that will not. With the cause: `Failed(Retained)` lifts when snapshots are
dropped, `Failed(JobLimit | OutputLimit)` only when limits change,
`Panicked` never (only `Engine::compact` drains level 0), and `None` at the
stop does not occur (a pass ends early only on a failure). `RunLimitError`
and `UpdateError` are no longer `Copy` (the cause holds a
`CompactionError`, boxed because a `CompactionError` can hold a
`RunLimitError`).

### Level priority and the job budget (implemented)

Two corrections to selection, both found by attributing the background
measurement below:

- **Level 0 is scored 1 once due**, however many flushes it holds, so any
  deeper level over its target goes first. Scored by its flushes (up to
  12/4 = 3), a level-0 backlog let level 1 reach about three times its
  target before a level-1 job outranked level 0; each level-0 job, held to
  the byte budget, then took about one level-0 run with four or five
  level-1 runs. Synchronous compaction never lets level 0 pass its
  trigger, so its order is unchanged (its measurements were identical).
- **A job is formed only within 31/32 of `max_output_bytes`**
  (`OUTPUT_HEADROOM`). A merge's output, stored bytes and filters, can
  exceed its inputs' (up to 1,022 B a job measured, mostly filter and
  footer bytes of more output runs). A job admitted with its inputs at
  the limit then failed with `OutputLimit` before publishing, and the next
  selection, which reads the same state, chose it again: at the level-0
  stop, flushes were refused for good.
  `a_merge_at_the_byte_limits_is_not_admitted_to_overflow` reproduces it
  deterministically without the headroom (a 29,581-byte merge refused at a
  29,581-byte limit, three times in a row) and passes with it. The headroom
  is empirical, not a proof: a change of codec or dictionary between input
  and output grows output by far more (merge memory, below).
- **A source whose job outgrew the output limit gets a smaller allowance.**
  `max_output_bytes` limits all of one job's output runs together, so
  cutting the output into more runs cannot help; a smaller job can, and
  level 0 and every deeper level accept a job that takes fewer runs or
  splits its source at the boundaries below (the existing split, which
  keeps versions of a key together and levels disjoint). After such a
  failure, `Levels::outgrown` records the job's source (by weak
  reference, dropped once the run is retired) with half the job's input
  bytes as the most a job from it may take. Selection applies it to that
  source only: fewer joined runs, a split, or, once even the source with
  one run below exceeds it, a refusal before anything is read, reported
  as `OutputLimit` with the output that crossed the limit. Each failure
  halves the allowance, so a source fails at most about log2(limit / its
  size) times; a refusal at selection lets the other levels proceed in
  the same pass, where a failed build ends the pass. A single record
  whose output alone exceeds the limit (possible only across a codec
  change, since output keeps one version a key) is refused this way, for
  good, until the limit is raised.
  `output_growth_past_the_headroom_does_not_repeat_a_job` (Zstandard runs
  recovered uncompressed, limits at their minimum of two runs): without
  the allowance the same job failed 9,971 times in a row with 29 steps
  done; with it, 21 merges failed in all, 1,195 steps published, and
  compaction settled on a selection-time refusal with every record
  intact.

### Removed configuration

`CompactionOptions::disjoint_runs_above` is removed (no alias). The flat
selector survives only as a test reference with its old threshold. The
other limits stay: `max_overlap` is level 0's trigger; `max_inputs`,
`max_input_bytes`, `max_output_bytes`, `max_workspace_bytes`, `max_runs`,
`max_run_bytes` and the snapshot limits bound resources.

### Merge memory (measured)

`merges_stay_within_their_estimate_and_output` (release, 64 KiB blocks,
2 MiB segments, level-0 flushes small enough not to split). Peak Rust heap
during a level-0 merge, and native decoder contexts counted separately:

| Case | Inputs | Estimate MiB | Peak heap MiB | Peak − output MiB | Native contexts MiB |
|---|---|---|---|---|---|
| dictionary, text | 32 | 56.2 | 7.7 | 2.3 | 2.9 |
| zstd, text | 32 | 35.1 | 7.7 | 2.3 | 2.9 |
| lz4, text | 32 | 35.1 | 16.0 | 2.3 | 0 |
| uncompressed, text | 32 | 35.1 | 31.3 | 1.1 | 0 |
| zstd, incompressible | 32 | 35.1 | 32.4 | 2.4 | 2.9 |
| dictionary, incompressible | 32 | 56.2 | 32.4 | 2.4 | 2.9 |
| lz4, incompressible | 32 | 35.1 | 32.6 | 2.5 | 0 |
| zstd, 512 KiB records | 16 | 22.0 | 17.5 | 5.5 | 1.5 |
| zstd recovered as lz4 | 14 | 16.0 | 6.6 | 1.3 | 0.5 |
| dictionary recovered as uncompressed | 14 | 16.0 | 12.2 | 0.5 | 0.5 |

Working memory beside the output stays within the estimate in every case;
the output itself is bounded by `max_output_bytes`, not the workspace.
This holds at 64 KiB blocks for these record sizes; it is not a bound for
other block sizes or for records larger than a block, which the estimate
charges separately.

### Paired measurement (measured, reduced scale)

`split_flushes_compared` (ignored test; 200,000 writes, 64 KiB segments,
`max_input_bytes = max_output_bytes = 4` segments, release, 2 CPUs).
"Unsplit" and "Split" compact synchronously to idle after every flush;
"Background" uses the worker and the level-0 wait, then drains with
`wait_for_compaction`. Every policy sees the same seeded history, codec
and limits; WA is (flushed + compaction output bytes) / flushed bytes for
all three, counted from each step's report. "Before" is 5AU without the
two corrections above; "after" is with them (one run each; three runs of
the background rows after vary by at most 4%).

| Workload | Values | Unsplit before / after | Split before / after | Background before / after |
|---|---|---|---|---|
| ascending | both | 1.00 / 1.00 | 1.00 / 1.00 | 1.00 / 1.00 |
| uniform insert | text | 11.42 / 11.42 | 11.11 / 11.11 | 11.40 / 8.31 |
| uniform insert | incompressible | 24.37 / 24.74 | 16.28 / 16.79 | 45.15 / 14.82 |
| uniform overwrite | text | 7.37 / 7.37 | 7.11 / 7.11 | 7.84 / 5.78 |
| uniform overwrite | incompressible | 14.76 / 14.81 | 8.02 / 8.22 | 20.40 / 7.08 |
| hot range | text | 1.68 / 1.68 | 1.68 / 1.68 | 1.68 / 1.68 |
| hot range | incompressible | 1.67 / 1.67 | 1.66 / 1.66 | 1.67 / 1.67 |
| mixed with deletes | text | 9.05 / 9.25 | 8.80 / 8.88 | 12.92 / 6.85 |
| mixed with deletes | incompressible | 20.98 / 21.03 | 13.32 / 13.65 | 39.25 / 12.37 |

The synchronous "after" figures rise by up to 3% (incompressible values),
the cost of the headroom: jobs are at most 31/32 as large.

Background attribution (step log, uniform insert, incompressible):

| | Before | After |
|---|---|---|
| Level-0 merges | 4,375 | 931 |
| Level-0 / level-1 inputs per level-0 merge | 1.2 / 4.8 | 2.9 / 4.6 |
| Level-1 bytes rewritten per level-0 byte | 37.3 | 7.1 |
| Level 1 when a level-0 merge starts | 1,557 KiB | 651 KiB |
| Level-0 flushes when a level-0 merge starts | 11.9 | 11.6 |
| Level-0 merges leaving overlapping level-0 runs out (budget-bound) | 100% | 99% |
| Operating endpoint (before the drain): WA, levels (runs:KiB) | 43.72; 47:350/59:2001/165:7981/210:11163 | 14.72; 29:291/7:241/22:992/82:4064/300:15903 |
| Drained endpoint WA | 45.15 | 14.82 |
| Writer waits at the stop (total) | 675 (2.81 s) | 418 (0.83 s) |
| Foreground time | 2.94 s | 0.96 s |

Synchronous Split, for comparison: 2.3 / 4.2 inputs, 8.2 level-1 bytes per
level-0 byte, level 1 at 611 KiB. Before the correction the operating
endpoint was not representative: data sat in level 0 and an oversized
level 1, one level shallower than after draining. After it, the two
endpoints differ by under 3% WA in every workload.

Other measured columns:

- Pieces per flush (synchronous Split): mean 2.7–4.1, max 5; unsplit 2 (a
  memtable spans two segments here). Ascending inserts are never split.
- Merge-time splits with incompressible values, Unsplit → Split: 637 →
  379 (uniform insert), 582 → 36 (overwrite), 584 → 264 (mixed).
- Level 0: at most 4 flushes synchronously; 12 in background random
  workloads (the stop), 4 for hot ranges and 3 for ascending inserts.
- Space amplification: unchanged by either correction (1.00–1.39).
- Short prefix scans, Split vs Unsplit, uniform insert of text: 36 → 56 µs
  at the benchmark's endpoint, identical over three runs (deterministic):
  3.25 → 5.95 blocks per scan, 4 → 7 directory groups, because Unsplit
  ends just after a level-0 merge emptied level 0 and Split ends with
  three flushes' pieces there. Range-local level-0 jobs stop once level 0
  is below its trigger, leaving pieces of other flushes behind, so split
  level 0 sits nearer its trigger. Measured over the next four flush
  phases instead (new random keys, compacted after each): 36.3 → 42.1 µs
  and 3.27 → 3.82 blocks per scan on average; equal for overwrites and
  hot ranges, 3–13% faster for mixed deletes. A read cost of up to about
  16% on short scans against the WA reduction above; splitting is
  unchanged.
- Point reads (after): within ±5% across policies, except mixed deletes
  with incompressible values (1.40–1.64 µs). Short prefix scans: Split vs
  Unsplit 36 → 56 µs for uniform inserts of text (more level-0 runs at
  the end), elsewhere within noise (a hot-range gain in one run, 22 → 13
  µs, did not recur); background after differs from
  synchronous Split by −28% to +59% (3–56 µs). Long scans within ±10%.
  One run each: timing noise of this size is not separated.
- Compaction CPU tracks WA; whole-run flush time is 0.08–0.11 s for every
  policy, so the cost of cutting flushes is not separable at this scale.

### The unexplained refusal

One 5AU run of the background policy ended with a refused final flush
(`level0_flushes 13 > 12`); the run counted earlier write refusals without
printing them, and had no step log. The cause was not observed directly.
What the protocol allows: the waiting flush holds only the flush guard,
flushes are serialized, and the worker only lowers level 0's flush count,
so 13 at publication means the wait ended with level 0 at 12, which needs
a pass that ended without room. A pass ends early only on a failure (level
0 at its stop is always due), and in that configuration the only failure
found possible is the output-limit admission defect above: `JobLimit`
cannot arise (two segments fit four), `Retained` needs snapshots, `Stale`
publication needs a second compactor. The background policy then formed
10–36 jobs per run within 1 KiB of the limit, and the deterministic test
shows such a job failing repeatedly. That is a supported hypothesis, not
an observation: 4 more full runs, 8 runs of that configuration and 30
runs under CPU contention with the step log (errors and failed waits
recorded) did not reproduce the refusal, and in the measured runs no job
within 1 KiB of the limit grew. Ruled out by construction and test: lost
wakeups (the wait re-reads progress and level 0 after every step under
the worker's lock), waits holding locks the worker needs, and stranded
waiters at shutdown or panic (`a_worker_panic_ends_a_wait_for_level_0`).

### Against the model (derived)

The model (section 8) predicted splitting is negligible for compressible
values and helps where a job budget binds (8.9 vs 15.4). The engine agrees
in direction: text changes by 0.1–0.3 WA, incompressible values by 6–8.
The engine's incompressible WA is higher than the model's in both
policies. The model compacts synchronously and has no backlog, so it could
not show the level-0 priority defect.

### Not done in 5AU

- Asynchronous flushing: flushes still run on the writer.
- The refusal in one early run is explained by a demonstrated defect, not
  observed (above).
- No bound on output growth across a change of codec or dictionary: such
  merges fail at the output limit a bounded number of times per source,
  then are refused at selection until the limit is raised.
- No measurement beyond 200,000 writes at 64 KiB segments.
