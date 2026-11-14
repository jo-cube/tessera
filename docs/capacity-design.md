# Scalable capacity and range-local compaction (design, 5AR)

Status: design, with research measurements. Nothing here is implemented
beyond the test-only research in `src/engine/capacity_tests.rs`; the
smallest implementation step is the last section.

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

(**model**, `policies_as_the_database_grows`, `MODEL_RUNS=6400`.) With
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
   merged a range at a time. It bounds every job at `ratio + 2` segments
   and lowers WA slightly against merging level 0 whole.
4. *Ratio 4.* Lower WA and half the job size of ratio 8, for 1.33 against
   1.14 space amplification under pure overwrites (1.00 under inserts).
   It keeps the low-WA emphasis; ratio 8 is the documented alternative for
   update-heavy, memory-tight deployments, and the only tunable added.

## 4. Range-local compaction

### State

`State` keeps, beside the directory (unchanged, rebuilt at every
publication as now):

- `level0: Vec<SharedRun>`, in flush order;
- `levels: Vec<Vec<SharedRun>>` for levels 1..n, each sorted by smallest
  key, pairwise disjoint (`a.max < b.min`);
- per level, its stored bytes and a compaction cursor (a key, in memory).

The directory is built from all of them. Snapshots keep only the directory,
as now.

### Targets and choice

- Level 0's score is its flush groups over 4 (the trigger).
- The last level has no target. Level `i` (1 ≤ i < last) targets
  `max(base, last_bytes / 4^(last − i))`, with `base` = 4 segments.
- When level 1's target would exceed `4 · base`, a new empty level is
  inserted under level 0 (the base level is always level 1).
- The highest score above 1 is compacted. Scores are O(levels).

### Jobs

- **Trivial move.** A run of level `i` that overlaps nothing in level
  `i + 1` is relabelled to it, without rewriting. Found by binary search.
- **Level `i ≥ 1`.** Starting at the cursor, take the first segment of `i`
  whose largest key is at or above it; take the segments of `i + 1` it
  overlaps (`partition_point` for the first whose largest key ≥ its
  smallest, then while smallest ≤ its largest). Optionally extend along
  level `i` while the job stays within `ratio + 2` segments and the input
  limits.
- **Level 0.** Take the oldest level-0 run; take the base segments it
  overlaps; add every level-0 run contained in their combined range, oldest
  first, while the job fits. Runs straddling the range stay in level 0;
  that is safe because reads go by sequence (simplification 1).
- **Split fallback.** If one input and what it overlaps below exceed the job
  limits (a level-0 run cut at boundaries that have since moved, or a
  segment over a dense range of the next level), rewrite that run alone,
  cut at the next level's boundaries, in place. The fallback is bounded by
  one segment.
- **Small neighbours.** In a disjoint level, two adjacent segments both
  under half a segment are merged with each other (the current tail rule,
  per level). Without it, sequential inserts move half-segment flushes down
  unmerged and keep about 1.9 runs per segment (**model**: 11,945 runs for
  6,400 segments, WA 1.00). Hypothesis: it adds at most 1 to ascending WA.

### Output

- The merge is the existing `run::compact` over whole input runs: no
  logical slicing. Output covers exactly the union of the inputs' ranges;
  nothing outside it is read or rewritten. Untouched runs stay the same
  `Arc` (**measured**: `a_range_local_replacement_leaves_other_runs_shared`).
- Output is cut at `target_run_len`, and additionally where it would
  overlap more than `ratio` segments of the level below the target
  ("grandparent" cut), so the next job from it is bounded too.
- Cuts fall between keys. A merge emits one version per key (snapshots keep
  old versions through the runs they pin), so a version chain is never split
  across segments, and output segments are disjoint.
- Whole runs were preferred to logical slicing (a run visible through a key
  window) because slicing keeps dead bytes alive in partly consumed runs,
  needs a garbage policy and clips every read; bounded segments already
  make each rewrite small.

### Invariants of a job

- **Tombstones**: kept while any run outside the job overlaps the
  tombstone's key (today's rule, unchanged), whatever its level: older runs
  in level 0 included. The overlap query becomes per-level binary search
  plus level 0, O(levels · log n + k) instead of O(n).
- **Equal-sequence conflicts**: detected by the merge as now; inputs are
  whole runs, so no conflicting pair is hidden by a split.
- **Snapshots**: a snapshot holds its directory and so its runs; a job
  never waits for one. Replaced runs are retired (`State::retire`) and
  counted as retained while a snapshot holds them; `max_retained_bytes`
  still refuses compaction beyond it. Exposure per job is at most its
  input, `ratio + 2` segments.
- **Failure**: building happens outside the lock; on any error the output
  is dropped and the state is as before. Publication checks that every
  input is still published (compactions are serialized; flushes only add
  to level 0) and that the output is disjoint from the rest of its level.
- **Binary keys**: only byte comparisons of actual smallest and largest
  keys, inclusive. No successor or predecessor keys are computed, so empty
  keys and `0xFF` prefixes need nothing special.

## 5. Selection complexity

| Step | Current | Proposed |
|---|---|---|
| Choose what to compact | O(n²) over candidates | O(levels) scores |
| Find a job's inputs | — | O(log n + k) binary search, k ≤ ratio + 2 |
| Runs outside the job for tombstones | O(n) | O(levels · log n + level-0 runs) |
| Publication | O(n log n) directory rebuild | same; plus O(log n) level insert |
| Metadata | 8 B a run, 24 B a group | plus 8 B a run for level membership |

Measured directory build (production `RunDirectory::build`): 35 µs at 1,024
runs, 60–170 µs at 4,096, 0.27–0.72 ms at 16,384 and 2.1–4.0 ms at 65,536
(one and four levels). It holds the write lock, which stays acceptable to
tens of thousands of segments; an incremental directory is not needed yet.
Lookups through it: present 182–244 ns at 1,024 runs to 712–805 ns at
65,536; missing 91–275 ns to 458–1,249 ns (**measured**, tiny runs, so
these are directory and filter cost, not block decoding).

## 6. Publication and ownership

- **Atomic replacement** stays as it is: under the write lock, remove the
  inputs (by pointer), add the outputs to their level, rebuild the
  directory, retire the inputs. A trivial move changes only level
  membership and cannot fail.
- **Nothing new is persisted.** Checkpoints already stream live keys into
  fresh disjoint runs (`write_runs` over `snapshot.scan`). Recovery puts all
  of them in one disjoint level, the last; level 0 is empty and further
  levels are inserted as data arrives. WAL replay fills memtables, which
  flush into level 0. Run formats v3/v4/v5 and the checkpoint layout are
  unchanged.
- **Identity**: a segment is its `Arc` in memory and a file only inside a
  checkpoint generation. No segment or level identifiers are needed.
- **Directory lifetime**: each snapshot holds the directory of its moment
  (8 B a run). At 6,400 segments that is 51 KiB per distinct version
  (**derived**), within 16 live snapshots under 1 MiB.
- **Live and reclaimable bytes**: live are the published runs
  (`run_bytes + held_bytes`); reclaimable are the retired runs only
  snapshots hold (`retained_run_bytes`). Both are reported today.

## 7. Resource admission

Hard admission limits (a request is refused, safely, when exceeded):

- writes: `memtable_budget`;
- flush publication: `max_run_bytes − max_output_bytes` (new reserve), and
  level 0 at most `l0_stop` flush groups (12 suggested; a backpressure
  bound on read fan-in, not a capacity limit, since level 0 can always be
  drained by bounded jobs);
- compaction publication: `max_run_bytes`, which the reserve guarantees one
  job's output fits, so compaction can always make progress when flushes are
  admitted;
- compaction build: the existing input, output and workspace limits,
  checked against actual stored bytes; a job is shrunk to fit, or split
  (section 4), or refused;
- snapshots: `max_live`, `max_retained_bytes`.

Bounded auxiliary overhead (not charged, proportional, documented):
directory and level vectors (16 B a segment plus 24 B a group), run
structures (about 112 B a segment), one directory per held snapshot, the
job's selection state (O(k)). At 8,200 segments this is about 1 MiB
(**derived**). None of these limits bounds process RSS: allocator slack,
the output buffer's doubling and reader workspaces are outside them, as
today.

`max_runs` stops being a capacity limit. It stays as an optional safety
valve whose default is large enough never to bind before `max_run_bytes`
(for example, `max_run_bytes / 64 KiB`), so no new fixed ceiling appears.

## 8. Capacity projections

Keys of 24 bytes; `c` is the stored fraction after compression (0.25 is
close to the measured 0.21 on text-like values; 1.0 is incompressible).
Segments include 1.33 space amplification under overwrites. WA from the
ratio-4 split-flush model, interpolated in log size (**derived** from
**model**); below 100 segments the model's 100-segment figure is an upper
bound.

| Records | Value | Logical | Segments (insert / update) | Stored, c = 0.25 / 1.0 | Levels below 0 | Run metadata | Filters (10 bits a key) | WA insert / update |
|---|---|---|---|---|---|---|---|---|
| 1 M | 100 B | 0.13 GiB | 2 / 3 | 0.03 / 0.13 GiB | 1 | < 1 KiB | 2 MiB | ≤ 6.1 / 5.1 |
| 1 M | 1 KiB | 1.0 GiB | 16 / 21 | 0.25 / 1.0 GiB | 2 | 3 KiB | 2 MiB | ≤ 6.1 / 5.1 |
| 1 M | 4 KiB | 3.9 GiB | 62 / 82 | 1.0 / 3.9 GiB | 3 | 10 KiB | 2 MiB | ≤ 6.1 / 5.1 |
| 10 M | 100 B | 1.3 GiB | 21 / 28 | 0.3 / 1.3 GiB | 3 | 3 KiB | 16 MiB | ≤ 6.1 / 5.1 |
| 10 M | 1 KiB | 9.9 GiB | 159 / 211 | 2.5 / 9.9 GiB | 4 | 26 KiB | 16 MiB | 6.8 / 5.7 |
| 10 M | 4 KiB | 38.5 GiB | 616 / 820 | 9.6 / 38.5 GiB | 5 | 102 KiB | 16 MiB | 9.2 / 7.2 |
| 100 M | 100 B | 13 GiB | 209 / 277 | 3.3 / 13 GiB | 4 | 35 KiB | 159 MiB | 7.3 / 6.0 |
| 100 M | 1 KiB | 99 GiB | 1,585 / 2,109 | 25 / 99 GiB | 6 | 264 KiB | 159 MiB | 10.7 / 8.2 |
| 100 M | 4 KiB | 385 GiB | 6,163 / 8,197 | 96 / 385 GiB | 7 | 1 MiB | 159 MiB | 13.0 / 9.7 |

For every row (**derived**):

- **Lookup candidates**: at most one group per level plus level 0's groups,
  so 5–11 normally and up to `l0_stop` + 7 under backpressure; key filters
  leave about one block decode for a present key and 0.01 per group for a
  missing one. Today's directory gives 1 group for disjoint runs and up to
  `max_overlap` otherwise, so a missing-key lookup costs a few more filter
  probes (hypothesis: about 0.2 µs a group, from the measurements above).
- **Scan fan-in**: the same group count, at about 109 KiB of workspace a
  group (measured in 5AQ): 0.5–1.2 MiB per scan.
- **Job size**: at most 6 segments of input (384 MiB logical; 96 MiB
  stored at c = 0.25, 384 MiB at c = 1.0). At c = 1.0 this exceeds the
  default 256 MiB input limit, so the job bound must be derived from the
  limits: the selector stops extending at `max_input_bytes`, and when one
  segment and its overlap exceed it, the split fallback applies. The
  defaults should satisfy `max_input_bytes ≥ (ratio + 2) · target_run_len`
  for incompressible data; that is a validation rule, not a new limit.
- **Temporary memory per job**: its output (about its input), plus
  per-input decoder workspace (measured allowance: 1 MiB a context plus the
  largest block).
- **Snapshot exposure**: at most one job's input per compaction; over time,
  WA times the bytes flushed while the snapshot lives, cut off by
  `max_retained_bytes`.

100 M records of several KiB are hundreds of GiB of logical data; whether
they fit is decided by `max_run_bytes` (memory), not by the run count.
These projections are not a claim that the engine has been run at that
size.

## 9. Read and write amplification

- WA grows with the logarithm of size under leveling (5.1 → 9.8 for updates
  and 6.1 → 13.1 for inserts, 100 → 6,400 segments, **model**), against
  linear growth today. Sequential inserts move without rewriting.
- Below about 60 segments, where today's engine works, leveling costs
  more for sequential inserts (today 2.8–2.9, leveled 1.0 plus the
  small-neighbour merges) and less for random ones (today 14–21 at 100
  segments, leveled 5–6).
- Read fan-in rises from 1–4 groups to about 5–11. Point reads remain one
  block decode in the common case thanks to filters; scans open one group
  cursor per level.

## 10. Invariants

Preserved unchanged, because the read path, merge, tombstone rule,
publication and recovery are unchanged: atomic single-key writes, monotonic
sequence numbers, newest-version visibility, equal-sequence conflict
detection, tombstones and no resurrection, snapshot consistency, binary and
empty keys and `0xFF` prefixes, atomic publication, safe refusal, recovery
and checksums, independent compressed blocks, bounded depth (now
`l0_stop` + levels) and bounded job memory.

New invariants:

1. Each level `i ≥ 1` is sorted and pairwise disjoint; checked at
   publication.
2. Every run is in exactly one level, and the levels' union is the
   directory's run set.
3. A job's output range is the union of its inputs' ranges.
4. Level membership never affects what a read returns (a test can shuffle
   level tags and compare reads against the model).
5. Liveness: while a flush is admitted, some compaction can publish
   (reserve), and level 0 can always be reduced by a bounded job, so the
   permanent refusal of section 1 cannot occur below `max_run_bytes`.

## 11. Risks

- The WA figures are a model: uniform and sequential key distributions,
  one flush size, no deletes. Skewed and delete-heavy workloads are
  untested.
- Flush splitting multiplies small level-0 runs when the base level has
  many segments; a minimum piece size (a sixteenth of a segment, suggested)
  bounds it but is not modelled.
- Checkpoints still rewrite every live key. At tens of GiB that is the
  dominant write cost and a long snapshot; incremental checkpoints would
  need a format change and are out of scope.
- Foreground flushes stall writers (300–400 ms measured earlier); nothing
  here depends on flushes being foreground, so a flush worker can follow.
- Directory rebuild under the write lock grows linearly (4 ms at 65,536
  runs measured).
- Space amplification of 1.33 under overwrites is memory.

## 12. Smallest next milestone

Implement C behind the existing compaction entry points, without changing
formats or the read path:

1. In-memory levels in `State`; recovery and checkpoints put runs in the
   last level; flush output enters level 0, cut at base-level boundaries
   with a minimum piece size.
2. The selector of section 4 (scores, dynamic sizing, trivial moves,
   level-0 and level-i jobs, split fallback, small neighbours), replacing
   `select` and its quadratic stacks; per-level overlap query for the
   tombstone rule.
3. Admission: compaction reserve, `l0_stop`, `max_runs` default raised to a
   safety valve; documentation of the options that change meaning
   (`max_overlap`, `disjoint_runs_above`).
4. Tests: level invariants after every publication; reads unaffected by
   level tags; the 5AR model's scenarios at reduced scale as a WA
   regression check; recovery into one level; liveness at the byte limit.
