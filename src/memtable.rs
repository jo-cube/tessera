//! Bounded mutable storage and its conversion into immutable runs.
//!
//! A [`Memtable`] accepts updates. [`Memtable::freeze`] consumes it and returns
//! a [`FrozenMemtable`], which has no way to change, and
//! [`FrozenMemtable::flush_with`] builds compressed runs from it.
//!
//! # Versions
//!
//! A table is a copy-on-write B+ tree. Snapshots share every node and the
//! arena's chunk handles; copying the handles costs O(number of chunks).
//! Later writes copy shared nodes on their path and, when appending to a
//! shared partial chunk, copy that chunk first. No record straddles chunks.
//! Each tree holds only the latest update of each key it saw: the versions are
//! the trees, not chains inside one.
//!
//! An update replaces the previous update of its key in the table. A delete is
//! stored as a tombstone, even when the table holds nothing for that key,
//! because older runs may.
//!
//! # Sequence numbers
//!
//! The caller assigns sequence numbers, and a memtable only checks them. Every
//! accepted update must have a sequence number above all earlier ones in the
//! table, and above the `after` given to its constructor, so that a table can
//! continue from its frozen predecessor. Updates are never reordered.
//!
//! Every value of `u64` is usable. After `u64::MAX` no number is larger, so
//! every further update is rejected as
//! [`SequenceNotIncreasing`](TableError::SequenceNotIncreasing). Nothing is
//! ever incremented, so nothing can wrap.
//!
//! A rejected update changes nothing, and its sequence number stays unused, so
//! it may be retried with the same number.
//!
//! # Memory
//!
//! A table admits an update only if its charged bytes stay within the budget.
//! The charge is arena bytes used (including overwritten garbage), plus
//! [`ENTRY_OVERHEAD`] per live key and any history pinned by snapshots.
//! Overwrites and deletes append key/value bytes and never free old payloads.
//! A full table must be rotated even when an overwrite would shrink its value.
//!
//! Copied nodes and the used part of a copied arena chunk are charged as
//! [`MemoryUsage::pinned`] until the oldest snapshot needing them is gone.
//! This is an upper estimate when snapshots are dropped out of order; it
//! falls to zero when all snapshots are dropped. Arena garbage remains charged.
//!
//! The charge estimates used bytes, not resident memory. Chunk padding and
//! the bounded free reserve are reported by the engine's pool counters.
//! Records larger than a chunk have exact allocations outside the pool.

use std::cmp::Ordering;
use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Weak};

use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN, Record, Value, Version};
use crate::chunk::ChunkPool;

#[cfg(test)]
use crate::run::TARGET_BLOCK_COMPRESSED;
use crate::run::{Codec, RECORD_INPUT_OVERHEAD, Run, RunBuilder};
mod arena;
use arena::{Arena, Location};

#[cfg(test)]
mod tests;

/// Bytes charged per entry beyond its key and value.
///
/// This is an upper bound on the worst case, on 64-bit targets: the entry's
/// slot in a leaf at the half occupancy every leaf off the tree's right edge
/// keeps (about 66 bytes), plus its share of branch nodes. A table filled with
/// random keys measures about 55 bytes per entry. Allocator rounding is not
/// included, so this is an estimate.
pub const ENTRY_OVERHEAD: usize = 80;

/// Input per output run suggested for [`FrozenMemtable::flush_with`].
pub const TARGET_RUN_LEN: usize = 64 << 20;

pub use crate::run::MAX_TARGET_RUN_LEN;

const FLUSHABLE: &str = "records are ordered, within the limits, and fit in one run";

/// The most entries in a leaf and children in a branch. Nodes hold one more
/// while they split.
const NODE_CAPACITY: usize = 32;

/// Every node off the right edge holds at least half of [`NODE_CAPACITY`],
/// so a tree this deep would hold more entries than a budget can charge.
const MAX_DEPTH: usize = 16;

/// The bytes of a reference-counted allocation's counts.
const ARC_HEADER: usize = 2 * size_of::<usize>();

/// What a copied leaf or branch allocates: its node and its buffers.
const LEAF_BYTES: usize = ARC_HEADER + size_of::<Node>() + (NODE_CAPACITY + 1) * size_of::<Slot>();
const BRANCH_BYTES: usize = ARC_HEADER
    + size_of::<Node>()
    + (NODE_CAPACITY + 1) * (size_of::<Arc<[u8]>>() + size_of::<Arc<Node>>());

/// A key as a table stores it: a prefix, such as a keyspace's, followed
/// by the caller's key. Comparisons treat it as the two concatenated,
/// without building that.
#[derive(Clone, Copy, Debug)]
pub(crate) struct Key<'a> {
    pub(crate) prefix: &'a [u8],
    pub(crate) key: &'a [u8],
}

impl<'a> Key<'a> {
    /// `key` with no prefix.
    pub(crate) fn plain(key: &'a [u8]) -> Self {
        Self { prefix: &[], key }
    }

    fn len(&self) -> usize {
        self.prefix.len() + self.key.len()
    }

    /// How a stored key compares with this one.
    fn order(&self, stored: &[u8]) -> Ordering {
        if let &[prefix] = self.prefix {
            return match stored.split_first() {
                Some((&first, rest)) => first.cmp(&prefix).then_with(|| rest.cmp(self.key)),
                None => Ordering::Less,
            };
        }
        let shared = self.prefix.len().min(stored.len());
        match stored[..shared].cmp(&self.prefix[..shared]) {
            Ordering::Equal if stored.len() < self.prefix.len() => Ordering::Less,
            Ordering::Equal => stored[shared..].cmp(self.key),
            order => order,
        }
    }
}

/// High bit of [`Slot::key_len_and_flag`] marks a tombstone; stored keys are
/// at most `MAX_KEY_LEN + MAX_PREFIX_LEN`, far below `1 << 31`.
const TOMBSTONE_BIT: u32 = 1 << 31;

/// The latest update of one key in one tree. The arena also keeps overwritten
/// payloads until its last table or snapshot is dropped.
#[derive(Clone, Copy)]
struct Slot {
    seq: u64,
    data: Location,
    key_len_and_flag: u32,
}

impl Slot {
    fn new(key: Key<'_>, seq: u64, value: Option<&[u8]>, arena: &mut Arena) -> (Self, usize) {
        let (data, copied) = arena.append(&[key.prefix, key.key, value.unwrap_or_default()]);
        let key_len = key.len() as u32;
        debug_assert!(key_len & TOMBSTONE_BIT == 0);
        (
            Self {
                seq,
                data,
                key_len_and_flag: key_len | if value.is_none() { TOMBSTONE_BIT } else { 0 },
            },
            copied,
        )
    }
    fn key_len(&self) -> usize {
        (self.key_len_and_flag & !TOMBSTONE_BIT) as usize
    }
    fn is_tombstone(&self) -> bool {
        self.key_len_and_flag & TOMBSTONE_BIT != 0
    }
    fn key<'a>(&self, arena: &'a Arena) -> &'a [u8] {
        &arena.get(self.data)[..self.key_len()]
    }
    fn value<'a>(&self, arena: &'a Arena) -> Value<'a> {
        if self.is_tombstone() {
            Value::Tombstone
        } else {
            Value::Bytes(&arena.get(self.data)[self.key_len()..])
        }
    }
    fn value_len(&self, arena: &Arena) -> usize {
        arena.get(self.data).len() - self.key_len()
    }
}

enum Node {
    Leaf(Leaf),
    Branch(Branch),
}

/// Updates in ascending key order.
struct Leaf {
    slots: Vec<Slot>,
}

/// Child `i` holds the keys from `separators[i - 1]` up to, but excluding,
/// `separators[i]`. A separator is a copy of the first key of the child to
/// its right when it split, so it shares no update.
struct Branch {
    separators: Vec<Arc<[u8]>>,
    children: Vec<Arc<Node>>,
}

/// Copies `items` into a buffer that will not grow while the node splits.
fn node_buffer<T: Clone>(items: &[T]) -> Vec<T> {
    let mut buffer = Vec::with_capacity(NODE_CAPACITY + 1);
    buffer.extend_from_slice(items);
    buffer
}

impl Clone for Node {
    /// Copies the node, sharing its updates and children.
    fn clone(&self) -> Self {
        match self {
            Self::Leaf(leaf) => Self::Leaf(Leaf {
                slots: node_buffer(&leaf.slots),
            }),
            Self::Branch(branch) => Self::Branch(Branch {
                separators: node_buffer(&branch.separators),
                children: node_buffer(&branch.children),
            }),
        }
    }
}

impl Node {
    fn bytes(&self) -> usize {
        match self {
            Self::Leaf(_) => LEAF_BYTES,
            Self::Branch(_) => BRANCH_BYTES,
        }
    }
}

impl Leaf {
    fn search(&self, key: Key<'_>, arena: &Arena) -> Result<usize, usize> {
        self.slots
            .binary_search_by(|slot| key.order(slot.key(arena)))
    }

    /// Splits an overfull leaf. Appending at the right edge leaves the left
    /// leaf full, so ascending inserts fill their leaves.
    fn split(&mut self, append: bool, arena: &Arena) -> (Arc<[u8]>, Arc<Node>) {
        let at = if append {
            NODE_CAPACITY
        } else {
            self.slots.len() / 2
        };
        let right = Leaf {
            slots: node_buffer(&self.slots[at..]),
        };
        self.slots.truncate(at);
        (
            Arc::from(right.slots[0].key(arena)),
            Arc::new(Node::Leaf(right)),
        )
    }
}

impl Branch {
    /// The child whose range holds `key`.
    fn route(&self, key: Key<'_>) -> usize {
        self.separators
            .partition_point(|separator| key.order(separator) != Ordering::Greater)
    }

    /// Splits an overfull branch, like [`Leaf::split`].
    fn split(&mut self, append: bool) -> (Arc<[u8]>, Arc<Node>) {
        let at = if append {
            NODE_CAPACITY
        } else {
            self.children.len() / 2
        };
        let right = Branch {
            separators: node_buffer(&self.separators[at..]),
            children: node_buffer(&self.children[at..]),
        };
        self.children.truncate(at);
        self.separators.truncate(at);
        let promoted = self.separators.pop().expect("a split branch keeps a child");
        (promoted, Arc::new(Node::Branch(right)))
    }
}

/// Where a write goes: the child taken at each branch, root first, and the
/// position in the leaf, found once so that the write compares no keys.
struct Path {
    routes: [usize; MAX_DEPTH],
    leaf: Result<usize, usize>,
}

/// Returns the node to change in place, copying it first if another tree
/// shares it, and adds what the copy leaves to the sharers to `detached`.
///
/// Only the writer adds references, and nodes have no weak ones, so a count
/// above one can only fall before the copy is decided. It then counts a copy
/// that is not made, which overstates `detached`, as the plan does.
fn unshare<'n>(node: &'n mut Arc<Node>, detached: &mut usize) -> &'n mut Node {
    if Arc::strong_count(node) > 1 {
        *detached += node.bytes();
    }
    Arc::make_mut(node)
}

/// Writes `slot` at `path` below `node`, `level` branches down, returning
/// the right half and its separator if `node` split. `edge` says whether
/// `node` is on the right edge.
fn insert(
    node: &mut Arc<Node>,
    path: &Path,
    level: usize,
    slot: Slot,
    edge: bool,
    detached: &mut usize,
    arena: &Arena,
) -> Option<(Arc<[u8]>, Arc<Node>)> {
    match unshare(node, detached) {
        Node::Leaf(leaf) => match path.leaf {
            Ok(at) => {
                leaf.slots[at] = slot;
                None
            }
            Err(at) => {
                if leaf.slots.capacity() <= NODE_CAPACITY {
                    leaf.slots
                        .reserve_exact(NODE_CAPACITY + 1 - leaf.slots.len());
                }
                leaf.slots.insert(at, slot);
                (leaf.slots.len() > NODE_CAPACITY)
                    .then(|| leaf.split(edge && at == NODE_CAPACITY, arena))
            }
        },
        Node::Branch(branch) => {
            let at = path.routes[level];
            let last = at + 1 == branch.children.len();
            let (separator, right) = insert(
                &mut branch.children[at],
                path,
                level + 1,
                slot,
                edge && last,
                detached,
                arena,
            )?;
            branch.separators.insert(at, separator);
            branch.children.insert(at + 1, right);
            let full = branch.children.len() > NODE_CAPACITY;
            full.then(|| branch.split(edge && at + 1 == NODE_CAPACITY))
        }
    }
}

type Split = (Arc<[u8]>, Arc<Node>);

/// Single-pass write into a uniquely owned tree (no live snapshots).
/// Returns the overwritten value's byte length if the key existed, and the
/// right half and its separator if `node` split.
fn insert_unique(
    node: &mut Arc<Node>,
    key: Key<'_>,
    seq: u64,
    value: Option<&[u8]>,
    edge: bool,
    arena: &mut Arena,
) -> (Option<usize>, Option<Split>) {
    match Arc::get_mut(node).expect("no live snapshots share this tree") {
        Node::Leaf(leaf) => match leaf.search(key, arena) {
            Ok(at) => {
                let released = leaf.slots[at].value_len(arena);
                let (slot, _) = Slot::new(key, seq, value, arena);
                leaf.slots[at] = slot;
                (Some(released), None)
            }
            Err(at) => {
                let (slot, _) = Slot::new(key, seq, value, arena);
                if leaf.slots.capacity() <= NODE_CAPACITY {
                    leaf.slots
                        .reserve_exact(NODE_CAPACITY + 1 - leaf.slots.len());
                }
                leaf.slots.insert(at, slot);
                let split = (leaf.slots.len() > NODE_CAPACITY)
                    .then(|| leaf.split(edge && at == NODE_CAPACITY, arena));
                (None, split)
            }
        },
        Node::Branch(branch) => {
            let at = branch.route(key);
            let last = at + 1 == branch.children.len();
            let (released, split) = insert_unique(
                &mut branch.children[at],
                key,
                seq,
                value,
                edge && last,
                arena,
            );
            let Some((separator, right)) = split else {
                return (released, None);
            };
            branch.separators.insert(at, separator);
            branch.children.insert(at + 1, right);
            let full = branch.children.len() > NODE_CAPACITY;
            (
                released,
                full.then(|| branch.split(edge && at + 1 == NODE_CAPACITY)),
            )
        }
    }
}

/// A snapshot as its table tracks it.
struct View {
    /// Dead once the snapshot is dropped.
    pin: Weak<()>,
    /// The table's `detached` when it was taken.
    mark: usize,
    /// The table's `live` when it was taken: the charge of the tree it holds.
    live: usize,
}

impl View {
    fn is_alive(&self) -> bool {
        self.pin.strong_count() > 0
    }
}

/// The charge and pins of the trees of one table.
struct Table {
    arena: Arena,
    root: Arc<Node>,
    depth: usize,
    len: usize,
    /// Charged for the entries of this tree.
    live: usize,
    budget: usize,
    last_seq: Option<u64>,
    /// Bytes this table has copied or replaced while another tree shared
    /// them, ever. Only grows.
    detached: usize,
    /// Each snapshot not yet seen dropped, oldest first. What was detached
    /// since the oldest live one may still be held.
    views: VecDeque<View>,
    /// Held by the table and by every tree taken from it, so that a source
    /// handed to others can be followed until the last of them is dropped.
    /// Keep this after `arena` and `root` so a snapshot drops both before
    /// releasing the family reference used by batch admission.
    family: Arc<()>,
}

/// An update [`Table::admit_checked`] accepted, and where it goes.
struct Admitted<'v> {
    path: Path,
    new: bool,
    grows: usize,
    released: usize,
    bytes: Option<&'v [u8]>,
}

impl Table {
    fn new(budget: usize, last_seq: Option<u64>, pool: &ChunkPool) -> Self {
        Self {
            arena: Arena::new(pool),
            root: Arc::new(Node::Leaf(Leaf { slots: Vec::new() })),
            depth: 1,
            len: 0,
            live: 0,
            budget,
            last_seq,
            detached: 0,
            views: VecDeque::new(),
            family: Arc::new(()),
        }
    }

    fn pinned(&self) -> usize {
        self.views
            .iter()
            .find(|view| view.is_alive())
            .map_or(0, |view| self.detached - view.mark)
    }

    /// What this table's live snapshots hold without the table: the newest
    /// one's tree and the history pinned since the oldest. A node an older
    /// snapshot holds that the newest does not was detached between them,
    /// so the two cover every snapshot.
    fn held_by_views(&self) -> usize {
        self.views
            .iter()
            .rev()
            .find(|view| view.is_alive())
            .map_or(0, |view| view.live + self.pinned())
    }

    /// Finds where `key` goes, its slot if it has one, and what writing it
    /// would detach: every node from the first shared one down.
    /// Others can only drop references, so the write itself detaches no more
    /// than this.
    fn plan(&self, key: Key<'_>) -> (Path, Option<&Slot>, usize) {
        let mut routes = [0; MAX_DEPTH];
        let (mut node, mut level, mut shared, mut detach) = (&self.root, 0, false, 0);
        loop {
            shared |= Arc::strong_count(node) > 1;
            if shared {
                detach += node.bytes();
            }
            match &**node {
                Node::Branch(branch) => {
                    routes[level] = branch.route(key);
                    node = &branch.children[routes[level]];
                    level += 1;
                }
                Node::Leaf(leaf) => {
                    let found = leaf.search(key, &self.arena);
                    let slot = found.ok().map(|at| &leaf.slots[at]);
                    let path = Path {
                        routes,
                        leaf: found,
                    };
                    return (path, slot, detach);
                }
            }
        }
    }

    fn write(&mut self, key: Key<'_>, seq: u64, value: Value<'_>) -> Result<(), TableError> {
        let bytes = match value {
            Value::Bytes(bytes) => Some(bytes),
            Value::Tombstone => None,
        };
        let value_len = bytes.map_or(0, <[u8]>::len);
        // The prefix is the table user's, so the limit is on the caller's key.
        if key.key.len() > MAX_KEY_LEN {
            return Err(TableError::KeyTooLong { len: key.key.len() });
        }
        if value_len > MAX_VALUE_LEN {
            return Err(TableError::ValueTooLong { len: value_len });
        }
        if let Some(last) = self.last_seq.filter(|&last| seq <= last) {
            return Err(TableError::SequenceNotIncreasing { seq, last });
        }
        // Both lengths are within the limits above, so this cannot overflow.
        let charge = key.len() + value_len + ENTRY_OVERHEAD;
        if charge > self.budget {
            return Err(TableError::RecordExceedsBudget {
                charge,
                budget: self.budget,
            });
        }
        while self.views.front().is_some_and(|view| !view.is_alive()) {
            self.views.pop_front();
        }
        // Without live snapshots, every node and arena chunk is uniquely owned,
        // so an update that fits even as a new entry can descend once in place.
        if self.views.is_empty() && Arc::get_mut(&mut self.family).is_some() {
            let used = self.arena.used() + self.len * ENTRY_OVERHEAD;
            if charge <= self.budget.saturating_sub(used) {
                let (released, split) =
                    insert_unique(&mut self.root, key, seq, bytes, true, &mut self.arena);
                if let Some((separator, right)) = split {
                    let left = Arc::clone(&self.root);
                    self.root = Arc::new(Node::Branch(Branch {
                        separators: node_buffer(&[separator]),
                        children: node_buffer(&[left, right]),
                    }));
                    self.depth += 1;
                    assert!(self.depth < MAX_DEPTH, "a budget cannot fill a deeper tree");
                }
                match released {
                    Some(old_value_len) => self.live = self.live - old_value_len + value_len,
                    None => {
                        self.len += 1;
                        self.live += charge;
                    }
                }
                self.last_seq = Some(seq);
                return Ok(());
            }
        }
        let admitted = self.admit_checked(key, bytes, value_len, charge)?;
        self.insert(key, seq, admitted);
        Ok(())
    }

    fn insert(&mut self, key: Key<'_>, seq: u64, admitted: Admitted<'_>) {
        let Admitted {
            path,
            new,
            grows,
            released,
            bytes,
        } = admitted;
        let (replacement, copied) = Slot::new(key, seq, bytes, &mut self.arena);
        self.detached += copied;
        if let Some((separator, right)) = insert(
            &mut self.root,
            &path,
            0,
            replacement,
            true,
            &mut self.detached,
            &self.arena,
        ) {
            let left = Arc::clone(&self.root);
            self.root = Arc::new(Node::Branch(Branch {
                separators: node_buffer(&[separator]),
                children: node_buffer(&[left, right]),
            }));
            self.depth += 1;
            assert!(self.depth < MAX_DEPTH, "a budget cannot fill a deeper tree");
        }
        self.len += usize::from(new);
        self.live = self.live - released + grows;
        self.last_seq = Some(seq);
    }

    /// Remaining checks of [`Self::write`] when snapshots may share storage or
    /// the budget is tight enough that an overwrite may still fit.
    fn admit_checked<'v>(
        &mut self,
        key: Key<'_>,
        bytes: Option<&'v [u8]>,
        value_len: usize,
        charge: usize,
    ) -> Result<Admitted<'v>, TableError> {
        let (path, slot, detach) = self.plan(key);
        let (grows, released) = match slot {
            Some(slot) => (value_len, slot.value_len(&self.arena)),
            None => (charge, 0),
        };
        let appended = key.len() + value_len;
        let needed = appended
            + usize::from(slot.is_none()) * ENTRY_OVERHEAD
            + detach
            + self.arena.copy_charge(appended);
        let available = self.budget.saturating_sub(self.memory().charged);
        if needed > available {
            return Err(TableError::BudgetExceeded { needed, available });
        }
        Ok(Admitted {
            path,
            new: slot.is_none(),
            grows,
            released,
            bytes,
        })
    }

    fn get(&self, key: Key<'_>) -> Option<Version<'_>> {
        let mut node = &*self.root;
        loop {
            match node {
                Node::Branch(branch) => node = &branch.children[branch.route(key)],
                Node::Leaf(leaf) => {
                    let slot = &leaf.slots[leaf.search(key, &self.arena).ok()?];
                    return Some(Version {
                        seq: slot.seq,
                        value: slot.value(&self.arena),
                    });
                }
            }
        }
    }

    fn iter_from(&self, start: Key<'_>) -> Records<'_> {
        let mut path = [None; MAX_DEPTH];
        let mut depth = 0;
        let mut node = &*self.root;
        loop {
            match node {
                Node::Branch(branch) => {
                    let at = branch.route(start);
                    path[depth] = Some((branch, at));
                    depth += 1;
                    node = &branch.children[at];
                }
                Node::Leaf(leaf) => {
                    let at = leaf
                        .slots
                        .partition_point(|slot| start.order(slot.key(&self.arena)).is_lt());
                    return Records {
                        arena: &self.arena,
                        path,
                        depth,
                        leaf,
                        at,
                    };
                }
            }
        }
    }

    /// A tree sharing every node with this one, which this table's later
    /// writes do not change.
    fn share(&self) -> Self {
        Self {
            arena: self.arena.clone(),
            root: Arc::clone(&self.root),
            depth: self.depth,
            len: self.len,
            live: self.live,
            budget: self.budget,
            last_seq: self.last_seq,
            detached: 0,
            views: VecDeque::new(),
            family: Arc::clone(&self.family),
        }
    }

    fn memory(&self) -> MemoryUsage {
        let pinned = self.pinned();
        MemoryUsage {
            payload: self.live - self.len * ENTRY_OVERHEAD,
            arena_bytes: self.arena.used(),
            charged: self.arena.used() + self.len * ENTRY_OVERHEAD + pinned,
            pinned,
            budget: self.budget,
        }
    }
}

impl fmt::Debug for Table {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Table")
            .field("len", &self.len)
            .field("memory", &self.memory())
            .field("last_seq", &self.last_seq)
            .finish_non_exhaustive()
    }
}

/// How much memory a table accounts for.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct MemoryUsage {
    /// Key and value bytes of the table's entries.
    pub payload: usize,
    /// Arena bytes used, including overwritten keys/values and deletes.
    /// Unused chunk capacity is reported in the engine pool counters.
    pub arena_bytes: usize,
    /// Bytes charged against the budget, pinned bytes included.
    pub charged: usize,
    /// The part of `charged` that only snapshots of the table still need:
    /// nodes and used arena bytes its writes copied since the oldest live
    /// snapshot. See [Memory](crate::engine#memory).
    pub pinned: usize,
    /// The table's budget.
    pub budget: usize,
}

/// Why an update was rejected. The table is unchanged.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableError {
    /// The key exceeds [`MAX_KEY_LEN`].
    KeyTooLong {
        /// Length of the rejected key.
        len: usize,
    },
    /// The value exceeds [`MAX_VALUE_LEN`].
    ValueTooLong {
        /// Length of the rejected value.
        len: usize,
    },
    /// The sequence number is not above the last one accepted.
    SequenceNotIncreasing {
        /// The rejected sequence number.
        seq: u64,
        /// The sequence number it must exceed.
        last: u64,
    },
    /// The record alone exceeds the budget, so no empty table of this budget
    /// could admit it.
    RecordExceedsBudget {
        /// What the record would be charged in an empty table.
        charge: usize,
        /// The table's budget.
        budget: usize,
    },
    /// The update does not fit now. A new table may admit it, and so may this
    /// one once its snapshots are dropped; arena garbage is never reclaimed here.
    BudgetExceeded {
        /// Appended bytes and node overhead the update needs, including bytes it would pin.
        needed: usize,
        /// Bytes remaining in the budget.
        available: usize,
    },
}

impl fmt::Display for TableError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match *self {
            Self::KeyTooLong { len } => {
                write!(f, "key of {len} bytes exceeds limit of {MAX_KEY_LEN}")
            }
            Self::ValueTooLong { len } => {
                write!(f, "value of {len} bytes exceeds limit of {MAX_VALUE_LEN}")
            }
            Self::SequenceNotIncreasing { seq, last } => {
                write!(f, "sequence number {seq} is not above {last}")
            }
            Self::RecordExceedsBudget { charge, budget } => {
                write!(
                    f,
                    "record charged {charge} bytes exceeds budget of {budget}"
                )
            }
            Self::BudgetExceeded { needed, available } => {
                write!(
                    f,
                    "update needs {needed} bytes and {available} are available"
                )
            }
        }
    }
}

impl std::error::Error for TableError {}

/// An ordered, bounded table of the latest update to each key.
///
/// See the [module documentation](self) for its version, sequence number and
/// memory rules. Writes take `&mut self`: this type is single-writer.
#[derive(Debug)]
pub struct Memtable(Table);

impl Memtable {
    /// Creates an empty table limited to `budget` charged bytes.
    ///
    /// Updates must have sequence numbers above `after`. A table that follows
    /// a frozen one should pass [`FrozenMemtable::last_seq`] here.
    #[cfg(test)]
    pub fn new(budget: usize, after: Option<u64>) -> Self {
        Self::in_pool(budget, after, &ChunkPool::new(0))
    }

    pub(crate) fn in_pool(budget: usize, after: Option<u64>, pool: &ChunkPool) -> Self {
        Self(Table::new(budget, after, pool))
    }

    /// Sets `key` to `value` at sequence number `seq`.
    ///
    /// Key and value limits are checked first, then the sequence number, then
    /// the budget. A rejected update changes nothing.
    #[cfg(test)]
    pub fn put(&mut self, key: &[u8], seq: u64, value: &[u8]) -> Result<(), TableError> {
        self.write(Key::plain(key), seq, Value::Bytes(value))
    }

    /// Stores a tombstone for `key` at sequence number `seq`, under the same
    /// rules as [`put`](Self::put).
    #[cfg(test)]
    pub fn delete(&mut self, key: &[u8], seq: u64) -> Result<(), TableError> {
        self.write(Key::plain(key), seq, Value::Tombstone)
    }

    /// Sets `key` to `value`, or stores a tombstone for it, at sequence
    /// number `seq`, as [`put`](Self::put) and [`delete`](Self::delete) do.
    /// The limit on key length applies to `key.key` alone.
    pub(crate) fn write(
        &mut self,
        key: Key<'_>,
        seq: u64,
        value: Value<'_>,
    ) -> Result<(), TableError> {
        self.0.write(key, seq, value)
    }

    /// Returns the latest update of `key`, which may be a tombstone, or `None`
    /// if the table holds nothing for it.
    #[cfg(test)]
    pub fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.0.get(Key::plain(key))
    }

    /// [`get`](Self::get) for a prefixed key.
    pub(crate) fn get_in(&self, key: Key<'_>) -> Option<Version<'_>> {
        self.0.get(key)
    }

    /// Returns each key's latest update, in ascending key order.
    #[cfg(test)]
    pub fn iter(&self) -> Records<'_> {
        self.0.iter_from(Key::plain(&[]))
    }

    /// [`iter_from`](Self::iter_from) for a prefixed key.
    pub(crate) fn iter_from_in(&self, start: Key<'_>) -> Records<'_> {
        self.0.iter_from(start)
    }

    /// Returns the number of keys held, including tombstones.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.0.len
    }

    /// Returns whether the table holds no keys.
    pub fn is_empty(&self) -> bool {
        self.0.len == 0
    }

    /// Returns the sequence number every update must exceed, if any.
    #[cfg(test)]
    pub fn last_seq(&self) -> Option<u64> {
        self.0.last_seq
    }

    /// Returns the table's memory accounting.
    pub fn memory(&self) -> MemoryUsage {
        self.0.memory()
    }

    /// Whether batch admission must allow for copy-on-write storage.
    pub(crate) fn has_shared_storage(&mut self) -> bool {
        // The uniqueness check synchronizes with snapshot teardown. Its
        // family reference is released after its nodes and pages.
        Arc::get_mut(&mut self.0.family).is_none()
    }

    /// A conservative upper bound on the charge of one additional update,
    /// including nodes and records a live snapshot could pin.
    pub(crate) fn write_charge(&self, key: Key<'_>, value: Value<'_>) -> usize {
        let bytes = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        key.len()
            .saturating_add(bytes)
            .saturating_add(ENTRY_OVERHEAD)
            .saturating_add(self.0.plan(key).2)
            .saturating_add(self.0.arena.copy_charge(key.len().saturating_add(bytes)))
    }

    /// Shares every tree node and copies the arena chunk handles, leaving the
    /// table writable. Later updates do not change the snapshot; what
    /// they copy or replace for it is charged to this table as
    /// [`pinned`](MemoryUsage::pinned) until the snapshot is dropped.
    pub fn snapshot(&mut self) -> FrozenMemtable {
        self.0.views.retain(View::is_alive);
        let pin = Arc::new(());
        self.0.views.push_back(View {
            pin: Arc::downgrade(&pin),
            mark: self.0.detached,
            live: self.0.arena.used() + self.0.len * ENTRY_OVERHEAD,
        });
        FrozenMemtable {
            table: self.0.share(),
            _pin: Some(pin),
        }
    }

    /// Ends the table's mutable life, in constant time and without copying.
    pub fn freeze(self) -> FrozenMemtable {
        FrozenMemtable {
            table: self.0,
            _pin: None,
        }
    }
}

/// A memtable that cannot change: a frozen table, or a snapshot of a
/// table that may still be written.
#[derive(Debug)]
pub struct FrozenMemtable {
    table: Table,
    /// Held by a snapshot, so that its table knows when it is dropped.
    _pin: Option<Arc<()>>,
}

impl FrozenMemtable {
    /// Returns the latest update of `key`, which may be a tombstone, or `None`
    /// if the table holds nothing for it.
    #[cfg(test)]
    pub fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.table.get(Key::plain(key))
    }

    /// [`get`](Self::get) for a prefixed key.
    pub(crate) fn get_in(&self, key: Key<'_>) -> Option<Version<'_>> {
        self.table.get(key)
    }

    /// Returns each key's latest update, in ascending key order.
    pub fn iter(&self) -> Records<'_> {
        self.table.iter_from(Key::plain(&[]))
    }

    /// Returns each update whose key is at or above `start`, in ascending key
    /// order.
    #[cfg(test)]
    pub fn iter_from(&self, start: &[u8]) -> Records<'_> {
        self.table.iter_from(Key::plain(start))
    }

    /// [`iter_from`](Self::iter_from) for a prefixed key.
    pub(crate) fn iter_from_in(&self, start: Key<'_>) -> Records<'_> {
        self.table.iter_from(start)
    }

    /// Returns the number of keys held, including tombstones.
    pub fn len(&self) -> usize {
        self.table.len
    }

    /// Returns whether the table holds no keys.
    #[cfg(test)]
    pub fn is_empty(&self) -> bool {
        self.table.len == 0
    }

    /// Returns the sequence number a successor's updates must exceed, if any.
    pub fn last_seq(&self) -> Option<u64> {
        self.table.last_seq
    }

    /// Returns the table's memory accounting. A frozen table still reports
    /// what its earlier snapshots pin; a snapshot reports no pinned bytes.
    pub fn memory(&self) -> MemoryUsage {
        self.table.memory()
    }

    /// A reference that lives while this table, the table it came from, or
    /// any snapshot of that table does.
    pub(crate) fn family(&self) -> Weak<()> {
        Arc::downgrade(&self.table.family)
    }

    /// An upper estimate of what the live snapshots of this table hold once
    /// this table is dropped, in the terms of its charge: at most
    /// [`MemoryUsage::charged`], and zero without live snapshots.
    pub(crate) fn held_by_snapshots(&self) -> usize {
        self.table.held_by_views()
    }

    /// Returns an iterator that builds this table's runs one at a time, with
    /// the default codec and block length, as [`Self::flush_with`] does.
    #[cfg(test)]
    pub fn flush(&self, target_run_len: usize) -> Flush<'_> {
        self.flush_with(target_run_len, Codec::default(), TARGET_BLOCK_COMPRESSED)
    }

    /// Returns an iterator that builds this table's runs one at a time, with
    /// `codec` and an estimated compressed target of `target_block_compressed` bytes
    /// (see [`RunBuilder::with_target_block_compressed`]).
    ///
    /// A run ends once its records reach `target_run_len` bytes of input,
    /// reduced to [`MAX_TARGET_RUN_LEN`] if larger, so runs hold disjoint key
    /// ranges. An empty table produces no runs. Every tombstone is written.
    ///
    /// The table is only borrowed. A caller that cannot use a run can drop the
    /// iterator and flush again, which produces identical runs. The table and
    /// the runs built so far coexist until the caller drops the table.
    #[cfg(test)]
    pub fn flush_with(
        &self,
        target_run_len: usize,
        codec: Codec,
        target_block_compressed: usize,
    ) -> Flush<'_> {
        Flush {
            records: self.iter(),
            pool: ChunkPool::new(0),
            target: target_run_len.min(MAX_TARGET_RUN_LEN),
            codec,
            target_block_compressed,
        }
    }
}

/// Where a flush ends runs early, beside the target length: before the
/// first record at or above each of `at`, as long as the run would hold at
/// least `min_piece` bytes of input and the rest of the flush would too,
/// and one target length of input is cut into at most `max_pieces` runs.
/// Cuts change only how records are grouped into runs, never which records
/// are written, so any keys are correct, current or stale.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct FlushCuts {
    /// Ascending keys.
    pub(crate) at: Vec<Vec<u8>>,
    pub(crate) min_piece: usize,
    pub(crate) max_pieces: usize,
}

/// One key space of a flush: the keys that start with `prefix`, built into
/// runs of their own, without the prefix, and split at `cuts`.
#[derive(Clone, Copy, Debug)]
pub(crate) struct FlushSpace<'a> {
    pub(crate) prefix: &'a [u8],
    pub(crate) cuts: &'a FlushCuts,
}

/// What a flush built for one [`FlushSpace`].
#[derive(Debug, Default)]
pub(crate) struct SpaceRuns {
    /// In key order.
    pub(crate) runs: Vec<Run<Vec<u8>>>,
    /// The input of the space's records in every table, counting a key once
    /// per table that holds it, as run lengths count input.
    pub(crate) logical: usize,
}

/// The space `key` belongs to and the key without its prefix. `spaces` are
/// sorted by prefix, and no prefix starts another, so the only candidate is
/// the last prefix at or below `key`. `current` remembers the last space
/// found, which ascending keys mostly stay in.
fn locate<'k>(
    spaces: &[FlushSpace<'_>],
    current: &mut Option<usize>,
    key: &'k [u8],
) -> Option<(usize, &'k [u8])> {
    if let Some(at) = *current
        && let Some(rest) = key.strip_prefix(spaces[at].prefix)
    {
        return Some((at, rest));
    }
    let at = spaces
        .partition_point(|space| space.prefix <= key)
        .checked_sub(1)?;
    let rest = key.strip_prefix(spaces[at].prefix)?;
    *current = Some(at);
    Some((at, rest))
}

/// Builds runs from several frozen tables as if they were one, a separate
/// set for each of `spaces`, splitting them as [`FrozenMemtable::flush_with`]
/// does and at each space's cuts. `tables` is ordered oldest first, and
/// where tables share a key the newest update is the one written. Records
/// in no space are left out. The result is in the order of `spaces`, which
/// are sorted by prefix, with prefixes of which none starts another.
pub(crate) fn flush_merged(
    tables: &[&FrozenMemtable],
    target_run_len: usize,
    codec: Codec,
    target_block_compressed: usize,
    spaces: &[FlushSpace<'_>],
    pool: &ChunkPool,
) -> Vec<SpaceRuns> {
    let target = target_run_len.min(MAX_TARGET_RUN_LEN);
    let mut out: Vec<SpaceRuns> = spaces.iter().map(|_| SpaceRuns::default()).collect();
    // Input still to come in each space, for the rule that a cut leaves a
    // piece after it: exact for one table, an overestimate where tables
    // share keys.
    let rests: Vec<usize> = if spaces.iter().all(|space| space.cuts.at.is_empty()) {
        vec![0; spaces.len()]
    } else if let [space] = spaces {
        vec![
            tables
                .iter()
                .map(|table| {
                    (table.memory().payload + table.len() * RECORD_INPUT_OVERHEAD)
                        .saturating_sub(table.len() * space.prefix.len())
                })
                .sum(),
        ]
    } else {
        let mut rests = vec![0; spaces.len()];
        for table in tables {
            let mut current = None;
            for record in table.iter() {
                if let Some((at, key)) = locate(spaces, &mut current, record.key)
                    && !spaces[at].cuts.at.is_empty()
                {
                    rests[at] += crate::run::input_len(key, record.value);
                }
            }
        }
        rests
    };
    let mut heads: Vec<_> = tables.iter().map(|table| table.iter().peekable()).collect();
    let mut current = None;
    // The space being built, its next cut, the input in the run being
    // built, in the piece being built and still to come, and the runs
    // begun in this target length.
    let mut space = None::<usize>;
    let (mut next_cut, mut input, mut piece, mut rest, mut pieces) = (0, 0, 0, 0, 1);
    let mut builder = None::<RunBuilder>;
    loop {
        // The smallest key, and among tables holding it the newest.
        let mut newest: Option<(usize, &[u8])> = None;
        for (at, head) in heads.iter_mut().enumerate() {
            if let Some(record) = head.peek()
                && newest.is_none_or(|(_, smallest)| record.key <= smallest)
            {
                newest = Some((at, record.key));
            }
        }
        let Some((winner, stored)) = newest else {
            break;
        };
        let record = heads[winner].next().expect("the winner has a head");
        let located = locate(spaces, &mut current, stored);
        for head in &mut heads {
            if let Some(older) = head.next_if(|other| other.key == stored)
                && let Some((at, key)) = located
            {
                out[at].logical += crate::run::input_len(key, older.value);
            }
        }
        let Some((at, key)) = located else {
            continue;
        };
        let len = crate::run::input_len(key, record.value);
        out[at].logical += len;
        if space != Some(at) {
            if let Some(done) = space {
                out[done]
                    .runs
                    .extend(builder.take().map(RunBuilder::finish));
            }
            space = Some(at);
            (next_cut, input, piece, rest, pieces) = (0, 0, 0, rests[at], 1);
        }
        let cuts = spaces[at].cuts;
        let mut cut = false;
        while cuts.at.get(next_cut).is_some_and(|at| at.as_slice() <= key) {
            next_cut += 1;
            cut = true;
        }
        if cut
            && builder.is_some()
            && piece >= cuts.min_piece
            && rest >= cuts.min_piece
            && pieces < cuts.max_pieces
        {
            out[at].runs.extend(builder.take().map(RunBuilder::finish));
            piece = 0;
            pieces += 1;
        }
        builder
            .get_or_insert_with(|| RunBuilder::in_pool(codec, target_block_compressed, pool))
            .add(key, record.seq, record.value)
            .expect(FLUSHABLE);
        (input, piece, rest) = (input + len, piece + len, rest.saturating_sub(len));
        if input >= target {
            // A full run starts the next target length with its own pieces.
            out[at].runs.extend(builder.take().map(RunBuilder::finish));
            (input, piece, pieces) = (0, 0, 1);
        }
    }
    if let Some(done) = space {
        out[done].runs.extend(builder.map(RunBuilder::finish));
    }
    out
}

/// Iterates a table's latest updates in ascending key order. Holds the path
/// from the root inline, so creating one allocates nothing.
pub struct Records<'a> {
    arena: &'a Arena,
    /// Each branch above the leaf, root first, and the child taken.
    path: [Option<(&'a Branch, usize)>; MAX_DEPTH],
    depth: usize,
    leaf: &'a Leaf,
    at: usize,
}

impl<'a> Iterator for Records<'a> {
    type Item = Record<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        while self.at == self.leaf.slots.len() {
            // Climb to the nearest branch with a child to the right, then
            // descend to that child's first leaf.
            loop {
                let (branch, child) = self.path[self.depth.checked_sub(1)?]?;
                if child + 1 < branch.children.len() {
                    self.path[self.depth - 1] = Some((branch, child + 1));
                    let mut node = &*branch.children[child + 1];
                    while let Node::Branch(branch) = node {
                        self.path[self.depth] = Some((branch, 0));
                        self.depth += 1;
                        node = &branch.children[0];
                    }
                    let Node::Leaf(leaf) = node else {
                        unreachable!("the loop above stops at a leaf")
                    };
                    self.leaf = leaf;
                    self.at = 0;
                    break;
                }
                self.depth -= 1;
            }
        }
        let slot = &self.leaf.slots[self.at];
        self.at += 1;
        Some(Record {
            key: slot.key(self.arena),
            seq: slot.seq,
            value: slot.value(self.arena),
        })
    }
}

impl fmt::Debug for Records<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Records").finish_non_exhaustive()
    }
}

/// The runs of a [`FrozenMemtable`], built as they are requested.
#[cfg(test)]
pub struct Flush<'a> {
    pool: ChunkPool,
    records: Records<'a>,
    target: usize,
    codec: Codec,
    target_block_compressed: usize,
}

#[cfg(test)]
impl Iterator for Flush<'_> {
    type Item = Run<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.records.next()?;
        let mut builder = RunBuilder::in_pool(self.codec, self.target_block_compressed, &self.pool);
        let mut input = 0;
        loop {
            builder
                .add(record.key, record.seq, record.value)
                .expect(FLUSHABLE);
            input += crate::run::input_len(record.key, record.value);
            if input >= self.target {
                break;
            }
            let Some(next) = self.records.next() else {
                break;
            };
            record = next;
        }
        Some(builder.finish())
    }
}

#[cfg(test)]
impl fmt::Debug for Flush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flush")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}
