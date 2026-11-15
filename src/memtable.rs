//! Bounded mutable storage and its conversion into immutable runs.
//!
//! A [`Memtable`] accepts updates. [`Memtable::freeze`] consumes it and returns
//! a [`FrozenMemtable`], which has no way to change, and
//! [`FrozenMemtable::flush`] builds compressed runs from it.
//!
//! # Versions
//!
//! A table is a copy-on-write B+ tree: its nodes, keys and values are shared
//! through reference counts. [`Memtable::snapshot`] returns the current tree
//! as a [`FrozenMemtable`] in constant time, sharing every node. Later writes
//! copy the nodes on their path that a snapshot still shares, so a snapshot
//! never changes, and a node or value is freed with the last tree holding it.
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
//! table, and above the `after` given to [`Memtable::new`], so that a table can
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
//! An entry is charged the length of its key, the length of its value and
//! [`ENTRY_OVERHEAD`]. Replacing a value charges the difference in value
//! length.
//!
//! A table is also charged what it has stopped using but its snapshots still
//! share: the nodes an update copied and the values it replaced, from the
//! oldest snapshot still alive onwards. This is [`MemoryUsage::pinned`]. It
//! counts each copied node and replaced value once, at its allocated size, as
//! it leaves the table while shared, and nothing else leaves a table while
//! shared, so nothing a live snapshot holds apart from the table is missed.
//! It is an upper estimate, since a node copied after one snapshot may be
//! shared only by snapshots already dropped, and it falls to zero once every
//! snapshot of the table is dropped. An update is admitted only with the bytes it pins, so
//! a replacement that does not grow its entry is always admitted while no
//! snapshot shares its path, and the budget bounds the table together with the
//! history its snapshots hold.
//!
//! The charge is an estimate of what the table allocates, not a limit on
//! resident memory: allocator rounding and headers are not charged, and
//! replacing a value briefly holds both values.

use std::collections::VecDeque;
use std::fmt;
use std::sync::{Arc, Weak};

use crate::block::{MAX_KEY_LEN, MAX_VALUE_LEN, Record, Value, Version};
use crate::run::{Codec, RECORD_INPUT_OVERHEAD, Run, RunBuilder, TARGET_BLOCK_COMPRESSED};

#[cfg(test)]
mod tests;

/// Bytes charged per entry beyond its key and value.
///
/// This covers, on 64-bit targets, the entry's slot in a leaf at the half
/// occupancy every leaf off the tree's right edge keeps (about 66 bytes), the
/// header of the allocation holding its key and value (16), and its share of
/// branch nodes, with room to spare. Allocator rounding is not included, so
/// this is an estimate.
pub const ENTRY_OVERHEAD: usize = 128;

/// Input per output run suggested for [`FrozenMemtable::flush`].
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

/// The latest update of one key in one tree.
#[derive(Clone)]
struct Slot {
    seq: u64,
    /// The key followed by the value, in one allocation shared with the trees
    /// that still hold this update.
    data: Arc<[u8]>,
    /// At most [`MAX_KEY_LEN`], which fits.
    key_len: u32,
    tombstone: bool,
}

impl Slot {
    fn new(key: &[u8], seq: u64, value: Option<&[u8]>) -> Self {
        let tombstone = value.is_none();
        let value = value.unwrap_or_default();
        // One allocation, filled at memory speed: zeroing compiles to a fill,
        // where copying through a chained iterator goes byte by byte.
        let mut data: Arc<[u8]> = std::iter::repeat_n(0, key.len() + value.len()).collect();
        let buffer = Arc::get_mut(&mut data).expect("a new allocation is unshared");
        buffer[..key.len()].copy_from_slice(key);
        buffer[key.len()..].copy_from_slice(value);
        Self {
            seq,
            data,
            key_len: u32::try_from(key.len()).expect("keys are within MAX_KEY_LEN"),
            tombstone,
        }
    }

    fn key(&self) -> &[u8] {
        &self.data[..self.key_len as usize]
    }

    fn value(&self) -> Value<'_> {
        if self.tombstone {
            Value::Tombstone
        } else {
            Value::Bytes(&self.data[self.key_len as usize..])
        }
    }

    fn value_len(&self) -> usize {
        self.data.len() - self.key_len as usize
    }

    /// What this update's allocation holds: its counts and data, padded to
    /// the alignment of the counts.
    fn bytes(&self) -> usize {
        (ARC_HEADER + self.data.len()).next_multiple_of(align_of::<usize>())
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
    fn search(&self, key: &[u8]) -> Result<usize, usize> {
        self.slots.binary_search_by(|slot| slot.key().cmp(key))
    }

    /// Splits an overfull leaf. Appending at the right edge leaves the left
    /// leaf full, so ascending inserts fill their leaves.
    fn split(&mut self, append: bool) -> (Arc<[u8]>, Arc<Node>) {
        let at = if append {
            NODE_CAPACITY
        } else {
            self.slots.len() / 2
        };
        let right = Leaf {
            slots: node_buffer(&self.slots[at..]),
        };
        self.slots.truncate(at);
        (Arc::from(right.slots[0].key()), Arc::new(Node::Leaf(right)))
    }
}

impl Branch {
    /// The child whose range holds `key`.
    fn route(&self, key: &[u8]) -> usize {
        self.separators
            .partition_point(|separator| **separator <= *key)
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
) -> Option<(Arc<[u8]>, Arc<Node>)> {
    match unshare(node, detached) {
        Node::Leaf(leaf) => match path.leaf {
            Ok(at) => {
                let old = std::mem::replace(&mut leaf.slots[at], slot);
                // Another leaf holds it, so replacing it frees nothing.
                if Arc::strong_count(&old.data) > 1 {
                    *detached += old.bytes();
                }
                None
            }
            Err(at) => {
                if leaf.slots.capacity() <= NODE_CAPACITY {
                    leaf.slots
                        .reserve_exact(NODE_CAPACITY + 1 - leaf.slots.len());
                }
                leaf.slots.insert(at, slot);
                (leaf.slots.len() > NODE_CAPACITY).then(|| leaf.split(edge && at == NODE_CAPACITY))
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
            )?;
            branch.separators.insert(at, separator);
            branch.children.insert(at + 1, right);
            let full = branch.children.len() > NODE_CAPACITY;
            full.then(|| branch.split(edge && at + 1 == NODE_CAPACITY))
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
    family: Arc<()>,
}

/// An update [`Table::admit`] accepted, and where it goes.
struct Admitted<'v> {
    path: Path,
    new: bool,
    grows: usize,
    released: usize,
    bytes: Option<&'v [u8]>,
}

impl Table {
    fn new(budget: usize, last_seq: Option<u64>) -> Self {
        Self {
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
    /// would detach: every node from the first shared one down, and the old
    /// update if another tree holds it or will once its leaf is copied.
    /// Others can only drop references, so the write itself detaches no more
    /// than this.
    fn plan(&self, key: &[u8]) -> (Path, Option<&Slot>, usize) {
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
                    let found = leaf.search(key);
                    let slot = found.ok().map(|at| &leaf.slots[at]);
                    if let Some(slot) = slot
                        && (shared || Arc::strong_count(&slot.data) > 1)
                    {
                        detach += slot.bytes();
                    }
                    let path = Path {
                        routes,
                        leaf: found,
                    };
                    return (path, slot, detach);
                }
            }
        }
    }

    fn write(&mut self, key: &[u8], seq: u64, value: Value<'_>) -> Result<(), TableError> {
        let admitted = self.admit(key, seq, value)?;
        self.insert(key, seq, admitted);
        Ok(())
    }

    fn insert(&mut self, key: &[u8], seq: u64, admitted: Admitted<'_>) {
        let Admitted {
            path,
            new,
            grows,
            released,
            bytes,
        } = admitted;
        let replacement = Slot::new(key, seq, bytes);
        if let Some((separator, right)) = insert(
            &mut self.root,
            &path,
            0,
            replacement,
            true,
            &mut self.detached,
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

    /// Every check of [`Self::write`], changing nothing but forgetting dead
    /// snapshots.
    fn admit<'v>(
        &mut self,
        key: &[u8],
        seq: u64,
        value: Value<'v>,
    ) -> Result<Admitted<'v>, TableError> {
        let bytes = match value {
            Value::Bytes(bytes) => Some(bytes),
            Value::Tombstone => None,
        };
        let value_len = bytes.map_or(0, <[u8]>::len);
        if key.len() > MAX_KEY_LEN {
            return Err(TableError::KeyTooLong { len: key.len() });
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
        let (path, slot, detach) = self.plan(key);
        let (grows, released) = match slot {
            Some(slot) => (value_len, slot.value_len()),
            None => (charge, 0),
        };
        let needed = grows + detach;
        let available = self
            .budget
            .saturating_add(released)
            .saturating_sub(self.live + self.pinned());
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

    fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        let mut node = &*self.root;
        loop {
            match node {
                Node::Branch(branch) => node = &branch.children[branch.route(key)],
                Node::Leaf(leaf) => {
                    let slot = &leaf.slots[leaf.search(key).ok()?];
                    return Some(Version {
                        seq: slot.seq,
                        value: slot.value(),
                    });
                }
            }
        }
    }

    fn iter_from(&self, start: &[u8]) -> Records<'_> {
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
                    let at = leaf.slots.partition_point(|slot| slot.key() < start);
                    return Records {
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
            charged: self.live + pinned,
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
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct MemoryUsage {
    /// Key and value bytes of the table's entries.
    pub payload: usize,
    /// Bytes charged against the budget, pinned bytes included.
    pub charged: usize,
    /// The part of `charged` that only snapshots of the table still need:
    /// nodes and values its writes copied or replaced since the oldest live
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
    /// one once its snapshots are dropped.
    BudgetExceeded {
        /// Additional bytes the update needs, including those it would pin.
        needed: usize,
        /// Bytes available to it, including any it would release.
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
    pub fn new(budget: usize, after: Option<u64>) -> Self {
        Self(Table::new(budget, after))
    }

    /// Sets `key` to `value` at sequence number `seq`.
    ///
    /// Key and value limits are checked first, then the sequence number, then
    /// the budget. A rejected update changes nothing.
    pub fn put(&mut self, key: &[u8], seq: u64, value: &[u8]) -> Result<(), TableError> {
        self.0.write(key, seq, Value::Bytes(value))
    }

    /// Stores a tombstone for `key` at sequence number `seq`, under the same
    /// rules as [`put`](Self::put).
    pub fn delete(&mut self, key: &[u8], seq: u64) -> Result<(), TableError> {
        self.0.write(key, seq, Value::Tombstone)
    }

    /// Returns the latest update of `key`, which may be a tombstone, or `None`
    /// if the table holds nothing for it.
    pub fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.0.get(key)
    }

    /// Returns each key's latest update, in ascending key order.
    pub fn iter(&self) -> Records<'_> {
        self.0.iter_from(&[])
    }

    /// Returns each update whose key is at or above `start`, in ascending key
    /// order.
    pub fn iter_from(&self, start: &[u8]) -> Records<'_> {
        self.0.iter_from(start)
    }

    /// Returns the number of keys held, including tombstones.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn len(&self) -> usize {
        self.0.len
    }

    /// Returns whether the table holds no keys.
    pub fn is_empty(&self) -> bool {
        self.0.len == 0
    }

    /// Returns the sequence number every update must exceed, if any.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
    pub fn last_seq(&self) -> Option<u64> {
        self.0.last_seq
    }

    /// Returns the table's memory accounting.
    pub fn memory(&self) -> MemoryUsage {
        self.0.memory()
    }

    /// A conservative upper bound on the charge of one additional update,
    /// including nodes and records a live snapshot could pin.
    pub(crate) fn write_charge(&self, key: &[u8], value: Value<'_>) -> usize {
        let bytes = match value {
            Value::Bytes(bytes) => bytes.len(),
            Value::Tombstone => 0,
        };
        key.len()
            .saturating_add(bytes)
            .saturating_add(ENTRY_OVERHEAD)
            .saturating_add(self.0.plan(key).2)
    }

    /// Returns the table as it is now, in constant time and without copying,
    /// leaving it writable. Later updates do not change the snapshot; what
    /// they copy or replace for it is charged to this table as
    /// [`pinned`](MemoryUsage::pinned) until the snapshot is dropped.
    pub fn snapshot(&mut self) -> FrozenMemtable {
        self.0.views.retain(View::is_alive);
        let pin = Arc::new(());
        self.0.views.push_back(View {
            pin: Arc::downgrade(&pin),
            mark: self.0.detached,
            live: self.0.live,
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

/// A memtable that can no longer change: a frozen table, or a snapshot of a
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
    pub fn get(&self, key: &[u8]) -> Option<Version<'_>> {
        self.table.get(key)
    }

    /// Returns each key's latest update, in ascending key order.
    pub fn iter(&self) -> Records<'_> {
        self.table.iter_from(&[])
    }

    /// Returns each update whose key is at or above `start`, in ascending key
    /// order.
    pub fn iter_from(&self, start: &[u8]) -> Records<'_> {
        self.table.iter_from(start)
    }

    /// Returns the number of keys held, including tombstones.
    pub fn len(&self) -> usize {
        self.table.len
    }

    /// Returns whether the table holds no keys.
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
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
    #[cfg_attr(
        not(test),
        expect(dead_code, reason = "tested component API the engine does not use")
    )]
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
    pub fn flush_with(
        &self,
        target_run_len: usize,
        codec: Codec,
        target_block_compressed: usize,
    ) -> Flush<'_> {
        Flush {
            records: self.iter(),
            target: target_run_len.min(MAX_TARGET_RUN_LEN),
            codec,
            target_block_compressed,
        }
    }
}

fn input_len(record: &Record<'_>) -> usize {
    let value_len = match record.value {
        Value::Bytes(bytes) => bytes.len(),
        Value::Tombstone => 0,
    };
    record.key.len() + value_len + RECORD_INPUT_OVERHEAD
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

/// Finishes a flush's run: its last block, index and filter.
fn finish(builder: RunBuilder) -> Run<Vec<u8>> {
    builder.finish()
}

/// Builds runs from several frozen tables as if they were one, splitting
/// them as [`FrozenMemtable::flush_with`] does and at `cuts`. `tables` is
/// ordered oldest first, and where tables share a key the newest update is
/// the one written.
pub(crate) fn flush_merged(
    tables: &[&FrozenMemtable],
    target_run_len: usize,
    codec: Codec,
    target_block_compressed: usize,
    cuts: &FlushCuts,
) -> Vec<Run<Vec<u8>>> {
    if let ([table], []) = (tables, cuts.at.as_slice()) {
        return table
            .flush_with(target_run_len, codec, target_block_compressed)
            .collect();
    }
    let target = target_run_len.min(MAX_TARGET_RUN_LEN);
    // Input still to come, for the rule that a cut leaves a piece after it:
    // exact for one table, an overestimate where tables share keys.
    let mut rest: usize = tables
        .iter()
        .map(|table| table.memory().payload + table.len() * RECORD_INPUT_OVERHEAD)
        .sum();
    let mut next_cut = cuts.at.iter().peekable();
    // Input in the run being built, and runs begun in this target length.
    let (mut piece, mut pieces) = (0, 1);
    let mut heads: Vec<_> = tables.iter().map(|table| table.iter().peekable()).collect();
    let (mut runs, mut builder, mut input) = (Vec::new(), None::<RunBuilder>, 0);
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
        let Some((winner, key)) = newest else {
            break;
        };
        let record = heads[winner].next().expect("the winner has a head");
        for head in &mut heads {
            head.next_if(|other| other.key == key);
        }
        let mut cut = false;
        while next_cut.next_if(|at| at.as_slice() <= key).is_some() {
            cut = true;
        }
        if cut
            && builder.is_some()
            && piece >= cuts.min_piece
            && rest >= cuts.min_piece
            && pieces < cuts.max_pieces
        {
            runs.extend(builder.take().map(finish));
            piece = 0;
            pieces += 1;
        }
        builder
            .get_or_insert_with(|| {
                RunBuilder::with_target_block_compressed(codec, target_block_compressed)
            })
            .add(key, record.seq, record.value)
            .expect(FLUSHABLE);
        let len = input_len(&record);
        (input, piece, rest) = (input + len, piece + len, rest.saturating_sub(len));
        if input >= target {
            // A full run starts the next target length with its own pieces.
            runs.extend(builder.take().map(finish));
            (input, piece, pieces) = (0, 0, 1);
        }
    }
    runs.extend(builder.map(finish));
    runs
}

/// Iterates a table's latest updates in ascending key order. Holds the path
/// from the root inline, so creating one allocates nothing.
pub struct Records<'a> {
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
            key: slot.key(),
            seq: slot.seq,
            value: slot.value(),
        })
    }
}

impl fmt::Debug for Records<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Records").finish_non_exhaustive()
    }
}

/// The runs of a [`FrozenMemtable`], built as they are requested.
pub struct Flush<'a> {
    records: Records<'a>,
    target: usize,
    codec: Codec,
    target_block_compressed: usize,
}

impl Iterator for Flush<'_> {
    type Item = Run<Vec<u8>>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut record = self.records.next()?;
        let mut builder =
            RunBuilder::with_target_block_compressed(self.codec, self.target_block_compressed);
        let mut input = 0;
        loop {
            builder
                .add(record.key, record.seq, record.value)
                .expect(FLUSHABLE);
            input += input_len(&record);
            if input >= self.target {
                break;
            }
            let Some(next) = self.records.next() else {
                break;
            };
            record = next;
        }
        Some(finish(builder))
    }
}

impl fmt::Debug for Flush<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Flush")
            .field("target", &self.target)
            .finish_non_exhaustive()
    }
}
