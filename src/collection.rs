//! The live [`Collection`] — a realtime document store backed by ce-coord [`Merged`].
//!
//! A `Collection` wraps a [`Merged<DbMachine>`]: each device proposes [`DocOp`]s into its own writer
//! log, ce-coord replicates and merges them across the mesh, and reads fold the merged op-set into the
//! materialized document map. Every device is a writer **and** a reader, so concurrent offline edits
//! converge with no leader (the multi-writer Firestore story).
//!
//! [`Merged`]: ce_coord::Merged
//! [`Merged<DbMachine>`]: ce_coord::Merged
//!
//! ## Materialized cache
//!
//! `Merged::read` folds the entire op union on every call (O(total history)). To keep hot reads cheap
//! this `Collection` keeps a **materialized snapshot cache** keyed by the merged op-count: a read
//! re-folds only when the op-count has advanced since the last fold, so repeated reads at the same
//! version are O(1). The cache is invalidated automatically by the change-watch counter.
//!
//! ## Realtime (push-based)
//!
//! [`Collection::watch`] returns a [`tokio::sync::watch::Receiver`] that fires whenever the merged
//! op-set grows — a local write *or* any peer's op arriving over the mesh. ce-coord delivers peer ops
//! into background logs but only *folds* them on `pull()`, so a [`Collection`] spawns a lightweight
//! **background poller** that calls `pull()` on an interval, waking watchers on peer changes with no
//! external loop. [`Collection::next_change`] / [`Collection::changes`] turn that into materialized
//! snapshots and per-document [`DocChange`] deltas.
//!
//! ## Lamport clock
//!
//! Each write is stamped with `(lamport, writer_id)` in a single critical section that also advances
//! past every op key observed in the merged set, so a freshly written op always sorts after everything
//! this device has seen — last-writer-wins with the intuitive "most recent edit wins ties".

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use anyhow::Result;
use ce_coord::{Coord, Merged};
use serde_json::{Number, Value};
use tokio::sync::watch;

use crate::doc::{DbMachine, DocMeta, DocOp, Document, FieldOp, OpKey, OpKind};
use crate::limits::Limits;
use crate::query::Query;

/// A materialized view of the collection at one point in time: `doc_id -> Document`, plus the merged
/// op-count that produced it (a monotonically advancing version a UI can show / dedupe on).
#[derive(Clone, Debug, Default)]
pub struct Snapshot {
    /// All live (non-tombstoned) documents, sorted by id.
    pub docs: std::collections::BTreeMap<String, Document>,
    /// Number of distinct ops merged so far across all writers — a coarse collection version.
    pub op_count: usize,
}

impl Snapshot {
    /// One document by id.
    pub fn get(&self, doc_id: &str) -> Option<&Document> {
        self.docs.get(doc_id)
    }

    /// Run a query over this snapshot, returning matching `(doc_id, Document)` pairs.
    pub fn query(&self, q: &Query) -> Vec<(String, Document)> {
        q.run(self.docs.iter().map(|(k, v)| (k.clone(), v.clone())))
    }

    /// Number of live documents.
    pub fn len(&self) -> usize {
        self.docs.len()
    }

    /// True if there are no live documents.
    pub fn is_empty(&self) -> bool {
        self.docs.is_empty()
    }

    /// Diff this snapshot against an earlier one, producing per-document [`DocChange`] deltas
    /// (Firestore `DocumentChange`: added / modified / removed). Useful for `onSnapshot` listeners.
    pub fn diff(&self, prev: &Snapshot) -> Vec<DocChange> {
        let mut changes = Vec::new();
        for (id, doc) in &self.docs {
            match prev.docs.get(id) {
                None => changes.push(DocChange {
                    doc_id: id.clone(),
                    kind: ChangeKind::Added,
                    doc: Some(doc.clone()),
                }),
                Some(old) if old != doc => changes.push(DocChange {
                    doc_id: id.clone(),
                    kind: ChangeKind::Modified,
                    doc: Some(doc.clone()),
                }),
                Some(_) => {}
            }
        }
        for id in prev.docs.keys() {
            if !self.docs.contains_key(id) {
                changes.push(DocChange {
                    doc_id: id.clone(),
                    kind: ChangeKind::Removed,
                    doc: None,
                });
            }
        }
        changes
    }
}

/// The kind of a per-document change in a snapshot diff (Firestore `DocumentChange.Type`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ChangeKind {
    /// The document is new in the later snapshot.
    Added,
    /// The document exists in both snapshots but its contents changed.
    Modified,
    /// The document was present before and is now gone (deleted/tombstoned).
    Removed,
}

/// A single per-document change delta between two snapshots.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocChange {
    /// The affected document id.
    pub doc_id: String,
    /// Whether it was added, modified, or removed.
    pub kind: ChangeKind,
    /// The new document contents (`None` for [`ChangeKind::Removed`]).
    pub doc: Option<Document>,
}

/// Cached materialized state, invalidated when the merged op-count advances.
#[derive(Default)]
struct Cache {
    /// The op-count the cached snapshot was folded at (`None` = never folded).
    version: Option<usize>,
    snapshot: Snapshot,
}

/// A live, realtime, multi-writer document collection.
pub struct Collection {
    merged: Arc<Merged<DbMachine>>,
    self_id: String,
    /// Monotonic Lamport counter for this device's writes (guarded by a single critical section).
    lamport: Mutex<u64>,
    name: String,
    /// Hierarchical path of this (sub)collection, e.g. `users/ada/posts`. Equals `name` for a root.
    path: String,
    limits: Limits,
    /// Materialized snapshot cache (see module docs).
    cache: Mutex<Cache>,
    /// Handle to the background realtime poller, aborted on drop.
    poller: Mutex<Option<tokio::task::JoinHandle<()>>>,
}

impl Drop for Collection {
    fn drop(&mut self) {
        if let Ok(mut g) = self.poller.lock()
            && let Some(h) = g.take()
        {
            h.abort();
        }
    }
}

impl Collection {
    /// Open root collection `name` on this device. `self_id` is this device's NodeId hex; `peer_ids`
    /// are the *other* member devices whose logs to follow. Uses [`Limits::default`].
    pub async fn open(coord: &Coord, name: &str, peer_ids: &[String]) -> Result<Collection> {
        Self::open_with_limits(coord, name, peer_ids, Limits::default()).await
    }

    /// Open with explicit [`Limits`] (size/DoS ceilings) and an explicit hierarchical path. The
    /// `name` is the ce-coord log/topic name (so subcollections get distinct underlying logs).
    pub async fn open_with_limits(
        coord: &Coord,
        name: &str,
        peer_ids: &[String],
        limits: Limits,
    ) -> Result<Collection> {
        limits.check_collection(name)?;
        if limits.max_writers != 0 && peer_ids.len() > limits.max_writers {
            anyhow::bail!(
                "too many peers ({}), limit is {}",
                peer_ids.len(),
                limits.max_writers
            );
        }
        let self_id = coord.node_id().to_string();
        let merged = Arc::new(Merged::<DbMachine>::open(coord, name, &self_id, peer_ids).await?);
        let coll = Collection {
            merged,
            self_id,
            lamport: Mutex::new(0),
            name: name.to_string(),
            path: name.to_string(),
            limits,
            cache: Mutex::new(Cache::default()),
            poller: Mutex::new(None),
        };
        coll.advance_clock_past_merged();
        Ok(coll)
    }

    /// Convenience: open against the local node, discovering this device's id from the [`Coord`].
    pub async fn open_local(coord: &Coord, name: &str, peer_ids: &[String]) -> Result<Collection> {
        Self::open(coord, name, peer_ids).await
    }

    /// Open a **subcollection** of a document in this collection, e.g. `users/ada/posts`. The
    /// subcollection is a fully independent [`Merged`] log addressed by the hierarchical path, so it
    /// converges and is gated separately. Follows the same `peer_ids` as the parent by default.
    ///
    /// Hierarchical addressing follows Firestore: `collection/doc/subcollection[/doc/subcollection…]`.
    pub async fn subcollection(
        &self,
        coord: &Coord,
        parent_doc_id: &str,
        sub_name: &str,
        peer_ids: &[String],
    ) -> Result<Collection> {
        self.limits.check_doc_id(parent_doc_id)?;
        self.limits.check_collection(sub_name)?;
        let path = format!("{}/{}/{}", self.path, parent_doc_id, sub_name);
        // The ce-coord log name is the full path; deterministic across devices.
        let coll =
            Collection::open_with_limits(coord, &path, peer_ids, self.limits.clone()).await?;
        Ok(coll)
    }

    /// Start following another device's writer log (a newly added collaborator). Idempotent.
    pub async fn add_writer(&self, peer_id: &str) -> Result<()> {
        self.merged.add_writer(peer_id).await
    }

    /// The collection's underlying log name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// The collection's hierarchical path (equals [`name`](Self::name) for a root collection).
    pub fn path(&self) -> &str {
        &self.path
    }

    /// This device's NodeId hex.
    pub fn writer_id(&self) -> &str {
        &self.self_id
    }

    /// The configured [`Limits`] for this collection.
    pub fn limits(&self) -> &Limits {
        &self.limits
    }

    // --- writes ---

    /// Set (replace) a whole document. Last-writer-wins on the whole document. Validates the doc id
    /// and document against the configured [`Limits`].
    pub async fn set(&self, doc_id: &str, doc: Document) -> Result<()> {
        self.limits.check_doc_id(doc_id)?;
        self.limits.check_document(&doc)?;
        self.guard_op_count()?;
        self.propose(doc_id, OpKind::Set(doc)).await
    }

    /// Set a document from any serializable value (must serialize to a JSON object).
    pub async fn set_value<T: serde::Serialize>(&self, doc_id: &str, value: &T) -> Result<()> {
        let v = serde_json::to_value(value)?;
        let obj = v
            .as_object()
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("document must serialize to a JSON object"))?;
        self.set(doc_id, obj).await
    }

    /// Patch (field-level merge) a document. Each field is last-writer-wins independently; a `null`
    /// field value deletes that field; nested objects deep-merge. Concurrent patches to different
    /// fields all survive.
    pub async fn patch(&self, doc_id: &str, fields: Document) -> Result<()> {
        self.limits.check_doc_id(doc_id)?;
        self.limits.check_document(&fields)?;
        self.guard_op_count()?;
        self.propose(doc_id, OpKind::Patch(fields)).await
    }

    /// Set a single field on a document (a one-field [`patch`](Self::patch)).
    pub async fn set_field(&self, doc_id: &str, field: &str, value: Value) -> Result<()> {
        let mut obj = Document::new();
        obj.insert(field.to_string(), value);
        self.patch(doc_id, obj).await
    }

    /// Apply atomic [`FieldOp`]s to a document (CRDT-friendly increment / array union/remove).
    /// See [`Collection::increment`], [`array_union`](Self::array_union),
    /// [`array_remove`](Self::array_remove) for the common cases.
    pub async fn update(&self, doc_id: &str, ops: BTreeMap<String, FieldOp>) -> Result<()> {
        self.limits.check_doc_id(doc_id)?;
        if self.limits.max_fields != 0 && ops.len() > self.limits.max_fields {
            anyhow::bail!(
                "update touches {} fields, limit is {}",
                ops.len(),
                self.limits.max_fields
            );
        }
        for k in ops.keys() {
            if k.is_empty() {
                anyhow::bail!("update field names must be non-empty");
            }
            if self.limits.max_field_name_bytes != 0 && k.len() > self.limits.max_field_name_bytes {
                anyhow::bail!("update field name too long");
            }
        }
        self.guard_op_count()?;
        self.propose(doc_id, OpKind::Update(ops)).await
    }

    /// Atomically add `delta` to a numeric field (Firestore `FieldValue.increment`).
    pub async fn increment(&self, doc_id: &str, field: &str, delta: i64) -> Result<()> {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::Increment(Number::from(delta)));
        self.update(doc_id, ops).await
    }

    /// Atomically union elements into an array field (Firestore `FieldValue.arrayUnion`).
    pub async fn array_union(&self, doc_id: &str, field: &str, items: Vec<Value>) -> Result<()> {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::ArrayUnion(items));
        self.update(doc_id, ops).await
    }

    /// Atomically remove elements from an array field (Firestore `FieldValue.arrayRemove`).
    pub async fn array_remove(&self, doc_id: &str, field: &str, items: Vec<Value>) -> Result<()> {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::ArrayRemove(items));
        self.update(doc_id, ops).await
    }

    /// Patch a field with a server timestamp (this device's wall clock in unix seconds), resolved to
    /// a literal at write time — exactly how Firestore persists `serverTimestamp()`.
    pub async fn set_server_timestamp(&self, doc_id: &str, field: &str) -> Result<()> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        self.set_field(doc_id, field, Value::Number(Number::from(now)))
            .await
    }

    /// Delete (tombstone) a document. A later set/patch resurrects it.
    pub async fn delete(&self, doc_id: &str) -> Result<()> {
        self.limits.check_doc_id(doc_id)?;
        self.guard_op_count()?;
        self.propose(doc_id, OpKind::Delete).await
    }

    /// Begin an atomic batch of writes (Firestore `WriteBatch`). Stage `set`/`patch`/`delete`/atomic
    /// ops, then [`WriteBatch::commit`] proposes them to this device's writer log under one
    /// contiguous Lamport range, so a follower never observes a partial batch out of order.
    pub fn batch(&self) -> crate::batch::WriteBatch<'_> {
        crate::batch::WriteBatch::new(self)
    }

    // --- reads (cache-backed) ---

    /// Read one document by id (cache-backed).
    pub fn get(&self, doc_id: &str) -> Option<Document> {
        self.cached_snapshot().docs.get(doc_id).cloned()
    }

    /// Per-document metadata (create/update op keys) — the Firestore version/createTime/updateTime
    /// substrate. Folds the latest state. `None` if the doc was never touched.
    pub fn meta(&self, doc_id: &str) -> Option<DocMeta> {
        self.merged.read(|m| m.meta(doc_id))
    }

    /// A consistent snapshot of the whole collection (cache-backed).
    pub fn snapshot(&self) -> Snapshot {
        self.cached_snapshot()
    }

    /// Run a query and return matching `(doc_id, Document)` pairs (cache-backed).
    pub fn query(&self, q: &Query) -> Vec<(String, Document)> {
        let snap = self.cached_snapshot();
        q.run(snap.docs.iter().map(|(k, v)| (k.clone(), v.clone())))
    }

    /// Number of live documents (cache-backed).
    pub fn len(&self) -> usize {
        self.cached_snapshot().docs.len()
    }

    /// True if there are no live documents.
    pub fn is_empty(&self) -> bool {
        self.cached_snapshot().docs.is_empty()
    }

    /// Return (and cache) the materialized snapshot, re-folding only if the merged op-count advanced
    /// since the last fold. Repeated reads at the same version are O(1).
    fn cached_snapshot(&self) -> Snapshot {
        let op_count = self.merged.op_count(); // pulls; cheap
        let mut cache = lock_or_recover(&self.cache);
        if cache.version == Some(op_count) {
            return cache.snapshot.clone();
        }
        let docs = self.merged.read(|m| m.documents());
        let snap = Snapshot { docs, op_count };
        cache.version = Some(op_count);
        cache.snapshot = snap.clone();
        snap
    }

    // --- realtime ---

    /// A raw change signal: fires (with the merged op-count) on every local write and every peer op
    /// (the background poller delivers peer ops without an external loop). Prefer
    /// [`changes`](Self::changes) for per-document deltas.
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.merged.watch()
    }

    /// Start a background poller that calls `pull()` every `interval`, so peer ops arriving over the
    /// mesh wake watchers **without** an external poll loop. Idempotent: a second call replaces the
    /// previous poller. Aborted automatically on drop. Returns the [`watch::Receiver`] to react on.
    ///
    /// Without this, ce-coord only folds peer ops when something calls a read; with it, realtime is
    /// genuinely push-driven from the consumer's perspective.
    pub fn start_realtime(&self, interval: Duration) -> watch::Receiver<u64> {
        let rx = self.merged.watch();
        // The poller holds only a Weak reference to the Merged log, so it never keeps the collection
        // alive past Drop and exits cleanly once the Collection is dropped.
        let weak: std::sync::Weak<Merged<DbMachine>> = Arc::downgrade(&self.merged);
        let interval = interval.max(Duration::from_millis(20));
        let handle = tokio::spawn(async move {
            let mut ticker = tokio::time::interval(interval);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                match weak.upgrade() {
                    Some(merged) => merged.pull(),
                    None => break, // collection dropped
                }
            }
        });
        let mut g = lock_or_recover(&self.poller);
        if let Some(old) = g.replace(handle) {
            old.abort();
        }
        rx
    }

    /// Stop the background realtime poller, if running.
    pub fn stop_realtime(&self) {
        let mut g = lock_or_recover(&self.poller);
        if let Some(h) = g.take() {
            h.abort();
        }
    }

    /// Block until the collection changes after the given watch receiver's current value, then return
    /// a fresh snapshot. Returns `None` if the underlying channel closed.
    pub async fn next_change(&self, rx: &mut watch::Receiver<u64>) -> Option<Snapshot> {
        rx.changed().await.ok()?;
        Some(self.snapshot())
    }

    /// An async stream of per-document [`DocChange`] deltas (onSnapshot semantics). Each yielded
    /// batch is the diff between the previous and current snapshot. The first batch (on the initial
    /// poll) reports every existing document as [`ChangeKind::Added`], matching Firestore's initial
    /// `onSnapshot` callback. Requires [`start_realtime`](Self::start_realtime) (or external `pull`s)
    /// for peer-driven changes.
    pub fn changes(&self) -> ChangeStream<'_> {
        let rx = self.watch();
        let initial = Snapshot::default();
        ChangeStream {
            coll: self,
            rx,
            prev: initial,
            started: false,
        }
    }

    /// Force a refresh of the merged set (pull any peer ops delivered in the background).
    pub fn refresh(&self) {
        self.merged.pull();
    }

    /// Per-writer applied versions, for a sync-status display: `(device_id, version)`.
    pub fn sync_status(&self) -> Vec<(String, u64)> {
        self.merged.sync_status()
    }

    /// Snapshot this device's own writer log to a content-addressed blob and compact it.
    pub async fn compact(&self) -> Result<ce_coord::Checkpoint> {
        self.merged.writer_log().checkpoint().await
    }

    /// The merged op-count (a coarse collection version). Pulls peer ops first.
    pub fn op_count(&self) -> usize {
        self.merged.op_count()
    }

    /// Internal: op-count for batch backpressure (kept private-ish via crate visibility).
    pub(crate) fn op_count_for_batch(&self) -> usize {
        self.merged.op_count()
    }

    /// Run an **optimistic transaction**: read the current state, compute writes with `f`, then
    /// commit them iff the collection's version has not advanced since the read. Retries up to
    /// `max_attempts` times on a version conflict. Returns the closure's value on success.
    ///
    /// This provides optimistic concurrency over the *issuing device's* view. It is not cross-device
    /// serializable isolation (that needs the chain); two devices may still both commit and the CRDT
    /// converges. The closure must be pure and idempotent across retries.
    pub async fn run_transaction<T, F>(&self, max_attempts: u32, mut f: F) -> Result<T>
    where
        F: FnMut(&Snapshot, &mut crate::batch::WriteBatch<'_>) -> Result<T>,
    {
        let attempts = max_attempts.max(1);
        for _ in 0..attempts {
            let before = self.merged.op_count();
            let snap = self.snapshot();
            let mut batch = self.batch();
            let out = f(&snap, &mut batch)?;
            // Re-check the version right before committing.
            let now = self.merged.op_count();
            if now != before {
                continue; // someone else wrote; retry with fresh read
            }
            batch.commit().await?;
            return Ok(out);
        }
        anyhow::bail!("transaction failed after {attempts} attempts (version kept changing)")
    }

    // --- internals ---

    /// Reject a new write if the merged op-count has hit the configured ceiling (backpressure).
    fn guard_op_count(&self) -> Result<()> {
        if self.limits.max_ops != 0 {
            let n = self.merged.op_count();
            if n >= self.limits.max_ops {
                anyhow::bail!(
                    "collection '{}' op-count {} has reached the limit {}; compact first",
                    self.path,
                    n,
                    self.limits.max_ops
                );
            }
        }
        Ok(())
    }

    /// Stamp and propose one op into this device's writer log.
    async fn propose(&self, doc_id: &str, kind: OpKind) -> Result<()> {
        let key = self.next_key();
        let op = DocOp {
            key,
            doc_id: doc_id.to_string(),
            kind,
        };
        self.merged.propose(op).await?;
        Ok(())
    }

    /// Propose a pre-built op (used by [`WriteBatch`]). Validates nothing here — the batch validated
    /// already and assigned contiguous keys.
    pub(crate) async fn propose_op(&self, op: DocOp) -> Result<()> {
        self.merged.propose(op).await?;
        Ok(())
    }

    /// Allocate `n` strictly-increasing contiguous [`OpKey`]s for this device in one critical section
    /// (used by batched writes so the whole batch shares a contiguous Lamport range).
    pub(crate) fn next_keys(&self, n: usize) -> Vec<OpKey> {
        self.advance_clock_past_merged();
        let mut l = lock_or_recover(&self.lamport);
        let mut out = Vec::with_capacity(n);
        for _ in 0..n {
            *l += 1;
            out.push(OpKey {
                lamport: *l,
                writer: self.self_id.clone(),
            });
        }
        out
    }

    /// Allocate the next strictly-increasing [`OpKey`] for this device. Advancing-past-merged and the
    /// increment happen under one lock acquisition (single critical section), so concurrent
    /// same-device writes always get strictly increasing, non-colliding keys.
    fn next_key(&self) -> OpKey {
        let highest = self.highest_merged_lamport();
        let mut l = lock_or_recover(&self.lamport);
        if highest > *l {
            *l = highest;
        }
        *l += 1;
        OpKey {
            lamport: *l,
            writer: self.self_id.clone(),
        }
    }

    /// Advance the Lamport clock past the highest lamport in the merged set (single lock section).
    fn advance_clock_past_merged(&self) {
        let highest = self.highest_merged_lamport();
        let mut l = lock_or_recover(&self.lamport);
        if highest > *l {
            *l = highest;
        }
    }

    /// Highest lamport currently in the merged union (pulls peer ops first).
    fn highest_merged_lamport(&self) -> u64 {
        self.merged
            .merged_ops()
            .iter()
            .map(|op| op.key.lamport)
            .max()
            .unwrap_or(0)
    }
}

/// An async stream of per-document [`DocChange`] batches for a collection (onSnapshot semantics).
pub struct ChangeStream<'a> {
    coll: &'a Collection,
    rx: watch::Receiver<u64>,
    prev: Snapshot,
    started: bool,
}

impl ChangeStream<'_> {
    /// Await the next batch of document changes. The first call yields every existing doc as
    /// `Added`. Returns `None` when the change channel closes.
    pub async fn next(&mut self) -> Option<Vec<DocChange>> {
        if !self.started {
            self.started = true;
            self.coll.refresh();
            let snap = self.coll.snapshot();
            let changes = snap.diff(&self.prev);
            self.prev = snap;
            return Some(changes);
        }
        loop {
            self.rx.changed().await.ok()?;
            let snap = self.coll.snapshot();
            let changes = snap.diff(&self.prev);
            self.prev = snap;
            if !changes.is_empty() {
                return Some(changes);
            }
            // No effective change (e.g. duplicate op) — wait for the next signal.
        }
    }
}

/// Acquire a `Mutex` tolerating poisoning (a panic elsewhere while holding it must not cascade into a
/// panic on every subsequent write — standards forbid `unwrap()` in production paths). Poisoning here
/// only means a prior holder panicked; the data behind these mutexes (a counter / a cache / a task
/// handle) is still consistent to use, so recovering the guard is safe.
fn lock_or_recover<T>(m: &Mutex<T>) -> MutexGuard<'_, T> {
    m.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(v: serde_json::Value) -> Document {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn snapshot_get_and_query() {
        let mut docs = std::collections::BTreeMap::new();
        docs.insert("u1".to_string(), doc(json!({"name": "ada", "age": 36})));
        docs.insert("u2".to_string(), doc(json!({"name": "bob", "age": 28})));
        let snap = Snapshot { docs, op_count: 2 };
        assert_eq!(snap.len(), 2);
        assert_eq!(snap.get("u1").unwrap()["name"], json!("ada"));
        let q = Query::new().with(crate::query::Filter::new(
            "age",
            crate::query::Op::Gt,
            json!(30),
        ));
        let r = snap.query(&q);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "u1");
    }

    #[test]
    fn snapshot_diff_added_modified_removed() {
        let mut a = std::collections::BTreeMap::new();
        a.insert("u1".to_string(), doc(json!({"v": 1})));
        a.insert("u2".to_string(), doc(json!({"v": 2})));
        let prev = Snapshot {
            docs: a,
            op_count: 2,
        };

        let mut b = std::collections::BTreeMap::new();
        b.insert("u1".to_string(), doc(json!({"v": 1}))); // unchanged
        b.insert("u3".to_string(), doc(json!({"v": 3}))); // added
        let next = Snapshot {
            docs: b,
            op_count: 4,
        };

        let mut changes = next.diff(&prev);
        changes.sort_by(|x, y| x.doc_id.cmp(&y.doc_id));
        assert_eq!(changes.len(), 2);
        // u2 removed, u3 added.
        assert_eq!(changes[0].doc_id, "u2");
        assert_eq!(changes[0].kind, ChangeKind::Removed);
        assert_eq!(changes[1].doc_id, "u3");
        assert_eq!(changes[1].kind, ChangeKind::Added);
    }
}
