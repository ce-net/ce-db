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
//! ## Realtime
//!
//! [`Collection::watch`] returns a [`tokio::sync::watch::Receiver`] that fires whenever the merged
//! op-set grows — a local write *or* any peer's op arriving over the mesh. [`Collection::subscribe`]
//! wraps that into an async stream of materialized [`Snapshot`]s (one per change), which is what the
//! realtime two-reader demo and `onSnapshot` listeners consume.
//!
//! ## Lamport clock
//!
//! Each write is stamped with `(lamport, writer_id)`. The Lamport counter advances past every op key
//! observed in the merged set, so a freshly written op always sorts after everything this device has
//! seen — giving last-writer-wins the intuitive "most recent edit on this device wins ties" behavior
//! while staying a pure function of the op set.

use std::sync::Mutex;

use anyhow::Result;
use ce_coord::{Coord, Merged};
use serde_json::Value;
use tokio::sync::watch;

use crate::doc::{DbMachine, Document, DocOp, OpKey, OpKind};
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
}

/// A live, realtime, multi-writer document collection.
pub struct Collection {
    merged: Merged<DbMachine>,
    self_id: String,
    /// Monotonic Lamport counter for this device's writes.
    lamport: Mutex<u64>,
    name: String,
}

impl Collection {
    /// Open collection `name` on this device. `self_id` is this device's NodeId hex; `peer_ids` are
    /// the *other* member devices whose logs to follow (their writes converge into this replica).
    /// New members can be added later with [`add_writer`](Self::add_writer).
    pub async fn open(coord: &Coord, name: &str, peer_ids: &[String]) -> Result<Collection> {
        let self_id = coord.node_id().to_string();
        let merged = Merged::<DbMachine>::open(coord, name, &self_id, peer_ids).await?;
        let coll = Collection { merged, self_id, lamport: Mutex::new(0), name: name.to_string() };
        coll.advance_clock_past_merged();
        Ok(coll)
    }

    /// Convenience: open against the local node, discovering this device's id from the [`Coord`].
    /// Equivalent to [`open`](Self::open) — kept for symmetry with single-node demos.
    pub async fn open_local(coord: &Coord, name: &str, peer_ids: &[String]) -> Result<Collection> {
        Self::open(coord, name, peer_ids).await
    }

    /// Start following another device's writer log (a newly added collaborator). Idempotent.
    pub async fn add_writer(&self, peer_id: &str) -> Result<()> {
        self.merged.add_writer(peer_id).await
    }

    /// The collection name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// This device's NodeId hex.
    pub fn writer_id(&self) -> &str {
        &self.self_id
    }

    /// Set (replace) a whole document. Last-writer-wins on the whole document.
    pub async fn set(&self, doc_id: &str, doc: Document) -> Result<()> {
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
    /// field value deletes that field. Concurrent patches to different fields all survive.
    pub async fn patch(&self, doc_id: &str, fields: Document) -> Result<()> {
        self.propose(doc_id, OpKind::Patch(fields)).await
    }

    /// Set a single field on a document (a one-field [`patch`](Self::patch)).
    pub async fn set_field(&self, doc_id: &str, field: &str, value: Value) -> Result<()> {
        let mut obj = Document::new();
        obj.insert(field.to_string(), value);
        self.patch(doc_id, obj).await
    }

    /// Delete (tombstone) a document. A later set/patch resurrects it.
    pub async fn delete(&self, doc_id: &str) -> Result<()> {
        self.propose(doc_id, OpKind::Delete).await
    }

    /// Read one document by id, folding the latest merged state first.
    pub fn get(&self, doc_id: &str) -> Option<Document> {
        self.merged.read(|m| m.get(doc_id))
    }

    /// A consistent snapshot of the whole collection (folds the latest merged state).
    pub fn snapshot(&self) -> Snapshot {
        let op_count = self.merged.op_count();
        let docs = self.merged.read(|m| m.documents());
        Snapshot { docs, op_count }
    }

    /// Run a query and return matching `(doc_id, Document)` pairs.
    pub fn query(&self, q: &Query) -> Vec<(String, Document)> {
        let docs = self.merged.read(|m| m.documents());
        q.run(docs)
    }

    /// Number of live documents.
    pub fn len(&self) -> usize {
        self.merged.read(|m| m.len())
    }

    /// True if there are no live documents.
    pub fn is_empty(&self) -> bool {
        self.merged.read(|m| m.is_empty())
    }

    /// A raw change signal: fires (with the merged op-count) on every local write and every peer op.
    /// Prefer [`subscribe`](Self::subscribe) for materialized snapshots.
    pub fn watch(&self) -> watch::Receiver<u64> {
        self.merged.watch()
    }

    /// Block until the collection changes after the given watch receiver's current value, then return
    /// a fresh snapshot. Returns `None` if the underlying channel closed. This is the building block
    /// the realtime demo loops on.
    pub async fn next_change(&self, rx: &mut watch::Receiver<u64>) -> Option<Snapshot> {
        rx.changed().await.ok()?;
        Some(self.snapshot())
    }

    /// Force a refresh of the merged set (pull any peer ops delivered in the background). Reads do
    /// this automatically; call it before a `watch` loop's first read if you want the initial state.
    pub fn refresh(&self) {
        self.merged.pull();
    }

    /// Per-writer applied versions, for a sync-status display: `(device_id, version)`. The first
    /// entry is this device's own log.
    pub fn sync_status(&self) -> Vec<(String, u64)> {
        self.merged.sync_status()
    }

    /// Snapshot this device's own writer log to a content-addressed blob and compact it, so a fresh
    /// reader can bootstrap from the snapshot instead of replaying the full history (the Firestore
    /// Snapshot/compaction path). Returns the checkpoint `(base, cid)`.
    pub async fn compact(&self) -> Result<ce_coord::Checkpoint> {
        self.merged.writer_log().checkpoint().await
    }

    // --- internals ---

    /// Stamp and propose one op into this device's writer log.
    async fn propose(&self, doc_id: &str, kind: OpKind) -> Result<()> {
        let key = self.next_key();
        let op = DocOp { key, doc_id: doc_id.to_string(), kind };
        self.merged.propose(op).await?;
        Ok(())
    }

    /// Allocate the next strictly-increasing [`OpKey`] for this device, advancing past anything
    /// already merged so a new write always sorts last on this replica.
    fn next_key(&self) -> OpKey {
        self.advance_clock_past_merged();
        let mut l = self.lamport.lock().unwrap();
        *l += 1;
        OpKey { lamport: *l, writer: self.self_id.clone() }
    }

    /// Pull the Lamport clock up to (and we then go past) the highest lamport seen in the merged set.
    /// `merged_ops` pulls peer ops first, so this also serves as the refresh before stamping a write.
    fn advance_clock_past_merged(&self) {
        let highest = self
            .merged
            .merged_ops()
            .iter()
            .map(|op| op.key.lamport)
            .max()
            .unwrap_or(0);
        let mut l = self.lamport.lock().unwrap();
        if highest > *l {
            *l = highest;
        }
    }
}

#[cfg(test)]
mod tests {
    // The Collection's interaction with the live Merged/Coord layer is covered by the integration
    // test (`tests/realtime_sync.rs`) which spins an in-process two-reader scenario. The pure model
    // (DbMachine/Query) is unit-tested in `doc.rs` and `query.rs`. Here we assert the snapshot view
    // helpers behave on a hand-built snapshot, with no node needed.
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
        let q = Query::new().with(crate::query::Filter {
            field: "age".into(),
            op: crate::query::Op::Gt,
            value: json!(30),
        });
        let r = snap.query(&q);
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "u1");
    }
}
