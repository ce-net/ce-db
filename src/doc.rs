//! The document data model and the [`MergeMachine`] that converges it.
//!
//! A ce-db collection is a map `doc_id -> Document` where a `Document` is a JSON object
//! (`serde_json::Map<String, Value>`). Convergence is provided by ce-coord's [`Merged`] layer:
//! every writer appends [`DocOp`]s to its own log, readers take the key-ordered **union** of all
//! logs, and [`DbMachine::apply`] folds that union from a fresh state in ascending key order. Because
//! the fold is a pure function of the op *set*, every replica converges to the same map regardless of
//! delivery order — leaderless, Raft-free, offline-tolerant (exactly the Firestore offline story).
//!
//! [`Merged`]: ce_coord::Merged
//!
//! ## Conflict resolution
//!
//! Each op carries a strict total-order [`OpKey`] = `(lamport, writer)`. Two distinct ops never share
//! a key, so the union's `BTreeMap<OpKey, _>` is a deterministic sort.
//!
//! * [`DocOp::Set`] replaces the whole document. Last writer (by `OpKey`) wins.
//! * [`DocOp::Patch`] merges fields. Each *field* is last-writer-wins independently, so two devices
//!   editing different fields of the same document both survive (this is the field-level CRDT merge
//!   the Firestore design calls for).
//! * [`DocOp::Delete`] tombstones the document. A later `Set`/`Patch` (higher `OpKey`) resurrects it;
//!   an earlier one stays deleted. Tombstones are retained so the fold stays order-independent.
//!
//! Large fields belong in CE blobs (store the CID as a string field); ce-db keeps the metadata.

use std::collections::BTreeMap;

use ce_coord::MergeMachine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

/// A JSON document: an object of string keys to arbitrary JSON values.
pub type Document = Map<String, Value>;

/// A strict total-order key over ops: a Lamport clock paired with the writer's NodeId hex. No two
/// distinct ops ever collide (a writer never reuses a `(lamport, writer)` pair), which is exactly
/// what [`MergeMachine`] requires for order-independent convergence.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OpKey {
    /// Monotonic per-writer Lamport counter, advanced past any timestamp this writer has observed.
    pub lamport: u64,
    /// The proposing writer's NodeId (hex). Breaks ties when two writers reach the same lamport.
    pub writer: String,
}

/// A single mutation to one document in a collection.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct DocOp {
    /// Total-order key — decides which write wins on conflict.
    pub key: OpKey,
    /// The document this op targets.
    pub doc_id: String,
    /// What to do to it.
    pub kind: OpKind,
}

/// The three document mutations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    /// Replace the entire document with this object (whole-document last-writer-wins).
    Set(Document),
    /// Merge these fields into the document; each field is last-writer-wins independently.
    /// A field whose value is `null` deletes that field (Firestore-style field delete).
    Patch(Document),
    /// Tombstone the document. A later op resurrects it; an earlier one leaves it deleted.
    Delete,
}

/// Per-field provenance so [`OpKind::Patch`] can do *field-level* last-writer-wins. We remember the
/// winning [`OpKey`] for every field and for the whole-document baseline, so a `Patch` from an
/// older op never clobbers a newer field, and a whole-document `Set` correctly resets fields whose
/// last individual write predates it.
#[derive(Default, Clone)]
struct DocState {
    /// The live fields and, for each, the [`OpKey`] that last wrote it.
    fields: BTreeMap<String, (Value, OpKey)>,
    /// The highest `OpKey` of a whole-document op (`Set`/`Delete`) applied so far. Any field whose
    /// winning key is `<=` this baseline was established before the document was last wholesale
    /// replaced/cleared, so it must be dropped when that baseline op is the document's current truth.
    baseline: Option<OpKey>,
    /// True once a `Delete` whose key is the current baseline has been applied and not yet
    /// superseded by a newer whole-document op.
    deleted: bool,
}

/// The converged collection: `doc_id -> DocState`. Folded fresh from the key-ordered op union on
/// every read, so the result is a pure function of the op set.
#[derive(Default)]
pub struct DbMachine {
    docs: BTreeMap<String, DocState>,
}

impl DbMachine {
    /// The current materialized documents (tombstoned docs omitted). Cloned out for queries/reads.
    pub fn documents(&self) -> BTreeMap<String, Document> {
        let mut out = BTreeMap::new();
        for (id, st) in &self.docs {
            if st.deleted {
                continue;
            }
            let mut doc = Document::new();
            for (k, (v, _)) in &st.fields {
                doc.insert(k.clone(), v.clone());
            }
            out.insert(id.clone(), doc);
        }
        out
    }

    /// Fetch one materialized document by id (`None` if absent or tombstoned).
    pub fn get(&self, doc_id: &str) -> Option<Document> {
        let st = self.docs.get(doc_id)?;
        if st.deleted {
            return None;
        }
        let mut doc = Document::new();
        for (k, (v, _)) in &st.fields {
            doc.insert(k.clone(), v.clone());
        }
        Some(doc)
    }

    /// Number of live (non-tombstoned) documents.
    pub fn len(&self) -> usize {
        self.docs.values().filter(|s| !s.deleted).count()
    }

    /// True if there are no live documents.
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

impl MergeMachine for DbMachine {
    type Op = DocOp;
    type Key = OpKey;

    fn key(op: &DocOp) -> OpKey {
        op.key.clone()
    }

    fn apply(&mut self, op: DocOp) {
        // Folded in ascending OpKey order, so `op.key` is >= every key already applied to this doc.
        let st = self.docs.entry(op.doc_id).or_default();
        match op.kind {
            OpKind::Set(obj) => {
                // Whole-document replace: this op is the new baseline. Drop fields established by
                // older whole-document-or-field writes and install the new object's fields.
                st.fields.clear();
                for (k, v) in obj {
                    st.fields.insert(k, (v, op.key.clone()));
                }
                st.baseline = Some(op.key);
                st.deleted = false;
            }
            OpKind::Patch(obj) => {
                for (k, v) in obj {
                    // A field write only wins if it is newer than the field's current writer.
                    let newer = match st.fields.get(&k) {
                        Some((_, prev)) => op.key > *prev,
                        None => true,
                    };
                    if !newer {
                        continue;
                    }
                    if v.is_null() {
                        st.fields.remove(&k); // null = field delete
                    } else {
                        st.fields.insert(k, (v, op.key.clone()));
                    }
                }
                // A patch resurrects a tombstoned doc (its key is newer than the delete baseline).
                st.deleted = false;
            }
            OpKind::Delete => {
                // Whole-document clear; newer than the current baseline because of fold ordering.
                st.fields.clear();
                st.baseline = Some(op.key);
                st.deleted = true;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(l: u64, w: &str) -> OpKey {
        OpKey { lamport: l, writer: w.to_string() }
    }

    fn set(l: u64, w: &str, id: &str, v: Value) -> DocOp {
        let obj = v.as_object().cloned().unwrap_or_default();
        DocOp { key: key(l, w), doc_id: id.into(), kind: OpKind::Set(obj) }
    }

    fn patch(l: u64, w: &str, id: &str, v: Value) -> DocOp {
        let obj = v.as_object().cloned().unwrap_or_default();
        DocOp { key: key(l, w), doc_id: id.into(), kind: OpKind::Patch(obj) }
    }

    fn del(l: u64, w: &str, id: &str) -> DocOp {
        DocOp { key: key(l, w), doc_id: id.into(), kind: OpKind::Delete }
    }

    /// Fold ops the way `Merged` does: dedup into a key-ordered map, then apply in order.
    fn fold(ops: &[DocOp]) -> DbMachine {
        let mut union: BTreeMap<OpKey, DocOp> = BTreeMap::new();
        for op in ops {
            union.insert(op.key.clone(), op.clone());
        }
        let mut m = DbMachine::default();
        for op in union.values() {
            m.apply(op.clone());
        }
        m
    }

    #[test]
    fn set_then_get_roundtrips() {
        let m = fold(&[set(1, "a", "u1", json!({"name": "ada", "age": 36}))]);
        let d = m.get("u1").unwrap();
        assert_eq!(d["name"], json!("ada"));
        assert_eq!(d["age"], json!(36));
        assert_eq!(m.len(), 1);
    }

    #[test]
    fn whole_document_lww_highest_key_wins() {
        let ops = vec![
            set(1, "a", "u1", json!({"v": 1})),
            set(3, "b", "u1", json!({"v": 3})),
            set(2, "a", "u1", json!({"v": 2})),
        ];
        let m = fold(&ops);
        assert_eq!(m.get("u1").unwrap()["v"], json!(3));
    }

    #[test]
    fn field_level_patch_merge_preserves_concurrent_fields() {
        // Two writers patch different fields of the same doc; both survive (field-level CRDT).
        let ops = vec![
            set(1, "a", "u1", json!({"name": "ada"})),
            patch(2, "a", "u1", json!({"email": "ada@x"})),
            patch(2, "b", "u1", json!({"phone": "555"})),
        ];
        let m = fold(&ops);
        let d = m.get("u1").unwrap();
        assert_eq!(d["name"], json!("ada"));
        assert_eq!(d["email"], json!("ada@x"));
        assert_eq!(d["phone"], json!("555"));
    }

    #[test]
    fn field_patch_lww_newer_wins_regardless_of_order() {
        let older = patch(1, "a", "u1", json!({"score": 10}));
        let newer = patch(5, "b", "u1", json!({"score": 99}));
        // Apply in both orders; newer key must win either way.
        assert_eq!(fold(&[older.clone(), newer.clone()]).get("u1").unwrap()["score"], json!(99));
        assert_eq!(fold(&[newer, older]).get("u1").unwrap()["score"], json!(99));
    }

    #[test]
    fn null_field_in_patch_deletes_field() {
        let ops =
            vec![set(1, "a", "u1", json!({"a": 1, "b": 2})), patch(2, "a", "u1", json!({"b": null}))];
        let d = fold(&ops).get("u1").unwrap();
        assert_eq!(d["a"], json!(1));
        assert!(!d.contains_key("b"));
    }

    #[test]
    fn delete_tombstones_until_resurrected() {
        let m1 = fold(&[set(1, "a", "u1", json!({"v": 1})), del(2, "a", "u1")]);
        assert!(m1.get("u1").is_none());
        assert_eq!(m1.len(), 0);
        // A newer set after the delete resurrects.
        let m2 = fold(&[set(1, "a", "u1", json!({"v": 1})), del(2, "a", "u1"), set(3, "b", "u1", json!({"v": 9}))]);
        assert_eq!(m2.get("u1").unwrap()["v"], json!(9));
    }

    #[test]
    fn older_op_after_delete_stays_deleted() {
        // A set with a key LOWER than the delete must not resurrect, no matter the delivery order.
        let s = set(1, "a", "u1", json!({"v": 1}));
        let d = del(5, "b", "u1");
        assert!(fold(&[s.clone(), d.clone()]).get("u1").is_none());
        assert!(fold(&[d, s]).get("u1").is_none());
    }

    #[test]
    fn set_resets_stale_fields() {
        // A whole-document Set must clear fields written by older patches, even out of order.
        let ops = vec![
            patch(1, "a", "u1", json!({"old": true})),
            set(2, "b", "u1", json!({"fresh": 1})),
        ];
        let d = fold(&ops).get("u1").unwrap();
        assert!(!d.contains_key("old"));
        assert_eq!(d["fresh"], json!(1));
    }

    #[test]
    fn convergence_is_order_independent() {
        let ops = vec![
            set(1, "a", "u1", json!({"name": "x"})),
            patch(2, "b", "u1", json!({"score": 5})),
            set(3, "a", "u2", json!({"name": "y"})),
            patch(4, "b", "u1", json!({"score": 7})),
            del(5, "a", "u2"),
        ];
        let reference = fold(&ops).documents();
        // Reverse order, and a rotated order — all must converge identically.
        let mut rev = ops.clone();
        rev.reverse();
        assert_eq!(fold(&rev).documents(), reference);
        let rotated = [&ops[2..], &ops[..2]].concat();
        assert_eq!(fold(&rotated).documents(), reference);
        // Final truth: u1 has both fields with score=7; u2 is deleted.
        assert_eq!(reference["u1"]["score"], json!(7));
        assert!(!reference.contains_key("u2"));
    }
}
