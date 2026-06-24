//! The document data model and the [`DbMachine`] that converges it.
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
//! * [`OpKind::Set`] replaces the whole document. Last writer (by `OpKey`) wins.
//! * [`OpKind::Patch`] merges fields. Each *field* is last-writer-wins independently, so two devices
//!   editing different fields of the same document both survive (the field-level CRDT merge the
//!   Firestore design calls for). Nested objects merge **field by field** (dotted-path deep merge),
//!   not wholesale-replace, unless the value is a non-object (then it replaces).
//! * [`OpKind::Update`] applies CRDT-friendly **atomic field operations**
//!   ([`increment`](FieldOp::Increment), [`array_union`](FieldOp::ArrayUnion),
//!   [`array_remove`](FieldOp::ArrayRemove)) so concurrent counters and set edits compose instead of
//!   clobbering. `serverTimestamp` is resolved to a literal at propose time and rides as a `Patch`.
//! * [`OpKind::Delete`] tombstones the document. A later `Set`/`Patch`/`Update` (higher `OpKey`)
//!   resurrects it; an earlier one stays deleted. Tombstones are retained so the fold stays
//!   order-independent.
//!
//! Large fields belong in CE blobs (store the CID as a string field); ce-db keeps the metadata.

use std::collections::BTreeMap;

use ce_coord::MergeMachine;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Number, Value};

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

/// The document mutations.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum OpKind {
    /// Replace the entire document with this object (whole-document last-writer-wins).
    Set(Document),
    /// Merge these fields into the document; each field is last-writer-wins independently.
    /// A field whose value is `null` deletes that field (Firestore-style field delete). Nested
    /// objects deep-merge field by field.
    Patch(Document),
    /// Apply atomic field operations (increment / array union / array remove). CRDT-friendly: these
    /// compose across concurrent writers instead of overwriting each other.
    Update(BTreeMap<String, FieldOp>),
    /// Tombstone the document. A later op resurrects it; an earlier one leaves it deleted.
    Delete,
}

/// An atomic, CRDT-friendly field operation (the `FieldValue.*` sentinels in Firestore).
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum FieldOp {
    /// Add a numeric delta to the field (a grow-only/PN counter). The delta is stored as a JSON
    /// number; deltas from all writers sum, so concurrent increments compose exactly.
    Increment(Number),
    /// Add these elements to the field (treated as a set/array); elements already present are not
    /// duplicated. Applied deterministically in `OpKey` order.
    ArrayUnion(Vec<Value>),
    /// Remove these elements from the field (an array). Applied deterministically in `OpKey` order.
    ArrayRemove(Vec<Value>),
}

/// Per-field provenance so [`OpKind::Patch`] can do *field-level* last-writer-wins, plus the running
/// state needed for [`OpKind::Update`]'s atomic ops.
#[derive(Default, Clone)]
struct DocState {
    /// The live fields and, for each, the [`OpKey`] that last *literally* wrote it (Set/Patch).
    fields: BTreeMap<String, (Value, OpKey)>,
    /// The highest `OpKey` of a whole-document op (`Set`/`Delete`) applied so far. Any field whose
    /// winning key is `<=` this baseline was established before the document was last wholesale
    /// replaced/cleared, so it must be dropped when that baseline op is the document's current truth.
    baseline: Option<OpKey>,
    /// True once a `Delete` whose key is the current baseline has been applied and not yet
    /// superseded by a newer whole-document op.
    deleted: bool,
    /// Highest OpKey seen for this doc (its "update time" version).
    last_key: Option<OpKey>,
    /// Lowest OpKey seen for this doc (its "create time" version).
    first_key: Option<OpKey>,
}

/// Lightweight per-document metadata, exposed for Firestore-style `createTime`/`updateTime`/version.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocMeta {
    /// The op key that first created this document.
    pub created: Option<OpKey>,
    /// The op key of the most recent mutation.
    pub updated: Option<OpKey>,
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
            out.insert(id.clone(), materialize(st));
        }
        out
    }

    /// Fetch one materialized document by id (`None` if absent or tombstoned).
    pub fn get(&self, doc_id: &str) -> Option<Document> {
        let st = self.docs.get(doc_id)?;
        if st.deleted {
            return None;
        }
        Some(materialize(st))
    }

    /// Per-document metadata (create/update op keys). `None` if the doc was never touched.
    pub fn meta(&self, doc_id: &str) -> Option<DocMeta> {
        let st = self.docs.get(doc_id)?;
        Some(DocMeta {
            created: st.first_key.clone(),
            updated: st.last_key.clone(),
        })
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

/// Build a plain JSON document from a [`DocState`]'s live fields.
fn materialize(st: &DocState) -> Document {
    let mut doc = Document::new();
    for (k, (v, _)) in &st.fields {
        doc.insert(k.clone(), v.clone());
    }
    doc
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
        // Track create/update version metadata.
        if st.first_key.is_none() {
            st.first_key = Some(op.key.clone());
        }
        st.last_key = Some(op.key.clone());
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
                    apply_patch_field(&mut st.fields, k, v, &op.key);
                }
                // A patch resurrects a tombstoned doc (its key is newer than the delete baseline).
                st.deleted = false;
            }
            OpKind::Update(ops) => {
                for (k, fop) in ops {
                    apply_field_op(&mut st.fields, k, fop, &op.key);
                }
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

/// Apply one field of a [`OpKind::Patch`], with field-level LWW and nested deep-merge.
fn apply_patch_field(
    fields: &mut BTreeMap<String, (Value, OpKey)>,
    k: String,
    v: Value,
    key: &OpKey,
) {
    // A field write only wins if it is newer than the field's current writer.
    let newer = match fields.get(&k) {
        Some((_, prev)) => key > prev,
        None => true,
    };
    if !newer {
        return;
    }
    if v.is_null() {
        fields.remove(&k); // null = field delete
        return;
    }
    // Deep-merge nested objects field-by-field; otherwise replace.
    if let (Some((Value::Object(existing), _)), Value::Object(incoming)) = (fields.get(&k), &v) {
        let mut merged = existing.clone();
        deep_merge(&mut merged, incoming.clone());
        fields.insert(k, (Value::Object(merged), key.clone()));
    } else {
        fields.insert(k, (v, key.clone()));
    }
}

/// Recursively merge `incoming` into `target` (object deep-merge; `null` deletes a key).
fn deep_merge(target: &mut Map<String, Value>, incoming: Map<String, Value>) {
    for (k, v) in incoming {
        if v.is_null() {
            target.remove(&k);
        } else if let (Some(Value::Object(existing)), Value::Object(inc)) = (target.get_mut(&k), &v)
        {
            deep_merge(existing, inc.clone());
        } else {
            target.insert(k, v);
        }
    }
}

/// Apply one [`FieldOp`] atomically to a field, maintaining order-independent CRDT semantics.
fn apply_field_op(
    fields: &mut BTreeMap<String, (Value, OpKey)>,
    k: String,
    fop: FieldOp,
    key: &OpKey,
) {
    match fop {
        FieldOp::Increment(delta) => {
            let base = fields.get(&k).map(|(v, _)| v.clone());
            let next = add_numbers(base.as_ref(), &delta);
            fields.insert(k, (next, key.clone()));
        }
        FieldOp::ArrayUnion(items) => {
            let mut arr = match fields.get(&k) {
                Some((Value::Array(a), _)) => a.clone(),
                _ => Vec::new(),
            };
            for it in items {
                if !arr.contains(&it) {
                    arr.push(it);
                }
            }
            fields.insert(k, (Value::Array(arr), key.clone()));
        }
        FieldOp::ArrayRemove(items) => {
            let mut arr = match fields.get(&k) {
                Some((Value::Array(a), _)) => a.clone(),
                _ => Vec::new(),
            };
            arr.retain(|e| !items.contains(e));
            fields.insert(k, (Value::Array(arr), key.clone()));
        }
    }
}

/// Add a numeric delta to an optional base value, preserving integer precision when both are
/// integers. A non-numeric / missing base is treated as `0`.
fn add_numbers(base: Option<&Value>, delta: &Number) -> Value {
    let base_num = base.and_then(|v| match v {
        Value::Number(n) => Some(n),
        _ => None,
    });
    // Integer fast path: both fit i64.
    if let (Some(b), Some(d)) =
        (base_num.and_then(|n| n.as_i64()).or(Some(0)), delta.as_i64())
        && let Some(sum) = b.checked_add(d)
    {
        return Value::Number(Number::from(sum));
    }
    let b = base_num.and_then(|n| n.as_f64()).unwrap_or(0.0);
    let d = delta.as_f64().unwrap_or(0.0);
    Number::from_f64(b + d)
        .map(Value::Number)
        .unwrap_or(Value::Number(Number::from(0)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn key(l: u64, w: &str) -> OpKey {
        OpKey {
            lamport: l,
            writer: w.to_string(),
        }
    }

    fn set(l: u64, w: &str, id: &str, v: Value) -> DocOp {
        let obj = v.as_object().cloned().unwrap_or_default();
        DocOp {
            key: key(l, w),
            doc_id: id.into(),
            kind: OpKind::Set(obj),
        }
    }

    fn patch(l: u64, w: &str, id: &str, v: Value) -> DocOp {
        let obj = v.as_object().cloned().unwrap_or_default();
        DocOp {
            key: key(l, w),
            doc_id: id.into(),
            kind: OpKind::Patch(obj),
        }
    }

    fn del(l: u64, w: &str, id: &str) -> DocOp {
        DocOp {
            key: key(l, w),
            doc_id: id.into(),
            kind: OpKind::Delete,
        }
    }

    fn update(l: u64, w: &str, id: &str, ops: &[(&str, FieldOp)]) -> DocOp {
        let map: BTreeMap<String, FieldOp> = ops
            .iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();
        DocOp {
            key: key(l, w),
            doc_id: id.into(),
            kind: OpKind::Update(map),
        }
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

    /// Fold in a given order regardless of key (to prove apply is robust even if Merged's invariant
    /// weren't there — but in practice fold() is the canonical path).
    fn fold_seq(ops: &[DocOp]) -> DbMachine {
        let mut m = DbMachine::default();
        for op in ops {
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
        assert_eq!(
            fold(&[older.clone(), newer.clone()]).get("u1").unwrap()["score"],
            json!(99)
        );
        assert_eq!(fold(&[newer, older]).get("u1").unwrap()["score"], json!(99));
    }

    #[test]
    fn null_field_in_patch_deletes_field() {
        let ops = vec![
            set(1, "a", "u1", json!({"a": 1, "b": 2})),
            patch(2, "a", "u1", json!({"b": null})),
        ];
        let d = fold(&ops).get("u1").unwrap();
        assert_eq!(d["a"], json!(1));
        assert!(!d.contains_key("b"));
    }

    #[test]
    fn nested_patch_deep_merges() {
        let ops = vec![
            set(1, "a", "u1", json!({"loc": {"city": "lyon", "zip": "69"}})),
            patch(2, "a", "u1", json!({"loc": {"zip": "70"}})),
        ];
        let d = fold(&ops).get("u1").unwrap();
        // city preserved, zip updated (deep merge, not wholesale replace).
        assert_eq!(d["loc"]["city"], json!("lyon"));
        assert_eq!(d["loc"]["zip"], json!("70"));
    }

    #[test]
    fn nested_patch_null_deletes_nested_key() {
        let ops = vec![
            set(1, "a", "u1", json!({"loc": {"city": "lyon", "zip": "69"}})),
            patch(2, "a", "u1", json!({"loc": {"zip": null}})),
        ];
        let d = fold(&ops).get("u1").unwrap();
        assert_eq!(d["loc"]["city"], json!("lyon"));
        assert!(!d["loc"].as_object().unwrap().contains_key("zip"));
    }

    #[test]
    fn increment_composes_across_writers() {
        let ops = vec![
            set(1, "a", "c", json!({"n": 10})),
            update(2, "a", "c", &[("n", FieldOp::Increment(Number::from(5)))]),
            update(2, "b", "c", &[("n", FieldOp::Increment(Number::from(3)))]),
        ];
        // 10 + 5 + 3 = 18, regardless of fold order.
        assert_eq!(fold(&ops).get("c").unwrap()["n"], json!(18));
        let mut rev = ops.clone();
        rev.reverse();
        assert_eq!(fold(&rev).get("c").unwrap()["n"], json!(18));
    }

    #[test]
    fn increment_on_missing_field_starts_at_zero() {
        let ops = vec![update(
            1,
            "a",
            "c",
            &[("n", FieldOp::Increment(Number::from(7)))],
        )];
        assert_eq!(fold(&ops).get("c").unwrap()["n"], json!(7));
    }

    #[test]
    fn array_union_and_remove() {
        let ops = vec![
            set(1, "a", "c", json!({"tags": ["x"]})),
            update(
                2,
                "a",
                "c",
                &[("tags", FieldOp::ArrayUnion(vec![json!("y"), json!("x")]))],
            ),
            update(
                3,
                "b",
                "c",
                &[("tags", FieldOp::ArrayRemove(vec![json!("x")]))],
            ),
        ];
        let d = fold(&ops).get("c").unwrap();
        assert_eq!(d["tags"], json!(["y"])); // x added (dedup) then removed; y stays
    }

    #[test]
    fn delete_tombstones_until_resurrected() {
        let m1 = fold(&[set(1, "a", "u1", json!({"v": 1})), del(2, "a", "u1")]);
        assert!(m1.get("u1").is_none());
        assert_eq!(m1.len(), 0);
        let m2 = fold(&[
            set(1, "a", "u1", json!({"v": 1})),
            del(2, "a", "u1"),
            set(3, "b", "u1", json!({"v": 9})),
        ]);
        assert_eq!(m2.get("u1").unwrap()["v"], json!(9));
    }

    #[test]
    fn older_op_after_delete_stays_deleted() {
        let s = set(1, "a", "u1", json!({"v": 1}));
        let d = del(5, "b", "u1");
        assert!(fold(&[s.clone(), d.clone()]).get("u1").is_none());
        assert!(fold(&[d, s]).get("u1").is_none());
    }

    #[test]
    fn set_resets_stale_fields() {
        let ops = vec![
            patch(1, "a", "u1", json!({"old": true})),
            set(2, "b", "u1", json!({"fresh": 1})),
        ];
        let d = fold(&ops).get("u1").unwrap();
        assert!(!d.contains_key("old"));
        assert_eq!(d["fresh"], json!(1));
    }

    #[test]
    fn metadata_tracks_create_and_update() {
        let m = fold(&[
            set(1, "a", "u1", json!({"v": 1})),
            patch(5, "b", "u1", json!({"w": 2})),
        ]);
        let meta = m.meta("u1").unwrap();
        assert_eq!(meta.created.unwrap().lamport, 1);
        assert_eq!(meta.updated.unwrap().lamport, 5);
    }

    #[test]
    fn convergence_is_order_independent() {
        let ops = vec![
            set(1, "a", "u1", json!({"name": "x"})),
            patch(2, "b", "u1", json!({"score": 5})),
            set(3, "a", "u2", json!({"name": "y"})),
            patch(4, "b", "u1", json!({"score": 7})),
            del(5, "a", "u2"),
            update(
                6,
                "a",
                "u1",
                &[("score", FieldOp::Increment(Number::from(1)))],
            ),
        ];
        let reference = fold(&ops).documents();
        let mut rev = ops.clone();
        rev.reverse();
        assert_eq!(fold(&rev).documents(), reference);
        let rotated = [&ops[2..], &ops[..2]].concat();
        assert_eq!(fold(&rotated).documents(), reference);
        // u1.score = 7 then +1 = 8; u2 deleted.
        assert_eq!(reference["u1"]["score"], json!(8));
        assert!(!reference.contains_key("u2"));
    }

    #[test]
    fn fold_seq_equals_fold_for_keyed_ops() {
        // When ops are already in key order, the seq fold matches the canonical fold.
        let ops = vec![
            set(1, "a", "u1", json!({"v": 1})),
            patch(2, "a", "u1", json!({"w": 2})),
        ];
        assert_eq!(fold(&ops).documents(), fold_seq(&ops).documents());
    }
}
