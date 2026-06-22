//! Edge-case and failure-mode coverage for ce-db's public model/query/access surface beyond the
//! in-module unit tests: mixed-type query comparisons, ordering with missing fields, null-field
//! deletes, tombstone resurrection ordering, capability inspection of garbage, and path parsing.

use std::collections::BTreeMap;

use ce_cap::Resource;
use ce_coord::MergeMachine;
use ce_db::{
    CollectionGrant, DbMachine, DocOp, Document, OpKey, OpKind, Query, Filter, DocPath, Snapshot,
    ABILITY_READ, ABILITY_WRITE,
};
use ce_db::query::{Dir, Op};
use ce_identity::{Identity, NodeId};
use serde_json::json;

fn ident(tag: &str) -> Identity {
    let dir = std::env::temp_dir().join(format!("ce-db-edge-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let id = Identity::load_or_generate(&dir).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    id
}

fn never(_: &NodeId, _: u64) -> bool {
    false
}

fn doc(v: serde_json::Value) -> Document {
    v.as_object().cloned().unwrap()
}

fn key(l: u64, w: &str) -> OpKey {
    OpKey { lamport: l, writer: w.into() }
}

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

// ---------- query: mixed types, ordering with missing fields ----------

#[test]
fn comparison_across_incomparable_types_is_false() {
    let docs = vec![
        ("a".to_string(), doc(json!({"v": "string"}))),
        ("b".to_string(), doc(json!({"v": 5}))),
        ("c".to_string(), doc(json!({"v": true}))),
    ];
    // Gt against a number: only the numeric doc can compare; string/bool are incomparable → excluded.
    let q = Query::new().with(Filter { field: "v".into(), op: Op::Gt, value: json!(1) });
    let ids: Vec<String> = q.run(docs).into_iter().map(|(id, _)| id).collect();
    assert_eq!(ids, vec!["b"]);
}

#[test]
fn order_puts_missing_field_last_asc_first_desc() {
    let docs = vec![
        ("a".to_string(), doc(json!({"n": 2}))),
        ("b".to_string(), doc(json!({}))), // missing n
        ("c".to_string(), doc(json!({"n": 1}))),
    ];
    let asc: Vec<String> =
        Query::new().order("n", Dir::Asc).run(docs.clone()).into_iter().map(|(id, _)| id).collect();
    assert_eq!(asc, vec!["c", "a", "b"], "missing sorts last ascending");
    let desc: Vec<String> =
        Query::new().order("n", Dir::Desc).run(docs).into_iter().map(|(id, _)| id).collect();
    assert_eq!(desc, vec!["b", "a", "c"], "missing sorts first descending");
}

#[test]
fn contains_on_non_container_is_false() {
    let docs = vec![("a".to_string(), doc(json!({"v": 42})))];
    let q = Query::new().with(Filter { field: "v".into(), op: Op::Contains, value: json!(4) });
    assert_eq!(q.run(docs).len(), 0, "Contains on a number is false, not a panic");
}

#[test]
fn le_ge_boundary_inclusive() {
    let docs = vec![("a".to_string(), doc(json!({"n": 5})))];
    assert_eq!(Query::new().with(Filter { field: "n".into(), op: Op::Le, value: json!(5) }).run(docs.clone()).len(), 1);
    assert_eq!(Query::new().with(Filter { field: "n".into(), op: Op::Ge, value: json!(5) }).run(docs.clone()).len(), 1);
    assert_eq!(Query::new().with(Filter { field: "n".into(), op: Op::Lt, value: json!(5) }).run(docs.clone()).len(), 0);
    assert_eq!(Query::new().with(Filter { field: "n".into(), op: Op::Gt, value: json!(5) }).run(docs).len(), 0);
}

#[test]
fn limit_zero_returns_empty() {
    let docs = vec![
        ("a".to_string(), doc(json!({"n": 1}))),
        ("b".to_string(), doc(json!({"n": 2}))),
    ];
    assert_eq!(Query::new().take(0).run(docs).len(), 0);
}

// ---------- model: null-field delete, tombstone ordering, large numbers ----------

#[test]
fn patch_null_deletes_only_that_field() {
    let m = fold(&[
        DocOp { key: key(1, "a"), doc_id: "d".into(), kind: OpKind::Set(doc(json!({"x": 1, "y": 2}))) },
        DocOp { key: key(2, "a"), doc_id: "d".into(), kind: OpKind::Patch(doc(json!({"x": null}))) },
    ]);
    let d = m.get("d").unwrap();
    assert!(!d.contains_key("x"), "null patch deleted x");
    assert_eq!(d["y"], json!(2), "y untouched");
}

#[test]
fn older_set_after_delete_stays_deleted_either_order() {
    let s = DocOp { key: key(1, "a"), doc_id: "d".into(), kind: OpKind::Set(doc(json!({"v": 1}))) };
    let del = DocOp { key: key(5, "b"), doc_id: "d".into(), kind: OpKind::Delete };
    assert!(fold(&[s.clone(), del.clone()]).get("d").is_none());
    assert!(fold(&[del, s]).get("d").is_none());
}

#[test]
fn large_integer_field_survives_fold() {
    // A money-like u64 amount beyond 2^53 must survive the fold/serialize path exactly.
    let big = (1u64 << 60) + 7;
    let m = fold(&[DocOp {
        key: key(1, "a"),
        doc_id: "d".into(),
        kind: OpKind::Set(doc(json!({"amount": big}))),
    }]);
    assert_eq!(m.get("d").unwrap()["amount"], json!(big));
}

#[test]
fn empty_machine_is_empty() {
    let m = DbMachine::default();
    assert!(m.is_empty());
    assert_eq!(m.len(), 0);
    assert!(m.get("nope").is_none());
    assert!(m.documents().is_empty());
}

// ---------- snapshot helpers ----------

#[test]
fn snapshot_query_and_get() {
    let mut docs = BTreeMap::new();
    docs.insert("u1".to_string(), doc(json!({"age": 36})));
    docs.insert("u2".to_string(), doc(json!({"age": 28})));
    let snap = Snapshot { docs, op_count: 2 };
    assert_eq!(snap.len(), 2);
    assert!(!snap.is_empty());
    assert_eq!(snap.get("u1").unwrap()["age"], json!(36));
    let q = Query::new().with(Filter { field: "age".into(), op: Op::Gt, value: json!(30) });
    assert_eq!(snap.query(&q).len(), 1);
}

// ---------- path parsing ----------

#[test]
fn docpath_edge_cases() {
    assert!(DocPath::parse("c/d").is_ok());
    assert_eq!(DocPath::parse("c/a/b/c").unwrap().doc_id, "a/b/c");
    assert!(DocPath::parse("nocollection").is_err());
    assert!(DocPath::parse("/doc").is_err());
    assert!(DocPath::parse("coll/").is_err());
    assert!(DocPath::parse("").is_err());
    assert!(DocPath::parse("/").is_err());
}

// ---------- capability inspection / verification robustness ----------

#[test]
fn grant_from_garbage_token_errors() {
    assert!(CollectionGrant::from_token("").is_err());
    assert!(CollectionGrant::from_token("not hex").is_err());
    assert!(CollectionGrant::from_token("deadbeef").is_err());
}

#[test]
fn non_holder_cannot_attenuate() {
    let owner = ident("o");
    let mid = ident("m");
    let stranger = ident("s");
    let leaf = ident("l");
    let g = CollectionGrant::mint(&owner, mid.node_id(), "c", &[ABILITY_READ], Resource::Any, 0, 1);
    // A stranger (not the leaf audience) cannot attenuate the chain.
    assert!(g.attenuate(&stranger, leaf.node_id(), &[ABILITY_READ], Resource::Any, 0, 2).is_err());
}

#[test]
fn revoked_grant_is_denied() {
    let owner = ident("ro");
    let peer = ident("rp");
    let g = CollectionGrant::mint(&owner, peer.node_id(), "c", &[ABILITY_WRITE], Resource::Any, 0, 77);
    let revoke_77 = |_i: &NodeId, n: u64| n == 77;
    let r = g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_WRITE, "c", &revoke_77);
    assert!(r.is_err(), "a revoked grant must be denied");
    // Same grant verifies when nothing is revoked.
    assert!(g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_WRITE, "c", &never).is_ok());
}

#[test]
fn unrelated_root_denies() {
    // A grant rooted at `owner` must NOT verify when the enforcing node accepts only a DIFFERENT root.
    let owner = ident("u1");
    let other = ident("u2");
    let peer = ident("up");
    let g = CollectionGrant::mint(&owner, peer.node_id(), "c", &[ABILITY_READ], Resource::Any, 0, 1);
    // Enforcing node is `other`, accepting no extra roots → owner's chain is not rooted here.
    let r = g.verify(&other.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_READ, "c", &never);
    assert!(r.is_err(), "a chain rooted at an unaccepted key must be denied");
}
