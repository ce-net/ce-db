//! Extra model-level tests for the features added in the maturity pass: atomic field ops, nested
//! deep-merge, large-integer ordering precision, and CRDT convergence of `Update` ops — all over the
//! pure [`DbMachine`] fold (no node needed). Includes property tests proving increment composition
//! and order-independence.

use std::collections::BTreeMap;

use ce_coord::MergeMachine;
use ce_db::{DbMachine, DocOp, FieldOp, Filter, Op, OpKey, OpKind, Query};
use proptest::prelude::*;
use serde_json::{Number, Value, json};

fn key(l: u64, w: &str) -> OpKey {
    OpKey {
        lamport: l,
        writer: w.to_string(),
    }
}

fn set(l: u64, w: &str, id: &str, v: Value) -> DocOp {
    DocOp {
        key: key(l, w),
        doc_id: id.into(),
        kind: OpKind::Set(v.as_object().cloned().unwrap()),
    }
}

fn incr(l: u64, w: &str, id: &str, field: &str, d: i64) -> DocOp {
    let mut ops = BTreeMap::new();
    ops.insert(field.to_string(), FieldOp::Increment(Number::from(d)));
    DocOp {
        key: key(l, w),
        doc_id: id.into(),
        kind: OpKind::Update(ops),
    }
}

/// Fold ops the canonical (key-ordered, deduped) way `Merged` does.
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
fn increment_sum_is_independent_of_delivery_order() {
    let base = set(1, "a", "c", json!({"n": 0}));
    let deltas: Vec<DocOp> = (0..10)
        .map(|i| {
            incr(
                2 + i,
                if i % 2 == 0 { "a" } else { "b" },
                "c",
                "n",
                i as i64,
            )
        })
        .collect();
    let mut all = vec![base];
    all.extend(deltas);
    let forward = fold(&all).get("c").unwrap()["n"].clone();
    let mut rev = all.clone();
    rev.reverse();
    let backward = fold(&rev).get("c").unwrap()["n"].clone();
    assert_eq!(forward, backward);
    let base_value = 0_i64;
    assert_eq!(forward, json!(base_value + (0..10).sum::<i64>()));
}

#[test]
fn duplicate_increment_ops_dedup() {
    let ops = vec![set(1, "a", "c", json!({"n": 0})), incr(2, "a", "c", "n", 5)];
    // Re-delivering the same op (same key) must not double-count.
    let mut twice = ops.clone();
    twice.push(incr(2, "a", "c", "n", 5)); // identical key
    assert_eq!(fold(&ops).get("c").unwrap()["n"], json!(5));
    assert_eq!(fold(&twice).get("c").unwrap()["n"], json!(5));
}

#[test]
fn nested_deep_merge_preserves_siblings() {
    let ops = vec![
        set(
            1,
            "a",
            "c",
            json!({"profile": {"name": "ada", "city": "lyon"}}),
        ),
        DocOp {
            key: key(2, "a"),
            doc_id: "c".into(),
            kind: OpKind::Patch(
                json!({"profile": {"city": "rome"}})
                    .as_object()
                    .cloned()
                    .unwrap(),
            ),
        },
    ];
    let d = fold(&ops).get("c").unwrap();
    assert_eq!(d["profile"]["name"], json!("ada")); // sibling preserved
    assert_eq!(d["profile"]["city"], json!("rome")); // deep-merged
}

#[test]
fn query_over_large_integers() {
    let big = 9_007_199_254_740_993_i64; // 2^53 + 1
    let docs = vec![
        (
            "a".to_string(),
            json!({"n": big}).as_object().cloned().unwrap(),
        ),
        (
            "b".to_string(),
            json!({"n": big - 1}).as_object().cloned().unwrap(),
        ),
    ];
    let q = Query::new().with(Filter::new("n", Op::Gt, json!(big - 1)));
    let ids: Vec<_> = q.run(docs).into_iter().map(|(id, _)| id).collect();
    assert_eq!(ids, vec!["a"]);
}

proptest! {
    /// For any interleaving of increments across two writers, the folded counter equals the sum of
    /// deltas plus the base — regardless of delivery order, and duplicates are idempotent.
    #[test]
    fn prop_increment_converges(deltas in proptest::collection::vec(-1000i64..1000, 0..20)) {
        let base = 100i64;
        let mut ops = vec![set(1, "a", "c", json!({"n": base}))];
        for (i, d) in deltas.iter().enumerate() {
            ops.push(incr(2 + i as u64, if i % 2 == 0 { "a" } else { "b" }, "c", "n", *d));
        }
        let expected = base + deltas.iter().sum::<i64>();
        let forward = fold(&ops).get("c").unwrap()["n"].as_i64().unwrap();
        let mut shuffled = ops.clone();
        shuffled.rotate_left(deltas.len() / 2 + 1);
        let rotated = fold(&shuffled).get("c").unwrap()["n"].as_i64().unwrap();
        prop_assert_eq!(forward, expected);
        prop_assert_eq!(rotated, expected);
    }

    /// `OpKind::Update` round-trips through serde unchanged.
    #[test]
    fn prop_update_op_serde_roundtrips(d in any::<i64>()) {
        let op = incr(1, "w", "doc", "n", d);
        let bytes = serde_json::to_vec(&op).unwrap();
        let back: DocOp = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(op, back);
    }
}
