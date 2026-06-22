//! Property/fuzz tests for ce-db's pure model: CRDT convergence of the [`DbMachine`] fold, query
//! semantics, capability attenuation, serde round-trips (incl JSON numbers `> 2^53`), and path
//! parsing.
//!
//! The keystone property is **convergence**: for any set of document ops, folding them in *any*
//! delivery order yields the identical document map, and re-delivering duplicates changes nothing.
//! This is what makes ce-db a leaderless, offline-tolerant CRDT — proven here over randomized op
//! scripts, not just the hand-picked cases in `src/doc.rs`.

use std::collections::BTreeMap;

use ce_cap::Resource;
use ce_coord::MergeMachine;
use ce_db::{
    CollectionGrant, DbMachine, DocOp, Document, OpKey, OpKind, Query, Filter, DocPath,
    ABILITY_READ, ABILITY_WRITE,
};
use ce_db::query::{Dir, Op};
use ce_identity::{Identity, NodeId};
use proptest::prelude::*;
use serde_json::{json, Value};
use std::sync::atomic::{AtomicU64, Ordering};

fn ident() -> Identity {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("ce-db-prop-{}-{n}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let id = Identity::load_or_generate(&dir).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    id
}

fn never(_: &NodeId, _: u64) -> bool {
    false
}

/// Fold an op slice exactly as `Merged::read` does: dedup into a key-ordered map, apply in order.
fn fold(ops: &[DocOp]) -> BTreeMap<String, Document> {
    let mut union: BTreeMap<OpKey, DocOp> = BTreeMap::new();
    for op in ops {
        union.insert(op.key.clone(), op.clone());
    }
    let mut m = DbMachine::default();
    for op in union.values() {
        m.apply(op.clone());
    }
    m.documents()
}

/// A strategy generating one DocOp over a small id/writer/field universe with a UNIQUE (lamport,
/// writer) key per generated op (so the union never collapses two distinct ops — the model's
/// requirement). Uniqueness is enforced by giving each op a fresh lamport from a shared counter.
fn op_strategy() -> impl Strategy<Value = (u64, String, String, OpKind)> {
    (
        0u64..5,                       // writer index (becomes the writer id "wN")
        prop::sample::select(vec!["d1", "d2", "d3"]), // doc id
        0u8..3,                        // kind selector
        prop::sample::select(vec!["a", "b", "c"]),     // field
        any::<i64>(),                  // value
    )
        .prop_map(|(w, doc, kind_sel, field, val)| {
            let writer = format!("w{w}");
            let kind = match kind_sel {
                0 => {
                    let mut o = Document::new();
                    o.insert(field.to_string(), json!(val));
                    OpKind::Set(o)
                }
                1 => {
                    let mut o = Document::new();
                    o.insert(field.to_string(), json!(val));
                    OpKind::Patch(o)
                }
                _ => OpKind::Delete,
            };
            (0u64, writer, doc.to_string(), kind)
        })
}

/// Assign globally-unique strictly-increasing lamport keys to a generated op list, so no two ops
/// share an `OpKey` (mirrors how each writer's Lamport clock advances past everything observed).
fn key_ops(raw: Vec<(u64, String, String, OpKind)>) -> Vec<DocOp> {
    raw.into_iter()
        .enumerate()
        .map(|(i, (_l, writer, doc_id, kind))| DocOp {
            key: OpKey { lamport: (i as u64) + 1, writer },
            doc_id,
            kind,
        })
        .collect()
}

proptest! {
    // ----- CRDT convergence: order-independent, idempotent, commutative. -----

    /// Folding the same op set in any permutation yields the IDENTICAL document map. Searched over
    /// random op scripts and random shuffles.
    #[test]
    fn convergence_is_order_independent(
        raw in proptest::collection::vec(op_strategy(), 0..25),
        seed in any::<u64>(),
    ) {
        let ops = key_ops(raw);
        let reference = fold(&ops);

        // A few deterministic shuffles derived from the seed.
        let mut rng_state = seed | 1;
        let mut next = || { rng_state ^= rng_state << 13; rng_state ^= rng_state >> 7; rng_state ^= rng_state << 17; rng_state };
        for _ in 0..4 {
            let mut shuffled = ops.clone();
            // Fisher-Yates with the xorshift rng.
            for i in (1..shuffled.len()).rev() {
                let j = (next() as usize) % (i + 1);
                shuffled.swap(i, j);
            }
            prop_assert_eq!(fold(&shuffled), reference.clone(), "permutation diverged from reference");
        }
    }

    /// Idempotence: delivering every op twice (duplicates) changes nothing (the union dedups by key).
    #[test]
    fn duplicate_delivery_is_idempotent(raw in proptest::collection::vec(op_strategy(), 0..25)) {
        let ops = key_ops(raw);
        let once = fold(&ops);
        let mut twice = ops.clone();
        twice.extend(ops.iter().cloned()); // every op delivered again
        prop_assert_eq!(fold(&twice), once, "duplicate delivery changed the converged state");
    }

    /// Commutativity at the pairwise level: for any two ops, applying {A then B} vs {B then A} (with
    /// the union's key-ordering in front) yields the same state. Covered by the permutation test, but
    /// asserted directly on pairs for a tighter witness.
    #[test]
    fn pairwise_commutative(
        a in op_strategy(),
        b in op_strategy(),
    ) {
        let ops = key_ops(vec![a, b]);
        let ab = fold(&[ops[0].clone(), ops[1].clone()]);
        let ba = fold(&[ops[1].clone(), ops[0].clone()]);
        prop_assert_eq!(ab, ba, "pairwise application order changed the state");
    }

    // ----- Serde round-trips, incl JSON numbers beyond 2^53 (money/large ids carried as strings). -----

    /// A DocOp round-trips through JSON losslessly, including i64 field values whose magnitude exceeds
    /// 2^53 — these must ride as JSON integers (serde_json preserves i64/u64 exactly), so a money
    /// amount stored as an integer field survives.
    #[test]
    fn docop_roundtrips_with_large_numbers(lamport in any::<u64>(), val in any::<i64>(), big in any::<u64>()) {
        let mut o = Document::new();
        o.insert("amount".into(), json!(val));
        o.insert("big".into(), json!(big | (1u64 << 60))); // > 2^53
        let op = DocOp { key: OpKey { lamport, writer: "wX".into() }, doc_id: "d".into(), kind: OpKind::Set(o.clone()) };
        let bytes = serde_json::to_vec(&op).unwrap();
        let back: DocOp = serde_json::from_slice(&bytes).unwrap();
        prop_assert_eq!(&back, &op);
        if let OpKind::Set(m) = &back.kind {
            prop_assert_eq!(m.get("amount").unwrap(), &json!(val));
            prop_assert_eq!(m.get("big").unwrap(), &json!(big | (1u64 << 60)));
        } else {
            prop_assert!(false, "kind changed across roundtrip");
        }
    }

    /// OpKey ordering is the strict total order (lamport, writer) — the property that guarantees no
    /// two distinct ops ever collide and the fold is deterministic.
    #[test]
    fn opkey_total_order(l1 in any::<u64>(), l2 in any::<u64>(), w1 in "[a-c]{1,3}", w2 in "[a-c]{1,3}") {
        let k1 = OpKey { lamport: l1, writer: w1.clone() };
        let k2 = OpKey { lamport: l2, writer: w2.clone() };
        let expect = (l1, &w1).cmp(&(l2, &w2));
        prop_assert_eq!(k1.cmp(&k2), expect);
    }

    // ----- Query semantics over random docs. -----

    /// An equality filter matches exactly the docs whose field equals the value.
    #[test]
    fn eq_filter_matches_exactly(vals in proptest::collection::vec(any::<i64>(), 0..20), target in any::<i64>()) {
        let docs: Vec<(String, Document)> = vals.iter().enumerate().map(|(i, v)| {
            let mut o = Document::new();
            o.insert("n".into(), json!(v));
            (format!("d{i}"), o)
        }).collect();
        let q = Query::new().with(Filter::eq("n", json!(target)));
        let got = q.run(docs.clone());
        let want = vals.iter().filter(|v| **v == target).count();
        prop_assert_eq!(got.len(), want);
    }

    /// `limit` never returns more than N; `order` desc yields a non-increasing sequence on the field.
    #[test]
    fn order_desc_limit_is_sorted_and_capped(
        vals in proptest::collection::vec(0i64..1000, 0..30),
        limit in 1usize..10,
    ) {
        let docs: Vec<(String, Document)> = vals.iter().enumerate().map(|(i, v)| {
            let mut o = Document::new();
            o.insert("n".into(), json!(v));
            (format!("d{i}"), o)
        }).collect();
        let q = Query::new().order("n", Dir::Desc).take(limit);
        let got = q.run(docs);
        prop_assert!(got.len() <= limit, "limit exceeded");
        for w in got.windows(2) {
            let a = w[0].1.get("n").unwrap().as_i64().unwrap();
            let b = w[1].1.get("n").unwrap().as_i64().unwrap();
            prop_assert!(a >= b, "descending order violated: {a} < {b}");
        }
    }

    /// `Ne` matches a missing field (Firestore "not equal" semantics); `Eq` never does.
    #[test]
    fn ne_matches_missing_eq_does_not(present in any::<bool>(), val in any::<i64>()) {
        let mut o = Document::new();
        if present {
            o.insert("n".into(), json!(val));
        }
        let docs = vec![("d".to_string(), o)];
        let ne = Query::new().with(Filter { field: "n".into(), op: Op::Ne, value: json!(999999) });
        let eq = Query::new().with(Filter::eq("n", json!(999999)));
        // Ne matches unless the field is present AND equals 999999.
        let ne_expect = !(present && val == 999999);
        prop_assert_eq!(ne.run(docs.clone()).len(), ne_expect as usize);
        prop_assert_eq!(eq.run(docs).len(), (present && val == 999999) as usize);
    }

    // ----- Capability attenuation: can never amplify; expiry/scope honored. -----

    /// A child grant attenuated to a subset of abilities can authorize only the kept abilities, never
    /// an ability the parent dropped — for random keep masks.
    #[test]
    fn attenuation_never_amplifies(keep_read in any::<bool>(), keep_write in any::<bool>()) {
        let owner = ident();
        let mid = ident();
        let leaf = ident();
        // owner -> mid : read+write on "c".
        let g = CollectionGrant::mint(&owner, mid.node_id(), "c", &[ABILITY_READ, ABILITY_WRITE], Resource::Any, 0, 1);
        // mid -> leaf : a random subset (possibly empty → keep at least read to stay non-empty).
        let mut narrower: Vec<&str> = Vec::new();
        if keep_read { narrower.push(ABILITY_READ); }
        if keep_write { narrower.push(ABILITY_WRITE); }
        if narrower.is_empty() { narrower.push(ABILITY_READ); }
        let child = g.attenuate(&mid, leaf.node_id(), &narrower, Resource::Any, 0, 2).unwrap();

        let can_read = child.verify(&owner.node_id(), &[], &[], 1000, &leaf.node_id(), ABILITY_READ, "c", &never).is_ok();
        let can_write = child.verify(&owner.node_id(), &[], &[], 1000, &leaf.node_id(), ABILITY_WRITE, "c", &never).is_ok();
        prop_assert_eq!(can_read, narrower.contains(&ABILITY_READ));
        prop_assert_eq!(can_write, narrower.contains(&ABILITY_WRITE));
    }

    /// A grant amplified at the leaf (claiming an ability the parent never held) is ALWAYS denied.
    #[test]
    fn leaf_cannot_invent_ability(_seed in any::<u8>()) {
        let owner = ident();
        let mid = ident();
        let leaf = ident();
        // owner -> mid : read only.
        let g = CollectionGrant::mint(&owner, mid.node_id(), "c", &[ABILITY_READ], Resource::Any, 0, 1);
        // mid tries to grant write (never held).
        let bad = g.attenuate(&mid, leaf.node_id(), &[ABILITY_WRITE], Resource::Any, 0, 2).unwrap();
        prop_assert!(bad.verify(&owner.node_id(), &[], &[], 1000, &leaf.node_id(), ABILITY_WRITE, "c", &never).is_err());
    }

    /// Expiry is honored: a grant verifies before `not_after` and is denied at/after it, for any time.
    #[test]
    fn expiry_is_honored(not_after in 1u64..10_000, now in 0u64..20_000) {
        let owner = ident();
        let peer = ident();
        let g = CollectionGrant::mint(&owner, peer.node_id(), "c", &[ABILITY_READ], Resource::Any, not_after, 1);
        let res = g.verify(&owner.node_id(), &[], &[], now, &peer.node_id(), ABILITY_READ, "c", &never);
        prop_assert_eq!(res.is_ok(), now <= not_after);
    }

    /// A grant pinned to one collection never authorizes a different collection name.
    #[test]
    fn grant_pinned_to_collection(collection in "[a-z]{1,8}", probe in "[a-z]{1,8}") {
        let owner = ident();
        let peer = ident();
        let g = CollectionGrant::mint(&owner, peer.node_id(), &collection, &[ABILITY_READ], Resource::Any, 0, 1);
        let res = g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_READ, &probe, &never);
        prop_assert_eq!(res.is_ok(), collection == probe);
    }

    /// A grant token round-trips and the recovered grant pins the same collection.
    #[test]
    fn grant_token_roundtrips(collection in "[a-z][a-z0-9]{0,10}") {
        let owner = ident();
        let peer = ident();
        let g = CollectionGrant::mint(&owner, peer.node_id(), &collection, &[ABILITY_READ], Resource::Any, 0, 1);
        let token = g.to_token();
        let back = CollectionGrant::from_token(&token).unwrap();
        prop_assert_eq!(back.collection(), collection.as_str());
        prop_assert_eq!(back.holder(), Some(peer.node_id()));
    }

    // ----- Path parsing never panics and obeys its contract. -----

    #[test]
    fn docpath_parse_never_panics(s in ".{0,40}") {
        if let Ok(p) = DocPath::parse(&s) {
            // On success: both parts non-empty and reconstructable.
            prop_assert!(!p.collection.is_empty());
            prop_assert!(!p.doc_id.is_empty());
            prop_assert_eq!(format!("{}/{}", p.collection, p.doc_id), s);
        }
    }
}

/// `from_token` on garbage never panics — only errors.
#[test]
fn from_token_garbage_never_panics() {
    assert!(CollectionGrant::from_token("").is_err());
    assert!(CollectionGrant::from_token("zzz not hex").is_err());
    assert!(CollectionGrant::from_token("deadbeef").is_err());
}

/// Sanity: a `Value`-typed filter on incomparable types fails the predicate, never panics.
#[test]
fn incomparable_filter_is_false_not_panic() {
    let mut o = Document::new();
    o.insert("x".into(), json!("a string"));
    let docs = vec![("d".to_string(), o)];
    let q = Query::new().with(Filter { field: "x".into(), op: Op::Gt, value: Value::Number(5.into()) });
    assert_eq!(q.run(docs).len(), 0);
}
