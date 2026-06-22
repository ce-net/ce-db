//! Realtime sync / convergence tests.
//!
//! The cheap, always-runnable tests model what ce-coord's `Merged` layer does under the hood: two
//! replicas each keep their own writer log, exchange ops, and fold the **union** with the same
//! [`DbMachine`] the live [`Collection`](ce_db::Collection) uses. This proves the realtime
//! convergence guarantee (two readers reach the same documents regardless of delivery order, and
//! field-level edits from concurrent writers both survive) **without needing a running node** — the
//! transport is the only piece those leave out, and it is ce-coord's own tested concern.
//!
//! A live, node-backed two-collection test is provided but `#[ignore]`d, so `cargo test` is green in
//! CI yet the real mesh path can be exercised on demand with `cargo test -- --ignored` when a node is
//! up (`ce start`).

use std::collections::BTreeMap;

use ce_db::{DbMachine, DocOp, OpKey, OpKind};
use ce_coord::MergeMachine;
use serde_json::json;

/// A tiny in-memory stand-in for a device's replica: it accumulates the ops it has "received" (its
/// own writes plus peers' ops delivered over the mesh) and folds them with the production
/// [`DbMachine`] exactly as `Merged::read` does.
#[derive(Default)]
struct Replica {
    received: BTreeMap<OpKey, DocOp>,
}

impl Replica {
    /// Deliver an op to this replica (idempotent — duplicates dedup by key, like the real union).
    fn deliver(&mut self, op: DocOp) {
        self.received.insert(op.key.clone(), op);
    }

    /// Deliver a batch in the given order.
    fn deliver_all(&mut self, ops: &[DocOp]) {
        for op in ops {
            self.deliver(op.clone());
        }
    }

    /// Materialize: fold the key-ordered union into the document map (the converged state).
    fn docs(&self) -> BTreeMap<String, ce_db::Document> {
        let mut m = DbMachine::default();
        for op in self.received.values() {
            m.apply(op.clone());
        }
        m.documents()
    }
}

fn set(l: u64, w: &str, id: &str, v: serde_json::Value) -> DocOp {
    DocOp {
        key: OpKey { lamport: l, writer: w.into() },
        doc_id: id.into(),
        kind: OpKind::Set(v.as_object().cloned().unwrap()),
    }
}

fn patch(l: u64, w: &str, id: &str, v: serde_json::Value) -> DocOp {
    DocOp {
        key: OpKey { lamport: l, writer: w.into() },
        doc_id: id.into(),
        kind: OpKind::Patch(v.as_object().cloned().unwrap()),
    }
}

/// Two readers, fed the same ops in *different* orders, converge to identical documents. This is the
/// "show realtime sync between two readers" guarantee at the data layer.
#[test]
fn two_readers_converge_regardless_of_delivery_order() {
    // Writer A and writer B each produce concurrent edits.
    let a_ops = vec![
        set(1, "A", "ada", json!({"name": "Ada", "role": "math"})),
        patch(3, "A", "ada", json!({"role": "compiler"})),
    ];
    let b_ops = vec![
        patch(2, "B", "ada", json!({"city": "London"})),
        set(4, "B", "bob", json!({"name": "Bob"})),
    ];

    // Reader 1 gets A first, then B.
    let mut r1 = Replica::default();
    r1.deliver_all(&a_ops);
    r1.deliver_all(&b_ops);

    // Reader 2 gets them interleaved / reversed — the realistic mesh out-of-order case.
    let mut r2 = Replica::default();
    r2.deliver(b_ops[1].clone());
    r2.deliver(a_ops[1].clone());
    r2.deliver(b_ops[0].clone());
    r2.deliver(a_ops[0].clone());
    // duplicate delivery must be a no-op
    r2.deliver(a_ops[0].clone());

    let d1 = r1.docs();
    let d2 = r2.docs();
    assert_eq!(d1, d2, "two readers diverged");

    // Concurrent field edits from both writers survived (field-level CRDT merge).
    let ada = &d1["ada"];
    assert_eq!(ada["name"], json!("Ada"));
    assert_eq!(ada["role"], json!("compiler")); // A's later patch (lamport 3) wins over the set's role
    assert_eq!(ada["city"], json!("London")); // B's concurrent field preserved
    assert_eq!(d1["bob"]["name"], json!("Bob"));
}

/// A reader that joins late and replays the whole op set reaches the same state as one that saw every
/// op live — the Snapshot/bootstrap equivalence at the document level.
#[test]
fn late_reader_replay_equals_live_reader() {
    let ops = vec![
        set(1, "A", "u1", json!({"v": 1})),
        set(2, "A", "u2", json!({"v": 2})),
        patch(3, "B", "u1", json!({"w": 9})),
        DocOp { key: OpKey { lamport: 4, writer: "A".into() }, doc_id: "u2".into(), kind: OpKind::Delete },
    ];

    let mut live = Replica::default();
    for op in &ops {
        live.deliver(op.clone()); // saw each as it happened
    }

    let mut late = Replica::default();
    late.deliver_all(&ops); // bootstrapped all at once

    assert_eq!(live.docs(), late.docs());
    let d = live.docs();
    assert_eq!(d["u1"]["v"], json!(1));
    assert_eq!(d["u1"]["w"], json!(9));
    assert!(!d.contains_key("u2")); // deleted
}

/// Last-writer-wins ties break deterministically by writer id, so two readers never disagree even
/// when two writers pick the same lamport.
#[test]
fn lamport_ties_break_deterministically() {
    let from_a = set(5, "A", "x", json!({"owner": "A"}));
    let from_b = set(5, "B", "x", json!({"owner": "B"}));

    let mut r1 = Replica::default();
    r1.deliver(from_a.clone());
    r1.deliver(from_b.clone());

    let mut r2 = Replica::default();
    r2.deliver(from_b);
    r2.deliver(from_a);

    assert_eq!(r1.docs(), r2.docs());
    // (5,"B") > (5,"A") lexicographically on the writer id, so B wins on both.
    assert_eq!(r1.docs()["x"]["owner"], json!("B"));
}

/// Live two-collection realtime sync over an actual CE node. Requires `ce start` (or any node on
/// `CE_API_URL`). Ignored by default so CI stays green without infrastructure.
///
/// Run with: `cargo test --test realtime_sync -- --ignored --nocapture`.
#[tokio::test]
#[ignore = "requires a running CE node"]
async fn live_two_reader_realtime_sync() -> anyhow::Result<()> {
    use ce_coord::Coord;
    use ce_db::Collection;
    use std::time::Duration;

    // Both replicas run against the same local node here (one process, two Collections following each
    // other's writes). On separate machines you would point each at its own node and pass the other's
    // NodeId — the code path is identical.
    let coord = Coord::connect().await?;
    let me = coord.node_id().to_string();

    let writer = Collection::open(&coord, "live-users", &[]).await?;
    let reader = Collection::open(&coord, "live-users", std::slice::from_ref(&me)).await?;

    let mut rx = reader.watch();

    writer
        .set("ada", json!({"name": "Ada", "age": 36}).as_object().unwrap().clone())
        .await?;

    // Wait for the reader to converge (the pump is ~250ms; give it room).
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    loop {
        if reader.get("ada").is_some() {
            break;
        }
        tokio::select! {
            _ = rx.changed() => {}
            _ = tokio::time::sleep(Duration::from_millis(300)) => {}
        }
        if tokio::time::Instant::now() > deadline {
            anyhow::bail!("reader did not converge within 10s");
        }
    }

    let ada = reader.get("ada").expect("reader converged");
    assert_eq!(ada["name"], json!("Ada"));
    assert_eq!(ada["age"], json!(36));
    Ok(())
}
