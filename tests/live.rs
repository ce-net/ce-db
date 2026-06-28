//! Live integration tests for ce-db against real ephemeral CE nodes.
//!
//! A fresh 2-node loopback mesh is stood up per test (never the operator's :8844 node), each node
//! holding a [`Collection`] that follows the other's writer log. These exercise the actual ce-coord
//! `Merged` realtime path the `src/` model tests stub out:
//!
//! * **realtime convergence between TWO readers** — a write on A appears on B's collection live;
//! * **query filters / order / limit** over the converged state;
//! * **field-level LWW** — concurrent patches to different fields of one doc from both devices survive;
//! * **capability per collection** — a minted `db:write` grant authorizes its collection and only it;
//! * **snapshot bootstrap** — a writer compacts its log to a content-addressed blob (the cold-read
//!   path), and the checkpoint is real.
//!
//! If the release `ce` binary isn't built, every test logs the reason and returns early (pass).
//!
//! Run with: `cargo test -p ce-db --test live -- --nocapture`
//! Disable explicitly with: `CE_NO_LIVE=1 cargo test`.

mod harness;

use std::time::Duration;

use ce_iam_core::Resource;
use ce_coord::Coord;
use ce_db::{ABILITY_READ, ABILITY_WRITE, Collection, CollectionGrant, Dir, Filter, Op, Query};
use harness::{Node, live_available};
use serde_json::json;

/// Bring up a 2-node loopback mesh and a `Coord` on each node.
async fn two_node_mesh() -> anyhow::Result<Option<(Node, Node, Coord, Coord)>> {
    if !live_available() {
        return Ok(None);
    }
    let a = Node::start(None).await?;
    let b = Node::start(Some(&a.dial_addr())).await?;
    tokio::time::sleep(Duration::from_secs(3)).await;
    let ca = Coord::with_client(a.client.clone()).await?;
    let cb = Coord::with_client(b.client.clone()).await?;
    Ok(Some((a, b, ca, cb)))
}

/// Poll a condition with a deadline.
async fn within<F: FnMut() -> bool>(timeout: Duration, mut f: F) -> bool {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if std::time::Instant::now() > deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
}

const CONVERGE: Duration = Duration::from_secs(30);

fn obj(v: serde_json::Value) -> ce_db::Document {
    v.as_object().cloned().unwrap()
}

/// Realtime convergence between two readers: A writes a doc, B (following A) sees it live; then B
/// writes and A (following B) sees that — bidirectional realtime sync.
#[tokio::test]
async fn live_two_reader_realtime_convergence() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };

    // Each device opens the same collection, following the OTHER device's writer log.
    let coll_a = Collection::open(&ca, "users", std::slice::from_ref(&b.node_id)).await?;
    let coll_b = Collection::open(&cb, "users", std::slice::from_ref(&a.node_id)).await?;

    // A writes "ada"; B must converge to it.
    coll_a
        .set("ada", obj(json!({"name": "Ada", "age": 36})))
        .await?;
    assert!(
        within(CONVERGE, || coll_b.get("ada").is_some()).await,
        "reader B did not converge to A's write"
    );
    let ada = coll_b.get("ada").unwrap();
    assert_eq!(ada["name"], json!("Ada"));
    assert_eq!(ada["age"], json!(36));

    // B writes "bob"; A must converge to it (bidirectional).
    coll_b.set("bob", obj(json!({"name": "Bob"}))).await?;
    assert!(
        within(CONVERGE, || coll_a.get("bob").is_some()).await,
        "reader A did not converge to B's write"
    );
    assert_eq!(coll_a.get("bob").unwrap()["name"], json!("Bob"));

    // Both replicas agree on the full document set.
    assert!(within(CONVERGE, || coll_a.len() == 2 && coll_b.len() == 2).await);
    Ok(())
}

/// Field-level LWW: A and B concurrently patch DIFFERENT fields of the same doc; both survive on both
/// replicas (the Firestore field-merge story). A later patch to the SAME field wins by Lamport order.
#[tokio::test]
async fn live_field_level_lww() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll_a = Collection::open(&ca, "profiles", std::slice::from_ref(&b.node_id)).await?;
    let coll_b = Collection::open(&cb, "profiles", std::slice::from_ref(&a.node_id)).await?;

    // Seed the doc from A.
    coll_a.set("p1", obj(json!({"name": "Ada"}))).await?;
    assert!(
        within(CONVERGE, || coll_b.get("p1").is_some()).await,
        "seed converged"
    );

    // Concurrent field patches: A sets email, B sets phone.
    coll_a.patch("p1", obj(json!({"email": "ada@x"}))).await?;
    coll_b.patch("p1", obj(json!({"phone": "555"}))).await?;

    // Both fields survive on BOTH replicas (field-level CRDT merge).
    let ok = within(CONVERGE, || {
        let da = coll_a.get("p1");
        let db = coll_b.get("p1");
        matches!((&da, &db), (Some(x), Some(y))
            if x.contains_key("email") && x.contains_key("phone")
            && y.contains_key("email") && y.contains_key("phone"))
    })
    .await;
    assert!(
        ok,
        "concurrent field patches did not both survive on both replicas"
    );

    let da = coll_a.get("p1").unwrap();
    assert_eq!(da["name"], json!("Ada"));
    assert_eq!(da["email"], json!("ada@x"));
    assert_eq!(da["phone"], json!("555"));
    Ok(())
}

/// Query filters / order / limit over the live converged collection.
#[tokio::test]
async fn live_query_filters_order_limit() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll_a = Collection::open(&ca, "people", std::slice::from_ref(&b.node_id)).await?;
    let coll_b = Collection::open(&cb, "people", std::slice::from_ref(&a.node_id)).await?;

    coll_a
        .set("u1", obj(json!({"name": "ada", "age": 36})))
        .await?;
    coll_a
        .set("u2", obj(json!({"name": "bob", "age": 28})))
        .await?;
    coll_b
        .set("u3", obj(json!({"name": "cy", "age": 41})))
        .await?;

    // B converges to all three (two from A, one local).
    assert!(
        within(CONVERGE, || coll_b.len() == 3).await,
        "B converged to 3 docs"
    );

    // Filter: age > 30 → u1, u3.
    let q = Query::new().with(Filter::new("age", Op::Gt, json!(30)));
    let mut ids: Vec<String> = coll_b.query(&q).into_iter().map(|(id, _)| id).collect();
    ids.sort();
    assert_eq!(ids, vec!["u1", "u3"]);

    // Order desc by age, limit 2 → u3 (41), u1 (36).
    let q2 = Query::new().order("age", Dir::Desc).take(2);
    let ordered: Vec<String> = coll_b.query(&q2).into_iter().map(|(id, _)| id).collect();
    assert_eq!(ordered, vec!["u3", "u1"]);
    Ok(())
}

/// Capability per collection: a minted `db:write` grant authorizes its collection and rejects another;
/// attenuation to read-only drops write. Verified offline against the node's own identity as root.
#[tokio::test]
async fn live_capability_per_collection() -> anyhow::Result<()> {
    let Some((a, _b, _ca, _cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let owner = ce_identity::Identity::load_or_generate(&a.data_dir_path.join("identity"))?;
    let peer = ce_identity::Identity::load_or_generate(
        &std::env::temp_dir().join(format!("ce-db-live-peer-{}", std::process::id())),
    )?;
    let never = |_: &ce_identity::NodeId, _: u64| false;

    let g = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "users",
        &[ABILITY_READ, ABILITY_WRITE],
        Resource::Any,
        0,
        1,
    );
    // Authorizes its own collection for write.
    assert!(
        g.verify(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &peer.node_id(),
            ABILITY_WRITE,
            "users",
            &never
        )
        .is_ok()
    );
    // Rejects a different collection.
    assert!(
        g.verify(
            &owner.node_id(),
            &[],
            &[],
            1000,
            &peer.node_id(),
            ABILITY_WRITE,
            "orders",
            &never
        )
        .is_err()
    );

    // Attenuate to read-only and re-delegate to a third party.
    let leaf = ce_identity::Identity::load_or_generate(
        &std::env::temp_dir().join(format!("ce-db-live-leaf-{}", std::process::id())),
    )?;
    let narrowed = g.attenuate(&peer, leaf.node_id(), &[ABILITY_READ], Resource::Any, 0, 2)?;
    assert!(
        narrowed
            .verify(
                &owner.node_id(),
                &[],
                &[],
                1000,
                &leaf.node_id(),
                ABILITY_READ,
                "users",
                &never
            )
            .is_ok()
    );
    assert!(
        narrowed
            .verify(
                &owner.node_id(),
                &[],
                &[],
                1000,
                &leaf.node_id(),
                ABILITY_WRITE,
                "users",
                &never
            )
            .is_err(),
        "attenuation must have dropped write"
    );
    Ok(())
}

/// Snapshot bootstrap: a writer compacts its own log into a content-addressed blob (the cold-read /
/// Snapshot path), producing a real checkpoint `(base, cid)`.
#[tokio::test]
async fn live_snapshot_compact() -> anyhow::Result<()> {
    let Some((_a, _b, ca, _cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll = Collection::open(&ca, "snap", &[]).await?;
    // Produce a batch of writes so there's something to snapshot.
    for i in 0..12 {
        coll.set(&format!("k{i}"), obj(json!({"i": i}))).await?;
    }
    let cp = coll.compact().await?;
    assert!(
        cp.base > 0,
        "checkpoint base advanced past the snapshotted writes"
    );
    assert!(
        !cp.cid.is_empty(),
        "checkpoint produced a content-addressed cid"
    );
    Ok(())
}

/// Atomic increment composes across two concurrent writers (CRDT PN-counter): A and B both increment
/// the same counter; the converged value is the sum, never a clobber.
#[tokio::test]
async fn live_atomic_increment_composes() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll_a = Collection::open(&ca, "counters", std::slice::from_ref(&b.node_id)).await?;
    let coll_b = Collection::open(&cb, "counters", std::slice::from_ref(&a.node_id)).await?;

    coll_a.set("visits", obj(json!({"n": 0}))).await?;
    assert!(
        within(CONVERGE, || coll_b.get("visits").is_some()).await,
        "seed converged"
    );

    // Concurrent increments: A +5, B +3.
    coll_a.increment("visits", "n", 5).await?;
    coll_b.increment("visits", "n", 3).await?;

    // Both replicas converge to 0 + 5 + 3 = 8 (composition, not last-writer-wins).
    let ok = within(CONVERGE, || {
        let va = coll_a.get("visits").and_then(|d| d["n"].as_i64());
        let vb = coll_b.get("visits").and_then(|d| d["n"].as_i64());
        va == Some(8) && vb == Some(8)
    })
    .await;
    assert!(
        ok,
        "concurrent increments did not compose to the sum on both replicas"
    );
    Ok(())
}

/// A `WriteBatch` commits multiple documents atomically (from the issuer's perspective) under one
/// contiguous Lamport range; the peer converges to all of them.
#[tokio::test]
async fn live_write_batch_atomic() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll_a = Collection::open(&ca, "batched", std::slice::from_ref(&b.node_id)).await?;
    let coll_b = Collection::open(&cb, "batched", std::slice::from_ref(&a.node_id)).await?;

    coll_a
        .batch()
        .set("x", obj(json!({"v": 1})))
        .set("y", obj(json!({"v": 2})))
        .increment("counter", "n", 10)
        .commit()
        .await?;

    assert!(
        within(CONVERGE, || coll_b.len() == 3
            && coll_b.get("counter").is_some())
        .await,
        "peer did not converge to the whole batch"
    );
    assert_eq!(coll_b.get("counter").unwrap()["n"], json!(10));
    Ok(())
}

/// A `GuardedCollection` enforces capabilities on the data path: a read-only holder is denied writes,
/// and a write holder may write — over a real `Merged`-backed collection.
#[tokio::test]
async fn live_guarded_collection_enforces() -> anyhow::Result<()> {
    use ce_db::{AuthPolicy, GuardedCollection};
    let Some((a, _b, ca, _cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let owner = ce_identity::Identity::load_or_generate(&a.data_dir_path.join("identity"))?;
    let peer = ce_identity::Identity::load_or_generate(
        &std::env::temp_dir().join(format!("ce-db-guard-live-{}", std::process::id())),
    )?;

    // Read-only grant.
    let ro = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "secured",
        &[ABILITY_READ],
        Resource::Any,
        0,
        1,
    );
    let coll = Collection::open(&ca, "secured", &[]).await?;
    let guarded =
        GuardedCollection::new(coll, AuthPolicy::owner(owner.node_id(), peer.node_id(), ro));

    // Reads are allowed; writes are DENIED at the gate.
    assert!(guarded.snapshot().is_ok(), "read-only holder may read");
    assert!(
        guarded.set("doc", obj(json!({"v": 1}))).await.is_err(),
        "read-only holder must be denied a write"
    );

    // Now a write grant authorizes the write.
    let rw = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "secured",
        &[ABILITY_READ, ABILITY_WRITE],
        Resource::Any,
        0,
        2,
    );
    let coll2 = Collection::open(&ca, "secured", &[]).await?;
    let guarded2 = GuardedCollection::new(
        coll2,
        AuthPolicy::owner(owner.node_id(), peer.node_id(), rw),
    );
    guarded2.set("doc", obj(json!({"v": 1}))).await?;
    assert!(
        guarded2.get("doc")?.is_some(),
        "write holder wrote and read back"
    );
    Ok(())
}

/// Push-based realtime: with `start_realtime` running, a peer's write wakes a watcher on the other
/// node WITHOUT any manual `refresh()`/poll loop in the test.
#[tokio::test]
async fn live_push_realtime_wakes_watcher() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll_a = Collection::open(&ca, "rt", std::slice::from_ref(&b.node_id)).await?;
    let coll_b = Collection::open(&cb, "rt", std::slice::from_ref(&a.node_id)).await?;

    // B starts a background poller and watches — no manual refresh below.
    let mut rx = coll_b.start_realtime(Duration::from_millis(100));

    // A writes; B's watcher should wake purely from the background poller.
    coll_a.set("hello", obj(json!({"v": 1}))).await?;

    let woke = tokio::time::timeout(CONVERGE, async {
        loop {
            if rx.changed().await.is_err() {
                return false;
            }
            if coll_b.get("hello").is_some() {
                return true;
            }
        }
    })
    .await
    .unwrap_or(false);
    assert!(
        woke,
        "push-based watcher did not wake on a peer write without an external poll"
    );
    coll_b.stop_realtime();
    Ok(())
}

/// Subcollections: a document's subcollection is an independent, separately-converging collection
/// addressed hierarchically (collection/doc/sub).
#[tokio::test]
async fn live_subcollection_converges() -> anyhow::Result<()> {
    let Some((a, b, ca, cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let users_a = Collection::open(&ca, "users", std::slice::from_ref(&b.node_id)).await?;
    let users_b = Collection::open(&cb, "users", std::slice::from_ref(&a.node_id)).await?;

    users_a.set("ada", obj(json!({"name": "Ada"}))).await?;
    // A posts subcollection under users/ada.
    let posts_a = users_a
        .subcollection(&ca, "ada", "posts", std::slice::from_ref(&b.node_id))
        .await?;
    let posts_b = users_b
        .subcollection(&cb, "ada", "posts", std::slice::from_ref(&a.node_id))
        .await?;
    assert_eq!(posts_a.path(), "users/ada/posts");

    posts_a.set("p1", obj(json!({"title": "hello"}))).await?;
    assert!(
        within(CONVERGE, || posts_b.get("p1").is_some()).await,
        "subcollection did not converge across devices"
    );
    assert_eq!(posts_b.get("p1").unwrap()["title"], json!("hello"));
    // The subcollection is independent of the parent: parent has 1 doc, sub has 1 doc.
    assert_eq!(users_b.len(), 1);
    Ok(())
}

/// Concurrent same-device writes get a strictly-increasing, non-colliding Lamport sequence (the
/// single-critical-section allocator). 50 concurrent sets all land with distinct keys.
#[tokio::test]
async fn live_concurrent_writes_monotonic_lamport() -> anyhow::Result<()> {
    use std::sync::Arc;
    let Some((_a, _b, ca, _cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let coll = Arc::new(Collection::open(&ca, "race", &[]).await?);

    let mut handles = Vec::new();
    for i in 0..50u32 {
        let c = coll.clone();
        handles.push(tokio::spawn(async move {
            c.set(&format!("d{i}"), obj(json!({"i": i}))).await
        }));
    }
    for h in handles {
        h.await??;
    }
    // All 50 documents are present (no key collision dropped a write).
    assert!(
        within(CONVERGE, || coll.len() == 50).await,
        "concurrent writes lost documents"
    );
    assert_eq!(coll.len(), 50);
    Ok(())
}

/// Input-validation / DoS limits are enforced on the live write path: an oversize doc id and a doc
/// exceeding the configured byte limit are rejected.
#[tokio::test]
async fn live_limits_reject_bad_input() -> anyhow::Result<()> {
    use ce_db::Limits;
    let Some((_a, _b, ca, _cb)) = two_node_mesh().await? else {
        return Ok(());
    };
    let limits = Limits {
        max_doc_bytes: 64,
        max_doc_id_bytes: 8,
        ..Limits::default()
    };
    let coll = Collection::open_with_limits(&ca, "bounded", &[], limits).await?;

    // Oversize doc id rejected.
    assert!(
        coll.set("this-id-is-way-too-long", obj(json!({"v": 1})))
            .await
            .is_err()
    );
    // Oversize document rejected.
    assert!(
        coll.set("ok", obj(json!({"blob": "x".repeat(200)})))
            .await
            .is_err()
    );
    // A small, well-formed write is accepted.
    coll.set("ok", obj(json!({"v": 1}))).await?;
    assert_eq!(coll.get("ok").unwrap()["v"], json!(1));
    Ok(())
}
