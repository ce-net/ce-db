//! End-to-end ce-db example: open a collection on the local node, write/patch/atomic-op, query with
//! ordering and pagination, and watch realtime changes with per-document deltas.
//!
//! Run against a live node (`ce start` must be running):
//!
//! ```sh
//! cargo run --example basic
//! ```
//!
//! With no node running, `Coord::connect` returns an error and the example prints it and exits —
//! that's expected (the example needs the local CE node).

use std::time::Duration;

use ce_coord::Coord;
use ce_db::{Collection, Dir, Filter, Op, Query};
use serde_json::json;

fn obj(v: serde_json::Value) -> ce_db::Document {
    v.as_object().cloned().unwrap_or_default()
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt().with_env_filter("info").init();

    let coord = match Coord::connect().await {
        Ok(c) => c,
        Err(e) => {
            eprintln!("could not connect to the local CE node (is `ce start` running?): {e}");
            return Ok(());
        }
    };

    let users = Collection::open(&coord, "example_users", &[]).await?;

    // --- writes ---
    users
        .set(
            "ada",
            obj(json!({"name": "Ada", "age": 36, "tags": ["math"]})),
        )
        .await?;
    users
        .set(
            "bob",
            obj(json!({"name": "Bob", "age": 28, "tags": ["art"]})),
        )
        .await?;
    users
        .patch("ada", obj(json!({"email": "ada@example.com"})))
        .await?; // field-level merge
    users.increment("ada", "logins", 1).await?; // atomic counter
    users.array_union("ada", "tags", vec![json!("cs")]).await?; // atomic set add

    // --- batched write (atomic from this device's view) ---
    users
        .batch()
        .set("cy", obj(json!({"name": "Cy", "age": 41})))
        .increment("metrics", "signups", 1)
        .commit()
        .await?;

    // --- reads & query ---
    if let Some(ada) = users.get("ada") {
        println!("ada = {}", serde_json::to_string_pretty(&ada)?);
    }

    let q = Query::new()
        .with(Filter::new("age", Op::Gt, json!(30)))
        .order("age", Dir::Desc)
        .take(10);
    println!("\nover-30, newest first:");
    for (id, doc) in users.query(&q) {
        println!("  {id}: {}", serde_json::to_string(&doc)?);
    }

    // --- pagination via a cursor ---
    let page = Query::new()
        .order("age", Dir::Asc)
        .start_after(vec![json!(28)])
        .take(5);
    println!("\npage after age=28:");
    for (id, _) in users.query(&page) {
        println!("  {id}");
    }

    // --- realtime: react to the next change (push-based) ---
    let _rx = users.start_realtime(Duration::from_millis(200));
    let mut changes = users.changes();
    println!("\nwatching for one change batch (Ctrl-C to exit)...");
    if let Some(batch) = changes.next().await {
        for c in batch {
            println!("  {:?} {}", c.kind, c.doc_id);
        }
    }

    Ok(())
}
