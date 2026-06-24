# ce-db

**A Firestore-class realtime document database, built entirely out of CE primitives.**

ce-db gives you collections of JSON documents that sync in realtime across devices, work offline, and
converge without a server — by *composing* what CE already provides, never by changing the node. It is
the "ce-db (Firestore)" entry from the [CE Cloud portfolio](../PLAN/12-google-infra-portfolio.md):

> *ce-coord **Merged** CRDT collections + realtime pubsub + **Snapshot** compaction. The app-builder
> magnet.*

| Firestore concept | ce-db | CE primitive underneath |
|---|---|---|
| Document store | `Collection` of JSON documents | **ce-coord `Merged`** (multi-writer CRDT) |
| Offline + multi-device merge | field-level last-writer-wins fold | ce-coord `Merged` union-of-logs |
| Subcollections (`a/x/b/y`) | `Collection::subcollection` | a separate `Merged` log per hierarchical path |
| `onSnapshot` realtime listeners | `Collection::changes` (per-doc deltas) + a push poller | mesh pubsub (via ce-coord) |
| `FieldValue.*` atomic ops | `increment` / `array_union` / `array_remove` / serverTimestamp | CRDT-friendly `Update` ops |
| `WriteBatch` / `runTransaction` | `Collection::batch` / `run_transaction` | contiguous Lamport range / optimistic re-check |
| Cursors & pagination | `start_at`/`start_after`/`end_at`/`end_before`/`offset` | total-ordered sort + doc-id tie-break |
| Cheap cold reads / compaction | `Collection::compact` | ce-coord `Snapshot` + CE blobs |
| Security rules / IAM | `GuardedCollection` + `CollectionGrant` | **ce-cap** signed attenuating chains |
| Large fields | store a blob CID as a string field | CE blobs / ce-pin |

No new node endpoints, no allowlists, no stored `ip:port`. ce-db is pure SDK/app tier: it depends on
`ce-rs` (HTTP SDK), `ce-coord` (`Merged`/`Snapshot`), and `ce-cap` (authorization), all by path.

---

## How it converges (the important part)

Every device is **both a writer and a reader**. A write is one `DocOp` (`Set`, `Patch`, `Update`, or
`Delete`) stamped with a strict total-order key `(lamport, writer_id)`, appended to *that device's
own* writer log. ce-coord's `Merged` layer replicates every device's log over the mesh and takes the
**key-ordered union** of all of them; ce-db folds that union with a deterministic state machine
(`DbMachine`):

- **`Set`** replaces a whole document — whole-document last-writer-wins.
- **`Patch`** merges fields — *each field independently* last-writer-wins, so two devices editing
  different fields of the same doc **both survive** (the field-level CRDT merge Firestore offers).
  A `null` field value deletes that field, and **nested objects deep-merge** field by field.
- **`Update`** applies CRDT-friendly atomic ops: `increment` (a PN-counter — concurrent increments
  *sum*, never clobber), `arrayUnion`/`arrayRemove` (applied deterministically in key order).
- **`Delete`** tombstones a document; a later `Set`/`Patch`/`Update` resurrects it.

Because the fold is a pure function of the op *set*, **every replica converges to the same documents
regardless of delivery order** — leaderless, offline-tolerant, no Raft, no quorum. The chain (not
ce-db) is where you reach for global uniqueness/money invariants a CRDT provably can't do.

---

## Library

```rust
use ce_coord::Coord;
use ce_db::{Collection, Query, Filter, Op, Dir};
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let coord = Coord::connect().await?;                  // wraps the local CE node
    let users = Collection::open(&coord, "users", &[]).await?;

    // set / patch / delete / atomic ops
    users.set("ada", json!({"name": "Ada", "age": 36}).as_object().unwrap().clone()).await?;
    users.patch("ada", json!({"email": "ada@x"}).as_object().unwrap().clone()).await?;
    users.set_field("ada", "verified", json!(true)).await?;
    users.increment("ada", "logins", 1).await?;           // FieldValue.increment
    users.array_union("ada", "tags", vec![json!("vip")]).await?;

    // read
    let ada = users.get("ada").unwrap();
    assert_eq!(ada["email"], json!("ada@x"));

    // query: everyone older than 30, newest first, top 5
    let q = Query::new()
        .with(Filter::new("age", Op::Gt, json!(30)))
        .order("age", Dir::Desc)
        .take(5);
    for (id, doc) in users.query(&q) {
        println!("{id}: {doc:?}");
    }

    // pagination: page 2 after a known cursor value
    let page2 = Query::new().order("age", Dir::Asc).start_after(vec![json!(36)]).take(10);
    let _next = users.query(&page2);

    // batched write (atomic from this device's view)
    users.batch()
        .set("bob", json!({"name": "Bob"}).as_object().unwrap().clone())
        .increment("counters", "users", 1)
        .commit().await?;

    Ok(())
}
```

### Realtime (push-based, with per-document deltas)

`watch()` fires on any change. ce-coord only *folds* a peer's op when something reads, so call
`start_realtime(..)` once to spawn a background poller — then peer writes wake your watcher with **no
external loop**. `changes()` yields Firestore-style per-document deltas (added / modified / removed):

```rust
use std::time::Duration;

let _rx = users.start_realtime(Duration::from_millis(100));   // background poller
let mut stream = users.changes();
while let Some(batch) = stream.next().await {
    for change in batch {
        println!("{:?} {}", change.kind, change.doc_id);       // Added / Modified / Removed
    }
}
```

To follow other devices, pass their NodeIds when opening:
`Collection::open(&coord, "users", &[peer_a_hex, peer_b_hex]).await?` (or `add_writer` later).

### Subcollections

```rust
let users = Collection::open(&coord, "users", &[]).await?;
users.set("ada", json!({"name": "Ada"}).as_object().unwrap().clone()).await?;
// users/ada/posts — an independent, separately-gated, separately-converging collection
let posts = users.subcollection(&coord, "ada", "posts", &[]).await?;
posts.set("p1", json!({"title": "hello"}).as_object().unwrap().clone()).await?;
assert_eq!(posts.path(), "users/ada/posts");
```

---

## Capability **enforcement** (per collection)

`CollectionGrant` is a verification toolkit; **`GuardedCollection` is the enforcement point**. It runs
`CollectionGrant::verify` for the right ability before every operation — `db:read` for
get/query/watch, `db:write` for set/patch/delete/increment, `db:admin` for compact — and denies
unauthorized, expired, wrong-collection, wrong-requester, untrusted-root, or revoked grants *before*
touching any state:

```rust
use ce_db::{Collection, GuardedCollection, AuthPolicy, CollectionGrant, Resource,
            ABILITY_READ, ABILITY_WRITE};

// owner mints a read-only grant for a peer, scoped to one collection
let grant = CollectionGrant::mint(&owner, peer_id, "users", &[ABILITY_READ], Resource::Any, 0, 1);

let coll = Collection::open(&coord, "users", &[]).await?;
let guarded = GuardedCollection::new(coll, AuthPolicy::owner(owner_id, peer_id, grant));

guarded.snapshot()?;                          // OK — db:read granted
assert!(guarded.set("x", doc).await.is_err()); // DENIED — no db:write
```

Access is a signed `ce-cap` chain, verified **offline** — strictly better than an ACL table. The
collection owner mints a grant scoped to one collection and a set of abilities; the holder may
`attenuate` (never amplify) and re-delegate. Abilities are opaque strings owned by ce-db (`db:read`,
`db:write`, `db:admin`). Revocation = on-chain `RevokeCapability` + expiry (pass an `is_revoked`
closure to `AuthPolicy::with_revocation`).

```sh
# owner mints a read+write grant for a peer, scoped to `users`, 24h expiry:
ce-db grant <PEER_NODE_ID> users --abilities db:read,db:write --expires 86400
ce-db inspect <TOKEN>     # prints collection / holder / root / abilities / expiry
```

---

## CLI

The `ce-db` binary talks to the local node (`ce start` must be running). Documents are addressed
`<collection>/<doc_id>`.

```sh
# write
ce-db set   users/ada '{"name":"Ada","age":36}'
ce-db patch users/ada '{"email":"ada@x"}'      # field-level merge (nested deep-merge)
ce-db incr  users/ada logins 1                 # atomic increment
ce-db delete users/ada

# read
ce-db get users/ada

# query — repeatable --where field:op:value
#   op = eq|ne|gt|ge|lt|le|contains|in|notin|array-contains-any
#   --order repeatable/comma-separated for multi-field sort; --offset for pagination
ce-db query users --where age:gt:30 --where 'tags:contains:cs' \
                  --order age:desc,name:asc --offset 0 --limit 5

# realtime watch — prints a snapshot on every change
ce-db --peers <PEER_NODE_ID> watch users

# status & compaction
ce-db status users
ce-db compact users        # snapshot this device's log to a content-addressed blob
```

A *bare word* `--where` value is a string (`name:eq:ada`); a value that looks like JSON
(`age:gt:30`, `ok:eq:true`, `tags:in:[1,2]`) must be valid JSON, so a typo like `age:gt:3O` is an
error rather than silently matching nothing.

---

## Limits & validation (DoS guard rails)

ce-db folds the whole op-set on every read, so unbounded inputs are a memory/latency hazard. Every
write is validated against a configurable `Limits` (Firestore-like defaults): max document bytes
(1 MiB), field count (1024), nesting depth (32), id/field-name lengths, op-history ceiling
(backpressure), peer-count, and batch size (500). Open with `Collection::open_with_limits(.., limits)`
to tighten or loosen them. Collection names and doc ids must be non-empty, slash-free, control-char
free, and not `.`/`..`.

A materialized snapshot **cache** keyed by the merged op-count means repeated reads at the same
version are O(1) (only re-folding when new ops arrive), instead of an O(history) fold per call.

---

## Snapshot / bootstrap (cheap cold reads)

`Collection::compact()` serializes this device's writer state to a **content-addressed blob** via
ce-coord `Snapshot`, records a checkpoint `(base, cid)`, and compacts the log — so a fresh reader
bootstraps from the snapshot and tails only newer ops instead of replaying all history. Large document
fields belong in CE blobs: `put_object` the bytes, store the returned CID as a string field.

---

## What ce-db is **not** (and what's deferred)

- **Not a SQL engine.** Queries are conjunctive field filters (`AND`) + order + cursors/limit — enough
  for `where`-style listing and pagination, not joins, aggregates, or `OR` across fields.
- **No secondary indexes (yet).** Every query is a full O(n) scan of the folded map. The read **cache**
  makes repeated reads cheap, but large collections still scan linearly per *distinct* query. Single-
  field / composite indexes are the next major perf item — **deferred**.
- **`run_transaction` is optimistic, single-device.** It re-checks the collection version before
  committing and retries on conflict, giving optimistic concurrency *over the issuing device's view*.
  It is **not** cross-device serializable isolation — that needs the chain (or an opt-in ce-coord Raft
  group). Likewise `WriteBatch` is atomic from the issuer's perspective (one contiguous Lamport range),
  not a distributed 2-phase commit. This boundary is intentional and documented.
- **Not a uniqueness oracle.** "Claim @username exactly once" and credit balances are *consensus*
  problems; route those to the chain. ce-db deliberately does not pretend a CRDT can do them.
- **Compaction is per-device.** `compact()` snapshots this device's own writer log; it does not yet GC
  tombstones across the *merged* union or compact peer logs — **deferred**.
- **Not its own crypto.** Transport is CE's Noise-authenticated mesh; payloads are JSON. For
  confidentiality, encrypt field values before `set`/`patch` (app concern).

---

## Build & test

```sh
cargo build
cargo test          # pure model + convergence + query + guard + limits tests; no node needed

# live multi-node realtime/guard/batch/atomic/subcollection tests against real ephemeral nodes:
#   built automatically when the release `ce` binary exists at the shared target dir; otherwise
#   every live test logs the reason and returns early (the suite stays green without infrastructure).
cargo test --test live -- --nocapture
CE_NO_LIVE=1 cargo test     # force-skip the live suite
```

The default suite is hermetic: the `DbMachine` fold (incl. atomic-op convergence, nested merge),
the query engine (filters/operators/cursors/precision), capability minting/attenuation/verification
and the **enforcement gate** (`tests/guard.rs`), input limits, CLI parsing, and a two-reader
convergence model that mirrors `Merged`. `tests/live.rs` exercises the real ce-coord `Merged` path.

See [`examples/`](examples/) for runnable end-to-end examples and [`CHANGELOG.md`](CHANGELOG.md).

---

## Layout

```
ce-db/
├── src/
│   ├── lib.rs          # crate root, DocPath, re-exports
│   ├── doc.rs          # Document model + DbMachine (the MergeMachine that converges) + FieldOp
│   ├── query.rs        # Filter / Query (operators, nested paths, multi-order, cursors, precision)
│   ├── access.rs       # CollectionGrant — per-collection ce-cap gating (verification toolkit)
│   ├── guard.rs        # GuardedCollection + AuthPolicy — capability ENFORCEMENT on the data path
│   ├── limits.rs       # Limits — input validation & DoS/OOM guard rails
│   ├── batch.rs        # WriteBatch + runTransaction
│   ├── collection.rs   # Collection — live realtime store over ce-coord Merged (+ cache, push poller)
│   └── bin/ce_db.rs    # the `ce-db` CLI
├── examples/
│   ├── basic.rs        # set/patch/query/realtime in one file
│   └── guarded.rs      # capability handoff + enforcement
└── tests/
    ├── edge_cases.rs   # query/model/access edge cases
    ├── prop_db.rs      # property tests: convergence, query, serde, attenuation
    ├── model_extra.rs  # atomic-op convergence, nested merge, integer precision (property tests)
    ├── guard.rs        # capability ENFORCEMENT failure-path tests (deny unauthorized/expired/forged)
    ├── realtime_sync.rs# two-reader convergence model
    └── live.rs         # real ephemeral 2-node mesh: realtime, guard, batch, atomic, subcollections
```

MSRV: Rust 1.85 (edition 2024). License: MIT.
