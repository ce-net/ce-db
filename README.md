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
| `onSnapshot` realtime listeners | `Collection::watch` / `next_change` | mesh pubsub (via ce-coord) |
| Cheap cold reads / compaction | `Collection::compact` | ce-coord `Snapshot` + CE blobs |
| Security rules / IAM | `CollectionGrant` (`db:read`/`db:write`/`db:admin`) | **ce-cap** signed attenuating chains |
| Large fields | store a blob CID as a string field | CE blobs / ce-pin |

No new node endpoints, no allowlists, no stored `ip:port`. ce-db is pure SDK/app tier: it depends on
`ce-rs` (HTTP SDK), `ce-coord` (`Merged`/`Snapshot`), and `ce-cap` (authorization), all by path.

---

## How it converges (the important part)

Every device is **both a writer and a reader**. A write is one `DocOp` (`Set`, `Patch`, or `Delete`)
stamped with a strict total-order key `(lamport, writer_id)`, appended to *that device's own* writer
log. ce-coord's `Merged` layer replicates every device's log over the mesh and takes the **key-ordered
union** of all of them; ce-db folds that union with a deterministic state machine (`DbMachine`):

- **`Set`** replaces a whole document — whole-document last-writer-wins.
- **`Patch`** merges fields — *each field independently* last-writer-wins, so two devices editing
  different fields of the same doc **both survive** (the field-level CRDT merge Firestore offers).
  A `null` field value deletes that field.
- **`Delete`** tombstones a document; a later `Set`/`Patch` resurrects it.

Because the fold is a pure function of the op *set*, **every replica converges to the same documents
regardless of delivery order** — leaderless, offline-tolerant, no Raft, no quorum. The chain (not
ce-db) is where you reach for global uniqueness/money invariants a CRDT provably can't do.

---

## Library

```rust
use ce_coord::Coord;
use ce_db::{Collection, Query, Filter, Op};
use serde_json::json;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    let coord = Coord::connect().await?;                  // wraps the local CE node
    let users = Collection::open(&coord, "users", &[]).await?;

    // set / patch / delete
    users.set("ada", json!({"name": "Ada", "age": 36}).as_object().unwrap().clone()).await?;
    users.patch("ada", json!({"email": "ada@x"}).as_object().unwrap().clone()).await?;
    users.set_field("ada", "verified", json!(true)).await?;

    // read
    let ada = users.get("ada").unwrap();
    assert_eq!(ada["email"], json!("ada@x"));

    // query: everyone older than 30, newest first, top 5
    let q = Query::new()
        .with(Filter { field: "age".into(), op: Op::Gt, value: json!(30) })
        .order("age", ce_db::Dir::Desc)
        .take(5);
    for (id, doc) in users.query(&q) {
        println!("{id}: {doc:?}");
    }

    // realtime: react to every change (local write OR a peer's op arriving over the mesh)
    let mut rx = users.watch();
    while let Some(snap) = users.next_change(&mut rx).await {
        println!("collection changed: {} docs (op_count {})", snap.len(), snap.op_count);
    }
    Ok(())
}
```

To follow other devices, pass their NodeIds when opening:
`Collection::open(&coord, "users", &[peer_a_hex, peer_b_hex]).await?` (or `add_writer` later).

---

## CLI

The `ce-db` binary talks to the local node (`ce start` must be running). Documents are addressed
`<collection>/<doc_id>`.

```sh
# write
ce-db set   users/ada '{"name":"Ada","age":36}'
ce-db patch users/ada '{"email":"ada@x"}'      # field-level merge
ce-db delete users/ada

# read
ce-db get users/ada

# query — repeatable --where field:op:value (op = eq|ne|gt|ge|lt|le|contains)
ce-db query users --where age:gt:30 --where 'tags:contains:cs' --order age:desc --limit 5

# realtime watch — prints a snapshot on every change
ce-db --peers <PEER_NODE_ID> watch users

# status & compaction
ce-db status users
ce-db compact users        # snapshot this device's log to a content-addressed blob
```

### Realtime sync between two readers (the demo)

On **device A** (follows B), open a live watch, then write:

```sh
ce-db --peers <B_NODE_ID> watch users     # leave running
# in another shell on A:
ce-db --peers <B_NODE_ID> set users/ada '{"name":"Ada","age":36}'
```

On **device B** (follows A), open a live watch — `users/ada` appears within a poll interval:

```sh
ce-db --peers <A_NODE_ID> watch users
# -- change (op_count=1) --
# ada    {"age":36,"name":"Ada"}
```

Both devices converge to identical documents no matter who wrote what or in what order it arrived.
(Single-machine? Run two nodes with different `CE_DATA_DIR`/ports and point each `--peers` at the
other's `ce id`.)

---

## Capability gating (per collection)

Access is a signed `ce-cap` chain, verified **offline** — strictly better than an ACL table. The
collection owner mints a grant scoped to one collection and a set of abilities:

```sh
# owner mints a read+write grant for a peer, scoped to the `users` collection, 24h expiry:
ce-db grant <PEER_NODE_ID> users --abilities db:read,db:write --expires 86400
# -> prints a hex token the peer presents

# inspect any token:
ce-db inspect <TOKEN>
# collection: users
# holder:     <peer hex>
# abilities:  ["db:read", "db:write"]
```

In code, `CollectionGrant::mint` / `attenuate` / `verify` wrap `ce-cap`:

```rust
use ce_db::{CollectionGrant, Resource, ABILITY_READ, ABILITY_WRITE};

// owner mints
let grant = CollectionGrant::mint(&owner, peer_id, "users",
    &[ABILITY_READ, ABILITY_WRITE], Resource::Any, /*not_after*/ 0, /*nonce*/ 1);

// peer re-delegates read-only to a third party (cannot amplify)
let narrowed = grant.attenuate(&peer, third_party, &[ABILITY_READ], Resource::Any, 0, 2)?;

// any node verifies offline before honoring a write
grant.verify(&owner_id, &accepted_roots, &self_tags, now, &peer_id,
    ABILITY_WRITE, "users", &|_issuer, _nonce| false /* is_revoked */)?;
```

Abilities are opaque strings owned by ce-db (`db:read`, `db:write`, `db:admin`); resource scoping and
attenuation/revocation are exactly `ce-cap`'s. Revocation = on-chain `RevokeCapability` + expiry.

---

## Snapshot / bootstrap (cheap cold reads)

A long-lived collection's op log is unbounded. `Collection::compact()` serializes this device's
writer state to a **content-addressed blob** via ce-coord `Snapshot`, records a checkpoint
`(base, cid)`, and compacts the log — so a fresh reader bootstraps from the snapshot and tails only
newer ops instead of replaying all history. `snapshot + tail == full replay` is proven in ce-coord.

Large document fields belong in CE blobs: `put_object` the bytes, store the returned CID as a string
field, and `get_object` it on demand. ce-db keeps the lightweight metadata and the CID.

---

## What ce-db is **not**

- **Not a SQL engine.** Queries are conjunctive top-level field filters (`AND`), plus order/limit —
  enough for `where`-style listing, not joins or aggregates.
- **Not a uniqueness oracle.** "Claim @username exactly once" and credit balances are *consensus*
  problems; route those to the chain (or an opt-in ce-coord Raft group), per the honest CRDT-vs-
  consensus boundary. ce-db deliberately does not pretend a CRDT can do them.
- **Not its own crypto.** Transport is CE's Noise-authenticated mesh; payloads are JSON. For
  confidentiality, encrypt field values before `set`/`patch` (app concern), like CE Notes does.

---

## Build & test

```sh
cargo build
cargo test          # pure model + convergence tests; no node needed

# live two-reader realtime sync against a running node (ce start):
cargo test --test realtime_sync -- --ignored --nocapture
```

The default test suite is hermetic (the `DbMachine` fold, the query engine, capability minting/
attenuation/verification, and a two-reader convergence model that mirrors `Merged`). The node-backed
realtime test is `#[ignore]`d so CI stays green without infrastructure.

---

## Layout

```
ce-db/
├── src/
│   ├── lib.rs          # crate root, DocPath, re-exports
│   ├── doc.rs          # Document model + DbMachine (the MergeMachine that converges)
│   ├── query.rs        # Filter / Query (field filters, order, limit)
│   ├── access.rs       # CollectionGrant — per-collection ce-cap gating
│   ├── collection.rs   # Collection — the live realtime store over ce-coord Merged
│   └── bin/ce_db.rs    # the `ce-db` CLI
└── tests/
    └── realtime_sync.rs # two-reader convergence (+ #[ignore] live node test)
```

License: MIT.
