//! # ce-db — a Firestore-class realtime document database over CE
//!
//! `ce-db` is the **Firestore of the CE suite**: collections of JSON documents that sync in realtime
//! across devices, work offline, and converge without a server — built entirely by *composing*
//! existing CE primitives, never by changing the node.
//!
//! | Firestore concept | ce-db | CE primitive underneath |
//! |---|---|---|
//! | Document store | [`Collection`] of JSON [`Document`]s | ce-coord [`Merged`] (multi-writer CRDT) |
//! | Offline + multi-device merge | field-level LWW fold | ce-coord [`Merged`] union-of-logs |
//! | `onSnapshot` realtime listeners | [`Collection::watch`] + [`Collection::next_change`] | mesh pubsub (via ce-coord) |
//! | Cheap cold reads / compaction | [`Collection::compact`] | ce-coord [`Snapshot`] + CE blobs |
//! | Security rules / IAM | [`CollectionGrant`] (`db:read`/`db:write`/`db:admin`) | ce-cap signed attenuating chains |
//! | Large fields | store a blob CID as a string field | CE blobs / ce-pin |
//!
//! [`Merged`]: ce_coord::Merged
//! [`Snapshot`]: ce_coord::Snapshot
//!
//! Every device is a writer **and** a reader. Writes are stamped with a Lamport clock + writer id and
//! merged by ce-coord; reads fold the merged op-set into the live document map. Convergence is a pure
//! function of the op set — leaderless, offline-tolerant, identical on every replica. Capability
//! gating reuses `ce-cap`: a collection owner mints a [`CollectionGrant`] and any node verifies it
//! offline before honoring a read/write.
//!
//! ## Quick start (library)
//!
//! ```no_run
//! use ce_coord::Coord;
//! use ce_db::{Collection, Query, Filter};
//! use serde_json::json;
//!
//! # async fn demo() -> anyhow::Result<()> {
//! let coord = Coord::connect().await?;                  // wraps the local CE node
//! let users = Collection::open(&coord, "users", &[]).await?;
//!
//! users.set("ada", json!({"name": "Ada", "age": 36}).as_object().unwrap().clone()).await?;
//! users.patch("ada", json!({"email": "ada@x"}).as_object().unwrap().clone()).await?;
//!
//! let ada = users.get("ada").unwrap();
//! assert_eq!(ada["email"], json!("ada@x"));
//!
//! // Query: everyone older than 30.
//! let q = Query::new().with(Filter::new("age", ce_db::Op::Gt, json!(30)));
//! for (id, doc) in users.query(&q) { println!("{id}: {doc:?}"); }
//!
//! // Realtime: react to every change (local or from a peer).
//! let mut rx = users.watch();
//! while let Some(snap) = users.next_change(&mut rx).await {
//!     println!("collection changed: {} docs", snap.len());
//! }
//! # Ok(()) }
//! ```
//!
//! See the `ce-db` binary for the CLI (`set` / `get` / `query` / `delete` / `watch` / `grant`) and
//! `tests/realtime_sync.rs` for the two-reader realtime-convergence proof.

pub mod access;
pub mod batch;
pub mod collection;
pub mod doc;
pub mod guard;
pub mod limits;
pub mod query;

pub use access::{
    ABILITY_ADMIN, ABILITY_READ, ABILITY_WRITE, CollectionGrant, node_id_from_hex, node_id_hex,
};
pub use batch::WriteBatch;
pub use collection::{ChangeKind, ChangeStream, Collection, DocChange, Snapshot};
pub use doc::{DbMachine, DocMeta, DocOp, Document, FieldOp, OpKey, OpKind};
pub use guard::{AuthPolicy, GuardedCollection};
pub use limits::Limits;
pub use query::{Cursor, Dir, Filter, Op, OrderBy, Query};

// Re-export the substrate types callers most often touch, so an app needs only `ce_db` on its deps.
pub use ce_iam_core::Resource;
pub use ce_coord::{Checkpoint, Coord};

use anyhow::{Result, anyhow};

/// A parsed `<collection>/<doc_id>` path, the addressing form the CLI and many apps use.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DocPath {
    /// The collection name (everything before the first `/`).
    pub collection: String,
    /// The document id (everything after the first `/`; may itself contain `/`).
    pub doc_id: String,
}

impl DocPath {
    /// Parse `collection/doc_id`. The collection is the first path segment; the doc id is the rest
    /// (so `users/2026/ada` → collection `users`, doc `2026/ada`). Both parts must be non-empty.
    pub fn parse(path: &str) -> Result<DocPath> {
        let (collection, doc_id) = path
            .split_once('/')
            .ok_or_else(|| anyhow!("path must be '<collection>/<doc_id>', got '{path}'"))?;
        if collection.is_empty() || doc_id.is_empty() {
            return Err(anyhow!(
                "collection and doc id must both be non-empty in '{path}'"
            ));
        }
        Ok(DocPath {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docpath_parses() {
        let p = DocPath::parse("users/ada").unwrap();
        assert_eq!(p.collection, "users");
        assert_eq!(p.doc_id, "ada");
    }

    #[test]
    fn docpath_keeps_nested_doc_id() {
        let p = DocPath::parse("users/2026/ada").unwrap();
        assert_eq!(p.collection, "users");
        assert_eq!(p.doc_id, "2026/ada");
    }

    #[test]
    fn docpath_rejects_malformed() {
        assert!(DocPath::parse("nocollection").is_err());
        assert!(DocPath::parse("/ada").is_err());
        assert!(DocPath::parse("users/").is_err());
    }
}
