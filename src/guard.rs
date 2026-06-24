//! Capability **enforcement** at the data path — the missing piece that makes ce-db's "security
//! rules / IAM" real rather than aspirational.
//!
//! [`crate::CollectionGrant`] is a verification *toolkit*; on its own nothing calls it. A
//! [`GuardedCollection`] wires it into every operation: it verifies a presented capability chain for
//! `db:read` before [`get`](GuardedCollection::get)/[`query`](GuardedCollection::query)/
//! [`watch`](GuardedCollection::watch), for `db:write` before
//! [`set`](GuardedCollection::set)/[`patch`](GuardedCollection::patch)/
//! [`delete`](GuardedCollection::delete), and for `db:admin` before
//! [`compact`](GuardedCollection::compact). An unauthorized or expired grant is **denied** before any
//! state is read or written.
//!
//! ## Model
//!
//! Authorization in CE is always a signed attenuating capability chain (no allowlists, no stored
//! ip:port). A `GuardedCollection` is the *enforcing* side: it is configured with
//!
//! * `self_id` — the enforcing node's id (the resource owner / where the data lives),
//! * `accepted_roots` — the root keys whose chains it honors (typically just `self_id`),
//! * `requester` — the principal asking to act (the holder of the presented grant),
//! * `grant` — the [`CollectionGrant`] the requester presents,
//! * `is_revoked` — a closure consulting the on-chain revocation set (pass [`never_revoked`] to skip).
//!
//! Each op runs [`CollectionGrant::verify`] for the required ability and only then touches the inner
//! [`Collection`]. This is exactly the check a remote enforcer would run when honoring a mesh request,
//! made the single gate for the data API.

use std::sync::Arc;

use anyhow::Result;
use ce_identity::NodeId;
use tokio::sync::watch;

use crate::access::{ABILITY_ADMIN, ABILITY_READ, ABILITY_WRITE, CollectionGrant};
use crate::collection::{Collection, Snapshot};
use crate::doc::{DocMeta, Document};
use crate::query::Query;

/// A closure type for consulting the on-chain capability-revocation set: `(issuer, nonce) -> revoked`.
pub type RevokedFn = Arc<dyn Fn(&NodeId, u64) -> bool + Send + Sync>;

/// A revocation closure that always returns `false` (nothing revoked) — convenient for tests and
/// deployments that gate purely on expiry.
pub fn never_revoked() -> RevokedFn {
    Arc::new(|_: &NodeId, _: u64| false)
}

/// The enforcement configuration: who is enforcing, which roots are trusted, who is asking, and the
/// grant they present.
#[derive(Clone)]
pub struct AuthPolicy {
    /// The enforcing node's id (the resource owner / data location).
    pub self_id: NodeId,
    /// Root keys whose chains are honored (commonly just `[self_id]`).
    pub accepted_roots: Vec<NodeId>,
    /// Tags this enforcing node claims (for `Resource::Tag` scoping); usually empty.
    pub self_tags: Vec<String>,
    /// The principal requesting access (the holder presenting `grant`).
    pub requester: NodeId,
    /// The capability chain the requester presents for this collection.
    pub grant: CollectionGrant,
    /// On-chain revocation oracle.
    pub is_revoked: RevokedFn,
}

impl AuthPolicy {
    /// Build a policy where the enforcing node is also the grant's root issuer (the common
    /// owner-self-issues-to-a-peer case): accepted roots default to `[self_id]`.
    pub fn owner(self_id: NodeId, requester: NodeId, grant: CollectionGrant) -> AuthPolicy {
        AuthPolicy {
            self_id,
            accepted_roots: vec![self_id],
            self_tags: Vec::new(),
            requester,
            grant,
            is_revoked: never_revoked(),
        }
    }

    /// Set an explicit set of accepted root keys (builder style).
    pub fn with_roots(mut self, roots: Vec<NodeId>) -> AuthPolicy {
        self.accepted_roots = roots;
        self
    }

    /// Set the revocation oracle (builder style).
    pub fn with_revocation(mut self, f: RevokedFn) -> AuthPolicy {
        self.is_revoked = f;
        self
    }

    /// Verify the presented grant authorizes `action` on `collection` at time `now` (unix seconds).
    /// This is the exact gate every [`GuardedCollection`] operation runs; exposed so the decision can
    /// be tested and reused independently of a live [`Collection`].
    pub fn authorize(&self, action: &str, collection: &str, now: u64) -> Result<()> {
        self.grant.verify(
            &self.self_id,
            &self.accepted_roots,
            &self.self_tags,
            now,
            &self.requester,
            action,
            collection,
            self.is_revoked.as_ref(),
        )
    }
}

/// A [`Collection`] whose every data operation is gated by an [`AuthPolicy`]. Reads require
/// `db:read`, writes require `db:write`, admin ops require `db:admin`. Unauthorized ops return an
/// error and never touch state.
pub struct GuardedCollection {
    inner: Collection,
    policy: AuthPolicy,
}

impl GuardedCollection {
    /// Wrap a [`Collection`] with a capability-enforcing policy.
    pub fn new(inner: Collection, policy: AuthPolicy) -> GuardedCollection {
        GuardedCollection { inner, policy }
    }

    /// The underlying collection's hierarchical path.
    pub fn path(&self) -> &str {
        self.inner.path()
    }

    /// Current unix time (seconds) for capability expiry checks.
    fn now() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    fn check(&self, action: &str) -> Result<()> {
        self.policy
            .authorize(action, self.inner.name(), Self::now())
    }

    // --- writes (require db:write) ---

    /// Set a document (requires `db:write`).
    pub async fn set(&self, doc_id: &str, doc: Document) -> Result<()> {
        self.check(ABILITY_WRITE)?;
        self.inner.set(doc_id, doc).await
    }

    /// Patch a document (requires `db:write`).
    pub async fn patch(&self, doc_id: &str, fields: Document) -> Result<()> {
        self.check(ABILITY_WRITE)?;
        self.inner.patch(doc_id, fields).await
    }

    /// Set a single field (requires `db:write`).
    pub async fn set_field(
        &self,
        doc_id: &str,
        field: &str,
        value: serde_json::Value,
    ) -> Result<()> {
        self.check(ABILITY_WRITE)?;
        self.inner.set_field(doc_id, field, value).await
    }

    /// Atomic increment (requires `db:write`).
    pub async fn increment(&self, doc_id: &str, field: &str, delta: i64) -> Result<()> {
        self.check(ABILITY_WRITE)?;
        self.inner.increment(doc_id, field, delta).await
    }

    /// Delete a document (requires `db:write`).
    pub async fn delete(&self, doc_id: &str) -> Result<()> {
        self.check(ABILITY_WRITE)?;
        self.inner.delete(doc_id).await
    }

    // --- reads (require db:read) ---

    /// Read a document (requires `db:read`).
    pub fn get(&self, doc_id: &str) -> Result<Option<Document>> {
        self.check(ABILITY_READ)?;
        Ok(self.inner.get(doc_id))
    }

    /// Read document metadata (requires `db:read`).
    pub fn meta(&self, doc_id: &str) -> Result<Option<DocMeta>> {
        self.check(ABILITY_READ)?;
        Ok(self.inner.meta(doc_id))
    }

    /// Snapshot the collection (requires `db:read`).
    pub fn snapshot(&self) -> Result<Snapshot> {
        self.check(ABILITY_READ)?;
        Ok(self.inner.snapshot())
    }

    /// Run a query (requires `db:read`).
    pub fn query(&self, q: &Query) -> Result<Vec<(String, Document)>> {
        self.check(ABILITY_READ)?;
        Ok(self.inner.query(q))
    }

    /// Watch for changes (requires `db:read`).
    pub fn watch(&self) -> Result<watch::Receiver<u64>> {
        self.check(ABILITY_READ)?;
        Ok(self.inner.watch())
    }

    // --- admin (require db:admin) ---

    /// Compact this device's writer log (requires `db:admin`).
    pub async fn compact(&self) -> Result<ce_coord::Checkpoint> {
        self.check(ABILITY_ADMIN)?;
        self.inner.compact().await
    }

    /// Escape hatch: the inner collection (unguarded). Use sparingly — it bypasses enforcement.
    pub fn inner(&self) -> &Collection {
        &self.inner
    }
}

#[cfg(test)]
mod tests {
    // GuardedCollection enforcement is proven in the failure-path integration test
    // (tests/guard.rs), which needs a live Merged log to build a Collection. The verify() logic it
    // delegates to is unit-tested in access.rs.
}
