//! Per-collection capability gating via `ce-cap`.
//!
//! ce-db does not invent its own auth: it reuses CE's single trust primitive, the signed attenuating
//! capability chain ([`ce_cap`]). A collection owner mints a [`SignedCapability`] granting an
//! audience an opaque **db ability** (`db:read`, `db:write`, or `db:admin`) on a [`Resource`], and
//! the holder presents the chain when it wants to act. Verification is local and offline — exactly
//! the property that makes capabilities strictly better than an ACL table.
//!
//! ## Abilities (opaque strings, owned by ce-db)
//!
//! * [`ABILITY_READ`] — may read/query/watch the collection.
//! * [`ABILITY_WRITE`] — may set/patch/delete documents (implies read at the app layer).
//! * [`ABILITY_ADMIN`] — may delegate access to others and compact snapshots.
//!
//! These are just strings to `ce-cap`; the meaning lives here. Resource scoping reuses `ce-cap`'s
//! [`Resource`] (a specific node, a tag, etc.); ce-db treats the *collection name* as application
//! policy carried in the caveats' `path_prefix` (so a single chain can be scoped to one collection),
//! while node-level scoping rides the standard `Resource`.
//!
//! ## How a writer authorizes a peer
//!
//! 1. The collection owner (root of trust) `mint`s `db:write @ collection` to a peer's NodeId.
//! 2. The peer can `attenuate` it further (e.g. `db:read` only) before re-delegating.
//! 3. Any node holding the owner's key as an accepted root can `verify` the presented chain for a
//!    requested action **without contacting the owner** — offline, in microseconds.

use anyhow::{Result, anyhow};
use ce_cap::{Capability, Caveats, Resource, SignedCapability, authorize, decode_chain, encode_chain};
use ce_identity::{Identity, NodeId};

/// Read/query/watch a collection.
pub const ABILITY_READ: &str = "db:read";
/// Write (set/patch/delete) documents in a collection.
pub const ABILITY_WRITE: &str = "db:write";
/// Administer a collection: delegate access, compact snapshots.
pub const ABILITY_ADMIN: &str = "db:admin";

/// Decode a 64-hex NodeId string into the raw [`NodeId`].
pub fn node_id_from_hex(hex_str: &str) -> Result<NodeId> {
    let bytes = hex::decode(hex_str.trim()).map_err(|_| anyhow!("node id is not valid hex"))?;
    bytes.try_into().map_err(|_| anyhow!("node id must be 32 bytes (64 hex chars)"))
}

/// Hex-encode a [`NodeId`].
pub fn node_id_hex(id: &NodeId) -> String {
    hex::encode(id)
}

/// A capability scoped to a single ce-db collection, ready to present on the wire as a hex token.
///
/// Internally this is just a `ce-cap` chain (root-first). The collection name is carried in the
/// caveats' `path_prefix` so the same primitive that confines a sync to a directory confines a grant
/// to one collection — verified by [`CollectionGrant::verify`].
#[derive(Clone, Debug)]
pub struct CollectionGrant {
    chain: Vec<SignedCapability>,
    collection: String,
}

impl CollectionGrant {
    /// Mint a fresh root grant: `issuer` authorizes `audience` to use `abilities` on `collection`,
    /// for the node-set `resource`, until `not_after` (unix seconds; `0` = never). The collection is
    /// pinned via the caveat path-prefix so the grant cannot be replayed against another collection.
    pub fn mint(
        issuer: &Identity,
        audience: NodeId,
        collection: &str,
        abilities: &[&str],
        resource: Resource,
        not_after: u64,
        nonce: u64,
    ) -> CollectionGrant {
        let caveats = Caveats {
            not_after,
            path_prefix: Some(collection_prefix(collection)),
            ..Default::default()
        };
        let cap = SignedCapability::issue(
            issuer,
            audience,
            abilities.iter().map(|a| a.to_string()).collect(),
            resource,
            caveats,
            nonce,
            None,
        );
        CollectionGrant { chain: vec![cap], collection: collection.to_string() }
    }

    /// Attenuate the grant the holder owns, re-delegating a *subset* to `audience`. `narrower` must
    /// be a subset of the held abilities (enforced by `ce-cap` at verification time). The collection
    /// stays pinned. `holder` is the identity that currently holds the leaf (its audience).
    pub fn attenuate(
        &self,
        holder: &Identity,
        audience: NodeId,
        narrower: &[&str],
        resource: Resource,
        not_after: u64,
        nonce: u64,
    ) -> Result<CollectionGrant> {
        let parent = self
            .chain
            .last()
            .ok_or_else(|| anyhow!("cannot attenuate an empty grant chain"))?;
        if holder.node_id() != parent.cap.audience {
            return Err(anyhow!("only the current holder may attenuate this grant"));
        }
        let caveats = Caveats {
            not_after,
            path_prefix: Some(collection_prefix(&self.collection)),
            ..Default::default()
        };
        let child = SignedCapability::issue(
            holder,
            audience,
            narrower.iter().map(|a| a.to_string()).collect(),
            resource,
            caveats,
            nonce,
            Some(parent.id()),
        );
        let mut chain = self.chain.clone();
        chain.push(child);
        Ok(CollectionGrant { chain, collection: self.collection.clone() })
    }

    /// The collection this grant is pinned to.
    pub fn collection(&self) -> &str {
        &self.collection
    }

    /// The NodeId (raw) that holds the leaf of this chain — the principal who may act with it.
    pub fn holder(&self) -> Option<NodeId> {
        self.chain.last().map(|l| l.cap.audience)
    }

    /// The root issuer (the collection owner / trust anchor) of this chain.
    pub fn root_issuer(&self) -> Option<NodeId> {
        self.chain.first().map(|l| l.cap.issuer)
    }

    /// Encode to a portable hex token (root-first), the form carried on the wire and in a wallet.
    pub fn to_token(&self) -> String {
        encode_chain(&self.chain)
    }

    /// Decode a token produced by [`to_token`](Self::to_token), recovering the pinned collection
    /// from the leaf's caveat prefix.
    pub fn from_token(token: &str) -> Result<CollectionGrant> {
        let chain = decode_chain(token)?;
        let leaf = chain.last().ok_or_else(|| anyhow!("empty capability chain"))?;
        let collection = leaf
            .cap
            .caveats
            .path_prefix
            .as_deref()
            .and_then(parse_collection_prefix)
            .ok_or_else(|| anyhow!("grant is not pinned to a ce-db collection"))?;
        Ok(CollectionGrant { chain, collection })
    }

    /// Verify, **locally and offline**, that `requester` may perform `action` (`db:read`/`db:write`/
    /// `db:admin`) on `collection` at this enforcing node, given the accepted root keys and the
    /// current time. Delegates the heavy lifting to [`ce_cap::authorize`] and additionally checks the
    /// grant is pinned to the requested collection. `is_revoked` consults the on-chain revocation set
    /// (pass a closure returning `false` to skip).
    #[allow(clippy::too_many_arguments)]
    pub fn verify(
        &self,
        self_id: &NodeId,
        accepted_roots: &[NodeId],
        self_tags: &[String],
        now: u64,
        requester: &NodeId,
        action: &str,
        collection: &str,
        is_revoked: &dyn Fn(&NodeId, u64) -> bool,
    ) -> Result<()> {
        if collection != self.collection {
            return Err(anyhow!(
                "grant is pinned to collection '{}', not '{}'",
                self.collection,
                collection
            ));
        }
        authorize(self_id, accepted_roots, self_tags, now, requester, action, &self.chain, is_revoked)
            .map_err(|e| anyhow!(e))
    }

    /// The raw underlying `ce-cap` chain (escape hatch for callers that need `ce-cap` directly).
    pub fn chain(&self) -> &[SignedCapability] {
        &self.chain
    }

    /// The leaf capability (the one held by [`holder`](Self::holder)).
    pub fn leaf(&self) -> Option<&Capability> {
        self.chain.last().map(|l| &l.cap)
    }
}

/// The caveat path-prefix used to pin a grant to a collection. Namespaced so it never collides with
/// a real filesystem path prefix used by other apps' caps.
fn collection_prefix(collection: &str) -> String {
    format!("ce-db/{collection}")
}

/// Recover the collection name from a `ce-db/<name>` caveat prefix.
fn parse_collection_prefix(prefix: &str) -> Option<String> {
    prefix.strip_prefix("ce-db/").map(|s| s.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    fn ident(tag: &str) -> Identity {
        static SEQ: AtomicU64 = AtomicU64::new(0);
        let n = SEQ.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("ce-db-acc-{}-{n}-{tag}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        Identity::load_or_generate(&dir).unwrap()
    }

    fn never(_: &NodeId, _: u64) -> bool {
        false
    }

    #[test]
    fn minted_grant_authorizes_write() {
        let owner = ident("owner");
        let peer = ident("peer");
        let g = CollectionGrant::mint(
            &owner,
            peer.node_id(),
            "users",
            &[ABILITY_READ, ABILITY_WRITE],
            Resource::Any,
            0,
            1,
        );
        // The owner is the enforcing node (self == root issuer) — verifies offline.
        assert!(
            g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_WRITE, "users", &never)
                .is_ok()
        );
        // Wrong collection is rejected.
        assert!(
            g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_WRITE, "orders", &never)
                .is_err()
        );
    }

    #[test]
    fn read_only_grant_denies_write() {
        let owner = ident("owner");
        let peer = ident("peer");
        let g = CollectionGrant::mint(&owner, peer.node_id(), "users", &[ABILITY_READ], Resource::Any, 0, 1);
        assert!(g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_READ, "users", &never).is_ok());
        assert!(g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_WRITE, "users", &never).is_err());
    }

    #[test]
    fn token_roundtrips_and_pins_collection() {
        let owner = ident("owner");
        let peer = ident("peer");
        let g = CollectionGrant::mint(&owner, peer.node_id(), "inbox", &[ABILITY_READ], Resource::Any, 0, 7);
        let token = g.to_token();
        let back = CollectionGrant::from_token(&token).unwrap();
        assert_eq!(back.collection(), "inbox");
        assert_eq!(back.holder(), Some(peer.node_id()));
        assert!(back.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_READ, "inbox", &never).is_ok());
    }

    #[test]
    fn attenuation_narrows_and_chains() {
        let owner = ident("owner");
        let mid = ident("mid");
        let leaf = ident("leaf");
        let g = CollectionGrant::mint(
            &owner,
            mid.node_id(),
            "users",
            &[ABILITY_READ, ABILITY_WRITE],
            Resource::Any,
            0,
            1,
        );
        // mid re-delegates read-only to leaf.
        let narrowed =
            g.attenuate(&mid, leaf.node_id(), &[ABILITY_READ], Resource::Any, 0, 2).unwrap();
        assert_eq!(narrowed.chain().len(), 2);
        // leaf may read.
        assert!(narrowed.verify(&owner.node_id(), &[], &[], 1000, &leaf.node_id(), ABILITY_READ, "users", &never).is_ok());
        // leaf may NOT write — attenuation dropped that ability.
        assert!(narrowed.verify(&owner.node_id(), &[], &[], 1000, &leaf.node_id(), ABILITY_WRITE, "users", &never).is_err());
    }

    #[test]
    fn attenuation_cannot_amplify() {
        let owner = ident("owner");
        let mid = ident("mid");
        let leaf = ident("leaf");
        let g = CollectionGrant::mint(&owner, mid.node_id(), "users", &[ABILITY_READ], Resource::Any, 0, 1);
        // mid tries to grant write it never held.
        let bad = g.attenuate(&mid, leaf.node_id(), &[ABILITY_WRITE], Resource::Any, 0, 2).unwrap();
        let r = bad.verify(&owner.node_id(), &[], &[], 1000, &leaf.node_id(), ABILITY_WRITE, "users", &never);
        assert!(r.is_err());
    }

    #[test]
    fn non_holder_cannot_attenuate() {
        let owner = ident("owner");
        let mid = ident("mid");
        let stranger = ident("stranger");
        let leaf = ident("leaf");
        let g = CollectionGrant::mint(&owner, mid.node_id(), "users", &[ABILITY_READ], Resource::Any, 0, 1);
        // stranger is not the leaf's audience.
        assert!(g.attenuate(&stranger, leaf.node_id(), &[ABILITY_READ], Resource::Any, 0, 2).is_err());
    }

    #[test]
    fn expired_grant_is_denied() {
        let owner = ident("owner");
        let peer = ident("peer");
        let g = CollectionGrant::mint(&owner, peer.node_id(), "users", &[ABILITY_READ], Resource::Any, 500, 1);
        assert!(g.verify(&owner.node_id(), &[], &[], 1000, &peer.node_id(), ABILITY_READ, "users", &never).is_err());
        assert!(g.verify(&owner.node_id(), &[], &[], 400, &peer.node_id(), ABILITY_READ, "users", &never).is_ok());
    }

    #[test]
    fn node_id_hex_roundtrips() {
        let owner = ident("owner");
        let h = node_id_hex(&owner.node_id());
        assert_eq!(node_id_from_hex(&h).unwrap(), owner.node_id());
    }
}
