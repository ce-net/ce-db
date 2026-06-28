//! Capability-enforcement tests for the data path.
//!
//! These exercise [`AuthPolicy::authorize`] — the exact gate every [`GuardedCollection`] operation
//! runs before touching state — so unauthorized / expired / wrong-collection / forged grants are
//! proven to be **denied** without needing a live node. The live suite (`tests/live.rs`) additionally
//! drives a real [`GuardedCollection`] over a `Merged` log.

use std::sync::atomic::{AtomicU64, Ordering};

use ce_iam_core::Resource;
use ce_db::guard::never_revoked;
use ce_db::{ABILITY_ADMIN, ABILITY_READ, ABILITY_WRITE, AuthPolicy, CollectionGrant};
use ce_identity::{Identity, NodeId};

fn ident(tag: &str) -> Identity {
    static SEQ: AtomicU64 = AtomicU64::new(0);
    let n = SEQ.fetch_add(1, Ordering::Relaxed);
    let dir = std::env::temp_dir().join(format!("ce-db-guard-{}-{n}-{tag}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let id = Identity::load_or_generate(&dir).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
    id
}

/// Build an owner→peer grant policy for `collection` with the given abilities.
fn policy(
    owner: &Identity,
    peer: &Identity,
    collection: &str,
    abilities: &[&str],
    expires: u64,
) -> AuthPolicy {
    let grant = CollectionGrant::mint(
        owner,
        peer.node_id(),
        collection,
        abilities,
        Resource::Any,
        expires,
        1,
    );
    AuthPolicy::owner(owner.node_id(), peer.node_id(), grant)
}

#[test]
fn write_grant_authorizes_read_and_write() {
    let owner = ident("owner");
    let peer = ident("peer");
    let p = policy(&owner, &peer, "users", &[ABILITY_READ, ABILITY_WRITE], 0);
    assert!(p.authorize(ABILITY_READ, "users", 1000).is_ok());
    assert!(p.authorize(ABILITY_WRITE, "users", 1000).is_ok());
    // No admin ability granted.
    assert!(p.authorize(ABILITY_ADMIN, "users", 1000).is_err());
}

#[test]
fn read_only_grant_denies_write_at_the_gate() {
    let owner = ident("owner");
    let peer = ident("peer");
    let p = policy(&owner, &peer, "users", &[ABILITY_READ], 0);
    assert!(p.authorize(ABILITY_READ, "users", 1000).is_ok());
    // This is the enforcement point: a read-only holder is denied a write.
    assert!(p.authorize(ABILITY_WRITE, "users", 1000).is_err());
}

#[test]
fn grant_for_other_collection_is_denied() {
    let owner = ident("owner");
    let peer = ident("peer");
    let p = policy(&owner, &peer, "users", &[ABILITY_WRITE], 0);
    // Same grant cannot act on a different collection.
    assert!(p.authorize(ABILITY_WRITE, "orders", 1000).is_err());
}

#[test]
fn expired_grant_is_denied() {
    let owner = ident("owner");
    let peer = ident("peer");
    let p = policy(&owner, &peer, "users", &[ABILITY_WRITE], 500);
    assert!(p.authorize(ABILITY_WRITE, "users", 400).is_ok()); // before expiry
    assert!(p.authorize(ABILITY_WRITE, "users", 600).is_err()); // after expiry
}

#[test]
fn wrong_requester_is_denied() {
    let owner = ident("owner");
    let peer = ident("peer");
    let stranger = ident("stranger");
    let grant = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "users",
        &[ABILITY_WRITE],
        Resource::Any,
        0,
        1,
    );
    // Stranger presents a grant minted for `peer`; the gate must reject (audience mismatch).
    let p = AuthPolicy::owner(owner.node_id(), stranger.node_id(), grant);
    assert!(p.authorize(ABILITY_WRITE, "users", 1000).is_err());
}

#[test]
fn untrusted_root_is_denied() {
    let owner = ident("owner");
    let other_root = ident("other");
    let peer = ident("peer");
    let grant = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "users",
        &[ABILITY_WRITE],
        Resource::Any,
        0,
        1,
    );
    // The enforcing node accepts only `other_root` — owner's chain is not anchored to a trusted root.
    let p = AuthPolicy {
        self_id: other_root.node_id(),
        accepted_roots: vec![other_root.node_id()],
        self_tags: Vec::new(),
        requester: peer.node_id(),
        grant,
        is_revoked: never_revoked(),
    };
    assert!(p.authorize(ABILITY_WRITE, "users", 1000).is_err());
}

#[test]
fn revoked_grant_is_denied() {
    let owner = ident("owner");
    let peer = ident("peer");
    let grant = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "users",
        &[ABILITY_WRITE],
        Resource::Any,
        0,
        42,
    );
    let owner_id = owner.node_id();
    let mut p = AuthPolicy::owner(owner_id, peer.node_id(), grant);
    // Without revocation it authorizes.
    assert!(p.authorize(ABILITY_WRITE, "users", 1000).is_ok());
    // Revoke the owner's nonce-42 cap: now denied.
    p.is_revoked =
        std::sync::Arc::new(move |issuer: &NodeId, nonce: u64| *issuer == owner_id && nonce == 42);
    assert!(p.authorize(ABILITY_WRITE, "users", 1000).is_err());
}

#[test]
fn forged_token_does_not_parse_as_grant() {
    // A garbage token cannot be decoded into a grant at all (so it never reaches the gate).
    assert!(CollectionGrant::from_token("not-a-real-token").is_err());
    assert!(CollectionGrant::from_token("deadbeef").is_err());
}
