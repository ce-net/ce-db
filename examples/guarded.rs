//! Capability handoff + enforcement example.
//!
//! Demonstrates the full ce-db authorization story without needing a live node: an owner mints a
//! `CollectionGrant` for a peer, the peer attenuates it, and an `AuthPolicy` gate (the exact check a
//! `GuardedCollection` runs before every op) accepts/denies based on the presented chain.
//!
//! ```sh
//! cargo run --example guarded
//! ```

use ce_cap::Resource;
use ce_db::{ABILITY_READ, ABILITY_WRITE, AuthPolicy, CollectionGrant};
use ce_identity::Identity;

fn temp_identity(tag: &str) -> anyhow::Result<Identity> {
    let dir = std::env::temp_dir().join(format!("ce-db-example-{tag}-{}", std::process::id()));
    std::fs::create_dir_all(&dir)?;
    let id = Identity::load_or_generate(&dir)?;
    let _ = std::fs::remove_dir_all(&dir);
    Ok(id)
}

fn main() -> anyhow::Result<()> {
    let owner = temp_identity("owner")?;
    let peer = temp_identity("peer")?;
    let third = temp_identity("third")?;

    // 1. Owner mints a read+write grant for `peer`, scoped to the `orders` collection.
    let grant = CollectionGrant::mint(
        &owner,
        peer.node_id(),
        "orders",
        &[ABILITY_READ, ABILITY_WRITE],
        Resource::Any,
        0, // never expires
        1, // nonce
    );
    println!("owner minted a db:read+db:write grant for peer, scoped to 'orders'");
    println!("token = {}", grant.to_token());

    // 2. The owner's node enforces it. AuthPolicy::owner = enforcing node is the grant's root issuer.
    let policy = AuthPolicy::owner(owner.node_id(), peer.node_id(), grant.clone());
    let now = 1_000;
    println!("\npeer authorization at the gate:");
    println!(
        "  db:read  on orders -> {:?}",
        policy.authorize(ABILITY_READ, "orders", now).is_ok()
    );
    println!(
        "  db:write on orders -> {:?}",
        policy.authorize(ABILITY_WRITE, "orders", now).is_ok()
    );
    println!(
        "  db:write on users  -> {:?} (wrong collection, denied)",
        policy.authorize(ABILITY_WRITE, "users", now).is_ok()
    );

    // 3. Peer attenuates to READ-ONLY and re-delegates to a third party (cannot amplify).
    let narrowed = grant.attenuate(&peer, third.node_id(), &[ABILITY_READ], Resource::Any, 0, 2)?;
    let third_policy = AuthPolicy::owner(owner.node_id(), third.node_id(), narrowed);
    println!("\nthird-party (read-only, attenuated) authorization:");
    println!(
        "  db:read  on orders -> {:?}",
        third_policy.authorize(ABILITY_READ, "orders", now).is_ok()
    );
    println!(
        "  db:write on orders -> {:?} (attenuation dropped write, denied)",
        third_policy.authorize(ABILITY_WRITE, "orders", now).is_ok()
    );

    Ok(())
}
