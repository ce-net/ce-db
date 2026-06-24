//! `ce-db` — CLI for the Firestore-class realtime document database over CE.
//!
//! Talks to the local CE node (via ce-coord / ce-rs) and drives a [`Collection`]. Documents are
//! addressed `<collection>/<doc_id>`. To see realtime convergence between two readers, run a `watch`
//! on two machines (or two data-dirs) against the same collection, each following the other's NodeId.
//!
//! ```text
//! # device A — set a doc and stay live, following device B:
//! ce-db --peers <B-node-id> watch users
//! ce-db --peers <B-node-id> set users/ada '{"name":"Ada","age":36}'
//!
//! # device B — follow device A and watch the doc appear in realtime:
//! ce-db --peers <A-node-id> watch users
//!
//! # query and read:
//! ce-db query users --where age:gt:30 --order age:desc --limit 5
//! ce-db get users/ada
//!
//! # capability gating: mint a write grant for a peer, scoped to one collection:
//! ce-db grant <peer-node-id> users --abilities db:read,db:write --expires 86400
//! ```

use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow};
use ce_coord::Coord;
use ce_db::query::{Dir, Op};
use ce_db::{
    ABILITY_ADMIN, ABILITY_READ, ABILITY_WRITE, Collection, CollectionGrant, DocPath, Filter,
    Query, Resource, node_id_from_hex,
};
use clap::{Parser, Subcommand};
use serde_json::Value;

#[derive(Parser)]
#[command(
    name = "ce-db",
    about = "Firestore-class realtime document DB over CE (ce-coord Merged + ce-cap)",
    version
)]
struct Cli {
    /// Peer device NodeIds (hex) whose writes to converge into this replica. Repeat or comma-separate.
    #[arg(long, global = true, value_delimiter = ',')]
    peers: Vec<String>,

    #[command(subcommand)]
    cmd: Cmd,
}

#[derive(Subcommand)]
enum Cmd {
    /// Set (replace) a document: `set <collection>/<doc_id> '<json-object>'`.
    Set { path: String, json: String },
    /// Patch (field-level merge) a document: `patch <collection>/<doc_id> '<json-object>'`.
    Patch { path: String, json: String },
    /// Read one document: `get <collection>/<doc_id>`.
    Get { path: String },
    /// Delete (tombstone) a document: `delete <collection>/<doc_id>`.
    Delete { path: String },
    /// Atomically increment a numeric field: `incr <collection>/<doc_id> <field> <delta>`.
    Incr {
        path: String,
        field: String,
        delta: i64,
    },
    /// List a collection, optionally filtered/ordered/limited.
    Query {
        /// Collection name.
        collection: String,
        /// Filter `field:op:value`, op in eq|ne|gt|ge|lt|le|contains. Repeatable (AND).
        #[arg(long = "where")]
        wheres: Vec<String>,
        /// Order by `field:asc` or `field:desc`. Repeatable / comma-separated for multi-field sort.
        #[arg(long, value_delimiter = ',')]
        order: Vec<String>,
        /// Skip the first N results (pagination).
        #[arg(long)]
        offset: Option<usize>,
        /// Max results.
        #[arg(long)]
        limit: Option<usize>,
    },
    /// Watch a collection and print a snapshot on every realtime change (Ctrl-C to stop).
    Watch { collection: String },
    /// Print this node's id and sync status for a collection.
    Status { collection: String },
    /// Compact this device's writer log for a collection into a content-addressed snapshot blob.
    Compact { collection: String },
    /// Mint a capability granting a peer access to a collection, printing a hex token.
    Grant {
        /// Audience NodeId (hex) who receives the grant.
        audience: String,
        /// Collection name to scope the grant to.
        collection: String,
        /// Abilities, comma-separated: db:read,db:write,db:admin.
        #[arg(long, default_value = "db:read")]
        abilities: String,
        /// Seconds until expiry (0 = never).
        #[arg(long, default_value_t = 0)]
        expires: u64,
        /// Capability nonce (issuer-unique; used for revocation).
        #[arg(long, default_value_t = 1)]
        nonce: u64,
    },
    /// Inspect a capability token: print scope, holder, root, and collection.
    Inspect { token: String },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .with_writer(std::io::stderr)
        .init();

    let cli = Cli::parse();
    let peers = cli.peers.clone();

    match cli.cmd {
        Cmd::Set { path, json } => {
            let p = DocPath::parse(&path)?;
            let coll = open(&peers, &p.collection).await?;
            coll.set(&p.doc_id, parse_object(&json)?).await?;
            println!("set {}", path);
        }
        Cmd::Patch { path, json } => {
            let p = DocPath::parse(&path)?;
            let coll = open(&peers, &p.collection).await?;
            coll.patch(&p.doc_id, parse_object(&json)?).await?;
            println!("patched {}", path);
        }
        Cmd::Get { path } => {
            let p = DocPath::parse(&path)?;
            let coll = open(&peers, &p.collection).await?;
            match coll.get(&p.doc_id) {
                Some(doc) => println!("{}", serde_json::to_string_pretty(&doc)?),
                None => {
                    eprintln!("not found: {path}");
                    std::process::exit(1);
                }
            }
        }
        Cmd::Delete { path } => {
            let p = DocPath::parse(&path)?;
            let coll = open(&peers, &p.collection).await?;
            coll.delete(&p.doc_id).await?;
            println!("deleted {}", path);
        }
        Cmd::Incr { path, field, delta } => {
            let p = DocPath::parse(&path)?;
            let coll = open(&peers, &p.collection).await?;
            coll.increment(&p.doc_id, &field, delta).await?;
            println!("incremented {path}.{field} by {delta}");
        }
        Cmd::Query {
            collection,
            wheres,
            order,
            offset,
            limit,
        } => {
            let coll = open(&peers, &collection).await?;
            let q = build_query(&wheres, &order, offset, limit)?;
            let results = coll.query(&q);
            if results.is_empty() {
                println!("(no documents)");
            }
            for (id, doc) in results {
                println!("{id}\t{}", serde_json::to_string(&doc)?);
            }
        }
        Cmd::Watch { collection } => {
            let coll = open(&peers, &collection).await?;
            coll.refresh();
            print_snapshot(&coll.snapshot());
            let mut rx = coll.watch();
            println!("-- watching '{collection}' (node {}) --", coll.writer_id());
            while let Some(snap) = coll.next_change(&mut rx).await {
                println!("-- change (op_count={}) --", snap.op_count);
                print_snapshot(&snap);
            }
        }
        Cmd::Status { collection } => {
            let coll = open(&peers, &collection).await?;
            println!("node: {}", coll.writer_id());
            println!("collection '{}': {} docs", collection, coll.len());
            for (dev, v) in coll.sync_status() {
                println!("  writer {dev} @ version {v}");
            }
        }
        Cmd::Compact { collection } => {
            let coll = open(&peers, &collection).await?;
            let cp = coll
                .compact()
                .await
                .context("compaction (needs a running node)")?;
            println!("snapshot @ base {} -> cid {}", cp.base, cp.cid);
        }
        Cmd::Grant {
            audience,
            collection,
            abilities,
            expires,
            nonce,
        } => {
            grant(&audience, &collection, &abilities, expires, nonce)?;
        }
        Cmd::Inspect { token } => {
            inspect(&token)?;
        }
    }

    Ok(())
}

/// Open a collection against the local CE node, following the given peer NodeIds.
async fn open(peers: &[String], collection: &str) -> Result<Collection> {
    let coord = Coord::connect()
        .await
        .context("connecting to the local CE node (is `ce start` running?)")?;
    Collection::open(&coord, collection, peers).await
}

/// Parse a JSON string into a document object (must be a JSON object).
fn parse_object(s: &str) -> Result<ce_db::Document> {
    let v: Value = serde_json::from_str(s).context("document must be valid JSON")?;
    v.as_object()
        .cloned()
        .ok_or_else(|| anyhow!("document must be a JSON object, e.g. '{{\"name\":\"x\"}}'"))
}

/// Build a [`Query`] from `field:op:value` filters, `field:asc|desc` order keys (multi-field),
/// an offset, and a limit.
fn build_query(
    wheres: &[String],
    order: &[String],
    offset: Option<usize>,
    limit: Option<usize>,
) -> Result<Query> {
    let mut q = Query::new();
    for w in wheres {
        q = q.with(parse_filter(w)?);
    }
    for o in order {
        let (field, dir) = parse_order(o)?;
        q = q.then_order(field, dir);
    }
    if let Some(n) = offset {
        q = q.skip(n);
    }
    if let Some(n) = limit {
        q = q.take(n);
    }
    Ok(q)
}

/// Parse one `field:asc|desc` order key.
fn parse_order(o: &str) -> Result<(String, Dir)> {
    let (field, dir) = o
        .split_once(':')
        .ok_or_else(|| anyhow!("--order must be 'field:asc' or 'field:desc', got '{o}'"))?;
    if field.is_empty() {
        return Err(anyhow!("--order field must be non-empty in '{o}'"));
    }
    let dir = match dir {
        "asc" => Dir::Asc,
        "desc" => Dir::Desc,
        other => return Err(anyhow!("unknown order direction '{other}' (use asc|desc)")),
    };
    Ok((field.to_string(), dir))
}

/// Parse one `field:op:value` filter. The value is parsed as JSON when possible; a *bare word* (no
/// JSON-significant leading char) is taken as a string literal, but a value that *looks like* JSON
/// (starts with `{`, `[`, `"`, a digit, `-`, or is `true`/`false`/`null`) and fails to parse is an
/// **error** rather than silently degrading to a string — so a typo like `age:gt:3O` is caught.
fn parse_filter(s: &str) -> Result<Filter> {
    let mut parts = s.splitn(3, ':');
    let field = parts
        .next()
        .filter(|f| !f.is_empty())
        .ok_or_else(|| bad_filter(s))?;
    let op_str = parts.next().ok_or_else(|| bad_filter(s))?;
    let value_str = parts.next().ok_or_else(|| bad_filter(s))?;
    let op = parse_op(op_str)?;
    let value = parse_value(value_str)
        .with_context(|| format!("invalid filter value '{value_str}' in '{s}'"))?;
    Ok(Filter::new(field.to_string(), op, value))
}

/// Parse a filter operator name.
fn parse_op(op_str: &str) -> Result<Op> {
    Ok(match op_str {
        "eq" => Op::Eq,
        "ne" => Op::Ne,
        "gt" => Op::Gt,
        "ge" => Op::Ge,
        "lt" => Op::Lt,
        "le" => Op::Le,
        "contains" => Op::Contains,
        "in" => Op::In,
        "notin" | "not-in" => Op::NotIn,
        "array-contains-any" | "anyof" => Op::ArrayContainsAny,
        other => {
            return Err(anyhow!(
                "unknown filter op '{other}' (eq|ne|gt|ge|lt|le|contains|in|notin|array-contains-any)"
            ));
        }
    })
}

/// Parse a CLI value: a bare word is a string; anything JSON-shaped must parse as valid JSON.
fn parse_value(value_str: &str) -> Result<Value> {
    let looks_jsonish = value_str
        .chars()
        .next()
        .map(|c| matches!(c, '{' | '[' | '"' | '-') || c.is_ascii_digit())
        .unwrap_or(false)
        || matches!(value_str, "true" | "false" | "null");
    if looks_jsonish {
        serde_json::from_str::<Value>(value_str).context("not valid JSON")
    } else {
        // A bare word (identifier) is taken literally as a string.
        Ok(Value::String(value_str.to_string()))
    }
}

fn bad_filter(s: &str) -> anyhow::Error {
    anyhow!("--where must be 'field:op:value', got '{s}'")
}

/// Mint a collection grant from this node's identity and print the hex token.
fn grant(
    audience: &str,
    collection: &str,
    abilities: &str,
    expires: u64,
    nonce: u64,
) -> Result<()> {
    let identity = load_identity()?;
    let audience_id = node_id_from_hex(audience).context("invalid audience node id")?;
    let abilities: Vec<&str> = abilities
        .split(',')
        .map(|a| a.trim())
        .filter(|a| !a.is_empty())
        .collect();
    validate_abilities(&abilities)?;
    let not_after = if expires == 0 { 0 } else { now() + expires };
    let g = CollectionGrant::mint(
        &identity,
        audience_id,
        collection,
        &abilities,
        Resource::Any,
        not_after,
        nonce,
    );
    println!(
        "# grant for collection '{collection}', abilities {:?}",
        abilities
    );
    println!("# audience {audience}");
    println!("# present this token to gain access:");
    println!("{}", g.to_token());
    Ok(())
}

/// Print the human-readable scope of a capability token.
fn inspect(token: &str) -> Result<()> {
    let g = CollectionGrant::from_token(token).context("not a valid ce-db grant token")?;
    println!("collection: {}", g.collection());
    if let Some(h) = g.holder() {
        println!("holder:     {}", ce_db::node_id_hex(&h));
    }
    if let Some(r) = g.root_issuer() {
        println!("root:       {}", ce_db::node_id_hex(&r));
    }
    if let Some(leaf) = g.leaf() {
        println!("abilities:  {:?}", leaf.abilities);
        if leaf.caveats.not_after != 0 {
            println!("expires:    unix {}", leaf.caveats.not_after);
        } else {
            println!("expires:    never");
        }
    }
    println!("chain links: {}", g.chain().len());
    Ok(())
}

fn validate_abilities(abilities: &[&str]) -> Result<()> {
    for a in abilities {
        if ![ABILITY_READ, ABILITY_WRITE, ABILITY_ADMIN].contains(a) {
            return Err(anyhow!(
                "unknown ability '{a}' (use db:read, db:write, db:admin)"
            ));
        }
    }
    if abilities.is_empty() {
        return Err(anyhow!("at least one ability is required"));
    }
    Ok(())
}

/// Load this node's identity from the default CE data dir (the same key the node uses).
fn load_identity() -> Result<ce_identity::Identity> {
    let dir = ce_data_dir()?.join("identity");
    ce_identity::Identity::load_or_generate(&dir)
        .context("loading CE identity (expected at <data-dir>/identity)")
}

/// The CE data directory (honors `CE_DATA_DIR`, else the platform default `~/.local/share/ce`).
fn ce_data_dir() -> Result<std::path::PathBuf> {
    if let Ok(dir) = std::env::var("CE_DATA_DIR") {
        return Ok(std::path::PathBuf::from(dir));
    }
    let base = directories_base()?;
    Ok(base)
}

fn directories_base() -> Result<std::path::PathBuf> {
    // Platform-native data dir via the `directories` crate (matches ce-storage / ce-pubsub):
    // `~/.local/share/ce` on Linux, `~/Library/Application Support/ce` on macOS,
    // `%APPDATA%\ce` on Windows. `$HOME` is unset on Windows, so don't hardcode it; the
    // `CE_DATA_DIR` override is handled by the caller (`ce_data_dir`).
    directories::ProjectDirs::from("", "", "ce")
        .map(|p| p.data_dir().to_path_buf())
        .ok_or_else(|| anyhow!("could not resolve a platform data dir; set CE_DATA_DIR"))
}

fn print_snapshot(snap: &ce_db::Snapshot) {
    if snap.docs.is_empty() {
        println!("(empty)");
        return;
    }
    for (id, doc) in &snap.docs {
        if let Ok(s) = serde_json::to_string(doc) {
            println!("{id}\t{s}");
        }
    }
}

fn now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parse_object_rejects_non_object() {
        assert!(parse_object("[1,2,3]").is_err());
        assert!(parse_object("not json").is_err());
        assert!(parse_object(r#"{"a":1}"#).is_ok());
    }

    #[test]
    fn parse_value_bare_word_is_string() {
        assert_eq!(parse_value("hello").unwrap(), json!("hello"));
        assert_eq!(parse_value("ada").unwrap(), json!("ada"));
    }

    #[test]
    fn parse_value_jsonish_must_be_valid() {
        // A jsonish-looking value that is malformed is an ERROR, not silently a string.
        assert!(parse_value("3O").is_err()); // typo: letter O
        assert!(parse_value("[1,2").is_err());
        assert!(parse_value(r#"{"a":}"#).is_err());
        // Valid JSON parses.
        assert_eq!(parse_value("30").unwrap(), json!(30));
        assert_eq!(parse_value("true").unwrap(), json!(true));
        assert_eq!(parse_value("-5").unwrap(), json!(-5));
        assert_eq!(parse_value(r#""quoted""#).unwrap(), json!("quoted"));
    }

    #[test]
    fn parse_filter_ok_and_ops() {
        let f = parse_filter("age:gt:30").unwrap();
        assert_eq!(f.field, "age");
        assert_eq!(f.op, Op::Gt);
        assert_eq!(f.value, json!(30));

        assert_eq!(parse_filter("name:eq:ada").unwrap().value, json!("ada"));
        assert_eq!(parse_filter("tags:in:[1,2]").unwrap().op, Op::In);
        assert_eq!(
            parse_filter("tags:array-contains-any:[1]").unwrap().op,
            Op::ArrayContainsAny
        );
    }

    #[test]
    fn parse_filter_errors() {
        assert!(parse_filter("nocolon").is_err());
        assert!(parse_filter(":eq:1").is_err()); // empty field
        assert!(parse_filter("age:bogus:1").is_err()); // unknown op
        assert!(parse_filter("age:gt:3O").is_err()); // bad numeric value
    }

    #[test]
    fn parse_order_ok_and_errors() {
        assert_eq!(
            parse_order("age:asc").unwrap(),
            ("age".to_string(), Dir::Asc)
        );
        assert_eq!(parse_order("name:desc").unwrap().1, Dir::Desc);
        assert!(parse_order("age").is_err());
        assert!(parse_order("age:sideways").is_err());
        assert!(parse_order(":asc").is_err());
    }

    #[test]
    fn build_query_multi_field_order_and_offset() {
        let q = build_query(
            &["age:gt:18".to_string()],
            &["age:desc".to_string(), "name:asc".to_string()],
            Some(2),
            Some(5),
        )
        .unwrap();
        assert_eq!(q.filters.len(), 1);
        assert_eq!(q.order_by.len(), 2);
        assert_eq!(q.offset, 2);
        assert_eq!(q.limit, Some(5));
    }

    #[test]
    fn validate_abilities_works() {
        assert!(validate_abilities(&["db:read", "db:write"]).is_ok());
        assert!(validate_abilities(&["db:bogus"]).is_err());
        assert!(validate_abilities(&[]).is_err());
    }
}
