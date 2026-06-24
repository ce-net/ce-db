//! Input validation and DoS / OOM guard rails.
//!
//! ce-db folds the entire merged op-set on every read, so unbounded documents, field counts, or
//! op history are a memory-amplification and latency hazard. This module centralizes the bounds and
//! the validators every write path runs *before* an op enters the writer log. Validation is cheap,
//! deterministic, and returns an [`anyhow::Error`] (never panics) so callers surface a clean error
//! instead of growing the union unboundedly.
//!
//! The defaults mirror Firestore's own document constraints closely (≈1 MiB/doc, bounded id length)
//! and add op-history / writer-count ceilings that Firestore does not need because it is server-side.
//! All limits are configurable per [`Collection`](crate::Collection) via [`Limits`].

use anyhow::{Result, bail};
use serde_json::Value;

use crate::doc::Document;

/// Tunable size / count ceilings enforced on every write and on `open`.
///
/// Construct [`Limits::default`] for Firestore-like bounds, or build a custom set for tighter
/// (embedded) or looser (trusted-fleet) deployments. A value of `0` on any numeric field is treated
/// as *that specific limit disabled* (unbounded), which is occasionally useful in tests; production
/// deployments should keep the defaults.
///
/// ```
/// use ce_db::Limits;
/// use serde_json::json;
///
/// let limits = Limits { max_doc_bytes: 32, ..Limits::default() };
/// // A small doc passes; an oversize one is rejected before it can enter the log.
/// let small = json!({"v": 1}).as_object().unwrap().clone();
/// assert!(limits.check_document(&small).is_ok());
/// let big = json!({"blob": "x".repeat(100)}).as_object().unwrap().clone();
/// assert!(limits.check_document(&big).is_err());
/// // Structure is always validated: ids may not contain '/'.
/// assert!(limits.check_doc_id("a/b").is_err());
/// ```
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Limits {
    /// Maximum serialized byte size of a single document (default 1 MiB, matching Firestore).
    pub max_doc_bytes: usize,
    /// Maximum number of top-level fields in a document (default 1024).
    pub max_fields: usize,
    /// Maximum byte length of a field *name* (default 1500, matching Firestore).
    pub max_field_name_bytes: usize,
    /// Maximum nesting depth of any JSON value in a document (default 32, matching Firestore).
    pub max_depth: usize,
    /// Maximum byte length of a document id (default 1500, matching Firestore).
    pub max_doc_id_bytes: usize,
    /// Maximum byte length of a collection name / path segment (default 256).
    pub max_collection_bytes: usize,
    /// Maximum number of distinct ops in the merged union before writes are refused (default 1_000_000).
    /// This is the backpressure ceiling that keeps the in-memory fold bounded; compaction lowers it.
    pub max_ops: usize,
    /// Maximum number of *peer* writer logs a collection will follow (default 1024).
    pub max_writers: usize,
    /// Maximum number of ops in a single [`WriteBatch`](crate::WriteBatch) / transaction (default 500,
    /// matching Firestore's batch limit).
    pub max_batch_ops: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Limits {
            max_doc_bytes: 1024 * 1024,
            max_fields: 1024,
            max_field_name_bytes: 1500,
            max_depth: 32,
            max_doc_id_bytes: 1500,
            max_collection_bytes: 256,
            max_ops: 1_000_000,
            max_writers: 1024,
            max_batch_ops: 500,
        }
    }
}

impl Limits {
    /// A permissive set used by tests that intentionally exercise large inputs. All ceilings are kept
    /// finite but very high; it does NOT disable validation of *structure* (charset, non-empty).
    pub fn permissive() -> Self {
        Limits {
            max_doc_bytes: 64 * 1024 * 1024,
            max_fields: 1 << 20,
            max_field_name_bytes: 1 << 20,
            max_depth: 1024,
            max_doc_id_bytes: 1 << 16,
            max_collection_bytes: 1 << 16,
            max_ops: usize::MAX,
            max_writers: usize::MAX,
            max_batch_ops: usize::MAX,
        }
    }

    /// Validate a collection name / path segment: non-empty, within byte bound, no control chars,
    /// and not `.`/`..` (which would be ambiguous in a hierarchical path). The `/` separator is
    /// rejected here because a single segment must never contain it.
    pub fn check_collection(&self, name: &str) -> Result<()> {
        check_segment(name, self.max_collection_bytes, "collection name")
    }

    /// Validate a document id: non-empty, within byte bound, no control chars, not `.`/`..`. Unlike a
    /// collection name, a doc id MAY NOT contain `/` either (hierarchy is expressed via subcollections
    /// — see [`Collection::subcollection`](crate::Collection::subcollection) — not slashes in the id).
    pub fn check_doc_id(&self, id: &str) -> Result<()> {
        check_segment(id, self.max_doc_id_bytes, "document id")
    }

    /// Validate a whole document about to be written: field count, field-name lengths, nesting depth,
    /// and total serialized byte size. Returns the byte size on success (handy for metrics/logging).
    pub fn check_document(&self, doc: &Document) -> Result<usize> {
        if self.max_fields != 0 && doc.len() > self.max_fields {
            bail!(
                "document has {} fields, limit is {}",
                doc.len(),
                self.max_fields
            );
        }
        for name in doc.keys() {
            if name.is_empty() {
                bail!("document field names must be non-empty");
            }
            if self.max_field_name_bytes != 0 && name.len() > self.max_field_name_bytes {
                bail!(
                    "field name '{}' is {} bytes, limit is {}",
                    truncate(name),
                    name.len(),
                    self.max_field_name_bytes
                );
            }
        }
        for v in doc.values() {
            self.check_depth(v, 1)?;
        }
        let bytes = serialized_len(doc)?;
        if self.max_doc_bytes != 0 && bytes > self.max_doc_bytes {
            bail!("document is {bytes} bytes, limit is {}", self.max_doc_bytes);
        }
        Ok(bytes)
    }

    fn check_depth(&self, v: &Value, depth: usize) -> Result<()> {
        if self.max_depth != 0 && depth > self.max_depth {
            bail!("document nesting exceeds depth limit {}", self.max_depth);
        }
        match v {
            Value::Array(items) => {
                for i in items {
                    self.check_depth(i, depth + 1)?;
                }
            }
            Value::Object(map) => {
                for (k, i) in map {
                    if self.max_field_name_bytes != 0 && k.len() > self.max_field_name_bytes {
                        bail!("nested field name '{}' exceeds limit", truncate(k));
                    }
                    self.check_depth(i, depth + 1)?;
                }
            }
            _ => {}
        }
        Ok(())
    }
}

/// Validate one path segment (collection name or doc id).
fn check_segment(s: &str, max_bytes: usize, what: &str) -> Result<()> {
    if s.is_empty() {
        bail!("{what} must be non-empty");
    }
    if max_bytes != 0 && s.len() > max_bytes {
        bail!("{what} is {} bytes, limit is {max_bytes}", s.len());
    }
    if s == "." || s == ".." {
        bail!("{what} may not be '.' or '..'");
    }
    if s.contains('/') {
        bail!("{what} may not contain '/'");
    }
    if s.chars().any(|c| c.is_control()) {
        bail!("{what} may not contain control characters");
    }
    Ok(())
}

/// Serialized JSON byte length of a document (the metric `max_doc_bytes` bounds).
fn serialized_len(doc: &Document) -> Result<usize> {
    Ok(serde_json::to_vec(doc)?.len())
}

/// Truncate a long string for error messages.
fn truncate(s: &str) -> String {
    if s.len() <= 64 {
        s.to_string()
    } else {
        format!("{}…", &s[..64])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn obj(v: Value) -> Document {
        v.as_object().cloned().unwrap()
    }

    #[test]
    fn rejects_empty_and_slash_and_dotdot() {
        let l = Limits::default();
        assert!(l.check_collection("").is_err());
        assert!(l.check_collection("a/b").is_err());
        assert!(l.check_doc_id("..").is_err());
        assert!(l.check_doc_id(".").is_err());
        assert!(l.check_doc_id("ok").is_ok());
    }

    #[test]
    fn rejects_control_chars() {
        let l = Limits::default();
        assert!(l.check_doc_id("a\nb").is_err());
        assert!(l.check_collection("a\u{0}b").is_err());
    }

    #[test]
    fn rejects_oversize_id() {
        let l = Limits {
            max_doc_id_bytes: 4,
            ..Limits::default()
        };
        assert!(l.check_doc_id("toolong").is_err());
        assert!(l.check_doc_id("ok").is_ok());
    }

    #[test]
    fn rejects_too_many_fields() {
        let l = Limits {
            max_fields: 2,
            ..Limits::default()
        };
        assert!(l.check_document(&obj(json!({"a":1,"b":2,"c":3}))).is_err());
        assert!(l.check_document(&obj(json!({"a":1,"b":2}))).is_ok());
    }

    #[test]
    fn rejects_oversize_doc() {
        let l = Limits {
            max_doc_bytes: 16,
            ..Limits::default()
        };
        assert!(
            l.check_document(&obj(json!({"a":"xxxxxxxxxxxxxxxxxxxxxxxx"})))
                .is_err()
        );
    }

    #[test]
    fn rejects_deep_nesting() {
        let l = Limits {
            max_depth: 3,
            ..Limits::default()
        };
        // depth 4 nested object
        let deep = json!({"a": {"b": {"c": {"d": 1}}}});
        assert!(l.check_document(&obj(deep)).is_err());
        let ok = json!({"a": {"b": 1}});
        assert!(l.check_document(&obj(ok)).is_ok());
    }

    #[test]
    fn rejects_empty_field_name() {
        let l = Limits::default();
        let mut d = Document::new();
        d.insert(String::new(), json!(1));
        assert!(l.check_document(&d).is_err());
    }

    #[test]
    fn permissive_allows_big_but_still_validates_structure() {
        let l = Limits::permissive();
        assert!(
            l.check_document(&obj(json!({"a": "x".repeat(100_000)})))
                .is_ok()
        );
        assert!(l.check_doc_id("a/b").is_err()); // structure still enforced
    }
}
