//! Queries: Firestore-style field filters over the materialized collection.
//!
//! A [`Query`] is a conjunction of [`Filter`]s (`AND` semantics, like Firestore's `where` chain),
//! evaluated against the folded document map. It is intentionally simple — equality, comparison, and
//! membership on top-level fields — because ce-db is a CRDT document store, not a SQL engine. Ordering
//! and limiting happen after filtering. The whole thing is a pure function of the materialized state,
//! so it runs the same on every replica.

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::doc::Document;

/// A comparison operator on a single field.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Op {
    /// Field equals the value (JSON-equality).
    Eq,
    /// Field does not equal the value.
    Ne,
    /// Field is numerically/lexically greater than the value.
    Gt,
    /// Field is greater than or equal to the value.
    Ge,
    /// Field is less than the value.
    Lt,
    /// Field is less than or equal to the value.
    Le,
    /// Field (an array) contains the value, or (a string) contains the value as a substring.
    Contains,
}

/// One field predicate: `<field> <op> <value>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    /// Top-level field name to test.
    pub field: String,
    /// Comparison operator.
    pub op: Op,
    /// Right-hand value.
    pub value: Value,
}

impl Filter {
    /// Build an equality filter (the common case).
    pub fn eq(field: impl Into<String>, value: Value) -> Filter {
        Filter { field: field.into(), op: Op::Eq, value }
    }

    /// Evaluate this predicate against a document. A missing field never matches (except `Ne`, which
    /// matches a missing field since it is "not equal" to the value).
    pub fn matches(&self, doc: &Document) -> bool {
        let lhs = doc.get(&self.field);
        match (self.op.clone(), lhs) {
            (Op::Eq, Some(v)) => v == &self.value,
            (Op::Eq, None) => false,
            (Op::Ne, Some(v)) => v != &self.value,
            (Op::Ne, None) => true,
            (Op::Contains, Some(v)) => contains(v, &self.value),
            (Op::Contains, None) => false,
            (cmp, Some(v)) => match order(v, &self.value) {
                Some(std::cmp::Ordering::Less) => matches!(cmp, Op::Lt | Op::Le | Op::Ne),
                Some(std::cmp::Ordering::Equal) => matches!(cmp, Op::Ge | Op::Le),
                Some(std::cmp::Ordering::Greater) => matches!(cmp, Op::Gt | Op::Ge | Op::Ne),
                None => false,
            },
            (_, None) => false,
        }
    }
}

/// Sort direction for [`Query::order_by`].
#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum Dir {
    /// Ascending.
    Asc,
    /// Descending.
    Desc,
}

/// A conjunctive query: all filters must match. Optionally orders and limits the result.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Query {
    /// `AND`ed field predicates. Empty = match everything.
    pub filters: Vec<Filter>,
    /// Optional `(field, direction)` ordering applied after filtering. Documents missing the field
    /// sort last (ascending) / first (descending).
    pub order_by: Option<(String, Dir)>,
    /// Optional cap on the number of returned documents (after ordering).
    pub limit: Option<usize>,
}

impl Query {
    /// An empty query (matches all documents).
    pub fn new() -> Query {
        Query::default()
    }

    /// Add a filter (builder style).
    pub fn with(mut self, filter: Filter) -> Query {
        self.filters.push(filter);
        self
    }

    /// Set ordering (builder style).
    pub fn order(mut self, field: impl Into<String>, dir: Dir) -> Query {
        self.order_by = Some((field.into(), dir));
        self
    }

    /// Set a limit (builder style).
    pub fn take(mut self, n: usize) -> Query {
        self.limit = Some(n);
        self
    }

    /// Does this document satisfy every filter?
    pub fn matches(&self, doc: &Document) -> bool {
        self.filters.iter().all(|f| f.matches(doc))
    }

    /// Run the query over `(doc_id, Document)` pairs: filter, order, then limit. Returns owned
    /// `(doc_id, Document)` results so callers don't borrow the engine's state.
    pub fn run<I>(&self, docs: I) -> Vec<(String, Document)>
    where
        I: IntoIterator<Item = (String, Document)>,
    {
        let mut out: Vec<(String, Document)> =
            docs.into_iter().filter(|(_, d)| self.matches(d)).collect();

        if let Some((field, dir)) = &self.order_by {
            out.sort_by(|(_, a), (_, b)| {
                let ord = match (a.get(field), b.get(field)) {
                    (Some(x), Some(y)) => order(x, y).unwrap_or(std::cmp::Ordering::Equal),
                    (Some(_), None) => std::cmp::Ordering::Less,
                    (None, Some(_)) => std::cmp::Ordering::Greater,
                    (None, None) => std::cmp::Ordering::Equal,
                };
                match dir {
                    Dir::Asc => ord,
                    Dir::Desc => ord.reverse(),
                }
            });
        }

        if let Some(n) = self.limit {
            out.truncate(n);
        }
        out
    }
}

/// Total-ish ordering of two JSON values for comparison filters and `order_by`. Numbers compare
/// numerically; strings lexically; bools and null have a fixed order. Mixed/incomparable types
/// return `None` (predicate fails; sort treats as equal).
fn order(a: &Value, b: &Value) -> Option<std::cmp::Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => {
            x.as_f64().and_then(|xf| y.as_f64().map(|yf| xf.total_cmp(&yf)))
        }
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Null, Value::Null) => Some(std::cmp::Ordering::Equal),
        _ => None,
    }
}

/// `Contains` semantics: array membership or substring.
fn contains(haystack: &Value, needle: &Value) -> bool {
    match haystack {
        Value::Array(items) => items.iter().any(|i| i == needle),
        Value::String(s) => needle.as_str().is_some_and(|n| s.contains(n)),
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn doc(v: Value) -> Document {
        v.as_object().cloned().unwrap()
    }

    fn sample() -> Vec<(String, Document)> {
        vec![
            ("u1".into(), doc(json!({"name": "ada", "age": 36, "tags": ["math", "cs"]}))),
            ("u2".into(), doc(json!({"name": "bob", "age": 28, "tags": ["art"]}))),
            ("u3".into(), doc(json!({"name": "cy", "age": 41, "tags": ["cs"]}))),
        ]
    }

    #[test]
    fn eq_filter() {
        let q = Query::new().with(Filter::eq("name", json!("ada")));
        let r = q.run(sample());
        assert_eq!(r.len(), 1);
        assert_eq!(r[0].0, "u1");
    }

    #[test]
    fn comparison_filters() {
        let q = Query::new().with(Filter { field: "age".into(), op: Op::Gt, value: json!(30) });
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);
    }

    #[test]
    fn ne_matches_missing_field() {
        let q = Query::new().with(Filter { field: "missing".into(), op: Op::Ne, value: json!(1) });
        assert_eq!(q.run(sample()).len(), 3);
    }

    #[test]
    fn contains_array_and_substring() {
        let q = Query::new().with(Filter { field: "tags".into(), op: Op::Contains, value: json!("cs") });
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);

        let q2 = Query::new().with(Filter { field: "name".into(), op: Op::Contains, value: json!("a") });
        let ids2: Vec<_> = q2.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids2, vec!["u1"]); // only "ada" contains "a"
    }

    #[test]
    fn conjunction_of_filters() {
        let q = Query::new()
            .with(Filter { field: "age".into(), op: Op::Ge, value: json!(30) })
            .with(Filter { field: "tags".into(), op: Op::Contains, value: json!("cs") });
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);
    }

    #[test]
    fn order_and_limit() {
        let q = Query::new().order("age", Dir::Desc).take(2);
        let ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["u3", "u1"]); // 41, 36 (then 28 dropped by limit)
    }

    #[test]
    fn empty_query_matches_all() {
        assert_eq!(Query::new().run(sample()).len(), 3);
    }
}
