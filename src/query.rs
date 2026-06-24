//! Queries: Firestore-style field filters, ordering, cursors, and limits over the materialized
//! collection.
//!
//! A [`Query`] is a conjunction of [`Filter`]s (`AND` semantics, like Firestore's `where` chain),
//! evaluated against the folded document map. It supports:
//!
//! * **Comparison / membership operators**: `Eq`, `Ne`, `Gt`, `Ge`, `Lt`, `Le`, `Contains`
//!   (array-contains / substring), `In`, `NotIn`, `ArrayContainsAny`.
//! * **Nested field paths**: `a.b.c` walks into nested objects for both filters and `order_by`
//!   (a literal field whose name contains a `.` is still reachable via [`Filter::eq_key`]).
//! * **Multi-field ordering** with a deterministic **document-id tie-break**, so a stable total order
//!   exists for cursor-based pagination.
//! * **Cursors** (`start_at` / `start_after` / `end_at` / `end_before`) plus `offset` and `limit`,
//!   enabling real pagination over ordered results.
//!
//! Numbers compare **numerically with full integer precision** (i64/u64 beyond 2^53 are compared as
//! integers, not coerced through f64), so large-integer fields sort correctly. The whole thing is a
//! pure function of the materialized state, so it runs identically on every replica.

use std::cmp::Ordering;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::doc::Document;

/// A comparison / membership operator on a single field.
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
    /// Field equals one of the values in the (array) right-hand side. Firestore `in`.
    In,
    /// Field equals none of the values in the (array) right-hand side. Firestore `not-in`. A missing
    /// field is treated as "not in" (it equals none of them), matching `Ne`'s missing-field rule.
    NotIn,
    /// Field (an array) shares at least one element with the (array) right-hand side. Firestore
    /// `array-contains-any`.
    ArrayContainsAny,
}

/// One field predicate: `<field_path> <op> <value>`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Filter {
    /// Field path to test. A `.` separates nested object keys (`a.b.c`); see [`Filter::eq_key`] for a
    /// literal top-level key whose name contains a dot.
    pub field: String,
    /// Whether `field` is a dotted nested path (`true`) or a single literal key (`false`).
    #[serde(default = "default_true")]
    pub nested: bool,
    /// Comparison operator.
    pub op: Op,
    /// Right-hand value.
    pub value: Value,
}

fn default_true() -> bool {
    true
}

impl Filter {
    /// Build an equality filter on a (possibly nested, dotted) field path.
    pub fn eq(field: impl Into<String>, value: Value) -> Filter {
        Filter {
            field: field.into(),
            nested: true,
            op: Op::Eq,
            value,
        }
    }

    /// Build an equality filter on a single **literal** top-level key (no dotted-path interpretation),
    /// for the rare case a field name itself contains a `.`.
    pub fn eq_key(field: impl Into<String>, value: Value) -> Filter {
        Filter {
            field: field.into(),
            nested: false,
            op: Op::Eq,
            value,
        }
    }

    /// Build a filter on a (possibly nested) field path with an explicit operator.
    pub fn new(field: impl Into<String>, op: Op, value: Value) -> Filter {
        Filter {
            field: field.into(),
            nested: true,
            op,
            value,
        }
    }

    /// Resolve this filter's field value out of a document (walking a dotted path if `nested`).
    fn lookup<'a>(&self, doc: &'a Document) -> Option<&'a Value> {
        if self.nested {
            get_path(doc, &self.field)
        } else {
            doc.get(&self.field)
        }
    }

    /// Evaluate this predicate against a document. A missing field never matches (except `Ne`/`NotIn`,
    /// which match a missing field since it is "not equal" to / "not in" the value set).
    pub fn matches(&self, doc: &Document) -> bool {
        let lhs = self.lookup(doc);
        match (&self.op, lhs) {
            (Op::Eq, Some(v)) => v == &self.value,
            (Op::Eq, None) => false,
            (Op::Ne, Some(v)) => v != &self.value,
            (Op::Ne, None) => true,
            (Op::Contains, Some(v)) => contains(v, &self.value),
            (Op::Contains, None) => false,
            (Op::In, Some(v)) => in_set(v, &self.value),
            (Op::In, None) => false,
            (Op::NotIn, Some(v)) => !in_set(v, &self.value),
            (Op::NotIn, None) => true,
            (Op::ArrayContainsAny, Some(v)) => array_contains_any(v, &self.value),
            (Op::ArrayContainsAny, None) => false,
            (cmp, Some(v)) => match order(v, &self.value) {
                Some(Ordering::Less) => matches!(cmp, Op::Lt | Op::Le | Op::Ne),
                Some(Ordering::Equal) => matches!(cmp, Op::Ge | Op::Le),
                Some(Ordering::Greater) => matches!(cmp, Op::Gt | Op::Ge | Op::Ne),
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

/// One ordering key: `(field_path, direction)`. Multiple are applied lexicographically.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct OrderBy {
    /// Dotted field path to sort on.
    pub field: String,
    /// Sort direction.
    pub dir: Dir,
}

/// A pagination cursor: a position in the ordered result, expressed as the ordering-key values of a
/// boundary document (plus its id for the tie-break). Built with [`Query::start_at`] et al.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Cursor {
    /// The ordering-key values, one per `order_by` clause, in order.
    pub values: Vec<Value>,
    /// The boundary document id (used for the doc-id tie-break). `None` = compare on values only.
    pub doc_id: Option<String>,
    /// `true` = the boundary is **inclusive** (`startAt`/`endAt`); `false` = **exclusive**
    /// (`startAfter`/`endBefore`).
    pub inclusive: bool,
}

/// A conjunctive query: all filters must match. Optionally orders, paginates (cursors/offset), and
/// limits the result.
///
/// ```
/// use ce_db::{Query, Filter, Op, Dir, Document};
/// use serde_json::json;
///
/// let docs: Vec<(String, Document)> = vec![
///     ("a".into(), json!({"age": 41}).as_object().unwrap().clone()),
///     ("b".into(), json!({"age": 28}).as_object().unwrap().clone()),
///     ("c".into(), json!({"age": 36}).as_object().unwrap().clone()),
/// ];
/// // Everyone over 30, oldest first, paginated after age 36.
/// let q = Query::new()
///     .with(Filter::new("age", Op::Gt, json!(30)))
///     .order("age", Dir::Asc)
///     .start_after(vec![json!(36)]);
/// let ids: Vec<_> = q.run(docs).into_iter().map(|(id, _)| id).collect();
/// assert_eq!(ids, vec!["a"]); // only age 41 is after the cursor
/// ```
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct Query {
    /// `AND`ed field predicates. Empty = match everything.
    pub filters: Vec<Filter>,
    /// Multi-field ordering applied after filtering, lexicographically. An implicit document-id
    /// tie-break (ascending) is always appended, so the order is a stable total order.
    #[serde(default)]
    pub order_by: Vec<OrderBy>,
    /// Optional lower-bound cursor (`startAt`/`startAfter`). Requires `order_by`.
    #[serde(default)]
    pub start: Option<Cursor>,
    /// Optional upper-bound cursor (`endAt`/`endBefore`). Requires `order_by`.
    #[serde(default)]
    pub end: Option<Cursor>,
    /// Number of leading results to skip (applied after cursors, before `limit`).
    #[serde(default)]
    pub offset: usize,
    /// Optional cap on the number of returned documents.
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

    /// Set the primary ordering, replacing any existing ordering (builder style). Chain
    /// [`then_order`](Self::then_order) for multi-field sorts.
    pub fn order(mut self, field: impl Into<String>, dir: Dir) -> Query {
        self.order_by = vec![OrderBy {
            field: field.into(),
            dir,
        }];
        self
    }

    /// Add a secondary (and further) ordering key (builder style).
    pub fn then_order(mut self, field: impl Into<String>, dir: Dir) -> Query {
        self.order_by.push(OrderBy {
            field: field.into(),
            dir,
        });
        self
    }

    /// Set a limit (builder style).
    pub fn take(mut self, n: usize) -> Query {
        self.limit = Some(n);
        self
    }

    /// Skip the first `n` results (builder style).
    pub fn skip(mut self, n: usize) -> Query {
        self.offset = n;
        self
    }

    /// Start the result **at** (inclusive) the position given by these ordering-key values.
    pub fn start_at(mut self, values: Vec<Value>) -> Query {
        self.start = Some(Cursor {
            values,
            doc_id: None,
            inclusive: true,
        });
        self
    }

    /// Start the result **after** (exclusive) the position given by these ordering-key values.
    pub fn start_after(mut self, values: Vec<Value>) -> Query {
        self.start = Some(Cursor {
            values,
            doc_id: None,
            inclusive: false,
        });
        self
    }

    /// End the result **at** (inclusive) the position given by these ordering-key values.
    pub fn end_at(mut self, values: Vec<Value>) -> Query {
        self.end = Some(Cursor {
            values,
            doc_id: None,
            inclusive: true,
        });
        self
    }

    /// End the result **before** (exclusive) the position given by these ordering-key values.
    pub fn end_before(mut self, values: Vec<Value>) -> Query {
        self.end = Some(Cursor {
            values,
            doc_id: None,
            inclusive: false,
        });
        self
    }

    /// Page **after** a specific result document (the Firestore `startAfter(docSnapshot)` form). The
    /// cursor captures the document's ordering-key values plus its id, so pagination is exact even
    /// when multiple documents share the same sort key.
    pub fn start_after_doc(mut self, doc_id: &str, doc: &Document) -> Query {
        self.start = Some(Cursor {
            values: self.key_values(doc),
            doc_id: Some(doc_id.into()),
            inclusive: false,
        });
        self
    }

    /// Page **at** a specific result document (inclusive).
    pub fn start_at_doc(mut self, doc_id: &str, doc: &Document) -> Query {
        self.start = Some(Cursor {
            values: self.key_values(doc),
            doc_id: Some(doc_id.into()),
            inclusive: true,
        });
        self
    }

    /// Extract this query's ordering-key values from a document (one per `order_by` clause).
    fn key_values(&self, doc: &Document) -> Vec<Value> {
        self.order_by
            .iter()
            .map(|o| get_path(doc, &o.field).cloned().unwrap_or(Value::Null))
            .collect()
    }

    /// Does this document satisfy every filter?
    pub fn matches(&self, doc: &Document) -> bool {
        self.filters.iter().all(|f| f.matches(doc))
    }

    /// Run the query over `(doc_id, Document)` pairs: filter, order, apply cursors, offset, then
    /// limit. Returns owned `(doc_id, Document)` results so callers don't borrow the engine's state.
    pub fn run<I>(&self, docs: I) -> Vec<(String, Document)>
    where
        I: IntoIterator<Item = (String, Document)>,
    {
        let mut out: Vec<(String, Document)> =
            docs.into_iter().filter(|(_, d)| self.matches(d)).collect();

        if !self.order_by.is_empty() {
            out.sort_by(|(id_a, a), (id_b, b)| self.cmp_docs(id_a, a, id_b, b));
        } else {
            // No explicit ordering: still sort by id for a deterministic, cursor-friendly result.
            out.sort_by(|(id_a, _), (id_b, _)| id_a.cmp(id_b));
        }

        // Cursors operate on the ordered sequence.
        if let Some(start) = &self.start {
            let pos = self.cursor_pos(&out, start, true);
            out.drain(..pos);
        }
        if let Some(end) = &self.end {
            let pos = self.cursor_pos(&out, end, false);
            out.truncate(pos);
        }

        if self.offset > 0 {
            let n = self.offset.min(out.len());
            out.drain(..n);
        }
        if let Some(n) = self.limit {
            out.truncate(n);
        }
        out
    }

    /// Compare two result rows by the multi-field ordering, with an ascending doc-id tie-break.
    fn cmp_docs(&self, id_a: &str, a: &Document, id_b: &str, b: &Document) -> Ordering {
        for ob in &self.order_by {
            let va = get_path(a, &ob.field);
            let vb = get_path(b, &ob.field);
            let base = match (va, vb) {
                (Some(x), Some(y)) => {
                    order(x, y).unwrap_or_else(|| type_rank(x).cmp(&type_rank(y)))
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            let dir_adjusted = match ob.dir {
                Dir::Asc => base,
                Dir::Desc => base.reverse(),
            };
            if dir_adjusted != Ordering::Equal {
                return dir_adjusted;
            }
        }
        // Stable total order: tie-break on doc id (always ascending).
        id_a.cmp(id_b)
    }

    /// Find the index in the ordered `rows` where a cursor boundary falls. For a `start` cursor
    /// (`is_start = true`) returns the index of the first row that is **at or after** the cursor (or
    /// strictly after, for an exclusive cursor). For an `end` cursor returns the count of rows to
    /// keep.
    fn cursor_pos(&self, rows: &[(String, Document)], cur: &Cursor, is_start: bool) -> usize {
        // partition_point over the ordered rows using the cursor comparison.
        rows.partition_point(|(id, doc)| {
            let c = self.cmp_to_cursor(id, doc, cur);
            if is_start {
                // Skip while row < cursor (and, if exclusive, while row == cursor).
                match c {
                    Ordering::Less => true,
                    Ordering::Equal => !cur.inclusive,
                    Ordering::Greater => false,
                }
            } else {
                // Keep while row < cursor (and, if inclusive, while row == cursor).
                match c {
                    Ordering::Less => true,
                    Ordering::Equal => cur.inclusive,
                    Ordering::Greater => false,
                }
            }
        })
    }

    /// Compare a row against a cursor in the query's ordering (respecting per-field direction and the
    /// optional doc-id tie-break).
    fn cmp_to_cursor(&self, id: &str, doc: &Document, cur: &Cursor) -> Ordering {
        for (i, ob) in self.order_by.iter().enumerate() {
            let row_v = get_path(doc, &ob.field);
            let cur_v = cur.values.get(i);
            let base = match (row_v, cur_v) {
                (Some(x), Some(y)) => {
                    order(x, y).unwrap_or_else(|| type_rank(x).cmp(&type_rank(y)))
                }
                (Some(_), None) => Ordering::Less,
                (None, Some(_)) => Ordering::Greater,
                (None, None) => Ordering::Equal,
            };
            let adjusted = match ob.dir {
                Dir::Asc => base,
                Dir::Desc => base.reverse(),
            };
            if adjusted != Ordering::Equal {
                return adjusted;
            }
        }
        match &cur.doc_id {
            Some(cid) => id.cmp(cid.as_str()),
            None => Ordering::Equal,
        }
    }
}

/// Walk a dotted path (`a.b.c`) into a document. A non-object encountered mid-path yields `None`.
pub fn get_path<'a>(doc: &'a Document, path: &str) -> Option<&'a Value> {
    let mut parts = path.split('.');
    let first = parts.next()?;
    let mut cur = doc.get(first)?;
    for part in parts {
        cur = cur.as_object()?.get(part)?;
    }
    Some(cur)
}

/// A coarse type ranking so heterogeneous values still sort deterministically (Firestore orders
/// null < bool < number < string < array < object). Used only as a tie-break when `order` returns
/// `None` (mixed types), to keep the sort a total order.
fn type_rank(v: &Value) -> u8 {
    match v {
        Value::Null => 0,
        Value::Bool(_) => 1,
        Value::Number(_) => 2,
        Value::String(_) => 3,
        Value::Array(_) => 4,
        Value::Object(_) => 5,
    }
}

/// Total-ish ordering of two JSON values for comparison filters and `order_by`. Numbers compare
/// numerically **with full integer precision** (i64/u64 are compared as integers, not via f64);
/// strings lexically; bools and null have a fixed order. Mixed/incomparable types return `None`.
pub(crate) fn order(a: &Value, b: &Value) -> Option<Ordering> {
    match (a, b) {
        (Value::Number(x), Value::Number(y)) => Some(cmp_numbers(x, y)),
        (Value::String(x), Value::String(y)) => Some(x.cmp(y)),
        (Value::Bool(x), Value::Bool(y)) => Some(x.cmp(y)),
        (Value::Null, Value::Null) => Some(Ordering::Equal),
        _ => None,
    }
}

/// Compare two JSON numbers without losing integer precision. If both fit i64 or both fit u64,
/// compare as integers; otherwise fall back to f64 (only reached for actual floats).
fn cmp_numbers(x: &serde_json::Number, y: &serde_json::Number) -> Ordering {
    if let (Some(xi), Some(yi)) = (x.as_i64(), y.as_i64()) {
        return xi.cmp(&yi);
    }
    if let (Some(xu), Some(yu)) = (x.as_u64(), y.as_u64()) {
        return xu.cmp(&yu);
    }
    // Mixed i64/u64 (one negative, one > i64::MAX): the i64 one is negative, so it is smaller.
    if let (Some(_), Some(_)) = (x.as_i64(), y.as_u64()) {
        return Ordering::Less; // x negative, y huge-positive
    }
    if let (Some(_), Some(_)) = (x.as_u64(), y.as_i64()) {
        return Ordering::Greater; // x huge-positive, y negative
    }
    let xf = x.as_f64().unwrap_or(f64::NAN);
    let yf = y.as_f64().unwrap_or(f64::NAN);
    xf.total_cmp(&yf)
}

/// `Contains` semantics: array membership or substring.
fn contains(haystack: &Value, needle: &Value) -> bool {
    match haystack {
        Value::Array(items) => items.iter().any(|i| i == needle),
        Value::String(s) => needle.as_str().is_some_and(|n| s.contains(n)),
        _ => false,
    }
}

/// `In`/`NotIn` semantics: the field value equals one of the array elements on the RHS.
fn in_set(field: &Value, set: &Value) -> bool {
    match set {
        Value::Array(items) => items.iter().any(|i| i == field),
        _ => false,
    }
}

/// `ArrayContainsAny`: the (array) field shares ≥1 element with the (array) RHS.
fn array_contains_any(field: &Value, set: &Value) -> bool {
    match (field, set) {
        (Value::Array(fa), Value::Array(sa)) => fa.iter().any(|f| sa.iter().any(|s| s == f)),
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
            (
                "u1".into(),
                doc(
                    json!({"name": "ada", "age": 36, "tags": ["math", "cs"], "loc": {"city": "lyon"}}),
                ),
            ),
            (
                "u2".into(),
                doc(json!({"name": "bob", "age": 28, "tags": ["art"], "loc": {"city": "rome"}})),
            ),
            (
                "u3".into(),
                doc(json!({"name": "cy", "age": 41, "tags": ["cs"], "loc": {"city": "lyon"}})),
            ),
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
        let q = Query::new().with(Filter::new("age", Op::Gt, json!(30)));
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);
    }

    #[test]
    fn ne_matches_missing_field() {
        let q = Query::new().with(Filter::new("missing", Op::Ne, json!(1)));
        assert_eq!(q.run(sample()).len(), 3);
    }

    #[test]
    fn contains_array_and_substring() {
        let q = Query::new().with(Filter::new("tags", Op::Contains, json!("cs")));
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);

        let q2 = Query::new().with(Filter::new("name", Op::Contains, json!("a")));
        let ids2: Vec<_> = q2.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids2, vec!["u1"]); // only "ada" contains "a"
    }

    #[test]
    fn in_and_not_in() {
        let q = Query::new().with(Filter::new("name", Op::In, json!(["ada", "cy"])));
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);

        let q2 = Query::new().with(Filter::new("name", Op::NotIn, json!(["ada", "cy"])));
        let ids2: Vec<_> = q2.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids2, vec!["u2"]);
    }

    #[test]
    fn array_contains_any() {
        let q = Query::new().with(Filter::new(
            "tags",
            Op::ArrayContainsAny,
            json!(["art", "cs"]),
        ));
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u2", "u3"]);
    }

    #[test]
    fn nested_field_path_filter() {
        let q = Query::new().with(Filter::eq("loc.city", json!("lyon")));
        let mut ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        ids.sort();
        assert_eq!(ids, vec!["u1", "u3"]);
    }

    #[test]
    fn conjunction_of_filters() {
        let q = Query::new()
            .with(Filter::new("age", Op::Ge, json!(30)))
            .with(Filter::new("tags", Op::Contains, json!("cs")));
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
    fn empty_query_matches_all_sorted_by_id() {
        let ids: Vec<_> = Query::new()
            .run(sample())
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(ids, vec!["u1", "u2", "u3"]);
    }

    #[test]
    fn multi_field_order_with_id_tiebreak() {
        // Both lyon docs share city; order by city asc then age asc, id tiebreak.
        let q = Query::new()
            .order("loc.city", Dir::Asc)
            .then_order("age", Dir::Asc);
        let ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        // lyon: u1(36), u3(41); rome: u2(28) -> lyon first
        assert_eq!(ids, vec!["u1", "u3", "u2"]);
    }

    #[test]
    fn cursor_start_after_value() {
        let q = Query::new()
            .order("age", Dir::Asc)
            .start_after(vec![json!(28)]);
        let ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["u1", "u3"]); // skip 28, keep 36 & 41
    }

    #[test]
    fn cursor_start_at_value() {
        let q = Query::new()
            .order("age", Dir::Asc)
            .start_at(vec![json!(36)]);
        let ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["u1", "u3"]);
    }

    #[test]
    fn cursor_end_before_value() {
        let q = Query::new()
            .order("age", Dir::Asc)
            .end_before(vec![json!(41)]);
        let ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["u2", "u1"]); // 28, 36
    }

    #[test]
    fn pagination_with_doc_cursor() {
        let s = sample();
        let q1 = Query::new().order("age", Dir::Asc).take(1);
        let page1 = q1.run(s.clone());
        assert_eq!(page1[0].0, "u2"); // 28
        // next page after that doc
        let q2 = Query::new()
            .order("age", Dir::Asc)
            .start_after_doc(&page1[0].0, &page1[0].1)
            .take(1);
        let page2 = q2.run(s.clone());
        assert_eq!(page2[0].0, "u1"); // 36
    }

    #[test]
    fn offset_and_limit() {
        let q = Query::new().order("age", Dir::Asc).skip(1).take(1);
        let ids: Vec<_> = q.run(sample()).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["u1"]); // skip 28, take 36
    }

    #[test]
    fn large_integers_compare_precisely() {
        // Two integers that are distinct but identical when coerced to f64 (both > 2^53).
        let a = json!(9_007_199_254_740_993_i64); // 2^53 + 1
        let b = json!(9_007_199_254_740_992_i64); // 2^53
        assert_eq!(order(&a, &b), Some(Ordering::Greater));
        let docs = vec![
            (
                "big".to_string(),
                doc(json!({"n": 9_007_199_254_740_993_i64})),
            ),
            (
                "small".to_string(),
                doc(json!({"n": 9_007_199_254_740_992_i64})),
            ),
        ];
        let q = Query::new().with(Filter::new("n", Op::Gt, json!(9_007_199_254_740_992_i64)));
        let ids: Vec<_> = q.run(docs).into_iter().map(|(id, _)| id).collect();
        assert_eq!(ids, vec!["big"]);
    }

    #[test]
    fn literal_key_with_dot() {
        let mut d = Document::new();
        d.insert("a.b".to_string(), json!(1));
        let m = vec![("x".to_string(), d)];
        // nested path "a.b" would look for object a -> b and fail; literal key matches.
        assert!(
            Query::new()
                .with(Filter::eq("a.b", json!(1)))
                .run(m.clone())
                .is_empty()
        );
        assert_eq!(
            Query::new()
                .with(Filter::eq_key("a.b", json!(1)))
                .run(m)
                .len(),
            1
        );
    }
}
