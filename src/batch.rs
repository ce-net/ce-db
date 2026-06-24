//! Batched writes and optimistic transactions (Firestore `WriteBatch` / `runTransaction`).
//!
//! ## WriteBatch
//!
//! A [`WriteBatch`] stages multiple document mutations and commits them in **one contiguous Lamport
//! range** on this device's writer log. Because the keys are contiguous and strictly increasing, a
//! peer that pulls the batch observes the ops together and in order — it never interleaves another
//! device's op *between* two ops of the same batch on this writer (cross-device ordering is still
//! governed by the global `(lamport, writer)` total order, as it must be in a leaderless CRDT).
//!
//! This gives the local-atomicity Firestore batches provide for the *issuing* client: either the
//! whole staged set is proposed, or (on a validation error before commit) none of it is. It does
//! **not** provide cross-device serializable isolation — that requires the chain (see the crate-level
//! "What ce-db is not" docs). Validation of every staged op happens up front, so a batch with one bad
//! document fails as a unit without writing anything.
//!
//! ## runTransaction
//!
//! [`Collection::run_transaction`](crate::Collection) is an *optimistic* read-then-write helper: it
//! reads the current materialized state, runs your closure to compute writes, then re-checks that the
//! collection's version (op-count) has not advanced before committing. If it has, it retries (bounded
//! attempts). This is optimistic concurrency over a single device's view; the CRDT still converges if
//! two devices commit concurrently, but the transaction's read-set is only guaranteed stable on the
//! issuing device. The caveat is documented and tested.

use std::collections::BTreeMap;

use anyhow::Result;
use serde_json::{Number, Value};

use crate::collection::Collection;
use crate::doc::{DocOp, Document, FieldOp, OpKind};

/// A staged sequence of writes committed atomically to this device's writer log.
///
/// Build it via [`Collection::batch`](crate::Collection::batch), stage operations, then
/// [`commit`](WriteBatch::commit). Operations apply in stage order within the batch.
pub struct WriteBatch<'a> {
    coll: &'a Collection,
    staged: Vec<(String, OpKind)>,
    error: Option<anyhow::Error>,
}

impl<'a> WriteBatch<'a> {
    pub(crate) fn new(coll: &'a Collection) -> WriteBatch<'a> {
        WriteBatch {
            coll,
            staged: Vec::new(),
            error: None,
        }
    }

    /// Stage a whole-document set. Validation is deferred to [`commit`](Self::commit) so the builder
    /// chains cleanly; the first staging error is remembered and surfaced at commit.
    pub fn set(mut self, doc_id: &str, doc: Document) -> Self {
        self.stage(doc_id, OpKind::Set(doc));
        self
    }

    /// Stage a field-level patch.
    pub fn patch(mut self, doc_id: &str, fields: Document) -> Self {
        self.stage(doc_id, OpKind::Patch(fields));
        self
    }

    /// Stage a delete (tombstone).
    pub fn delete(mut self, doc_id: &str) -> Self {
        self.stage(doc_id, OpKind::Delete);
        self
    }

    /// Stage an atomic numeric increment.
    pub fn increment(mut self, doc_id: &str, field: &str, delta: i64) -> Self {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::Increment(Number::from(delta)));
        self.stage(doc_id, OpKind::Update(ops));
        self
    }

    /// Stage an atomic array union.
    pub fn array_union(mut self, doc_id: &str, field: &str, items: Vec<Value>) -> Self {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::ArrayUnion(items));
        self.stage(doc_id, OpKind::Update(ops));
        self
    }

    /// Stage an atomic array remove.
    pub fn array_remove(mut self, doc_id: &str, field: &str, items: Vec<Value>) -> Self {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::ArrayRemove(items));
        self.stage(doc_id, OpKind::Update(ops));
        self
    }

    // --- in-place staging (for `run_transaction` closures, which borrow `&mut WriteBatch`) ---

    /// Stage a whole-document set in place.
    pub fn add_set(&mut self, doc_id: &str, doc: Document) {
        self.stage(doc_id, OpKind::Set(doc));
    }

    /// Stage a field-level patch in place.
    pub fn add_patch(&mut self, doc_id: &str, fields: Document) {
        self.stage(doc_id, OpKind::Patch(fields));
    }

    /// Stage a delete in place.
    pub fn add_delete(&mut self, doc_id: &str) {
        self.stage(doc_id, OpKind::Delete);
    }

    /// Stage an atomic increment in place.
    pub fn add_increment(&mut self, doc_id: &str, field: &str, delta: i64) {
        let mut ops = BTreeMap::new();
        ops.insert(field.to_string(), FieldOp::Increment(Number::from(delta)));
        self.stage(doc_id, OpKind::Update(ops));
    }

    /// Number of staged operations.
    pub fn len(&self) -> usize {
        self.staged.len()
    }

    /// True if no operations are staged.
    pub fn is_empty(&self) -> bool {
        self.staged.is_empty()
    }

    fn stage(&mut self, doc_id: &str, kind: OpKind) {
        if self.error.is_some() {
            return;
        }
        if let Err(e) = self.validate(doc_id, &kind) {
            self.error = Some(e);
            return;
        }
        self.staged.push((doc_id.to_string(), kind));
    }

    /// Validate one staged op against the collection's limits (without writing).
    fn validate(&self, doc_id: &str, kind: &OpKind) -> Result<()> {
        let limits = self.coll.limits();
        limits.check_doc_id(doc_id)?;
        match kind {
            OpKind::Set(d) | OpKind::Patch(d) => {
                limits.check_document(d)?;
            }
            OpKind::Update(ops) => {
                if limits.max_fields != 0 && ops.len() > limits.max_fields {
                    anyhow::bail!("update touches too many fields");
                }
            }
            OpKind::Delete => {}
        }
        Ok(())
    }

    /// Commit all staged ops atomically (from the issuing device's perspective) under one contiguous
    /// Lamport range. Fails as a unit (writing nothing) if any staged op was invalid or the batch
    /// exceeds the configured `max_batch_ops`.
    pub async fn commit(self) -> Result<()> {
        if let Some(e) = self.error {
            return Err(e);
        }
        let n = self.staged.len();
        if n == 0 {
            return Ok(());
        }
        let limits = self.coll.limits();
        if limits.max_batch_ops != 0 && n > limits.max_batch_ops {
            anyhow::bail!("batch has {n} ops, limit is {}", limits.max_batch_ops);
        }
        // Backpressure: refuse if committing would blow past the op ceiling.
        if limits.max_ops != 0 {
            let cur = self.coll.op_count_for_batch();
            if cur.saturating_add(n) > limits.max_ops {
                anyhow::bail!(
                    "batch would exceed collection op-count limit {}",
                    limits.max_ops
                );
            }
        }
        // Allocate a contiguous key range, then propose in order.
        let keys = self.coll.next_keys(n);
        for ((doc_id, kind), key) in self.staged.into_iter().zip(keys) {
            let op = DocOp { key, doc_id, kind };
            self.coll.propose_op(op).await?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // WriteBatch's live behavior is covered in tests/live.rs (it needs a Merged log). The pure
    // staging/validation logic is covered here only indirectly; see the live suite for commit.
}
