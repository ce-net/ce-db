# Changelog

All notable changes to `ce-db` are documented here. This project adheres to semantic versioning once
it reaches 1.0; pre-1.0 minor versions may include breaking changes.

## [Unreleased] — maturity pass

### Added

- **Capability enforcement on the data path** (`GuardedCollection` + `AuthPolicy`): every read requires
  `db:read`, every write `db:write`, admin ops `db:admin`, verified against accepted roots + on-chain
  revocation **before** any state is touched. `CollectionGrant` was previously only a verification
  toolkit; it is now actually enforced. Failure-path tests prove unauthorized / expired /
  wrong-collection / wrong-requester / untrusted-root / revoked / forged grants are denied.
- **Atomic field operations** (`FieldOp` / `OpKind::Update`): `increment` (a CRDT PN-counter —
  concurrent increments sum), `array_union`, `array_remove`, and `set_server_timestamp`. Convergence
  is order-independent and proven by property tests.
- **Subcollections**: `Collection::subcollection` gives hierarchical addressing
  (`collection/doc/sub/...`), each an independent, separately-gated, separately-converging `Merged`
  log.
- **Query cursors & pagination**: `start_at` / `start_after` / `end_at` / `end_before`, plus
  `start_after_doc` / `start_at_doc`, `offset`, and a stable total order (multi-field `order_by` with
  an always-appended doc-id tie-break).
- **Richer query operators**: `In`, `NotIn`, `ArrayContainsAny`, plus **nested dotted field paths**
  (`a.b.c`) for both filters and ordering.
- **Nested patch deep-merge**: `Patch` now merges nested objects field-by-field (with `null` deleting
  a nested key) instead of wholesale-replacing them.
- **Per-document change deltas** (`Snapshot::diff`, `Collection::changes`, `DocChange` /
  `ChangeKind`): onSnapshot-style added/modified/removed streams.
- **Push-based realtime** (`Collection::start_realtime` / `stop_realtime`): a background poller folds
  peer ops on an interval so watchers wake on peer writes with no external loop. Aborted on drop.
- **Batched writes & optimistic transactions**: `Collection::batch` (`WriteBatch`, atomic from the
  issuer's view via one contiguous Lamport range) and `Collection::run_transaction` (optimistic
  read-then-write with version re-check and bounded retries).
- **Input validation & DoS limits** (`Limits`): max document bytes, field count, nesting depth, id /
  field-name lengths, op-history ceiling (backpressure), peer count, and batch size — validated on
  every write, with Firestore-like defaults. Failure-path tests included.
- **Document metadata** (`DocMeta` / `Collection::meta`): create/update op-key versions.
- CLI: `incr` command, multi-field `--order` (comma-separated), `--offset`, and `in`/`notin`/
  `array-contains-any` operators in `--where`.
- `examples/basic.rs` and `examples/guarded.rs`; expanded README and rustdoc.

### Changed / Fixed

- **Materialized read cache**: reads now re-fold only when the merged op-count advances, so repeated
  reads at the same version are O(1) instead of O(history) per call.
- **Integer-precision ordering**: numeric comparison/ordering compares i64/u64 as integers (no longer
  coerced through `f64`), so fields beyond 2^53 sort and filter correctly.
- **Lamport allocator hardening**: clock-advance and key-increment now happen in a single critical
  section (no two-lock race), and all production-path mutexes are poison-tolerant (no `unwrap()` on
  lock).
- **CLI value parsing**: a JSON-shaped `--where` value that fails to parse is now an error instead of
  silently degrading to a string literal (e.g. `age:gt:3O` is rejected).

### Deferred (documented honestly)

- Secondary indexes (queries are still O(n) scans; the cache mitigates repeated reads).
- Cross-merged-set tombstone GC / peer-log compaction (compaction is per-device).
- Cross-device serializable transactions (use the chain); `run_transaction` is optimistic/single-device.
