# Architecture decisions

| Decision | Choice and consequence |
|---|---|
| Transactional queue | Service Bus supports per-message settlement, retry, and DLQ. A partitioned log would require checkpoint and sink semantics beyond replacing a publisher. |
| PostgreSQL admission | Event identity, payload, and initial outbox commit together. Database fences and effects share a transaction; no distributed transaction or external-effect promise. |
| Two identities | Business identity dedupes effects; outbox identity dedupes transport retries and allows immediate intentional replay. |
| Dispatcher locking | SKIP LOCKED claims with bounded network I/O inside the transaction avoid lease ownership states. Separate pool and timeout bound contention. Measure before adopting leases. |
| Capture receipts | Permanent receipt uniqueness is independent of mutable incidents, preventing capture/replay/redelivery races. |
| Retained tombstones | Payloads can expire after terminal work; identity/hash/completion markers cannot. Metadata growth is an accepted cost until an explicit expiry/epoch protocol exists. |
| Immutable wire versions | Strict readers require consumer-first rollout of newly supported versions. Canonical hash is over original wire shape; upcasters do not change dedupe identity. |
| EF migrations + Npgsql | EF tracks schema version; parameterized SQL makes locks, conflict insertion, and atomic effects explicit. A second repository model would duplicate transaction semantics. |
| Supervised processor | Only a separate task invokes stop/start. Message callbacks signal pause without waiting for themselves. Prefetch starts at zero to simplify lock budgets. |
| Retry boundaries | Three fresh transaction attempts with jitter within a total ten-second callback budget. Lightweight bounded loop rather than a generalized policy framework; SDK publish retries fit a separate total deadline. |
| Fault-only artifacts | Failpoint bodies compile only in `FaultTests`. Process kills validate actual commit gaps, not exception-only simulations. |
| Narrow frontend | Static same-origin console demonstrates admission/conflicts/recovery without a second build system. CSP and text rendering reduce the frontend attack surface. |
| Scope | Ordering, extra adapters, AI triage, Terraform deployment, and distributed quota coordination are deferred. Reliability evidence is the core deliverable. |
