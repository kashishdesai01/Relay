# Relay operations

## Aging dispatch backlog

Check API readiness, database connections, `outbox` pending count/oldest age, broker availability, and worker dispatcher logs by trace/category. A growing outbox indicates admission exceeds successful publishing. Check `next_attempt_at` and `last_error`; persisted retry schedules coordinate dispatcher replicas. Restore the dependency, watch backlog drain, then inspect consumer end-to-end lag. Do not delete pending outbox rows or payloads to clear an alert.

503 with `Retry-After` is server shedding; 429 is a per-replica producer/authentication budget. Clients should retry the same identity and body with bounded jitter. The API snapshot can lag one second and fails closed after ten seconds stale. Per-producer limits are not distributed quotas.

## Consumer retry ratio / database pause

Check PostgreSQL health/pool pressure, worker readiness, `relay_breaker_state`, and sanitized exception categories. The supervisor stops receiving while DB probes fail and resumes after recovery. Already-locked messages can expire and reach DLQ; investigate them after the database is stable. Do not restart all replicas repeatedly: doing so can burn delivery counts.

## Quarantined dispatch / consumer incidents

Use an operator key to list `/v1/admin/incidents`. Inspect source/reason and own event status. Correct the contract/configuration/handler problem before replay. Replay uses the retained canonical admission payload, not an edited broker body. A foreign transport incident cannot be replayed automatically: investigate its provenance and explicitly discard after diagnosis.

Replay creates one new transport identity. Processed work is a no-op; already-resolved incidents return 409. Discard is terminal for that business identity and prevents queued duplicates from applying. The action and actor are durable. Operators should record external incident notes separately; this version stores actor/action/time, not free-form potentially sensitive notes.

## Graceful shutdown

Send SIGTERM and allow 65 seconds at the container boundary. The host has a 60-second budget; consumer stop waits for callbacks under a 45-second shutdown token. Dispatch sends are bounded to ten seconds. SIGKILL recovery relies on DB transaction rollback and broker redelivery. Confirm committed effects and remaining pending/open incidents after recovery.

## Credentials and recovery

Provision keys once through `Relay.Migrator provision <producer> <comma-separated-scopes>` using a migration credential. Normal APIs cannot create admin users. Revoke with a parameterized operator database update setting `producers.disabled=true`; rotation currently requires provisioning a new producer identity and coordinating the sender. Key rotation within one producer is deferred and must not be claimed.

Cloud configuration uses `Relay__ServiceBusNamespace` with `DefaultAzureCredential` when no connection string is supplied. Assign only the sender/receiver roles required by each separately deployed worker role. Use separate runtime/migration database credentials, TLS, private networking, and an authenticated telemetry collector in production. Place the API behind a trusted TLS gateway; do not expose development HTTP ports publicly.

Back up PostgreSQL and verify restore to an isolated environment, including identities, payloads, outbox, fences, receipts, and incidents. Coordinate broker source identity with restoration; do not assume restoring a DB snapshot and recreating a queue preserves receipt uniqueness. Never prune retained fences manually as a space-saving incident response.
