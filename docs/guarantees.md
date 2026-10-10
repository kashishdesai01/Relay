# Guarantees and lifecycle

Business identity is `(producer_id,event_id)`. A SHA-256 hash of the validated, original wire version catches identity reuse with different content. JSON whitespace/property order and equivalent typed double encodings normalize; event timestamps normalize to UTC. Duplicate JSON members are rejected. Canonicalization has a golden fixture; changing it is a contract migration.

Transport identity is `relay:<outbox_id>`. It is stable for retries of a dispatch row, different for intentional replay, and independent of business ID length. Broker duplicate detection is an optimization. The unique `processed_events` key and every handler write commit together. Unknown commit outcomes may be retried safely against this fence. No handler performs external I/O.

Capture receipts are permanently unique by queue and transport ID. Incident resolution does not delete them. A crash after capture commit followed by operator replay cannot turn the original DLQ redelivery into a new incident. Foreign messages use broker sequence as a fallback physical receipt; queue/namespace recreation with the same database requires a fresh source identity. Do not reuse broker namespaces blindly after restore or recreation.

Replay locks the incident and event, checks completion/discard state, schedules a new outbox row, and records the actor in one transaction. Replaying processed work is an audited no-op. Discard locks the event and permanently suppresses subsequent handler admission, including copies already queued. A committed process transaction wins over a later discard. Replaying a previously discarded identity is forbidden; a deliberate new business operation requires a new event ID.

Thirty-day payload cleanup only affects processed/discarded events without pending/quarantined dispatch or open incidents. Identity/hash/processed fences, receipts, effects, and incident history remain. Metadata is intentionally not bounded by automatic deletion in this version; plan storage for it. This version prioritizes correctness over a speculative tombstone expiry protocol.

The guarantee assumes PostgreSQL durable commit settings, durable storage, and consistent database restoration. A broker send acknowledgement is not proof of processing. Expired cloud messages must dead-letter rather than disappear. Emulator data is lost on restart; emulator restart is not a test of production broker durability. A database outage can still exhaust a message's broker delivery count; the resulting DLQ remains recoverable.

The dispatcher holds a database transaction across a bounded broker operation. Its own connection pool, total send deadline, small claims, and idle transaction timeout limit the tradeoff. A partial/uncertain send leaves all affected rows retryable; duplicate physical messages are safe. Permanent serialization/size failures are database incidents with operator actions.

Current status uses committed processing/discard facts, open incidents, and active outbox attempts. Historical replay is not a terminal success. An unresolved incident can coexist with already-committed effects; status reports `processed`, while incident history remains inspectable.

Metric buckets use UTC `occurredAt`, rounded down to the minute; late events update their event-time bucket. Unit is part of the rollup key. V1 unit is `unspecified`, never assumed milliseconds. Metric values are finite and bounded to ±1e12. This is a simple aggregation example, not a general metrics store.
