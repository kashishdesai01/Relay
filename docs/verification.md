# Verification record

Verification is local evidence from October 13, 2026. The final gate writes `artifacts/verification-results.json`, including xUnit counters, gate names, UTC time, elapsed time, and a SHA-256 fingerprint of implementation/test/deployment sources. Logs are under `.review-tools/`; ignored artifacts can be regenerated.

## Reproduce

Start the local Docker infrastructure and observability stack with `python3 scripts/demo.py up`, then run:

```sh
python3 scripts/verify.py
```

The gate creates disposable databases and uses the dedicated test queues. Run it sequentially, without another broker test or recovery demo. The test runtime needs local sockets and child-process execution; a sandbox blocking these is an environment failure rather than a test assertion.

## Final result

The final source gate passed at 2026-10-14T03:46:24Z in 265.8 seconds. All 18 gates passed. Source fingerprint: `302311594eaa93983b3e4052851e93811bf06f3c15891957860155cea802dfb3`.

| Check | Result |
|---|---|
| .NET xUnit | 40 passed, 0 failed, 0 skipped |
| Python credential/evidence regressions | 6 passed |
| Actual-process fault checks | 13 passed |
| Recovery rehearsal | 8 checks passed |
| Seeded chaos | 200 accepted, 200 exact committed effects, 0 unresolved |
| Published Release container checks | 19 passed |
| Builds | Release and FaultTests; 0 warnings/errors |
| Dependency audit | No known vulnerable direct/transitive packages reported |
| Prometheus rules | 4 rules validated |

### Measured load

The final run admitted all 3,000 unique requests with HTTP 202 over 60.02 seconds (49.98/sec), using two worker processes. Admission p50/p95/p99: 7.6/169.9/291.5 ms. Time to committed effects p50/p95/p99: 0.66/15.28/16.18 seconds. Additional post-admission drain: 11.40 seconds. Count/sum/min/max reconciled exactly.

With workers stopped, 10 pending events triggered 503 and Retry-After. After resuming workers, that backlog drained in 1.35 seconds. The broker remained running; this is a worker outage scenario.

Machine-readable reports: `artifacts/verification-results.json`, `fault-results.json`, `chaos-results.json`, `recovery-demo-results.json`, `sustained-load-results.json`, `demo-results.json`, `trace-results.json`, and `test-results/final.trx`.

## Coverage

- Locked dependency restore, formatting, Release and FaultTests builds with warnings as errors, and a scan that Release artifacts exclude test-only fault controls.
- Contract validation/versioning, canonical hash conflicts, authentication endpoint selection, database concurrency, deduplication, aggregate atomicity, retention, capture/replay/discard transactions, trace context, and worker role configuration.
- Actual Service Bus emulator operations: native AMQP readiness, combined sender/receiver roles, duplicate detection, fresh-ID intentional replay, abandon/lock expiry, DLQ metadata, lock renewal, TTL expiry, byte-aware batches, and processor stop/start.
- Actual child-process faults before/after admission commit, dispatcher publish, consumer commit, and replay commit. Capture commit plus immediate replay and original DLQ redelivery; active-handler SIGTERM; database outage at worker startup plus recovery; public/unmatched HTTP paths during DB outage; log key redaction. A deliberate dedupe mutation must fail the exact-effect oracle.
- Seeded chaos with duplicates and repeated consumer kills: every accepted business identity reconciles to an exact committed effect or active recoverable incident; aggregate count/sum/min/max are checked.
- Isolated recovery rehearsal: real test-only handler failure reaches broker DLQ, capture diagnostics are authenticated, replay commits one effect, repeated actions conflict, discard commits no effect, actor audits persist.
- Sustained Release traffic and a dispatcher/consumer pause: exact aggregates, 503 with Retry-After at the configured pending threshold, and resume/drain.
- Rebuilt non-root Release containers: all supported event contracts, console/CSP, scope enforcement, retry/conflict behavior, admin endpoints, fresh ingest-to-dispatch-to-consumer trace parent relationships, current-run Azure SDK spans, live OTLP metrics and Prometheus scrape. Empty initial scrape results and historical-span rejection have Python regressions.
- Repeat provisioning/startup preserves credentials. Credential publication is private and atomic; interrupted writes and malformed existing files are regression-tested.
- Fresh transitive dependency vulnerability audit, Compose validation, and Prometheus alert-rule syntax.

## Test environment

The load harness runs native Release API/workers on an arm64 macOS host against PostgreSQL 17.11 and Service Bus emulator 2.0.1 in a dedicated 4-CPU/6-GiB Colima VM. SQL Server runs amd64 under Rosetta, outside Microsoft's supported SQL Server platforms. HTTP requests open new connections; the load is finite and is not a saturation sweep or a stable latency SLO. Results vary with host/VM contention. Per-producer limits are raised for this load scenario; the pause scenario separately lowers the pending threshold to prove shedding.

This record covers local PostgreSQL and emulator execution. GitHub Actions provides its own run logs and uploaded evidence. Database effect guarantees are scoped to the retained identity fences and transaction; cloud environment validation is a separate deployment step.

See [the recovery walkthrough](recovery.md) for an interactive DLQ and replay scenario.

## Console verification

After the final gate, native browser interactions verified Release admission → processed, unchanged retry → 202, changed content → 409, and authenticated incident listing. A separate temporary recovery session verified Inspect → replay → event processed through the visible controls. No browser runtime errors were recorded. Screenshots are `artifacts/console-verified.jpg` and `artifacts/recovery-console-verified.jpg`. The temporary recovery session was stopped and cleaned up; the normal Release demo remains available at http://127.0.0.1:5080.
