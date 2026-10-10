# Recovery walkthrough

Relay exposes durable delivery incidents through the operations console and scoped admin endpoints. This walkthrough creates an isolated local database, causes a real broker dead-letter transition, then restores a healthy consumer so the retained event can be replayed.

## Start a session

Start the normal local infrastructure first. Build the separate fault-test configuration and launch the recovery session:

```sh
bash scripts/dotnet.sh build Relay.sln -c FaultTests --no-restore
python3 scripts/demo-recovery.py
```

Open the printed console URL. Paste the producer and operator keys from the private `artifacts/local-secrets-recovery.json` file. This session has its own temporary database and uses the test queue. Run it separately from broker tests. Ctrl+C stops its processes and cleans up its database.

## Inspect and replay

1. Select **Refresh incidents** and inspect `recovery-demo`.
2. Review its source, reason, transport ID, delivery count, and trace context.
3. Select **replay**. The healthy consumer applies the retained event.
4. Look up `recovery-demo`; its event state becomes `processed`.

The incident records the `replay_pending` scheduling decision. Event status is the authoritative processing outcome. Replay creates a new transport ID while preserving the original business identity; the permanent capture receipt prevents the original DLQ redelivery from creating another incident.

The synthetic failure hook is compiled only into FaultTests artifacts. The database, broker, DLQ capture, and admin recovery operations use the normal implementations.

## Automated rehearsal

```sh
python3 scripts/demo-recovery.py --verify
```

This also verifies repeat-action conflicts, audited discard, no effects for discarded work, and durable actor records. The session cleans up after verification.
