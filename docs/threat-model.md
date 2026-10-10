# Threat model

The API trusts only a scoped producer identity derived from a 256-bit random key. Hashes are indexed for lookup and fixed-time compared after retrieval. A pre-authentication IP budget bounds database lookups; a separate failed-auth budget and producer token buckets bound abuse per replica. Forwarded IPs are not trusted by default. A real deployment must configure trusted proxies deliberately.

Cross-producer reads are scoped to the authenticated producer. Mutating admin routes require `relay.admin`; authentication/authorization failures use problem details. Admin keys are powerful and must be separately provisioned, revoked, and kept out of browser persistence. The demo console keeps them only in input/tab memory; browser compromise remains outside that protection.

Request limits, strict source-generated deserialization, duplicate-member rejection, semantic validation, and parameterized SQL address oversized bodies, malformed contracts, and injection. Payloads and secrets are excluded from logs. Raw incident bytes are retained in the database for diagnosis and are not returned by the list endpoint; treat DB access as sensitive.

Broker RBAC and network access are part of the trust boundary. The consumer checks business identity and canonical hash against durable admission before applying. Malformed/unadmitted messages become transport incidents. The local emulator has development credentials and no cloud identity fidelity; it must stay bound to loopback.

Replay/discard serialize with handler admission. Discard is destructive to pending business processing, so the console confirms it and the backend audits the actor. No UI confirmation substitutes for authorization. CSP, same-origin requests, custom authentication headers, and `textContent` rendering constrain frontend injection/CSRF; there is no cookie-based session.

Production release requires cloud identity validation, TLS/private networking, runtime DB least privilege, secure telemetry transport, patched immutable images, dependency scans, secret rotation, and tested backup/restore. These are concrete environment gates, not claims established by the local demo.
