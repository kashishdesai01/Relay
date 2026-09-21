CREATE TABLE producers (
 id text PRIMARY KEY, name text NOT NULL, key_hash bytea NOT NULL UNIQUE,
 scopes text[] NOT NULL, disabled boolean NOT NULL DEFAULT false,
 created_at timestamptz NOT NULL DEFAULT now(),
 CHECK (octet_length(key_hash)=32), CHECK (id ~ '^[A-Za-z0-9._-]{1,128}$')
);
CREATE TABLE events (
 producer_id text NOT NULL REFERENCES producers(id), event_id text NOT NULL,
 type text NOT NULL, schema_version int NOT NULL, payload_hash bytea NOT NULL,
 received_at timestamptz NOT NULL DEFAULT now(), processed_at timestamptz, discarded_at timestamptz,
 PRIMARY KEY(producer_id,event_id), CHECK(octet_length(payload_hash)=32)
);
CREATE TABLE event_payloads (
 producer_id text NOT NULL, event_id text NOT NULL, body text NOT NULL,
 PRIMARY KEY(producer_id,event_id), FOREIGN KEY(producer_id,event_id) REFERENCES events
);
CREATE TABLE outbox (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 producer_id text NOT NULL, event_id text NOT NULL,
 status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','sent','quarantined','discarded')),
 attempts int NOT NULL DEFAULT 0, next_attempt_at timestamptz NOT NULL DEFAULT now(),
 trace_parent text, replay_of bigint, created_at timestamptz NOT NULL DEFAULT now(), sent_at timestamptz,
 last_error text, FOREIGN KEY(producer_id,event_id) REFERENCES events
);
CREATE INDEX outbox_pending ON outbox(next_attempt_at,id) WHERE status='pending';
CREATE INDEX outbox_event ON outbox(producer_id,event_id);
CREATE TABLE processed_events (
 producer_id text NOT NULL, event_id text NOT NULL, processed_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY(producer_id,event_id), FOREIGN KEY(producer_id,event_id) REFERENCES events
);
CREATE TABLE event_records (
 producer_id text NOT NULL, event_id text NOT NULL, type text NOT NULL,
 created_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(producer_id,event_id),
 FOREIGN KEY(producer_id,event_id) REFERENCES events
);
CREATE TABLE rollups_minute (
 producer_id text NOT NULL REFERENCES producers(id), metric_name text NOT NULL, unit text NOT NULL,
 bucket_start timestamptz NOT NULL, count bigint NOT NULL, sum double precision NOT NULL,
 min double precision NOT NULL, max double precision NOT NULL,
 PRIMARY KEY(producer_id,metric_name,unit,bucket_start)
);
CREATE TABLE capture_receipts (
 source_queue text NOT NULL, receipt_id text NOT NULL,
 captured_at timestamptz NOT NULL DEFAULT now(), PRIMARY KEY(source_queue,receipt_id)
);
CREATE TABLE incidents (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
 producer_id text, event_id text, source text NOT NULL CHECK(source IN ('dispatch','consumer','transport')),
 receipt_id text NOT NULL, reason text NOT NULL,
 state text NOT NULL DEFAULT 'captured' CHECK(state IN ('captured','replay_pending','discarded','processed')),
 raw_body bytea NOT NULL, captured_at timestamptz NOT NULL DEFAULT now(),
 FOREIGN KEY(producer_id,event_id) REFERENCES events,
 CHECK ((producer_id IS NULL) = (event_id IS NULL))
);
CREATE INDEX incidents_open ON incidents(id DESC) WHERE state='captured';
CREATE INDEX incidents_event ON incidents(producer_id,event_id);
ALTER TABLE outbox ADD CONSTRAINT outbox_replay FOREIGN KEY(replay_of) REFERENCES incidents(id);
CREATE TABLE incident_actions (
 id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY, incident_id bigint NOT NULL REFERENCES incidents(id),
 action text NOT NULL, actor text NOT NULL REFERENCES producers(id), at timestamptz NOT NULL DEFAULT now()
);
