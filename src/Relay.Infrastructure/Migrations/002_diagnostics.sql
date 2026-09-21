ALTER TABLE events ADD COLUMN trace_parent text;
ALTER TABLE incidents ADD COLUMN metadata_json text NOT NULL DEFAULT '{}';
ALTER TABLE incidents ADD COLUMN description text;
