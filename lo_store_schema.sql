\set ON_ERROR_STOP

BEGIN;

CREATE EXTENSION IF NOT EXISTS lo;

DROP TABLE IF EXISTS lo_store;

CREATE TABLE lo_store (
	id uuid PRIMARY KEY,
	created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
	originating_db integer NOT NULL,
	loid lo
);

COMMIT;
