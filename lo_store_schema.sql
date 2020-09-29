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

DROP TABLE IF EXISTS lo_store_manual;

CREATE TABLE lo_store_manual (
	id uuid PRIMARY KEY,
	created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
	originating_db integer NOT NULL,
	loid lo
);

DROP TABLE IF EXISTS lo_store_multi;

CREATE TABLE lo_store_multi (
	id1 uuid NOT NULL,
	id2 uuid NOT NULL,
	id3 uuid NOT NULL,
	loid1 lo,
	-- Make one lo column NOT NULL just to make sure they're never accidentally set to NULL when not wanted
	loid2 lo NOT NULL,
	-- Move other columns here so the lo columns aren't the last in the table
	originating_db integer NOT NULL,
	loid3 lo,
	created_at timestamptz NOT NULL DEFAULT clock_timestamp(),
	PRIMARY KEY (id1, id2, id3)
);

COMMIT;
