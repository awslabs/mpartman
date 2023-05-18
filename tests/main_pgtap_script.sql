-- Turn off echo and keep things quiet.
\unset ECHO
\set QUIET 1

-- Turn off echo and keep things quiet.
\pset format unaligned
\pset tuples_only true
\pset pager off

SET client_min_messages = error;

-- Revert all changes on failure.
\set ON_ERROR_ROLLBACK 1
\set ON_ERROR_STOP true

-- Set schema name where MPARTMAN is installed
\set mpschema 'mpartman'

-- Set schema name for test tables
\set testschema 'testpartbypgtap'

-- Start the tests.
BEGIN;

-- Plan the tests.
SELECT plan(126);

-- Run the tests.
\ir test_list.sql

-- Finish the tests and clean up.
SELECT * FROM finish();
ROLLBACK;

