-- complain if script is sourced in psql, rather than via CREATE EXTENSION
\echo Use "CREATE EXTENSION pg_columnar" to load this extension. \quit

CREATE FUNCTION columnar_handler(internal)
RETURNS table_am_handler
AS 'MODULE_PATHNAME'
LANGUAGE C STRICT;

CREATE ACCESS METHOD columnar TYPE TABLE HANDLER columnar_handler;

COMMENT ON ACCESS METHOD columnar IS 'columnar table access method using Arrow IPC storage';
