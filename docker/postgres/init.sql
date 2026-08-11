-- =============================================================================
-- PostgreSQL Initialization Script
-- Runs once when the postgres container is first created.
-- =============================================================================

-- Ensure our database exists (it's already created by POSTGRES_DB env var,
-- but this is a good place to add extensions we need)
\connect email_validation;

-- Enable UUID extension for generating UUIDs in PostgreSQL
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- Useful for generating UUIDs natively in queries if needed
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
