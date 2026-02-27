-- ============================================================
-- Qail Integration Test Database Bootstrap
-- ============================================================
-- Run once:  psql -h localhost -U orion -f pg/tests/test_setup.sql
--
-- Creates:
--   • Database: qail_test
--   • Roles:    qail (password), qail_user (superuser), qail_app (restricted)
--   • Tables:   operators, users, vessels, orders, agents, destinations, odysseys
--   • RLS:      Multi-tenant filtering on operator_id
--   • Seed:     2 operators with distinct data counts
-- ============================================================

-- Phase 1: Roles (must be created in any database)
DO $$
BEGIN
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'qail') THEN
    CREATE ROLE qail LOGIN PASSWORD 'qail';
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'qail_user') THEN
    CREATE ROLE qail_user LOGIN SUPERUSER;
  END IF;
  IF NOT EXISTS (SELECT FROM pg_roles WHERE rolname = 'qail_app') THEN
    CREATE ROLE qail_app LOGIN;
  END IF;
END $$;

-- Phase 2: Database
SELECT pg_terminate_backend(pid)
  FROM pg_stat_activity
 WHERE datname = 'qail_test' AND pid <> pg_backend_pid();

DROP DATABASE IF EXISTS qail_test;
CREATE DATABASE qail_test OWNER orion;

-- Reconnect to the new database
\c qail_test

-- Phase 3: Tables
CREATE TABLE operators (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL,
  slug VARCHAR NOT NULL UNIQUE,
  is_active BOOLEAN NOT NULL DEFAULT true,
  operator_id UUID, -- self-reference for RLS consistency
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE users (
  id SERIAL PRIMARY KEY,
  name VARCHAR NOT NULL,
  email VARCHAR NOT NULL,
  active BOOLEAN NOT NULL DEFAULT true,
  operator_id UUID REFERENCES operators(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE vessels (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL,
  slug VARCHAR NOT NULL,
  operator_id UUID NOT NULL REFERENCES operators(id),
  seat_capacity INT NOT NULL DEFAULT 50,
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE agents (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL,
  slug VARCHAR NOT NULL UNIQUE,
  operator_id UUID REFERENCES operators(id),
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE destinations (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL,
  slug VARCHAR NOT NULL UNIQUE,
  operator_id UUID REFERENCES operators(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE orders (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  status VARCHAR NOT NULL DEFAULT 'Draft',
  total_amount BIGINT NOT NULL DEFAULT 0,
  operator_id UUID NOT NULL REFERENCES operators(id),
  agent_id UUID REFERENCES agents(id),
  created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE odysseys (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  name VARCHAR NOT NULL,
  operator_id UUID NOT NULL REFERENCES operators(id),
  is_active BOOLEAN NOT NULL DEFAULT true,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Phase 4: Seed Data
-- Operator A: 00000000-0000-0000-0000-000000000001
-- Operator B: 00000000-0000-0000-0000-000000000002
INSERT INTO operators (id, name, slug, operator_id) VALUES
  ('00000000-0000-0000-0000-000000000001', 'Operator Alpha', 'operator-alpha', '00000000-0000-0000-0000-000000000001'),
  ('00000000-0000-0000-0000-000000000002', 'Operator Beta',  'operator-beta',  '00000000-0000-0000-0000-000000000002');

-- Users (for basic integration tests: expects at least 2)
INSERT INTO users (name, email, active, operator_id) VALUES
  ('Alice',   'alice@test.com',   true,  '00000000-0000-0000-0000-000000000001'),
  ('Bob',     'bob@test.com',     true,  '00000000-0000-0000-0000-000000000001'),
  ('Charlie', 'charlie@test.com', false, '00000000-0000-0000-0000-000000000002'),
  ('Diana',   'diana@test.com',   true,  '00000000-0000-0000-0000-000000000002');

-- Operator A: 11 vessels (RLS tests expect exactly 11 for Operator A)
INSERT INTO vessels (name, slug, operator_id) VALUES
  ('Alpha Vessel 1',  'av1',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 2',  'av2',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 3',  'av3',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 4',  'av4',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 5',  'av5',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 6',  'av6',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 7',  'av7',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 8',  'av8',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 9',  'av9',  '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 10', 'av10', '00000000-0000-0000-0000-000000000001'),
  ('Alpha Vessel 11', 'av11', '00000000-0000-0000-0000-000000000001');

-- Operator B: 5 vessels (different count for isolation test: a_count != b_count)
INSERT INTO vessels (name, slug, operator_id) VALUES
  ('Beta Vessel 1', 'bv1', '00000000-0000-0000-0000-000000000002'),
  ('Beta Vessel 2', 'bv2', '00000000-0000-0000-0000-000000000002'),
  ('Beta Vessel 3', 'bv3', '00000000-0000-0000-0000-000000000002'),
  ('Beta Vessel 4', 'bv4', '00000000-0000-0000-0000-000000000002'),
  ('Beta Vessel 5', 'bv5', '00000000-0000-0000-0000-000000000002');

-- Agents
INSERT INTO agents (name, slug, operator_id) VALUES
  ('Agent Smith', 'agent-smith', '00000000-0000-0000-0000-000000000001'),
  ('Agent Jones', 'agent-jones', '00000000-0000-0000-0000-000000000002');

-- Destinations
INSERT INTO destinations (name, slug, operator_id) VALUES
  ('Nusa Penida', 'nusa-penida', '00000000-0000-0000-0000-000000000001'),
  ('Gili Islands', 'gili-islands', '00000000-0000-0000-0000-000000000002');

-- Orders (for EXPLAIN join tests)
INSERT INTO orders (status, total_amount, operator_id, agent_id) VALUES
  ('Confirmed', 500000, '00000000-0000-0000-0000-000000000001', (SELECT id FROM agents WHERE slug = 'agent-smith')),
  ('Draft',     250000, '00000000-0000-0000-0000-000000000001', (SELECT id FROM agents WHERE slug = 'agent-smith')),
  ('Confirmed', 750000, '00000000-0000-0000-0000-000000000002', (SELECT id FROM agents WHERE slug = 'agent-jones'));

-- Odysseys (for RLS multi-table tests)
INSERT INTO odysseys (name, operator_id) VALUES
  ('Morning Express',  '00000000-0000-0000-0000-000000000001'),
  ('Sunset Cruise',    '00000000-0000-0000-0000-000000000001'),
  ('Evening Crossing', '00000000-0000-0000-0000-000000000002');

-- Phase 5: RLS Policies
ALTER TABLE vessels ENABLE ROW LEVEL SECURITY;
ALTER TABLE vessels FORCE ROW LEVEL SECURITY;

CREATE POLICY vessels_operator_isolation ON vessels
  FOR ALL
  USING (
    current_setting('app.is_super_admin', true) = 'true'
    OR operator_id::text = current_setting('app.current_operator_id', true)
  );

ALTER TABLE orders ENABLE ROW LEVEL SECURITY;
ALTER TABLE orders FORCE ROW LEVEL SECURITY;

CREATE POLICY orders_operator_isolation ON orders
  FOR ALL
  USING (
    current_setting('app.is_super_admin', true) = 'true'
    OR operator_id::text = current_setting('app.current_operator_id', true)
  );

ALTER TABLE odysseys ENABLE ROW LEVEL SECURITY;
ALTER TABLE odysseys FORCE ROW LEVEL SECURITY;

CREATE POLICY odysseys_operator_isolation ON odysseys
  FOR ALL
  USING (
    current_setting('app.is_super_admin', true) = 'true'
    OR operator_id::text = current_setting('app.current_operator_id', true)
  );

-- Phase 6: Grants
-- qail_user is superuser — bypasses RLS, used for EXPLAIN/red-team tests
-- qail_app is restricted — subject to RLS, used for isolation tests
GRANT CONNECT ON DATABASE qail_test TO qail, qail_user, qail_app;
GRANT USAGE ON SCHEMA public TO qail, qail_user, qail_app;
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA public TO qail, qail_user, qail_app;
GRANT USAGE, SELECT ON ALL SEQUENCES IN SCHEMA public TO qail, qail_user, qail_app;

-- Allow qail_app to create schemas (for RPC tests that create qail_test schema)
GRANT CREATE ON DATABASE qail_test TO qail_app, qail, qail_user;

\echo '✅ qail_test database ready for integration tests'
