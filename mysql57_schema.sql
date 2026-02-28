-- =============================================================================
-- MySQL 5.7 / PolarDB-X compatible schema for LangGraph checkpoint savers
-- =============================================================================
--
-- Apply this file once to initialise the database:
--
--   mysql -u <user> -p <database> < mysql57_schema.sql
--
-- Or from a MySQL client session:
--
--   SOURCE /path/to/mysql57_schema.sql;
--
-- After applying, call  cp.setup()  (or  await cp.setup() ) in Python to
-- run the migration bookkeeping — setup() is idempotent and will not re-run
-- migrations that are already recorded in checkpoint_migrations.
--
-- Compatibility
-- -------------
-- ✓ MySQL 5.7+
-- ✓ MariaDB 10.2+
-- ✓ PolarDB-X (Alibaba Cloud)
-- ✓ Alibaba Cloud RDS MySQL 5.7
--
-- Key design choices (vs official langgraph-checkpoint-mysql)
-- -----------------------------------------------------------
-- • LONGTEXT instead of JSON
--     MySQL 5.7 JSON columns reject DEFAULT expressions such as DEFAULT ('{}').
--     LONGTEXT accepts any text; JSON_CONTAINS() and other JSON functions work
--     on LONGTEXT at query time.
-- • Explicit checkpoint_ns_hash BINARY(16) column (not a generated column)
--     MySQL 5.7 restricts STORED generated columns in composite primary keys.
--     We compute  UNHEX(MD5(checkpoint_ns))  explicitly in every DML statement.
-- • No JSON_TABLE, JSON_ARRAYAGG, or WITH CTE in queries
--     These features require MySQL 8.0.  We load blobs and writes in separate
--     Python-side queries and assemble them in memory instead.
-- • ON DUPLICATE KEY UPDATE col = VALUES(col)  (not  AS new  syntax)
--     The  INSERT ... AS alias  syntax was introduced in MySQL 8.0.19.
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 0. Migration bookkeeping
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checkpoint_migrations (
    v INTEGER PRIMARY KEY
);

-- -----------------------------------------------------------------------------
-- 1. Main checkpoint table
--
-- One row per (thread_id, checkpoint_ns, checkpoint_id) triple.
-- The  checkpoint  column stores the serialised graph state including
-- inline primitive channel values and  channel_versions  (version map).
-- Complex channel values (messages, tool results, …) live in checkpoint_blobs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id              VARCHAR(150)  NOT NULL,
    checkpoint_ns          VARCHAR(2000) NOT NULL DEFAULT '',
    checkpoint_ns_hash     BINARY(16)    NOT NULL        -- UNHEX(MD5(checkpoint_ns))
                                                         -- computed at DML time
                           COMMENT 'MD5 hash of checkpoint_ns for PK sizing',
    checkpoint_id          VARCHAR(150)  NOT NULL,
    parent_checkpoint_id   VARCHAR(150),
    checkpoint             LONGTEXT      NOT NULL,       -- JSON; LONGTEXT avoids
                                                         -- DEFAULT expression limit
    metadata               LONGTEXT      NOT NULL,

    PRIMARY KEY (thread_id, checkpoint_ns_hash, checkpoint_id)
);

CREATE INDEX idx_checkpoints_thread_id  ON checkpoints (thread_id);
CREATE INDEX idx_checkpoints_cp_id      ON checkpoints (checkpoint_id);

-- -----------------------------------------------------------------------------
-- 2. Blob storage for complex channel values
--
-- Blobs are content-addressed: a given (channel, version) tuple always maps
-- to the same bytes, so INSERT IGNORE is safe and avoids re-serialisation.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id          VARCHAR(150)  NOT NULL,
    checkpoint_ns      VARCHAR(2000) NOT NULL DEFAULT '',
    checkpoint_ns_hash BINARY(16)    NOT NULL,
    channel            VARCHAR(150)  NOT NULL,
    version            VARCHAR(150)  NOT NULL,
    type               VARCHAR(150)  NOT NULL,           -- serde type tag
    `blob`             LONGBLOB,                         -- NULL ↔ type='empty'

    PRIMARY KEY (thread_id, checkpoint_ns_hash, channel, version)
);

CREATE INDEX idx_blobs_thread_id ON checkpoint_blobs (thread_id);

-- -----------------------------------------------------------------------------
-- 3. Pending writes
--
-- Used for Human-in-the-Loop (interrupt / resume) and task retry scenarios.
-- Error / interrupt channels (negative idx) may be overwritten on retry;
-- normal channels use INSERT IGNORE so completed work survives re-runs.
-- -----------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id          VARCHAR(150)  NOT NULL,
    checkpoint_ns      VARCHAR(2000) NOT NULL DEFAULT '',
    checkpoint_ns_hash BINARY(16)    NOT NULL,
    checkpoint_id      VARCHAR(150)  NOT NULL,
    task_id            VARCHAR(150)  NOT NULL,
    task_path          VARCHAR(2000) NOT NULL DEFAULT '',
    idx                INTEGER       NOT NULL,            -- WRITES_IDX_MAP or
                                                          -- sequential task index
    channel            VARCHAR(150)  NOT NULL,
    type               VARCHAR(150),
    `blob`             LONGBLOB      NOT NULL,

    PRIMARY KEY (thread_id, checkpoint_ns_hash, checkpoint_id, task_id, idx)
);

CREATE INDEX idx_writes_thread_id ON checkpoint_writes (thread_id);

-- -----------------------------------------------------------------------------
-- 4. Seed the migration table so setup() knows the schema is current
--
-- The 8 migrations correspond to the MIGRATIONS list in base.py:
--   0 → checkpoint_migrations table
--   1 → checkpoints table
--   2 → checkpoint_blobs table
--   3 → checkpoint_writes table
--   4 → idx_checkpoints_thread_id
--   5 → idx_blobs_thread_id
--   6 → idx_writes_thread_id
--   7 → idx_checkpoints_cp_id
--
-- If you call  cp.setup()  in Python after applying this file, it will find
-- version 7 already recorded and skip all migrations (no-op).
-- -----------------------------------------------------------------------------
INSERT IGNORE INTO checkpoint_migrations (v) VALUES
    (0),
    (1),
    (2),
    (3),
    (4),
    (5),
    (6),
    (7);

-- =============================================================================
-- Verification queries (optional — run manually)
-- =============================================================================
-- SHOW TABLES LIKE 'checkpoint%';
-- SELECT * FROM checkpoint_migrations ORDER BY v;
-- DESCRIBE checkpoints;
-- DESCRIBE checkpoint_blobs;
-- DESCRIBE checkpoint_writes;
