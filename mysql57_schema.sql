-- ============================================
-- LangGraph Checkpoint MySQL 5.7 兼容版本
-- ============================================
--
-- 这个脚本创建兼容 MySQL 5.7 的表结构
-- 主要修改：
--   1. 移除 JSON 字段的 DEFAULT 表达式
--   2. 使用普通列代替生成列
--   3. 直接创建最终结构（包含所有迁移）
--
-- 使用方法：
--   mysql -u user -p database < mysql57_schema.sql
-- ============================================

-- 1. 创建迁移版本表
CREATE TABLE IF NOT EXISTS checkpoint_migrations (
    v INTEGER PRIMARY KEY
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;

-- 2. 创建主检查点表
CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id VARCHAR(150) NOT NULL,
    checkpoint_ns VARCHAR(2000) NOT NULL DEFAULT '',
    checkpoint_ns_hash BINARY(16) NOT NULL COMMENT 'MD5 hash of checkpoint_ns',
    checkpoint_id VARCHAR(150) NOT NULL,
    parent_checkpoint_id VARCHAR(150),
    type VARCHAR(150),
    checkpoint JSON NOT NULL COMMENT 'Complete checkpoint data',
    metadata JSON NOT NULL COMMENT 'Checkpoint metadata (no default in MySQL 5.7)',
    PRIMARY KEY (thread_id, checkpoint_ns_hash, checkpoint_id),
    INDEX checkpoints_thread_id_idx (thread_id),
    INDEX checkpoints_checkpoint_id_idx (checkpoint_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Main checkpoint table - stores graph state snapshots';

-- 3. 创建二进制对象表
CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id VARCHAR(150) NOT NULL,
    checkpoint_ns VARCHAR(2000) NOT NULL DEFAULT '',
    checkpoint_ns_hash BINARY(16) NOT NULL COMMENT 'MD5 hash of checkpoint_ns',
    channel VARCHAR(150) NOT NULL COMMENT 'Channel name',
    version VARCHAR(150) NOT NULL COMMENT 'Channel version',
    type VARCHAR(150) NOT NULL COMMENT 'Serialization type',
    `blob` LONGBLOB COMMENT 'Serialized channel value',
    PRIMARY KEY (thread_id, checkpoint_ns_hash, channel, version),
    INDEX checkpoint_blobs_thread_id_idx (thread_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Stores large/complex channel values separately from checkpoints';

-- 4. 创建检查点写入表
CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id VARCHAR(150) NOT NULL,
    checkpoint_ns VARCHAR(2000) NOT NULL DEFAULT '',
    checkpoint_ns_hash BINARY(16) NOT NULL COMMENT 'MD5 hash of checkpoint_ns',
    checkpoint_id VARCHAR(150) NOT NULL,
    task_id VARCHAR(150) NOT NULL COMMENT 'Task that created this write',
    task_path VARCHAR(2000) NOT NULL DEFAULT '' COMMENT 'Path to nested task',
    idx INTEGER NOT NULL COMMENT 'Write index for ordering',
    channel VARCHAR(150) NOT NULL COMMENT 'Target channel',
    type VARCHAR(150) COMMENT 'Serialization type',
    `blob` LONGBLOB NOT NULL COMMENT 'Serialized write value',
    PRIMARY KEY (thread_id, checkpoint_ns_hash, checkpoint_id, task_id, idx),
    INDEX checkpoint_writes_thread_id_idx (thread_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci
COMMENT='Stores pending writes to be applied to channels';

-- 5. 记录所有迁移版本
-- 告诉系统所有迁移（v0-v21）已完成
INSERT IGNORE INTO checkpoint_migrations (v) VALUES
(0),   -- Create migrations table
(1),   -- Create checkpoints table
(2),   -- Create checkpoint_blobs table
(3),   -- Create checkpoint_writes table
(4),   -- Modify blob column to LONGBLOB
(5),   -- Create index on checkpoints.thread_id
(6),   -- Create index on checkpoint_blobs.thread_id
(7),   -- Create index on checkpoint_writes.thread_id
(8),   -- Create index on checkpoints.checkpoint_id
(9),   -- Extend checkpoint_ns to VARCHAR(255)
(10),  -- Extend checkpoint_blobs.checkpoint_ns to VARCHAR(255)
(11),  -- Extend checkpoint_writes.checkpoint_ns to VARCHAR(255)
(12),  -- Extend checkpoint_ns to VARCHAR(2000), modify PK
(13),  -- Extend checkpoint_blobs.checkpoint_ns to VARCHAR(2000), modify PK
(14),  -- Extend checkpoint_writes.checkpoint_ns to VARCHAR(2000), modify PK
(15),  -- Add checkpoint_ns_hash to checkpoints, modify PK
(16),  -- Add checkpoint_ns_hash to checkpoint_blobs, modify PK
(17),  -- Add checkpoint_ns_hash to checkpoint_writes, modify PK
(18),  -- Add task_path to checkpoint_writes
(19),  -- Convert generated column to regular column (checkpoints)
(20),  -- Convert generated column to regular column (checkpoint_blobs)
(21);  -- Convert generated column to regular column (checkpoint_writes)

-- 6. 验证安装
SELECT
    'Tables created:' as info,
    COUNT(*) as count
FROM information_schema.tables
WHERE table_schema = DATABASE()
    AND table_name LIKE 'checkpoint%';

SELECT
    'Migration versions:' as info,
    COUNT(*) as count
FROM checkpoint_migrations;

-- 7. 显示表结构（可选，用于调试）
-- SHOW CREATE TABLE checkpoints\G
-- SHOW CREATE TABLE checkpoint_blobs\G
-- SHOW CREATE TABLE checkpoint_writes\G

-- ============================================
-- 安装完成
-- ============================================
--
-- 下一步：
--   1. 在 Python 中导入：from mysql57_compat_saver import MySQL57Saver
--   2. 创建连接：MySQL57Saver.from_conn_string(DB_URI)
--   3. 不要调用 setup() 方法
--   4. 直接使用 checkpointer
--
-- 注意事项：
--   - checkpoint_ns_hash 由代码自动计算（UNHEX(MD5(checkpoint_ns))）
--   - metadata 字段必须有值（代码已处理）
--   - 不支持自动迁移，需要手动更新表结构
--
-- ============================================
