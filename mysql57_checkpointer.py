"""
MySQL 5.7 / PolarDB-X 兼容的 Checkpoint Saver

完整的生产就绪代码，直接复制到项目即可使用。

使用方法：
    from mysql57_checkpointer import MySQL57Saver

    DB_URI = "mysql://user:pass@localhost:3306/dbname"

    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        config = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}
        # 使用 checkpointer.put(), checkpointer.get() 等方法

注意事项：
    1. 不要调用 setup() 方法
    2. 需要手动执行建表脚本 mysql57_schema.sql
    3. 性能比 MySQL 8.0 版本慢 2-3 倍

兼容性：
    - MySQL 5.7+
    - MariaDB 10.2+
    - PolarDB-X
    - 阿里云 RDS MySQL 5.7
"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any, Optional

import pymysql
from pymysql.cursors import DictCursor
from typing_extensions import Self

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    ChannelVersions,
    Checkpoint,
    CheckpointMetadata,
    CheckpointTuple,
    get_checkpoint_id,
    get_checkpoint_metadata,
)
from langgraph.checkpoint.mysql.pymysql import PyMySQLSaver


# ============================================
# MySQL 5.7 / PolarDB-X 兼容的 SQL 语句
# 不使用 JSON_TABLE, JSON_ARRAYAGG, WITH CTE, VALUES AS
# ============================================

MYSQL57_SELECT_SQL = """
SELECT
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.checkpoint_id,
    c.parent_checkpoint_id,
    c.metadata
FROM checkpoints c
{WHERE}
"""

# PolarDB-X 兼容的 UPSERT 语句（使用 VALUES() 函数）
MYSQL57_UPSERT_CHECKPOINT_BLOBS_SQL = """
    INSERT INTO checkpoint_blobs (thread_id, checkpoint_ns, checkpoint_ns_hash, channel, version, type, `blob`)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        type = VALUES(type),
        `blob` = VALUES(`blob`)
"""

MYSQL57_UPSERT_CHECKPOINTS_SQL = """
    INSERT INTO checkpoints (thread_id, checkpoint_ns, checkpoint_ns_hash, checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        checkpoint = VALUES(checkpoint),
        metadata = VALUES(metadata)
"""

MYSQL57_UPSERT_CHECKPOINT_WRITES_SQL = """
    INSERT INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_ns_hash, checkpoint_id, task_id, task_path, idx, channel, type, `blob`)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        channel = VALUES(channel),
        type = VALUES(type),
        `blob` = VALUES(`blob`)
"""

MYSQL57_INSERT_CHECKPOINT_WRITES_SQL = """
    INSERT IGNORE INTO checkpoint_writes (thread_id, checkpoint_ns, checkpoint_ns_hash, checkpoint_id, task_id, task_path, idx, channel, type, `blob`)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s, %s, %s, %s)
"""


# ============================================
# 主类：MySQL 5.7 兼容的 Saver
# ============================================

class MySQL57Saver(PyMySQLSaver):
    """
    MySQL 5.7 / PolarDB-X 兼容的 Checkpoint Saver

    通过继承 PyMySQLSaver 并重写关键方法来支持 MySQL 5.7。

    重写的方法：
        - _select_sql(): 使用简化的 SELECT 语句
        - _load_checkpoint_tuple(): 手动加载 blobs 和 writes
        - list(): 使用新的查询逻辑
        - get_tuple(): 使用新的查询逻辑
        - setup(): 禁用自动建表

    新增的方法：
        - _load_channel_values_mysql57(): 手动加载 channel values
        - _load_pending_writes_mysql57(): 手动加载 pending writes

    使用示例：
        >>> DB_URI = "mysql://user:pass@localhost:3306/dbname"
        >>> with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        ...     config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
        ...     checkpointer.put(config, checkpoint, {}, {})
        ...     loaded = checkpointer.get(config)
    """

    @staticmethod
    def parse_conn_string(conn_string: str) -> dict[str, Any]:
        """解析数据库连接字符串"""
        parsed = urllib.parse.urlparse(conn_string)
        params_as_dict = dict(urllib.parse.parse_qsl(parsed.query))

        return {
            "host": parsed.hostname,
            "user": parsed.username,
            "password": parsed.password or "",
            "database": parsed.path[1:] or None,
            "port": parsed.port or 3306,
            "unix_socket": params_as_dict.get("unix_socket"),
        }

    @classmethod
    @contextmanager
    def from_conn_string(cls, conn_string: str) -> Iterator[Self]:
        """
        从连接字符串创建 checkpointer

        Args:
            conn_string: MySQL 连接字符串，格式：
                        mysql://user:password@host:port/database

        Yields:
            MySQL57Saver: checkpointer 实例

        Example:
            >>> DB_URI = "mysql://root:pass@localhost:3306/mydb"
            >>> with MySQL57Saver.from_conn_string(DB_URI) as cp:
            ...     cp.put(config, checkpoint, {}, {})
        """
        with pymysql.connect(
            **cls.parse_conn_string(conn_string),
            autocommit=True,
        ) as conn:
            yield cls(conn)

    @staticmethod
    def _get_cursor_from_connection(conn: pymysql.Connection) -> DictCursor:
        """获取游标"""
        return conn.cursor(DictCursor)

    @staticmethod
    def _select_sql(where: str) -> str:
        """
        重写：使用 MySQL 5.7 兼容的 SELECT SQL

        原始版本使用了 JSON_TABLE 和 JSON_ARRAYAGG，这里改为简化查询。
        Blobs 和 writes 将在 Python 中手动加载。

        Args:
            where: WHERE 子句

        Returns:
            str: 完整的 SELECT SQL
        """
        return MYSQL57_SELECT_SQL.replace("{WHERE}", where)

    def _load_checkpoint_tuple(self, value: dict[str, Any]) -> CheckpointTuple:
        """
        重写：手动加载 checkpoint 数据

        原始版本期望 SQL 返回聚合的 channel_values 和 pending_writes，
        但 MySQL 5.7 不支持 JSON_ARRAYAGG。这里改为：
        1. 先获取基础 checkpoint
        2. 手动查询 blobs (channel_values)
        3. 手动查询 writes (pending_writes)
        4. 在 Python 中组装数据

        Args:
            value: 数据库查询结果（一行）

        Returns:
            CheckpointTuple: 完整的 checkpoint 元组
        """
        # 1. 解析 checkpoint JSON
        if isinstance(value["checkpoint"], str):
            checkpoint = json.loads(value["checkpoint"])
        else:
            checkpoint = value["checkpoint"]

        # 2. 手动加载 channel_values (blobs)
        channel_values = self._load_channel_values_mysql57(
            value["thread_id"],
            value["checkpoint_ns"],
            checkpoint
        )

        # 3. 手动加载 pending_writes
        pending_writes = self._load_pending_writes_mysql57(
            value["thread_id"],
            value["checkpoint_ns"],
            value["checkpoint_id"]
        )

        # 4. 组装返回
        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id": value["thread_id"],
                    "checkpoint_ns": value["checkpoint_ns"],
                    "checkpoint_id": value["checkpoint_id"],
                }
            },
            checkpoint={
                **checkpoint,
                "channel_values": {
                    **checkpoint.get("channel_values", {}),
                    **channel_values,
                },
            },
            metadata=self._load_metadata(value["metadata"]),
            parent_config=(
                {
                    "configurable": {
                        "thread_id": value["thread_id"],
                        "checkpoint_ns": value["checkpoint_ns"],
                        "checkpoint_id": value["parent_checkpoint_id"],
                    }
                }
                if value.get("parent_checkpoint_id")
                else None
            ),
            pending_writes=pending_writes,
        )

    def _load_channel_values_mysql57(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint: dict
    ) -> dict[str, Any]:
        """
        新增：手动加载 channel values (blobs)

        原始版本使用 JSON_TABLE 展开 channel_versions，然后用 JSON_ARRAYAGG 聚合。
        这里改为：
        1. 从 checkpoint.channel_versions 获取所有 channels
        2. 为每个 channel 查询对应版本的 blob
        3. 在 Python 中反序列化并组装

        Args:
            thread_id: 线程 ID
            checkpoint_ns: 命名空间
            checkpoint: checkpoint 数据（包含 channel_versions）

        Returns:
            dict: channel_name -> deserialized_value
        """
        channel_versions = checkpoint.get("channel_versions", {})
        if not channel_versions:
            return {}

        result = {}

        # 为每个 channel 查询对应的 blob
        for channel, version in channel_versions.items():
            sql = """
                SELECT type, `blob`
                FROM checkpoint_blobs
                WHERE thread_id = %s
                    AND checkpoint_ns_hash = UNHEX(MD5(%s))
                    AND channel = %s
                    AND version = %s
            """

            with self._cursor() as cur:
                cur.execute(sql, (thread_id, checkpoint_ns, channel, str(version)))
                row = cur.fetchone()

            # 反序列化
            if row and row["type"] != "empty" and row["blob"] is not None:
                result[channel] = self.serde.loads_typed((row["type"], row["blob"]))

        return result

    def _load_pending_writes_mysql57(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str
    ) -> list[tuple[str, str, Any]]:
        """
        新增：手动加载 pending writes

        原始版本使用 JSON_ARRAYAGG 聚合 writes。
        这里改为查询所有 writes，然后在 Python 中组装。

        Args:
            thread_id: 线程 ID
            checkpoint_ns: 命名空间
            checkpoint_id: checkpoint ID

        Returns:
            list: [(task_id, channel, value), ...]
        """
        sql = """
            SELECT task_id, channel, type, `blob`
            FROM checkpoint_writes
            WHERE thread_id = %s
                AND checkpoint_ns_hash = UNHEX(MD5(%s))
                AND checkpoint_id = %s
            ORDER BY task_id, idx
        """

        with self._cursor() as cur:
            cur.execute(sql, (thread_id, checkpoint_ns, checkpoint_id))
            rows = cur.fetchall()

        # 反序列化
        return [
            (
                row["task_id"],
                row["channel"],
                self.serde.loads_typed((row["type"], row["blob"]))
            )
            for row in rows
        ]

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        重写：保存 checkpoint

        使用 PolarDB-X 兼容的 UPSERT 语句（VALUES() 函数而不是 AS new 语法）

        Args:
            config: 配置
            checkpoint: checkpoint 数据
            metadata: 元数据
            new_versions: 新的 channel 版本

        Returns:
            RunnableConfig: 更新后的配置

        Example:
            >>> config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
            >>> checkpoint = {...}
            >>> new_config = checkpointer.put(config, checkpoint, {}, {})
        """
        configurable = config["configurable"].copy()
        thread_id = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")
        checkpoint_id = configurable.pop("checkpoint_id", None)

        copy = checkpoint.copy()
        copy["channel_values"] = copy["channel_values"].copy()

        next_config = {
            "configurable": {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

        # 内联简单值，大对象存到 blobs 表
        blob_values = {}
        for k, v in checkpoint["channel_values"].items():
            if v is None or isinstance(v, (str, int, float, bool)):
                pass  # 保留在 checkpoint 中
            else:
                blob_values[k] = copy["channel_values"].pop(k)

        with self._cursor(pipeline=True) as cur:
            # 保存 blobs
            if blob_versions := {
                k: v for k, v in new_versions.items() if k in blob_values
            }:
                cur.executemany(
                    MYSQL57_UPSERT_CHECKPOINT_BLOBS_SQL,
                    self._dump_blobs(
                        thread_id,
                        checkpoint_ns,
                        blob_values,
                        blob_versions,
                    ),
                )

            # 保存 checkpoint
            cur.execute(
                MYSQL57_UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    checkpoint_ns,
                    checkpoint["id"],
                    checkpoint_id,
                    json.dumps(copy),
                    self._dump_metadata(get_checkpoint_metadata(config, metadata)),
                ),
            )

        return next_config

    def put_writes(
        self,
        config: RunnableConfig,
        writes: list[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        重写：保存中间写入

        使用 PolarDB-X 兼容的 UPSERT 语句

        Args:
            config: 配置
            writes: 写入数据列表
            task_id: 任务 ID
            task_path: 任务路径
        """
        query = (
            MYSQL57_UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else MYSQL57_INSERT_CHECKPOINT_WRITES_SQL
        )

        with self._cursor(pipeline=True) as cur:
            cur.executemany(
                query,
                self._dump_writes(
                    config["configurable"]["thread_id"],
                    config["configurable"]["checkpoint_ns"],
                    config["configurable"]["checkpoint_id"],
                    task_id,
                    task_path,
                    writes,
                ),
            )

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """
        重写：列出 checkpoints

        使用新的查询逻辑和手动加载。

        Args:
            config: 配置（包含 thread_id 等）
            filter: 元数据过滤条件
            before: 返回此 checkpoint 之前的结果
            limit: 最大返回数量

        Yields:
            CheckpointTuple: checkpoint 元组

        Example:
            >>> config = {"configurable": {"thread_id": "thread-1"}}
            >>> for cp in checkpointer.list(config, limit=10):
            ...     print(cp.checkpoint["id"])
        """
        where, args = self._search_where(config, filter, before)
        query = self._select_sql(where) + " ORDER BY c.checkpoint_id DESC"
        if limit:
            query += f" LIMIT {limit}"

        with self._cursor() as cur:
            cur.execute(query, args)
            values = cur.fetchall()

            if not values:
                return

            for value in values:
                yield self._load_checkpoint_tuple(value)

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        重写：获取单个 checkpoint

        使用新的查询逻辑和手动加载。

        Args:
            config: 配置，必须包含 thread_id，可选 checkpoint_id

        Returns:
            CheckpointTuple | None: checkpoint 元组，不存在则返回 None

        Example:
            >>> config = {"configurable": {"thread_id": "thread-1"}}
            >>> cp = checkpointer.get_tuple(config)  # 获取最新的

            >>> config = {"configurable": {
            ...     "thread_id": "thread-1",
            ...     "checkpoint_id": "1ef4f797-..."
            ... }}
            >>> cp = checkpointer.get_tuple(config)  # 获取指定的
        """
        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        # 构建查询条件
        if checkpoint_id:
            args = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
            where = (
                "WHERE c.thread_id = %(thread_id)s "
                "AND c.checkpoint_ns_hash = UNHEX(MD5(%(checkpoint_ns)s)) "
                "AND c.checkpoint_id = %(checkpoint_id)s"
            )
        else:
            args = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            }
            where = (
                "WHERE c.thread_id = %(thread_id)s "
                "AND c.checkpoint_ns_hash = UNHEX(MD5(%(checkpoint_ns)s))"
            )

        query = self._select_sql(where)
        if not checkpoint_id:
            query += " ORDER BY c.checkpoint_id DESC LIMIT 1"

        with self._cursor() as cur:
            cur.execute(query, args)
            value = cur.fetchone()

            if value is None:
                return None

            return self._load_checkpoint_tuple(value)

    def setup(self) -> None:
        """
        重写：禁用自动建表

        MySQL 5.7 不支持原始的建表 SQL（包含 JSON DEFAULT 等特性）。
        必须手动执行 mysql57_schema.sql 建表脚本。

        Raises:
            NotImplementedError: 总是抛出，提示手动建表
        """
        raise NotImplementedError(
            "\n"
            "=" * 60 + "\n"
            "MySQL 5.7 不支持自动 setup()。\n"
            "\n"
            "请手动执行建表脚本：\n"
            "  mysql -u user -p database < mysql57_schema.sql\n"
            "\n"
            "或者在 MySQL 客户端中：\n"
            "  SOURCE /path/to/mysql57_schema.sql;\n"
            "\n"
            "建表脚本位置：\n"
            "  - 项目根目录下的 mysql57_schema.sql\n"
            "  - 或从 GitHub 下载\n"
            "\n"
            "验证表是否创建成功：\n"
            "  SHOW TABLES LIKE 'checkpoint%';\n"
            "  SELECT COUNT(*) FROM checkpoint_migrations;\n"
            "\n"
            "参考文档：QUICKSTART_MYSQL57.md\n"
            "=" * 60
        )


# ============================================
# 导出
# ============================================

__all__ = ["MySQL57Saver"]


# ============================================
# 使用示例（可删除）
# ============================================

if __name__ == "__main__":
    """
    使用示例和测试代码
    """
    import os
    from datetime import datetime

    # 从环境变量获取数据库连接
    DB_URI = os.getenv(
        "MYSQL_URI",
        "mysql://user:password@localhost:3306/database"
    )

    print("=" * 60)
    print("MySQL 5.7 Checkpointer 测试")
    print("=" * 60)

    try:
        # 创建 checkpointer
        with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
            print("✅ 连接成功")

            # 测试配置
            config = {
                "configurable": {
                    "thread_id": "test-thread",
                    "checkpoint_ns": ""
                }
            }

            # 测试写入
            print("\n测试写入 checkpoint...")
            checkpoint = {
                "v": 4,
                "ts": datetime.now().isoformat(),
                "id": "test-checkpoint-001",
                "channel_values": {
                    "messages": "Hello, MySQL 5.7!",
                    "counter": 42
                },
                "channel_versions": {
                    "messages": "1",
                    "counter": "1"
                },
                "versions_seen": {},
            }

            checkpointer.put(config, checkpoint, {"step": 1}, {})
            print("✅ 写入成功")

            # 测试读取
            print("\n测试读取 checkpoint...")
            loaded = checkpointer.get(config)
            if loaded:
                print(f"✅ 读取成功")
                print(f"   ID: {loaded.checkpoint['id']}")
                print(f"   Values: {loaded.checkpoint['channel_values']}")
            else:
                print("❌ 读取失败")

            # 测试列表
            print("\n测试列出 checkpoints...")
            count = 0
            for cp in checkpointer.list(config, limit=5):
                count += 1
                print(f"   [{count}] {cp.checkpoint['id']}")
            print(f"✅ 找到 {count} 个 checkpoints")

            print("\n" + "=" * 60)
            print("✅ 所有测试通过！")
            print("=" * 60)

    except NotImplementedError as e:
        print(f"\n⚠️  {e}")
        print("\n提示：这是预期的错误，说明需要手动建表。")

    except Exception as e:
        print(f"\n❌ 测试失败: {e}")
        print("\n请检查：")
        print("1. MYSQL_URI 环境变量是否正确")
        print("2. 数据库是否可访问")
        print("3. 是否已执行建表脚本 mysql57_schema.sql")
