"""
MySQL 5.7 兼容版本的 Checkpoint Saver

这个版本避免使用 MySQL 8.0+ 特性：
- 不使用 JSON_TABLE()
- 不使用 JSON_ARRAYAGG()
- 不使用 CTE (WITH)
- 不使用 JSON 字段的 DEFAULT 表达式

使用方法：
    from mysql57_compat_saver import MySQL57Saver

    DB_URI = "mysql://user:pass@localhost:3306/dbname"
    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        # 注意：setup() 仍然会失败，需要手动建表
        # checkpointer.setup()  # 不要调用

        # 直接使用
        config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
        # ... 使用 checkpointer
"""

from __future__ import annotations

import json
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

import pymysql
from pymysql.cursors import DictCursor
from typing_extensions import Self

from langgraph.checkpoint.mysql import BaseSyncMySQLSaver
from langgraph.checkpoint.base import CheckpointTuple
from langchain_core.runnables import RunnableConfig


# MySQL 5.7 兼容的 SELECT SQL - 不使用 JSON_TABLE, JSON_ARRAYAGG, CTE
MYSQL57_SELECT_SQL = """
select
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.checkpoint_id,
    c.parent_checkpoint_id,
    c.metadata
from checkpoints c
{WHERE}
"""


class MySQL57Saver(BaseSyncMySQLSaver[pymysql.Connection, DictCursor]):
    """
    MySQL 5.7 兼容的 Checkpoint Saver

    重写了查询方法以避免使用 MySQL 8.0+ 特性
    """

    @staticmethod
    def parse_conn_string(conn_string: str) -> dict[str, Any]:
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
        """创建连接"""
        with pymysql.connect(
            **cls.parse_conn_string(conn_string),
            autocommit=True,
        ) as conn:
            yield cls(conn)

    @staticmethod
    def _get_cursor_from_connection(conn: pymysql.Connection) -> DictCursor:
        return conn.cursor(DictCursor)

    @staticmethod
    def _select_sql(where: str) -> str:
        """重写 SELECT SQL，使用 MySQL 5.7 兼容的版本"""
        return MYSQL57_SELECT_SQL.replace("{WHERE}", where)

    def _load_checkpoint_tuple(self, value: dict[str, Any]) -> CheckpointTuple:
        """
        重写 _load_checkpoint_tuple，手动加载 blobs 和 writes
        因为 MySQL 5.7 不支持 JSON_ARRAYAGG
        """
        # 解析 checkpoint JSON
        if isinstance(value["checkpoint"], str):
            value["checkpoint"] = json.loads(value["checkpoint"])

        # 手动加载 channel_values (blobs)
        channel_values = self._load_channel_values_mysql57(
            value["thread_id"],
            value["checkpoint_ns"],
            value["checkpoint_id"],
            value["checkpoint"]
        )

        # 手动加载 pending_writes
        pending_writes = self._load_pending_writes_mysql57(
            value["thread_id"],
            value["checkpoint_ns"],
            value["checkpoint_id"]
        )

        return CheckpointTuple(
            {
                "configurable": {
                    "thread_id": value["thread_id"],
                    "checkpoint_ns": value["checkpoint_ns"],
                    "checkpoint_id": value["checkpoint_id"],
                }
            },
            {
                **value["checkpoint"],
                "channel_values": {
                    **value["checkpoint"].get("channel_values", {}),
                    **channel_values,
                },
            },
            self._load_metadata(value["metadata"]),
            (
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
            pending_writes,
        )

    def _load_channel_values_mysql57(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        checkpoint: dict
    ) -> dict[str, Any]:
        """
        MySQL 5.7 兼容的方式加载 channel values
        不使用 JSON_TABLE 和 JSON_ARRAYAGG
        """
        # 从 checkpoint 中获取 channel_versions
        channel_versions = checkpoint.get("channel_versions", {})
        if not channel_versions:
            return {}

        # 构建查询 - 使用 IN 子句而不是 JSON_TABLE
        channels = list(channel_versions.keys())
        if not channels:
            return {}

        # 为每个 channel 查询对应的 blob
        placeholders = ", ".join(["%s"] * len(channels))
        sql = f"""
            SELECT channel, type, `blob`
            FROM checkpoint_blobs
            WHERE thread_id = %s
                AND checkpoint_ns_hash = UNHEX(MD5(%s))
                AND channel IN ({placeholders})
        """

        params = [thread_id, checkpoint_ns] + channels

        with self._cursor() as cur:
            cur.execute(sql, params)
            rows = cur.fetchall()

        # 过滤出匹配版本的 blobs
        result = {}
        for row in rows:
            channel = row["channel"]
            expected_version = str(channel_versions.get(channel, ""))

            # 需要单独查询版本匹配的记录
            sql_versioned = """
                SELECT type, `blob`
                FROM checkpoint_blobs
                WHERE thread_id = %s
                    AND checkpoint_ns_hash = UNHEX(MD5(%s))
                    AND channel = %s
                    AND version = %s
            """
            with self._cursor() as cur:
                cur.execute(sql_versioned, (thread_id, checkpoint_ns, channel, expected_version))
                versioned_row = cur.fetchone()

            if versioned_row and versioned_row["type"] != "empty":
                blob_data = versioned_row["blob"]
                if blob_data is not None:
                    result[channel] = self.serde.loads_typed((versioned_row["type"], blob_data))

        return result

    def _load_pending_writes_mysql57(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str
    ) -> list[tuple[str, str, Any]]:
        """
        MySQL 5.7 兼容的方式加载 pending writes
        不使用 JSON_ARRAYAGG
        """
        sql = """
            SELECT task_id, channel, type, `blob`, idx
            FROM checkpoint_writes
            WHERE thread_id = %s
                AND checkpoint_ns_hash = UNHEX(MD5(%s))
                AND checkpoint_id = %s
            ORDER BY task_id, idx
        """

        with self._cursor() as cur:
            cur.execute(sql, (thread_id, checkpoint_ns, checkpoint_id))
            rows = cur.fetchall()

        return [
            (
                row["task_id"],
                row["channel"],
                self.serde.loads_typed((row["type"], row["blob"]))
            )
            for row in rows
        ]

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """
        重写 list 方法，使用 MySQL 5.7 兼容的查询
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
        重写 get_tuple 方法，使用 MySQL 5.7 兼容的查询
        """
        from langgraph.checkpoint.base import get_checkpoint_id

        thread_id = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        if checkpoint_id:
            args = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint_id,
            }
            where = "WHERE c.thread_id = %(thread_id)s AND c.checkpoint_ns_hash = UNHEX(MD5(%(checkpoint_ns)s)) AND c.checkpoint_id = %(checkpoint_id)s"
        else:
            args = {
                "thread_id": thread_id,
                "checkpoint_ns": checkpoint_ns,
            }
            where = "WHERE c.thread_id = %(thread_id)s AND c.checkpoint_ns_hash = UNHEX(MD5(%(checkpoint_ns)s))"

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
        警告：MySQL 5.7 不支持原始的 setup() 方法
        请手动创建表或使用提供的兼容 SQL 脚本
        """
        raise NotImplementedError(
            "MySQL 5.7 不支持自动 setup()。\n"
            "请手动执行 MySQL 5.7 兼容的建表脚本。\n"
            "参考：https://your-docs-link"
        )


__all__ = ["MySQL57Saver"]
