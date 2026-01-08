"""
MySQL 5.7 兼容方案 - 通过继承重写关键方法

这个方案通过继承原包的类，只重写不兼容 MySQL 5.7 的方法。
优点：
- 不修改原包代码
- 可以升级原包
- 只重写必要的方法
- 维护成本低

使用方法：
    from mysql57_override import MySQL57PyMySQLSaver

    DB_URI = "mysql://user:pass@localhost:3306/dbname"
    with MySQL57PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
        # 直接使用，不要调用 setup()
        config = {"configurable": {"thread_id": "1", "checkpoint_ns": ""}}
        checkpointer.put(config, checkpoint, {}, {})
"""

from __future__ import annotations

import json
from typing import Any, Iterator
from collections.abc import Iterator as ABCIterator

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import CheckpointTuple, get_checkpoint_id
from langgraph.checkpoint.mysql.pymysql import PyMySQLSaver


# MySQL 5.7 兼容的简化 SELECT SQL
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


class MySQL57PyMySQLSaver(PyMySQLSaver):
    """
    MySQL 5.7 兼容的 PyMySQLSaver

    继承自官方的 PyMySQLSaver，只重写不兼容的方法。
    """

    @staticmethod
    def _select_sql(where: str) -> str:
        """重写：使用简化的 SELECT SQL，避免 JSON_TABLE 和 JSON_ARRAYAGG"""
        return MYSQL57_SELECT_SQL.replace("{WHERE}", where)

    def _load_checkpoint_tuple(self, value: dict[str, Any]) -> CheckpointTuple:
        """
        重写：手动加载 blobs 和 writes，避免使用 JSON 函数
        """
        # 解析 checkpoint JSON
        if isinstance(value["checkpoint"], str):
            value["checkpoint"] = json.loads(value["checkpoint"])

        # 手动加载 channel_values (blobs)
        channel_values = self._load_channel_values_mysql57(
            value["thread_id"],
            value["checkpoint_ns"],
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
        checkpoint: dict
    ) -> dict[str, Any]:
        """
        MySQL 5.7 兼容：手动加载 channel values
        """
        channel_versions = checkpoint.get("channel_versions", {})
        if not channel_versions:
            return {}

        result = {}
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
        MySQL 5.7 兼容：手动加载 pending writes
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

        return [
            (row["task_id"], row["channel"], self.serde.loads_typed((row["type"], row["blob"])))
            for row in rows
        ]

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: dict[str, Any] | None = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> ABCIterator[CheckpointTuple]:
        """重写：使用简化的查询"""
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
        """重写：使用简化的查询"""
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
        重写：禁用自动 setup，提示手动建表
        """
        raise NotImplementedError(
            "MySQL 5.7 不支持自动 setup()。\n"
            "请手动执行建表脚本：\n"
            "  mysql -u user -p database < mysql57_schema.sql\n"
            "或参考文档：QUICKSTART_MYSQL57.md"
        )


# 异步版本（如果需要）
try:
    from langgraph.checkpoint.mysql.aio import AIOMySQLSaver

    class MySQL57AIOMySQLSaver(AIOMySQLSaver):
        """MySQL 5.7 兼容的异步版本"""

        @staticmethod
        def _select_sql(where: str) -> str:
            return MYSQL57_SELECT_SQL.replace("{WHERE}", where)

        # 类似地重写异步方法...
        # async def _load_checkpoint_tuple(...)
        # async def _load_channel_values_mysql57(...)
        # 等等

        async def setup(self) -> None:
            raise NotImplementedError(
                "MySQL 5.7 不支持自动 setup()。请手动执行建表脚本。"
            )

except ImportError:
    # aiomysql 未安装
    MySQL57AIOMySQLSaver = None


__all__ = ["MySQL57PyMySQLSaver", "MySQL57AIOMySQLSaver"]
