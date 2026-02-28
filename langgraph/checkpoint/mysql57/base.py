"""
Base class for MySQL 5.7 / PolarDB-X compatible checkpoint savers.

Compatibility matrix
--------------------
Feature                    MySQL 5.7   MySQL 8.0   MariaDB 10.2+
JSON column type            YES         YES         YES
JSON DEFAULT expression     NO          YES         YES   ← use LONGTEXT
JSON_TABLE()                NO          YES         NO    ← multi-step query
JSON_ARRAYAGG()             NO          YES         YES   ← multi-step query
WITH CTE                    NO          YES         YES   ← rewrite
VALUES(...) AS new          NO          YES(8.0.19) NO    ← use VALUES(col)
STORED generated col in PK  limited     YES         NO    ← explicit BINARY(16)

This module implements all database-agnostic logic (SQL constants, schema
migrations, serialization helpers). Driver-specific classes live in
sync.py (pymysql) and aio.py (aiomysql).
"""

from __future__ import annotations

import json
import random
from collections.abc import Sequence
from typing import Any, Optional, cast

from langchain_core.runnables import RunnableConfig
from langgraph.checkpoint.base import (
    WRITES_IDX_MAP,
    BaseCheckpointSaver,
    ChannelVersions,
    get_checkpoint_id,
)

MetadataInput = Optional[dict[str, Any]]

# ---------------------------------------------------------------------------
# Schema migrations (MySQL 5.7 compatible)
#
# Key differences from official langgraph-checkpoint-mysql schema:
#   1. LONGTEXT instead of JSON  — MySQL 5.7 JSON columns do not accept
#      DEFAULT expressions (e.g. DEFAULT ('{}')). LONGTEXT is byte-for-byte
#      equivalent for our use case; JSON functions still work on it.
#   2. Explicit BINARY(16) column for checkpoint_ns_hash — MySQL 5.7
#      has restrictions on using STORED generated columns as primary key
#      members; we compute UNHEX(MD5(...)) explicitly in every DML statement.
#   3. No generated / virtual columns at all.
# ---------------------------------------------------------------------------

MIGRATIONS: list[str] = [
    # v0 ── migration bookkeeping
    """CREATE TABLE IF NOT EXISTS checkpoint_migrations (
    v INTEGER PRIMARY KEY
)""",
    # v1 ── checkpoints
    """CREATE TABLE IF NOT EXISTS checkpoints (
    thread_id           VARCHAR(150)    NOT NULL,
    checkpoint_ns       VARCHAR(2000)   NOT NULL DEFAULT '',
    checkpoint_ns_hash  BINARY(16)      NOT NULL,
    checkpoint_id       VARCHAR(150)    NOT NULL,
    parent_checkpoint_id VARCHAR(150),
    checkpoint          LONGTEXT        NOT NULL,
    metadata            LONGTEXT        NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns_hash, checkpoint_id)
)""",
    # v2 ── blobs (immutable: same channel+version ⟹ same bytes)
    """CREATE TABLE IF NOT EXISTS checkpoint_blobs (
    thread_id           VARCHAR(150)    NOT NULL,
    checkpoint_ns       VARCHAR(2000)   NOT NULL DEFAULT '',
    checkpoint_ns_hash  BINARY(16)      NOT NULL,
    channel             VARCHAR(150)    NOT NULL,
    version             VARCHAR(150)    NOT NULL,
    type                VARCHAR(150)    NOT NULL,
    `blob`              LONGBLOB,
    PRIMARY KEY (thread_id, checkpoint_ns_hash, channel, version)
)""",
    # v3 ── pending writes (error channels may be overwritten)
    """CREATE TABLE IF NOT EXISTS checkpoint_writes (
    thread_id           VARCHAR(150)    NOT NULL,
    checkpoint_ns       VARCHAR(2000)   NOT NULL DEFAULT '',
    checkpoint_ns_hash  BINARY(16)      NOT NULL,
    checkpoint_id       VARCHAR(150)    NOT NULL,
    task_id             VARCHAR(150)    NOT NULL,
    task_path           VARCHAR(2000)   NOT NULL DEFAULT '',
    idx                 INTEGER         NOT NULL,
    channel             VARCHAR(150)    NOT NULL,
    type                VARCHAR(150),
    `blob`              LONGBLOB        NOT NULL,
    PRIMARY KEY (thread_id, checkpoint_ns_hash, checkpoint_id, task_id, idx)
)""",
    # v4-v7 ── performance indexes
    "CREATE INDEX idx_checkpoints_thread_id ON checkpoints (thread_id)",
    "CREATE INDEX idx_blobs_thread_id       ON checkpoint_blobs (thread_id)",
    "CREATE INDEX idx_writes_thread_id      ON checkpoint_writes (thread_id)",
    "CREATE INDEX idx_checkpoints_cp_id     ON checkpoints (checkpoint_id)",
]

# ---------------------------------------------------------------------------
# SQL constants  (MySQL 5.7 compatible — no JSON_TABLE, JSON_ARRAYAGG, CTE)
# ---------------------------------------------------------------------------

# Main SELECT: returns one row per checkpoint; blobs and writes are loaded
# separately in Python (see _load_blobs / _load_writes_from_rows).
SELECT_SQL = """
SELECT
    c.thread_id,
    c.checkpoint,
    c.checkpoint_ns,
    c.checkpoint_id,
    c.parent_checkpoint_id,
    c.metadata
FROM checkpoints c
{WHERE}"""

# Batch-load blobs for a set of channels.  Version matching is done in Python
# so that we need only one query regardless of how many channels exist.
SELECT_BLOBS_SQL = """
SELECT channel, version, type, `blob`
FROM checkpoint_blobs
WHERE thread_id = %s
  AND checkpoint_ns_hash = UNHEX(MD5(%s))
  AND channel IN ({PLACEHOLDERS})"""

# Load pending writes for one checkpoint, ordered for deterministic assembly.
SELECT_WRITES_SQL = """
SELECT task_id, channel, type, `blob`, idx
FROM checkpoint_writes
WHERE thread_id = %s
  AND checkpoint_ns_hash = UNHEX(MD5(%s))
  AND checkpoint_id = %s
ORDER BY task_id, idx"""

# ---------------------------------------------------------------------------
# UPSERT / INSERT SQL  (MySQL 5.7 compatible)
#
# Uses  ON DUPLICATE KEY UPDATE col = VALUES(col)  instead of the MySQL 8.0
# syntax  INSERT ... AS new ... ON DUPLICATE KEY UPDATE col = new.col
# ---------------------------------------------------------------------------

# Blobs are content-addressed: identical channel+version ⟹ identical bytes.
# INSERT IGNORE is safe and avoids unnecessary writes.
UPSERT_CHECKPOINT_BLOBS_SQL = """
    INSERT IGNORE INTO checkpoint_blobs
        (thread_id, checkpoint_ns, checkpoint_ns_hash,
         channel, version, type, `blob`)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s)"""

# Checkpoints may be re-saved (e.g. on graph retry).
UPSERT_CHECKPOINTS_SQL = """
    INSERT INTO checkpoints
        (thread_id, checkpoint_ns, checkpoint_ns_hash,
         checkpoint_id, parent_checkpoint_id, checkpoint, metadata)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        checkpoint = VALUES(checkpoint),
        metadata   = VALUES(metadata)"""

# Error/interrupt channels (idx in WRITES_IDX_MAP) may be overwritten.
UPSERT_CHECKPOINT_WRITES_SQL = """
    INSERT INTO checkpoint_writes
        (thread_id, checkpoint_ns, checkpoint_ns_hash,
         checkpoint_id, task_id, task_path, idx, channel, type, `blob`)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        channel = VALUES(channel),
        type    = VALUES(type),
        `blob`  = VALUES(`blob`)"""

# Normal task output: INSERT IGNORE so re-runs don't overwrite completed work.
INSERT_CHECKPOINT_WRITES_SQL = """
    INSERT IGNORE INTO checkpoint_writes
        (thread_id, checkpoint_ns, checkpoint_ns_hash,
         checkpoint_id, task_id, task_path, idx, channel, type, `blob`)
    VALUES (%s, %s, UNHEX(MD5(%s)), %s, %s, %s, %s, %s, %s, %s)"""


# ---------------------------------------------------------------------------
# Base class
# ---------------------------------------------------------------------------

class BaseMySQLSaver57(BaseCheckpointSaver[str]):
    """
    Database-agnostic base for MySQL 5.7 / PolarDB-X checkpoint savers.

    Provides:
    - MIGRATIONS list (MySQL 5.7 compatible DDL)
    - Serialisation helpers: _dump_blobs(), _dump_writes()
    - Deserialisation helpers: _load_blobs(), _load_writes_from_rows()
    - Query builder: _search_where(), _select_sql(), _select_blobs_sql()
    - Checkpoint assembly: _build_checkpoint_tuple()
    - Version generator: get_next_version()

    Subclasses supply connection management and implement the public API.
    """

    MIGRATIONS = MIGRATIONS

    # ------------------------------------------------------------------
    # Serialisation
    # ------------------------------------------------------------------

    def _dump_blobs(
        self,
        thread_id: str,
        checkpoint_ns: str,
        values: dict[str, Any],
        versions: ChannelVersions,
    ) -> list[tuple]:
        """
        Serialize channel values into rows for checkpoint_blobs.

        The third element in each tuple is checkpoint_ns again: it feeds
        the UNHEX(MD5(%s)) expression for the checkpoint_ns_hash column.
        """
        if not versions:
            return []
        return [
            (
                thread_id,
                checkpoint_ns,
                checkpoint_ns,          # → UNHEX(MD5(%s))
                k,
                cast(str, ver),
                *(
                    self.serde.dumps_typed(values[k])
                    if k in values
                    else ("empty", None)
                ),
            )
            for k, ver in versions.items()
        ]

    def _dump_writes(
        self,
        thread_id: str,
        checkpoint_ns: str,
        checkpoint_id: str,
        task_id: str,
        task_path: str,
        writes: Sequence[tuple[str, Any]],
    ) -> list[tuple]:
        """Serialize writes into rows for checkpoint_writes."""
        return [
            (
                thread_id,
                checkpoint_ns,
                checkpoint_ns,          # → UNHEX(MD5(%s))
                checkpoint_id,
                task_id,
                task_path,
                WRITES_IDX_MAP.get(channel, idx),
                channel,
                *self.serde.dumps_typed(value),
            )
            for idx, (channel, value) in enumerate(writes)
        ]

    # ------------------------------------------------------------------
    # Deserialisation
    # ------------------------------------------------------------------

    def _load_blobs(
        self,
        rows: list[dict],
        channel_versions: dict[str, str],
    ) -> dict[str, Any]:
        """
        Deserialize blob rows into channel values.

        We match each row's (channel, version) against channel_versions so
        that stale blobs from earlier runs are silently skipped.

        Args:
            rows: rows from SELECT_BLOBS_SQL
            channel_versions: {channel: expected_version} from the checkpoint

        Returns:
            {channel_name: deserialized_value}
        """
        if not rows:
            return {}
        result: dict[str, Any] = {}
        for row in rows:
            channel = row["channel"]
            version = row["version"]
            if channel_versions.get(channel) == version:
                if row["type"] != "empty" and row["blob"] is not None:
                    result[channel] = self.serde.loads_typed(
                        (row["type"], row["blob"])
                    )
        return result

    def _load_writes_from_rows(
        self,
        rows: list[dict],
    ) -> list[tuple[str, str, Any]]:
        """
        Deserialize write rows into pending_writes format.

        Args:
            rows: rows from SELECT_WRITES_SQL (already ordered by task_id, idx)

        Returns:
            [(task_id, channel, deserialized_value), ...]
        """
        return [
            (
                row["task_id"],
                row["channel"],
                self.serde.loads_typed((row["type"], row["blob"])),
            )
            for row in rows
        ]

    # ------------------------------------------------------------------
    # Query builders
    # ------------------------------------------------------------------

    def get_next_version(self, current: str | None, channel: None) -> str:
        """Generate a monotonically increasing version string."""
        if current is None:
            current_v = 0
        elif isinstance(current, int):
            current_v = current
        else:
            current_v = int(current.split(".")[0])
        next_v = current_v + 1
        next_h = random.random()
        return f"{next_v:032}.{next_h:016}"

    def _search_where(
        self,
        config: RunnableConfig | None,
        filter: MetadataInput,
        before: RunnableConfig | None = None,
    ) -> tuple[str, dict[str, Any]]:
        """
        Build a parameterised WHERE clause for list() queries.

        Returns:
            (where_clause, params_dict)
        """
        wheres: list[str] = []
        params: dict[str, Any] = {}

        if config:
            wheres.append("c.thread_id = %(thread_id)s")
            params["thread_id"] = config["configurable"]["thread_id"]

            checkpoint_ns = config["configurable"].get("checkpoint_ns")
            if checkpoint_ns is not None:
                wheres.append(
                    "c.checkpoint_ns_hash = UNHEX(MD5(%(checkpoint_ns)s))"
                )
                params["checkpoint_ns"] = checkpoint_ns

            if checkpoint_id := get_checkpoint_id(config):
                wheres.append("c.checkpoint_id = %(checkpoint_id)s")
                params["checkpoint_id"] = checkpoint_id

        if filter:
            # json_contains() works on LONGTEXT columns in MySQL 5.7
            wheres.append("json_contains(c.metadata, %(filter)s)")
            params["filter"] = json.dumps(filter)

        if before is not None:
            wheres.append("c.checkpoint_id < %(before)s")
            params["before"] = get_checkpoint_id(before)

        where_clause = "WHERE " + " AND ".join(wheres) if wheres else ""
        return where_clause, params

    @staticmethod
    def _select_sql(where: str) -> str:
        return SELECT_SQL.replace("{WHERE}", where)

    @staticmethod
    def _select_blobs_sql(channels: list[str]) -> str:
        """Build SELECT_BLOBS_SQL with the correct number of placeholders."""
        placeholders = ", ".join(["%s"] * len(channels))
        return SELECT_BLOBS_SQL.replace("{PLACEHOLDERS}", placeholders)

    # ------------------------------------------------------------------
    # Checkpoint assembly
    # ------------------------------------------------------------------

    def _build_checkpoint_tuple(
        self,
        row: dict,
        checkpoint: dict,
        blob_rows: list[dict],
        write_rows: list[dict],
    ) -> "CheckpointTuple":
        """
        Assemble a CheckpointTuple from raw database rows.

        Args:
            row:        main row from checkpoints table
            checkpoint: already JSON-parsed checkpoint dict
            blob_rows:  rows from checkpoint_blobs
            write_rows: rows from checkpoint_writes
        """
        from langgraph.checkpoint.base import CheckpointTuple  # local import to avoid circular

        channel_versions: dict[str, str] = checkpoint.get("channel_versions", {})
        channel_values = self._load_blobs(blob_rows, channel_versions)
        pending_writes = self._load_writes_from_rows(write_rows)

        return CheckpointTuple(
            config={
                "configurable": {
                    "thread_id":     row["thread_id"],
                    "checkpoint_ns": row["checkpoint_ns"],
                    "checkpoint_id": row["checkpoint_id"],
                }
            },
            checkpoint={
                **checkpoint,
                "channel_values": {
                    **checkpoint.get("channel_values", {}),
                    **channel_values,
                },
            },
            metadata=json.loads(row["metadata"]),
            parent_config=(
                {
                    "configurable": {
                        "thread_id":     row["thread_id"],
                        "checkpoint_ns": row["checkpoint_ns"],
                        "checkpoint_id": row["parent_checkpoint_id"],
                    }
                }
                if row.get("parent_checkpoint_id")
                else None
            ),
            pending_writes=pending_writes,
        )
