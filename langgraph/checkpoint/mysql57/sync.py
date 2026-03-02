"""
Synchronous MySQL 5.7 / PolarDB-X checkpoint saver (pymysql driver).

Usage
-----
    from langgraph.checkpoint.mysql57 import MySQL57Saver

    DB_URI = "mysql://user:password@host:3306/database"

    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        checkpointer.setup()          # run once to create tables
        config = {
            "configurable": {
                "thread_id": "thread-1",
                "checkpoint_ns": "",
            }
        }
        cp = checkpointer.get_tuple(config)

Notes
-----
- autocommit=True is mandatory; transactions are managed per-operation
  by the _cursor(pipeline=True) context manager.
- Thread-safe: a threading.Lock serialises all cursor access on a single
  connection. For high-concurrency use cases, prefer the async pool saver.
"""

from __future__ import annotations

import json
import threading
import urllib.parse
from collections.abc import Iterator
from contextlib import contextmanager
from typing import Any

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
    get_serializable_checkpoint_metadata,
)
from langgraph.checkpoint.serde.base import SerializerProtocol

from .base import (
    BaseMySQLSaver57,
    INSERT_CHECKPOINT_WRITES_SQL,
    MetadataInput,
    SELECT_WRITES_SQL,
    UPSERT_CHECKPOINT_BLOBS_SQL,
    UPSERT_CHECKPOINTS_SQL,
    UPSERT_CHECKPOINT_WRITES_SQL,
    _md5_hash,
)


class MySQL57Saver(BaseMySQLSaver57):
    """
    Synchronous checkpoint saver for MySQL 5.7 / PolarDB-X.

    Inherits from BaseMySQLSaver57 (which extends BaseCheckpointSaver[str])
    and uses pymysql as the database driver.

    Compatibility
    -------------
    - MySQL 5.7+
    - MariaDB 10.2+
    - PolarDB-X (Alibaba Cloud)
    - Alibaba Cloud RDS MySQL 5.7

    Example
    -------
    ::

        DB_URI = "mysql://root:secret@127.0.0.1:3306/langgraph"

        with MySQL57Saver.from_conn_string(DB_URI) as cp:
            cp.setup()
            graph = build_graph(checkpointer=cp)
            result = graph.invoke(
                {"messages": [HumanMessage("hi")]},
                config={"configurable": {"thread_id": "1"}},
            )
    """

    conn: pymysql.Connection
    lock: threading.Lock

    def __init__(
        self,
        conn: pymysql.Connection,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.conn = conn
        self.lock = threading.Lock()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_conn_string(conn_string: str) -> dict[str, Any]:
        """
        Parse a MySQL connection URL into pymysql.connect() keyword arguments.

        Format: ``mysql://user:password@host:port/database[?unix_socket=...]``
        """
        parsed = urllib.parse.urlparse(conn_string)
        extra = dict(urllib.parse.parse_qsl(parsed.query))
        return {
            "host":        parsed.hostname or "localhost",
            "user":        parsed.username,
            "password":    parsed.password or "",
            "database":    parsed.path.lstrip("/") or None,
            "port":        parsed.port or 3306,
            "unix_socket": extra.get("unix_socket"),
            "charset":     "utf8mb4",
        }

    @classmethod
    @contextmanager
    def from_conn_string(
        cls,
        conn_string: str,
        serde: SerializerProtocol | None = None,
    ) -> Iterator[Self]:
        """
        Create a :class:`MySQL57Saver` from a MySQL connection string.

        The connection is closed automatically when the context manager exits.

        Args:
            conn_string: ``mysql://user:password@host:port/database``
            serde:       optional custom serialiser (default: JsonPlusSerializer)

        Yields:
            :class:`MySQL57Saver`

        Example::

            with MySQL57Saver.from_conn_string(DB_URI) as cp:
                cp.setup()
        """
        with pymysql.connect(
            **cls.parse_conn_string(conn_string),
            autocommit=True,
        ) as conn:
            yield cls(conn=conn, serde=serde)

    @contextmanager
    def _cursor(self, *, pipeline: bool = False) -> Iterator[DictCursor]:
        """
        Yield a DictCursor, optionally wrapped in a transaction.

        Args:
            pipeline: if True, wraps the cursor in BEGIN / COMMIT with
                      automatic ROLLBACK on exception.
        """
        with self.lock:
            if pipeline:
                self.conn.begin()
                try:
                    with self.conn.cursor(DictCursor) as cur:
                        yield cur
                    self.conn.commit()
                except Exception:
                    self.conn.rollback()
                    raise
            else:
                with self.conn.cursor(DictCursor) as cur:
                    yield cur

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    def setup(self) -> None:
        """
        Create tables and run all pending migrations (idempotent).

        Must be called once before any checkpoint operations.  Safe to
        call again after adding new migrations to MIGRATIONS.
        """
        with self._cursor() as cur:
            cur.execute(self.MIGRATIONS[0])
            cur.execute(
                "SELECT v FROM checkpoint_migrations ORDER BY v DESC LIMIT 1"
            )
            row = cur.fetchone()
            version = -1 if row is None else row["v"]

        for v, migration in zip(
            range(version + 1, len(self.MIGRATIONS)),
            self.MIGRATIONS[version + 1:],
        ):
            with self._cursor() as cur:
                cur.execute(migration)
            with self._cursor() as cur:
                cur.execute(
                    "INSERT INTO checkpoint_migrations (v) VALUES (%s)", (v,)
                )

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    def get_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """
        Retrieve one checkpoint.

        If ``checkpoint_id`` is present in config, that specific checkpoint
        is returned.  Otherwise the latest checkpoint for the thread is
        returned.

        Args:
            config: must contain ``thread_id``; optionally
                    ``checkpoint_ns`` (default ``""``) and ``checkpoint_id``

        Returns:
            :class:`~langgraph.checkpoint.base.CheckpointTuple` or ``None``
        """
        thread_id     = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

        ns_hash = _md5_hash(checkpoint_ns)  # pre-compute to avoid UNHEX(MD5()) in SQL

        if checkpoint_id:
            where = (
                "WHERE c.thread_id = %(thread_id)s"
                " AND c.checkpoint_ns_hash = %(checkpoint_ns_hash)s"
                " AND c.checkpoint_id = %(checkpoint_id)s"
            )
            args: dict[str, Any] = {
                "thread_id":          thread_id,
                "checkpoint_ns_hash": ns_hash,
                "checkpoint_id":      checkpoint_id,
            }
        else:
            where = (
                "WHERE c.thread_id = %(thread_id)s"
                " AND c.checkpoint_ns_hash = %(checkpoint_ns_hash)s"
            )
            args = {
                "thread_id":          thread_id,
                "checkpoint_ns_hash": ns_hash,
            }

        query = self._select_sql(where)
        if not checkpoint_id:
            query += " ORDER BY c.checkpoint_id DESC LIMIT 1"

        with self._cursor() as cur:
            cur.execute(query, args)
            row = cur.fetchone()
            if row is None:
                return None

            checkpoint = json.loads(row["checkpoint"])
            channel_versions: dict[str, str] = checkpoint.get("channel_versions", {})

            # Batch-load blobs for all channels referenced by this checkpoint
            blob_rows: list[dict] = []
            if channel_versions:
                channels = list(channel_versions.keys())
                cur.execute(
                    self._select_blobs_sql(channels),
                    (thread_id, ns_hash, *channels),
                )
                blob_rows = cur.fetchall()

            # Load pending writes
            cur.execute(
                SELECT_WRITES_SQL,
                (thread_id, ns_hash, row["checkpoint_id"]),
            )
            write_rows = cur.fetchall()

        return self._build_checkpoint_tuple(row, checkpoint, blob_rows, write_rows)

    def list(
        self,
        config: RunnableConfig | None,
        *,
        filter: MetadataInput = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> Iterator[CheckpointTuple]:
        """
        Iterate over checkpoints, newest first.

        Args:
            config: filter by ``thread_id`` and optionally ``checkpoint_ns``
            filter: metadata key-value filter (uses ``json_contains``)
            before: only return checkpoints older than this checkpoint_id
            limit:  maximum number of results

        Yields:
            :class:`~langgraph.checkpoint.base.CheckpointTuple`
        """
        where, args = self._search_where(config, filter, before)
        query = self._select_sql(where) + " ORDER BY c.checkpoint_id DESC"
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        # Fetch all main rows first so we release the cursor before
        # the per-checkpoint blob/write queries.
        with self._cursor() as cur:
            cur.execute(query, args)
            rows = cur.fetchall()

        for row in rows:
            checkpoint = json.loads(row["checkpoint"])
            channel_versions: dict[str, str] = checkpoint.get("channel_versions", {})
            thread_id     = row["thread_id"]
            checkpoint_ns = row["checkpoint_ns"]
            checkpoint_id = row["checkpoint_id"]
            ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute

            with self._cursor() as cur:
                blob_rows: list[dict] = []
                if channel_versions:
                    channels = list(channel_versions.keys())
                    cur.execute(
                        self._select_blobs_sql(channels),
                        (thread_id, ns_hash, *channels),
                    )
                    blob_rows = cur.fetchall()

                cur.execute(
                    SELECT_WRITES_SQL,
                    (thread_id, ns_hash, checkpoint_id),
                )
                write_rows = cur.fetchall()

            yield self._build_checkpoint_tuple(row, checkpoint, blob_rows, write_rows)

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def put(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """
        Persist a checkpoint and its changed channel values.

        Primitive channel values (str, int, float, bool, None) are stored
        inline in the ``checkpoints.checkpoint`` JSON column.  All other
        values (message lists, tool results, etc.) are serialised to
        ``checkpoint_blobs`` via :meth:`_dump_blobs`.

        Returns:
            Updated config pointing to the newly saved checkpoint.
        """
        configurable = config["configurable"].copy()
        thread_id     = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")
        checkpoint_id = configurable.pop("checkpoint_id", None)

        copy = checkpoint.copy()
        copy["channel_values"] = copy["channel_values"].copy()

        # Separate primitive (inline) values from complex (blob) values
        blob_values: dict[str, Any] = {}
        for k, v in checkpoint["channel_values"].items():
            if not (v is None or isinstance(v, (str, int, float, bool))):
                blob_values[k] = copy["channel_values"].pop(k)

        ns_hash = _md5_hash(checkpoint_ns)  # pre-compute once for all writes

        with self._cursor(pipeline=True) as cur:
            blob_versions = {
                k: v for k, v in new_versions.items() if k in blob_values
            }
            if blob_versions:
                cur.executemany(
                    UPSERT_CHECKPOINT_BLOBS_SQL,
                    self._dump_blobs(
                        thread_id, checkpoint_ns, blob_values, blob_versions
                    ),
                )
            cur.execute(
                UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    ns_hash,                # raw bytes → BINARY(16)
                    checkpoint["id"],
                    checkpoint_id,
                    json.dumps(copy),
                    json.dumps(
                        get_serializable_checkpoint_metadata(config, metadata)
                    ),
                ),
            )

        return {
            "configurable": {
                "thread_id":     thread_id,
                "checkpoint_ns": checkpoint_ns,
                "checkpoint_id": checkpoint["id"],
            }
        }

    def put_writes(
        self,
        config: RunnableConfig,
        writes: list[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """
        Persist intermediate writes for a checkpoint.

        Error / interrupt channels (keys in ``WRITES_IDX_MAP``) use an
        UPSERT so they can be overwritten on retry.  Normal task outputs
        use INSERT IGNORE to avoid duplicate-key errors on re-runs.
        """
        query = (
            UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else INSERT_CHECKPOINT_WRITES_SQL
        )
        params = self._dump_writes(
            config["configurable"]["thread_id"],
            config["configurable"]["checkpoint_ns"],
            config["configurable"]["checkpoint_id"],
            task_id,
            task_path,
            writes,
        )
        with self._cursor(pipeline=True) as cur:
            cur.executemany(query, params)

    def delete_thread(self, thread_id: str) -> None:
        """
        Delete all checkpoints, blobs, and writes for *thread_id*.

        This is a cascading delete across all three tables and is wrapped
        in a transaction.
        """
        with self._cursor(pipeline=True) as cur:
            cur.execute(
                "DELETE FROM checkpoints WHERE thread_id = %s", (thread_id,)
            )
            cur.execute(
                "DELETE FROM checkpoint_blobs WHERE thread_id = %s", (thread_id,)
            )
            cur.execute(
                "DELETE FROM checkpoint_writes WHERE thread_id = %s", (thread_id,)
            )


__all__ = ["MySQL57Saver"]
