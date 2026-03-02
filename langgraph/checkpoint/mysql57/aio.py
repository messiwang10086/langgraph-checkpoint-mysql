"""
Asynchronous MySQL 5.7 / PolarDB-X checkpoint savers (aiomysql driver).

Two classes are provided:

AIOMySQL57Saver
    Single-connection saver with built-in auto-reconnect.
    Good for simple deployments where one coroutine accesses the DB
    at a time.

AIOMySQL57PoolSaver
    Connection-pool-backed saver.  Each operation acquires its own
    connection, so no asyncio.Lock is needed and many coroutines can
    operate concurrently.  Recommended for production.

Usage (single connection)
-------------------------
::

    from langgraph.checkpoint.mysql57 import AIOMySQL57Saver

    DB_URI = "mysql://user:password@host:3306/database"

    async with AIOMySQL57Saver.from_conn_string(DB_URI) as cp:
        await cp.setup()
        result = await graph.ainvoke(
            {"messages": [HumanMessage("hi")]},
            config={"configurable": {"thread_id": "1"}},
        )

Usage (connection pool)
-----------------------
::

    from langgraph.checkpoint.mysql57 import AIOMySQL57PoolSaver

    async with AIOMySQL57PoolSaver.from_conn_string(
        DB_URI, minsize=2, maxsize=20
    ) as cp:
        await cp.setup()
        ...
"""

from __future__ import annotations

import asyncio
import json
import logging
import urllib.parse
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from typing import Any, cast

import aiomysql
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

logger = logging.getLogger(__name__)

# Substrings that indicate the TCP connection to MySQL was lost
_CONNECTION_ERROR_SIGNALS = (
    "Not connected",
    "Lost connection",
    "MySQL server has gone away",
    "(0,",          # aiomysql OperationalError with errno 0
    "Broken pipe",
    "Connection reset",
)


def _is_connection_error(exc: BaseException) -> bool:
    """Return True if *exc* looks like a transient connection failure."""
    msg = str(exc)
    return any(sig in msg for sig in _CONNECTION_ERROR_SIGNALS)


# ---------------------------------------------------------------------------
# Shared helpers (used by both classes)
# ---------------------------------------------------------------------------

def _parse_conn_string(conn_string: str) -> dict[str, Any]:
    """
    Parse a MySQL connection URL into ``aiomysql.connect()`` keyword args.

    Note: aiomysql uses ``db=`` (not ``database=``) for the schema name.
    """
    parsed = urllib.parse.urlparse(conn_string)
    extra  = dict(urllib.parse.parse_qsl(parsed.query))
    return {
        "host":        parsed.hostname or "localhost",
        "user":        parsed.username,
        "password":    parsed.password or "",
        "db":          parsed.path.lstrip("/") or None,   # aiomysql kwarg
        "port":        parsed.port or 3306,
        "unix_socket": extra.get("unix_socket"),
        "charset":     "utf8mb4",
    }


async def _run_migrations(
    saver: BaseMySQLSaver57,
    acquire,                    # callable() → async context manager yielding cursor
) -> None:
    """Run pending migrations using the provided cursor factory."""
    async with acquire() as cur:
        await cur.execute(saver.MIGRATIONS[0])
        await cur.execute(
            "SELECT v FROM checkpoint_migrations ORDER BY v DESC LIMIT 1"
        )
        row = await cur.fetchone()
        version = -1 if row is None else row["v"]

    for v, migration in zip(
        range(version + 1, len(saver.MIGRATIONS)),
        saver.MIGRATIONS[version + 1:],
    ):
        async with acquire() as cur:
            await cur.execute(migration)
        async with acquire() as cur:
            await cur.execute(
                "INSERT INTO checkpoint_migrations (v) VALUES (%s)", (v,)
            )


# ---------------------------------------------------------------------------
# AIOMySQL57Saver  ──  single-connection, auto-reconnect
# ---------------------------------------------------------------------------

class AIOMySQL57Saver(BaseMySQLSaver57):
    """
    Async checkpoint saver backed by a single aiomysql connection.

    Features
    --------
    - Auto-reconnect on ``Not connected`` / ``Lost connection`` errors
      (one retry per operation).
    - Thread-safe: ``asyncio.Lock`` serialises all cursor access.
    - Connection params are stored at construction time so reconnect
      can open a fresh socket without requiring user intervention.

    Limitations
    -----------
    - One connection ⟹ one in-flight operation at a time.
      For higher concurrency use :class:`AIOMySQL57PoolSaver`.
    """

    conn: aiomysql.Connection
    lock: asyncio.Lock
    _conn_params: dict[str, Any] | None
    _reconnect_lock: asyncio.Lock

    def __init__(
        self,
        conn: aiomysql.Connection,
        serde: SerializerProtocol | None = None,
        *,
        _conn_params: dict[str, Any] | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.conn           = conn
        self.lock           = asyncio.Lock()
        self._conn_params   = _conn_params
        self._reconnect_lock = asyncio.Lock()

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @staticmethod
    def parse_conn_string(conn_string: str) -> dict[str, Any]:
        """Parse a MySQL URL into ``aiomysql.connect()`` kwargs."""
        return _parse_conn_string(conn_string)

    @classmethod
    @asynccontextmanager
    async def from_conn_string(
        cls,
        conn_string: str,
        serde: SerializerProtocol | None = None,
    ) -> AsyncIterator[Self]:
        """
        Create an :class:`AIOMySQL57Saver` from a connection string.

        The connection params are stored internally so that the saver
        can reconnect automatically if the TCP link is lost.

        Args:
            conn_string: ``mysql://user:password@host:port/database``
            serde:       optional custom serialiser

        Yields:
            :class:`AIOMySQL57Saver`
        """
        conn_params = _parse_conn_string(conn_string)
        conn = await aiomysql.connect(**conn_params, autocommit=True)
        try:
            yield cls(conn=conn, serde=serde, _conn_params=conn_params)
        finally:
            conn.close()

    async def _reconnect(self) -> None:
        """
        Replace ``self.conn`` with a fresh connection (idempotent under lock).

        Multiple coroutines may detect a dead connection simultaneously.
        The inner ``_reconnect_lock`` ensures only one actually opens a new
        socket; the others return after it completes.
        """
        if self._conn_params is None:
            raise RuntimeError(
                "Cannot auto-reconnect: connection params were not stored. "
                "Use AIOMySQL57Saver.from_conn_string() to enable reconnect."
            )
        async with self._reconnect_lock:
            if not self.conn.closed:
                return  # already reconnected by another coroutine
            logger.warning("[AIOMySQL57Saver] Connection lost — reconnecting…")
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = await aiomysql.connect(
                **self._conn_params, autocommit=True
            )
            logger.info("[AIOMySQL57Saver] Reconnected successfully.")

    @asynccontextmanager
    async def _cursor(
        self, *, pipeline: bool = False
    ) -> AsyncIterator[aiomysql.DictCursor]:
        """
        Yield an ``aiomysql.DictCursor``, optionally in a transaction.

        The ``asyncio.Lock`` ensures only one coroutine uses the single
        underlying connection at a time.
        """
        async with self.lock:
            if pipeline:
                await self.conn.begin()
                try:
                    async with self.conn.cursor(aiomysql.DictCursor) as cur:
                        yield cur
                    await self.conn.commit()
                except Exception:
                    await self.conn.rollback()
                    raise
            else:
                async with self.conn.cursor(aiomysql.DictCursor) as cur:
                    yield cur

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Create tables and run pending migrations (idempotent)."""
        await _run_migrations(self, self._cursor)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def aget_tuple(
        self,
        config: RunnableConfig,
        *,
        _retry: bool = False,
    ) -> CheckpointTuple | None:
        """
        Retrieve one checkpoint (async).

        Automatically retries once after reconnecting on connection errors.
        """
        try:
            return await self._aget_tuple_impl(config)
        except Exception as exc:
            if not _retry and _is_connection_error(exc) and self._conn_params:
                logger.warning(
                    "[aget_tuple] Connection error (%s) — reconnecting…", exc
                )
                await self._reconnect()
                return await self.aget_tuple(config, _retry=True)
            raise

    async def _aget_tuple_impl(
        self, config: RunnableConfig
    ) -> CheckpointTuple | None:
        thread_id     = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute

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

        # Keep all three queries inside one cursor context (one lock acq.)
        async with self._cursor() as cur:
            await cur.execute(query, args)
            row = await cur.fetchone()
            if row is None:
                return None

            checkpoint = json.loads(row["checkpoint"])
            channel_versions: dict[str, str] = checkpoint.get(
                "channel_versions", {}
            )

            blob_rows: list[dict] = []
            if channel_versions:
                channels = list(channel_versions.keys())
                await cur.execute(
                    self._select_blobs_sql(channels),
                    (thread_id, ns_hash, *channels),
                )
                blob_rows = await cur.fetchall()

            await cur.execute(
                SELECT_WRITES_SQL,
                (thread_id, ns_hash, row["checkpoint_id"]),
            )
            write_rows = await cur.fetchall()

        return self._build_checkpoint_tuple(row, checkpoint, blob_rows, write_rows)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: MetadataInput = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
        _retry: bool = False,
    ) -> AsyncIterator[CheckpointTuple]:
        """
        Async-iterate over checkpoints, newest first.

        Automatically retries once after reconnecting if the first DB query
        fails due to a connection error.
        """
        # Phase 1: fetch all main rows (may raise — handled for retry)
        try:
            rows = await self._fetch_list_rows(config, filter, before, limit)
        except Exception as exc:
            if not _retry and _is_connection_error(exc) and self._conn_params:
                logger.warning(
                    "[alist] Connection error (%s) — reconnecting…", exc
                )
                await self._reconnect()
                async for item in self.alist(
                    config,
                    filter=filter,
                    before=before,
                    limit=limit,
                    _retry=True,
                ):
                    yield item
                return
            raise

        # Phase 2: enrich each row with blobs + writes, then yield
        for row in rows:
            checkpoint = json.loads(row["checkpoint"])
            channel_versions: dict[str, str] = checkpoint.get(
                "channel_versions", {}
            )
            thread_id     = row["thread_id"]
            checkpoint_ns = row["checkpoint_ns"]
            checkpoint_id = row["checkpoint_id"]
            ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute

            async with self._cursor() as cur:
                blob_rows: list[dict] = []
                if channel_versions:
                    channels = list(channel_versions.keys())
                    await cur.execute(
                        self._select_blobs_sql(channels),
                        (thread_id, ns_hash, *channels),
                    )
                    blob_rows = await cur.fetchall()

                await cur.execute(
                    SELECT_WRITES_SQL,
                    (thread_id, ns_hash, checkpoint_id),
                )
                write_rows = await cur.fetchall()

            yield self._build_checkpoint_tuple(
                row, checkpoint, blob_rows, write_rows
            )

    async def _fetch_list_rows(
        self,
        config: RunnableConfig | None,
        filter: MetadataInput,
        before: RunnableConfig | None,
        limit: int | None,
    ) -> list[dict]:
        where, args = self._search_where(config, filter, before)
        query = self._select_sql(where) + " ORDER BY c.checkpoint_id DESC"
        if limit is not None:
            query += f" LIMIT {int(limit)}"
        async with self._cursor() as cur:
            await cur.execute(query, args)
            return await cur.fetchall()

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
        *,
        _retry: bool = False,
    ) -> RunnableConfig:
        """
        Persist a checkpoint (async).

        Auto-reconnects and retries once on connection errors.
        """
        try:
            return await self._aput_impl(config, checkpoint, metadata, new_versions)
        except Exception as exc:
            if not _retry and _is_connection_error(exc) and self._conn_params:
                logger.warning(
                    "[aput] Connection error (%s) — reconnecting…", exc
                )
                await self._reconnect()
                return await self.aput(
                    config, checkpoint, metadata, new_versions, _retry=True
                )
            raise

    async def _aput_impl(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        configurable  = config["configurable"].copy()
        thread_id     = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")
        checkpoint_id = configurable.pop("checkpoint_id", None)
        ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute once

        copy = checkpoint.copy()
        copy["channel_values"] = copy["channel_values"].copy()

        blob_values: dict[str, Any] = {}
        for k, v in checkpoint["channel_values"].items():
            if not (v is None or isinstance(v, (str, int, float, bool))):
                blob_values[k] = copy["channel_values"].pop(k)

        async with self._cursor(pipeline=True) as cur:
            blob_versions = {
                k: v for k, v in new_versions.items() if k in blob_values
            }
            if blob_versions:
                await cur.executemany(
                    UPSERT_CHECKPOINT_BLOBS_SQL,
                    await asyncio.to_thread(
                        self._dump_blobs,
                        thread_id,
                        checkpoint_ns,
                        blob_values,
                        blob_versions,
                    ),
                )
            await cur.execute(
                UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    ns_hash,            # raw bytes → BINARY(16)
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

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: list[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Persist intermediate writes for a checkpoint (async)."""
        query = (
            UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else INSERT_CHECKPOINT_WRITES_SQL
        )
        params = await asyncio.to_thread(
            self._dump_writes,
            config["configurable"]["thread_id"],
            config["configurable"]["checkpoint_ns"],
            config["configurable"]["checkpoint_id"],
            task_id,
            task_path,
            writes,
        )
        async with self._cursor(pipeline=True) as cur:
            await cur.executemany(query, params)

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints, blobs, and writes for *thread_id* (async)."""
        async with self._cursor(pipeline=True) as cur:
            await cur.execute(
                "DELETE FROM checkpoints WHERE thread_id = %s", (thread_id,)
            )
            await cur.execute(
                "DELETE FROM checkpoint_blobs WHERE thread_id = %s", (thread_id,)
            )
            await cur.execute(
                "DELETE FROM checkpoint_writes WHERE thread_id = %s", (thread_id,)
            )


# ---------------------------------------------------------------------------
# AIOMySQL57PoolSaver  ──  connection pool, no lock needed
# ---------------------------------------------------------------------------

class AIOMySQL57PoolSaver(BaseMySQLSaver57):
    """
    Async checkpoint saver backed by an ``aiomysql`` connection pool.

    Each operation acquires its own connection from the pool and releases
    it when done.  No ``asyncio.Lock`` is needed because concurrent
    coroutines each get their own connection.

    This is the **recommended** saver for production async services.

    Example
    -------
    ::

        async with AIOMySQL57PoolSaver.from_conn_string(
            DB_URI,
            minsize=2,
            maxsize=20,
        ) as cp:
            await cp.setup()
            graph = build_graph(checkpointer=cp)
    """

    pool: aiomysql.Pool

    def __init__(
        self,
        pool: aiomysql.Pool,
        serde: SerializerProtocol | None = None,
    ) -> None:
        super().__init__(serde=serde)
        self.pool = pool

    # ------------------------------------------------------------------
    # Connection helpers
    # ------------------------------------------------------------------

    @classmethod
    @asynccontextmanager
    async def from_pool(
        cls,
        pool: aiomysql.Pool,
        serde: SerializerProtocol | None = None,
    ) -> AsyncIterator[Self]:
        """
        Create a saver from an **existing** pool (pool lifetime is external).

        Useful when the pool is shared with other parts of the application.
        """
        yield cls(pool=pool, serde=serde)

    @classmethod
    @asynccontextmanager
    async def from_conn_string(
        cls,
        conn_string: str,
        serde: SerializerProtocol | None = None,
        *,
        minsize: int = 1,
        maxsize: int = 10,
    ) -> AsyncIterator[Self]:
        """
        Create a pool-backed saver from a connection string.

        The pool is created on entry and closed on exit.

        Args:
            conn_string: ``mysql://user:password@host:port/database``
            serde:       optional custom serialiser
            minsize:     minimum number of idle connections in the pool
            maxsize:     maximum number of connections the pool will open
        """
        params = _parse_conn_string(conn_string)
        pool = await aiomysql.create_pool(
            **params,
            autocommit=True,
            minsize=minsize,
            maxsize=maxsize,
        )
        try:
            yield cls(pool=pool, serde=serde)
        finally:
            pool.close()
            await pool.wait_closed()

    @asynccontextmanager
    async def _acquire(
        self, *, pipeline: bool = False
    ) -> AsyncIterator[aiomysql.DictCursor]:
        """
        Acquire a connection from the pool and yield a DictCursor.

        Wraps the cursor in a BEGIN / COMMIT transaction when
        ``pipeline=True``.
        """
        async with self.pool.acquire() as conn:
            if pipeline:
                await conn.begin()
                try:
                    async with conn.cursor(aiomysql.DictCursor) as cur:
                        yield cur
                    await conn.commit()
                except Exception:
                    await conn.rollback()
                    raise
            else:
                async with conn.cursor(aiomysql.DictCursor) as cur:
                    yield cur

    # ------------------------------------------------------------------
    # Schema management
    # ------------------------------------------------------------------

    async def setup(self) -> None:
        """Create tables and run pending migrations (idempotent)."""
        await _run_migrations(self, self._acquire)

    # ------------------------------------------------------------------
    # Read operations
    # ------------------------------------------------------------------

    async def aget_tuple(self, config: RunnableConfig) -> CheckpointTuple | None:
        """Retrieve one checkpoint (async)."""
        thread_id     = config["configurable"]["thread_id"]
        checkpoint_id = get_checkpoint_id(config)
        checkpoint_ns = config["configurable"].get("checkpoint_ns", "")
        ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute

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

        # All three queries share one pool connection for efficiency
        async with self._acquire() as cur:
            await cur.execute(query, args)
            row = await cur.fetchone()
            if row is None:
                return None

            checkpoint = json.loads(row["checkpoint"])
            channel_versions: dict[str, str] = checkpoint.get(
                "channel_versions", {}
            )

            blob_rows: list[dict] = []
            if channel_versions:
                channels = list(channel_versions.keys())
                await cur.execute(
                    self._select_blobs_sql(channels),
                    (thread_id, ns_hash, *channels),
                )
                blob_rows = await cur.fetchall()

            await cur.execute(
                SELECT_WRITES_SQL,
                (thread_id, ns_hash, row["checkpoint_id"]),
            )
            write_rows = await cur.fetchall()

        return self._build_checkpoint_tuple(row, checkpoint, blob_rows, write_rows)

    async def alist(
        self,
        config: RunnableConfig | None,
        *,
        filter: MetadataInput = None,
        before: RunnableConfig | None = None,
        limit: int | None = None,
    ) -> AsyncIterator[CheckpointTuple]:
        """Async-iterate over checkpoints, newest first."""
        where, args = self._search_where(config, filter, before)
        query = self._select_sql(where) + " ORDER BY c.checkpoint_id DESC"
        if limit is not None:
            query += f" LIMIT {int(limit)}"

        # Fetch main rows first, then enrich each with blobs+writes
        async with self._acquire() as cur:
            await cur.execute(query, args)
            rows = await cur.fetchall()

        for row in rows:
            checkpoint = json.loads(row["checkpoint"])
            channel_versions: dict[str, str] = checkpoint.get(
                "channel_versions", {}
            )
            thread_id     = row["thread_id"]
            checkpoint_ns = row["checkpoint_ns"]
            checkpoint_id = row["checkpoint_id"]
            ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute

            async with self._acquire() as cur:
                blob_rows: list[dict] = []
                if channel_versions:
                    channels = list(channel_versions.keys())
                    await cur.execute(
                        self._select_blobs_sql(channels),
                        (thread_id, ns_hash, *channels),
                    )
                    blob_rows = await cur.fetchall()

                await cur.execute(
                    SELECT_WRITES_SQL,
                    (thread_id, ns_hash, checkpoint_id),
                )
                write_rows = await cur.fetchall()

            yield self._build_checkpoint_tuple(
                row, checkpoint, blob_rows, write_rows
            )

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    async def aput(
        self,
        config: RunnableConfig,
        checkpoint: Checkpoint,
        metadata: CheckpointMetadata,
        new_versions: ChannelVersions,
    ) -> RunnableConfig:
        """Persist a checkpoint (async)."""
        configurable  = config["configurable"].copy()
        thread_id     = configurable.pop("thread_id")
        checkpoint_ns = configurable.pop("checkpoint_ns")
        checkpoint_id = configurable.pop("checkpoint_id", None)
        ns_hash       = _md5_hash(checkpoint_ns)  # pre-compute once

        copy = checkpoint.copy()
        copy["channel_values"] = copy["channel_values"].copy()

        blob_values: dict[str, Any] = {}
        for k, v in checkpoint["channel_values"].items():
            if not (v is None or isinstance(v, (str, int, float, bool))):
                blob_values[k] = copy["channel_values"].pop(k)

        async with self._acquire(pipeline=True) as cur:
            blob_versions = {
                k: v for k, v in new_versions.items() if k in blob_values
            }
            if blob_versions:
                await cur.executemany(
                    UPSERT_CHECKPOINT_BLOBS_SQL,
                    await asyncio.to_thread(
                        self._dump_blobs,
                        thread_id,
                        checkpoint_ns,
                        blob_values,
                        blob_versions,
                    ),
                )
            await cur.execute(
                UPSERT_CHECKPOINTS_SQL,
                (
                    thread_id,
                    checkpoint_ns,
                    ns_hash,            # raw bytes → BINARY(16)
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

    async def aput_writes(
        self,
        config: RunnableConfig,
        writes: list[tuple[str, Any]],
        task_id: str,
        task_path: str = "",
    ) -> None:
        """Persist intermediate writes for a checkpoint (async)."""
        query = (
            UPSERT_CHECKPOINT_WRITES_SQL
            if all(w[0] in WRITES_IDX_MAP for w in writes)
            else INSERT_CHECKPOINT_WRITES_SQL
        )
        params = await asyncio.to_thread(
            self._dump_writes,
            config["configurable"]["thread_id"],
            config["configurable"]["checkpoint_ns"],
            config["configurable"]["checkpoint_id"],
            task_id,
            task_path,
            writes,
        )
        async with self._acquire(pipeline=True) as cur:
            await cur.executemany(query, params)

    async def adelete_thread(self, thread_id: str) -> None:
        """Delete all checkpoints, blobs, and writes for *thread_id* (async)."""
        async with self._acquire(pipeline=True) as cur:
            await cur.execute(
                "DELETE FROM checkpoints WHERE thread_id = %s", (thread_id,)
            )
            await cur.execute(
                "DELETE FROM checkpoint_blobs WHERE thread_id = %s", (thread_id,)
            )
            await cur.execute(
                "DELETE FROM checkpoint_writes WHERE thread_id = %s", (thread_id,)
            )


__all__ = ["AIOMySQL57Saver", "AIOMySQL57PoolSaver"]
