"""
Tests for AIOMySQL57Saver and AIOMySQL57PoolSaver (async, aiomysql driver).

Run against a real MySQL 5.7 instance (same setup as sync tests):

    pytest tests/test_mysql57_async.py -v

Environment variables:
    MYSQL57_HOST      default: localhost
    MYSQL57_PORT      default: 3306
    MYSQL57_USER      default: root
    MYSQL57_PASSWORD  default: test
    MYSQL57_DB        default: testdb
"""

from __future__ import annotations

import os
import uuid
from typing import Any

import pytest
import pytest_asyncio

# Skip entire module if aiomysql is not installed
aiomysql = pytest.importorskip("aiomysql")
pytestmark = pytest.mark.asyncio

from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    empty_checkpoint,
)
from langgraph.checkpoint.mysql57 import AIOMySQL57Saver, AIOMySQL57PoolSaver


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _db_uri() -> str:
    host = os.getenv("MYSQL57_HOST", "localhost")
    port = os.getenv("MYSQL57_PORT", "3306")
    user = os.getenv("MYSQL57_USER", "root")
    pw   = os.getenv("MYSQL57_PASSWORD", "test")
    db   = os.getenv("MYSQL57_DB", "testdb")
    return f"mysql://{user}:{pw}@{host}:{port}/{db}"


def _new_thread() -> str:
    return str(uuid.uuid4())


def _make_checkpoint(values: dict[str, Any] | None = None) -> Checkpoint:
    cp = empty_checkpoint()
    if values:
        cp["channel_values"].update(values)
    return cp


def _cfg(thread_id: str, *, ns: str = "", checkpoint_id: str | None = None) -> dict:
    conf: dict = {
        "configurable": {
            "thread_id":     thread_id,
            "checkpoint_ns": ns,
        }
    }
    if checkpoint_id:
        conf["configurable"]["checkpoint_id"] = checkpoint_id
    return conf


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest_asyncio.fixture(scope="module")
async def single_conn_saver():
    """AIOMySQL57Saver (single connection)."""
    try:
        async with AIOMySQL57Saver.from_conn_string(_db_uri()) as cp:
            await cp.setup()
            yield cp
    except Exception as exc:
        pytest.skip(f"MySQL 5.7 not available: {exc}")


@pytest_asyncio.fixture(scope="module")
async def pool_saver():
    """AIOMySQL57PoolSaver (connection pool)."""
    try:
        async with AIOMySQL57PoolSaver.from_conn_string(
            _db_uri(), minsize=1, maxsize=5
        ) as cp:
            await cp.setup()
            yield cp
    except Exception as exc:
        pytest.skip(f"MySQL 5.7 not available: {exc}")


# ---------------------------------------------------------------------------
# Parametrise tests over both saver types
# ---------------------------------------------------------------------------

@pytest.fixture(params=["single_conn_saver", "pool_saver"])
def saver(request):
    return request.getfixturevalue(request.param)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSetupAsync:
    async def test_setup_idempotent(self, single_conn_saver: AIOMySQL57Saver) -> None:
        await single_conn_saver.setup()   # second call — must not raise

    async def test_migrations_recorded(self, single_conn_saver: AIOMySQL57Saver) -> None:
        async with single_conn_saver._cursor() as cur:
            await cur.execute(
                "SELECT MAX(v) AS max_v FROM checkpoint_migrations"
            )
            row = await cur.fetchone()
        assert row is not None
        assert row["max_v"] >= 7


class TestAgetTupleEmpty:
    async def test_returns_none_for_unknown_thread(self, saver) -> None:
        result = await saver.aget_tuple(_cfg("unknown-" + _new_thread()))
        assert result is None

    async def test_returns_none_for_unknown_checkpoint_id(self, saver) -> None:
        result = await saver.aget_tuple(
            _cfg(_new_thread(), checkpoint_id=str(uuid.uuid4()))
        )
        assert result is None


class TestAputAndAget:
    async def test_aput_returns_config_with_checkpoint_id(self, saver) -> None:
        thread = _new_thread()
        cp = _make_checkpoint({"x": 42})
        saved = await saver.aput(
            _cfg(thread),
            cp,
            {"source": "input", "step": 0, "writes": {}},
            {},
        )
        assert saved["configurable"]["thread_id"] == thread
        assert saved["configurable"]["checkpoint_id"] == cp["id"]

    async def test_aget_tuple_latest(self, saver) -> None:
        thread = _new_thread()
        cp = _make_checkpoint()
        await saver.aput(
            _cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {}
        )
        result = await saver.aget_tuple(_cfg(thread))
        assert result is not None
        assert result.config["configurable"]["checkpoint_id"] == cp["id"]

    async def test_aget_tuple_by_id(self, saver) -> None:
        thread = _new_thread()
        cp = _make_checkpoint()
        await saver.aput(
            _cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {}
        )
        result = await saver.aget_tuple(_cfg(thread, checkpoint_id=cp["id"]))
        assert result is not None
        assert result.checkpoint["id"] == cp["id"]

    async def test_metadata_roundtrip(self, saver) -> None:
        thread = _new_thread()
        meta: CheckpointMetadata = {
            "source": "loop",
            "step":   7,
            "writes": {"node": "output"},
        }
        cp = _make_checkpoint()
        await saver.aput(_cfg(thread), cp, meta, {})
        result = await saver.aget_tuple(_cfg(thread))
        assert result is not None
        assert result.metadata["step"] == 7

    async def test_latest_checkpoint_returned(self, saver) -> None:
        thread = _new_thread()
        cp1 = _make_checkpoint()
        cp2 = _make_checkpoint()
        await saver.aput(
            _cfg(thread), cp1, {"source": "input", "step": 1, "writes": {}}, {}
        )
        await saver.aput(
            _cfg(thread, checkpoint_id=cp1["id"]),
            cp2,
            {"source": "loop", "step": 2, "writes": {}},
            {},
        )
        result = await saver.aget_tuple(_cfg(thread))
        assert result is not None
        assert result.checkpoint["id"] == cp2["id"]


class TestNamespaceAsync:
    async def test_different_namespaces_are_independent(self, saver) -> None:
        thread = _new_thread()
        cp_a = _make_checkpoint()
        cp_b = _make_checkpoint()
        await saver.aput(
            _cfg(thread, ns="nsX"), cp_a, {"source": "input", "step": 0, "writes": {}}, {}
        )
        await saver.aput(
            _cfg(thread, ns="nsY"), cp_b, {"source": "input", "step": 0, "writes": {}}, {}
        )

        r_a = await saver.aget_tuple(_cfg(thread, ns="nsX"))
        r_b = await saver.aget_tuple(_cfg(thread, ns="nsY"))
        assert r_a is not None and r_b is not None
        assert r_a.checkpoint["id"] == cp_a["id"]
        assert r_b.checkpoint["id"] == cp_b["id"]
        assert r_a.checkpoint["id"] != r_b.checkpoint["id"]


class TestAlist:
    async def test_alist_all_for_thread(self, saver) -> None:
        thread = _new_thread()
        for i in range(3):
            cp = _make_checkpoint()
            await saver.aput(
                _cfg(thread), cp, {"source": "loop", "step": i, "writes": {}}, {}
            )

        results = [item async for item in await saver.alist(_cfg(thread))]
        assert len(results) == 3

    async def test_alist_newest_first(self, saver) -> None:
        thread = _new_thread()
        for i in range(4):
            cp = _make_checkpoint()
            await saver.aput(
                _cfg(thread), cp, {"source": "loop", "step": i, "writes": {}}, {}
            )

        results = [item async for item in await saver.alist(_cfg(thread))]
        steps = [r.metadata["step"] for r in results]
        assert steps == sorted(steps, reverse=True)

    async def test_alist_with_limit(self, saver) -> None:
        thread = _new_thread()
        for i in range(5):
            cp = _make_checkpoint()
            await saver.aput(
                _cfg(thread), cp, {"source": "loop", "step": i, "writes": {}}, {}
            )

        results = [item async for item in await saver.alist(_cfg(thread), limit=2)]
        assert len(results) == 2

    async def test_alist_with_metadata_filter(self, saver) -> None:
        thread = _new_thread()
        for src in ("input", "loop", "input"):
            cp = _make_checkpoint()
            await saver.aput(
                _cfg(thread), cp, {"source": src, "step": 0, "writes": {}}, {}
            )

        results = [
            item
            async for item in await saver.alist(
                _cfg(thread), filter={"source": "input"}
            )
        ]
        assert len(results) == 2
        for r in results:
            assert r.metadata["source"] == "input"

    async def test_alist_empty_for_unknown_thread(self, saver) -> None:
        results = [
            item
            async for item in await saver.alist(_cfg("no-thread-" + _new_thread()))
        ]
        assert results == []


class TestAputWrites:
    async def test_aput_writes_roundtrip(self, saver) -> None:
        thread    = _new_thread()
        cp        = _make_checkpoint()
        saved_cfg = await saver.aput(
            _cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {}
        )
        write_cfg = {
            "configurable": {
                **saved_cfg["configurable"],
                "checkpoint_id": cp["id"],
            }
        }
        await saver.aput_writes(
            write_cfg,
            [("result", {"answer": 42})],
            task_id="t-1",
        )

        result = await saver.aget_tuple(_cfg(thread))
        assert result is not None
        assert len(result.pending_writes) == 1
        _, channel, value = result.pending_writes[0]
        assert channel == "result"
        assert value == {"answer": 42}

    async def test_aput_writes_multiple(self, saver) -> None:
        thread    = _new_thread()
        cp        = _make_checkpoint()
        saved_cfg = await saver.aput(
            _cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {}
        )
        write_cfg = {
            "configurable": {
                **saved_cfg["configurable"],
                "checkpoint_id": cp["id"],
            }
        }
        await saver.aput_writes(
            write_cfg,
            [("ch_a", "hello"), ("ch_b", [1, 2, 3])],
            task_id="t-2",
        )

        result = await saver.aget_tuple(_cfg(thread))
        assert result is not None
        assert len(result.pending_writes) == 2


class TestAdeleteThread:
    async def test_adelete_thread_removes_all_data(self, saver) -> None:
        thread = _new_thread()
        for i in range(2):
            cp = _make_checkpoint()
            await saver.aput(
                _cfg(thread), cp, {"source": "loop", "step": i, "writes": {}}, {}
            )

        assert await saver.aget_tuple(_cfg(thread)) is not None

        await saver.adelete_thread(thread)

        assert await saver.aget_tuple(_cfg(thread)) is None
        results = [item async for item in await saver.alist(_cfg(thread))]
        assert results == []

    async def test_adelete_thread_does_not_affect_others(self, saver) -> None:
        t1 = _new_thread()
        t2 = _new_thread()
        cp1 = _make_checkpoint()
        cp2 = _make_checkpoint()
        await saver.aput(_cfg(t1), cp1, {"source": "input", "step": 0, "writes": {}}, {})
        await saver.aput(_cfg(t2), cp2, {"source": "input", "step": 0, "writes": {}}, {})

        await saver.adelete_thread(t1)

        assert await saver.aget_tuple(_cfg(t1)) is None
        assert await saver.aget_tuple(_cfg(t2)) is not None


class TestParentConfigAsync:
    async def test_parent_config_links(self, saver) -> None:
        thread = _new_thread()
        cp1 = _make_checkpoint()
        cp2 = _make_checkpoint()

        await saver.aput(
            _cfg(thread), cp1, {"source": "input", "step": 0, "writes": {}}, {}
        )
        await saver.aput(
            _cfg(thread, checkpoint_id=cp1["id"]),
            cp2,
            {"source": "loop", "step": 1, "writes": {}},
            {},
        )

        result = await saver.aget_tuple(_cfg(thread))
        assert result is not None
        assert result.parent_config is not None
        assert result.parent_config["configurable"]["checkpoint_id"] == cp1["id"]


class TestPoolSaverSpecific:
    """Tests specific to the pool-based saver."""

    async def test_concurrent_puts(self, pool_saver: AIOMySQL57PoolSaver) -> None:
        """Multiple coroutines should be able to write concurrently."""
        import asyncio

        threads = [_new_thread() for _ in range(5)]

        async def put_checkpoint(thread_id: str) -> None:
            cp = _make_checkpoint({"t": thread_id})
            await pool_saver.aput(
                _cfg(thread_id),
                cp,
                {"source": "input", "step": 0, "writes": {}},
                {},
            )

        await asyncio.gather(*[put_checkpoint(t) for t in threads])

        for t in threads:
            result = await pool_saver.aget_tuple(_cfg(t))
            assert result is not None

    async def test_concurrent_gets(self, pool_saver: AIOMySQL57PoolSaver) -> None:
        """Concurrent reads should return correct data."""
        import asyncio

        threads = [_new_thread() for _ in range(5)]
        for t in threads:
            cp = _make_checkpoint()
            await pool_saver.aput(
                _cfg(t), cp, {"source": "input", "step": 0, "writes": {}}, {}
            )

        results = await asyncio.gather(
            *[pool_saver.aget_tuple(_cfg(t)) for t in threads]
        )
        assert all(r is not None for r in results)
