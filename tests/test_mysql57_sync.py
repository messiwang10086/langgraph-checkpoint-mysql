"""
Tests for MySQL57Saver (synchronous, pymysql driver).

Run against a real MySQL 5.7 instance:

    # start MySQL 5.7 via docker
    docker run -d --name mysql57 \
        -e MYSQL_ROOT_PASSWORD=test \
        -e MYSQL_DATABASE=testdb \
        -p 3306:3306 \
        mysql:5.7

    # run tests
    pytest tests/test_mysql57_sync.py -v

Environment variables (all optional, defaults match docker command above):
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

# Skip entire module if pymysql is not installed
pymysql = pytest.importorskip("pymysql")

from langgraph.checkpoint.base import (
    Checkpoint,
    CheckpointMetadata,
    create_checkpoint,
    empty_checkpoint,
)
from langgraph.checkpoint.mysql57 import MySQL57Saver


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

@pytest.fixture(scope="module")
def saver():
    """Module-scoped saver — setup() called once per test run."""
    try:
        with MySQL57Saver.from_conn_string(_db_uri()) as cp:
            cp.setup()
            yield cp
    except Exception as exc:
        pytest.skip(f"MySQL 5.7 not available: {exc}")


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestSetup:
    def test_tables_exist(self, saver: MySQL57Saver) -> None:
        """setup() must create all four tables."""
        with saver._cursor() as cur:
            cur.execute("SHOW TABLES LIKE 'checkpoint%'")
            tables = {row[f"Tables_in_{os.getenv('MYSQL57_DB','testdb')} (checkpoint%)"]
                      for row in cur.fetchall()}
        assert "checkpoints"          in tables
        assert "checkpoint_blobs"     in tables
        assert "checkpoint_writes"    in tables
        assert "checkpoint_migrations" in tables

    def test_migrations_seeded(self, saver: MySQL57Saver) -> None:
        """Migration table must record at least 8 versions (0-7)."""
        with saver._cursor() as cur:
            cur.execute("SELECT MAX(v) AS max_v FROM checkpoint_migrations")
            row = cur.fetchone()
        assert row is not None
        assert row["max_v"] >= 7

    def test_setup_idempotent(self, saver: MySQL57Saver) -> None:
        """Calling setup() twice must not raise or duplicate rows."""
        saver.setup()   # second call


class TestGetTupleEmpty:
    def test_returns_none_for_unknown_thread(self, saver: MySQL57Saver) -> None:
        cfg = _cfg("nonexistent-thread-" + _new_thread())
        assert saver.get_tuple(cfg) is None

    def test_returns_none_for_unknown_checkpoint_id(self, saver: MySQL57Saver) -> None:
        cfg = _cfg(_new_thread(), checkpoint_id=str(uuid.uuid4()))
        assert saver.get_tuple(cfg) is None


class TestPutAndGet:
    def test_put_returns_config_with_checkpoint_id(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        cp = _make_checkpoint({"counter": 1})
        saved = saver.put(
            _cfg(thread),
            cp,
            {"source": "input", "step": 0, "writes": {}},
            {},
        )
        assert saved["configurable"]["thread_id"] == thread
        assert saved["configurable"]["checkpoint_id"] == cp["id"]

    def test_get_tuple_latest(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        cp = _make_checkpoint({"msg": "hello"})
        saved_cfg = saver.put(
            _cfg(thread),
            cp,
            {"source": "input", "step": 0, "writes": {}},
            {},
        )
        result = saver.get_tuple(_cfg(thread))
        assert result is not None
        assert result.config["configurable"]["thread_id"] == thread
        assert result.config["configurable"]["checkpoint_id"] == cp["id"]

    def test_get_tuple_by_id(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        cp = _make_checkpoint()
        saver.put(_cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {})

        result = saver.get_tuple(_cfg(thread, checkpoint_id=cp["id"]))
        assert result is not None
        assert result.checkpoint["id"] == cp["id"]

    def test_metadata_roundtrip(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        meta: CheckpointMetadata = {
            "source": "loop",
            "step":   5,
            "writes": {"agent": "done"},
        }
        cp = _make_checkpoint()
        saver.put(_cfg(thread), cp, meta, {})

        result = saver.get_tuple(_cfg(thread))
        assert result is not None
        assert result.metadata["step"] == 5
        assert result.metadata["writes"] == {"agent": "done"}

    def test_latest_checkpoint_returned(self, saver: MySQL57Saver) -> None:
        """get_tuple without checkpoint_id must return the newest checkpoint."""
        thread = _new_thread()
        cp1 = _make_checkpoint({"v": 1})
        cp2 = _make_checkpoint({"v": 2})

        saver.put(_cfg(thread), cp1, {"source": "input", "step": 1, "writes": {}}, {})
        saver.put(
            _cfg(thread, checkpoint_id=cp1["id"]),
            cp2,
            {"source": "loop", "step": 2, "writes": {}},
            {},
        )

        result = saver.get_tuple(_cfg(thread))
        assert result is not None
        assert result.checkpoint["id"] == cp2["id"]


class TestNamespace:
    def test_different_namespaces_are_independent(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        cp_a = _make_checkpoint()
        cp_b = _make_checkpoint()
        saver.put(_cfg(thread, ns="nsA"), cp_a, {"source": "input", "step": 0, "writes": {}}, {})
        saver.put(_cfg(thread, ns="nsB"), cp_b, {"source": "input", "step": 0, "writes": {}}, {})

        r_a = saver.get_tuple(_cfg(thread, ns="nsA"))
        r_b = saver.get_tuple(_cfg(thread, ns="nsB"))
        assert r_a is not None and r_b is not None
        assert r_a.checkpoint["id"] == cp_a["id"]
        assert r_b.checkpoint["id"] == cp_b["id"]
        assert r_a.checkpoint["id"] != r_b.checkpoint["id"]

    def test_empty_and_non_empty_ns_are_independent(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        cp_main  = _make_checkpoint()
        cp_child = _make_checkpoint()
        saver.put(_cfg(thread, ns=""),     cp_main,  {"source": "input", "step": 0, "writes": {}}, {})
        saver.put(_cfg(thread, ns="sub"),  cp_child, {"source": "input", "step": 0, "writes": {}}, {})

        r_main  = saver.get_tuple(_cfg(thread, ns=""))
        r_child = saver.get_tuple(_cfg(thread, ns="sub"))
        assert r_main is not None  and r_main.checkpoint["id"]  == cp_main["id"]
        assert r_child is not None and r_child.checkpoint["id"] == cp_child["id"]


class TestList:
    def test_list_all_for_thread(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        for i in range(3):
            cp = _make_checkpoint({"i": i})
            saver.put(
                _cfg(thread),
                cp,
                {"source": "loop", "step": i, "writes": {}},
                {},
            )

        results = list(saver.list(_cfg(thread)))
        assert len(results) == 3
        # Newest first
        steps = [r.metadata["step"] for r in results]
        assert steps == sorted(steps, reverse=True)

    def test_list_with_limit(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        for i in range(5):
            cp = _make_checkpoint()
            saver.put(
                _cfg(thread),
                cp,
                {"source": "loop", "step": i, "writes": {}},
                {},
            )

        results = list(saver.list(_cfg(thread), limit=2))
        assert len(results) == 2

    def test_list_with_metadata_filter(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        for src in ("input", "loop", "input"):
            cp = _make_checkpoint()
            saver.put(
                _cfg(thread),
                cp,
                {"source": src, "step": 0, "writes": {}},
                {},
            )

        results = list(saver.list(_cfg(thread), filter={"source": "input"}))
        assert len(results) == 2
        for r in results:
            assert r.metadata["source"] == "input"

    def test_list_returns_empty_for_unknown_thread(self, saver: MySQL57Saver) -> None:
        results = list(saver.list(_cfg("unknown-" + _new_thread())))
        assert results == []


class TestPutWrites:
    def test_put_writes_roundtrip(self, saver: MySQL57Saver) -> None:
        thread    = _new_thread()
        cp        = _make_checkpoint()
        saved_cfg = saver.put(
            _cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {}
        )
        write_cfg = {
            "configurable": {
                **saved_cfg["configurable"],
                "checkpoint_id": cp["id"],
            }
        }
        saver.put_writes(
            write_cfg,
            [("agent", {"result": "done"})],
            task_id="task-1",
        )

        result = saver.get_tuple(_cfg(thread))
        assert result is not None
        pending = result.pending_writes
        assert len(pending) == 1
        task_id, channel, value = pending[0]
        assert channel == "agent"
        assert value == {"result": "done"}

    def test_put_writes_multiple_channels(self, saver: MySQL57Saver) -> None:
        thread    = _new_thread()
        cp        = _make_checkpoint()
        saved_cfg = saver.put(
            _cfg(thread), cp, {"source": "input", "step": 0, "writes": {}}, {}
        )
        write_cfg = {
            "configurable": {
                **saved_cfg["configurable"],
                "checkpoint_id": cp["id"],
            }
        }
        saver.put_writes(
            write_cfg,
            [("ch1", "value1"), ("ch2", 42)],
            task_id="task-abc",
        )

        result = saver.get_tuple(_cfg(thread))
        assert result is not None
        assert len(result.pending_writes) == 2


class TestDeleteThread:
    def test_delete_thread_removes_all_data(self, saver: MySQL57Saver) -> None:
        thread = _new_thread()
        for i in range(2):
            cp = _make_checkpoint()
            saver.put(
                _cfg(thread),
                cp,
                {"source": "loop", "step": i, "writes": {}},
                {},
            )

        assert saver.get_tuple(_cfg(thread)) is not None

        saver.delete_thread(thread)

        assert saver.get_tuple(_cfg(thread)) is None
        assert list(saver.list(_cfg(thread))) == []

    def test_delete_thread_does_not_affect_other_threads(
        self, saver: MySQL57Saver
    ) -> None:
        t1 = _new_thread()
        t2 = _new_thread()
        cp1 = _make_checkpoint()
        cp2 = _make_checkpoint()
        saver.put(_cfg(t1), cp1, {"source": "input", "step": 0, "writes": {}}, {})
        saver.put(_cfg(t2), cp2, {"source": "input", "step": 0, "writes": {}}, {})

        saver.delete_thread(t1)

        assert saver.get_tuple(_cfg(t1)) is None
        assert saver.get_tuple(_cfg(t2)) is not None


class TestParentConfig:
    def test_parent_config_links_checkpoints(self, saver: MySQL57Saver) -> None:
        """Each successive checkpoint should reference its parent."""
        thread = _new_thread()
        cp1 = _make_checkpoint()
        cp2 = _make_checkpoint()

        saver.put(_cfg(thread), cp1, {"source": "input", "step": 0, "writes": {}}, {})
        saver.put(
            _cfg(thread, checkpoint_id=cp1["id"]),
            cp2,
            {"source": "loop", "step": 1, "writes": {}},
            {},
        )

        result = saver.get_tuple(_cfg(thread))
        assert result is not None
        assert result.parent_config is not None
        assert result.parent_config["configurable"]["checkpoint_id"] == cp1["id"]
