"""
langgraph.checkpoint.mysql57
============================

MySQL 5.7 / PolarDB-X compatible LangGraph checkpoint savers.

This package is a **standalone** implementation that does NOT depend on
``langgraph-checkpoint-mysql``.  It inherits directly from
``langgraph.checkpoint.base.BaseCheckpointSaver`` and rewrites all SQL to
be compatible with MySQL 5.7 (no JSON_TABLE, JSON_ARRAYAGG, WITH CTE, or
``VALUES(...) AS new`` syntax).

Classes
-------
MySQL57Saver
    Synchronous saver using ``pymysql``.

AIOMySQL57Saver
    Asynchronous saver using ``aiomysql`` (single connection, auto-reconnect).

AIOMySQL57PoolSaver
    Asynchronous saver using an ``aiomysql`` connection pool.
    Recommended for production.

Quick-start
-----------
**Synchronous**::

    from langgraph.checkpoint.mysql57 import MySQL57Saver

    with MySQL57Saver.from_conn_string("mysql://user:pw@host/db") as cp:
        cp.setup()          # run once; idempotent
        graph = build_graph(checkpointer=cp)

**Asynchronous (single connection)**::

    from langgraph.checkpoint.mysql57 import AIOMySQL57Saver

    async with AIOMySQL57Saver.from_conn_string("mysql://user:pw@host/db") as cp:
        await cp.setup()
        result = await graph.ainvoke(...)

**Asynchronous (connection pool — recommended for production)**::

    from langgraph.checkpoint.mysql57 import AIOMySQL57PoolSaver

    async with AIOMySQL57PoolSaver.from_conn_string(
        "mysql://user:pw@host/db",
        minsize=2,
        maxsize=20,
    ) as cp:
        await cp.setup()
        result = await graph.ainvoke(...)

Schema
------
Run ``cp.setup()`` (or ``await cp.setup()``) to create the four tables:

- ``checkpoint_migrations`` — migration version bookkeeping
- ``checkpoints``           — main checkpoint data
- ``checkpoint_blobs``      — complex channel values (messages, tool results…)
- ``checkpoint_writes``     — pending writes for human-in-the-loop / retry

Alternatively, apply ``mysql57_schema.sql`` manually::

    mysql -u user -p mydb < mysql57_schema.sql
"""

__version__ = "1.0.0"

__all__ = [
    "MySQL57Saver",
    "AIOMySQL57Saver",
    "AIOMySQL57PoolSaver",
]


def __getattr__(name: str):  # type: ignore[return]
    """
    Lazy attribute access so that importing this package does not require
    both ``pymysql`` and ``aiomysql`` to be installed simultaneously.

    - ``MySQL57Saver``       requires ``pymysql``
    - ``AIOMySQL57Saver``    requires ``aiomysql``
    - ``AIOMySQL57PoolSaver`` requires ``aiomysql``
    """
    if name == "MySQL57Saver":
        from langgraph.checkpoint.mysql57.sync import MySQL57Saver
        return MySQL57Saver
    if name in ("AIOMySQL57Saver", "AIOMySQL57PoolSaver"):
        from langgraph.checkpoint.mysql57.aio import AIOMySQL57Saver, AIOMySQL57PoolSaver
        return locals()[name]
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
