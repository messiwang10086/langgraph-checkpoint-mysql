"""
MySQL 5.7 兼容版本使用示例

展示如何在 MySQL 5.7 或 PolarDB-X 中使用 langgraph-checkpoint-mysql
"""

import os
from datetime import datetime

# 导入兼容版本
from mysql57_compat_saver import MySQL57Saver


def example_basic_usage():
    """基础使用示例"""
    print("\n=== 基础使用示例 ===\n")

    # 数据库连接
    DB_URI = os.getenv(
        "MYSQL_URI",
        "mysql://user:password@localhost:3306/dbname"
    )

    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        # 配置
        config = {
            "configurable": {
                "thread_id": "example-thread-1",
                "checkpoint_ns": ""
            }
        }

        # 创建 checkpoint
        checkpoint = {
            "v": 4,
            "ts": datetime.now().isoformat(),
            "id": "checkpoint-001",
            "channel_values": {
                "messages": "Hello, World!",
                "counter": 42
            },
            "channel_versions": {
                "messages": "1",
                "counter": "1"
            },
            "versions_seen": {},
        }

        # 保存
        print("保存 checkpoint...")
        checkpointer.put(config, checkpoint, {"step": 1}, {})
        print("✅ 保存成功")

        # 读取
        print("\n读取 checkpoint...")
        loaded = checkpointer.get(config)
        if loaded:
            print(f"✅ 读取成功: {loaded.checkpoint['id']}")
            print(f"   channel_values: {loaded.checkpoint['channel_values']}")
        else:
            print("❌ 未找到 checkpoint")


def example_list_checkpoints():
    """列出 checkpoints 示例"""
    print("\n=== 列出 Checkpoints 示例 ===\n")

    DB_URI = os.getenv("MYSQL_URI", "mysql://user:password@localhost:3306/dbname")

    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        # 创建多个 checkpoints
        thread_id = "example-thread-2"

        for i in range(3):
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": ""
                }
            }

            checkpoint = {
                "v": 4,
                "ts": datetime.now().isoformat(),
                "id": f"checkpoint-{i:03d}",
                "channel_values": {"step": i},
                "channel_versions": {"step": str(i)},
                "versions_seen": {},
            }

            checkpointer.put(config, checkpoint, {"iteration": i}, {})
            print(f"✅ 创建 checkpoint {i}")

        # 列出所有 checkpoints
        print(f"\n列出 thread_id='{thread_id}' 的所有 checkpoints:")
        list_config = {"configurable": {"thread_id": thread_id}}

        count = 0
        for cp_tuple in checkpointer.list(list_config, limit=10):
            count += 1
            print(f"  [{count}] {cp_tuple.checkpoint['id']} - step: {cp_tuple.checkpoint['channel_values'].get('step')}")

        print(f"\n共找到 {count} 个 checkpoints")


def example_with_langgraph():
    """在 LangGraph 中使用示例"""
    print("\n=== LangGraph 集成示例 ===\n")

    try:
        from langgraph.graph import StateGraph, END
        from typing import TypedDict
    except ImportError:
        print("❌ 需要安装 langgraph: pip install langgraph")
        return

    # 定义状态
    class State(TypedDict):
        messages: list[str]
        count: int

    # 定义节点
    def process_node(state: State) -> State:
        return {
            "messages": state["messages"] + ["processed"],
            "count": state["count"] + 1
        }

    # 创建 graph
    graph = StateGraph(State)
    graph.add_node("process", process_node)
    graph.set_entry_point("process")
    graph.add_edge("process", END)

    # 使用 MySQL 5.7 checkpointer
    DB_URI = os.getenv("MYSQL_URI", "mysql://user:password@localhost:3306/dbname")

    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        app = graph.compile(checkpointer=checkpointer)

        # 运行 graph
        config = {"configurable": {"thread_id": "langgraph-example"}}
        initial_state = {"messages": ["hello"], "count": 0}

        print("运行 LangGraph...")
        result = app.invoke(initial_state, config=config)
        print(f"✅ 完成: {result}")

        # 查看保存的 checkpoints
        print("\n保存的 checkpoints:")
        for cp in checkpointer.list(config, limit=5):
            print(f"  - {cp.checkpoint['id']}")


def example_error_handling():
    """错误处理示例"""
    print("\n=== 错误处理示例 ===\n")

    import pymysql

    DB_URI = "mysql://wrong_user:wrong_pass@localhost:3306/dbname"

    try:
        with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
            config = {"configurable": {"thread_id": "test"}}
            checkpointer.get(config)
    except pymysql.err.OperationalError as e:
        print(f"✅ 捕获到连接错误: {e.args[0]}")
        print("   提示：检查数据库连接参数")
    except Exception as e:
        print(f"❌ 其他错误: {e}")


def example_performance_test():
    """性能测试示例"""
    print("\n=== 性能测试示例 ===\n")

    import time

    DB_URI = os.getenv("MYSQL_URI", "mysql://user:password@localhost:3306/dbname")

    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        thread_id = "perf-test"
        num_checkpoints = 10

        # 写入测试
        print(f"写入 {num_checkpoints} 个 checkpoints...")
        start = time.time()

        for i in range(num_checkpoints):
            config = {
                "configurable": {
                    "thread_id": thread_id,
                    "checkpoint_ns": ""
                }
            }

            checkpoint = {
                "v": 4,
                "ts": datetime.now().isoformat(),
                "id": f"perf-{i:05d}",
                "channel_values": {"data": f"value-{i}"},
                "channel_versions": {"data": str(i)},
                "versions_seen": {},
            }

            checkpointer.put(config, checkpoint, {"index": i}, {})

        write_time = time.time() - start
        print(f"✅ 写入完成: {write_time:.2f}s ({num_checkpoints/write_time:.1f} ops/s)")

        # 读取测试
        print(f"\n读取 {num_checkpoints} 个 checkpoints...")
        start = time.time()

        config = {"configurable": {"thread_id": thread_id}}
        count = sum(1 for _ in checkpointer.list(config, limit=num_checkpoints))

        read_time = time.time() - start
        print(f"✅ 读取完成: {read_time:.2f}s ({count/read_time:.1f} ops/s)")

        print(f"\n⚠️  注意: MySQL 5.7 版本比 MySQL 8.0 慢 2-3 倍")


if __name__ == "__main__":
    print("=" * 60)
    print("MySQL 5.7 兼容版本示例")
    print("=" * 60)

    # 检查环境变量
    if not os.getenv("MYSQL_URI"):
        print("\n⚠️  警告: 未设置 MYSQL_URI 环境变量")
        print("   使用示例: export MYSQL_URI='mysql://user:pass@host:3306/db'")
        print("   将使用默认值进行演示（可能失败）\n")

    # 运行示例
    try:
        example_basic_usage()
        example_list_checkpoints()
        # example_with_langgraph()  # 需要安装 langgraph
        example_error_handling()
        example_performance_test()

    except Exception as e:
        print(f"\n❌ 运行出错: {e}")
        print("   请检查：")
        print("   1. 数据库连接是否正确")
        print("   2. 是否已执行建表脚本 (mysql57_schema.sql)")
        print("   3. 数据库用户是否有足够权限")

    print("\n" + "=" * 60)
    print("示例完成")
    print("=" * 60)
