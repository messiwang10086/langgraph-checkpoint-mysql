# 📦 直接可用的 MySQL 5.7 兼容方案

## 🚀 只需 2 个文件

您只需要复制这 2 个文件到您的项目：

1. **`mysql57_checkpointer.py`** - Python 代码（必需）
2. **`mysql57_schema.sql`** - 建表脚本（必需）

---

## ⚡ 3 步开始使用

### 步骤 1: 复制文件

```bash
# 复制到您的项目目录
cp mysql57_checkpointer.py /path/to/your/ainet-agent/
cp mysql57_schema.sql /path/to/your/ainet-agent/
```

### 步骤 2: 执行建表脚本

```bash
# 连接到您的数据库并执行
mysql -u admin -p -h your-host agentdb < mysql57_schema.sql
```

**验证：**
```sql
-- 检查表是否创建
SHOW TABLES LIKE 'checkpoint%';
-- 应该显示 4 个表

-- 检查迁移版本
SELECT COUNT(*) FROM checkpoint_migrations;
-- 应该返回 22
```

### 步骤 3: 在代码中使用

```python
from mysql57_checkpointer import MySQL57Saver

# 数据库连接字符串
DB_URI = "mysql://user:password@host:3306/database"

# 使用 checkpointer
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 配置
    config = {
        "configurable": {
            "thread_id": "user-session-123",
            "checkpoint_ns": ""
        }
    }

    # 保存 checkpoint
    checkpoint = {
        "v": 4,
        "ts": "2024-01-01T00:00:00",
        "id": "checkpoint-001",
        "channel_values": {"data": "your data"},
        "channel_versions": {"data": "1"},
        "versions_seen": {},
    }
    checkpointer.put(config, checkpoint, {}, {})

    # 读取 checkpoint
    loaded = checkpointer.get(config)
    print(loaded.checkpoint)
```

---

## 💻 在您的项目中集成

### 替换原来的导入

**Before (会报错):**
```python
# app/main.py
from app.core.checkpoint import init_checkpointer, close_checkpointer
# ❌ ModuleNotFoundError: No module named 'app.core.checkpoint'
```

**After (正确):**
```python
# app/main.py
from mysql57_checkpointer import MySQL57Saver
import os

DB_URI = os.getenv("MYSQL_URI", "mysql://user:pass@host:3306/db")

with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 您的业务逻辑
    config = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}
    result = checkpointer.get(config)
```

### 或者封装为模块

创建 `app/core/checkpoint.py`：

```python
"""
app/core/checkpoint.py
"""
from contextlib import contextmanager
from mysql57_checkpointer import MySQL57Saver
import os

DB_URI = os.getenv("MYSQL_URI")

@contextmanager
def init_checkpointer():
    """初始化 checkpointer"""
    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        yield checkpointer

def close_checkpointer(conn):
    """关闭连接"""
    if conn:
        conn.close()
```

然后在 `app/main.py` 中：

```python
from app.core.checkpoint import init_checkpointer

with init_checkpointer() as checkpointer:
    # 使用 checkpointer
    pass
```

---

## 🔧 完整示例

```python
"""
完整的使用示例
"""
from mysql57_checkpointer import MySQL57Saver
from datetime import datetime
import os

# 1. 配置数据库连接
DB_URI = os.getenv("MYSQL_URI", "mysql://user:pass@localhost:3306/db")

# 2. 使用 checkpointer
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 配置
    config = {
        "configurable": {
            "thread_id": "conversation-123",
            "checkpoint_ns": ""
        }
    }

    # 创建 checkpoint
    checkpoint = {
        "v": 4,
        "ts": datetime.now().isoformat(),
        "id": "checkpoint-001",
        "channel_values": {
            "messages": ["Hello", "How are you?"],
            "state": "active"
        },
        "channel_versions": {
            "messages": "1",
            "state": "1"
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
        print(f"   Messages: {loaded.checkpoint['channel_values']['messages']}")

    # 列出所有
    print("\n列出所有 checkpoints:")
    for i, cp in enumerate(checkpointer.list(config, limit=10), 1):
        print(f"  [{i}] {cp.checkpoint['id']}")
```

---

## 🎯 在 LangGraph 中使用

```python
from langgraph.graph import StateGraph, END
from mysql57_checkpointer import MySQL57Saver
from typing import TypedDict
import os

# 定义状态
class State(TypedDict):
    messages: list[str]
    count: int

# 定义节点
def my_node(state: State) -> State:
    return {
        "messages": state["messages"] + ["processed"],
        "count": state["count"] + 1
    }

# 创建 graph
graph = StateGraph(State)
graph.add_node("process", my_node)
graph.set_entry_point("process")
graph.add_edge("process", END)

# 使用 MySQL 5.7 checkpointer
DB_URI = os.getenv("MYSQL_URI")

with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 编译 graph
    app = graph.compile(checkpointer=checkpointer)

    # 运行
    config = {"configurable": {"thread_id": "session-1"}}
    result = app.invoke(
        {"messages": ["hello"], "count": 0},
        config=config
    )

    print(f"Result: {result}")
```

---

## ⚠️ 重要提示

### ✅ 可以做

- ✅ 使用 `MySQL57Saver.from_conn_string()` 创建连接
- ✅ 调用 `put()`, `get()`, `list()`, `put_writes()`
- ✅ 在 LangGraph 中使用
- ✅ 设置环境变量 `MYSQL_URI`

### ❌ 不要做

- ❌ **不要调用** `checkpointer.setup()` - 会抛出异常
- ❌ 不要期望与 MySQL 8.0 相同的性能
- ❌ 不要在高并发场景使用（性能较差）

---

## 🔍 故障排查

### 问题 1: 找不到模块

```
ModuleNotFoundError: No module named 'mysql57_checkpointer'
```

**解决：**
- 确保 `mysql57_checkpointer.py` 在 Python 路径中
- 或使用绝对路径导入

### 问题 2: 连接失败

```
OperationalError: (2003, "Can't connect to MySQL server")
```

**解决：**
```bash
# 测试连接
mysql -u user -p -h host database

# 检查环境变量
echo $MYSQL_URI
```

### 问题 3: 表不存在

```
ProgrammingError: (1146, "Table 'checkpoints' doesn't exist")
```

**解决：**
```bash
# 重新执行建表脚本
mysql -u user -p database < mysql57_schema.sql
```

### 问题 4: setup() 报错

```
NotImplementedError: MySQL 5.7 不支持自动 setup()
```

**这是正常的！** 需要手动执行建表脚本，不要调用 `setup()`。

---

## 📊 性能说明

| 操作 | MySQL 8.0 | MySQL 5.7 兼容版 | 性能影响 |
|------|-----------|-----------------|---------|
| `put()` | 1-2 次查询 | 1-2 次查询 | 🟢 无影响 |
| `get()` | 1 次查询 | 3-5 次查询 | 🟡 2-3x 慢 |
| `list(10)` | 1 次查询 | 30-50 次查询 | 🔴 10x 慢 |

**建议：**
- 使用连接池
- 限制 `list()` 的数量
- 添加缓存层

---

## 📦 文件清单

您只需要这 2 个文件：

```
your-project/
├── mysql57_checkpointer.py    # ← Python 代码（必需）
└── mysql57_schema.sql          # ← 建表脚本（必需）
```

**可选文档：**
- `READY_TO_USE.md` - 本文档
- `QUICKSTART_MYSQL57.md` - 快速开始
- `INHERITANCE_GUIDE.md` - 技术细节

---

## ✅ 验证安装

运行以下代码测试：

```python
from mysql57_checkpointer import MySQL57Saver
import os

DB_URI = os.getenv("MYSQL_URI", "mysql://user:pass@localhost:3306/db")

try:
    with MySQL57Saver.from_conn_string(DB_URI) as cp:
        print("✅ 连接成功")

        config = {"configurable": {"thread_id": "test", "checkpoint_ns": ""}}
        checkpoint = {
            "v": 4, "ts": "2024-01-01", "id": "test",
            "channel_values": {}, "channel_versions": {}, "versions_seen": {}
        }

        cp.put(config, checkpoint, {}, {})
        print("✅ 写入成功")

        result = cp.get(config)
        print(f"✅ 读取成功: {result.checkpoint['id']}")

        print("\n🎉 所有测试通过！可以开始使用了。")

except Exception as e:
    print(f"❌ 测试失败: {e}")
    print("\n请检查：")
    print("1. 数据库连接是否正确")
    print("2. 是否已执行建表脚本")
```

---

## 🎉 开始使用

现在您可以：

1. ✅ 复制 2 个文件到项目
2. ✅ 执行建表脚本
3. ✅ 在代码中导入并使用

就这么简单！祝使用顺利！🚀

---

**需要帮助？** 查看详细文档或运行测试代码。
