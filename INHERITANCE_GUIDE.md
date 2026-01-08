# MySQL 5.7 兼容方案 - 继承重写详细指南

## 📖 方案概述

通过**继承原包的类**并**重写不兼容的方法**来支持 MySQL 5.7，无需修改原包代码。

### 核心思路

```python
from langgraph.checkpoint.mysql.pymysql import PyMySQLSaver

class MySQL57PyMySQLSaver(PyMySQLSaver):  # 继承原类
    def _select_sql(self, where):
        # 重写不兼容的方法
        return MYSQL57_COMPATIBLE_SQL

    def _load_checkpoint_tuple(self, value):
        # 重写数据加载逻辑
        ...
```

---

## 🎯 需要重写的方法

### 1. `_select_sql()` - 查询 SQL 生成

**原因：** 原方法使用了 `JSON_TABLE`, `JSON_ARRAYAGG`, `WITH CTE`

**原始代码：**
```python
SELECT_SQL = f"""
WITH channel_versions AS (
    SELECT ... FROM checkpoints, json_table(...)  -- ❌ MySQL 5.7 不支持
)
SELECT ..., json_arrayagg(...) FROM ...  -- ❌ MySQL 5.7 不支持
"""
```

**重写后：**
```python
@staticmethod
def _select_sql(where: str) -> str:
    """使用简化的 SELECT SQL"""
    return """
        SELECT thread_id, checkpoint, checkpoint_ns, ...
        FROM checkpoints
        {WHERE}
    """.replace("{WHERE}", where)
```

### 2. `_load_checkpoint_tuple()` - 加载 checkpoint 数据

**原因：** 原方法依赖 SQL 返回的聚合数据（通过 `JSON_ARRAYAGG`）

**重写策略：**
```python
def _load_checkpoint_tuple(self, value: dict) -> CheckpointTuple:
    # 1. 解析基础 checkpoint
    checkpoint = json.loads(value["checkpoint"])

    # 2. 手动加载 blobs（原本通过 JSON_ARRAYAGG）
    channel_values = self._load_channel_values_mysql57(...)

    # 3. 手动加载 writes（原本通过 JSON_ARRAYAGG）
    pending_writes = self._load_pending_writes_mysql57(...)

    # 4. 组装返回
    return CheckpointTuple(...)
```

### 3. `_load_channel_values_mysql57()` - 加载 channel 值（新增）

**作用：** 替代原来的 `JSON_ARRAYAGG` 聚合逻辑

**实现：**
```python
def _load_channel_values_mysql57(self, thread_id, checkpoint_ns, checkpoint):
    channel_versions = checkpoint.get("channel_versions", {})
    result = {}

    # 为每个 channel 单独查询
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

        if row and row["blob"]:
            result[channel] = self.serde.loads_typed((row["type"], row["blob"]))

    return result
```

### 4. `_load_pending_writes_mysql57()` - 加载待处理写入（新增）

**作用：** 替代原来的 `JSON_ARRAYAGG` 聚合逻辑

**实现：**
```python
def _load_pending_writes_mysql57(self, thread_id, checkpoint_ns, checkpoint_id):
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
        (row["task_id"], row["channel"], self.serde.loads_typed(...))
        for row in rows
    ]
```

### 5. `list()` - 列出 checkpoints（重写）

**原因：** 原方法调用 `_select_sql()` 和 `_load_checkpoint_tuple()`

**重写：**
```python
def list(self, config, *, filter=None, before=None, limit=None):
    where, args = self._search_where(config, filter, before)
    query = self._select_sql(where) + " ORDER BY checkpoint_id DESC"
    if limit:
        query += f" LIMIT {limit}"

    with self._cursor() as cur:
        cur.execute(query, args)
        for value in cur.fetchall():
            yield self._load_checkpoint_tuple(value)  # 使用重写的方法
```

### 6. `get_tuple()` - 获取单个 checkpoint（重写）

**原因：** 同样依赖重写的方法

**重写：**
```python
def get_tuple(self, config):
    thread_id = config["configurable"]["thread_id"]
    checkpoint_id = get_checkpoint_id(config)
    checkpoint_ns = config["configurable"].get("checkpoint_ns", "")

    # 构建 WHERE 条件
    if checkpoint_id:
        where = "WHERE thread_id = ... AND checkpoint_id = ..."
    else:
        where = "WHERE thread_id = ..."

    query = self._select_sql(where)
    with self._cursor() as cur:
        cur.execute(query, args)
        value = cur.fetchone()

    return self._load_checkpoint_tuple(value) if value else None
```

### 7. `setup()` - 禁用自动建表（重写）

**原因：** MySQL 5.7 不支持原来的建表 SQL

**重写：**
```python
def setup(self) -> None:
    raise NotImplementedError(
        "MySQL 5.7 不支持自动 setup()。\n"
        "请手动执行建表脚本：mysql57_schema.sql"
    )
```

---

## 📊 重写方法对比表

| 方法 | 是否重写 | 原因 | 复杂度 |
|------|---------|------|--------|
| `_select_sql()` | ✅ 必须 | 使用 MySQL 8.0 特性 | 🟢 简单 |
| `_load_checkpoint_tuple()` | ✅ 必须 | 依赖聚合数据 | 🟡 中等 |
| `_load_channel_values_mysql57()` | ✅ 新增 | 替代 JSON_ARRAYAGG | 🟡 中等 |
| `_load_pending_writes_mysql57()` | ✅ 新增 | 替代 JSON_ARRAYAGG | 🟡 中等 |
| `list()` | ✅ 必须 | 调用重写的方法 | 🟢 简单 |
| `get_tuple()` | ✅ 必须 | 调用重写的方法 | 🟢 简单 |
| `setup()` | ✅ 必须 | 不兼容的 SQL | 🟢 简单 |
| `put()` | ❌ 继承 | 不需要修改 | - |
| `put_writes()` | ❌ 继承 | 不需要修改 | - |
| `delete_thread()` | ❌ 继承 | 不需要修改 | - |

**总结：** 只需重写 **7 个方法**（其中 2 个是新增），其余方法全部继承。

---

## 💻 完整使用示例

### 示例 1: 基础使用

```python
from mysql57_override import MySQL57PyMySQLSaver

DB_URI = "mysql://user:pass@localhost:3306/dbname"

# 使用继承的类
with MySQL57PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
    config = {"configurable": {"thread_id": "test", "checkpoint_ns": ""}}

    # 保存
    checkpoint = {
        "v": 4,
        "ts": "2024-01-01T00:00:00",
        "id": "cp-001",
        "channel_values": {"data": "value"},
        "channel_versions": {"data": "1"},
        "versions_seen": {},
    }
    checkpointer.put(config, checkpoint, {}, {})

    # 读取
    loaded = checkpointer.get(config)
    print(loaded.checkpoint)
```

### 示例 2: 在项目中封装

```python
# app/core/checkpoint.py
from contextlib import contextmanager
from mysql57_override import MySQL57PyMySQLSaver
import os

DB_URI = os.getenv("MYSQL_URI")

@contextmanager
def init_checkpointer():
    """初始化 checkpointer"""
    with MySQL57PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
        yield checkpointer

# 使用
from app.core.checkpoint import init_checkpointer

with init_checkpointer() as checkpointer:
    # 使用 checkpointer
    pass
```

### 示例 3: 在 LangGraph 中使用

```python
from langgraph.graph import StateGraph
from mysql57_override import MySQL57PyMySQLSaver

DB_URI = "mysql://user:pass@localhost:3306/dbname"

# 创建 graph
graph = StateGraph(MyState)
# ... 添加节点 ...

# 使用 MySQL 5.7 兼容的 checkpointer
with MySQL57PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
    app = graph.compile(checkpointer=checkpointer)

    result = app.invoke(
        input_data,
        config={"configurable": {"thread_id": "session-1"}}
    )
```

---

## 🔍 实现细节

### 查询执行流程

**原始流程（MySQL 8.0）：**
```
1. 执行复杂 SQL（WITH + JSON_TABLE + JSON_ARRAYAGG）
   ↓
2. 获取完整数据（包括聚合的 blobs 和 writes）
   ↓
3. 直接构建 CheckpointTuple
```

**重写后流程（MySQL 5.7）：**
```
1. 执行简化 SQL（只查 checkpoints 表）
   ↓
2. 获取基础 checkpoint 数据
   ↓
3. 额外查询 blobs（N 次，N = channel 数量）
   ↓
4. 额外查询 writes（1 次）
   ↓
5. 在 Python 中组装数据
   ↓
6. 构建 CheckpointTuple
```

### 性能对比

| 操作 | MySQL 8.0 | MySQL 5.7 重写版 | 影响 |
|------|-----------|-----------------|------|
| `put()` | 1-2 次查询 | 1-2 次查询 | 🟢 无影响 |
| `get()` | 1 次查询 | 3-5 次查询 | 🟡 2-3x 慢 |
| `list(10)` | 1 次查询 | 30-50 次查询 | 🔴 10x 慢 |

**优化建议：**
- 使用连接池
- 添加缓存层
- 限制 `list()` 的 limit

---

## 📋 部署步骤

### 1. 复制文件

```bash
# 复制继承实现
cp mysql57_override.py /path/to/your/project/

# 复制建表脚本
cp mysql57_schema.sql /path/to/your/project/
```

### 2. 执行建表脚本

```bash
mysql -u user -p database < mysql57_schema.sql
```

### 3. 修改代码

**Before:**
```python
from langgraph.checkpoint.mysql.pymysql import PyMySQLSaver

with PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
    checkpointer.setup()  # ❌ 会报错
```

**After:**
```python
from mysql57_override import MySQL57PyMySQLSaver

with MySQL57PyMySQLSaver.from_conn_string(DB_URI) as checkpointer:
    # ✅ 直接使用，不调用 setup()
    pass
```

### 4. 测试

```python
# test.py
from mysql57_override import MySQL57PyMySQLSaver

DB_URI = "mysql://user:pass@localhost:3306/db"

try:
    with MySQL57PyMySQLSaver.from_conn_string(DB_URI) as cp:
        config = {"configurable": {"thread_id": "test", "checkpoint_ns": ""}}

        # 测试写入
        checkpoint = {
            "v": 4, "ts": "2024-01-01T00:00:00", "id": "test",
            "channel_values": {}, "channel_versions": {}, "versions_seen": {}
        }
        cp.put(config, checkpoint, {}, {})
        print("✅ 写入成功")

        # 测试读取
        loaded = cp.get(config)
        print(f"✅ 读取成功: {loaded.checkpoint['id']}")

except Exception as e:
    print(f"❌ 测试失败: {e}")
```

---

## 🆚 与独立实现对比

| 方面 | 继承重写（mysql57_override.py） | 独立实现（mysql57_compat_saver.py） |
|------|-------------------------------|----------------------------------|
| **代码量** | ~200 行 | ~500 行 |
| **维护成本** | 🟢 低（只重写必要部分） | 🟡 中（完整实现） |
| **升级难度** | 🟢 容易（继承新版本） | 🔴 困难（需要手动合并） |
| **理解难度** | 🟡 需要了解继承关系 | 🟢 独立，易理解 |
| **推荐场景** | 长期维护，需要跟进上游更新 | 短期使用，完全独立 |

---

## ⚠️ 注意事项

### 1. 继承链

```python
MySQL57PyMySQLSaver  # 您的类
  ↓ 继承
PyMySQLSaver  # langgraph.checkpoint.mysql.pymysql
  ↓ 继承
BaseSyncMySQLSaver  # langgraph.checkpoint.mysql
  ↓ 继承
BaseMySQLSaver  # langgraph.checkpoint.mysql.base
```

### 2. 方法调用顺序

```python
checkpointer.get(config)
  ↓
get_tuple(config)  # 重写的
  ↓
_select_sql(where)  # 重写的
  ↓
_load_checkpoint_tuple(value)  # 重写的
  ↓
_load_channel_values_mysql57(...)  # 新增的
_load_pending_writes_mysql57(...)  # 新增的
```

### 3. 升级原包

当 langgraph-checkpoint-mysql 升级时：

```bash
# 1. 升级原包
pip install --upgrade langgraph-checkpoint-mysql

# 2. 检查是否有新方法需要重写
# （通常不需要，因为只重写了内部方法）

# 3. 测试您的代码
python test.py
```

---

## 📚 相关资源

- **继承实现代码：** `mysql57_override.py`
- **建表脚本：** `mysql57_schema.sql`
- **快速开始：** `QUICKSTART_MYSQL57.md`
- **完整文档：** `MYSQL57_GUIDE.md`

---

## 🎯 总结

**继承重写方案的优势：**

1. ✅ **代码少**：只重写 7 个方法，~200 行代码
2. ✅ **易维护**：继承上游所有功能和 bug 修复
3. ✅ **易升级**：原包升级后通常无需修改
4. ✅ **安全**：不修改原包，不影响其他项目
5. ✅ **灵活**：可以选择性重写需要的方法

**推荐使用场景：**
- 长期项目
- 需要跟进上游更新
- 团队开发（多人维护）
- 生产环境

选择这个方案，您可以轻松支持 MySQL 5.7，同时保持代码的可维护性！
