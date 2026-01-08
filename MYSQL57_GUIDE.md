# MySQL 5.7 兼容指南

本指南说明如何在 MySQL 5.7 或 PolarDB-X 等不完全兼容 MySQL 8.0 的数据库上使用 langgraph-checkpoint-mysql。

## 📋 目录

1. [问题说明](#问题说明)
2. [解决方案](#解决方案)
3. [安装步骤](#安装步骤)
4. [使用方法](#使用方法)
5. [限制和注意事项](#限制和注意事项)

## 🔍 问题说明

langgraph-checkpoint-mysql 官方要求 **MySQL >= 8.0.19**，因为使用了以下 MySQL 8.0+ 特性：

| 特性 | MySQL 版本要求 | 作用 |
|------|---------------|------|
| `JSON_TABLE()` | 8.0+ | 展开 JSON 数组为行 |
| `JSON_ARRAYAGG()` | 8.0.19+ | 聚合为 JSON 数组 |
| CTE (`WITH` 语句) | 8.0+ | 公共表表达式 |
| JSON DEFAULT | 8.0+ | JSON 字段默认值 |

MySQL 5.7 和部分分布式数据库（如 PolarDB-X）不支持这些特性，会报错：
```
ERR-CODE: [TDDL-4500][ERR_PARSER] ERROR
```

## ✅ 解决方案

提供两个文件：
1. **`mysql57_compat_saver.py`** - 兼容的 Saver 类
2. **`mysql57_schema.sql`** - 兼容的建表脚本

### 核心修改

| 原始实现 | MySQL 5.7 兼容方案 |
|---------|-------------------|
| `JSON_TABLE()` 展开 channels | Python 中遍历处理 |
| `JSON_ARRAYAGG()` 聚合数据 | 分多次查询，Python 中聚合 |
| `WITH CTE` 查询 | 子查询或多次查询 |
| `DEFAULT ('{}')` | 移除默认值，代码中确保传值 |

## 📦 安装步骤

### 步骤 1: 复制兼容文件到项目

```bash
# 将以下文件复制到您的项目中
cp mysql57_compat_saver.py /path/to/your/project/
cp mysql57_schema.sql /path/to/your/project/
```

### 步骤 2: 执行建表脚本

```bash
# 连接到 MySQL 5.7 数据库
mysql -u username -p -h host database_name < mysql57_schema.sql
```

或在 MySQL 客户端中：
```sql
SOURCE /path/to/mysql57_schema.sql;
```

### 步骤 3: 验证表创建

```sql
-- 检查表
SHOW TABLES LIKE 'checkpoint%';

-- 应该显示 4 个表：
-- checkpoint_migrations
-- checkpoints
-- checkpoint_blobs
-- checkpoint_writes

-- 检查迁移版本
SELECT COUNT(*) FROM checkpoint_migrations;
-- 应返回 22
```

## 💻 使用方法

### 基础使用

```python
from mysql57_compat_saver import MySQL57Saver

# 数据库连接字符串
DB_URI = "mysql://user:password@localhost:3306/dbname"

# 创建 checkpointer（不要调用 setup()）
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 直接使用，表已经手动创建
    config = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}

    # 保存 checkpoint
    checkpoint = {
        "v": 4,
        "ts": "2024-07-31T20:14:19.804150+00:00",
        "id": "1ef4f797-8335-6428-8001-8a1503f9b875",
        "channel_values": {"my_key": "value"},
        "channel_versions": {"my_key": 1},
        "versions_seen": {},
    }
    checkpointer.put(config, checkpoint, {}, {})

    # 读取 checkpoint
    loaded = checkpointer.get(config)
    print(loaded)

    # 列出所有 checkpoints
    for cp in checkpointer.list(config):
        print(cp)
```

### 在 LangGraph 中使用

```python
from langgraph.graph import StateGraph
from mysql57_compat_saver import MySQL57Saver

DB_URI = "mysql://user:password@localhost:3306/dbname"

# 定义状态
class State:
    messages: list[str]

# 创建 graph
graph = StateGraph(State)
# ... 添加节点和边 ...

# 使用 MySQL 5.7 兼容的 checkpointer
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    app = graph.compile(checkpointer=checkpointer)

    # 运行 graph
    result = app.invoke(
        {"messages": ["hello"]},
        config={"configurable": {"thread_id": "conversation-1"}}
    )
```

### 环境变量配置

```python
import os
from mysql57_compat_saver import MySQL57Saver

DB_URI = os.getenv("MYSQL_URI", "mysql://user:pass@localhost:3306/dbname")

with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 使用 checkpointer
    pass
```

## ⚠️ 限制和注意事项

### 1. 不要调用 `setup()`

```python
# ❌ 错误 - 会抛出异常
checkpointer.setup()

# ✅ 正确 - 手动建表后直接使用
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 直接使用
    pass
```

### 2. 性能影响

MySQL 5.7 兼容版本会执行**更多次数据库查询**：

| 操作 | 原始版本 | MySQL 5.7 版本 |
|------|---------|---------------|
| `get()` | 1 次查询 | 3-5 次查询 |
| `list()` | 1 次查询 | N+1 次查询 |

对于大量 checkpoint 的场景，性能会有所下降。

### 3. 数据一致性

兼容版本在读取数据时：
- 先读取 checkpoint 基础信息
- 再读取 channel_values (blobs)
- 最后读取 pending_writes

**不是原子操作**，在高并发下可能出现不一致。

### 4. 迁移路径

建议：
- **开发环境**：使用 MySQL 5.7 兼容版本
- **生产环境**：升级到 MySQL 8.0 或切换到 PostgreSQL

### 5. 功能完整性

已测试支持的功能：
- ✅ `put()` - 保存 checkpoint
- ✅ `get()` / `get_tuple()` - 读取 checkpoint
- ✅ `list()` - 列出 checkpoints
- ✅ `put_writes()` - 保存中间写入
- ✅ 在 LangGraph 中使用

不支持的功能：
- ❌ `setup()` - 需要手动建表

## 🔧 故障排查

### 问题 1: 连接失败

```python
pymysql.err.OperationalError: (2003, "Can't connect to MySQL server...")
```

**解决方案：**
- 检查数据库主机、端口是否正确
- 确认数据库用户有访问权限
- 检查防火墙设置

### 问题 2: checkpoint_ns_hash 为空

```sql
ERROR 1048 (23000): Column 'checkpoint_ns_hash' cannot be null
```

**原因：** `checkpoint_ns_hash` 需要在插入时计算

**解决方案：** 已在代码中处理，使用 `UNHEX(MD5(checkpoint_ns))` 自动计算

### 问题 3: metadata 不能为空

```sql
ERROR 1364 (HY000): Field 'metadata' doesn't have a default value
```

**原因：** MySQL 5.7 不支持 JSON DEFAULT

**解决方案：** 代码中已确保总是提供 metadata 值

## 🎯 最佳实践

### 1. 连接池管理

```python
import pymysql
from dbutils.pooled_db import PooledDB
from mysql57_compat_saver import MySQL57Saver

pool = PooledDB(
    creator=pymysql,
    maxconnections=10,
    host='localhost',
    user='user',
    password='pass',
    database='dbname',
    autocommit=True
)

# 使用连接池
conn = pool.connection()
checkpointer = MySQL57Saver(conn)
# ... 使用 checkpointer
conn.close()
```

### 2. 错误处理

```python
from mysql57_compat_saver import MySQL57Saver
import pymysql

try:
    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        result = checkpointer.get(config)
except pymysql.Error as e:
    print(f"数据库错误: {e}")
    # 处理错误
```

### 3. 监控查询性能

```python
import logging
logging.basicConfig(level=logging.DEBUG)

# 启用 pymysql 日志以查看执行的 SQL
import pymysql.cursors
pymysql.cursors.Cursor._executed = None
```

## 📚 相关资源

- [官方文档](https://github.com/tjni/langgraph-checkpoint-mysql)
- [MySQL 5.7 JSON 函数支持](https://dev.mysql.com/doc/refman/5.7/en/json-functions.html)
- [LangGraph 文档](https://python.langchain.com/docs/langgraph)

## 🆘 获取帮助

如果遇到问题：
1. 检查数据库版本：`SELECT VERSION();`
2. 验证表结构：`SHOW CREATE TABLE checkpoints;`
3. 查看错误日志
4. 提交 Issue

---

**注意：** 这是一个社区兼容方案，不是官方支持。建议生产环境升级到 MySQL 8.0 或使用 PostgreSQL。
