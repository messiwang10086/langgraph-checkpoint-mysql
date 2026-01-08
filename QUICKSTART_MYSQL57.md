# MySQL 5.7 快速开始指南

3 步快速开始在 MySQL 5.7 / PolarDB-X 上使用 langgraph-checkpoint-mysql。

## 📦 步骤 1: 复制文件到项目

```bash
# 将以下 3 个文件复制到您的项目
cp mysql57_compat_saver.py /path/to/your/project/
cp mysql57_schema.sql /path/to/your/project/
cp example_mysql57.py /path/to/your/project/  # 可选：示例代码
```

## 🗄️ 步骤 2: 执行建表脚本

```bash
# 连接到 MySQL 5.7 数据库并执行
mysql -u username -p -h hostname database_name < mysql57_schema.sql
```

**验证安装：**
```sql
-- 检查表是否创建成功
SHOW TABLES LIKE 'checkpoint%';
-- 应该显示 4 个表

-- 检查迁移版本
SELECT COUNT(*) FROM checkpoint_migrations;
-- 应该返回 22
```

## 💻 步骤 3: 在代码中使用

### 最简示例

```python
from mysql57_compat_saver import MySQL57Saver

DB_URI = "mysql://user:password@localhost:3306/dbname"

# ⚠️ 注意：不要调用 setup()
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 配置
    config = {
        "configurable": {
            "thread_id": "my-thread",
            "checkpoint_ns": ""
        }
    }

    # 创建 checkpoint
    checkpoint = {
        "v": 4,
        "ts": "2024-01-01T00:00:00",
        "id": "checkpoint-001",
        "channel_values": {"data": "hello"},
        "channel_versions": {"data": "1"},
        "versions_seen": {},
    }

    # 保存
    checkpointer.put(config, checkpoint, {}, {})

    # 读取
    loaded = checkpointer.get(config)
    print(loaded.checkpoint)
```

### 在 LangGraph 中使用

```python
from langgraph.graph import StateGraph
from mysql57_compat_saver import MySQL57Saver

# 定义 state
class State:
    messages: list[str]

# 创建 graph
graph = StateGraph(State)
# ... 添加节点和边 ...

# 使用 checkpointer
DB_URI = "mysql://user:password@localhost:3306/dbname"
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    app = graph.compile(checkpointer=checkpointer)

    result = app.invoke(
        {"messages": ["hello"]},
        config={"configurable": {"thread_id": "conv-1"}}
    )
```

## ⚠️ 重要提示

| ❌ 不要做 | ✅ 要做 |
|---------|--------|
| `checkpointer.setup()` | 手动执行 SQL 建表 |
| 使用 MySQL 5.6 或更早版本 | 使用 MySQL 5.7+ |
| 期望与 MySQL 8.0 相同性能 | 接受 2-3x 性能损失 |

## 📊 文件说明

| 文件 | 大小 | 说明 |
|------|------|------|
| `mysql57_compat_saver.py` | ~10KB | 兼容的 Saver 类实现 |
| `mysql57_schema.sql` | ~5KB | 建表 SQL 脚本 |
| `MYSQL57_GUIDE.md` | ~15KB | 详细使用文档 |
| `example_mysql57.py` | ~8KB | 示例代码 |

## 🔧 常见问题

### Q: 为什么不能调用 `setup()`？
A: MySQL 5.7 不支持原始的建表 SQL（包含 JSON DEFAULT 等特性），必须手动建表。

### Q: 性能如何？
A: 比 MySQL 8.0 版本慢 **2-3 倍**，因为需要执行更多查询。

### Q: 支持哪些功能？
A: ✅ put, get, list, put_writes | ❌ setup

### Q: 生产环境能用吗？
A: 可以，但建议：
- ✅ 开发/测试环境使用
- ⚠️ 生产环境建议升级到 MySQL 8.0 或用 PostgreSQL

## 🆘 遇到问题？

1. **连接失败** → 检查 `DB_URI` 格式和数据库权限
2. **表不存在** → 确认执行了 `mysql57_schema.sql`
3. **字段为 NULL** → 检查迁移版本是否插入完整 (22 条)

## 📚 下一步

- 阅读 [完整文档](MYSQL57_GUIDE.md)
- 查看 [示例代码](example_mysql57.py)
- 了解 [性能优化](MYSQL57_GUIDE.md#性能影响)

---

**提示：** 这是社区兼容方案。生产环境建议使用官方支持的 MySQL 8.0 或 PostgreSQL。
