# MySQL 5.7 兼容方案总结

## 📁 已创建的文件

我为您创建了完整的 MySQL 5.7 兼容方案，包含以下文件：

| 文件名 | 类型 | 用途 |
|--------|------|------|
| **mysql57_compat_saver.py** | Python | 兼容 MySQL 5.7 的 Saver 实现 |
| **mysql57_schema.sql** | SQL | 建表脚本（兼容 MySQL 5.7） |
| **QUICKSTART_MYSQL57.md** | 文档 | 3 步快速开始指南 |
| **MYSQL57_GUIDE.md** | 文档 | 完整使用文档 |
| **example_mysql57.py** | Python | 示例代码集合 |
| **MYSQL57_SUMMARY.md** | 文档 | 本文档 - 技术总结 |

## 🔍 核心技术修改

### 1. 不兼容特性及解决方案

| MySQL 8.0 特性 | MySQL 5.7 状态 | 解决方案 |
|---------------|---------------|----------|
| `JSON_TABLE()` | ❌ 不支持 | Python 中解析 JSON，多次查询 |
| `JSON_ARRAYAGG()` | ❌ 不支持 | 分别查询后在 Python 中聚合 |
| CTE (`WITH`) | ❌ 不支持 | 改用子查询或多次查询 |
| `JSON DEFAULT ('{}')` | ❌ 不支持 | 移除 DEFAULT，代码中保证传值 |
| 生成列 `AS (...)` | ⚠️ 部分支持 | 改为普通列，代码中计算 |

### 2. 重写的核心方法

#### `_select_sql()`
**原始版本：**
```sql
WITH channel_versions AS (
    SELECT ... FROM checkpoints, json_table(...)
)
SELECT ..., json_arrayagg(...) FROM ...
```

**MySQL 5.7 版本：**
```sql
SELECT thread_id, checkpoint, checkpoint_ns, ...
FROM checkpoints
WHERE ...
```
→ 简化查询，在 Python 中加载 blobs 和 writes

#### `_load_checkpoint_tuple()`
**新增逻辑：**
- 调用 `_load_channel_values_mysql57()` 手动加载 blobs
- 调用 `_load_pending_writes_mysql57()` 手动加载 writes
- 每个 checkpoint 额外执行 2-5 次查询

#### `_load_channel_values_mysql57()`
**实现：**
```python
# 1. 从 checkpoint.channel_versions 获取 channels
# 2. 为每个 channel 查询对应版本的 blob
# 3. 在 Python 中反序列化并组装
```

#### `_load_pending_writes_mysql57()`
**实现：**
```python
# 1. 查询所有 pending writes
# 2. 在 Python 中排序和组装
```

### 3. 建表脚本修改

**关键差异：**

| 字段/特性 | MySQL 8.0 | MySQL 5.7 兼容版 |
|----------|-----------|-----------------|
| `metadata` | `JSON NOT NULL DEFAULT ('{}')` | `JSON NOT NULL` |
| `checkpoint_ns_hash` | `AS (UNHEX(MD5(...))) STORED` | `BINARY(16) NOT NULL` |
| 索引创建 | 分步执行 | 建表时一起创建 |
| 迁移记录 | 自动插入 | 手动插入 v0-v21 |

## 📊 性能影响

### 查询次数对比

| 操作 | MySQL 8.0 | MySQL 5.7 兼容版 | 性能影响 |
|------|-----------|-----------------|---------|
| `get()` | 1 次查询 | 3-5 次查询 | 🟡 2-3x 慢 |
| `list(10)` | 1 次查询 | 11-30 次查询 | 🔴 5-10x 慢 |
| `put()` | 1-2 次查询 | 1-2 次查询 | 🟢 基本相同 |

### 为什么更慢？

**MySQL 8.0 版本：**
```sql
-- 一次查询获取所有数据
WITH ... SELECT ..., json_arrayagg(blobs), json_arrayagg(writes) FROM ...
```

**MySQL 5.7 版本：**
```sql
-- 1. 查询 checkpoint
SELECT * FROM checkpoints WHERE ...

-- 2. 查询每个 channel 的 blob (N 次)
SELECT * FROM checkpoint_blobs WHERE channel = ? AND version = ?

-- 3. 查询 pending writes
SELECT * FROM checkpoint_writes WHERE ...
```

## ⚠️ 使用限制

### 1. 功能限制

| 功能 | 支持状态 | 说明 |
|------|---------|------|
| `put()` | ✅ 完全支持 | |
| `get()` / `get_tuple()` | ✅ 完全支持 | |
| `list()` | ✅ 完全支持 | 但性能较差 |
| `put_writes()` | ✅ 完全支持 | |
| `delete_thread()` | ✅ 继承支持 | 未修改 |
| `setup()` | ❌ 不支持 | 必须手动建表 |

### 2. 并发安全

**MySQL 8.0 版本：**
- ✅ 单次查询，原子性强
- ✅ 事务隔离级别保证一致性

**MySQL 5.7 版本：**
- ⚠️ 多次查询，非原子操作
- ⚠️ 读取过程中数据可能被修改
- 💡 建议：使用 `SELECT ... FOR UPDATE` 或应用层锁

### 3. 数据一致性风险

**场景：** 并发读写同一 checkpoint

```python
# Thread 1: 读取
checkpoint = checkpointer.get(config)  # 查询 1: 读 checkpoint
                                        # 查询 2: 读 blobs
# >>> Thread 2: 写入新 blob <<<
                                        # 查询 3: 读 writes (可能不一致)
```

**缓解措施：**
- 使用事务（但会影响性能）
- 应用层加锁
- 接受最终一致性

## 🎯 适用场景

### ✅ 适合使用

- 开发和测试环境
- 低并发场景（< 10 QPS）
- 数据量较小（< 10000 checkpoints）
- 无法升级 MySQL 版本的遗留系统
- PolarDB-X 等分布式数据库（不支持 MySQL 8.0 特性）

### ❌ 不适合使用

- 高并发生产环境（> 100 QPS）
- 大数据量场景（> 100000 checkpoints）
- 对性能敏感的应用
- 需要强一致性的场景

### 💡 生产环境建议

| 场景 | 推荐方案 | 原因 |
|------|---------|------|
| 新项目 | PostgreSQL + langgraph-checkpoint-postgres | 官方支持，性能最好 |
| 已有 MySQL 8.0 | 直接使用原版 | 无需兼容改造 |
| MySQL 5.7 | 升级到 MySQL 8.0 | 长期最佳方案 |
| 无法升级 | 使用此兼容版本 | 权宜之计 |

## 📝 部署清单

### 开发环境

- [ ] 复制 `mysql57_compat_saver.py` 到项目
- [ ] 执行 `mysql57_schema.sql` 建表
- [ ] 修改代码导入：`from mysql57_compat_saver import MySQL57Saver`
- [ ] 移除所有 `checkpointer.setup()` 调用
- [ ] 测试基本功能

### 生产环境

- [ ] 完成开发环境测试
- [ ] 评估性能影响（建议压测）
- [ ] 设置监控（查询慢日志）
- [ ] 准备回滚方案
- [ ] 文档化部署步骤
- [ ] 制定升级计划（迁移到 MySQL 8.0）

## 🔧 故障排查

### 问题 1: 导入失败

```python
ModuleNotFoundError: No module named 'langchain_core'
```

**解决：**
```bash
pip install langchain-core langgraph
```

### 问题 2: 连接失败

```
OperationalError: (2003, "Can't connect to MySQL server")
```

**检查：**
- 数据库地址、端口
- 用户名密码
- 防火墙设置
- MySQL 服务状态

### 问题 3: checkpoint_ns_hash 为空

```sql
ERROR 1048: Column 'checkpoint_ns_hash' cannot be null
```

**原因：** 代码中应自动计算，如果出现此错误，检查：
- 是否使用了正确的 `MySQL57Saver`
- 表结构是否正确

### 问题 4: metadata 为空

```sql
ERROR 1364: Field 'metadata' doesn't have a default value
```

**原因：** MySQL 5.7 不支持 JSON DEFAULT

**解决：** 确保：
- 使用 `MySQL57Saver`（代码中总是传 metadata）
- 表是用 `mysql57_schema.sql` 创建的

### 问题 5: 性能很差

**表现：** 每个操作耗时 > 1s

**排查：**
```sql
-- 检查索引
SHOW INDEX FROM checkpoints;
SHOW INDEX FROM checkpoint_blobs;

-- 检查慢查询
SHOW VARIABLES LIKE 'slow_query%';
```

**优化：**
- 确保所有索引都已创建
- 使用连接池
- 考虑增加缓存层

## 📚 相关资源

- [官方文档](https://github.com/tjni/langgraph-checkpoint-mysql)
- [MySQL 5.7 文档](https://dev.mysql.com/doc/refman/5.7/en/)
- [LangGraph 文档](https://python.langchain.com/docs/langgraph)

## 🙋 获取帮助

遇到问题？

1. 查看 [MYSQL57_GUIDE.md](MYSQL57_GUIDE.md) 完整文档
2. 运行 [example_mysql57.py](example_mysql57.py) 测试示例
3. 检查数据库版本和表结构
4. 提交 Issue（附带错误日志和环境信息）

---

**最后提醒：** 这是一个兼容方案，不是长期解决方案。生产环境强烈建议：
1. 升级到 MySQL 8.0+
2. 或迁移到 PostgreSQL + langgraph-checkpoint-postgres

祝使用顺利！🚀
