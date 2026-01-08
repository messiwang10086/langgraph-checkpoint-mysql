# 部署 MySQL 5.7 兼容方案到您的项目

## 🚀 快速部署（3 分钟）

### 1️⃣ 复制文件

将以下文件复制到您的项目中：

```bash
# 假设您的项目路径是 /path/to/your/project

# 必需文件
cp mysql57_compat_saver.py /path/to/your/project/
cp mysql57_schema.sql /path/to/your/project/

# 可选文件（文档和示例）
cp QUICKSTART_MYSQL57.md /path/to/your/project/
cp MYSQL57_GUIDE.md /path/to/your/project/
cp example_mysql57.py /path/to/your/project/
```

**推荐项目结构：**
```
your-project/
├── app/
│   └── core/
│       └── checkpoint.py         # 您原来的代码
├── mysql57_compat_saver.py       # ← 新增：兼容类
├── mysql57_schema.sql            # ← 新增：建表脚本
└── example_mysql57.py            # ← 可选：示例
```

### 2️⃣ 执行建表脚本

```bash
# 方式 1: 命令行执行
mysql -u your_user -p -h your_host your_database < mysql57_schema.sql

# 方式 2: MySQL 客户端
mysql> SOURCE /path/to/mysql57_schema.sql;

# 方式 3: Python 执行
python -c "
import pymysql
conn = pymysql.connect(host='localhost', user='user', password='pass', database='db')
with open('mysql57_schema.sql') as f:
    cursor = conn.cursor()
    for statement in f.read().split(';'):
        if statement.strip():
            cursor.execute(statement)
conn.close()
"
```

### 3️⃣ 修改您的代码

#### 原来的代码（会报错）：

```python
# app/main.py 或 app/core/checkpoint.py
from app.core.checkpoint import init_checkpointer, close_checkpointer
# ❌ 这个模块不存在，会报错
```

#### 修改后的代码：

**选项 A：直接使用（最简单）**

```python
# app/main.py
from mysql57_compat_saver import MySQL57Saver
import os

DB_URI = os.getenv("MYSQL_URI", "mysql://user:pass@localhost:3306/dbname")

# 直接使用
with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
    # 您的业务逻辑
    config = {"configurable": {"thread_id": "user-123", "checkpoint_ns": ""}}
    # checkpointer.put(...), checkpointer.get(...), etc.
```

**选项 B：封装为模块（推荐）**

创建 `app/core/checkpoint.py`：

```python
"""
app/core/checkpoint.py
封装 checkpoint 功能
"""
from contextlib import contextmanager
from mysql57_compat_saver import MySQL57Saver
import os

# 从环境变量读取数据库配置
DB_URI = os.getenv(
    "MYSQL_URI",
    "mysql://user:password@localhost:3306/database"
)

@contextmanager
def init_checkpointer():
    """
    初始化并返回 checkpointer

    使用方法:
        with init_checkpointer() as checkpointer:
            # 使用 checkpointer
            pass
    """
    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        yield checkpointer


# 如果需要非上下文管理器版本
def get_checkpointer():
    """
    获取 checkpointer 实例（需要手动关闭连接）
    """
    import pymysql
    from mysql57_compat_saver import MySQL57Saver

    conn = pymysql.connect(
        **MySQL57Saver.parse_conn_string(DB_URI),
        autocommit=True
    )
    return MySQL57Saver(conn), conn


def close_checkpointer(conn):
    """关闭连接"""
    if conn:
        conn.close()


# 向后兼容：保持原来的接口
__all__ = ["init_checkpointer", "close_checkpointer", "get_checkpointer"]
```

然后在 `app/main.py` 中：

```python
# app/main.py
from app.core.checkpoint import init_checkpointer

# 方式 1: 上下文管理器（推荐）
with init_checkpointer() as checkpointer:
    # 使用 checkpointer
    config = {"configurable": {"thread_id": "thread-1", "checkpoint_ns": ""}}
    result = checkpointer.get(config)

# 方式 2: 手动管理
from app.core.checkpoint import get_checkpointer, close_checkpointer
checkpointer, conn = get_checkpointer()
try:
    # 使用 checkpointer
    pass
finally:
    close_checkpointer(conn)
```

**选项 C：在 LangGraph 中使用**

```python
# app/graph.py
from langgraph.graph import StateGraph
from mysql57_compat_saver import MySQL57Saver
import os

DB_URI = os.getenv("MYSQL_URI")

class MyGraph:
    def __init__(self):
        self.graph = StateGraph(MyState)
        # ... 构建 graph

    def get_app(self):
        """返回编译后的 app"""
        with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
            return self.graph.compile(checkpointer=checkpointer)

# 使用
my_graph = MyGraph()
app = my_graph.get_app()
result = app.invoke(
    input_data,
    config={"configurable": {"thread_id": "user-session"}}
)
```

## 📋 部署检查清单

### 代码修改

- [ ] 复制 `mysql57_compat_saver.py` 到项目
- [ ] 修改导入语句
  - [ ] 移除 `from app.core.checkpoint import ...`（如果不存在）
  - [ ] 改为 `from mysql57_compat_saver import MySQL57Saver`
- [ ] 移除所有 `checkpointer.setup()` 调用
- [ ] 确保设置了 `MYSQL_URI` 环境变量

### 数据库配置

- [ ] 执行 `mysql57_schema.sql` 建表
- [ ] 验证表创建成功：`SHOW TABLES LIKE 'checkpoint%';`
- [ ] 验证迁移版本：`SELECT COUNT(*) FROM checkpoint_migrations;` (应该是 22)
- [ ] 确认数据库用户有读写权限

### 环境配置

- [ ] 安装依赖：
  ```bash
  pip install pymysql langgraph langchain-core
  ```
- [ ] 设置环境变量：
  ```bash
  export MYSQL_URI="mysql://user:pass@host:3306/database"
  ```
- [ ] 或在代码中硬编码（不推荐生产环境）

### 测试验证

- [ ] 运行 `example_mysql57.py` 验证基本功能
- [ ] 测试您的应用能否连接数据库
- [ ] 测试保存和读取 checkpoint
- [ ] 检查日志无错误

## 🔍 验证部署

运行以下脚本验证部署是否成功：

```python
# test_deployment.py
from mysql57_compat_saver import MySQL57Saver
import os

DB_URI = os.getenv("MYSQL_URI", "mysql://user:pass@localhost:3306/db")

try:
    print("🔍 测试连接...")
    with MySQL57Saver.from_conn_string(DB_URI) as checkpointer:
        print("✅ 连接成功")

        print("\n🔍 测试写入...")
        config = {"configurable": {"thread_id": "test", "checkpoint_ns": ""}}
        checkpoint = {
            "v": 4,
            "ts": "2024-01-01T00:00:00",
            "id": "test-checkpoint",
            "channel_values": {},
            "channel_versions": {},
            "versions_seen": {},
        }
        checkpointer.put(config, checkpoint, {}, {})
        print("✅ 写入成功")

        print("\n🔍 测试读取...")
        result = checkpointer.get(config)
        if result and result.checkpoint["id"] == "test-checkpoint":
            print("✅ 读取成功")
        else:
            print("❌ 读取失败")

        print("\n✅ 所有测试通过！")

except Exception as e:
    print(f"\n❌ 测试失败: {e}")
    print("\n请检查：")
    print("1. MYSQL_URI 环境变量是否正确")
    print("2. 数据库是否可访问")
    print("3. 是否已执行建表脚本")
```

运行：
```bash
python test_deployment.py
```

## 🚨 常见问题

### 问题 1: 找不到模块

```
ModuleNotFoundError: No module named 'mysql57_compat_saver'
```

**解决：**
- 确保 `mysql57_compat_saver.py` 在 Python 路径中
- 或使用绝对导入：`from your_project.mysql57_compat_saver import MySQL57Saver`

### 问题 2: 数据库连接失败

```
OperationalError: (2003, "Can't connect to MySQL server")
```

**检查：**
```bash
# 测试数据库连接
mysql -u user -p -h host database

# 检查环境变量
echo $MYSQL_URI
```

### 问题 3: 表不存在

```
ProgrammingError: (1146, "Table 'database.checkpoints' doesn't exist")
```

**解决：**
```bash
# 重新执行建表脚本
mysql -u user -p database < mysql57_schema.sql
```

## 📚 下一步

1. ✅ 阅读 [QUICKSTART_MYSQL57.md](QUICKSTART_MYSQL57.md) - 快速开始
2. ✅ 查看 [MYSQL57_GUIDE.md](MYSQL57_GUIDE.md) - 详细文档
3. ✅ 运行 [example_mysql57.py](example_mysql57.py) - 学习示例
4. ✅ 部署到测试环境验证
5. ✅ 压测评估性能
6. ✅ 部署到生产环境

## 🎯 迁移计划

这是一个**临时兼容方案**，建议制定升级计划：

### 短期（现在）
- ✅ 使用 MySQL 5.7 兼容版本
- ✅ 在开发/测试环境验证

### 中期（3-6 个月）
- 📋 评估升级到 MySQL 8.0 的可行性
- 📋 或评估迁移到 PostgreSQL

### 长期（6-12 个月）
- 🎯 完成升级到 MySQL 8.0
- 🎯 或迁移到 PostgreSQL + langgraph-checkpoint-postgres
- 🎯 移除兼容代码，使用官方版本

---

**需要帮助？** 查看 [MYSQL57_SUMMARY.md](MYSQL57_SUMMARY.md) 了解技术细节和故障排查。
