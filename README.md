# SmartDB

SmartDB 是一个基于 **CMake + Conan 2 + C++17** 的轻量数据库抽象项目，当前已包含：

- 统一数据库值类型 `DbValue`
- 数据库接口抽象：`IConnection` / `IResultSet` / `IDriver`
- 驱动注册与配置管理：`DatabaseManager`
- 线程安全连接池：`ConnectionPool`
- SQLite / MySQL 驱动实现（含基础查询、执行、事务接口）

## 当前能力

### 1) 核心抽象

位于 `src/sdb/`：

- `types.hpp`：跨数据库的值类型定义与辅助函数
- `idb.hpp`：统一数据库接口定义
- `db.hpp`：驱动注册、JSON 配置加载、连接工厂
- `connection_pool.hpp`：线程安全连接池与超时/容量控制

### 2) 驱动实现

位于 `src/sdb/drivers/`：

- `sqlite_driver.hpp`
  - 支持 `query / execute / execute(参数化)`
  - 支持 `int/int64/double/bool/string/blob/null` 参数绑定
  - 支持 `BLOB` 结果读取
- `mysql_driver.hpp`
  - 支持连接、查询、基础执行和事务
  - 参数化执行接口预留（当前未实现）

### 3) 示例入口

`src/main.cpp` 演示：

1. 注册 SQLite / MySQL 驱动
2. 读取 `db_config.json`
3. 创建连接并执行 SQL

## 配置文件

默认读取项目根目录下的 `db_config.json`，结构如下：

```json
{
  "connections": {
    "my_mysql": {
      "driver": "mysql",
      "host": "127.0.0.1",
      "port": 3306,
      "user": "root",
      "password": "root",
      "database": "my_app"
    },
    "my_sqlite": {
      "driver": "sqlite",
      "path": "local_data.db"
    }
  }
}
```

## Conan 环境初始化（无 Conan 环境时）

如果你的机器或 CI 环境还没有 Conan，请先安装并初始化：

### Linux/macOS

```bash
python3 -m pip install --upgrade pip
python3 -m pip install "conan>=2.0"
conan profile detect --force
```

### Windows (PowerShell)

```powershell
py -m pip install --upgrade pip
py -m pip install "conan>=2.0"
conan profile detect --force
```

> CI 中建议按构建类型分别安装依赖并指定输出目录：
>
> - Debug: `conan install . -of build/Debug --build=missing -s build_type=Debug`
> - Release: `conan install . -of build/Release --build=missing -s build_type=Release`

## 构建与测试

### 1) 安装依赖

```bash
conan install . -of build/Debug --build=missing -s build_type=Debug
conan install . -of build/Release --build=missing -s build_type=Release
```

### 2) 配置

```bash
cmake --preset conan-debug
cmake --preset conan-release
```

### 3) 编译

```bash
cmake --build --preset conan-debug
cmake --build --preset conan-release
```

### 4) 测试

```bash
ctest --preset conan-debug --output-on-failure
ctest --preset conan-release --output-on-failure
```

## 项目结构

```text
SmartDB/
├── CMakeLists.txt
├── conanfile.py
├── db_config.json
├── src/
│   ├── CMakeLists.txt
│   ├── main.cpp
│   ├── cppsharp/
│   │   ├── my_lib.hpp
│   │   └── my_lib.cpp
│   └── sdb/
│       ├── types.hpp
│       ├── idb.hpp
│       ├── db.hpp
│       └── drivers/
│           ├── sqlite_driver.hpp
│           └── mysql_driver.hpp
└── tests/
    ├── CMakeLists.txt
    └── main_test.cpp
```

## 后续建议

- 为 MySQL 驱动补齐真正的参数化执行（`MYSQL_STMT`）
- 增加连接池健康检查/指标
- 增加统一错误码与错误分类
- 增加更多集成测试（真实 MySQL 服务）
