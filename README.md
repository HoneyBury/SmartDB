# SmartDB（当前仓库状态说明）

SmartDB 是一个基于 **CMake + Conan 2** 的现代 C++ 项目，目前处于从通用脚手架向数据库抽象库演进的早期阶段。

> 说明：本 README 按当前仓库的真实内容整理，避免仅停留在模板描述。

## 项目现状

当前代码中已经存在两条主线：

1. **通用 C++ 示例库（cppsharp）**
   - `greet()`：基于 `fmt` 生成欢迎文案并通过 `spdlog` 输出。
   - `setup_logger()`：初始化日志级别与输出格式。
2. **SmartDB 类型层（sdb）**
   - `sdb::DbValue`：统一数据库字段值的变体类型（NULL/int/int64/double/bool/string/blob）。
   - `sdb::isNull()`：判断字段是否为空。
   - `sdb::toString()`：将字段值转换为字符串，便于日志与调试。

另外，`src/main.cpp` 中包含了面向 MySQL/SQLite 的数据库管理器调用示例（注册驱动、读取 JSON 配置、执行 SQL），用于展示目标架构方向。

## 当前目录结构

```text
SmartDB/
├── CMakeLists.txt              # 顶层构建、安装、CPack 打包配置
├── conanfile.py                # Conan 2 配方（依赖、布局、构建流程）
├── src/
│   ├── CMakeLists.txt
│   ├── main.cpp                # SmartDB 连接流程示例
│   ├── cppsharp/
│   │   ├── my_lib.hpp
│   │   └── my_lib.cpp
│   └── sdb/
│       └── types.hpp           # DbValue 及辅助函数
├── tests/
│   ├── CMakeLists.txt
│   └── main_test.cpp           # gtest 示例测试
├── cmake/                      # 自定义 CMake 模块与配置模板
├── assets/                     # 安装包图标
└── LICENSE
```

## 技术栈与依赖

- **语言标准**：C++17（代码校验最低 C++14）
- **构建系统**：CMake >= 3.23
- **包管理**：Conan >= 2.0
- **已声明三方库**：
  - `fmt`
  - `spdlog`
  - `nlohmann_json`
  - `sqlite3`
  - `libmysqlclient`
  - `gtest`

## 构建步骤（Conan + CMake）

### 1) 安装依赖

```bash
conan install . -s build_type=Debug --build=missing
conan install . -s build_type=Release --build=missing
```

### 2) 配置工程

```bash
cmake --preset conan-debug
cmake --preset conan-release
```

### 3) 编译

```bash
cmake --build --preset conan-debug
cmake --build --preset conan-release
```

### 4) 运行测试

```bash
ctest --preset conan-debug --output-on-failure
ctest --preset conan-release --output-on-failure
```

## 打包

项目已配置 CPack：

- Windows：NSIS
- Linux：DEB
- macOS：DragNDrop

示例：

```bash
cmake --build --preset conan-release --target package
```

## 开发提示（重要）

当前仓库中 `src/main.cpp` 与 `src/CMakeLists.txt` 引用了 `sdb/db.hpp`、`sdb/idb.hpp`、`sdb/drivers/*` 等文件，但这些文件尚未在仓库落地。

因此如果你直接构建完整示例程序，可能会因为缺失头文件而失败。建议下一步优先补齐以下模块：

1. `IDatabaseDriver` / `IConnection` / `IResultSet` 接口定义
2. `DatabaseManager`（驱动注册 + 配置装载 + 工厂创建连接）
3. SQLite / MySQL driver 最小可运行实现
4. 对应单元测试（连接生命周期、查询结果映射、错误路径）

---

如果你愿意，我可以下一步直接帮你把上述缺失的 `sdb` 核心接口和一个可跑通的 SQLite 最小实现补齐，并把 README 同步为“可运行版本”。
