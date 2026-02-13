#pragma once
#include "types.hpp"
#include <memory>
#include <string>
#include <vector>
#include <nlohmann/json.hpp>
namespace sdb {

// 结果集接口
class IResultSet {
public:
 virtual ~IResultSet() = default;

 // 移动游标到下一行
 virtual bool next() = 0;

 // 获取数据
 virtual DbValue get(int index) = 0;
 virtual DbValue get(const std::string& columnName) = 0;

 // 元数据
 virtual std::vector<std::string> columnNames() = 0;
};

// 数据库连接接口
class IConnection {
public:
 virtual ~IConnection() = default;

 virtual bool open() = 0;
 virtual void close() = 0;
 virtual bool isOpen() const = 0;

 // 基础执行
 // query: 用于 SELECT, 返回结果集
 virtual std::shared_ptr<IResultSet> query(const std::string& sql) = 0;
 // execute: 用于 INSERT/UPDATE/DELETE, 返回受影响行数
 virtual int64_t execute(const std::string& sql) = 0;

 // 预编译执行 (参数化查询，防止注入)
 virtual int64_t execute(const std::string& sql, const std::vector<DbValue>& params) = 0;

 // 事务支持
 virtual bool begin() = 0;
 virtual bool commit() = 0;
 virtual bool rollback() = 0;

 virtual std::string lastError() const = 0;
};

// 驱动工厂接口
class IDriver {
public:
 virtual ~IDriver() = default;
 // URL 格式示例: "file:mydb.sqlite" 或 "host=127.0.0.1;user=root..."
 virtual std::unique_ptr<IConnection> createConnection(const nlohmann::json& config) = 0;
 virtual std::string name() const = 0;
};

} // namespace sdb