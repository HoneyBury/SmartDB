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

 virtual DbResult<void> open() = 0;
 virtual void close() = 0;
 virtual bool isOpen() const = 0;

 // 基础执行
 // query: 用于 SELECT, 返回结果集
 virtual DbResult<std::shared_ptr<IResultSet>> query(const std::string& sql) = 0;
 // execute: 用于 INSERT/UPDATE/DELETE, 返回受影响行数
 virtual DbResult<int64_t> execute(const std::string& sql) = 0;

 // 预编译执行 (参数化查询，防止注入)
 virtual DbResult<int64_t> execute(const std::string& sql, const std::vector<DbValue>& params) = 0;

 // 事务支持
 virtual DbResult<void> begin() = 0;
 virtual DbResult<void> commit() = 0;
 virtual DbResult<void> rollback() = 0;
};

class TransactionGuard {
public:
 static DbResult<TransactionGuard> begin(IConnection& conn) {
     auto res = conn.begin();
     if (!res) {
         return DbResult<TransactionGuard>::failure(res.error().message, res.error().code);
     }
     return DbResult<TransactionGuard>::success(TransactionGuard(conn));
 }

 TransactionGuard(TransactionGuard&& other) noexcept
     : conn_(other.conn_), active_(other.active_) {
     other.conn_ = nullptr;
     other.active_ = false;
 }

 TransactionGuard& operator=(TransactionGuard&& other) noexcept {
     if (this == &other) {
         return *this;
     }
     if (active_ && conn_) {
         (void)conn_->rollback();
     }
     conn_ = other.conn_;
     active_ = other.active_;
     other.conn_ = nullptr;
     other.active_ = false;
     return *this;
 }

 TransactionGuard(const TransactionGuard&) = delete;
 TransactionGuard& operator=(const TransactionGuard&) = delete;

 ~TransactionGuard() {
     if (active_ && conn_) {
         (void)conn_->rollback();
     }
 }

 DbResult<void> commit() {
     if (!active_ || !conn_) {
         return DbResult<void>::failure("Transaction is not active");
     }
     auto res = conn_->commit();
     if (!res) {
         return res;
     }
     active_ = false;
     return DbResult<void>::success();
 }

 DbResult<void> rollback() {
     if (!active_ || !conn_) {
         return DbResult<void>::failure("Transaction is not active");
     }
     auto res = conn_->rollback();
     if (!res) {
         return res;
     }
     active_ = false;
     return DbResult<void>::success();
 }

 bool active() const { return active_; }

private:
 explicit TransactionGuard(IConnection& conn) : conn_(&conn), active_(true) {}

 IConnection* conn_ = nullptr;
 bool active_ = false;
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
