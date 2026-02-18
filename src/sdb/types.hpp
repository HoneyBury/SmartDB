#pragma once
#include <variant>
#include <string>
#include <vector>
#include <map>
#include <optional>
#include <utility>
#include <cstdint>
#include <nlohmann/json.hpp>

namespace sdb {

// 定义数据库支持的通用值类型
using DbValue = std::variant<
    std::monostate,         // Null
    int,                    // Integer
    int64_t,                // BigInt
    double,                 // Float/Double
    bool,                   // Boolean
    std::string,            // Text/Varchar
    std::vector<uint8_t>    // Blob/Binary
>;

enum class DbErrorKind {
 Unknown = 0,
 Configuration,
 Connection,
 Authentication,
 Timeout,
 NotFound,
 InvalidArgument,
 Transaction,
 Query,
 Execution,
 Internal
};

struct DbError {
 int code = 0;
 std::string message;
 DbErrorKind kind = DbErrorKind::Unknown;
 bool retryable = false;
};

template <typename T>
class DbResult {
public:
 static DbResult success(T value) {
     return DbResult(std::move(value));
 }

 static DbResult failure(std::string message, int code = 0) {
     return DbResult(DbError{code, std::move(message), DbErrorKind::Unknown, false});
 }

 static DbResult failure(std::string message, int code, DbErrorKind kind, bool retryable = false) {
     return DbResult(DbError{code, std::move(message), kind, retryable});
 }

 static DbResult failure(DbError error) {
     return DbResult(std::move(error));
 }

 bool ok() const { return value_.has_value(); }
 explicit operator bool() const { return ok(); }

 const T& value() const { return *value_; }
 T& value() { return *value_; }

 const DbError& error() const { return error_; }

private:
 explicit DbResult(T value) : value_(std::move(value)) {}
 explicit DbResult(DbError error) : value_(std::nullopt), error_(std::move(error)) {}

 std::optional<T> value_;
 DbError error_{};
};

template <>
class DbResult<void> {
public:
 static DbResult success() { return DbResult(true, {}); }

 static DbResult failure(std::string message, int code = 0) {
     return DbResult(false, DbError{code, std::move(message), DbErrorKind::Unknown, false});
 }

 static DbResult failure(std::string message, int code, DbErrorKind kind, bool retryable = false) {
     return DbResult(false, DbError{code, std::move(message), kind, retryable});
 }

 static DbResult failure(DbError error) {
     return DbResult(false, std::move(error));
 }

 bool ok() const { return ok_; }
 explicit operator bool() const { return ok(); }

 const DbError& error() const { return error_; }

private:
 DbResult(bool ok, DbError error) : ok_(ok), error_(std::move(error)) {}

 bool ok_ = false;
 DbError error_{};
};

// 辅助函数：判断是否为空
inline bool isNull(const DbValue& v) {
 return std::holds_alternative<std::monostate>(v);
}

// 辅助函数：转换为字符串（用于日志或简单展示）
inline std::string toString(const DbValue& v) {
 return std::visit([](auto&& arg) -> std::string {
     using T = std::decay_t<decltype(arg)>;
     if constexpr (std::is_same_v<T, std::monostate>) return "NULL";
     else if constexpr (std::is_same_v<T, std::string>) return arg;
     else if constexpr (std::is_same_v<T, std::vector<uint8_t>>) return "[BLOB]";
     else if constexpr (std::is_same_v<T, bool>) return arg ? "true" : "false";
     else return std::to_string(arg);
 }, v);
}

} // namespace sdb
