#pragma once
#include <variant>
#include <string>
#include <vector>
#include <map>
#include <optional>
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