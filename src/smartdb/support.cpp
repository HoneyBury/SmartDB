//
// Created by HoneyBury on 25-6-22.
//
#include "smartdb/support.hpp"
#include <fmt/core.h> // 使用 fmt
// 使用我们的依赖库
#include <spdlog/spdlog.h>

namespace sdb::support {

void greet(const std::string& name) {
    // 使用 fmt 库格式化字符串
    std::string message = fmt::format("Hello, {}! Welcome to SmartDB.", name);

    // 使用 spdlog 记录信息
    spdlog::info(message);
}

void setupLogger() {
    // 设置一个全局的日志记录器
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
}

} // namespace sdb::support
