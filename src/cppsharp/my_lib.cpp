//
// Created by HoneyBury on 25-6-22.
//
#include "cppsharp/my_lib.hpp"
#include <fmt/core.h> // 使用 fmt
// 使用我们的依赖库
#include <spdlog/spdlog.h>
void greet(const std::string& name) {
    // 使用 fmt 库格式化字符串
    std::string message = fmt::format("Hello, {}! Welcome to our modern C++ project.", name);

    // 使用 spdlog 记录信息
    spdlog::info(message);
}

void setup_logger() {
    // 设置一个全局的日志记录器
    spdlog::set_level(spdlog::level::debug);
    spdlog::set_pattern("[%Y-%m-%d %H:%M:%S.%e] [%^%l%$] %v");
}