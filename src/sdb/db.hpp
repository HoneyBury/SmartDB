#pragma once
#include "idb.hpp"
#include <unordered_map>
#include <mutex>
#include <fstream>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace sdb {

class DatabaseManager {
public:
    static DatabaseManager& instance() {
        static DatabaseManager inst;
        return inst;
    }

    // 1. 注册驱动
    void registerDriver(std::shared_ptr<IDriver> driver) {
        std::lock_guard<std::mutex> lock(mtx_);
        drivers_[driver->name()] = driver;
    }

    // 2. 加载配置文件
    bool loadConfig(const std::string& filePath) {
        std::ifstream f(filePath);
        if (!f.is_open()) {
            spdlog::error("Cannot open config file: {}", filePath);
            return false;
        }
        try {
            nlohmann::json j = nlohmann::json::parse(f);
            if (j.contains("connections")) {
                configs_ = j["connections"];
                spdlog::info("Loaded {} connection configs.", configs_.size());
                return true;
            }
        } catch (const std::exception& e) {
            spdlog::error("JSON parse error: {}", e.what());
        }
        return false;
    }

    // 3. 通过配置名称创建连接 (推荐)
    std::unique_ptr<IConnection> createConnection(const std::string& connectionName) {
        std::lock_guard<std::mutex> lock(mtx_);

        // 查找配置
        if (!configs_.contains(connectionName)) {
            throw std::runtime_error("Connection config not found: " + connectionName);
        }
        auto config = configs_[connectionName];

        // 获取驱动名称
        std::string driverName = config.value("driver", "");
        if (drivers_.find(driverName) == drivers_.end()) {
            throw std::runtime_error("Driver not supported or registered: " + driverName);
        }

        // 创建连接
        return drivers_[driverName]->createConnection(config);
    }

    // 4. (可选) 手动传递 JSON 创建连接
    std::unique_ptr<IConnection> createConnectionRaw(const std::string& driverName, const nlohmann::json& config) {
        std::lock_guard<std::mutex> lock(mtx_);
        if (drivers_.find(driverName) == drivers_.end()) {
             throw std::runtime_error("Driver not found: " + driverName);
        }
        return drivers_[driverName]->createConnection(config);
    }

private:
    std::unordered_map<std::string, std::shared_ptr<IDriver>> drivers_;
    nlohmann::json configs_; // 存储加载的配置
    std::mutex mtx_;
};

} // namespace sdb