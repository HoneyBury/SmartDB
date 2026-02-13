#pragma once
#include "idb.hpp"
#include <unordered_map>
#include <mutex>
#include <fstream>
#include <stdexcept>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace sdb {

class DatabaseManager {
public:
    static DatabaseManager& instance() {
        static DatabaseManager inst;
        return inst;
    }

    void registerDriver(std::shared_ptr<IDriver> driver) {
        if (!driver) {
            throw std::invalid_argument("Driver is null");
        }

        std::lock_guard<std::mutex> lock(mtx_);
        drivers_[driver->name()] = std::move(driver);
    }

    bool loadConfig(const std::string& filePath) {
        std::ifstream f(filePath);
        if (!f.is_open()) {
            spdlog::error("Cannot open config file: {}", filePath);
            return false;
        }

        try {
            nlohmann::json j = nlohmann::json::parse(f);
            if (!j.contains("connections") || !j["connections"].is_object()) {
                spdlog::error("Invalid config file format: missing object key 'connections'");
                return false;
            }

            std::lock_guard<std::mutex> lock(mtx_);
            configs_ = j["connections"];
            spdlog::info("Loaded {} connection configs.", configs_.size());
            return true;
        } catch (const std::exception& e) {
            spdlog::error("JSON parse error: {}", e.what());
            return false;
        }
    }

    std::unique_ptr<IConnection> createConnection(const std::string& connectionName) {
        std::lock_guard<std::mutex> lock(mtx_);

        if (!configs_.contains(connectionName)) {
            throw std::runtime_error("Connection config not found: " + connectionName);
        }

        const auto& config = configs_[connectionName];
        std::string driverName = config.value("driver", "");
        if (driverName.empty()) {
            throw std::runtime_error("Missing required field 'driver' for connection: " + connectionName);
        }

        auto it = drivers_.find(driverName);
        if (it == drivers_.end()) {
            throw std::runtime_error("Driver not supported or registered: " + driverName);
        }

        return it->second->createConnection(config);
    }

    std::unique_ptr<IConnection> createConnectionRaw(const std::string& driverName, const nlohmann::json& config) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = drivers_.find(driverName);
        if (it == drivers_.end()) {
            throw std::runtime_error("Driver not found: " + driverName);
        }
        return it->second->createConnection(config);
    }

private:
    std::unordered_map<std::string, std::shared_ptr<IDriver>> drivers_;
    nlohmann::json configs_;
    std::mutex mtx_;
};

} // namespace sdb
