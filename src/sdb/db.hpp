#pragma once
#include "idb.hpp"
#include "connection_pool.hpp"
#include <unordered_map>
#include <mutex>
#include <fstream>
#include <spdlog/spdlog.h>
#include <nlohmann/json.hpp>

namespace sdb {

class DatabaseManager {
public:
    DatabaseManager() = default;
    DatabaseManager(const DatabaseManager&) = delete;
    DatabaseManager& operator=(const DatabaseManager&) = delete;

    static DatabaseManager& instance() {
        static DatabaseManager inst;
        return inst;
    }

    bool registerDriver(std::shared_ptr<IDriver> driver) {
        if (!driver) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = "Driver is null";
            return false;
        }

        std::lock_guard<std::mutex> lock(mtx_);
        drivers_[driver->name()] = std::move(driver);
        lastError_.clear();
        return true;
    }

    bool loadConfig(const std::string& filePath) {
        std::ifstream f(filePath);
        if (!f.is_open()) {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                lastError_ = "Cannot open config file: " + filePath;
            }
            spdlog::error("Cannot open config file: {}", filePath);
            return false;
        }

        try {
            nlohmann::json j = nlohmann::json::parse(f);
            if (!j.contains("connections") || !j["connections"].is_object()) {
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    lastError_ = "Invalid config file format: missing object key 'connections'";
                }
                spdlog::error("Invalid config file format: missing object key 'connections'");
                return false;
            }

            std::lock_guard<std::mutex> lock(mtx_);
            configs_ = j["connections"];
            lastError_.clear();
            spdlog::info("Loaded {} connection configs.", configs_.size());
            return true;
        } catch (const std::exception& e) {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                lastError_ = std::string("JSON parse error: ") + e.what();
            }
            spdlog::error("JSON parse error: {}", e.what());
            return false;
        }
    }

    std::unique_ptr<IConnection> createConnection(const std::string& connectionName) {
        std::lock_guard<std::mutex> lock(mtx_);

        if (!configs_.contains(connectionName)) {
            lastError_ = "Connection config not found: " + connectionName;
            return {};
        }

        const auto& config = configs_[connectionName];
        std::string driverName = config.value("driver", "");
        if (driverName.empty()) {
            lastError_ = "Missing required field 'driver' for connection: " + connectionName;
            return {};
        }

        auto it = drivers_.find(driverName);
        if (it == drivers_.end()) {
            lastError_ = "Driver not supported or registered: " + driverName;
            return {};
        }

        auto conn = it->second->createConnection(config);
        if (!conn) {
            lastError_ = "Driver factory returned null connection: " + driverName;
            return {};
        }

        lastError_.clear();
        return conn;
    }

    std::unique_ptr<IConnection> createConnectionRaw(const std::string& driverName, const nlohmann::json& config) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = drivers_.find(driverName);
        if (it == drivers_.end()) {
            lastError_ = "Driver not found: " + driverName;
            return {};
        }
        auto conn = it->second->createConnection(config);
        if (!conn) {
            lastError_ = "Driver factory returned null connection: " + driverName;
            return {};
        }

        lastError_.clear();
        return conn;
    }

    std::shared_ptr<ConnectionPool> createPool(const std::string& connectionName) {
        return createPool(connectionName, ConnectionPool::Options{});
    }

    std::shared_ptr<ConnectionPool> createPool(const std::string& connectionName,
                                               ConnectionPool::Options options) {
        options = normalizeOptions(options);
        if (options.maxSize == 0) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = "ConnectionPool maxSize must be greater than 0";
            return {};
        }

        const auto key = poolKeyForName(connectionName, options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return cached;
            }
        }

        auto factory = [this, connectionName]() {
            return this->createConnection(connectionName);
        };
        std::shared_ptr<ConnectionPool> pool;
        try {
            pool = ConnectionPool::createWithFactory(std::move(factory), options);
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = std::string("Create pool failed: ") + e.what();
            return {};
        }

        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return cached;
            }
            poolCache_[key] = pool;
            lastError_.clear();
        }
        return pool;
    }

    std::shared_ptr<ConnectionPool> createPoolRaw(const std::string& driverName,
                                                  const nlohmann::json& config) {
        return createPoolRaw(driverName, config, ConnectionPool::Options{});
    }

    std::shared_ptr<ConnectionPool> createPoolRaw(const std::string& driverName,
                                                  const nlohmann::json& config,
                                                  ConnectionPool::Options options) {
        options = normalizeOptions(options);
        if (options.maxSize == 0) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = "ConnectionPool maxSize must be greater than 0";
            return {};
        }

        const auto key = poolKeyForRaw(driverName, config, options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return cached;
            }
            if (drivers_.find(driverName) == drivers_.end()) {
                lastError_ = "Driver not found: " + driverName;
                return {};
            }
        }

        auto factory = [this, driverName, config]() {
            return this->createConnectionRaw(driverName, config);
        };
        std::shared_ptr<ConnectionPool> pool;
        try {
            pool = ConnectionPool::createWithFactory(std::move(factory), options);
        } catch (const std::exception& e) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = std::string("Create pool failed: ") + e.what();
            return {};
        }

        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return cached;
            }
            poolCache_[key] = pool;
            lastError_.clear();
        }
        return pool;
    }

    std::string lastError() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return lastError_;
    }

private:
    static ConnectionPool::Options normalizeOptions(ConnectionPool::Options options) {
        if (options.minSize > options.maxSize) {
            options.minSize = options.maxSize;
        }
        return options;
    }

    static std::string optionsKey(const ConnectionPool::Options& options) {
        std::string key = "min=" + std::to_string(options.minSize);
        key += ";max=" + std::to_string(options.maxSize);
        key += ";wait=" + std::to_string(options.waitTimeout.count());
        key += ";borrow=" + std::to_string(options.testOnBorrow ? 1 : 0);
        key += ";return=" + std::to_string(options.testOnReturn ? 1 : 0);
        return key;
    }

    static std::string poolKeyForName(const std::string& connectionName,
                                      const ConnectionPool::Options& options) {
        return "name:" + connectionName + "|" + optionsKey(options);
    }

    static std::string poolKeyForRaw(const std::string& driverName,
                                     const nlohmann::json& config,
                                     const ConnectionPool::Options& options) {
        return "raw:" + driverName + "|" + config.dump() + "|" + optionsKey(options);
    }

    std::shared_ptr<ConnectionPool> getCachedPoolLocked(const std::string& key) {
        auto it = poolCache_.find(key);
        if (it == poolCache_.end()) {
            return {};
        }
        auto pool = it->second.lock();
        if (!pool) {
            poolCache_.erase(it);
        }
        return pool;
    }

    std::unordered_map<std::string, std::shared_ptr<IDriver>> drivers_;
    nlohmann::json configs_;
    std::unordered_map<std::string, std::weak_ptr<ConnectionPool>> poolCache_;
    mutable std::mutex mtx_;
    std::string lastError_;
};

} // namespace sdb
