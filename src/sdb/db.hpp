#pragma once
#include "idb.hpp"
#include "connection_pool.hpp"
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

    std::shared_ptr<ConnectionPool> createPool(const std::string& connectionName) {
        return createPool(connectionName, ConnectionPool::Options{});
    }

    std::shared_ptr<ConnectionPool> createPool(const std::string& connectionName,
                                               ConnectionPool::Options options) {
        options = normalizeOptions(options);
        const auto key = poolKeyForName(connectionName, options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                return cached;
            }
        }

        auto factory = [this, connectionName]() {
            return this->createConnection(connectionName);
        };
        auto pool = ConnectionPool::createWithFactory(std::move(factory), options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                return cached;
            }
            poolCache_[key] = pool;
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
        const auto key = poolKeyForRaw(driverName, config, options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                return cached;
            }
        }

        auto factory = [this, driverName, config]() {
            return this->createConnectionRaw(driverName, config);
        };
        auto pool = ConnectionPool::createWithFactory(std::move(factory), options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                return cached;
            }
            poolCache_[key] = pool;
        }
        return pool;
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
    std::mutex mtx_;
};

} // namespace sdb
