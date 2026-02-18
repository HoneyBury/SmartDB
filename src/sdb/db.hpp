#pragma once
#include "idb.hpp"
#include "connection_pool.hpp"
#include "logging.hpp"
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

    DbResult<void> registerDriver(std::shared_ptr<IDriver> driver) {
        if (!driver) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = "Driver is null";
            errorCounters_.increment(DbErrorKind::InvalidArgument);
            logDbError(spdlog::level::warn, "db_manager_register_driver",
                       DbError{0, lastError_, DbErrorKind::InvalidArgument, false});
            return DbResult<void>::failure(lastError_, 0, DbErrorKind::InvalidArgument, false);
        }

        std::lock_guard<std::mutex> lock(mtx_);
        drivers_[driver->name()] = std::move(driver);
        lastError_.clear();
        return DbResult<void>::success();
    }

    DbResult<void> registerDriver(std::shared_ptr<IDriver> driver, const OperationContext& ctx) {
        OperationScope scope(ctx);
        return registerDriver(std::move(driver));
    }

    DbResult<void> loadConfig(const std::string& filePath) {
        std::ifstream f(filePath);
        if (!f.is_open()) {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                lastError_ = "Cannot open config file: " + filePath;
                errorCounters_.increment(DbErrorKind::Configuration);
            }
            logDbError(spdlog::level::err, "db_manager_load_config",
                       DbError{0, lastError_, DbErrorKind::Configuration, false});
            return DbResult<void>::failure(lastError_, 0, DbErrorKind::Configuration, false);
        }

        try {
            nlohmann::json j = nlohmann::json::parse(f);
            if (!j.contains("connections") || !j["connections"].is_object()) {
                {
                    std::lock_guard<std::mutex> lock(mtx_);
                    lastError_ = "Invalid config file format: missing object key 'connections'";
                    errorCounters_.increment(DbErrorKind::Configuration);
                }
                logDbError(spdlog::level::err, "db_manager_load_config",
                           DbError{0, lastError_, DbErrorKind::Configuration, false});
                return DbResult<void>::failure(lastError_, 0, DbErrorKind::Configuration, false);
            }

            std::lock_guard<std::mutex> lock(mtx_);
            configs_ = j["connections"];
            lastError_.clear();
            spdlog::info("Loaded {} connection configs.", configs_.size());
            return DbResult<void>::success();
        } catch (const std::exception& e) {
            {
                std::lock_guard<std::mutex> lock(mtx_);
                lastError_ = std::string("JSON parse error: ") + e.what();
                errorCounters_.increment(DbErrorKind::Configuration);
            }
            logDbError(spdlog::level::err, "db_manager_load_config",
                       DbError{0, lastError_, DbErrorKind::Configuration, false});
            return DbResult<void>::failure(lastError_, 0, DbErrorKind::Configuration, false);
        }
    }

    DbResult<void> loadConfig(const std::string& filePath, const OperationContext& ctx) {
        OperationScope scope(ctx);
        return loadConfig(filePath);
    }

    DbResult<std::unique_ptr<IConnection>> createConnection(const std::string& connectionName) {
        std::lock_guard<std::mutex> lock(mtx_);

        if (!configs_.contains(connectionName)) {
            lastError_ = "Connection config not found: " + connectionName;
            errorCounters_.increment(DbErrorKind::NotFound);
            logDbError(spdlog::level::warn, "db_manager_create_connection",
                       DbError{0, lastError_, DbErrorKind::NotFound, false});
            return DbResult<std::unique_ptr<IConnection>>::failure(lastError_, 0, DbErrorKind::NotFound, false);
        }

        const auto& config = configs_[connectionName];
        std::string driverName = config.value("driver", "");
        if (driverName.empty()) {
            lastError_ = "Missing required field 'driver' for connection: " + connectionName;
            errorCounters_.increment(DbErrorKind::Configuration);
            logDbError(spdlog::level::warn, "db_manager_create_connection",
                       DbError{0, lastError_, DbErrorKind::Configuration, false});
            return DbResult<std::unique_ptr<IConnection>>::failure(lastError_, 0, DbErrorKind::Configuration, false);
        }

        auto it = drivers_.find(driverName);
        if (it == drivers_.end()) {
            lastError_ = "Driver not supported or registered: " + driverName;
            errorCounters_.increment(DbErrorKind::NotFound);
            logDbError(spdlog::level::warn, "db_manager_create_connection",
                       DbError{0, lastError_, DbErrorKind::NotFound, false});
            return DbResult<std::unique_ptr<IConnection>>::failure(lastError_, 0, DbErrorKind::NotFound, false);
        }

        auto conn = it->second->createConnection(config);
        if (!conn) {
            lastError_ = "Driver factory returned null connection: " + driverName;
            errorCounters_.increment(DbErrorKind::Internal);
            logDbError(spdlog::level::warn, "db_manager_create_connection",
                       DbError{0, lastError_, DbErrorKind::Internal, true});
            return DbResult<std::unique_ptr<IConnection>>::failure(lastError_, 0, DbErrorKind::Internal, true);
        }

        lastError_.clear();
        return DbResult<std::unique_ptr<IConnection>>::success(std::move(conn));
    }

    DbResult<std::unique_ptr<IConnection>> createConnection(const std::string& connectionName,
                                                            const OperationContext& ctx) {
        OperationScope scope(ctx);
        return createConnection(connectionName);
    }

    DbResult<std::unique_ptr<IConnection>> createConnectionRaw(const std::string& driverName, const nlohmann::json& config) {
        std::lock_guard<std::mutex> lock(mtx_);
        auto it = drivers_.find(driverName);
        if (it == drivers_.end()) {
            lastError_ = "Driver not found: " + driverName;
            errorCounters_.increment(DbErrorKind::NotFound);
            logDbError(spdlog::level::warn, "db_manager_create_connection_raw",
                       DbError{0, lastError_, DbErrorKind::NotFound, false});
            return DbResult<std::unique_ptr<IConnection>>::failure(lastError_, 0, DbErrorKind::NotFound, false);
        }
        auto conn = it->second->createConnection(config);
        if (!conn) {
            lastError_ = "Driver factory returned null connection: " + driverName;
            errorCounters_.increment(DbErrorKind::Internal);
            logDbError(spdlog::level::warn, "db_manager_create_connection_raw",
                       DbError{0, lastError_, DbErrorKind::Internal, true});
            return DbResult<std::unique_ptr<IConnection>>::failure(lastError_, 0, DbErrorKind::Internal, true);
        }

        lastError_.clear();
        return DbResult<std::unique_ptr<IConnection>>::success(std::move(conn));
    }

    DbResult<std::unique_ptr<IConnection>> createConnectionRaw(const std::string& driverName,
                                                               const nlohmann::json& config,
                                                               const OperationContext& ctx) {
        OperationScope scope(ctx);
        return createConnectionRaw(driverName, config);
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPool(const std::string& connectionName) {
        return createPool(connectionName, ConnectionPool::Options{});
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPool(const std::string& connectionName,
                                                         const OperationContext& ctx) {
        OperationScope scope(ctx);
        return createPool(connectionName, ConnectionPool::Options{});
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPool(const std::string& connectionName,
                                                         ConnectionPool::Options options) {
        options = normalizeOptions(options);
        if (options.maxSize == 0) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = "ConnectionPool maxSize must be greater than 0";
            errorCounters_.increment(DbErrorKind::InvalidArgument);
            logDbError(spdlog::level::warn, "db_manager_create_pool",
                       DbError{0, lastError_, DbErrorKind::InvalidArgument, false});
            return DbResult<std::shared_ptr<ConnectionPool>>::failure(lastError_, 0, DbErrorKind::InvalidArgument, false);
        }

        const auto key = poolKeyForName(connectionName, options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return DbResult<std::shared_ptr<ConnectionPool>>::success(cached);
            }
        }

        auto factory = [this, connectionName]() {
            return this->createConnection(connectionName);
        };
        auto poolRes = ConnectionPool::createWithFactory(std::move(factory), options);
        if (!poolRes) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = poolRes.error().message;
            errorCounters_.increment(poolRes.error().kind);
            logDbError(spdlog::level::warn, "db_manager_create_pool", poolRes.error());
            return DbResult<std::shared_ptr<ConnectionPool>>::failure(poolRes.error());
        }
        auto pool = std::move(poolRes.value());

        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return DbResult<std::shared_ptr<ConnectionPool>>::success(cached);
            }
            poolCache_[key] = pool;
            lastError_.clear();
        }
        return DbResult<std::shared_ptr<ConnectionPool>>::success(std::move(pool));
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPool(const std::string& connectionName,
                                                         ConnectionPool::Options options,
                                                         const OperationContext& ctx) {
        OperationScope scope(ctx);
        return createPool(connectionName, options);
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPoolRaw(const std::string& driverName,
                                                            const nlohmann::json& config) {
        return createPoolRaw(driverName, config, ConnectionPool::Options{});
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPoolRaw(const std::string& driverName,
                                                            const nlohmann::json& config,
                                                            const OperationContext& ctx) {
        OperationScope scope(ctx);
        return createPoolRaw(driverName, config, ConnectionPool::Options{});
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPoolRaw(const std::string& driverName,
                                                            const nlohmann::json& config,
                                                            ConnectionPool::Options options) {
        options = normalizeOptions(options);
        if (options.maxSize == 0) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = "ConnectionPool maxSize must be greater than 0";
            errorCounters_.increment(DbErrorKind::InvalidArgument);
            logDbError(spdlog::level::warn, "db_manager_create_pool_raw",
                       DbError{0, lastError_, DbErrorKind::InvalidArgument, false});
            return DbResult<std::shared_ptr<ConnectionPool>>::failure(lastError_, 0, DbErrorKind::InvalidArgument, false);
        }

        const auto key = poolKeyForRaw(driverName, config, options);
        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return DbResult<std::shared_ptr<ConnectionPool>>::success(cached);
            }
            if (drivers_.find(driverName) == drivers_.end()) {
                lastError_ = "Driver not found: " + driverName;
                errorCounters_.increment(DbErrorKind::NotFound);
                logDbError(spdlog::level::warn, "db_manager_create_pool_raw",
                           DbError{0, lastError_, DbErrorKind::NotFound, false});
                return DbResult<std::shared_ptr<ConnectionPool>>::failure(lastError_, 0, DbErrorKind::NotFound, false);
            }
        }

        auto factory = [this, driverName, config]() {
            return this->createConnectionRaw(driverName, config);
        };
        auto poolRes = ConnectionPool::createWithFactory(std::move(factory), options);
        if (!poolRes) {
            std::lock_guard<std::mutex> lock(mtx_);
            lastError_ = poolRes.error().message;
            errorCounters_.increment(poolRes.error().kind);
            logDbError(spdlog::level::warn, "db_manager_create_pool_raw", poolRes.error());
            return DbResult<std::shared_ptr<ConnectionPool>>::failure(poolRes.error());
        }
        auto pool = std::move(poolRes.value());

        {
            std::lock_guard<std::mutex> lock(mtx_);
            auto cached = getCachedPoolLocked(key);
            if (cached) {
                lastError_.clear();
                return DbResult<std::shared_ptr<ConnectionPool>>::success(cached);
            }
            poolCache_[key] = pool;
            lastError_.clear();
        }
        return DbResult<std::shared_ptr<ConnectionPool>>::success(std::move(pool));
    }

    DbResult<std::shared_ptr<ConnectionPool>> createPoolRaw(const std::string& driverName,
                                                            const nlohmann::json& config,
                                                            ConnectionPool::Options options,
                                                            const OperationContext& ctx) {
        OperationScope scope(ctx);
        return createPoolRaw(driverName, config, options);
    }

    std::string lastError() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return lastError_;
    }

    DbErrorCounters errorCounters() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return errorCounters_;
    }

    void resetErrorCounters() {
        std::lock_guard<std::mutex> lock(mtx_);
        errorCounters_ = {};
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
    DbErrorCounters errorCounters_{};
};

} // namespace sdb
