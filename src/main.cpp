#include <chrono>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <variant>

#include <spdlog/spdlog.h>

#include "sdb/db.hpp"
#include "sdb/connection_pool.hpp"
#include "sdb/logging.hpp"
#include "sdb/drivers/mysql_driver.hpp"
#include "sdb/drivers/sqlite_driver.hpp"

// 一个简单的辅助函数来生成配置文件，方便测试
void createTestConfigFile() {
    nlohmann::json j;
    j["connections"]["my_mysql"] = {
        {"driver", "mysql"},
        {"host", "127.0.0.1"},
        {"port", 3306},
        {"user", "root"},
        {"password", "root"},
        {"database", "my_app"}
    };
    j["connections"]["my_sqlite"] = {
        {"driver", "sqlite"},
        {"path", "local_data.db"} // 或者 :memory:
    };

    std::ofstream o("db_config.json");
    o << std::setw(4) << j << std::endl;
}

int main() {
    spdlog::set_level(spdlog::level::debug);
    const auto rootCtx = sdb::makeOperationContext("app_main");
    sdb::OperationScope rootScope(rootCtx);
    sdb::logOperationEvent(spdlog::level::info, "startup", "application_start", rootCtx);

    // 0. 准备测试配置文件
    createTestConfigFile();

    sdb::DatabaseManager manager;

    // 1. 注册驱动
    auto regCtx = sdb::childOperationContext(rootCtx, "register_driver");
    auto sqliteReg = manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>(), regCtx);
    auto mysqlReg = manager.registerDriver(std::make_shared<sdb::drivers::MysqlDriver>(), regCtx);
    if (!sqliteReg || !mysqlReg) {
        sdb::logResultError(
            spdlog::level::err,
            "register_driver",
            !sqliteReg ? sqliteReg : mysqlReg,
            regCtx);
        return -1;
    }

    // 2. 加载配置
    auto loadCtx = sdb::childOperationContext(rootCtx, "load_config");
    auto loadRes = manager.loadConfig("db_config.json", loadCtx);
    if (!loadRes) {
        sdb::logResultError(spdlog::level::err, "load_config", loadRes, loadCtx);
        return -1;
    }

    try {
        // ==========================================
        // 使用配置名连接 MySQL（演示参数化执行）
        // ==========================================
        spdlog::info("--- Connecting to 'my_mysql' ---");
        auto mysqlCreateCtx = sdb::childOperationContext(rootCtx, "create_mysql_connection");
        auto mysqlConnRes = manager.createConnection("my_mysql", mysqlCreateCtx);
        if (!mysqlConnRes) {
            sdb::logResultError(
                spdlog::level::warn,
                "create_mysql_connection",
                mysqlConnRes,
                mysqlCreateCtx);
            return -1;
        }
        auto mysqlConn = std::move(mysqlConnRes.value());

        auto mysqlOpenCtx = sdb::childOperationContext(rootCtx, "open_mysql_connection");
        sdb::OperationScope mysqlOpenScope(mysqlOpenCtx);
        auto mysqlOpen = mysqlConn->open();
        if (mysqlOpen) {
            spdlog::info("MySQL Connected!");

            mysqlConn->execute("DROP TABLE IF EXISTS test_tb");
            mysqlConn->execute("CREATE TABLE test_tb (id BIGINT PRIMARY KEY, val VARCHAR(255), active TINYINT, payload BLOB)");
            mysqlConn->execute("DELETE FROM test_tb WHERE id = 1");

            auto affectedRes = mysqlConn->execute(
                "INSERT INTO test_tb (id, val, active, payload) VALUES (?, ?, ?, ?)",
                {int64_t{1}, std::string("Prepared Works"), true, std::vector<uint8_t>{0x53, 0x44, 0x42}}
            );
            if (!affectedRes) {
                spdlog::warn("MySQL insert failed: {}", affectedRes.error().message);
            } else {
                spdlog::info("MySQL insert affected rows: {}", affectedRes.value());
            }

            auto rsRes = mysqlConn->query("SELECT id, val, active, payload FROM test_tb WHERE id = 1");
            if (rsRes && rsRes.value()->next()) {
                const auto id = std::get<int64_t>(rsRes.value()->get("id"));
                const auto val = std::get<std::string>(rsRes.value()->get("val"));
                const auto payload = std::get<std::vector<uint8_t>>(rsRes.value()->get("payload"));
                spdlog::info("MySQL row => id={}, val={}, payload_size={}", id, val, payload.size());
            }
        } else {
            sdb::logResultError(
                spdlog::level::warn,
                "open_mysql_connection",
                mysqlOpen,
                mysqlOpenCtx);
        }

        // ==========================================
        // 使用配置名连接 SQLite
        // ==========================================
        spdlog::info("--- Connecting to 'my_sqlite' ---");
        auto sqliteCreateCtx = sdb::childOperationContext(rootCtx, "create_sqlite_connection");
        auto sqliteConnRes = manager.createConnection("my_sqlite", sqliteCreateCtx);
        if (!sqliteConnRes) {
            sdb::logResultError(
                spdlog::level::warn,
                "create_sqlite_connection",
                sqliteConnRes,
                sqliteCreateCtx);
            return -1;
        }
        auto sqliteConn = std::move(sqliteConnRes.value());
        auto sqliteOpenCtx = sdb::childOperationContext(rootCtx, "open_sqlite_connection");
        sdb::OperationScope sqliteOpenScope(sqliteOpenCtx);
        auto sqliteOpen = sqliteConn->open();
        if (sqliteOpen) {
            spdlog::info("SQLite Connected!");
            // 创建表并插入数据
            sqliteConn->execute("CREATE TABLE IF NOT EXISTS test_tb (id INTEGER, val TEXT)");
            sqliteConn->execute("INSERT INTO test_tb VALUES (1, 'Hello from SQLite!')");
            auto rsRes = sqliteConn->query("SELECT * FROM test_tb LIMIT 1");
            if (rsRes && rsRes.value()->next()) {
                spdlog::info("Result: {}", std::get<std::string>(rsRes.value()->get("val")));
            } else {
                spdlog::error("No results returned from SQLite query.");
            }
        }

        // ==========================================
        // 使用连接池连接 SQLite
        // ==========================================
        spdlog::info("--- Pooling 'my_sqlite' ---");
        sdb::ConnectionPool::Options poolOptions;
        poolOptions.minSize = 1;
        poolOptions.maxSize = 4;
        poolOptions.waitTimeout = std::chrono::milliseconds(2000);

        auto poolCreateCtx = sdb::childOperationContext(rootCtx, "create_sqlite_pool");
        auto poolRes = manager.createPool("my_sqlite", poolOptions, poolCreateCtx);
        if (!poolRes) {
            sdb::logResultError(
                spdlog::level::warn,
                "create_sqlite_pool",
                poolRes,
                poolCreateCtx);
            return -1;
        }
        auto pool = poolRes.value();
        auto poolAcquireCtx = sdb::childOperationContext(rootCtx, "acquire_pooled_connection");
        auto pooledConnRes = pool->acquire(poolAcquireCtx);
        if (pooledConnRes) {
            auto pooledConn = std::move(pooledConnRes.value());
            pooledConn->execute("CREATE TABLE IF NOT EXISTS pool_tb (id INTEGER, val TEXT)");
            pooledConn->execute("INSERT INTO pool_tb VALUES (1, 'Hello from Pool!')");
            auto rsRes = pooledConn->query("SELECT val FROM pool_tb WHERE id = 1");
            if (rsRes && rsRes.value()->next()) {
                spdlog::info("Pool result: {}", std::get<std::string>(rsRes.value()->get("val")));
            }
        } else {
            sdb::logResultError(
                spdlog::level::warn,
                "acquire_pooled_connection",
                pooledConnRes,
                poolAcquireCtx);
        }

    } catch (const std::exception& e) {
        sdb::DbError err{0, e.what(), sdb::DbErrorKind::Internal, false};
        sdb::logDbError(spdlog::level::err, "unhandled_exception", err, sdb::childOperationContext(rootCtx, "exception"));
    }

    sdb::logOperationEvent(spdlog::level::info, "shutdown", "application_end", rootCtx);
    return 0;
}
