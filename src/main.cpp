#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <variant>

#include <spdlog/spdlog.h>

#include "sdb/db.hpp"
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

    // 0. 准备测试配置文件
    createTestConfigFile();

    auto& manager = sdb::DatabaseManager::instance();

    // 1. 注册驱动
    manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>());
    manager.registerDriver(std::make_shared<sdb::drivers::MysqlDriver>());

    // 2. 加载配置
    if (!manager.loadConfig("db_config.json")) {
        return -1;
    }

    try {
        // ==========================================
        // 使用配置名连接 MySQL（演示参数化执行）
        // ==========================================
        spdlog::info("--- Connecting to 'my_mysql' ---");
        auto mysqlConn = manager.createConnection("my_mysql");

        if (mysqlConn->open()) {
            spdlog::info("MySQL Connected!");

            mysqlConn->execute("DROP TABLE IF EXISTS test_tb");
            mysqlConn->execute("CREATE TABLE test_tb (id BIGINT PRIMARY KEY, val VARCHAR(255), active TINYINT, payload BLOB)");
            mysqlConn->execute("DELETE FROM test_tb WHERE id = 1");

            const int64_t affected = mysqlConn->execute(
                "INSERT INTO test_tb (id, val, active, payload) VALUES (?, ?, ?, ?)",
                {int64_t{1}, std::string("Prepared Works"), true, std::vector<uint8_t>{0x53, 0x44, 0x42}}
            );
            spdlog::info("MySQL insert affected rows: {}", affected);

            auto rs = mysqlConn->query("SELECT id, val, active, payload FROM test_tb WHERE id = 1");
            if (rs && rs->next()) {
                const auto id = std::get<int64_t>(rs->get("id"));
                const auto val = std::get<std::string>(rs->get("val"));
                const auto payload = std::get<std::vector<uint8_t>>(rs->get("payload"));
                spdlog::info("MySQL row => id={}, val={}, payload_size={}", id, val, payload.size());
            }
        } else {
            spdlog::warn("MySQL open failed: {}", mysqlConn->lastError());
        }

        // ==========================================
        // 使用配置名连接 SQLite
        // ==========================================
        spdlog::info("--- Connecting to 'my_sqlite' ---");
        auto sqliteConn = manager.createConnection("my_sqlite");
        if (sqliteConn->open()) {
            spdlog::info("SQLite Connected!");
            // 创建表并插入数据
            sqliteConn->execute("CREATE TABLE IF NOT EXISTS test_tb (id INTEGER, val TEXT)");
            sqliteConn->execute("INSERT INTO test_tb VALUES (1, 'Hello from SQLite!')");
            auto rs = sqliteConn->query("SELECT * FROM test_tb LIMIT 1");
            if (rs && rs->next()) {
                spdlog::info("Result: {}", std::get<std::string>(rs->get("val")));
            } else {
                spdlog::error("No results returned from SQLite query.");
            }
        }

    } catch (const std::exception& e) {
        spdlog::error("Error: {}", e.what());
    }

    return 0;
}
