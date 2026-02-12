#include <iostream>
#include <spdlog/spdlog.h>
#include <filesystem>
#include "sdb/db.hpp"
#include "sdb/drivers/sqlite_driver.hpp"
#include "sdb/drivers/mysql_driver.hpp"

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

    try
    {
     // ==========================================
     // 使用配置名连接 MySQL
     // ==========================================
     spdlog::info("--- Connecting to 'my_mysql' ---");
     auto mysqlConn = manager.createConnection("my_mysql");

     if (mysqlConn->open()) {
      spdlog::info("MySQL Connected!");
      mysqlConn->execute("CREATE TABLE IF NOT EXISTS test_tb (id INT, val VARCHAR(20))");
      mysqlConn->execute("INSERT INTO test_tb VALUES (1, 'JSON Config Works!')");

      auto rs = mysqlConn->query("SELECT * FROM test_tb LIMIT 1");
      if (rs && rs->next()) {
       spdlog::info("Result: {}", std::get<std::string>(rs->get("val")));
      }
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
      if (rs && rs->next()){
       spdlog::info("Result: {}", std::get<std::string>(rs->get("val")));
      }else{
       spdlog::error("No results returned from SQLite query.");
      }
     }

    } catch (const std::exception& e) {
        spdlog::error("Error: {}", e.what());
    }

    return 0;
}