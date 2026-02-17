#include <gtest/gtest.h>
#include "smartdb/support.hpp"
#include "sdb/types.hpp"
#include "sdb/db.hpp"
#include "sdb/connection_pool.hpp"
#include "sdb/drivers/sqlite_driver.hpp"
#include "sdb/drivers/mysql_driver.hpp"

#include <atomic>
#include <chrono>
#include <cstdlib>
#include <filesystem>
#include <fstream>
#include <future>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

class SupportTest : public ::testing::Test {
protected:
    void SetUp() override {
        sdb::support::setupLogger();
    }
};

TEST_F(SupportTest, GreetFunction) {
    ASSERT_NO_THROW(sdb::support::greet("Tester"));
}

TEST(SupportStandaloneTest, AlwaysPass) {
    EXPECT_EQ(1, 1);
    ASSERT_TRUE(true);
}

TEST(SdbTypesTest, ToStringAndNullHelpers) {
    sdb::DbValue nullValue = std::monostate{};
    sdb::DbValue intValue = int64_t{42};
    sdb::DbValue boolValue = true;

    EXPECT_TRUE(sdb::isNull(nullValue));
    EXPECT_EQ(sdb::toString(nullValue), "NULL");
    EXPECT_EQ(sdb::toString(intValue), "42");
    EXPECT_EQ(sdb::toString(boolValue), "true");
}

TEST(SqliteDriverTest, InMemoryInsertQueryAndBlob) {
    sdb::drivers::SqliteDriver driver;
    auto conn = driver.createConnection({{"path", ":memory:"}});

    auto openRes = conn->open();
    ASSERT_TRUE(openRes) << openRes.error().message;
    auto createRes = conn->execute("CREATE TABLE demo (id INTEGER, name TEXT, payload BLOB)");
    ASSERT_TRUE(createRes) << createRes.error().message;

    std::vector<uint8_t> blob{0x41, 0x42, 0x43};
    auto affectedRes = conn->execute(
        "INSERT INTO demo (id, name, payload) VALUES (?, ?, ?)",
        {int64_t{7}, std::string("smartdb"), blob}
    );
    ASSERT_TRUE(affectedRes) << affectedRes.error().message;
    ASSERT_EQ(affectedRes.value(), 1);

    auto rsRes = conn->query("SELECT id, name, payload FROM demo LIMIT 1");
    ASSERT_TRUE(rsRes) << rsRes.error().message;
    auto rs = rsRes.value();
    ASSERT_TRUE(rs && rs->next());

    EXPECT_EQ(std::get<int64_t>(rs->get("id")), 7);
    EXPECT_EQ(std::get<std::string>(rs->get("name")), "smartdb");

    auto payload = std::get<std::vector<uint8_t>>(rs->get("payload"));
    EXPECT_EQ(payload, blob);
}

TEST(ConnectionPoolTest, ReusesSingleConnection) {
    auto driver = std::make_shared<sdb::drivers::SqliteDriver>();
    sdb::ConnectionPool::Options options;
    options.maxSize = 1;
    options.minSize = 0;
    options.waitTimeout = std::chrono::milliseconds(0);

    auto poolRes = sdb::ConnectionPool::createWithFactory(
        [driver]() { return sdb::DbResult<std::unique_ptr<sdb::IConnection>>::success(driver->createConnection({{"path", ":memory:"}})); },
        options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    auto pool = poolRes.value();

    auto conn1Res = pool->acquire();
    ASSERT_TRUE(conn1Res) << conn1Res.error().message;
    auto conn1 = std::move(conn1Res.value());
    auto* firstPtr = conn1.get();
    conn1.reset();

    auto conn2Res = pool->acquire();
    ASSERT_TRUE(conn2Res) << conn2Res.error().message;
    auto conn2 = std::move(conn2Res.value());
    EXPECT_EQ(conn2.get(), firstPtr);
}

TEST(ConnectionPoolTest, ExhaustedPoolTimesOut) {
    auto driver = std::make_shared<sdb::drivers::SqliteDriver>();
    sdb::ConnectionPool::Options options;
    options.maxSize = 1;
    options.minSize = 0;
    options.waitTimeout = std::chrono::milliseconds(50);

    auto poolRes = sdb::ConnectionPool::createWithFactory(
        [driver]() { return sdb::DbResult<std::unique_ptr<sdb::IConnection>>::success(driver->createConnection({{"path", ":memory:"}})); },
        options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    auto pool = poolRes.value();

    auto conn1Res = pool->acquire();
    ASSERT_TRUE(conn1Res) << conn1Res.error().message;
    auto conn1 = std::move(conn1Res.value());

    auto conn2Res = pool->acquire();
    EXPECT_FALSE(conn2Res);
    EXPECT_NE(conn2Res.error().message.find("timed out"), std::string::npos);
    EXPECT_LE(pool->totalSize(), options.maxSize);
}

TEST(ConnectionPoolTest, ConcurrentAcquireRespectsMaxSize) {
    auto driver = std::make_shared<sdb::drivers::SqliteDriver>();
    sdb::ConnectionPool::Options options;
    options.maxSize = 4;
    options.minSize = 0;
    options.waitTimeout = std::chrono::milliseconds(500);

    auto poolRes = sdb::ConnectionPool::createWithFactory(
        [driver]() { return sdb::DbResult<std::unique_ptr<sdb::IConnection>>::success(driver->createConnection({{"path", ":memory:"}})); },
        options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    auto pool = poolRes.value();

    std::atomic<int> current{0};
    std::atomic<int> failures{0};
    int maxInUse = 0;
    std::mutex maxMtx;
    std::vector<std::thread> threads;
    threads.reserve(12);

    for (int i = 0; i < 12; ++i) {
        threads.emplace_back([&]() {
            auto connRes = pool->acquire();
            if (!connRes) {
                ++failures;
                return;
            }
            auto conn = std::move(connRes.value());
            const int inUse = ++current;
            {
                std::lock_guard<std::mutex> lock(maxMtx);
                if (inUse > maxInUse) {
                    maxInUse = inUse;
                }
            }
            std::this_thread::sleep_for(std::chrono::milliseconds(20));
            --current;
        });
    }

    for (auto& t : threads) {
        t.join();
    }

    EXPECT_EQ(failures.load(), 0);
    EXPECT_LE(maxInUse, static_cast<int>(options.maxSize));
    EXPECT_LE(pool->totalSize(), options.maxSize);
    EXPECT_EQ(pool->idleSize(), pool->totalSize());
}

TEST(ConnectionPoolTest, CreateFromDatabaseManagerConfig) {
    sdb::DatabaseManager manager;
    auto regRes = manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>());
    ASSERT_TRUE(regRes) << regRes.error().message;

    nlohmann::json j;
    j["connections"]["pool_sqlite"] = {
        {"driver", "sqlite"},
        {"path", ":memory:"}
    };

    const auto stamp = std::to_string(std::chrono::steady_clock::now().time_since_epoch().count());
    const auto path = std::filesystem::temp_directory_path() / ("smartdb_pool_config_" + stamp + ".json");
    {
        std::ofstream out(path);
        ASSERT_TRUE(out.is_open());
        out << j.dump(2);
    }

    auto loadRes = manager.loadConfig(path.string());
    ASSERT_TRUE(loadRes) << loadRes.error().message;

    sdb::ConnectionPool::Options options;
    options.maxSize = 2;
    options.waitTimeout = std::chrono::milliseconds(200);

    auto poolRes = manager.createPool("pool_sqlite", options);
    auto poolAgainRes = manager.createPool("pool_sqlite", options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    ASSERT_TRUE(poolAgainRes) << poolAgainRes.error().message;
    auto pool = poolRes.value();
    auto poolAgain = poolAgainRes.value();
    ASSERT_EQ(pool.get(), poolAgain.get());

    auto connRes = pool->acquire();
    ASSERT_TRUE(connRes) << connRes.error().message;
    auto conn = std::move(connRes.value());
    ASSERT_TRUE(conn->isOpen());
    auto execRes = conn->execute("CREATE TABLE IF NOT EXISTS pool_demo (id INTEGER)");
    ASSERT_TRUE(execRes) << execRes.error().message;

    std::filesystem::remove(path);
}

TEST(ConnectionPoolTest, CreateFromDatabaseManagerRaw) {
    sdb::DatabaseManager manager;
    auto regRes = manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>());
    ASSERT_TRUE(regRes) << regRes.error().message;

    sdb::ConnectionPool::Options options;
    options.maxSize = 1;
    options.waitTimeout = std::chrono::milliseconds(100);

    auto poolRes = manager.createPoolRaw("sqlite", {{"path", ":memory:"}}, options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    auto pool = poolRes.value();
    auto connRes = pool->acquire();
    ASSERT_TRUE(connRes) << connRes.error().message;
    auto conn = std::move(connRes.value());
    ASSERT_TRUE(conn->isOpen());
    auto execRes = conn->execute("CREATE TABLE IF NOT EXISTS pool_raw (id INTEGER)");
    ASSERT_TRUE(execRes) << execRes.error().message;
}

TEST(ConnectionPoolTest, DatabaseManagerPoolCacheReuseSameOptions) {
    sdb::DatabaseManager manager;
    auto regRes = manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>());
    ASSERT_TRUE(regRes) << regRes.error().message;

    sdb::ConnectionPool::Options options;
    options.maxSize = 2;
    options.waitTimeout = std::chrono::milliseconds(100);

    auto pool1Res = manager.createPoolRaw("sqlite", {{"path", ":memory:"}}, options);
    auto pool2Res = manager.createPoolRaw("sqlite", {{"path", ":memory:"}}, options);

    ASSERT_TRUE(pool1Res) << pool1Res.error().message;
    ASSERT_TRUE(pool2Res) << pool2Res.error().message;
    auto pool1 = pool1Res.value();
    auto pool2 = pool2Res.value();
    EXPECT_EQ(pool1.get(), pool2.get());
}

TEST(ConnectionPoolTest, DatabaseManagerPoolCacheSeparatesOptions) {
    sdb::DatabaseManager manager;
    auto regRes = manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>());
    ASSERT_TRUE(regRes) << regRes.error().message;

    sdb::ConnectionPool::Options optionsA;
    optionsA.maxSize = 1;
    optionsA.waitTimeout = std::chrono::milliseconds(100);

    sdb::ConnectionPool::Options optionsB = optionsA;
    optionsB.maxSize = 2;

    auto pool1Res = manager.createPoolRaw("sqlite", {{"path", ":memory:"}}, optionsA);
    auto pool2Res = manager.createPoolRaw("sqlite", {{"path", ":memory:"}}, optionsB);

    ASSERT_TRUE(pool1Res) << pool1Res.error().message;
    ASSERT_TRUE(pool2Res) << pool2Res.error().message;
    auto pool1 = pool1Res.value();
    auto pool2 = pool2Res.value();
    EXPECT_NE(pool1.get(), pool2.get());
}

TEST(DatabaseManagerTest, MissingConfigUsesLastErrorInsteadOfException) {
    sdb::DatabaseManager manager;
    auto regRes = manager.registerDriver(std::make_shared<sdb::drivers::SqliteDriver>());
    ASSERT_TRUE(regRes) << regRes.error().message;

    auto connRes = manager.createConnection("missing_name");
    EXPECT_FALSE(connRes);
    EXPECT_NE(connRes.error().message.find("Connection config not found"), std::string::npos);
}

TEST(DatabaseManagerTest, CreatePoolRawUnknownDriverShouldFailGracefully) {
    sdb::DatabaseManager manager;
    auto poolRes = manager.createPoolRaw("unknown_driver", {{"path", ":memory:"}});
    EXPECT_FALSE(poolRes);
    EXPECT_NE(poolRes.error().message.find("Driver not found"), std::string::npos);
}


namespace {

bool mysqlTestEnabled() {
    const char* enabled = std::getenv("SMARTDB_MYSQL_TEST_ENABLE");
    if (!enabled) {
        return false;
    }
    const std::string value(enabled);
    return value == "1" || value == "true" || value == "TRUE" || value == "on" || value == "ON";
}

nlohmann::json mysqlConfigFromEnv() {
    nlohmann::json cfg;
    const auto read = [](const char* key, const char* fallback) {
        const char* value = std::getenv(key);
        return std::string(value ? value : fallback);
    };

    cfg["host"] = read("SMARTDB_MYSQL_HOST", "127.0.0.1");
    cfg["port"] = std::stoi(read("SMARTDB_MYSQL_PORT", "3306"));
    cfg["user"] = read("SMARTDB_MYSQL_USER", "root");
    cfg["password"] = read("SMARTDB_MYSQL_PASSWORD", "root");
    cfg["database"] = read("SMARTDB_MYSQL_DATABASE", "my_app");
    cfg["charset"] = read("SMARTDB_MYSQL_CHARSET", "utf8mb4");
    return cfg;
}

} // namespace

TEST(MysqlDriverTest, ParameterizedInsertAndQueryTypes) {
    if (!mysqlTestEnabled()) {
        GTEST_SKIP() << "Set SMARTDB_MYSQL_TEST_ENABLE=1 to run MySQL integration tests.";
    }

    sdb::drivers::MysqlDriver driver;
    auto conn = driver.createConnection(mysqlConfigFromEnv());

    auto openRes = conn->open();
    ASSERT_TRUE(openRes) << openRes.error().message;
    auto createRes = conn->execute("CREATE TABLE IF NOT EXISTS smartdb_mysql_test (id BIGINT PRIMARY KEY, name VARCHAR(64), enabled BIT(1), payload BLOB)");
    ASSERT_TRUE(createRes) << createRes.error().message;
    auto cleanupRes = conn->execute("DELETE FROM smartdb_mysql_test WHERE id IN (1001, 1002)");
    ASSERT_TRUE(cleanupRes) << cleanupRes.error().message;

    const std::vector<uint8_t> payload{0x00, 0x01, 0x7f, 0xff};
    auto ins1 = conn->execute(
        "INSERT INTO smartdb_mysql_test (id, name, enabled, payload) VALUES (?, ?, ?, ?)",
        {int64_t{1001}, std::string("row-enabled"), true, payload});
    ASSERT_TRUE(ins1) << ins1.error().message;
    ASSERT_EQ(ins1.value(), 1);

    auto ins2 = conn->execute(
        "INSERT INTO smartdb_mysql_test (id, name, enabled, payload) VALUES (?, ?, ?, ?)",
        {int64_t{1002}, std::string("row-disabled"), false, payload});
    ASSERT_TRUE(ins2) << ins2.error().message;
    ASSERT_EQ(ins2.value(), 1);

    auto rsRes = conn->query("SELECT id, name, enabled, payload FROM smartdb_mysql_test WHERE id = 1002");
    ASSERT_TRUE(rsRes) << rsRes.error().message;
    auto rs = rsRes.value();
    ASSERT_TRUE(rs && rs->next());

    EXPECT_EQ(std::get<int64_t>(rs->get("id")), 1002);
    EXPECT_EQ(std::get<std::string>(rs->get("name")), "row-disabled");
    EXPECT_FALSE(std::get<bool>(rs->get("enabled")));
    EXPECT_EQ(std::get<std::vector<uint8_t>>(rs->get("payload")), payload);
}

TEST(MysqlDriverTest, ParameterCountMismatchShouldFail) {
    if (!mysqlTestEnabled()) {
        GTEST_SKIP() << "Set SMARTDB_MYSQL_TEST_ENABLE=1 to run MySQL integration tests.";
    }

    sdb::drivers::MysqlDriver driver;
    auto conn = driver.createConnection(mysqlConfigFromEnv());

    auto openRes = conn->open();
    ASSERT_TRUE(openRes) << openRes.error().message;

    auto execRes = conn->execute("INSERT INTO smartdb_mysql_test (id, name) VALUES (?, ?)", {int64_t{3001}});
    EXPECT_FALSE(execRes);
    EXPECT_NE(execRes.error().message.find("parameter count mismatch"), std::string::npos);
}

TEST(MysqlDriverTest, OpenCloseShouldBeIdempotent) {
    if (!mysqlTestEnabled()) {
        GTEST_SKIP() << "Set SMARTDB_MYSQL_TEST_ENABLE=1 to run MySQL integration tests.";
    }

    sdb::drivers::MysqlDriver driver;
    auto conn = driver.createConnection(mysqlConfigFromEnv());

    auto open1 = conn->open();
    ASSERT_TRUE(open1) << open1.error().message;
    auto open2 = conn->open();
    ASSERT_TRUE(open2) << open2.error().message;
    ASSERT_TRUE(conn->isOpen());

    conn->close();
    conn->close();
    EXPECT_FALSE(conn->isOpen());
}
