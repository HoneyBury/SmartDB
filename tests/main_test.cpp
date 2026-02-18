#include <gtest/gtest.h>
#include "smartdb/support.hpp"
#include "sdb/types.hpp"
#include "sdb/db.hpp"
#include "sdb/connection_pool.hpp"
#include "sdb/query_utils.hpp"
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

namespace {

class FakeTxConnection : public sdb::IConnection {
public:
    bool beginShouldFail = false;
    int beginCount = 0;
    int commitCount = 0;
    int rollbackCount = 0;

    sdb::DbResult<void> open() override { return sdb::DbResult<void>::success(); }
    void close() override {}
    bool isOpen() const override { return true; }
    sdb::DbResult<std::shared_ptr<sdb::IResultSet>> query(const std::string&) override {
        return sdb::DbResult<std::shared_ptr<sdb::IResultSet>>::failure("Not implemented");
    }
    sdb::DbResult<int64_t> execute(const std::string&) override {
        return sdb::DbResult<int64_t>::failure("Not implemented");
    }
    sdb::DbResult<int64_t> execute(const std::string&, const std::vector<sdb::DbValue>&) override {
        return sdb::DbResult<int64_t>::failure("Not implemented");
    }
    sdb::DbResult<void> begin() override {
        ++beginCount;
        if (beginShouldFail) {
            return sdb::DbResult<void>::failure("begin failed");
        }
        return sdb::DbResult<void>::success();
    }
    sdb::DbResult<void> commit() override {
        ++commitCount;
        return sdb::DbResult<void>::success();
    }
    sdb::DbResult<void> rollback() override {
        ++rollbackCount;
        return sdb::DbResult<void>::success();
    }
};

} // namespace

TEST(TransactionGuardTest, RollsBackWhenNotCommitted) {
    FakeTxConnection conn;
    {
        auto txRes = sdb::TransactionGuard::begin(conn);
        ASSERT_TRUE(txRes) << txRes.error().message;
        ASSERT_TRUE(txRes.value().active());
    }
    EXPECT_EQ(conn.beginCount, 1);
    EXPECT_EQ(conn.commitCount, 0);
    EXPECT_EQ(conn.rollbackCount, 1);
}

TEST(TransactionGuardTest, CommitDisarmsAutoRollback) {
    FakeTxConnection conn;
    {
        auto txRes = sdb::TransactionGuard::begin(conn);
        ASSERT_TRUE(txRes) << txRes.error().message;
        auto commitRes = txRes.value().commit();
        ASSERT_TRUE(commitRes) << commitRes.error().message;
        EXPECT_FALSE(txRes.value().active());
    }
    EXPECT_EQ(conn.beginCount, 1);
    EXPECT_EQ(conn.commitCount, 1);
    EXPECT_EQ(conn.rollbackCount, 0);
}

TEST(TransactionGuardTest, BeginFailureReturnsError) {
    FakeTxConnection conn;
    conn.beginShouldFail = true;

    auto txRes = sdb::TransactionGuard::begin(conn);
    EXPECT_FALSE(txRes);
    EXPECT_NE(txRes.error().message.find("begin failed"), std::string::npos);
    EXPECT_EQ(conn.beginCount, 1);
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

TEST(QueryUtilsTest, QueryOneReturnsSingleRow) {
    sdb::drivers::SqliteDriver driver;
    auto conn = driver.createConnection({{"path", ":memory:"}});
    ASSERT_TRUE(conn->open());
    ASSERT_TRUE(conn->execute("CREATE TABLE t (id INTEGER, name TEXT)"));
    ASSERT_TRUE(conn->execute("INSERT INTO t VALUES (1, 'alice')"));

    auto rowRes = sdb::queryOne(*conn, "SELECT id, name FROM t WHERE id = 1");
    ASSERT_TRUE(rowRes) << rowRes.error().message;
    ASSERT_EQ(rowRes.value().size(), 2);
    EXPECT_EQ(std::get<int64_t>(rowRes.value()[0]), 1);
    EXPECT_EQ(std::get<std::string>(rowRes.value()[1]), "alice");
}

TEST(QueryUtilsTest, QueryAllReturnsAllRows) {
    sdb::drivers::SqliteDriver driver;
    auto conn = driver.createConnection({{"path", ":memory:"}});
    ASSERT_TRUE(conn->open());
    ASSERT_TRUE(conn->execute("CREATE TABLE t2 (id INTEGER, name TEXT)"));
    ASSERT_TRUE(conn->execute("INSERT INTO t2 VALUES (1, 'a')"));
    ASSERT_TRUE(conn->execute("INSERT INTO t2 VALUES (2, 'b')"));

    auto rowsRes = sdb::queryAll(*conn, "SELECT id, name FROM t2 ORDER BY id ASC");
    ASSERT_TRUE(rowsRes) << rowsRes.error().message;
    ASSERT_EQ(rowsRes.value().size(), 2);
    EXPECT_EQ(std::get<int64_t>(rowsRes.value()[0][0]), 1);
    EXPECT_EQ(std::get<std::string>(rowsRes.value()[1][1]), "b");
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

TEST(ConnectionPoolTest, MetricsTrackTimeoutAndPeakUsage) {
    auto driver = std::make_shared<sdb::drivers::SqliteDriver>();
    sdb::ConnectionPool::Options options;
    options.maxSize = 1;
    options.minSize = 0;
    options.waitTimeout = std::chrono::milliseconds(40);

    auto poolRes = sdb::ConnectionPool::createWithFactory(
        [driver]() {
            return sdb::DbResult<std::unique_ptr<sdb::IConnection>>::success(
                driver->createConnection({{"path", ":memory:"}}));
        },
        options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    auto pool = poolRes.value();

    auto firstRes = pool->acquire();
    ASSERT_TRUE(firstRes) << firstRes.error().message;
    auto first = std::move(firstRes.value());

    auto secondRes = pool->acquire();
    EXPECT_FALSE(secondRes);
    EXPECT_NE(secondRes.error().message.find("timed out"), std::string::npos);

    first.reset();

    const auto metrics = pool->metrics();
    EXPECT_EQ(metrics.acquireAttempts, 2);
    EXPECT_EQ(metrics.acquireSuccesses, 1);
    EXPECT_EQ(metrics.acquireFailures, 1);
    EXPECT_EQ(metrics.acquireTimeouts, 1);
    EXPECT_GE(metrics.waitEvents, 1);
    EXPECT_GE(metrics.peakInUse, static_cast<size_t>(1));
    EXPECT_GT(metrics.totalAcquireWaitMicros, static_cast<uint64_t>(0));
}

TEST(ConnectionPoolTest, MetricsTrackFactoryFailures) {
    sdb::ConnectionPool::Options options;
    options.maxSize = 1;
    options.waitTimeout = std::chrono::milliseconds(10);

    auto poolRes = sdb::ConnectionPool::createWithFactory(
        []() { return sdb::DbResult<std::unique_ptr<sdb::IConnection>>::failure("factory boom"); },
        options);
    ASSERT_TRUE(poolRes) << poolRes.error().message;
    auto pool = poolRes.value();

    auto connRes = pool->acquire();
    EXPECT_FALSE(connRes);
    EXPECT_NE(connRes.error().message.find("factory boom"), std::string::npos);

    const auto metrics = pool->metrics();
    EXPECT_EQ(metrics.acquireAttempts, 1);
    EXPECT_EQ(metrics.acquireFailures, 1);
    EXPECT_EQ(metrics.factoryFailures, 1);
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

std::string readEnvOrDefault(const char* key, const char* fallback) {
#if defined(_WIN32)
    char* value = nullptr;
    size_t len = 0;
    if (_dupenv_s(&value, &len, key) != 0 || value == nullptr) {
        return std::string(fallback);
    }
    std::string result(value);
    std::free(value);
    return result;
#else
    const char* value = std::getenv(key);
    return std::string(value ? value : fallback);
#endif
}

bool mysqlTestEnabled() {
    const std::string value = readEnvOrDefault("SMARTDB_MYSQL_TEST_ENABLE", "");
    if (value.empty()) {
        return false;
    }
    return value == "1" || value == "true" || value == "TRUE" || value == "on" || value == "ON";
}

nlohmann::json mysqlConfigFromEnv() {
    nlohmann::json cfg;
    const auto read = [](const char* key, const char* fallback) { return readEnvOrDefault(key, fallback); };

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
