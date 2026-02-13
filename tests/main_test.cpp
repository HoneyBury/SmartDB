#include <gtest/gtest.h>
#include "cppsharp/my_lib.hpp"
#include "sdb/types.hpp"
#include "sdb/drivers/sqlite_driver.hpp"

class MyLibTest : public ::testing::Test {
protected:
    void SetUp() override {
        setup_logger();
    }
};

TEST_F(MyLibTest, GreetFunction) {
    ASSERT_NO_THROW(greet("Tester"));
}

TEST(MyLibStandaloneTest, AlwaysPass) {
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

    ASSERT_TRUE(conn->open());
    ASSERT_GE(conn->execute("CREATE TABLE demo (id INTEGER, name TEXT, payload BLOB)"), 0);

    std::vector<uint8_t> blob{0x41, 0x42, 0x43};
    const int64_t affected = conn->execute(
        "INSERT INTO demo (id, name, payload) VALUES (?, ?, ?)",
        {int64_t{7}, std::string("smartdb"), blob}
    );
    ASSERT_EQ(affected, 1);

    auto rs = conn->query("SELECT id, name, payload FROM demo LIMIT 1");
    ASSERT_NE(rs, nullptr);
    ASSERT_TRUE(rs->next());

    EXPECT_EQ(std::get<int64_t>(rs->get("id")), 7);
    EXPECT_EQ(std::get<std::string>(rs->get("name")), "smartdb");

    auto payload = std::get<std::vector<uint8_t>>(rs->get("payload"));
    EXPECT_EQ(payload, blob);
}
