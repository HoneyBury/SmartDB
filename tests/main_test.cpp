//
// Created by HoneyBury on 25-6-22.
//
#include <gtest/gtest.h>
#include "cppsharp/my_lib.hpp"

// 一个简单的测试 fixture (如果需要)
class MyLibTest : public ::testing::Test {
protected:
    void SetUp() override {
        // 每个测试用例开始前执行
        setup_logger();
    }
};

TEST_F(MyLibTest, GreetFunction) {
    // 这是一个简单的示例，实际中可能需要捕获stdout或检查log输出来验证
    // 这里我们只确保函数能被调用而不崩溃
    ASSERT_NO_THROW(greet("Tester"));
}

TEST(MyLibStandaloneTest, AlwaysPass) {
    // 一个保证通过的简单测试
    EXPECT_EQ(1, 1);
    ASSERT_TRUE(true);
}