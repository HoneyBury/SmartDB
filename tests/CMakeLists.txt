# tests/CMakeLists.txt

find_package(GTest REQUIRED)

# 步骤 1: 将可执行文件的目标名从 "run_tests" 改为 "unit_tests"
# 这是一个实际的、需要被编译和链接的目标。
add_executable(unit_tests
        main_test.cpp
)

# 步骤 2: 链接 unit_tests 到我们的库和GTest
target_link_libraries(unit_tests PRIVATE
        ${PROJECT_NAME}
        GTest::gtest_main
)

# 步骤 3: 为 unit_tests 设置编译属性
set_project_properties(unit_tests)

include(GoogleTest)

# 步骤 4: 告诉 gtest_discover_tests 去处理 "unit_tests" 这个目标。
# 它会自动创建一个名为 "unit_tests" 的CTest测试，这个测试会运行编译好的 unit_tests.exe。
# 同时，它还会修改一个名为 "RUN_TESTS" 的全局目标（如果存在），或者你可以自己创建一个。
gtest_discover_tests(unit_tests)