# src/CMakeLists.txt (修改后)

# 1. 使用项目名创建核心库
add_library(${PROJECT_NAME} STATIC
        smartdb/support.cpp
        smartdb/support.hpp
        sdb/types.hpp
        sdb/idb.hpp
        sdb/db.hpp
        sdb/connection_pool.hpp
        sdb/drivers/sqlite_driver.hpp
        sdb/drivers/mysql_driver.hpp
)

# 2. 设置库的属性
target_include_directories(${PROJECT_NAME}
        PUBLIC
        $<BUILD_INTERFACE:${CMAKE_CURRENT_SOURCE_DIR}>
        PRIVATE
)
# 注意：set_project_properties 是你自定义的函数，确保它能正确处理 ${PROJECT_NAME}
set_project_properties(${PROJECT_NAME})

# 3. 链接第三方库
find_package(fmt REQUIRED)
find_package(spdlog REQUIRED)
find_package(SQLite3 REQUIRED)
find_package(nlohmann_json REQUIRED)
find_package(libmysqlclient REQUIRED)
target_link_libraries(${PROJECT_NAME} PUBLIC
        fmt::fmt
        spdlog::spdlog
        SQLite::SQLite3
        nlohmann_json::nlohmann_json
        libmysqlclient::libmysqlclient
)

# --- 安装规则 ---
install(TARGETS ${PROJECT_NAME}  # <--- 修改这里
        EXPORT ${PROJECT_NAME}Targets
        ARCHIVE DESTINATION ${CMAKE_INSTALL_LIBDIR}
        LIBRARY DESTINATION ${CMAKE_INSTALL_LIBDIR}
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
        INCLUDES DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}
)
# 安装公共头文件
install(FILES smartdb/support.hpp DESTINATION ${CMAKE_INSTALL_INCLUDEDIR}/${PROJECT_NAME})


# 4. 创建主可执行文件 (app)
add_executable(app
        main.cpp
)

# 5. 链接 app 到我们的核心库
target_link_libraries(app PRIVATE ${PROJECT_NAME}) # <--- 修改这里

# 6. 应用通用编译选项
set_project_properties(app)

install(TARGETS app
        RUNTIME DESTINATION ${CMAKE_INSTALL_BINDIR}
)
