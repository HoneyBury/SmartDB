# ./CMakeLists.txt (Enhanced Version)

# 1. 设置CMake最低版本和项目信息
cmake_minimum_required(VERSION 3.23)
project(CppSharp VERSION 4.1.2
        DESCRIPTION "A modern C++ project template using Conan and CMake"
        LANGUAGES CXX)

# 2. 包含 GNU 标准目录变量
include(GNUInstallDirs)

# 3. 健壮的输出目录设置
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR})
set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
if(CMAKE_CONFIGURATION_TYPES)
    foreach(config ${CMAKE_CONFIGURATION_TYPES})
        string(TOUPPER ${config} config_upper)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_BINDIR})
        set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
        set(CMAKE_LIBRARY_OUTPUT_DIRECTORY_${config_upper} ${CMAKE_BINARY_DIR}/${CMAKE_INSTALL_LIBDIR})
    endforeach()
endif()

# 4. 自定义CMake模块路径
list(APPEND CMAKE_MODULE_PATH "${CMAKE_CURRENT_SOURCE_DIR}/cmake")
include(utils)
# 5. 全局构建选项
option(BUILD_TESTING "Build the tests" ON)
option(CPACK_CREATE_DESKTOP_SHORTCUT "Offer to create a desktop shortcut during installation" ON) # 新增选项

# 6. 添加子目录
add_subdirectory(src)
if(BUILD_TESTING)
    enable_testing()
    add_subdirectory(tests)
endif()

# =================== 安装与打包配置 (CPack) ===================
include(CMakePackageConfigHelpers)

# 设置通用元数据
set(CPACK_PACKAGE_NAME "${PROJECT_NAME}")
set(CPACK_PACKAGE_VERSION "${PROJECT_VERSION}")
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "${PROJECT_DESCRIPTION}")
set(CPACK_PACKAGE_VENDOR "MyProject Inc.")
set(PROJECT_AUTHOR "HoneyBury zoujiahe389@gmail.com")
set(CPACK_PACKAGE_CONTACT "${PROJECT_AUTHOR}")
set(PROJECT_URL "https://github.com/HoneyBury/CppSharp.git")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_README "${CMAKE_CURRENT_SOURCE_DIR}/README.md")

# --- 更精细的跨平台安装包文件名 (区分处理器架构) ---

# 1. 获取并规范化操作系统名称
if(CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OS_IDENTIFIER "macos")
else()
    string(TOLOWER "${CMAKE_SYSTEM_NAME}" OS_IDENTIFIER)
endif()

# 2. 获取处理器架构名称
# CMAKE_SYSTEM_PROCESSOR 的值可能是 "x86_64", "AMD64", "arm64", "aarch64" 等
string(TOLOWER "${CMAKE_SYSTEM_PROCESSOR}" ARCH_IDENTIFIER)

# (可选) 统一 x86_64 架构的名称为 "x64"
if(ARCH_IDENTIFIER STREQUAL "amd64" OR ARCH_IDENTIFIER STREQUAL "x86_64")
    set(ARCH_IDENTIFIER "x64")
endif()

# 3. 组合成最终的文件名
set(CPACK_PACKAGE_FILE_NAME "${PROJECT_NAME}-${PROJECT_VERSION}-${OS_IDENTIFIER}-${ARCH_IDENTIFIER}")

# 根据操作系统设置CPack生成器
if(WIN32)
    set(CPACK_GENERATOR "NSIS")
elseif(UNIX AND NOT APPLE)
    set(CPACK_GENERATOR "DEB")
elseif(APPLE)
    set(CPACK_GENERATOR "DragNDrop")
endif()

# --- 增强的 NSIS 特定配置 ---
if(CPACK_GENERATOR STREQUAL "NSIS")
    # 基础信息
    set(CPACK_NSIS_DISPLAY_NAME "${CPACK_PACKAGE_NAME} ${CPACK_PACKAGE_VERSION}")
    set(CPACK_NSIS_PUBLISHER "${CPACK_PACKAGE_VENDOR}")
    set(CPACK_NSIS_URL_INFO_ABOUT "${PROJECT_URL}")
    set(CPACK_NSIS_HELP_LINK "${PROJECT_URL}/issues")
    set(CPACK_NSIS_CONTACT "${CPACK_PACKAGE_CONTACT}")

    # 安装程序外观和图标
    set(CPACK_NSIS_MUI_ICON "${CMAKE_CURRENT_SOURCE_DIR}/assets/install_icon.ico")
    set(CPACK_NSIS_MUI_UNIICON "${CMAKE_CURRENT_SOURCE_DIR}/assets/uninstall_icon.ico")

    # 开始菜单设置
    set(CPACK_NSIS_PACKAGE_NAME "${CPACK_PACKAGE_NAME}") # 开始菜单文件夹的名称
    set(CPACK_NSIS_CREATE_ICONS_EXTRA "") # 初始化快捷方式列表
    list(APPEND CPACK_NSIS_CREATE_ICONS_EXTRA "CreateShortCut '$SMPROGRAMS\\\\${CPACK_NSIS_PACKAGE_NAME}\\\\${PROJECT_NAME}.lnk' '$INSTDIR\\\\bin\\\\${PROJECT_NAME}.exe'")
    list(APPEND CPACK_NSIS_CREATE_ICONS_EXTRA "CreateShortCut '$SMPROGRAMS\\\\${CPACK_NSIS_PACKAGE_NAME}\\\\Uninstall.lnk' '$INSTDIR\\\\Uninstall.exe'")

    # 桌面快捷方式 (如果选项开启)
    if(CPACK_CREATE_DESKTOP_SHORTCUT)
        list(APPEND CPACK_NSIS_CREATE_ICONS_EXTRA "CreateShortCut '$DESKTOP\\\\${PROJECT_NAME}.lnk' '$INSTDIR\\\\bin\\\\${PROJECT_NAME}.exe'")
    endif()

    # 在开始菜单中添加网页链接
    set(CPACK_NSIS_MENU_LINKS "${PROJECT_URL}" "Project Website")

    # 安装过程中的用户选项
    set(CPACK_NSIS_MODIFY_PATH ON) # 提供 "添加到系统 PATH" 的选项，并默认勾选

endif()

# --- DEB 包特定配置 ---
if(CPACK_GENERATOR STREQUAL "DEB")
    set(CPACK_DEBIAN_PACKAGE_MAINTAINER "${PROJECT_AUTHOR}")
    set(CPACK_DEBIAN_PACKAGE_SECTION "devel")
    set(CPACK_DEBIAN_PACKAGE_PRIORITY "optional")
endif()

# 设置源文件打包时要忽略的文件
set(CPACK_SOURCE_IGNORE_FILES "/.git/" "/build/" "/.vscode/" "/.idea/" ".DS_Store" "*.swp" "*.bak" "/.venv/")

# 生成和安装 CMake 包配置文件 (供开发者使用)
write_basic_package_version_file(
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
        VERSION ${PROJECT_VERSION}
        COMPATIBILITY AnyNewerVersion
)
configure_package_config_file(
        "${CMAKE_SOURCE_DIR}/cmake/config.cmake.in"
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
        INSTALL_DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)
install(FILES
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}Config.cmake"
        "${CMAKE_CURRENT_BINARY_DIR}/${PROJECT_NAME}ConfigVersion.cmake"
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)
install(EXPORT ${PROJECT_NAME}Targets
        FILE ${PROJECT_NAME}Targets.cmake
        NAMESPACE ${PROJECT_NAME}::
        DESTINATION ${CMAKE_INSTALL_LIBDIR}/cmake/${PROJECT_NAME}
)

# 最后包含 CPack 模块，使其应用所有上述配置
include(CPack)