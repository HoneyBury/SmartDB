# My Modern C++ Project

这是一个基于现代CMake和Conan 2.0的C++项目脚手架。

## 特性

-   C++17 标准，支持 C++14 及以上
-   CMake (Target-based)
-   Conan 2.0 依赖管理，自动从 CMakeLists.txt 读取项目信息
-   Google Test 集成
-   跨平台打包（支持 NSIS/DEB/DragNDrop 等）
-   支持桌面/开始菜单快捷方式、图标自定义
-   清晰的项目结构，源码/测试/资源分离
-   Github Action CI/CD 集成

## 依赖

-   C++ 编译器 (支持 C++14及以上)
-   CMake (>= 3.23)
-   Conan (>= 2.0)

## 如何构建

1.  **克隆仓库**
    ```bash
    git clone https://github.com/HoneyBury/CppSharp.git
    cd CppSharp
    ```

2.  **安装Conan依赖**
    此命令会读取 `conanfile.py`，下载依赖，并在 `build/` 目录下生成CMake集成所需的文件。
    ```bash
    conan install . -s build_type=Debug --build=missing
    conan install . -s build_type=Release --build=missing
    ```

3.  **配置CMake项目**
    使用Conan生成的toolchain文件来配置CMake。
    ```bash
    # 推荐使用预设命令（Windows/Linux/Mac 通用）
    cmake --preset conan-debug
    cmake --preset conan-release
    # 或手动指定 toolchain
    cmake -S . -B build/release -DCMAKE_TOOLCHAIN_FILE="build/generators/conan_toolchain.cmake" -DCMAKE_BUILD_TYPE=Release
    cmake -S . -B build/debug -DCMAKE_TOOLCHAIN_FILE="build/generators/conan_toolchain.cmake" -DCMAKE_BUILD_TYPE=Debug
    ```

4.  **构建项目**
    ```bash
    cmake --build --preset conan-debug
    cmake --build --preset conan-release
    # 或
    cmake --build build/release
    cmake --build build/debug
    ```

5.  **运行**
    ```bash
    # 运行主程序
    ./build/release/bin/app
    # Windows 预设方式
    ./build/bin/app
    # 运行测试
    cd build/release
    ctest -C Release --output-on-failure
    cd ../..
    ```

6. **打包与安装包生成**
    ```bash
    # 推荐使用 CPack 生成安装包（需 CMake >= 3.23）
    cmake --build --preset conan-release --target package
    # 或直接用 cpack
    cpack -G NSIS   # Windows 下生成 .exe 安装包
    cpack -G DEB    # Linux 下生成 .deb 包
    cpack -G DragNDrop # Mac 下生成 .dmg 包
    ```
    - 安装包会自动包含 LICENSE、README.md、图标、快捷方式等资源。
    - Windows 下支持桌面/开始菜单快捷方式，图标自定义。

## 其他说明

- `conanfile.py` 会自动从 `CMakeLists.txt` 读取项目名称、版本、描述，确保元数据一致。
- `CMakeLists.txt` 支持多平台、多架构输出目录和打包配置。
- 所有源码、测试、资源、配置文件均通过 Conan/CMake 自动导出和打包。
- 如需自定义打包行为，可修改 `CMakeLists.txt` 中的 CPack/NSIS/DEB 相关配置。
