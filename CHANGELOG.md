# Changelog
log for this program 

this file base on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)，
 [Version](https://semver.org/spec/v2.0.0.html) (Semantic Versioning)。

---

## [4.1.2] - 2025-06-28

### Added
- 新增 NSIS 安装包支持，支持 Windows 下生成带图标、桌面/开始菜单快捷方式的安装包。
- 增加 `CPack` 跨平台打包配置，支持 DEB、NSIS、DragNDrop 等多平台格式。
- `conanfile.py` 支持自动从 `CMakeLists.txt` 读取项目信息（名称、版本、描述）。
- 新增 `CPACK_CREATE_DESKTOP_SHORTCUT` 选项，安装时可选创建桌面快捷方式。
- `CMakeLists.txt` 增加对 GNUInstallDirs、CMakePackageConfigHelpers、CPack 等模块的集成。
- `conanfile.py` 增加 `exports`、`exports_sources`，确保构建和打包时所有源码、资源、配置文件完整。

### Changed
- 项目版本号升级为 4.1.2。
- `CMakeLists.txt` 输出目录、安装目录、包名、架构等配置更健壮，支持多平台和多架构。
- `conanfile.py` 结构优化，依赖声明、配置、验证、打包流程更清晰。
- `README.md` 构建、安装、打包说明更详细，增加多平台构建/打包命令示例。

### Fixed
- 修复部分平台下构建输出目录不一致的问题。
- 修复 Conan 构建时源文件缺失、路径不正确等问题。
- 修复 CPack 打包时 LICENSE、README.md 等文件未正确包含的问题。

---

## [4.0.0] - 2025-6-28

### Added
- add LICENSE file 
- add more install option

### Changed
- update connafile
- update CMakeLists.txt
- update cxx  standard required

### Fixed
- fix install problem

---