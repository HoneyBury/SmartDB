#pragma once
#include "../idb.hpp"
#include <mysql.h>
#include <spdlog/spdlog.h>
#include <cstring>

namespace sdb::drivers {

class MysqlResultSet : public IResultSet {
    MYSQL_RES* res_ = nullptr;
    MYSQL_ROW row_ = nullptr;
    std::vector<std::string> colNames_;
    std::vector<enum enum_field_types> colTypes_;

public:
    MysqlResultSet(MYSQL_RES* res) : res_(res) {
        if (!res_) return;

        int num_fields = mysql_num_fields(res_);
        MYSQL_FIELD* fields = mysql_fetch_fields(res_);
        for(int i = 0; i < num_fields; i++) {
            colNames_.emplace_back(fields[i].name);
            colTypes_.push_back(fields[i].type);
        }
    }

    ~MysqlResultSet() {
        if (res_) mysql_free_result(res_);
    }

    bool next() override {
        if (!res_) return false;
        row_ = mysql_fetch_row(res_);
        return row_ != nullptr;
    }

    DbValue get(int index) override {
        if (!row_ || index < 0 || index >= (int)colNames_.size()) return {};

        char* val = row_[index];
        if (val == nullptr) return std::monostate{};

        switch (colTypes_[index]) {
            case MYSQL_TYPE_TINY:
            case MYSQL_TYPE_SHORT:
            case MYSQL_TYPE_LONG:
            case MYSQL_TYPE_INT24:
            case MYSQL_TYPE_LONGLONG:
                return std::stoll(val);
            case MYSQL_TYPE_FLOAT:
            case MYSQL_TYPE_DOUBLE:
            case MYSQL_TYPE_DECIMAL:
            case MYSQL_TYPE_NEWDECIMAL:
                return std::stod(val);
            default:
                return std::string(val);
        }
    }

    DbValue get(const std::string& columnName) override {
        for (size_t i = 0; i < colNames_.size(); ++i) {
            if (colNames_[i] == columnName) return get(i);
        }
        return {};
    }

    std::vector<std::string> columnNames() override { return colNames_; }
};

class MysqlConnection : public IConnection {
    MYSQL* conn_ = nullptr;
    nlohmann::json config_;
    std::string lastErr_;

public:
    MysqlConnection(const nlohmann::json& config) : config_(config) {}

    ~MysqlConnection() { close(); }

    bool open() override {
        conn_ = mysql_init(nullptr);
        if (!conn_) {
            lastErr_ = "mysql_init failed: out of memory";
            return false;
        }

        std::string host = config_.value("host", "127.0.0.1");
        int port = config_.value("port", 3306);
        std::string user = config_.value("user", "root");
        std::string pass = config_.value("password", "");
        std::string db   = config_.value("database", "");
        std::string charset = config_.value("charset", "utf8mb4");

        unsigned int timeout = 10;
        mysql_options(conn_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        mysql_set_character_set(conn_, charset.c_str());

        // 关键修复：确保连接时指定了 DB，否则后续建表如果没有 USE db 可能会失败
        if (!mysql_real_connect(conn_, host.c_str(), user.c_str(),
                                pass.c_str(), db.empty() ? nullptr : db.c_str(),
                                port, nullptr, 0)) {
            lastErr_ = mysql_error(conn_);
            mysql_close(conn_);
            conn_ = nullptr;
            return false;
        }
        return true;
    }

    void close() override {
        if (conn_) {
            mysql_close(conn_);
            conn_ = nullptr;
        }
    }

    bool isOpen() const override { return conn_ != nullptr; }

    // ================== 修复重点：补全 Query 逻辑 ==================
    std::shared_ptr<IResultSet> query(const std::string& sql) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return nullptr;
        }

        if (mysql_query(conn_, sql.c_str())) {
            lastErr_ = mysql_error(conn_);
            spdlog::error("MySQL Query Error: {} | SQL: {}", lastErr_, sql);
            return nullptr;
        }

        MYSQL_RES* res = mysql_store_result(conn_);
        if (!res) {
            if (mysql_field_count(conn_) > 0) {
                lastErr_ = mysql_error(conn_);
                spdlog::error("MySQL Store Result Error: {}", lastErr_);
                return nullptr;
            }
            return std::make_shared<MysqlResultSet>(nullptr);
        }

        return std::make_shared<MysqlResultSet>(res);
    }

    int64_t execute(const std::string& sql) override {
        if (!isOpen()) {
             lastErr_ = "Connection is closed";
             return -1;
        }

        if (mysql_query(conn_, sql.c_str())) {
            lastErr_ = mysql_error(conn_);
            spdlog::error("MySQL Execute Error: {} | SQL: {}", lastErr_, sql);
            return -1;
        }
        return (int64_t)mysql_affected_rows(conn_);
    }

    int64_t execute(const std::string& sql, const std::vector<DbValue>& params) override {
        // 暂时未实现参数绑定，先打个日志
        spdlog::warn("Parameterized execute not implemented yet for MySQL");
        return -1;
    }

    bool begin() override { return execute("START TRANSACTION") >= 0; }
    bool commit() override { return execute("COMMIT") >= 0; }
    bool rollback() override { return execute("ROLLBACK") >= 0; }

    std::string lastError() const override { return lastErr_; }
};

class MysqlDriver : public IDriver {
public:
    std::unique_ptr<IConnection> createConnection(const nlohmann::json& config) override {
        return std::make_unique<MysqlConnection>(config);
    }
    std::string name() const override { return "mysql"; }
};

} // namespace sdb::drivers