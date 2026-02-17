#pragma once
#include "../idb.hpp"

#include <mysql.h>
#include <spdlog/spdlog.h>

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <limits>
#include <memory>
#include <stdexcept>
#include <string>
#include <vector>

namespace sdb::drivers {

class MysqlResultSet : public IResultSet {
    MYSQL_RES* res_ = nullptr;
    MYSQL_ROW row_ = nullptr;
    unsigned long* lengths_ = nullptr;
    std::vector<std::string> colNames_;
    std::vector<enum enum_field_types> colTypes_;

public:
    explicit MysqlResultSet(MYSQL_RES* res) : res_(res) {
        if (!res_) {
            return;
        }

        const int num_fields = mysql_num_fields(res_);
        MYSQL_FIELD* fields = mysql_fetch_fields(res_);
        colNames_.reserve(num_fields);
        colTypes_.reserve(num_fields);
        for (int i = 0; i < num_fields; i++) {
            colNames_.emplace_back(fields[i].name);
            colTypes_.push_back(fields[i].type);
        }
    }

    ~MysqlResultSet() override {
        if (res_) {
            mysql_free_result(res_);
        }
    }

    bool next() override {
        if (!res_) {
            return false;
        }

        row_ = mysql_fetch_row(res_);
        if (!row_) {
            lengths_ = nullptr;
            return false;
        }

        lengths_ = mysql_fetch_lengths(res_);
        return true;
    }

    DbValue get(int index) override {
        if (!row_ || index < 0 || index >= static_cast<int>(colNames_.size())) {
            return {};
        }

        const char* val = row_[index];
        if (val == nullptr) {
            return std::monostate{};
        }

        const unsigned long len = lengths_ ? lengths_[index] : static_cast<unsigned long>(std::strlen(val));
        const std::string text(val, len);

        try {
            switch (colTypes_[index]) {
                case MYSQL_TYPE_TINY:
                    return static_cast<int>(std::stoi(text));
                case MYSQL_TYPE_SHORT:
                case MYSQL_TYPE_LONG:
                case MYSQL_TYPE_INT24:
                    return std::stoi(text);
                case MYSQL_TYPE_LONGLONG:
                    return std::stoll(text);
                case MYSQL_TYPE_FLOAT:
                case MYSQL_TYPE_DOUBLE:
                case MYSQL_TYPE_DECIMAL:
                case MYSQL_TYPE_NEWDECIMAL:
                    return std::stod(text);
                case MYSQL_TYPE_BIT:
                    if (len == 1) {
                        return static_cast<unsigned char>(val[0]) != 0;
                    }
                    return text == "1";
                case MYSQL_TYPE_BLOB:
                case MYSQL_TYPE_TINY_BLOB:
                case MYSQL_TYPE_MEDIUM_BLOB:
                case MYSQL_TYPE_LONG_BLOB:
                case MYSQL_TYPE_GEOMETRY:
                    return std::vector<uint8_t>(reinterpret_cast<const uint8_t*>(val), reinterpret_cast<const uint8_t*>(val) + len);
                default:
                    return text;
            }
        } catch (const std::exception&) {
            return text;
        }
    }

    DbValue get(const std::string& columnName) override {
        for (size_t i = 0; i < colNames_.size(); ++i) {
            if (colNames_[i] == columnName) {
                return get(static_cast<int>(i));
            }
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
    explicit MysqlConnection(const nlohmann::json& config) : config_(config) {}

    ~MysqlConnection() override { close(); }

    DbResult<void> open() override {
        if (isOpen()) {
            return DbResult<void>::success();
        }

        conn_ = mysql_init(nullptr);
        if (!conn_) {
            lastErr_ = "mysql_init failed: out of memory";
            return DbResult<void>::failure(lastErr_);
        }

        const std::string host = config_.value("host", "127.0.0.1");
        const int port = config_.value("port", 3306);
        const std::string user = config_.value("user", "root");
        const std::string pass = config_.value("password", "");
        const std::string db = config_.value("database", "");
        const std::string charset = config_.value("charset", "utf8mb4");

        unsigned int timeout = 10;
        mysql_options(conn_, MYSQL_OPT_CONNECT_TIMEOUT, &timeout);
        mysql_options(conn_, MYSQL_SET_CHARSET_NAME, charset.c_str());

        if (!mysql_real_connect(conn_, host.c_str(), user.c_str(),
                                pass.c_str(), db.empty() ? nullptr : db.c_str(),
                                port, nullptr, 0)) {
            lastErr_ = mysql_error(conn_);
            const int errCode = mysql_errno(conn_);
            mysql_close(conn_);
            conn_ = nullptr;
            return DbResult<void>::failure(lastErr_, errCode);
        }

        lastErr_.clear();
        return DbResult<void>::success();
    }

    void close() override {
        if (conn_) {
            mysql_close(conn_);
            conn_ = nullptr;
        }
    }

    bool isOpen() const override { return conn_ != nullptr; }

    DbResult<std::shared_ptr<IResultSet>> query(const std::string& sql) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return DbResult<std::shared_ptr<IResultSet>>::failure(lastErr_);
        }

        if (mysql_query(conn_, sql.c_str())) {
            lastErr_ = mysql_error(conn_);
            spdlog::error("MySQL Query Error: {} | SQL: {}", lastErr_, sql);
            return DbResult<std::shared_ptr<IResultSet>>::failure(lastErr_, mysql_errno(conn_));
        }

        MYSQL_RES* res = mysql_store_result(conn_);
        if (!res) {
            if (mysql_field_count(conn_) > 0) {
                lastErr_ = mysql_error(conn_);
                spdlog::error("MySQL Store Result Error: {}", lastErr_);
                return DbResult<std::shared_ptr<IResultSet>>::failure(lastErr_, mysql_errno(conn_));
            }
            return DbResult<std::shared_ptr<IResultSet>>::success(std::make_shared<MysqlResultSet>(nullptr));
        }

        lastErr_.clear();
        return DbResult<std::shared_ptr<IResultSet>>::success(std::make_shared<MysqlResultSet>(res));
    }

    DbResult<int64_t> execute(const std::string& sql) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return DbResult<int64_t>::failure(lastErr_);
        }

        if (mysql_query(conn_, sql.c_str())) {
            lastErr_ = mysql_error(conn_);
            spdlog::error("MySQL Execute Error: {} | SQL: {}", lastErr_, sql);
            return DbResult<int64_t>::failure(lastErr_, mysql_errno(conn_));
        }

        lastErr_.clear();
        return DbResult<int64_t>::success(static_cast<int64_t>(mysql_affected_rows(conn_)));
    }

    DbResult<int64_t> execute(const std::string& sql, const std::vector<DbValue>& params) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return DbResult<int64_t>::failure(lastErr_);
        }

        MYSQL_STMT* stmt = mysql_stmt_init(conn_);
        if (!stmt) {
            lastErr_ = "mysql_stmt_init failed";
            return DbResult<int64_t>::failure(lastErr_, mysql_errno(conn_));
        }

        auto cleanupStmt = [&stmt]() {
            if (stmt) {
                mysql_stmt_close(stmt);
                stmt = nullptr;
            }
        };

        if (mysql_stmt_prepare(stmt, sql.c_str(), static_cast<unsigned long>(sql.size())) != 0) {
            lastErr_ = mysql_stmt_error(stmt);
            spdlog::error("MySQL Prepare Error: {} | SQL: {}", lastErr_, sql);
            const int errCode = mysql_stmt_errno(stmt);
            cleanupStmt();
            return DbResult<int64_t>::failure(lastErr_, errCode);
        }

        const auto expectedParams = mysql_stmt_param_count(stmt);
        if (expectedParams != params.size()) {
            lastErr_ = "parameter count mismatch: expected " + std::to_string(expectedParams) +
                       ", got " + std::to_string(params.size());
            cleanupStmt();
            return DbResult<int64_t>::failure(lastErr_);
        }

        std::vector<MYSQL_BIND> binds(params.size());
        std::vector<unsigned long> lengths(params.size(), 0);
        std::vector<std::unique_ptr<bool>> isNulls;
        isNulls.reserve(params.size());
        std::vector<int32_t> i32Vals(params.size(), 0);
        std::vector<int64_t> i64Vals(params.size(), 0);
        std::vector<double> f64Vals(params.size(), 0.0);
        std::vector<int8_t> boolVals(params.size(), 0);
        std::vector<std::string> strVals(params.size());
        std::vector<std::vector<uint8_t>> blobVals(params.size());

        for (size_t i = 0; i < params.size(); ++i) {
            isNulls.emplace_back(std::make_unique<bool>(false));
            MYSQL_BIND& bind = binds[i];
            std::memset(&bind, 0, sizeof(MYSQL_BIND));
            bind.length = &lengths[i];
            bind.is_null = isNulls[i].get();

            const auto& value = params[i];
            if (std::holds_alternative<std::monostate>(value)) {
                *isNulls[i] = true;
                bind.buffer_type = MYSQL_TYPE_NULL;
                continue;
            }

            if (auto v = std::get_if<int>(&value)) {
                i32Vals[i] = static_cast<int32_t>(*v);
                bind.buffer_type = MYSQL_TYPE_LONG;
                bind.buffer = &i32Vals[i];
                bind.buffer_length = sizeof(i32Vals[i]);
                continue;
            }

            if (auto v = std::get_if<int64_t>(&value)) {
                i64Vals[i] = *v;
                bind.buffer_type = MYSQL_TYPE_LONGLONG;
                bind.buffer = &i64Vals[i];
                bind.buffer_length = sizeof(i64Vals[i]);
                continue;
            }

            if (auto v = std::get_if<double>(&value)) {
                f64Vals[i] = *v;
                bind.buffer_type = MYSQL_TYPE_DOUBLE;
                bind.buffer = &f64Vals[i];
                bind.buffer_length = sizeof(f64Vals[i]);
                continue;
            }

            if (auto v = std::get_if<bool>(&value)) {
                boolVals[i] = static_cast<int8_t>(*v ? 1 : 0);
                bind.buffer_type = MYSQL_TYPE_TINY;
                bind.buffer = &boolVals[i];
                bind.buffer_length = sizeof(boolVals[i]);
                continue;
            }

            if (auto v = std::get_if<std::string>(&value)) {
                strVals[i] = *v;
                lengths[i] = static_cast<unsigned long>(strVals[i].size());
                bind.buffer_type = MYSQL_TYPE_STRING;
                bind.buffer = strVals[i].empty() ? nullptr : strVals[i].data();
                bind.buffer_length = lengths[i];
                continue;
            }

            if (auto v = std::get_if<std::vector<uint8_t>>(&value)) {
                blobVals[i] = *v;
                lengths[i] = static_cast<unsigned long>(blobVals[i].size());
                bind.buffer_type = MYSQL_TYPE_BLOB;
                bind.buffer = blobVals[i].empty() ? nullptr : blobVals[i].data();
                bind.buffer_length = lengths[i];
                continue;
            }
        }

        if (!binds.empty() && mysql_stmt_bind_param(stmt, binds.data()) != 0) {
            lastErr_ = mysql_stmt_error(stmt);
            spdlog::error("MySQL Bind Error: {} | SQL: {}", lastErr_, sql);
            const int errCode = mysql_stmt_errno(stmt);
            cleanupStmt();
            return DbResult<int64_t>::failure(lastErr_, errCode);
        }

        if (mysql_stmt_execute(stmt) != 0) {
            lastErr_ = mysql_stmt_error(stmt);
            spdlog::error("MySQL Stmt Execute Error: {} | SQL: {}", lastErr_, sql);
            const int errCode = mysql_stmt_errno(stmt);
            cleanupStmt();
            return DbResult<int64_t>::failure(lastErr_, errCode);
        }

        const auto affected = static_cast<int64_t>(mysql_stmt_affected_rows(stmt));
        cleanupStmt();
        lastErr_.clear();
        return DbResult<int64_t>::success(affected);
    }

    DbResult<void> begin() override {
        auto res = execute("START TRANSACTION");
        if (!res) {
            return DbResult<void>::failure(res.error().message, res.error().code);
        }
        return DbResult<void>::success();
    }

    DbResult<void> commit() override {
        auto res = execute("COMMIT");
        if (!res) {
            return DbResult<void>::failure(res.error().message, res.error().code);
        }
        return DbResult<void>::success();
    }

    DbResult<void> rollback() override {
        auto res = execute("ROLLBACK");
        if (!res) {
            return DbResult<void>::failure(res.error().message, res.error().code);
        }
        return DbResult<void>::success();
    }
};

class MysqlDriver : public IDriver {
public:
    std::unique_ptr<IConnection> createConnection(const nlohmann::json& config) override {
        return std::make_unique<MysqlConnection>(config);
    }

    std::string name() const override { return "mysql"; }
};

} // namespace sdb::drivers
