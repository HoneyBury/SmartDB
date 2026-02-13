#pragma once
#include "../idb.hpp"
#include <sqlite3.h>
#include <spdlog/spdlog.h>

namespace sdb::drivers {

class SqliteResultSet : public IResultSet {
    sqlite3_stmt* stmt_ = nullptr;
    bool hasRow_ = false;
    std::vector<std::string> cols_;

public:
    SqliteResultSet(sqlite3_stmt* stmt) : stmt_(stmt) {
        int count = sqlite3_column_count(stmt_);
        for(int i=0; i<count; ++i) {
            cols_.emplace_back(sqlite3_column_name(stmt_, i));
        }
    }
    ~SqliteResultSet() { if(stmt_) sqlite3_finalize(stmt_); }

    bool next() override {
        int rc = sqlite3_step(stmt_);
        hasRow_ = (rc == SQLITE_ROW);
        return hasRow_;
    }

    DbValue get(int index) override {
        if (!hasRow_) return {};
        switch(sqlite3_column_type(stmt_, index)) {
            case SQLITE_INTEGER: return (int64_t)sqlite3_column_int64(stmt_, index);
            case SQLITE_FLOAT: return sqlite3_column_double(stmt_, index);
            case SQLITE_TEXT: return std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt_, index)));
            case SQLITE_NULL: return std::monostate{};
            // TODO: Blob handling
            default: return std::monostate{};
        }
    }

    DbValue get(const std::string& name) override {
        for(size_t i=0; i<cols_.size(); ++i) {
            if(cols_[i] == name) return get(i);
        }
        return {};
    }

    std::vector<std::string> columnNames() override { return cols_; }
};

class SqliteConnection : public IConnection {
    sqlite3* db_ = nullptr;
    std::string connStr_;
    std::string lastErr_;

public:
    SqliteConnection(const std::string& str) : connStr_(str) {}
    ~SqliteConnection() { close(); }

    bool open() override {
        int rc = sqlite3_open(connStr_.c_str(), &db_);
        if (rc != SQLITE_OK) {
            lastErr_ = sqlite3_errmsg(db_ ? db_ : nullptr);
            return false;
        }
        return true;
    }

    void close() override {
        if(db_) { sqlite3_close(db_); db_ = nullptr; }
    }
    bool isOpen() const override { return db_ != nullptr; }

    std::shared_ptr<IResultSet> query(const std::string& sql) override {
        sqlite3_stmt* stmt;
        if(sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            lastErr_ = sqlite3_errmsg(db_);
            spdlog::error("Query failed: {}", lastErr_);
            return nullptr;
        }
        return std::make_shared<SqliteResultSet>(stmt);
    }

    int64_t execute(const std::string& sql) override {
        char* err = nullptr;
        if(sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            lastErr_ = err ? err : "Unknown error";
            sqlite3_free(err);
            return -1;
        }
        return sqlite3_changes(db_);
    }

    int64_t execute(const std::string& sql, const std::vector<DbValue>& params) override {
        // 简化的 Parameter Binding 实现
        sqlite3_stmt* stmt;
        if(sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) return -1;

        for(size_t i=0; i<params.size(); ++i) {
            // 参数绑定 (简化版，仅处理 Int/String)
            if(std::holds_alternative<int>(params[i]))
                sqlite3_bind_int(stmt, i+1, std::get<int>(params[i]));
            else if(std::holds_alternative<std::string>(params[i]))
                sqlite3_bind_text(stmt, i+1, std::get<std::string>(params[i]).c_str(), -1, SQLITE_TRANSIENT);
            else if(std::holds_alternative<double>(params[i]))
                sqlite3_bind_double(stmt, i+1, std::get<double>(params[i]));
        }

        if(sqlite3_step(stmt) != SQLITE_DONE) {
             lastErr_ = sqlite3_errmsg(db_);
             sqlite3_finalize(stmt);
             return -1;
        }
        sqlite3_finalize(stmt);
        return sqlite3_changes(db_);
    }

    bool begin() override { return execute("BEGIN") >= 0; }
    bool commit() override { return execute("COMMIT") >= 0; }
    bool rollback() override { return execute("ROLLBACK") >= 0; }
    std::string lastError() const override { return lastErr_; }
};

class SqliteDriver : public IDriver {
public:
    std::unique_ptr<IConnection> createConnection(const nlohmann::json & config) override {
        std::string connString = config.value("path", ":memory:");
        return std::make_unique<SqliteConnection>(connString);
    }
    std::string name() const override { return "sqlite"; }
};

} // namespace