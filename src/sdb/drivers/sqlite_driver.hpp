#pragma once
#include "../idb.hpp"
#include <sqlite3.h>
#include <spdlog/spdlog.h>
#include <utility>

namespace sdb::drivers {

class SqliteResultSet : public IResultSet {
    sqlite3_stmt* stmt_ = nullptr;
    bool hasRow_ = false;
    std::vector<std::string> cols_;

public:
    explicit SqliteResultSet(sqlite3_stmt* stmt) : stmt_(stmt) {
        if (!stmt_) {
            return;
        }
        const int count = sqlite3_column_count(stmt_);
        cols_.reserve(static_cast<size_t>(count));
        for (int i = 0; i < count; ++i) {
            cols_.emplace_back(sqlite3_column_name(stmt_, i));
        }
    }

    ~SqliteResultSet() override {
        if (stmt_) {
            sqlite3_finalize(stmt_);
        }
    }

    bool next() override {
        if (!stmt_) {
            return false;
        }
        const int rc = sqlite3_step(stmt_);
        hasRow_ = (rc == SQLITE_ROW);
        return hasRow_;
    }

    DbValue get(int index) override {
        if (!hasRow_ || !stmt_ || index < 0 || index >= static_cast<int>(cols_.size())) {
            return std::monostate{};
        }

        switch (sqlite3_column_type(stmt_, index)) {
            case SQLITE_INTEGER:
                return static_cast<int64_t>(sqlite3_column_int64(stmt_, index));
            case SQLITE_FLOAT:
                return sqlite3_column_double(stmt_, index);
            case SQLITE_TEXT:
                return std::string(reinterpret_cast<const char*>(sqlite3_column_text(stmt_, index)));
            case SQLITE_BLOB: {
                const auto* blob = static_cast<const uint8_t*>(sqlite3_column_blob(stmt_, index));
                const int size = sqlite3_column_bytes(stmt_, index);
                if (!blob || size <= 0) {
                    return std::vector<uint8_t>{};
                }
                return std::vector<uint8_t>(blob, blob + size);
            }
            case SQLITE_NULL:
            default:
                return std::monostate{};
        }
    }

    DbValue get(const std::string& name) override {
        for (size_t i = 0; i < cols_.size(); ++i) {
            if (cols_[i] == name) {
                return get(static_cast<int>(i));
            }
        }
        return std::monostate{};
    }

    std::vector<std::string> columnNames() override { return cols_; }
};

class SqliteConnection : public IConnection {
    sqlite3* db_ = nullptr;
    std::string connStr_;
    std::string lastErr_;

public:
    explicit SqliteConnection(std::string str) : connStr_(std::move(str)) {}
    ~SqliteConnection() override { close(); }

    bool open() override {
        if (isOpen()) {
            return true;
        }

        const int rc = sqlite3_open(connStr_.c_str(), &db_);
        if (rc != SQLITE_OK) {
            lastErr_ = sqlite3_errmsg(db_ ? db_ : nullptr);
            close();
            return false;
        }
        return true;
    }

    void close() override {
        if (db_) {
            sqlite3_close(db_);
            db_ = nullptr;
        }
    }

    bool isOpen() const override { return db_ != nullptr; }

    std::shared_ptr<IResultSet> query(const std::string& sql) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return nullptr;
        }

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            lastErr_ = sqlite3_errmsg(db_);
            spdlog::error("SQLite query prepare failed: {}", lastErr_);
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            return nullptr;
        }
        return std::make_shared<SqliteResultSet>(stmt);
    }

    int64_t execute(const std::string& sql) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return -1;
        }

        char* err = nullptr;
        if (sqlite3_exec(db_, sql.c_str(), nullptr, nullptr, &err) != SQLITE_OK) {
            lastErr_ = err ? err : "Unknown error";
            sqlite3_free(err);
            return -1;
        }
        return sqlite3_changes(db_);
    }

    int64_t execute(const std::string& sql, const std::vector<DbValue>& params) override {
        if (!isOpen()) {
            lastErr_ = "Connection is closed";
            return -1;
        }

        sqlite3_stmt* stmt = nullptr;
        if (sqlite3_prepare_v2(db_, sql.c_str(), -1, &stmt, nullptr) != SQLITE_OK) {
            lastErr_ = sqlite3_errmsg(db_);
            return -1;
        }

        for (size_t i = 0; i < params.size(); ++i) {
            const int bindIndex = static_cast<int>(i + 1);
            const auto& p = params[i];
            int rc = SQLITE_OK;

            if (std::holds_alternative<std::monostate>(p)) {
                rc = sqlite3_bind_null(stmt, bindIndex);
            } else if (std::holds_alternative<int>(p)) {
                rc = sqlite3_bind_int(stmt, bindIndex, std::get<int>(p));
            } else if (std::holds_alternative<int64_t>(p)) {
                rc = sqlite3_bind_int64(stmt, bindIndex, std::get<int64_t>(p));
            } else if (std::holds_alternative<double>(p)) {
                rc = sqlite3_bind_double(stmt, bindIndex, std::get<double>(p));
            } else if (std::holds_alternative<bool>(p)) {
                rc = sqlite3_bind_int(stmt, bindIndex, std::get<bool>(p) ? 1 : 0);
            } else if (std::holds_alternative<std::string>(p)) {
                const auto& str = std::get<std::string>(p);
                rc = sqlite3_bind_text(stmt, bindIndex, str.c_str(), -1, SQLITE_TRANSIENT);
            } else if (std::holds_alternative<std::vector<uint8_t>>(p)) {
                const auto& blob = std::get<std::vector<uint8_t>>(p);
                rc = sqlite3_bind_blob(stmt, bindIndex, blob.data(), static_cast<int>(blob.size()), SQLITE_TRANSIENT);
            }

            if (rc != SQLITE_OK) {
                lastErr_ = sqlite3_errmsg(db_);
                sqlite3_finalize(stmt);
                return -1;
            }
        }

        if (sqlite3_step(stmt) != SQLITE_DONE) {
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
    std::unique_ptr<IConnection> createConnection(const nlohmann::json& config) override {
        std::string connString = config.value("path", ":memory:");
        return std::make_unique<SqliteConnection>(connString);
    }

    std::string name() const override { return "sqlite"; }
};

} // namespace sdb::drivers
