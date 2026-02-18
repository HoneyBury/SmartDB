#pragma once
#include "idb.hpp"

#include <vector>

namespace sdb {

using DbRow = std::vector<DbValue>;

inline DbResult<DbRow> queryOne(IConnection& conn, const std::string& sql) {
    auto rsRes = conn.query(sql);
    if (!rsRes) {
        return DbResult<DbRow>::failure(rsRes.error());
    }

    auto rs = rsRes.value();
    if (!rs || !rs->next()) {
        return DbResult<DbRow>::failure("No rows returned", 0, DbErrorKind::NotFound, false);
    }

    const auto cols = rs->columnNames();
    DbRow row;
    row.reserve(cols.size());
    for (size_t i = 0; i < cols.size(); ++i) {
        row.push_back(rs->get(static_cast<int>(i)));
    }
    return DbResult<DbRow>::success(std::move(row));
}

inline DbResult<std::vector<DbRow>> queryAll(IConnection& conn, const std::string& sql) {
    auto rsRes = conn.query(sql);
    if (!rsRes) {
        return DbResult<std::vector<DbRow>>::failure(rsRes.error());
    }

    auto rs = rsRes.value();
    std::vector<DbRow> rows;
    if (!rs) {
        return DbResult<std::vector<DbRow>>::success(std::move(rows));
    }

    const auto cols = rs->columnNames();
    while (rs->next()) {
        DbRow row;
        row.reserve(cols.size());
        for (size_t i = 0; i < cols.size(); ++i) {
            row.push_back(rs->get(static_cast<int>(i)));
        }
        rows.push_back(std::move(row));
    }

    return DbResult<std::vector<DbRow>>::success(std::move(rows));
}

} // namespace sdb
