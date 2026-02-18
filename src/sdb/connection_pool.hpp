#pragma once
#include "idb.hpp"
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <string>
#include <utility>
#include <vector>

namespace sdb {

class ConnectionPool : public std::enable_shared_from_this<ConnectionPool> {
public:
    struct Options {
        size_t minSize = 0;
        size_t maxSize = 16;
        std::chrono::milliseconds waitTimeout{5000};
        bool testOnBorrow = true;
        bool testOnReturn = false;
    };

    struct MetricsSnapshot {
        uint64_t acquireAttempts = 0;
        uint64_t acquireSuccesses = 0;
        uint64_t acquireFailures = 0;
        uint64_t acquireTimeouts = 0;
        uint64_t waitEvents = 0;
        uint64_t factoryFailures = 0;
        uint64_t totalAcquireWaitMicros = 0;
        uint64_t averageAcquireWaitMicros = 0;
        size_t peakInUse = 0;
    };

    struct ReturnToPool {
        std::shared_ptr<ConnectionPool> pool;
        void operator()(IConnection* conn) const {
            if (!conn) {
                return;
            }
            if (pool) {
                pool->release(std::unique_ptr<IConnection>(conn));
                return;
            }
            delete conn;
        }
    };

    using Handle = std::unique_ptr<IConnection, ReturnToPool>;
    using Factory = std::function<DbResult<std::unique_ptr<IConnection>>()>;

    static DbResult<std::shared_ptr<ConnectionPool>> createWithFactory(Factory factory) {
        return createWithFactory(std::move(factory), Options{});
    }

    static DbResult<std::shared_ptr<ConnectionPool>> createWithFactory(Factory factory, Options options) {
        if (!factory) {
            return DbResult<std::shared_ptr<ConnectionPool>>::failure(
                "ConnectionPool requires a valid factory", 0, DbErrorKind::InvalidArgument, false);
        }
        if (options.maxSize == 0) {
            return DbResult<std::shared_ptr<ConnectionPool>>::failure(
                "ConnectionPool maxSize must be greater than 0", 0, DbErrorKind::InvalidArgument, false);
        }
        if (options.minSize > options.maxSize) {
            options.minSize = options.maxSize;
        }

        auto pool = std::shared_ptr<ConnectionPool>(new ConnectionPool(std::move(factory), options));
        return DbResult<std::shared_ptr<ConnectionPool>>::success(std::move(pool));
    }

    DbResult<Handle> acquire() {
        std::unique_lock<std::mutex> lock(mtx_);
        ++acquireAttempts_;
        const auto acquireStart = std::chrono::steady_clock::now();

        if (closed_) {
            const std::string error = "Connection pool is closed";
            lastError_ = error;
            recordFailureLocked(acquireStart, false);
            return DbResult<Handle>::failure(error, 0, DbErrorKind::Connection, true);
        }

        const auto deadline = std::chrono::steady_clock::now() + options_.waitTimeout;

        while (true) {
            if (!idle_.empty()) {
                auto conn = std::move(idle_.back());
                idle_.pop_back();
                lock.unlock();

                if (options_.testOnBorrow && !ensureOpen(*conn)) {
                    conn->close();
                    lock.lock();
                    if (total_ > 0) {
                        --total_;
                    }
                    cv_.notify_one();
                    if (options_.waitTimeout.count() == 0 ||
                        std::chrono::steady_clock::now() >= deadline) {
                        recordFailureLocked(acquireStart, false);
                        return DbResult<Handle>::failure(lastError_, 0, DbErrorKind::Connection, true);
                    }
                    continue;
                }

                lock.lock();
                recordSuccessLocked(acquireStart);
                lock.unlock();
                return DbResult<Handle>::success(wrap(std::move(conn)));
            }

            if (total_ < options_.maxSize) {
                ++total_;
                lock.unlock();

                auto connRes = createConnection();
                if (!connRes) {
                    lock.lock();
                    if (total_ > 0) {
                        --total_;
                    }
                    if (lastError_.empty()) {
                        lastError_ = "Connection factory returned null";
                    }
                    cv_.notify_one();
                    recordFailureLocked(acquireStart, false);
                    return DbResult<Handle>::failure(lastError_, 0, DbErrorKind::Connection, true);
                }

                auto conn = std::move(connRes.value());
                if (options_.testOnBorrow && !ensureOpen(*conn)) {
                    conn->close();
                    lock.lock();
                    if (total_ > 0) {
                        --total_;
                    }
                    cv_.notify_one();
                    if (options_.waitTimeout.count() == 0 ||
                        std::chrono::steady_clock::now() >= deadline) {
                        recordFailureLocked(acquireStart, false);
                        return DbResult<Handle>::failure(lastError_, 0, DbErrorKind::Connection, true);
                    }
                    continue;
                }

                lock.lock();
                recordSuccessLocked(acquireStart);
                lock.unlock();
                return DbResult<Handle>::success(wrap(std::move(conn)));
            }

            if (options_.waitTimeout.count() == 0) {
                const std::string error = "Connection pool exhausted";
                lastError_ = error;
                recordFailureLocked(acquireStart, false);
                return DbResult<Handle>::failure(error, 0, DbErrorKind::Connection, true);
            }

            ++waitEvents_;
            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
                const std::string error = "Connection pool acquire timed out";
                lastError_ = error;
                recordFailureLocked(acquireStart, true);
                return DbResult<Handle>::failure(error, 0, DbErrorKind::Timeout, true);
            }
        }
    }

    void shutdown() {
        std::vector<std::unique_ptr<IConnection>> toClose;
        {
            std::lock_guard<std::mutex> lock(mtx_);
            if (closed_) {
                return;
            }
            closed_ = true;
            toClose.swap(idle_);
            if (total_ >= toClose.size()) {
                total_ -= toClose.size();
            } else {
                total_ = 0;
            }
        }

        for (auto& conn : toClose) {
            if (conn) {
                conn->close();
            }
        }
        cv_.notify_all();
    }

    size_t totalSize() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return total_;
    }

    size_t idleSize() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return idle_.size();
    }

    size_t inUseSize() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return inUseSizeLocked();
    }

    std::string lastError() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return lastError_;
    }

    MetricsSnapshot metrics() const {
        std::lock_guard<std::mutex> lock(mtx_);
        MetricsSnapshot snapshot;
        snapshot.acquireAttempts = acquireAttempts_;
        snapshot.acquireSuccesses = acquireSuccesses_;
        snapshot.acquireFailures = acquireFailures_;
        snapshot.acquireTimeouts = acquireTimeouts_;
        snapshot.waitEvents = waitEvents_;
        snapshot.factoryFailures = factoryFailures_;
        snapshot.totalAcquireWaitMicros = totalAcquireWaitMicros_;
        snapshot.peakInUse = peakInUse_;
        const uint64_t completed = acquireSuccesses_ + acquireFailures_;
        snapshot.averageAcquireWaitMicros = completed == 0 ? 0 : (totalAcquireWaitMicros_ / completed);
        return snapshot;
    }

    void resetMetrics() {
        std::lock_guard<std::mutex> lock(mtx_);
        acquireAttempts_ = 0;
        acquireSuccesses_ = 0;
        acquireFailures_ = 0;
        acquireTimeouts_ = 0;
        waitEvents_ = 0;
        factoryFailures_ = 0;
        totalAcquireWaitMicros_ = 0;
        peakInUse_ = inUseSizeLocked();
    }

    ~ConnectionPool() { shutdown(); }

private:
    ConnectionPool(Factory factory, Options options) : factory_(std::move(factory)), options_(options) {
        idle_.reserve(options_.maxSize);

        for (size_t i = 0; i < options_.minSize; ++i) {
            auto connRes = createConnection();
            if (!connRes) {
                continue;
            }
            auto conn = std::move(connRes.value());
            if (options_.testOnBorrow && !ensureOpen(*conn)) {
                conn->close();
                continue;
            }
            idle_.push_back(std::move(conn));
            ++total_;
        }
    }

    Handle wrap(std::unique_ptr<IConnection> conn) {
        return Handle(conn.release(), ReturnToPool{shared_from_this()});
    }

    void release(std::unique_ptr<IConnection> conn) {
        if (!conn) {
            return;
        }

        std::unique_lock<std::mutex> lock(mtx_);
        const bool shouldDrop = closed_ || (options_.testOnReturn && !conn->isOpen());
        if (!shouldDrop) {
            idle_.push_back(std::move(conn));
            lock.unlock();
            cv_.notify_one();
            return;
        }

        lock.unlock();
        conn->close();
        lock.lock();
        if (total_ > 0) {
            --total_;
        }
        lock.unlock();
        cv_.notify_one();
    }

    DbResult<std::unique_ptr<IConnection>> createConnection() {
        try {
            auto connRes = factory_();
            if (!connRes) {
                const std::string msg = connRes.error().message.empty() ? "Connection factory returned null" : connRes.error().message;
                setError(msg);
                std::lock_guard<std::mutex> lock(mtx_);
                ++factoryFailures_;
                DbError err = connRes.error();
                err.message = msg;
                if (err.kind == DbErrorKind::Unknown) {
                    err.kind = DbErrorKind::Internal;
                }
                err.retryable = true;
                return DbResult<std::unique_ptr<IConnection>>::failure(std::move(err));
            }

            auto conn = std::move(connRes.value());
            if (!conn) {
                const std::string msg = "Connection factory returned null";
                setError(msg);
                std::lock_guard<std::mutex> lock(mtx_);
                ++factoryFailures_;
                return DbResult<std::unique_ptr<IConnection>>::failure(msg, 0, DbErrorKind::Internal, true);
            }

            return DbResult<std::unique_ptr<IConnection>>::success(std::move(conn));
        } catch (const std::exception& e) {
            const std::string msg = std::string("Connection factory error: ") + e.what();
            setError(msg);
            std::lock_guard<std::mutex> lock(mtx_);
            ++factoryFailures_;
            return DbResult<std::unique_ptr<IConnection>>::failure(msg, 0, DbErrorKind::Internal, true);
        } catch (...) {
            const std::string msg = "Connection factory error: unknown exception";
            setError(msg);
            std::lock_guard<std::mutex> lock(mtx_);
            ++factoryFailures_;
            return DbResult<std::unique_ptr<IConnection>>::failure(msg, 0, DbErrorKind::Internal, true);
        }
    }

    bool ensureOpen(IConnection& conn) {
        if (conn.isOpen()) {
            return true;
        }
        auto openRes = conn.open();
        if (!openRes) {
            setError(openRes.error().message);
            return false;
        }
        return true;
    }

    void setError(std::string message) {
        std::lock_guard<std::mutex> lock(mtx_);
        lastError_ = std::move(message);
    }

    size_t inUseSizeLocked() const {
        if (total_ < idle_.size()) {
            return 0;
        }
        return total_ - idle_.size();
    }

    void recordSuccessLocked(const std::chrono::steady_clock::time_point& acquireStart) {
        ++acquireSuccesses_;
        const auto waitMicros = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - acquireStart).count();
        totalAcquireWaitMicros_ += static_cast<uint64_t>(waitMicros < 0 ? 0 : waitMicros);

        const auto inUse = inUseSizeLocked();
        if (inUse > peakInUse_) {
            peakInUse_ = inUse;
        }
    }

    void recordFailureLocked(const std::chrono::steady_clock::time_point& acquireStart, bool timedOut) {
        ++acquireFailures_;
        if (timedOut) {
            ++acquireTimeouts_;
        }
        const auto waitMicros = std::chrono::duration_cast<std::chrono::microseconds>(
            std::chrono::steady_clock::now() - acquireStart).count();
        totalAcquireWaitMicros_ += static_cast<uint64_t>(waitMicros < 0 ? 0 : waitMicros);
    }

    Factory factory_;
    Options options_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::unique_ptr<IConnection>> idle_;
    size_t total_ = 0;
    bool closed_ = false;
    std::string lastError_;

    uint64_t acquireAttempts_ = 0;
    uint64_t acquireSuccesses_ = 0;
    uint64_t acquireFailures_ = 0;
    uint64_t acquireTimeouts_ = 0;
    uint64_t waitEvents_ = 0;
    uint64_t factoryFailures_ = 0;
    uint64_t totalAcquireWaitMicros_ = 0;
    size_t peakInUse_ = 0;
};

} // namespace sdb
