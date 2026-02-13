#pragma once
#include "idb.hpp"
#include <chrono>
#include <condition_variable>
#include <functional>
#include <memory>
#include <mutex>
#include <stdexcept>
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
    using Factory = std::function<std::unique_ptr<IConnection>()>;

    static std::shared_ptr<ConnectionPool> createWithFactory(Factory factory) {
        return createWithFactory(std::move(factory), Options{});
    }

    static std::shared_ptr<ConnectionPool> createWithFactory(Factory factory, Options options) {
        return std::shared_ptr<ConnectionPool>(new ConnectionPool(std::move(factory), options));
    }

    Handle acquire() {
        std::unique_lock<std::mutex> lock(mtx_);
        if (closed_) {
            lastError_ = "Connection pool is closed";
            return {};
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
                        return {};
                    }
                    continue;
                }
                return wrap(std::move(conn));
            }

            if (total_ < options_.maxSize) {
                ++total_;
                lock.unlock();
                auto conn = createConnection();
                if (!conn) {
                    lock.lock();
                    if (total_ > 0) {
                        --total_;
                    }
                    if (lastError_.empty()) {
                        lastError_ = "Connection factory returned null";
                    }
                    cv_.notify_one();
                    return {};
                }
                if (options_.testOnBorrow && !ensureOpen(*conn)) {
                    conn->close();
                    lock.lock();
                    if (total_ > 0) {
                        --total_;
                    }
                    cv_.notify_one();
                    if (options_.waitTimeout.count() == 0 ||
                        std::chrono::steady_clock::now() >= deadline) {
                        return {};
                    }
                    continue;
                }
                return wrap(std::move(conn));
            }

            if (options_.waitTimeout.count() == 0) {
                lastError_ = "Connection pool exhausted";
                return {};
            }

            if (cv_.wait_until(lock, deadline) == std::cv_status::timeout) {
                lastError_ = "Connection pool acquire timed out";
                return {};
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
        if (total_ < idle_.size()) {
            return 0;
        }
        return total_ - idle_.size();
    }

    std::string lastError() const {
        std::lock_guard<std::mutex> lock(mtx_);
        return lastError_;
    }

    ~ConnectionPool() { shutdown(); }

private:
    ConnectionPool(Factory factory, Options options) : factory_(std::move(factory)), options_(options) {
        if (!factory_) {
            throw std::invalid_argument("ConnectionPool requires a valid factory");
        }
        if (options_.maxSize == 0) {
            throw std::invalid_argument("ConnectionPool maxSize must be greater than 0");
        }
        if (options_.minSize > options_.maxSize) {
            options_.minSize = options_.maxSize;
        }

        idle_.reserve(options_.maxSize);

        for (size_t i = 0; i < options_.minSize; ++i) {
            auto conn = createConnection();
            if (!conn) {
                continue;
            }
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

    std::unique_ptr<IConnection> createConnection() {
        try {
            return factory_();
        } catch (const std::exception& e) {
            setError(std::string("Connection factory error: ") + e.what());
        } catch (...) {
            setError("Connection factory error: unknown exception");
        }
        return {};
    }

    bool ensureOpen(IConnection& conn) {
        if (conn.isOpen()) {
            return true;
        }
        if (!conn.open()) {
            setError(conn.lastError());
            return false;
        }
        return true;
    }

    void setError(std::string message) {
        std::lock_guard<std::mutex> lock(mtx_);
        lastError_ = std::move(message);
    }

    Factory factory_;
    Options options_;
    mutable std::mutex mtx_;
    std::condition_variable cv_;
    std::vector<std::unique_ptr<IConnection>> idle_;
    size_t total_ = 0;
    bool closed_ = false;
    std::string lastError_;
};

} // namespace sdb
