#pragma once
#include "types.hpp"

#include <spdlog/spdlog.h>

#include <atomic>
#include <chrono>
#include <optional>
#include <string>
#include <string_view>
#include <type_traits>
#include <utility>

namespace sdb {

struct OperationContext {
    std::string traceId;
    std::string operation;
};

inline std::string makeTraceId() {
    static std::atomic<uint64_t> seq{0};
    const auto now = std::chrono::system_clock::now().time_since_epoch();
    const auto micros = std::chrono::duration_cast<std::chrono::microseconds>(now).count();
    const auto n = seq.fetch_add(1, std::memory_order_relaxed);
    return std::to_string(micros) + "-" + std::to_string(n);
}

inline OperationContext makeOperationContext(std::string operation) {
    return OperationContext{makeTraceId(), std::move(operation)};
}

inline OperationContext childOperationContext(const OperationContext& parent, std::string operation) {
    return OperationContext{parent.traceId, std::move(operation)};
}

inline std::optional<OperationContext>& currentOperationContextSlot() {
    thread_local std::optional<OperationContext> ctx;
    return ctx;
}

inline std::optional<OperationContext> currentOperationContext() {
    return currentOperationContextSlot();
}

class OperationScope {
public:
    explicit OperationScope(OperationContext ctx)
        : previous_(currentOperationContextSlot()) {
        currentOperationContextSlot() = std::move(ctx);
    }

    ~OperationScope() {
        currentOperationContextSlot() = std::move(previous_);
    }

    OperationScope(const OperationScope&) = delete;
    OperationScope& operator=(const OperationScope&) = delete;

private:
    std::optional<OperationContext> previous_;
};

template <typename Fn>
class BoundOperation {
public:
    BoundOperation(std::optional<OperationContext> ctx, Fn fn)
        : ctx_(std::move(ctx)), fn_(std::move(fn)) {}

    template <typename... Args>
    decltype(auto) operator()(Args&&... args) {
        if (ctx_.has_value()) {
            OperationScope scope(*ctx_);
            return fn_(std::forward<Args>(args)...);
        }
        return fn_(std::forward<Args>(args)...);
    }

private:
    std::optional<OperationContext> ctx_;
    Fn fn_;
};

template <typename Fn>
auto bindOperationContext(const OperationContext& ctx, Fn&& fn) {
    return BoundOperation<std::decay_t<Fn>>(ctx, std::forward<Fn>(fn));
}

template <typename Fn>
auto bindCurrentOperationContext(Fn&& fn) {
    return BoundOperation<std::decay_t<Fn>>(currentOperationContext(), std::forward<Fn>(fn));
}

inline std::string escapeJson(std::string_view input) {
    std::string out;
    out.reserve(input.size() + 8);
    for (char c : input) {
        switch (c) {
            case '\\': out += "\\\\"; break;
            case '"': out += "\\\""; break;
            case '\n': out += "\\n"; break;
            case '\r': out += "\\r"; break;
            case '\t': out += "\\t"; break;
            default: out.push_back(c); break;
        }
    }
    return out;
}

inline std::string toStructuredLog(std::string_view event, const DbError& err) {
    std::string json = "{";
    json += "\"event\":\"" + escapeJson(event) + "\"";
    json += ",\"kind\":\"" + std::string(toString(err.kind)) + "\"";
    json += ",\"retryable\":" + std::string(err.retryable ? "true" : "false");
    json += ",\"code\":" + std::to_string(err.code);
    json += ",\"message\":\"" + escapeJson(err.message) + "\"";
    json += "}";
    return json;
}

inline std::string toStructuredLog(std::string_view event,
                                   const DbError& err,
                                   const OperationContext& ctx) {
    std::string json = "{";
    json += "\"event\":\"" + escapeJson(event) + "\"";
    json += ",\"trace_id\":\"" + escapeJson(ctx.traceId) + "\"";
    json += ",\"operation\":\"" + escapeJson(ctx.operation) + "\"";
    json += ",\"kind\":\"" + std::string(toString(err.kind)) + "\"";
    json += ",\"retryable\":" + std::string(err.retryable ? "true" : "false");
    json += ",\"code\":" + std::to_string(err.code);
    json += ",\"message\":\"" + escapeJson(err.message) + "\"";
    json += "}";
    return json;
}

inline std::string toStructuredEvent(std::string_view event,
                                     std::string_view message,
                                     const OperationContext& ctx) {
    std::string json = "{";
    json += "\"event\":\"" + escapeJson(event) + "\"";
    json += ",\"trace_id\":\"" + escapeJson(ctx.traceId) + "\"";
    json += ",\"operation\":\"" + escapeJson(ctx.operation) + "\"";
    json += ",\"message\":\"" + escapeJson(message) + "\"";
    json += "}";
    return json;
}

inline void logDbError(spdlog::level::level_enum level,
                       std::string_view event,
                       const DbError& err) {
    const auto& current = currentOperationContextSlot();
    if (current.has_value()) {
        spdlog::log(level, "{}", toStructuredLog(event, err, *current));
        return;
    }
    spdlog::log(level, "{}", toStructuredLog(event, err));
}

inline void logDbError(spdlog::level::level_enum level,
                       std::string_view event,
                       const DbError& err,
                       const OperationContext& ctx) {
    spdlog::log(level, "{}", toStructuredLog(event, err, ctx));
}

inline void logOperationEvent(spdlog::level::level_enum level,
                              std::string_view event,
                              std::string_view message,
                              const OperationContext& ctx) {
    spdlog::log(level, "{}", toStructuredEvent(event, message, ctx));
}

inline void logOperationEvent(spdlog::level::level_enum level,
                              std::string_view event,
                              std::string_view message) {
    const auto& current = currentOperationContextSlot();
    if (current.has_value()) {
        spdlog::log(level, "{}", toStructuredEvent(event, message, *current));
        return;
    }
    spdlog::log(level, "{{\"event\":\"{}\",\"message\":\"{}\"}}", escapeJson(event), escapeJson(message));
}

template <typename T>
inline void logResultError(spdlog::level::level_enum level,
                           std::string_view event,
                           const DbResult<T>& res) {
    if (!res) {
        logDbError(level, event, res.error());
    }
}

template <typename T>
inline void logResultError(spdlog::level::level_enum level,
                           std::string_view event,
                           const DbResult<T>& res,
                           const OperationContext& ctx) {
    if (!res) {
        logDbError(level, event, res.error(), ctx);
    }
}

} // namespace sdb
