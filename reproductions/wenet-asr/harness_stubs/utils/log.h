#ifndef UTILS_LOG_H_
#define UTILS_LOG_H_

// Lightweight glog-compatible stub for compiling unmodified upstream
// wenet/runtime/core/grpc/grpc_server.cc and grpc_client.cc without OpenFST/glog.
// CHECK(cond) preserves Google glog semantics: logs FATAL and calls std::abort().

#include <cstdlib>
#include <iostream>
#include <mutex>
#include <sstream>

#ifndef DISALLOW_COPY_AND_ASSIGN
#define DISALLOW_COPY_AND_ASSIGN(TypeName) \
  TypeName(const TypeName&) = delete;      \
  TypeName& operator=(const TypeName&) = delete
#endif

namespace wenet_harness_log {

enum Severity {
  LOG_INFO = 0,
  LOG_WARNING = 1,
  LOG_ERROR = 2,
  LOG_FATAL = 3,
};

inline std::mutex& LogMutex() {
  static std::mutex mu;
  return mu;
}

class LogMessage {
 public:
  LogMessage(const char* file, int line, Severity severity)
      : file_(file), line_(line), severity_(severity) {}

  ~LogMessage() {
    const char* sev_str = "I";
    switch (severity_) {
      case LOG_INFO:
        sev_str = "I";
        break;
      case LOG_WARNING:
        sev_str = "W";
        break;
      case LOG_ERROR:
        sev_str = "E";
        break;
      case LOG_FATAL:
        sev_str = "F";
        break;
    }
    {
      std::lock_guard<std::mutex> lock(LogMutex());
      std::cerr << "[" << sev_str << " " << file_ << ":" << line_ << "] "
                << stream_.str() << std::endl;
      std::cerr.flush();
    }
    if (severity_ == LOG_FATAL) {
      std::abort();
    }
  }

  std::ostream& stream() { return stream_; }

 private:
  const char* file_;
  int line_;
  Severity severity_;
  std::ostringstream stream_;
};

class NullStream {
 public:
  template <typename T>
  NullStream& operator<<(const T&) {
    return *this;
  }
  NullStream& operator<<(std::ostream& (*)(std::ostream&)) { return *this; }
};

class Voidify {
 public:
  void operator&(std::ostream&) {}
};

}  // namespace wenet_harness_log

#define LOG(severity) \
  ::wenet_harness_log::LogMessage(__FILE__, __LINE__, ::wenet_harness_log::LOG_##severity).stream()

#define VLOG(level) \
  ::wenet_harness_log::NullStream()

#define CHECK(condition)                                                        \
  (condition) ? (void)0                                                         \
              : ::wenet_harness_log::Voidify() &                                \
                    ::wenet_harness_log::LogMessage(                            \
                        __FILE__, __LINE__, ::wenet_harness_log::LOG_FATAL).stream() \
                        << "Check failed: " #condition " "

#define CHECK_EQ(a, b) CHECK((a) == (b))

#endif  // UTILS_LOG_H_
