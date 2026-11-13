// Faithful runtime reproduction harness for Apache Arrow Flight (cpp/src/arrow/flight/transport/grpc/grpc_client.cc)
// Compiled against real Flight.proto and real libgrpc++ 1.72.0 (MSYS2 UCRT64).
//
// Purpose: Empirically verify every claimed candidate bug (1A, 1B, 1C, 1D, 1E) and
// the GrpcResultStream baseline against real grpc::ClientReader, grpc::ClientReaderWriter,
// grpc::ClientContext, and grpc::experimental::ClientInterceptorFactoryInterface.

#include <atomic>
#include <cassert>
#include <chrono>
#include <condition_variable>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>
#include <grpcpp/version_info.h>

#include "Flight.grpc.pb.h"
#include "Flight.pb.h"

namespace flight_pb = arrow::flight::protocol;

// ============================================================================
// Minimal faithful Arrow Status / StopToken / Middleware / Auth abstractions
// (Matching upstream cpp/src/arrow/status.h, cancel.cc, client_auth.h,
//  serialization_internal.cc, and grpc_client.cc)
// ============================================================================

namespace arrow_repro {

enum class StatusCode {
  OK,
  Invalid,
  Cancelled,
  IOError,
  Unauthenticated,
  Unimplemented,
  UnknownError,
  Internal
};

inline const char* StatusCodeName(StatusCode c) {
  switch (c) {
    case StatusCode::OK: return "OK";
    case StatusCode::Invalid: return "Invalid";
    case StatusCode::Cancelled: return "Cancelled";
    case StatusCode::IOError: return "IOError";
    case StatusCode::Unauthenticated: return "Unauthenticated";
    case StatusCode::Unimplemented: return "Unimplemented";
    case StatusCode::UnknownError: return "UnknownError";
    case StatusCode::Internal: return "Internal";
  }
  return "?";
}

struct Status {
  StatusCode code_ = StatusCode::OK;
  std::string msg_;

  bool ok() const { return code_ == StatusCode::OK; }
  StatusCode code() const { return code_; }
  const std::string& message() const { return msg_; }
  bool IsCancelled() const { return code_ == StatusCode::Cancelled; }

  std::string ToString() const {
    if (ok()) return "OK";
    return std::string(StatusCodeName(code_)) + ": " + msg_;
  }

  static Status OK() { return {StatusCode::OK, ""}; }
  static Status Invalid(std::string m) { return {StatusCode::Invalid, std::move(m)}; }
  static Status Cancelled(std::string m) { return {StatusCode::Cancelled, std::move(m)}; }
  static Status IOError(std::string m) { return {StatusCode::IOError, std::move(m)}; }
  static Status Unauthenticated(std::string m) {
    return {StatusCode::Unauthenticated, std::move(m)};
  }
  static Status Unimplemented(std::string m) {
    return {StatusCode::Unimplemented, std::move(m)};
  }
  static Status UnknownError(std::string m) {
    return {StatusCode::UnknownError, std::move(m)};
  }
  static Status Internal(std::string m) { return {StatusCode::Internal, std::move(m)}; }
};

#define RETURN_NOT_OK(expr)     \
  do {                          \
    ::arrow_repro::Status _s = (expr); \
    if (!_s.ok()) return _s;    \
  } while (false)

#define ARROW_UNUSED(x) (void)(x)

// Faithful mirror of cpp/src/arrow/util/cancel.cc (lines 47-103)
struct StopSourceImpl {
  std::atomic<int> requested_{0};
  std::mutex mutex_;
  Status cancel_error_;
};

class StopToken {
 public:
  StopToken() = default;
  explicit StopToken(std::shared_ptr<StopSourceImpl> impl) : impl_(std::move(impl)) {}

  bool IsStopRequested() const {
    if (!impl_) return false;
    return impl_->requested_.load() != 0;
  }

  Status Poll() const {
    if (!impl_) return Status::OK();
    if (!impl_->requested_.load()) return Status::OK();
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    return impl_->cancel_error_;
  }

 private:
  std::shared_ptr<StopSourceImpl> impl_;
};

class StopSource {
 public:
  StopSource() : impl_(std::make_shared<StopSourceImpl>()) {}
  void RequestStop() { RequestStop(Status::Cancelled("Operation cancelled")); }
  void RequestStop(Status st) {
    std::lock_guard<std::mutex> lock(impl_->mutex_);
    if (!impl_->requested_) {
      impl_->requested_ = -1;
      impl_->cancel_error_ = std::move(st);
    }
  }
  StopToken token() { return StopToken(impl_); }

 private:
  std::shared_ptr<StopSourceImpl> impl_;
};

struct FlightCallOptions {
  std::chrono::milliseconds timeout{-1};
  std::vector<std::pair<std::string, std::string>> headers;
  StopToken stop_token;
};

// Faithful mirror of FromGrpcCode / FromGrpcStatus (util_internal.cc:183-262)
inline Status FromGrpcStatus(const ::grpc::Status& grpc_status,
                             ::grpc::ClientContext* ctx = nullptr) {
  (void)ctx;
  switch (grpc_status.error_code()) {
    case ::grpc::StatusCode::OK:
      return Status::OK();
    case ::grpc::StatusCode::CANCELLED:
      return Status::Cancelled(grpc_status.error_message());
    case ::grpc::StatusCode::INVALID_ARGUMENT:
      return Status::Invalid(grpc_status.error_message());
    case ::grpc::StatusCode::UNIMPLEMENTED:
      return Status::Unimplemented(grpc_status.error_message());
    case ::grpc::StatusCode::UNAUTHENTICATED:
      return Status::Unauthenticated(grpc_status.error_message());
    case ::grpc::StatusCode::INTERNAL:
      return Status::Internal(grpc_status.error_message());
    default:
      return Status::UnknownError("gRPC failed with error code " +
                                  std::to_string(static_cast<int>(grpc_status.error_code())) +
                                  ": " + grpc_status.error_message());
  }
}

// Faithful mirror of ClientRpc (grpc_client.cc:72-96)
struct ClientRpc {
  ::grpc::ClientContext context;

  explicit ClientRpc(const FlightCallOptions& options) {
    if (options.timeout.count() >= 0) {
      context.set_deadline(std::chrono::system_clock::now() + options.timeout);
    }
    for (const auto& header : options.headers) {
      context.AddMetadata(header.first, header.second);
    }
  }
};

// ============================================================================
// Serialization helpers (faithful to serialization_internal.cc)
// ============================================================================

struct FlightDescriptor {
  enum DescriptorType { UNKNOWN = 0, PATH = 1, CMD = 2 };
  DescriptorType type = UNKNOWN;
  std::string cmd;
  std::vector<std::string> path;
};

struct FlightEndpoint {
  std::string ticket;
  std::vector<std::string> locations;
};

struct FlightInfoData {
  std::string schema;
  FlightDescriptor descriptor;
  std::vector<FlightEndpoint> endpoints;
  int64_t total_records = -1;
  int64_t total_bytes = -1;
  bool ordered = false;
  std::string app_metadata;
};

struct ActionType {
  std::string type;
  std::string description;
};

struct Result {
  std::string body;
};

// Matches serialization_internal.cc:193-195 & types.cc Location::Parse (requires valid URI scheme)
inline Status ParseLocationUri(const std::string& uri, std::string* out) {
  auto pos = uri.find("://");
  if (uri.empty() || pos == std::string::npos || pos == 0) {
    return Status::Invalid("Cannot parse URI: '" + uri + "'");
  }
  *out = uri;
  return Status::OK();
}

// Matches serialization_internal.cc:282-297
inline Status FromProto(const flight_pb::FlightDescriptor& pb_descriptor,
                        FlightDescriptor* descriptor) {
  if (pb_descriptor.type() == flight_pb::FlightDescriptor::PATH) {
    descriptor->type = FlightDescriptor::PATH;
    descriptor->path.reserve(pb_descriptor.path_size());
    for (int i = 0; i < pb_descriptor.path_size(); ++i) {
      descriptor->path.emplace_back(pb_descriptor.path(i));
    }
  } else if (pb_descriptor.type() == flight_pb::FlightDescriptor::CMD) {
    descriptor->type = FlightDescriptor::CMD;
    descriptor->cmd = pb_descriptor.cmd();
  } else {
    return Status::Invalid("Client sent UNKNOWN descriptor type");
  }
  return Status::OK();
}

// Matches serialization_internal.cc:237-250
inline Status FromProto(const flight_pb::FlightEndpoint& pb_endpoint, FlightEndpoint* endpoint) {
  endpoint->ticket = pb_endpoint.ticket().ticket();
  endpoint->locations.resize(pb_endpoint.location_size());
  for (int i = 0; i < pb_endpoint.location_size(); ++i) {
    RETURN_NOT_OK(ParseLocationUri(pb_endpoint.location(i).uri(), &endpoint->locations[i]));
  }
  return Status::OK();
}

// Matches serialization_internal.cc:314-329
inline Status FromProto(const flight_pb::FlightInfo& pb_info, FlightInfoData* info) {
  RETURN_NOT_OK(FromProto(pb_info.flight_descriptor(), &info->descriptor));
  info->schema = pb_info.schema();
  info->endpoints.resize(pb_info.endpoint_size());
  for (int i = 0; i < pb_info.endpoint_size(); ++i) {
    RETURN_NOT_OK(FromProto(pb_info.endpoint(i), &info->endpoints[i]));
  }
  info->total_records = pb_info.total_records();
  info->total_bytes = pb_info.total_bytes();
  info->ordered = pb_info.ordered();
  info->app_metadata = pb_info.app_metadata();
  return Status::OK();
}

// Matches serialization_internal.cc:124-128 EXACTLY: no validation, always returns OK!
inline Status FromProto(const flight_pb::ActionType& pb_type, ActionType* type) {
  type->type = pb_type.type();
  type->description = pb_type.description();
  return Status::OK();
}

// Matches serialization_internal.cc:154-159 EXACTLY: no validation, always returns OK!
inline Status FromProto(const flight_pb::Result& pb_result, Result* result) {
  result->body = pb_result.body();
  return Status::OK();
}

// ============================================================================
// Telemetry & Arrow-faithful GrpcClientInterceptorAdapter (grpc_client.cc:112-215)
// ============================================================================

struct CallTelemetry {
  std::string case_id;
  std::string method;
  int start_call_count = 0;
  int pre_send_initial_metadata_count = 0;
  int post_recv_initial_metadata_count = 0;
  int post_recv_message_count = 0;
  int pre_send_close_count = 0;
  int pre_send_cancel_count = 0;
  int post_recv_status_count = 0;
  int received_trailers_calls = 0;
  int call_completed_calls = 0;
  ::grpc::StatusCode recv_grpc_status_code = ::grpc::StatusCode::OK;
  std::string recv_grpc_error_message;
  Status call_completed_arrow_status;
  std::map<std::string, std::string> trailing_metadata;
};

class TelemetryRegistry {
 public:
  std::shared_ptr<CallTelemetry> CreateCall(const std::string& method) {
    std::lock_guard<std::mutex> lock(mu_);
    auto rec = std::make_shared<CallTelemetry>();
    rec->method = method;
    rec->start_call_count = 1;
    records_.push_back(rec);
    return rec;
  }

  std::vector<std::shared_ptr<CallTelemetry>> Records() const {
    std::lock_guard<std::mutex> lock(mu_);
    return records_;
  }

  void Clear() {
    std::lock_guard<std::mutex> lock(mu_);
    records_.clear();
  }

 private:
  mutable std::mutex mu_;
  std::vector<std::shared_ptr<CallTelemetry>> records_;
};

class ClientMiddleware {
 public:
  explicit ClientMiddleware(std::shared_ptr<CallTelemetry> rec) : rec_(std::move(rec)) {}

  void ReceivedHeaders(
      const std::multimap<::grpc::string_ref, ::grpc::string_ref>& metadata) {
    std::lock_guard<std::mutex> lock(mu_);
    rec_->received_trailers_calls++;
    for (const auto& entry : metadata) {
      rec_->trailing_metadata[std::string(entry.first.data(), entry.first.length())] =
          std::string(entry.second.data(), entry.second.length());
    }
  }

  void CallCompleted(const Status& status) {
    std::lock_guard<std::mutex> lock(mu_);
    rec_->call_completed_calls++;
    rec_->call_completed_arrow_status = status;
  }

  std::shared_ptr<CallTelemetry> record() const { return rec_; }

 private:
  mutable std::mutex mu_;
  std::shared_ptr<CallTelemetry> rec_;
};

// Mirrors GrpcClientInterceptorAdapter (grpc_client.cc:112-162)
class GrpcClientInterceptorAdapter final : public ::grpc::experimental::Interceptor {
 public:
  explicit GrpcClientInterceptorAdapter(std::shared_ptr<CallTelemetry> rec)
      : rec_(std::move(rec)), middleware_(std::make_unique<ClientMiddleware>(rec_)) {}

  void Intercept(::grpc::experimental::InterceptorBatchMethods* methods) override {
    using Hook = ::grpc::experimental::InterceptionHookPoints;

    if (methods->QueryInterceptionHookPoint(Hook::PRE_SEND_INITIAL_METADATA)) {
      rec_->pre_send_initial_metadata_count++;
      auto* md = methods->GetSendInitialMetadata();
      if (md) {
        auto it = md->find("x-case-id");
        if (it != md->end()) {
          rec_->case_id = it->second;
        }
      }
    }

    if (methods->QueryInterceptionHookPoint(Hook::POST_RECV_INITIAL_METADATA)) {
      rec_->post_recv_initial_metadata_count++;
      if (methods->GetRecvInitialMetadata() && !methods->GetRecvInitialMetadata()->empty()) {
        middleware_->ReceivedHeaders(*methods->GetRecvInitialMetadata());
      }
    }

    if (methods->QueryInterceptionHookPoint(Hook::POST_RECV_MESSAGE)) {
      rec_->post_recv_message_count++;
    }

    if (methods->QueryInterceptionHookPoint(Hook::PRE_SEND_CLOSE)) {
      rec_->pre_send_close_count++;
    }

    if (methods->QueryInterceptionHookPoint(Hook::PRE_SEND_CANCEL)) {
      rec_->pre_send_cancel_count++;
    }

    // Exact mirror of grpc_client.cc:135-143:
    if (methods->QueryInterceptionHookPoint(Hook::POST_RECV_STATUS)) {
      rec_->post_recv_status_count++;
      assert(methods->GetRecvStatus() != nullptr);
      assert(methods->GetRecvTrailingMetadata() != nullptr);
      middleware_->ReceivedHeaders(*methods->GetRecvTrailingMetadata());
      const ::grpc::Status* grpc_st = methods->GetRecvStatus();
      rec_->recv_grpc_status_code = grpc_st->error_code();
      rec_->recv_grpc_error_message = grpc_st->error_message();
      const Status status = FromGrpcStatus(*grpc_st);
      middleware_->CallCompleted(status);
    }

    methods->Proceed();
  }

 private:
  std::shared_ptr<CallTelemetry> rec_;
  std::unique_ptr<ClientMiddleware> middleware_;
};

class GrpcClientInterceptorAdapterFactory final
    : public ::grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  explicit GrpcClientInterceptorAdapterFactory(std::shared_ptr<TelemetryRegistry> reg)
      : registry_(std::move(reg)) {}

  ::grpc::experimental::Interceptor* CreateClientInterceptor(
      ::grpc::experimental::ClientRpcInfo* info) override {
    auto rec = registry_->CreateCall(info ? info->method() : "<null>");
    return new GrpcClientInterceptorAdapter(std::move(rec));
  }

 private:
  std::shared_ptr<TelemetryRegistry> registry_;
};

// ============================================================================
// Auth Interfaces & Wrappers (faithful to client_auth.h & grpc_client.cc:217-259)
// ============================================================================

class ClientAuthSender {
 public:
  virtual ~ClientAuthSender() = default;
  virtual Status Write(const std::string& token) = 0;
};

class ClientAuthReader {
 public:
  virtual ~ClientAuthReader() = default;
  virtual Status Read(std::string* response) = 0;
};

class ClientAuthHandler {
 public:
  virtual ~ClientAuthHandler() = default;
  virtual Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) = 0;
};

class GrpcClientAuthSender : public ClientAuthSender {
 public:
  explicit GrpcClientAuthSender(
      std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>>
          stream)
      : stream_(std::move(stream)) {}

  Status Write(const std::string& token) override {
    flight_pb::HandshakeRequest response;
    response.set_payload(token);
    if (stream_->Write(response)) {
      return Status::OK();
    }
    return FromGrpcStatus(stream_->Finish());
  }

 private:
  std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>> stream_;
};

class GrpcClientAuthReader : public ClientAuthReader {
 public:
  explicit GrpcClientAuthReader(
      std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>>
          stream)
      : stream_(std::move(stream)) {}

  Status Read(std::string* token) override {
    flight_pb::HandshakeResponse request;
    if (stream_->Read(&request)) {
      *token = std::move(*request.mutable_payload());
      return Status::OK();
    }
    return FromGrpcStatus(stream_->Finish());
  }

 private:
  std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>> stream_;
};

// ============================================================================
// Faithful mirror of GrpcResultStream (grpc_client.cc:496-559)
// ============================================================================

class GrpcResultStream {
 public:
  explicit GrpcResultStream(const FlightCallOptions& options)
      : rpc_(options),
        stop_token_(options.stop_token),
        status_(Status::UnknownError("Internal implementation error, stream not started")) {}

  ~GrpcResultStream() {
    if (stream_) {
      rpc_.context.TryCancel();
      auto status = FromGrpcStatus(stream_->Finish(), &rpc_.context);
      ARROW_UNUSED(status);
    }
  }

  Status Init(flight_pb::FlightService::Stub* stub, const flight_pb::Action& pb_action) {
    stream_ = stub->DoAction(&rpc_.context, pb_action);
    stream_->WaitForInitialMetadata();
    return Status::OK();
  }

  Status Next(std::unique_ptr<Result>* out) {
    if (stream_) {
      flight_pb::Result pb_result;
      if (!stop_token_.IsStopRequested() && stream_->Read(&pb_result)) {
        auto result = std::make_unique<Result>();
        RETURN_NOT_OK(FromProto(pb_result, result.get()));
        *out = std::move(result);
        return Status::OK();
      } else if (stop_token_.IsStopRequested()) {
        rpc_.context.TryCancel();
      }
      RETURN_NOT_OK(stop_token_.Poll());

      status_ = FromGrpcStatus(stream_->Finish(), &rpc_.context);
      stream_.reset();
    }
    RETURN_NOT_OK(status_);
    *out = nullptr;
    return Status::OK();
  }

 private:
  ClientRpc rpc_;
  StopToken stop_token_;
  Status status_;
  std::unique_ptr<::grpc::ClientReader<flight_pb::Result>> stream_;
};

// ============================================================================
// Faithful mirrors of upstream GrpcClientImpl methods + Fixed versions
// ============================================================================

class ArrowGrpcClientMethods {
 public:
  explicit ArrowGrpcClientMethods(std::shared_ptr<::grpc::Channel> channel)
      : stub_(flight_pb::FlightService::NewStub(std::move(channel))) {}

  // Exact upstream ListFlights (grpc_client.cc:892-914)
  Status ListFlights_Upstream(const FlightCallOptions& options,
                              const flight_pb::Criteria& pb_criteria,
                              std::vector<FlightInfoData>* out_flights) {
    ClientRpc rpc(options);
    std::unique_ptr<::grpc::ClientReader<flight_pb::FlightInfo>> stream(
        stub_->ListFlights(&rpc.context, pb_criteria));

    std::vector<FlightInfoData> flights;
    flight_pb::FlightInfo pb_info;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_info)) {
      FlightInfoData info_data;
      RETURN_NOT_OK(FromProto(pb_info, &info_data));  // Line 907: early return!
      flights.emplace_back(std::move(info_data));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();  // Line 910
    RETURN_NOT_OK(options.stop_token.Poll());                           // Line 911: early return!
    *out_flights = std::move(flights);
    return FromGrpcStatus(stream->Finish(), &rpc.context);              // Line 913
  }

  // Fixed ListFlights (TryCancel + Finish on early exits)
  Status ListFlights_Fixed(const FlightCallOptions& options,
                           const flight_pb::Criteria& pb_criteria,
                           std::vector<FlightInfoData>* out_flights) {
    ClientRpc rpc(options);
    std::unique_ptr<::grpc::ClientReader<flight_pb::FlightInfo>> stream(
        stub_->ListFlights(&rpc.context, pb_criteria));

    std::vector<FlightInfoData> flights;
    flight_pb::FlightInfo pb_info;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_info)) {
      FlightInfoData info_data;
      Status status = FromProto(pb_info, &info_data);
      if (!status.ok()) {
        rpc.context.TryCancel();
        ARROW_UNUSED(FromGrpcStatus(stream->Finish(), &rpc.context));
        return status;
      }
      flights.emplace_back(std::move(info_data));
    }
    if (options.stop_token.IsStopRequested()) {
      rpc.context.TryCancel();
      ARROW_UNUSED(FromGrpcStatus(stream->Finish(), &rpc.context));
      return options.stop_token.Poll();
    }
    *out_flights = std::move(flights);
    return FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  // Exact upstream ListActions (grpc_client.cc:923-941)
  Status ListActions_Upstream(const FlightCallOptions& options,
                              std::vector<ActionType>* types) {
    flight_pb::Empty empty;
    ClientRpc rpc(options);
    std::unique_ptr<::grpc::ClientReader<flight_pb::ActionType>> stream(
        stub_->ListActions(&rpc.context, empty));

    flight_pb::ActionType pb_type;
    ActionType type;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_type)) {
      RETURN_NOT_OK(FromProto(pb_type, &type));  // Line 935 (FromProto always OK!)
      types->emplace_back(std::move(type));
    }
    if (options.stop_token.IsStopRequested()) rpc.context.TryCancel();  // Line 938
    RETURN_NOT_OK(options.stop_token.Poll());                           // Line 939: early return!
    return FromGrpcStatus(stream->Finish(), &rpc.context);              // Line 940
  }

  // Fixed ListActions
  Status ListActions_Fixed(const FlightCallOptions& options,
                           std::vector<ActionType>* types) {
    flight_pb::Empty empty;
    ClientRpc rpc(options);
    std::unique_ptr<::grpc::ClientReader<flight_pb::ActionType>> stream(
        stub_->ListActions(&rpc.context, empty));

    flight_pb::ActionType pb_type;
    ActionType type;
    while (!options.stop_token.IsStopRequested() && stream->Read(&pb_type)) {
      Status status = FromProto(pb_type, &type);
      if (!status.ok()) {
        rpc.context.TryCancel();
        ARROW_UNUSED(FromGrpcStatus(stream->Finish(), &rpc.context));
        return status;
      }
      types->emplace_back(std::move(type));
    }
    if (options.stop_token.IsStopRequested()) {
      rpc.context.TryCancel();
      ARROW_UNUSED(FromGrpcStatus(stream->Finish(), &rpc.context));
      return options.stop_token.Poll();
    }
    return FromGrpcStatus(stream->Finish(), &rpc.context);
  }

  // Exact upstream AuthenticateInternal (grpc_client.cc:1067-1091)
  Status AuthenticateInternal_Upstream(ClientRpc& rpc, ClientAuthHandler* auth_handler,
                                       int pre_writes_done_delay_ms = 0,
                                       bool* out_writes_done_ret = nullptr) {
    std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>>
        stream = stub_->Handshake(&rpc.context);
    if (auth_handler) {
      GrpcClientAuthSender outgoing{stream};
      GrpcClientAuthReader incoming{stream};
      RETURN_NOT_OK(auth_handler->Authenticate(&outgoing, &incoming));  // Line 1074
    }
    if (pre_writes_done_delay_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(pre_writes_done_delay_ms));
    }
    // Explicitly close our side of the connection
    bool finished_writes = stream->WritesDone();  // Line 1077
    if (out_writes_done_ret) *out_writes_done_ret = finished_writes;
    if (!finished_writes) {
      return Status::Internal("Could not finish writing before closing");  // Line 1079
    }
    flight_pb::HandshakeResponse response;
    while (stream->Read(&response)) {
    }
    RETURN_NOT_OK(FromGrpcStatus(stream->Finish(), &rpc.context));  // Line 1089
    return Status::OK();
  }

  // Fixed AuthenticateInternal (also tracks whether AuthSender/Reader already called Finish)
  Status AuthenticateInternal_Fixed(ClientRpc& rpc, ClientAuthHandler* auth_handler,
                                    int pre_writes_done_delay_ms = 0) {
    // Wrap stream with RAII guard that ensures Finish() unless already finished.
    // Note: GrpcClientAuthSender::Write / GrpcClientAuthReader::Read call stream->Finish()
    // only on Write()/Read() returning false; if Authenticate() fails directly on a valid
    // message or WritesDone() returns false, Finish() must still be called!
    struct HandshakeStreamGuard {
      ::grpc::ClientContext* ctx;
      std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>>
          stream;
      bool finished = false;
      ~HandshakeStreamGuard() {
        if (stream && !finished) {
          ctx->TryCancel();
          ARROW_UNUSED(stream->Finish());
        }
      }
    };

    HandshakeStreamGuard guard{&rpc.context, stub_->Handshake(&rpc.context), false};
    if (auth_handler) {
      // Custom tracking wrappers so guard doesn't double-call Finish() if Write/Read called it
      struct TrackedSender : ClientAuthSender {
        std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>> s;
        bool* finished;
        TrackedSender(decltype(s) st, bool* f) : s(std::move(st)), finished(f) {}
        Status Write(const std::string& token) override {
          flight_pb::HandshakeRequest req;
          req.set_payload(token);
          if (s->Write(req)) return Status::OK();
          *finished = true;
          return FromGrpcStatus(s->Finish());
        }
      } outgoing{guard.stream, &guard.finished};

      struct TrackedReader : ClientAuthReader {
        std::shared_ptr<::grpc::ClientReaderWriter<flight_pb::HandshakeRequest, flight_pb::HandshakeResponse>> s;
        bool* finished;
        TrackedReader(decltype(s) st, bool* f) : s(std::move(st)), finished(f) {}
        Status Read(std::string* token) override {
          flight_pb::HandshakeResponse resp;
          if (s->Read(&resp)) {
            *token = std::move(*resp.mutable_payload());
            return Status::OK();
          }
          *finished = true;
          return FromGrpcStatus(s->Finish());
        }
      } incoming{guard.stream, &guard.finished};

      RETURN_NOT_OK(auth_handler->Authenticate(&outgoing, &incoming));
    }
    if (pre_writes_done_delay_ms > 0) {
      std::this_thread::sleep_for(std::chrono::milliseconds(pre_writes_done_delay_ms));
    }
    bool finished_writes = guard.stream->WritesDone();
    if (!finished_writes) {
      guard.finished = true;
      Status server_st = FromGrpcStatus(guard.stream->Finish(), &rpc.context);
      if (!server_st.ok()) {
        return server_st;
      }
      return Status::Internal("Could not finish writing before closing");
    }
    flight_pb::HandshakeResponse response;
    while (guard.stream->Read(&response)) {
    }
    guard.finished = true;
    RETURN_NOT_OK(FromGrpcStatus(guard.stream->Finish(), &rpc.context));
    return Status::OK();
  }

  std::unique_ptr<GrpcResultStream> MakeResultStream(const FlightCallOptions& options,
                                                     const flight_pb::Action& action) {
    auto rs = std::make_unique<GrpcResultStream>(options);
    rs->Init(stub_.get(), action);
    return rs;
  }

 private:
  std::unique_ptr<flight_pb::FlightService::Stub> stub_;
};

}  // namespace arrow_repro

// ============================================================================
// Loopback FlightService Server
// ============================================================================

class TestFlightService final : public flight_pb::FlightService::Service {
 public:
  struct ServerStats {
    std::atomic<bool> saw_cancelled{false};
    std::atomic<int> items_written{0};
    void Reset() {
      saw_cancelled = false;
      items_written = 0;
    }
  };
  ServerStats stats;

  static std::string GetMode(const ::grpc::ServerContext* ctx) {
    const auto& md = ctx->client_metadata();
    auto it = md.find("x-test-mode");
    if (it == md.end()) return "";
    return std::string(it->second.data(), it->second.length());
  }

  static flight_pb::FlightInfo MakeValidFlightInfo(const std::string& cmd,
                                            const std::string& uri,
                                            int64_t records) {
    flight_pb::FlightInfo info;
    info.mutable_flight_descriptor()->set_type(flight_pb::FlightDescriptor::CMD);
    info.mutable_flight_descriptor()->set_cmd(cmd);
    auto* ep = info.add_endpoint();
    ep->mutable_ticket()->set_ticket("ticket-" + cmd);
    ep->add_location()->set_uri(uri);
    info.set_total_records(records);
    info.set_total_bytes(records * 64);
    return info;
  }

  ::grpc::Status DoAction(::grpc::ServerContext* context,
                          const flight_pb::Action* request,
                          ::grpc::ServerWriter<flight_pb::Result>* writer) override {
    (void)request;
    stats.Reset();
    std::string mode = GetMode(context);

    if (mode == "action-ok") {
      flight_pb::Result r1, r2;
      r1.set_body("res-1");
      r2.set_body("res-2");
      if (writer->Write(r1)) stats.items_written++;
      if (writer->Write(r2)) stats.items_written++;
      context->AddTrailingMetadata("x-server-trailer", "action-ok");
      return ::grpc::Status::OK;
    }

    if (mode == "action-slow") {
      for (int i = 0; i < 20; ++i) {
        if (context->IsCancelled()) {
          stats.saw_cancelled = true;
          break;
        }
        flight_pb::Result r;
        r.set_body("res-" + std::to_string(i));
        if (!writer->Write(r)) {
          stats.saw_cancelled = context->IsCancelled();
          break;
        }
        stats.items_written++;
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
      }
      if (context->IsCancelled()) stats.saw_cancelled = true;
      context->AddTrailingMetadata("x-server-trailer", "action-slow-end");
      return ::grpc::Status::OK;
    }

    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "unknown action mode");
  }

  ::grpc::Status ListFlights(::grpc::ServerContext* context,
                             const flight_pb::Criteria* request,
                             ::grpc::ServerWriter<flight_pb::FlightInfo>* writer) override {
    (void)request;
    stats.Reset();
    std::string mode = GetMode(context);

    if (mode == "flights-ok") {
      if (writer->Write(MakeValidFlightInfo("dataset_1", "grpc://127.0.0.1:8815", 10))) {
        stats.items_written++;
      }
      if (writer->Write(MakeValidFlightInfo("dataset_2", "grpc://127.0.0.1:8815", 20))) {
        stats.items_written++;
      }
      context->AddTrailingMetadata("x-server-trailer", "flights-ok");
      return ::grpc::Status::OK;
    }

    if (mode == "flights-bad-descriptor-eof") {
      if (writer->Write(MakeValidFlightInfo("good_1", "grpc://127.0.0.1:8815", 10))) {
        stats.items_written++;
      }
      flight_pb::FlightInfo bad;
      bad.mutable_flight_descriptor()->set_type(flight_pb::FlightDescriptor::UNKNOWN);
      bad.set_total_records(99);
      if (writer->Write(bad)) {
        stats.items_written++;
      }
      context->AddTrailingMetadata("x-server-trailer", "bad-descriptor-sent");
      return ::grpc::Status::OK;
    }

    if (mode == "flights-bad-descriptor-streaming") {
      if (writer->Write(MakeValidFlightInfo("good_1", "grpc://127.0.0.1:8815", 10))) {
        stats.items_written++;
      }
      flight_pb::FlightInfo bad;
      bad.mutable_flight_descriptor()->set_type(flight_pb::FlightDescriptor::UNKNOWN);
      if (writer->Write(bad)) {
        stats.items_written++;
      }
      // Keep streaming slowly until client cancels or limit reached
      for (int i = 0; i < 40; ++i) {
        if (context->IsCancelled()) {
          stats.saw_cancelled = true;
          break;
        }
        std::this_thread::sleep_for(std::chrono::milliseconds(10));
      }
      if (context->IsCancelled()) stats.saw_cancelled = true;
      return ::grpc::Status(::grpc::StatusCode::DATA_LOSS, "server-trailing-error");
    }

    if (mode == "flights-bad-location") {
      flight_pb::FlightInfo bad;
      bad.mutable_flight_descriptor()->set_type(flight_pb::FlightDescriptor::PATH);
      bad.mutable_flight_descriptor()->add_path("bad_loc_table");
      bad.add_endpoint()->add_location()->set_uri("://not-a-valid-uri");
      if (writer->Write(bad)) stats.items_written++;
      context->AddTrailingMetadata("x-server-trailer", "bad-location-sent");
      return ::grpc::Status::OK;
    }

    if (mode == "flights-slow-for-cancel") {
      for (int i = 0; i < 40; ++i) {
        if (context->IsCancelled()) {
          stats.saw_cancelled = true;
          break;
        }
        if (!writer->Write(MakeValidFlightInfo("slow_" + std::to_string(i),
                                               "grpc://127.0.0.1:8815", i))) {
          stats.saw_cancelled = context->IsCancelled();
          break;
        }
        stats.items_written++;
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
      }
      if (context->IsCancelled()) stats.saw_cancelled = true;
      context->AddTrailingMetadata("x-server-trailer", "flights-slow-done");
      return ::grpc::Status::OK;
    }

    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "unknown flights mode");
  }

  ::grpc::Status ListActions(::grpc::ServerContext* context,
                             const flight_pb::Empty* request,
                             ::grpc::ServerWriter<flight_pb::ActionType>* writer) override {
    (void)request;
    stats.Reset();
    std::string mode = GetMode(context);

    if (mode == "actions-weird-values") {
      flight_pb::ActionType a1;  // completely empty default strings
      flight_pb::ActionType a2;
      // Valid UTF-8 but empty/control-char/unrecognized strings (note: proto3 rejects
      // invalid UTF-8 bytes like 0xff inside protobuf wire deserialization during Read(),
      // before Arrow's FromProto() is ever called).
      a2.set_type(std::string("\x01\x02-unrecognized-action", 22));
      a2.set_description("");
      if (writer->Write(a1)) stats.items_written++;
      if (writer->Write(a2)) stats.items_written++;
      context->AddTrailingMetadata("x-server-trailer", "actions-weird-ok");
      return ::grpc::Status::OK;
    }

    if (mode == "actions-slow-for-cancel") {
      for (int i = 0; i < 40; ++i) {
        if (context->IsCancelled()) {
          stats.saw_cancelled = true;
          break;
        }
        flight_pb::ActionType a;
        a.set_type("act_" + std::to_string(i));
        a.set_description("desc_" + std::to_string(i));
        if (!writer->Write(a)) {
          stats.saw_cancelled = context->IsCancelled();
          break;
        }
        stats.items_written++;
        std::this_thread::sleep_for(std::chrono::milliseconds(15));
      }
      if (context->IsCancelled()) stats.saw_cancelled = true;
      context->AddTrailingMetadata("x-server-trailer", "actions-slow-done");
      return ::grpc::Status::OK;
    }

    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "unknown actions mode");
  }

  ::grpc::Status Handshake(
      ::grpc::ServerContext* context,
      ::grpc::ServerReaderWriter<flight_pb::HandshakeResponse, flight_pb::HandshakeRequest>* stream) override {
    stats.Reset();
    std::string mode = GetMode(context);

    if (mode == "handshake-unauthenticated-clean") {
      flight_pb::HandshakeRequest req;
      while (stream->Read(&req)) {
      }
      context->AddTrailingMetadata("x-auth-trailer", "clean-unauthenticated");
      return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Invalid credentials");
    }

    if (mode == "handshake-ok-with-bearer") {
      flight_pb::HandshakeRequest req;
      while (stream->Read(&req)) {
      }
      flight_pb::HandshakeResponse resp;
      resp.set_protocol_version(1);
      resp.set_payload("session-established");
      stream->Write(resp);
      context->AddTrailingMetadata("x-auth-trailer", "bearer-issued");
      return ::grpc::Status::OK;
    }

    if (mode == "handshake-challenge-then-client-reject") {
      flight_pb::HandshakeResponse challenge;
      challenge.set_protocol_version(1);
      challenge.set_payload("bad-server-challenge-nonce");
      stream->Write(challenge);
      flight_pb::HandshakeRequest req;
      while (stream->Read(&req)) {
      }
      if (context->IsCancelled()) stats.saw_cancelled = true;
      context->AddTrailingMetadata("x-auth-trailer", "challenge-mode");
      return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED, "Client abandoned challenge");
    }

    if (mode == "handshake-immediate-unimplemented") {
      context->AddTrailingMetadata("x-auth-trailer", "no-auth-handler");
      return ::grpc::Status(::grpc::StatusCode::UNIMPLEMENTED,
                            "This service does not have an authentication mechanism enabled.");
    }

    if (mode == "handshake-immediate-unauthenticated") {
      context->AddTrailingMetadata("x-auth-trailer", "immediate-unauth");
      return ::grpc::Status(::grpc::StatusCode::UNAUTHENTICATED,
                            "Immediate server rejection");
    }

    return ::grpc::Status(::grpc::StatusCode::INVALID_ARGUMENT, "unknown handshake mode");
  }
};

// ============================================================================
// Reporting helper
// ============================================================================

using namespace arrow_repro;

static void PrintTelemetry(const CallTelemetry& t) {
  std::cout << "  [Interceptor] case=" << t.case_id
            << " method=" << t.method
            << " StartCall=" << t.start_call_count
            << " PRE_SEND_META=" << t.pre_send_initial_metadata_count
            << " POST_RECV_META=" << t.post_recv_initial_metadata_count
            << " POST_RECV_MSG=" << t.post_recv_message_count
            << " PRE_SEND_CLOSE=" << t.pre_send_close_count
            << " PRE_SEND_CANCEL=" << t.pre_send_cancel_count
            << " POST_RECV_STATUS=" << t.post_recv_status_count
            << " CallCompleted=" << t.call_completed_calls;
  if (t.post_recv_status_count > 0) {
    std::cout << " (grpc=" << static_cast<int>(t.recv_grpc_status_code)
              << " \"" << t.recv_grpc_error_message << "\""
              << ", arrow=" << t.call_completed_arrow_status.ToString() << ")";
  }
  if (!t.trailing_metadata.empty()) {
    std::cout << " trailers={";
    bool first = true;
    for (const auto& kv : t.trailing_metadata) {
      if (!first) std::cout << ", ";
      std::cout << kv.first << "=" << kv.second;
      first = false;
    }
    std::cout << "}";
  }
  std::cout << "\n";
}

static FlightCallOptions MakeOpts(const std::string& case_id, const std::string& mode,
                                  StopToken token = StopToken()) {
  FlightCallOptions opts;
  opts.headers.emplace_back("x-case-id", case_id);
  opts.headers.emplace_back("x-test-mode", mode);
  opts.stop_token = std::move(token);
  return opts;
}

int main() {
  std::cout << std::unitbuf;
  std::cout << "==============================================================================\n";
  std::cout << "  Apache Arrow Flight C++ — Real libgrpc++ " << GRPC_CPP_VERSION_STRING
            << " Loopback Verification\n";
  std::cout << "  Source: cpp/src/arrow/flight/transport/grpc/grpc_client.cc (fb767a7)\n";
  std::cout << "==============================================================================\n\n";

  // Start real loopback gRPC server
  TestFlightService service;
  ::grpc::ServerBuilder builder;
  int selected_port = 0;
  builder.AddListeningPort("127.0.0.1:0", ::grpc::InsecureServerCredentials(), &selected_port);
  builder.RegisterService(&service);
  std::unique_ptr<::grpc::Server> server = builder.BuildAndStart();
  assert(server && selected_port > 0);
  const std::string target = "127.0.0.1:" + std::to_string(selected_port);
  std::cout << "[Server] Listening on loopback " << target << "\n\n";

  // Create intercepted channel mirroring GrpcClientImpl::Init (grpc_client.cc:839-846)
  auto registry = std::make_shared<TelemetryRegistry>();
  std::vector<std::unique_ptr<::grpc::experimental::ClientInterceptorFactoryInterface>> factories;
  factories.emplace_back(std::make_unique<GrpcClientInterceptorAdapterFactory>(registry));
  ::grpc::ChannelArguments args;
  auto channel = ::grpc::experimental::CreateCustomChannelWithInterceptors(
      target, ::grpc::InsecureChannelCredentials(), args, std::move(factories));

  ArrowGrpcClientMethods client(channel);

  auto last_rec = [&]() -> std::shared_ptr<CallTelemetry> {
    auto recs = registry->Records();
    assert(!recs.empty());
    return recs.back();
  };

  // --------------------------------------------------------------------------
  // T0: GrpcResultStream (DoAction) RAII Baseline
  // --------------------------------------------------------------------------
  std::cout << "--- [T0] Baseline: GrpcResultStream (DoAction, grpc_client.cc:496-559) ---\n";
  {
    flight_pb::Action action;
    action.set_type("test");
    auto rs = client.MakeResultStream(MakeOpts("T0a", "action-ok"), action);
    std::unique_ptr<Result> item;
    int count = 0;
    while (rs->Next(&item).ok() && item != nullptr) {
      count++;
    }
    assert(count == 2);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T0a PASS] Normal EOF finalized via Next() -> POST_RECV_STATUS=1\n";
  }
  {
    StopSource stop;
    flight_pb::Action action;
    action.set_type("slow");
    {
      auto rs = client.MakeResultStream(MakeOpts("T0b", "action-slow", stop.token()), action);
      std::unique_ptr<Result> item;
      assert(rs->Next(&item).ok() && item != nullptr);
      stop.RequestStop(Status::Cancelled("user stopped action"));
      Status st = rs->Next(&item);
      assert(st.IsCancelled());
      // Drop rs here: ~GrpcResultStream() MUST call TryCancel() + stream_->Finish()!
    }
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(t->pre_send_cancel_count >= 1);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T0b PASS] Early exit in Next() finalized by ~GrpcResultStream() -> POST_RECV_STATUS=1\n\n";
  }

  // --------------------------------------------------------------------------
  // T1: ListFlights Happy Path & Bug 1A (FromProto early return, line 907)
  // --------------------------------------------------------------------------
  std::cout << "--- [T1] ListFlights Happy Path & Bug 1A (FromProto, grpc_client.cc:907) ---\n";
  {
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Upstream(MakeOpts("T1a", "flights-ok"), flight_pb::Criteria(), &flights);
    assert(st.ok() && flights.size() == 2);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T1a PASS] Happy path reached line 913 Finish() -> POST_RECV_STATUS=1\n";
  }
  {
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Upstream(
        MakeOpts("T1b", "flights-bad-descriptor-eof"), flight_pb::Criteria(), &flights);
    assert(!st.ok() && st.code() == StatusCode::Invalid);
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T1b RESULT] Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n";
  }
  {
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Upstream(
        MakeOpts("T1b_stream", "flights-bad-descriptor-streaming"), flight_pb::Criteria(), &flights);
    assert(!st.ok() && st.code() == StatusCode::Invalid);
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T1b_stream RESULT] Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls
              << " | server_saw_cancelled=" << service.stats.saw_cancelled.load() << "\n";
  }
  {
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Upstream(
        MakeOpts("T1c", "flights-bad-location"), flight_pb::Criteria(), &flights);
    assert(!st.ok() && st.code() == StatusCode::Invalid);
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T1c RESULT] Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n\n";
  }

  // --------------------------------------------------------------------------
  // T2: ListFlights Bug 1B (TryCancel + StopToken::Poll() early return, lines 910-911)
  // --------------------------------------------------------------------------
  std::cout << "--- [T2] ListFlights Bug 1B (TryCancel + Poll(), grpc_client.cc:910-911) ---\n";
  {
    StopSource stop;
    stop.RequestStop(Status::Cancelled("pre-cancelled"));
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Upstream(
        MakeOpts("T2a", "flights-ok", stop.token()), flight_pb::Criteria(), &flights);
    assert(st.IsCancelled());
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T2a RESULT] Status=" << st.ToString()
              << " | PRE_SEND_CANCEL=" << t->pre_send_cancel_count
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n";
  }
  {
    StopSource stop;
    std::thread canceller([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(40));
      stop.RequestStop(Status::Cancelled("mid-stream cancel"));
    });
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Upstream(
        MakeOpts("T2b", "flights-slow-for-cancel", stop.token()), flight_pb::Criteria(), &flights);
    canceller.join();
    assert(st.IsCancelled());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T2b RESULT] Status=" << st.ToString()
              << " | PRE_SEND_CANCEL=" << t->pre_send_cancel_count
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls
              << " | server_saw_cancelled=" << service.stats.saw_cancelled.load() << "\n\n";
  }

  // --------------------------------------------------------------------------
  // T3: ListActions Bugs 1C (FromProto) & 1D (StopToken::Poll())
  // --------------------------------------------------------------------------
  std::cout << "--- [T3] ListActions Bug 1C (FromProto) & Bug 1D (StopToken, lines 935-940) ---\n";
  {
    std::vector<ActionType> actions;
    Status st = client.ListActions_Upstream(
        MakeOpts("T3a", "actions-weird-values"), &actions);
    assert(st.ok() && actions.size() == 2);
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T3a RESULT] ActionType FromProto ALWAYS returns OK (even on empty/binary strings); "
              << "line 935 early return is UNREACHABLE -> Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count << "\n";
  }
  {
    StopSource stop;
    std::thread canceller([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(40));
      stop.RequestStop(Status::Cancelled("mid-stream cancel actions"));
    });
    std::vector<ActionType> actions;
    Status st = client.ListActions_Upstream(
        MakeOpts("T3b", "actions-slow-for-cancel", stop.token()), &actions);
    canceller.join();
    assert(st.IsCancelled());
    std::this_thread::sleep_for(std::chrono::milliseconds(50));
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T3b RESULT] Status=" << st.ToString()
              << " | PRE_SEND_CANCEL=" << t->pre_send_cancel_count
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n\n";
  }

  // --------------------------------------------------------------------------
  // T4: AuthenticateInternal (lines 1067-1091) — False Positive vs Residual Bugs
  // --------------------------------------------------------------------------
  std::cout << "--- [T4] AuthenticateInternal (grpc_client.cc:1067-1091) ---\n";
  {
    // T4a-1: AuthenticateBasicToken path (auth_handler_ == nullptr) against UNAUTHENTICATED server
    ClientRpc rpc(MakeOpts("T4a_basic_unauth", "handshake-unauthenticated-clean"));
    Status st = client.AuthenticateInternal_Upstream(rpc, nullptr);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(st.code() == StatusCode::Unauthenticated);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T4a_basic_unauth RESULT] Upstream line 1089 DID call Finish()! Status="
              << st.ToString() << " (NOT masked as OK!) | POST_RECV_STATUS="
              << t->post_recv_status_count << "\n";
  }
  {
    // T4a-2: Custom ClientAuthHandler that succeeds against OK server
    struct GoodAuthHandler : ClientAuthHandler {
      Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) override {
        (void)incoming;
        return outgoing->Write("my-secret-token");
      }
    } handler;
    ClientRpc rpc(MakeOpts("T4a_handler_ok", "handshake-ok-with-bearer"));
    Status st = client.AuthenticateInternal_Upstream(rpc, &handler);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(st.ok());
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T4a_handler_ok RESULT] Normal Handshake Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count << "\n";
  }
  {
    // T4b: Residual Bug 1E-R1 — auth_handler_->Authenticate returns error at line 1074
    // after reading server challenge cleanly (Read() succeeded, so GrpcClientAuthReader did NOT call Finish)
    struct RejectChallengeHandler : ClientAuthHandler {
      Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) override {
        (void)outgoing;
        std::string challenge;
        RETURN_NOT_OK(incoming->Read(&challenge));
        if (challenge == "bad-server-challenge-nonce") {
          return Status::Invalid("Client rejected untrusted server challenge");
        }
        return Status::OK();
      }
    } handler;
    ClientRpc rpc(MakeOpts("T4b_handler_reject", "handshake-challenge-then-client-reject"));
    Status st = client.AuthenticateInternal_Upstream(rpc, &handler);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(st.code() == StatusCode::Invalid);
    std::cout << "  [T4b_handler_reject RESULT] Line 1074 early return -> Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n";
  }
  {
    // T4c-1: Residual Bug 1E-R2 — Immediate server UNIMPLEMENTED/UNAUTHENTICATED, no delay before WritesDone()
    ClientRpc rpc(MakeOpts("T4c_immediate_nodelay", "handshake-immediate-unimplemented"));
    bool wd_ret = true;
    Status st = client.AuthenticateInternal_Upstream(rpc, nullptr, 0, &wd_ret);
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T4c_immediate_nodelay RESULT] WritesDone()=" << (wd_ret ? "true" : "false")
              << " | Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n";
  }
  {
    // T4c-2: Residual Bug 1E-R2 — Immediate server UNAUTHENTICATED, with 40ms work before WritesDone()
    // (simulating server closing stream before client calls WritesDone())
    ClientRpc rpc(MakeOpts("T4c_delayed_writesdone", "handshake-immediate-unauthenticated"));
    bool wd_ret = true;
    Status st = client.AuthenticateInternal_Upstream(rpc, nullptr, 40, &wd_ret);
    auto t = last_rec();
    PrintTelemetry(*t);
    std::cout << "  [T4c_delayed_writesdone RESULT] WritesDone()=" << (wd_ret ? "true" : "false")
              << " | Status=" << st.ToString()
              << " | POST_RECV_STATUS=" << t->post_recv_status_count
              << " | CallCompleted=" << t->call_completed_calls << "\n\n";
  }

  // --------------------------------------------------------------------------
  // T5: Fixed Implementations Verification
  // --------------------------------------------------------------------------
  std::cout << "--- [T5] Fixed Implementations (TryCancel + Finish on Early Exits) ---\n";
  {
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Fixed(
        MakeOpts("T5a_bad_desc", "flights-bad-descriptor-streaming"), flight_pb::Criteria(), &flights);
    assert(st.code() == StatusCode::Invalid);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T5a_bad_desc PASS] Fixed ListFlights (FromProto error) -> POST_RECV_STATUS=1\n";
  }
  {
    StopSource stop;
    std::thread canceller([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(40));
      stop.RequestStop(Status::Cancelled("cancel fixed flights"));
    });
    std::vector<FlightInfoData> flights;
    Status st = client.ListFlights_Fixed(
        MakeOpts("T5a_stop", "flights-slow-for-cancel", stop.token()), flight_pb::Criteria(), &flights);
    canceller.join();
    assert(st.IsCancelled());
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T5a_stop PASS] Fixed ListFlights (StopToken) -> POST_RECV_STATUS=1\n";
  }
  {
    StopSource stop;
    std::thread canceller([&]() {
      std::this_thread::sleep_for(std::chrono::milliseconds(40));
      stop.RequestStop(Status::Cancelled("cancel fixed actions"));
    });
    std::vector<ActionType> actions;
    Status st = client.ListActions_Fixed(
        MakeOpts("T5b_stop", "actions-slow-for-cancel", stop.token()), &actions);
    canceller.join();
    assert(st.IsCancelled());
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T5b_stop PASS] Fixed ListActions (StopToken) -> POST_RECV_STATUS=1\n";
  }
  {
    struct RejectChallengeHandler : ClientAuthHandler {
      Status Authenticate(ClientAuthSender* outgoing, ClientAuthReader* incoming) override {
        (void)outgoing;
        std::string challenge;
        RETURN_NOT_OK(incoming->Read(&challenge));
        return Status::Invalid("Client rejected untrusted server challenge");
      }
    } handler;
    ClientRpc rpc(MakeOpts("T5c_auth_reject", "handshake-challenge-then-client-reject"));
    Status st = client.AuthenticateInternal_Fixed(rpc, &handler);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(st.code() == StatusCode::Invalid);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T5c_auth_reject PASS] Fixed AuthenticateInternal (auth_handler error) -> POST_RECV_STATUS=1\n";
  }
  {
    ClientRpc rpc(MakeOpts("T5c_delayed_wd", "handshake-immediate-unauthenticated"));
    Status st = client.AuthenticateInternal_Fixed(rpc, nullptr, 40);
    auto t = last_rec();
    PrintTelemetry(*t);
    assert(st.code() == StatusCode::Unauthenticated);
    assert(t->post_recv_status_count == 1 && t->call_completed_calls == 1);
    std::cout << "  [T5c_delayed_wd PASS] Fixed AuthenticateInternal (!finished_writes) -> preserved "
              << st.ToString() << ", POST_RECV_STATUS=1\n";
  }

  server->Shutdown();
  server->Wait();
  std::cout << "\nAll verification tests completed cleanly.\n";
  return 0;
}
