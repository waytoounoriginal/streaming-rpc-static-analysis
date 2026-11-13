#ifndef REPRO_FIXED_CACHING_INTERCEPTOR_H
#define REPRO_FIXED_CACHING_INTERCEPTOR_H

#include <grpcpp/support/client_interceptor.h>

#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <string>

#include "absl/log/check.h"
#include "keyvaluestore.grpc.pb.h"

// Corrected version of CachingInterceptor that remedies the protocol and error
// handling defects in examples/cpp/interceptors/caching_interceptor.h:
//  1. Checks boolean return values of stream_->Write() and stream_->Read().
//  2. Only inserts {requested_key, response_} into cached_map_ on success.
//  3. Fails the hijacked recv message (FailHijackedRecvMessage) instead of
//     synthesizing an empty response on upstream stream failure.
//  4. Always finalizes the backing stream via stream_->Finish() before
//     propagating status to PRE_RECV_STATUS (deferring PRE_RECV_STATUS if an
//     async ClientBidiReactor posts finish_ops_ eagerly during StartCall()).
class FixedCachingInterceptor : public grpc::experimental::Interceptor {
 public:
  explicit FixedCachingInterceptor(
      grpc::experimental::ClientRpcInfo* /*info*/) {}

  void Intercept(
      ::grpc::experimental::InterceptorBatchMethods* methods) override {
    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_CANCEL)) {
      context_.TryCancel();
      return;
    }

    bool hijack = false;
    bool defer_proceed = false;

    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::
                PRE_SEND_INITIAL_METADATA)) {
      hijack = true;
      stub_ = keyvaluestore::KeyValueStore::NewStub(
          methods->GetInterceptedChannel());
      stream_ = stub_->GetValues(&context_);
      if (!stream_) {
        std::lock_guard<std::mutex> lock(mu_);
        stream_failed_ = true;
        finished_ = true;
        status_ = grpc::Status(grpc::StatusCode::INTERNAL,
                               "failed to open backing stream");
      }
    }

    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_MESSAGE)) {
      std::string requested_key;
      const keyvaluestore::Request* req_msg =
          static_cast<const keyvaluestore::Request*>(methods->GetSendMessage());
      if (req_msg != nullptr) {
        requested_key = req_msg->key();
      } else {
        keyvaluestore::Request deserialized;
        auto* buffer = methods->GetSerializedSendMessage();
        auto copied_buffer = *buffer;
        CHECK(grpc::SerializationTraits<keyvaluestore::Request>::Deserialize(
                  &copied_buffer, &deserialized)
                  .ok());
        requested_key = deserialized.key();
      }

      if (stream_failed_ || finished_) {
        last_read_failed_ = true;
        methods->FailHijackedSendMessage();
      } else {
        auto search = cached_map_.find(requested_key);
        if (search != cached_map_.end()) {
          std::cout << "[FIXED] " << requested_key << " found in map"
                    << std::endl;
          response_ = search->second;
          last_read_failed_ = false;
        } else {
          std::cout << "[FIXED] " << requested_key << " not found in cache"
                    << std::endl;
          keyvaluestore::Request req;
          req.set_key(requested_key);
          keyvaluestore::Response resp;
          bool w_ok = stream_->Write(req);
          bool r_ok = w_ok && stream_->Read(&resp);
          if (!w_ok || !r_ok) {
            last_read_failed_ = true;
            FinalizeBackingStream(/*need_writes_done=*/false);
            stream_failed_ = true;
            // Do NOT insert failed lookup into cached_map_!
          } else {
            response_ = resp.value();
            last_read_failed_ = false;
            cached_map_.insert({requested_key, response_});
          }
        }
      }
    }

    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_SEND_CLOSE)) {
      bool has_deferred_status = false;
      {
        std::lock_guard<std::mutex> lock(mu_);
        saw_pre_send_close_ = true;
        has_deferred_status = (pending_recv_status_methods_ != nullptr);
      }
      if (!finished_ && !writes_done_) {
        stream_->WritesDone();
        writes_done_ = true;
      }
      // If a callback reactor posted PRE_RECV_STATUS before PRE_SEND_CLOSE,
      // finalize the stream now and mark to unblock after Proceed().
      if (has_deferred_status) {
        FinalizeBackingStream(/*need_writes_done=*/false);
      }
    }

    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_RECV_MESSAGE)) {
      if (last_read_failed_ || stream_failed_) {
        methods->FailHijackedRecvMessage();
      } else {
        keyvaluestore::Response* resp =
            static_cast<keyvaluestore::Response*>(methods->GetRecvMessage());
        resp->set_value(response_);
      }
    }

    if (methods->QueryInterceptionHookPoint(
            grpc::experimental::InterceptionHookPoints::PRE_RECV_STATUS)) {
      std::unique_lock<std::mutex> lock(mu_);
      if (finished_ || saw_pre_send_close_) {
        lock.unlock();
        if (!finished_) {
          FinalizeBackingStream(/*need_writes_done=*/!writes_done_);
        }
        PopulateRecvStatus(methods);
      } else {
        // Callback ClientBidiReactor::StartCall() posts finish_ops_
        // (PRE_RECV_STATUS) eagerly before WritesDone/PRE_SEND_CLOSE!
        // Defer completing PRE_RECV_STATUS until backing stream finishes.
        pending_recv_status_methods_ = methods;
        defer_proceed = true;
      }
    }

    if (hijack) {
      methods->Hijack();
    } else if (!defer_proceed) {
      methods->Proceed();
    }

    // If PRE_SEND_CLOSE or PRE_SEND_MESSAGE finished the backing stream while
    // a deferred PRE_RECV_STATUS was waiting (ClientBidiReactor), release it.
    if (finished_) {
      FlushDeferredRecvStatus();
    }
  }

 private:
  void FinalizeBackingStream(bool need_writes_done) {
    std::unique_lock<std::mutex> lock(mu_);
    if (finished_) {
      return;
    }
    lock.unlock();
    if (need_writes_done && !writes_done_) {
      stream_->WritesDone();
      writes_done_ = true;
    }
    grpc::Status s = stream_->Finish();
    lock.lock();
    if (!finished_) {
      status_ = s;
      finished_ = true;
    }
  }

  void PopulateRecvStatus(
      ::grpc::experimental::InterceptorBatchMethods* methods) {
    auto* status = methods->GetRecvStatus();
    if (status != nullptr) {
      *status = status_;
    }
  }

  void FlushDeferredRecvStatus() {
    ::grpc::experimental::InterceptorBatchMethods* deferred = nullptr;
    {
      std::lock_guard<std::mutex> lock(mu_);
      if (finished_ && pending_recv_status_methods_ != nullptr) {
        deferred = pending_recv_status_methods_;
        pending_recv_status_methods_ = nullptr;
      }
    }
    if (deferred != nullptr) {
      PopulateRecvStatus(deferred);
      deferred->Proceed();
    }
  }

  grpc::ClientContext context_;
  std::unique_ptr<keyvaluestore::KeyValueStore::Stub> stub_;
  std::unique_ptr<
      grpc::ClientReaderWriter<keyvaluestore::Request, keyvaluestore::Response>>
      stream_;
  std::map<std::string, std::string> cached_map_;
  std::string response_;

  std::mutex mu_;
  grpc::Status status_ = grpc::Status::OK;
  bool writes_done_ = false;
  bool finished_ = false;
  bool stream_failed_ = false;
  bool last_read_failed_ = false;
  bool saw_pre_send_close_ = false;
  ::grpc::experimental::InterceptorBatchMethods* pending_recv_status_methods_ =
      nullptr;
};

class FixedCachingInterceptorFactory
    : public grpc::experimental::ClientInterceptorFactoryInterface {
 public:
  grpc::experimental::Interceptor* CreateClientInterceptor(
      grpc::experimental::ClientRpcInfo* /*info*/) override {
    return new FixedCachingInterceptor(nullptr);
  }
};

#endif  // REPRO_FIXED_CACHING_INTERCEPTOR_H
