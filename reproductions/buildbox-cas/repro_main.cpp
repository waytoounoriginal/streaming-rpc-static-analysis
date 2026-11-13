#include <atomic>
#include <chrono>
#include <condition_variable>
#include <cstdint>
#include <filesystem>
#include <fstream>
#include <future>
#include <iostream>
#include <memory>
#include <mutex>
#include <sstream>
#include <stdexcept>
#include <string>
#include <system_error>
#include <thread>
#include <vector>

#include <grpcpp/grpcpp.h>
#include <grpcpp/support/client_interceptor.h>

#include "buildbox_repro.grpc.pb.h"
#include "buildbox_repro.pb.h"

namespace fs = std::filesystem;

namespace {

// Deterministic simple digest helper (exact length + 64-bit FNV-1a hex) so we
// do not depend on OpenSSL headers while faithfully checking content & size.
std::string computeHash(const std::string &data) {
    uint64_t h = 14695981039346656037ULL;
    for (unsigned char c : data) {
        h ^= c;
        h *= 1099511628211ULL;
    }
    std::ostringstream oss;
    oss << std::hex << h;
    return oss.str();
}

buildbox_repro::Digest makeDigest(const std::string &data) {
    buildbox_repro::Digest d;
    d.set_hash(computeHash(data));
    d.set_size_bytes(static_cast<int64_t>(data.size()));
    return d;
}

// ============================================================================
// Client Interceptor to empirically observe whether POST_RECV_STATUS fires
// when Finish() is skipped vs called (a real consequence in libgrpc++).
// ============================================================================
struct InterceptorStats {
    std::atomic<int> started{0};
    std::atomic<int> post_recv_status{0};
    std::atomic<int> post_recv_status_ok{0};
    std::atomic<int> post_recv_status_cancelled{0};
    std::atomic<int> post_recv_status_other{0};

    void reset() {
        started = 0;
        post_recv_status = 0;
        post_recv_status_ok = 0;
        post_recv_status_cancelled = 0;
        post_recv_status_other = 0;
    }
};

class TrackingInterceptor final : public grpc::experimental::Interceptor {
  public:
    explicit TrackingInterceptor(InterceptorStats *stats) : stats_(stats) {
        stats_->started.fetch_add(1, std::memory_order_relaxed);
    }

    void Intercept(grpc::experimental::InterceptorBatchMethods *methods) override {
        if (methods->QueryInterceptionHookPoint(
                grpc::experimental::InterceptionHookPoints::POST_RECV_STATUS)) {
            stats_->post_recv_status.fetch_add(1, std::memory_order_relaxed);
            const grpc::Status *status = methods->GetRecvStatus();
            if (status != nullptr) {
                if (status->ok()) {
                    stats_->post_recv_status_ok.fetch_add(1, std::memory_order_relaxed);
                } else if (status->error_code() == grpc::StatusCode::CANCELLED) {
                    stats_->post_recv_status_cancelled.fetch_add(
                        1, std::memory_order_relaxed);
                } else {
                    stats_->post_recv_status_other.fetch_add(
                        1, std::memory_order_relaxed);
                }
            }
        }
        methods->Proceed();
    }

  private:
    InterceptorStats *stats_;
};

class TrackingInterceptorFactory final
    : public grpc::experimental::ClientInterceptorFactoryInterface {
  public:
    explicit TrackingInterceptorFactory(InterceptorStats *stats)
        : stats_(stats) {}

    grpc::experimental::Interceptor *CreateClientInterceptor(
        grpc::experimental::ClientRpcInfo * /*info*/) override {
        return new TrackingInterceptor(stats_);
    }

  private:
    InterceptorStats *stats_;
};

std::shared_ptr<grpc::Channel>
createTrackedChannel(const std::string &target, InterceptorStats *stats) {
    std::vector<
        std::unique_ptr<grpc::experimental::ClientInterceptorFactoryInterface>>
        creators;
    creators.push_back(std::make_unique<TrackingInterceptorFactory>(stats));
    return grpc::experimental::CreateCustomChannelWithInterceptors(
        target, grpc::InsecureChannelCredentials(), grpc::ChannelArguments(),
        std::move(creators));
}

// ============================================================================
// Server Simulation 1: LocalCasInstance::StageTree
// Matches casd/buildboxcasd_server.cpp:1096-1113 and
// casd/buildboxcasd_localcasinstance.cpp:387-638.
// ============================================================================
struct StageServerConfig {
    std::atomic<int> cleanup_delay_ms{0};
    std::atomic<bool> force_cleanup_error{false};
    std::atomic<bool> reject_nonempty_restage{false};
};

struct StageCallRecord {
    int call_id = 0;
    std::string stage_path;
    bool received_cleanup_request = false;
    bool cleanup_request_was_empty = false;
    bool cancelled_before_cleanup = false;
    bool cancelled_after_cleanup = false;
    bool ack_write_ok = false;
    bool dir_existed_before_cleanup = false;
    bool dir_deleted_by_server = false;
    double cleanup_duration_ms = 0.0;
    grpc::Status returned_status;
};

class StageTreeServiceImpl final
    : public buildbox_repro::LocalContentAddressableStorage::Service {
  public:
    StageTreeServiceImpl(fs::path root_tmp, StageServerConfig *cfg)
        : root_tmp_(std::move(root_tmp)), cfg_(cfg) {}

    // Prepare synchronization for the next incoming StageTree RPC.
    std::future<StageCallRecord> expectNextCall() {
        std::lock_guard<std::mutex> lk(mu_);
        pending_promises_.emplace_back();
        return pending_promises_.back().get_future();
    }

    grpc::Status StageTree(
        grpc::ServerContext *ctx,
        grpc::ServerReaderWriter<buildbox_repro::StageTreeResponse,
                                 buildbox_repro::StageTreeRequest> *stream)
        override {
        StageCallRecord rec;
        rec.call_id = next_id_.fetch_add(1, std::memory_order_relaxed);

        // Mirrors LocalCasServicer::StageTree (buildboxcasd_server.cpp:1101)
        buildbox_repro::StageTreeRequest stage_request;
        if (!stream->Read(&stage_request)) {
            rec.returned_status = grpc::Status(grpc::StatusCode::INTERNAL,
                                               "Could not read request.");
            completeRecord(std::move(rec));
            return rec.returned_status;
        }

        // Mirrors LocalCasInstance::StageTree path handling (lines 416-446)
        bool stage_directory_needs_deleting = false;
        fs::path stage_path;
        if (stage_request.path().empty()) {
            stage_path =
                root_tmp_ / ("staged_" + std::to_string(rec.call_id));
            fs::create_directories(stage_path);
            stage_directory_needs_deleting = true;
        } else {
            stage_path = fs::path(stage_request.path());
            std::error_code chk_ec;
            if (cfg_->reject_nonempty_restage.load() &&
                fs::exists(stage_path, chk_ec) &&
                !fs::is_empty(stage_path, chk_ec)) {
                rec.stage_path = stage_path.string();
                rec.returned_status = grpc::Status(
                    grpc::StatusCode::FAILED_PRECONDITION,
                    "Stage path exists and is not empty (race collision)");
                completeRecord(std::move(rec));
                return rec.returned_status;
            }
            if (!fs::exists(stage_path, chk_ec)) {
                fs::create_directories(stage_path, chk_ec);
                stage_directory_needs_deleting = true;
            }
        }
        rec.stage_path = stage_path.string();

        // Populate realistic staged tree contents on disk
        for (int i = 0; i < 8; ++i) {
            std::ofstream ofs(stage_path / ("artifact_" + std::to_string(i) + ".dat"),
                              std::ios::binary);
            ofs << "staged payload chunk " << i << "\n";
        }
        rec.dir_existed_before_cleanup = fs::exists(stage_path);

        // Phase 1 response: tree is staged and ready
        buildbox_repro::StageTreeResponse response;
        response.set_path(stage_path.string());
        if (!stream->Write(response)) {
            rec.returned_status = grpc::Status(grpc::StatusCode::CANCELLED,
                                               "Failed to write stage response");
            completeRecord(std::move(rec));
            return rec.returned_status;
        }

        // Phase 2: Wait for empty cleanup request (casd line 572)
        buildbox_repro::StageTreeRequest cleanup_request;
        rec.received_cleanup_request = stream->Read(&cleanup_request);
        rec.cleanup_request_was_empty =
            cleanup_request.path().empty() &&
            cleanup_request.root_digest().hash().empty() &&
            cleanup_request.root_digest().size_bytes() == 0;
        rec.cancelled_before_cleanup = ctx->IsCancelled();

        grpc::Status stage_status = grpc::Status::OK;
        if (rec.received_cleanup_request && !rec.cleanup_request_was_empty) {
            stage_status = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                        "Unexpected non-empty request after staging");
        }
        if (cfg_->force_cleanup_error.load()) {
            stage_status = grpc::Status(grpc::StatusCode::INTERNAL,
                                        "Simulated pre-unstage command / cleanup error");
        }

        const auto cleanup_start = std::chrono::steady_clock::now();

        // Simulate pre_unstage_commands (lines 592-614)
        const int delay_ms = cfg_->cleanup_delay_ms.load();
        if (delay_ms > 0) {
            std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
        }
        for (const auto &marker_path : stage_request.pre_unstage_commands()) {
            std::ofstream marker(marker_path, std::ios::binary);
            marker << "pre_unstage_done\n";
        }

        // Delete staged directory contents / directory (lines 618-630)
        std::error_code ec;
        if (stage_directory_needs_deleting) {
            fs::remove_all(stage_path, ec);
        } else {
            for (const auto &entry : fs::directory_iterator(stage_path, ec)) {
                fs::remove_all(entry.path(), ec);
            }
        }
        rec.dir_deleted_by_server =
            stage_directory_needs_deleting ? !fs::exists(stage_path)
                                           : (fs::exists(stage_path) && fs::is_empty(stage_path));

        const auto cleanup_end = std::chrono::steady_clock::now();
        rec.cleanup_duration_ms =
            std::chrono::duration<double, std::milli>(cleanup_end - cleanup_start)
                .count();

        rec.cancelled_after_cleanup = ctx->IsCancelled();

        // casd lines 633-635: Sending empty reply signaling cleanup is complete
        if (rec.received_cleanup_request) {
            rec.ack_write_ok = stream->Write(buildbox_repro::StageTreeResponse());
        }

        rec.returned_status = stage_status;
        completeRecord(rec);
        return stage_status;
    }

  private:
    void completeRecord(StageCallRecord rec) {
        std::lock_guard<std::mutex> lk(mu_);
        if (!pending_promises_.empty()) {
            auto p = std::move(pending_promises_.front());
            pending_promises_.erase(pending_promises_.begin());
            p.set_value(std::move(rec));
        }
    }

    fs::path root_tmp_;
    StageServerConfig *cfg_;
    std::atomic<int> next_id_{1};
    std::mutex mu_;
    std::vector<std::promise<StageCallRecord>> pending_promises_;
};

// Mirrors GrpcCASClient::GrpcStagedDirectory (common/buildboxcommon_grpccasclient.h:94-125,
// common/buildboxcommon_grpccasclient.cpp:1306-1323)
class ReproGrpcStagedDirectory {
  public:
    ReproGrpcStagedDirectory(
        std::shared_ptr<grpc::ClientContext> context,
        std::shared_ptr<grpc::ClientReaderWriterInterface<
            buildbox_repro::StageTreeRequest, buildbox_repro::StageTreeResponse>>
            reader_writer,
        std::string path, bool fixed_mode, grpc::Status *out_finish_status)
        : d_path(std::move(path)), d_context(std::move(context)),
          d_reader_writer(std::move(reader_writer)), d_fixed_mode(fixed_mode),
          d_out_finish_status(out_finish_status) {}

    ~ReproGrpcStagedDirectory() {
        if (!d_fixed_mode) {
            // EXACT UPSTREAM BUGGY IMPLEMENTATION (buildboxcommon_grpccasclient.cpp:1319-1322):
            d_reader_writer->Write(buildbox_repro::StageTreeRequest());
            d_reader_writer->WritesDone();
            // Returns immediately without 2nd Read() or Finish(), then destroys
            // d_reader_writer and d_context (~ClientContext).
        } else {
            // FIXED IMPLEMENTATION:
            buildbox_repro::StageTreeRequest cleanup_req;
            if (d_reader_writer->Write(cleanup_req)) {
                d_reader_writer->WritesDone();
                buildbox_repro::StageTreeResponse cleanup_resp;
                d_reader_writer->Read(&cleanup_resp);
            }
            grpc::Status st = d_reader_writer->Finish();
            if (d_out_finish_status != nullptr) {
                *d_out_finish_status = st;
            }
        }
    }

    const std::string &path() const { return d_path; }

  private:
    std::string d_path;
    const std::shared_ptr<grpc::ClientContext> d_context;
    const std::shared_ptr<grpc::ClientReaderWriterInterface<
        buildbox_repro::StageTreeRequest, buildbox_repro::StageTreeResponse>>
        d_reader_writer;
    bool d_fixed_mode;
    grpc::Status *d_out_finish_status;
};

std::unique_ptr<ReproGrpcStagedDirectory> stageDirectory(
    buildbox_repro::LocalContentAddressableStorage::Stub *stub,
    const std::string &explicit_path,
    const std::vector<std::string> &pre_unstage_cmds, bool fixed_mode,
    grpc::Status *out_finish_status = nullptr) {
    auto context = std::make_shared<grpc::ClientContext>();
    std::shared_ptr<grpc::ClientReaderWriterInterface<
        buildbox_repro::StageTreeRequest, buildbox_repro::StageTreeResponse>>
        reader_writer(stub->StageTree(context.get()));

    buildbox_repro::StageTreeRequest req;
    req.set_instance_name("test-instance");
    req.mutable_root_digest()->set_hash("deadbeef");
    req.mutable_root_digest()->set_size_bytes(42);
    req.set_path(explicit_path);
    req.set_staging_mode(buildbox_repro::StageTreeRequest::COPY_OR_LINK);
    for (const auto &cmd : pre_unstage_cmds) {
        *req.add_pre_unstage_commands() = cmd;
    }

    if (!reader_writer->Write(req)) {
        const grpc::Status err = reader_writer->Finish();
        throw std::runtime_error("StageTree Write failed: " + err.error_message());
    }

    buildbox_repro::StageTreeResponse resp;
    if (!reader_writer->Read(&resp)) {
        reader_writer->WritesDone();
        const grpc::Status err = reader_writer->Finish();
        throw std::runtime_error("StageTree Read failed: " + err.error_message());
    }

    return std::make_unique<ReproGrpcStagedDirectory>(
        context, reader_writer, resp.path(), fixed_mode, out_finish_status);
}

// ============================================================================
// Server Simulation 2: ByteStream::Read & ByteStream::Write
// Also simulates CasInstance::processWriteRequest (buildboxcasd_casinstance.cpp:176-240)
// ============================================================================
struct ByteStreamCallRecord {
    int chunks_received = 0;
    int chunks_sent = 0;
    int64_t total_bytes = 0;
    bool server_read_eof_clean = false;
    bool server_write_failed = false;
    bool server_cancelled = false;
    grpc::Status returned_status;
    std::string accumulated_data;
};

class ByteStreamServiceImpl final : public buildbox_repro::ByteStream::Service {
  public:
    enum class WriteMode {
        BUGGY_OFFSET_NO_RETURN, // Mirrors buildboxcasd_casinstance.cpp:196-200
        FIXED_OFFSET_RETURN
    };

    void setReadPayload(std::string payload, size_t chunk_size, int chunk_delay_ms) {
        std::lock_guard<std::mutex> lk(mu_);
        read_payload_ = std::move(payload);
        read_chunk_size_ = chunk_size;
        read_chunk_delay_ms_ = chunk_delay_ms;
    }

    void setWriteMode(WriteMode mode) { write_mode_.store(mode); }

    std::future<ByteStreamCallRecord> expectNextCall() {
        std::lock_guard<std::mutex> lk(mu_);
        promises_.emplace_back();
        return promises_.back().get_future();
    }

    grpc::Status Read(
        grpc::ServerContext *ctx, const buildbox_repro::ReadRequest * /*request*/,
        grpc::ServerWriter<buildbox_repro::ReadResponse> *writer) override {
        std::string payload;
        size_t chunk_size = 0;
        int delay_ms = 0;
        {
            std::lock_guard<std::mutex> lk(mu_);
            payload = read_payload_;
            chunk_size = read_chunk_size_;
            delay_ms = read_chunk_delay_ms_;
        }

        ByteStreamCallRecord rec;
        size_t offset = 0;
        while (offset < payload.size()) {
            const size_t n = std::min(chunk_size, payload.size() - offset);
            buildbox_repro::ReadResponse resp;
            resp.set_data(payload.data() + offset, n);
            if (!writer->Write(resp)) {
                rec.server_write_failed = true;
                break;
            }
            rec.chunks_sent++;
            rec.total_bytes += static_cast<int64_t>(n);
            offset += n;
            if (delay_ms > 0) {
                std::this_thread::sleep_for(std::chrono::milliseconds(delay_ms));
            }
        }
        rec.server_cancelled = ctx->IsCancelled();
        rec.returned_status = rec.server_cancelled
                                  ? grpc::Status::CANCELLED
                                  : grpc::Status::OK;
        completeRecord(std::move(rec));
        return grpc::Status::OK;
    }

    grpc::Status Write(
        grpc::ServerContext *ctx,
        grpc::ServerReader<buildbox_repro::WriteRequest> *request,
        buildbox_repro::WriteResponse *response) override {
        ByteStreamCallRecord rec;
        buildbox_repro::WriteRequest request_message;
        if (!request->Read(&request_message)) {
            rec.server_cancelled = ctx->IsCancelled();
            rec.returned_status = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                               "No request received.");
            completeRecord(std::move(rec));
            return rec.returned_status;
        }

        const std::string resource_name = request_message.resource_name();
        // Parse ".../blobs/<hash>/<size>"
        const auto last_slash = resource_name.rfind('/');
        const auto prev_slash =
            (last_slash != std::string::npos && last_slash > 0)
                ? resource_name.rfind('/', last_slash - 1)
                : std::string::npos;
        if (last_slash == std::string::npos || prev_slash == std::string::npos) {
            rec.returned_status = grpc::Status(grpc::StatusCode::INVALID_ARGUMENT,
                                               "Invalid resource name.");
            completeRecord(std::move(rec));
            return rec.returned_status;
        }
        const std::string expected_hash =
            resource_name.substr(prev_slash + 1, last_slash - prev_slash - 1);
        const int64_t expected_size =
            std::stoll(resource_name.substr(last_slash + 1));

        bool commit_write = false;
        int64_t bytes_written = 0;
        const WriteMode mode = write_mode_.load();

        do {
            rec.chunks_received++;
            const std::string &new_rn = request_message.resource_name();
            if (!new_rn.empty() && new_rn != resource_name) {
                rec.returned_status = grpc::Status(
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "`resource_name` changed between requests.");
                completeRecord(rec);
                return rec.returned_status;
            } else if (commit_write) {
                rec.returned_status = grpc::Status(
                    grpc::StatusCode::INVALID_ARGUMENT,
                    "Extra request sent after setting `finish_write`.");
                completeRecord(rec);
                return rec.returned_status;
            }

            if (request_message.write_offset() != bytes_written) {
                if (mode == WriteMode::BUGGY_OFFSET_NO_RETURN) {
                    // EXACT UPSTREAM BUG (casd/buildboxcasd_casinstance.cpp:198-199):
                    // Constructs temporary grpc::Status WITHOUT `return`!
                    grpc::Status(grpc::StatusCode::OUT_OF_RANGE,
                                 "`write_offset` is not valid");
                } else {
                    rec.returned_status = grpc::Status(
                        grpc::StatusCode::OUT_OF_RANGE,
                        "`write_offset` is not valid");
                    completeRecord(rec);
                    return rec.returned_status;
                }
            }

            const std::string &data = request_message.data();
            rec.accumulated_data.append(data);
            bytes_written += static_cast<int64_t>(data.size());
            commit_write = request_message.finish_write();
        } while (request->Read(&request_message));

        rec.server_read_eof_clean = true;
        rec.total_bytes = bytes_written;
        rec.server_cancelled = ctx->IsCancelled();

        if (!commit_write) {
            rec.returned_status = grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "`finish_write` was not set in the last request.");
            completeRecord(rec);
            return rec.returned_status;
        }
        if (bytes_written != expected_size) {
            rec.returned_status = grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "Data size does not match digest size.");
            completeRecord(rec);
            return rec.returned_status;
        }
        if (computeHash(rec.accumulated_data) != expected_hash) {
            rec.returned_status = grpc::Status(
                grpc::StatusCode::INVALID_ARGUMENT,
                "Expected blob digest mismatch.");
            completeRecord(rec);
            return rec.returned_status;
        }

        response->set_committed_size(bytes_written);
        rec.returned_status = grpc::Status::OK;
        completeRecord(rec);
        return grpc::Status::OK;
    }

  private:
    void completeRecord(ByteStreamCallRecord rec) {
        std::lock_guard<std::mutex> lk(mu_);
        if (!promises_.empty()) {
            auto p = std::move(promises_.front());
            promises_.erase(promises_.begin());
            p.set_value(std::move(rec));
        }
    }

    std::mutex mu_;
    std::string read_payload_;
    size_t read_chunk_size_ = 65536;
    int read_chunk_delay_ms_ = 0;
    std::atomic<WriteMode> write_mode_{WriteMode::BUGGY_OFFSET_NO_RETURN};
    std::vector<std::promise<ByteStreamCallRecord>> promises_;
};

// ============================================================================
// Suite 1 Runner: ~GrpcStagedDirectory
// ============================================================================
void runStageTreeIterations(
    const std::string &label,
    buildbox_repro::LocalContentAddressableStorage::Stub *stub,
    StageTreeServiceImpl *service, StageServerConfig *cfg,
    InterceptorStats *stats, const fs::path &work_dir, int iterations,
    int delay_ms, bool fixed_mode, bool use_pre_unstage_cmd) {
    cfg->cleanup_delay_ms.store(delay_ms);
    cfg->force_cleanup_error.store(false);
    cfg->reject_nonempty_restage.store(false);
    stats->reset();

    int recv_cleanup_count = 0;
    int cancelled_before_count = 0;
    int cancelled_after_count = 0;
    int ack_write_ok_count = 0;
    int dir_still_exists_on_return = 0;
    int pre_unstage_incomplete_on_return = 0;
    int finish_ok_count = 0;

    for (int i = 0; i < iterations; ++i) {
        std::future<StageCallRecord> srv_fut = service->expectNextCall();
        std::vector<std::string> cmds;
        fs::path marker =
            work_dir / ("marker_" + label + "_" + std::to_string(i) + ".txt");
        std::error_code rm_ec;
        fs::remove(marker, rm_ec);
        if (use_pre_unstage_cmd) {
            cmds.push_back(marker.string());
        }

        grpc::Status finish_status = grpc::Status::OK;
        std::string actual_stage_path;
        {
            auto staged = stageDirectory(stub, "", cmds, fixed_mode,
                                         &finish_status);
            actual_stage_path = staged->path();
            // Scope exit destroys `staged` -> runs ~ReproGrpcStagedDirectory()
        }

        // Check filesystem IMMEDIATELY after ~GrpcStagedDirectory() returns!
        std::error_code fs_ec;
        const bool exists_immediately = fs::exists(actual_stage_path, fs_ec);
        const bool non_empty_immediately =
            exists_immediately && !fs::is_empty(actual_stage_path, fs_ec);
        if (non_empty_immediately) {
            dir_still_exists_on_return++;
        }
        if (use_pre_unstage_cmd && !fs::exists(marker, fs_ec)) {
            pre_unstage_incomplete_on_return++;
        }

        const StageCallRecord rec = srv_fut.get();
        if (rec.received_cleanup_request)
            recv_cleanup_count++;
        if (rec.cancelled_before_cleanup)
            cancelled_before_count++;
        if (rec.cancelled_after_cleanup)
            cancelled_after_count++;
        if (rec.ack_write_ok)
            ack_write_ok_count++;
        if (fixed_mode && finish_status.ok())
            finish_ok_count++;
    }

    std::cout << "\n[" << label << "] (" << iterations << " iterations, delay="
              << delay_ms << "ms, fixed=" << (fixed_mode ? "true" : "false")
              << ")\n"
              << "  server received_cleanup_request : " << recv_cleanup_count
              << "/" << iterations << "\n"
              << "  server IsCancelled (before del) : " << cancelled_before_count
              << "/" << iterations << "\n"
              << "  server IsCancelled (after del)  : " << cancelled_after_count
              << "/" << iterations << "\n"
              << "  server ack Write(StageTreeResp) : " << ack_write_ok_count
              << "/" << iterations << "\n"
              << "  client saw non-empty dir on ret : "
              << dir_still_exists_on_return << "/" << iterations << "\n";
    if (use_pre_unstage_cmd) {
        std::cout << "  client saw pre_unstage missing  : "
                  << pre_unstage_incomplete_on_return << "/" << iterations << "\n";
    }
    if (fixed_mode) {
        std::cout << "  client Finish().ok()            : " << finish_ok_count
                  << "/" << iterations << "\n";
    }
    std::cout << "  interceptor POST_RECV_STATUS    : "
              << stats->post_recv_status.load() << "/" << stats->started.load()
              << " (OK=" << stats->post_recv_status_ok.load()
              << ", CANCELLED=" << stats->post_recv_status_cancelled.load()
              << ")\n";
}

void runStageTreeRestageCollisionTest(
    const std::string &label,
    buildbox_repro::LocalContentAddressableStorage::Stub *stub,
    StageTreeServiceImpl *service, StageServerConfig *cfg,
    const fs::path &work_dir, int iterations, int delay_ms, bool fixed_mode) {
    cfg->cleanup_delay_ms.store(delay_ms);
    cfg->force_cleanup_error.store(false);
    int collisions = 0;

    for (int i = 0; i < iterations; ++i) {
        fs::path slot = work_dir / ("shared_slot_" + std::to_string(i));
        std::error_code ec;
        fs::remove_all(slot, ec);

        cfg->reject_nonempty_restage.store(false);
        auto fut1 = service->expectNextCall();
        {
            grpc::Status st1;
            auto staged1 = stageDirectory(stub, slot.string(), {}, fixed_mode, &st1);
        }
        // Immediately attempt second stage into the exact same explicit path
        // (matching local_cas.proto requirement: path must be empty or non-existent)
        cfg->reject_nonempty_restage.store(true);
        auto fut2 = service->expectNextCall();
        try {
            grpc::Status st2;
            auto staged2 = stageDirectory(stub, slot.string(), {}, true, &st2);
            staged2.reset();
            fut2.get();
        } catch (const std::exception &) {
            collisions++;
            fut2.get();
        }
        fut1.get();
        cfg->reject_nonempty_restage.store(false);
    }

    std::cout << "\n[" << label << "] (" << iterations << " iterations, delay="
              << delay_ms << "ms, fixed=" << (fixed_mode ? "true" : "false")
              << ")\n"
              << "  immediate restage collisions    : " << collisions << "/"
              << iterations << "\n";
}

void runStageTreeErrorLossTest(
    buildbox_repro::LocalContentAddressableStorage::Stub *stub,
    StageTreeServiceImpl *service, StageServerConfig *cfg,
    InterceptorStats *stats) {
    cfg->cleanup_delay_ms.store(0);
    cfg->force_cleanup_error.store(true);
    cfg->reject_nonempty_restage.store(false);

    for (bool fixed_mode : {false, true}) {
        stats->reset();
        auto srv_fut = service->expectNextCall();
        grpc::Status observed_finish_status = grpc::Status::OK;
        {
            auto staged =
                stageDirectory(stub, "", {}, fixed_mode, &observed_finish_status);
        }
        const auto rec = srv_fut.get();
        std::cout << "\n[Suite 1D - Server Cleanup Error Propagation, fixed="
                  << (fixed_mode ? "true" : "false") << "]\n"
                  << "  server returned status          : "
                  << rec.returned_status.error_code() << " ("
                  << rec.returned_status.error_message() << ")\n"
                  << "  server ack_write_ok             : "
                  << (rec.ack_write_ok ? "true" : "false") << "\n"
                  << "  client Finish() observed        : "
                  << (fixed_mode ? std::to_string(observed_finish_status.error_code()) +
                                       " (" + observed_finish_status.error_message() +
                                       ")"
                                 : "<NOT CALLED - SILENTLY DROPPED>")
                  << "\n"
                  << "  interceptor POST_RECV_STATUS    : "
                  << stats->post_recv_status.load() << "/"
                  << stats->started.load() << "\n";
    }
    cfg->force_cleanup_error.store(false);
}

// ============================================================================
// Suite 2 Runner: ByteStream Mid-Stream Exception Leaks vs RAII Fix vs Control
// ============================================================================

// Mimics GrpcRetrier::issueRequest (common/buildboxcommon_grpcretrier.cpp:78-100):
// creates ClientContext, passes to invocation lambda, does NOT catch exceptions!
template <typename Fn>
grpc::Status simulateGrpcRetrierIssueRequest(Fn &&invocation) {
    auto context = std::make_unique<grpc::ClientContext>();
    return invocation(*context);
}

struct StreamCancelGuard {
    grpc::ClientContext *ctx;
    std::function<void()> finalize_fn;
    bool active = true;

    ~StreamCancelGuard() {
        if (active) {
            if (ctx != nullptr) {
                ctx->TryCancel();
            }
            if (finalize_fn) {
                finalize_fn();
            }
        }
    }
    void dismiss() { active = false; }
};

void runUploadFdRegionLeakTests(buildbox_repro::ByteStream::Stub *stub,
                                ByteStreamServiceImpl *service,
                                InterceptorStats *stats,
                                const fs::path &work_dir) {
    service->setWriteMode(ByteStreamServiceImpl::WriteMode::FIXED_OFFSET_RETURN);

    // Prepare a real 64 KiB file on disk, while claiming digest is 128 KiB.
    // Chunk 1 (64 KiB) succeeds and writes over HTTP/2; Chunk 2 reads 0 bytes
    // (unexpected EOF) and throws std::runtime_error, mirroring lines 545-550.
    const std::string chunk1(65536, 'A');
    const std::string full_expected = chunk1 + std::string(65536, 'B');
    const auto digest = makeDigest(full_expected);
    const std::string rn = "blobs/" + digest.hash() + "/" +
                           std::to_string(digest.size_bytes());

    const fs::path truncated_file = work_dir / "truncated_upload.bin";
    {
        std::ofstream ofs(truncated_file, std::ios::binary);
        ofs.write(chunk1.data(), static_cast<std::streamsize>(chunk1.size()));
    }

    for (int variant = 0; variant < 3; ++variant) {
        // variant 0 = buggy (no WritesDone/Finish on throw)
        // variant 1 = fixed RAII (TryCancel + Finish on throw)
        // variant 2 = fixed RAII (WritesDone + Finish without TryCancel)
        stats->reset();
        auto srv_fut = service->expectNextCall();
        bool caught = false;
        std::string caught_msg;
        grpc::Status guard_finish_status;

        try {
            simulateGrpcRetrierIssueRequest([&](grpc::ClientContext &context) {
                buildbox_repro::WriteResponse response;
                auto writer = stub->Write(&context, &response);
                StreamCancelGuard guard{
                    &context,
                    [&]() {
                        if (variant == 2) {
                            writer->WritesDone();
                        }
                        guard_finish_status = writer->Finish();
                    },
                    /*active=*/(variant != 0)};

                if (variant == 2) {
                    // Test WritesDone + Finish WITHOUT TryCancel:
                    guard.ctx = nullptr;
                }

                std::ifstream ifs(truncated_file, std::ios::binary);
                std::vector<char> buf(65536);
                int64_t chunk_rel_offset = 0;
                bool finish_write = false;
                while (!finish_write) {
                    ifs.read(buf.data(), static_cast<std::streamsize>(buf.size()));
                    const std::streamsize bytes_read = ifs.gcount();
                    buildbox_repro::WriteRequest req;
                    req.set_resource_name(rn);
                    req.set_write_offset(chunk_rel_offset);
                    req.set_data(buf.data(), static_cast<size_t>(bytes_read));

                    if (chunk_rel_offset + bytes_read < digest.size_bytes()) {
                        if (bytes_read == 0) {
                            throw std::runtime_error(
                                "Upload failed: unexpected end of file");
                        }
                    } else {
                        finish_write = true;
                        req.set_finish_write(true);
                    }
                    if (!writer->Write(req)) {
                        break;
                    }
                    chunk_rel_offset += bytes_read;
                }

                guard.dismiss();
                writer->WritesDone();
                return writer->Finish();
            });
        } catch (const std::exception &e) {
            caught = true;
            caught_msg = e.what();
        }

        const auto rec = srv_fut.get();
        const char *mode_name =
            (variant == 0)
                ? "BUGGY (no WritesDone/Finish)"
                : (variant == 1 ? "FIXED RAII (TryCancel + Finish)"
                                : "FIXED RAII (WritesDone + Finish)");
        std::cout << "\n[Suite 2A - uploadFdRegion mid-stream throw: "
                  << mode_name << "]\n"
                  << "  client caught exception         : "
                  << (caught ? caught_msg : "NONE") << "\n"
                  << "  guard Finish() status           : "
                  << (variant == 0
                          ? "<SKIPPED>"
                          : std::to_string(guard_finish_status.error_code()) +
                                " (" + guard_finish_status.error_message() + ")")
                  << "\n"
                  << "  server chunks received          : "
                  << rec.chunks_received << " (" << rec.total_bytes
                  << " bytes)\n"
                  << "  server Read() exited cleanly    : "
                  << (rec.server_read_eof_clean ? "true" : "false") << "\n"
                  << "  server IsCancelled()            : "
                  << (rec.server_cancelled ? "true" : "false") << "\n"
                  << "  server handler returned         : "
                  << rec.returned_status.error_code() << " ("
                  << rec.returned_status.error_message() << ")\n"
                  << "  interceptor POST_RECV_STATUS    : "
                  << stats->post_recv_status.load() << "/"
                  << stats->started.load()
                  << " (CANCELLED=" << stats->post_recv_status_cancelled.load()
                  << ", OTHER=" << stats->post_recv_status_other.load() << ")\n";
    }
}

void runDownloadLeakTests(buildbox_repro::ByteStream::Stub *stub,
                          ByteStreamServiceImpl *service,
                          InterceptorStats *stats) {
    // 5 chunks of 64 KiB with 5ms spacing so the server is actively mid-stream
    // when client throws on chunk 1.
    const std::string payload(5 * 65536, 'D');
    service->setReadPayload(payload, 65536, 5);

    for (bool fixed_mode : {false, true}) {
        stats->reset();
        auto srv_fut = service->expectNextCall();
        bool caught = false;
        std::string caught_msg;
        grpc::Status guard_finish_status;

        try {
            simulateGrpcRetrierIssueRequest([&](grpc::ClientContext &context) {
                buildbox_repro::ReadRequest req;
                req.set_resource_name("blobs/dummy/327680");
                auto reader = stub->Read(&context, req);

                StreamCancelGuard guard{
                    &context,
                    [&]() { guard_finish_status = reader->Finish(); },
                    /*active=*/fixed_mode};

                buildbox_repro::ReadResponse resp;
                while (reader->Read(&resp)) {
                    // Simulate write(fd, ...) < 0 (e.g., ENOSPC / EBADF) on 1st chunk
                    throw std::system_error(
                        ENOSPC, std::generic_category(),
                        "Error in write to descriptor: No space left on device");
                }

                guard.dismiss();
                return reader->Finish();
            });
        } catch (const std::exception &e) {
            caught = true;
            caught_msg = e.what();
        }

        const auto rec = srv_fut.get();
        std::cout << "\n[Suite 2B - download mid-stream write() throw, fixed="
                  << (fixed_mode ? "true" : "false") << "]\n"
                  << "  client caught exception         : "
                  << (caught ? caught_msg : "NONE") << "\n"
                  << "  guard Finish() status           : "
                  << (fixed_mode
                          ? std::to_string(guard_finish_status.error_code()) +
                                " (" + guard_finish_status.error_message() + ")"
                          : "<SKIPPED>")
                  << "\n"
                  << "  server chunks sent              : " << rec.chunks_sent
                  << "/5 (" << rec.total_bytes << " bytes)\n"
                  << "  server writer->Write() failed   : "
                  << (rec.server_write_failed ? "true" : "false") << "\n"
                  << "  server IsCancelled()            : "
                  << (rec.server_cancelled ? "true" : "false") << "\n"
                  << "  interceptor POST_RECV_STATUS    : "
                  << stats->post_recv_status.load() << "/"
                  << stats->started.load()
                  << " (CANCELLED=" << stats->post_recv_status_cancelled.load()
                  << ")\n";
    }
}

void runUploadStringControlTest(buildbox_repro::ByteStream::Stub *stub,
                                ByteStreamServiceImpl *service,
                                InterceptorStats *stats) {
    service->setWriteMode(ByteStreamServiceImpl::WriteMode::FIXED_OFFSET_RETURN);
    stats->reset();
    const std::string data = std::string(65536, 'X') + std::string(65536, 'Y');
    const auto digest = makeDigest(data);
    const std::string rn = "blobs/" + digest.hash() + "/" +
                           std::to_string(digest.size_bytes());

    auto srv_fut = service->expectNextCall();
    grpc::Status final_status;
    int64_t committed_size = -1;

    simulateGrpcRetrierIssueRequest([&](grpc::ClientContext &context) {
        buildbox_repro::WriteResponse response;
        auto writer = stub->Write(&context, &response);

        size_t offset = 0;
        bool last_chunk = false;
        while (!last_chunk) {
            buildbox_repro::WriteRequest req;
            req.set_resource_name(rn);
            req.set_write_offset(static_cast<int64_t>(offset));
            const size_t len = std::min<size_t>(65536, data.size() - offset);
            req.set_data(data.data() + offset, len);
            offset += len;
            last_chunk = (offset == data.size());
            if (last_chunk) {
                req.set_finish_write(true);
            }
            if (!writer->Write(req)) {
                break;
            }
        }
        writer->WritesDone();
        final_status = writer->Finish();
        committed_size = response.committed_size();
        return final_status;
    });

    const auto rec = srv_fut.get();
    std::cout << "\n[Suite 2C - Clean Control: upload(string)]\n"
              << "  client Finish().ok()            : "
              << (final_status.ok() ? "true" : "false")
              << " (committed=" << committed_size << ")\n"
              << "  server chunks received          : " << rec.chunks_received
              << "\n"
              << "  server IsCancelled()            : "
              << (rec.server_cancelled ? "true" : "false") << "\n"
              << "  interceptor POST_RECV_STATUS    : "
              << stats->post_recv_status.load() << "/" << stats->started.load()
              << " (OK=" << stats->post_recv_status_ok.load() << ")\n";
}

// ============================================================================
// Suite 3 Runner: Bonus Check — CasInstance::processWriteRequest missing return
// ============================================================================
void runProcessWriteRequestOffsetTests(buildbox_repro::ByteStream::Stub *stub,
                                       ByteStreamServiceImpl *service) {
    const std::string expected_blob = "helloworld";
    const auto digest = makeDigest(expected_blob);
    const std::string rn = "blobs/" + digest.hash() + "/" +
                           std::to_string(digest.size_bytes());

    auto sendBogusOffsetUpload = [&](int64_t offset1, const std::string &d1,
                                     int64_t offset2, const std::string &d2,
                                     bool finish2,
                                     int64_t *out_committed) -> grpc::Status {
        grpc::ClientContext ctx;
        buildbox_repro::WriteResponse resp;
        auto writer = stub->Write(&ctx, &resp);

        buildbox_repro::WriteRequest r1;
        r1.set_resource_name(rn);
        r1.set_write_offset(offset1);
        r1.set_data(d1);
        r1.set_finish_write(false);
        writer->Write(r1);

        buildbox_repro::WriteRequest r2;
        r2.set_resource_name(rn);
        r2.set_write_offset(offset2);
        r2.set_data(d2);
        r2.set_finish_write(finish2);
        writer->Write(r2);

        writer->WritesDone();
        grpc::Status st = writer->Finish();
        *out_committed = resp.committed_size();
        return st;
    };

    // 3A: Buggy server + invalid initial write_offset=999, matching total bytes & hash
    service->setWriteMode(
        ByteStreamServiceImpl::WriteMode::BUGGY_OFFSET_NO_RETURN);
    {
        auto srv_fut = service->expectNextCall();
        int64_t committed = -1;
        const grpc::Status st =
            sendBogusOffsetUpload(999, "hello", 5, "world", true, &committed);
        const auto rec = srv_fut.get();
        std::cout << "\n[Suite 3A - CasInstance::processWriteRequest missing `return` (invalid offset=999, matching digest)]\n"
                  << "  expected code (if fixed)        : OUT_OF_RANGE ("
                  << grpc::StatusCode::OUT_OF_RANGE << ")\n"
                  << "  actual client Finish() status   : " << st.error_code()
                  << " (" << st.error_message()
                  << "), committed_size=" << committed << "\n"
                  << "  server accumulated bytes        : \""
                  << rec.accumulated_data << "\"\n";
    }

    // 3B: Buggy server + duplicate offset=0 (sends "hello" twice for 5-byte digest)
    {
        const auto digest5 = makeDigest("hello");
        const std::string rn5 = "blobs/" + digest5.hash() + "/" +
                                std::to_string(digest5.size_bytes());
        auto srv_fut = service->expectNextCall();
        grpc::ClientContext ctx;
        buildbox_repro::WriteResponse resp;
        auto writer = stub->Write(&ctx, &resp);
        for (int i = 0; i < 2; ++i) {
            buildbox_repro::WriteRequest r;
            r.set_resource_name(rn5);
            r.set_write_offset(0); // 2nd chunk repeats offset=0 instead of 5!
            r.set_data("hello");
            r.set_finish_write(i == 1);
            writer->Write(r);
        }
        writer->WritesDone();
        const grpc::Status st = writer->Finish();
        const auto rec = srv_fut.get();
        std::cout << "\n[Suite 3B - CasInstance::processWriteRequest missing `return` (duplicate offset=0)]\n"
                  << "  expected code (if fixed)        : OUT_OF_RANGE ("
                  << grpc::StatusCode::OUT_OF_RANGE << ")\n"
                  << "  actual client Finish() status   : " << st.error_code()
                  << " (" << st.error_message() << ")\n"
                  << "  server corrupted buffer         : \""
                  << rec.accumulated_data << "\"\n";
    }

    // 3C: Fixed server + invalid offset=999
    service->setWriteMode(ByteStreamServiceImpl::WriteMode::FIXED_OFFSET_RETURN);
    {
        auto srv_fut = service->expectNextCall();
        int64_t committed = -1;
        const grpc::Status st =
            sendBogusOffsetUpload(999, "hello", 5, "world", true, &committed);
        srv_fut.get();
        std::cout << "\n[Suite 3C - Fixed CasInstance::processWriteRequest (`return grpc::Status(OUT_OF_RANGE, ...)`)]\n"
                  << "  actual client Finish() status   : " << st.error_code()
                  << " (" << st.error_message() << ")\n";
    }
}

} // namespace

int main(int argc, char **argv) {
    std::string mode = (argc > 1) ? argv[1] : "all";

    const fs::path work_dir =
        fs::temp_directory_path() / "buildbox_grpc_repro_work";
    fs::remove_all(work_dir);
    fs::create_directories(work_dir);

    StageServerConfig stage_cfg;
    StageTreeServiceImpl stage_service(work_dir, &stage_cfg);
    ByteStreamServiceImpl bs_service;

    int selected_port = 0;
    grpc::ServerBuilder builder;
    builder.AddListeningPort("127.0.0.1:0", grpc::InsecureServerCredentials(),
                             &selected_port);
    builder.RegisterService(&stage_service);
    builder.RegisterService(&bs_service);
    std::unique_ptr<grpc::Server> server(builder.BuildAndStart());
    if (!server || selected_port == 0) {
        std::cerr << "Failed to start gRPC server\n";
        return 1;
    }

    const std::string target = "127.0.0.1:" + std::to_string(selected_port);
    InterceptorStats stats;
    auto channel = createTrackedChannel(target, &stats);
    auto localcas_stub =
        buildbox_repro::LocalContentAddressableStorage::NewStub(channel);
    auto bs_stub = buildbox_repro::ByteStream::NewStub(channel);

    std::cout << "gRPC C++ Version: " << grpc::Version()
              << " | Loopback Server: " << target << "\n";

    if (mode == "all" || mode == "stagetree") {
        runStageTreeIterations("Suite 1A - Buggy ~GrpcStagedDirectory (0ms)",
                               localcas_stub.get(), &stage_service, &stage_cfg,
                               &stats, work_dir, 20, /*delay_ms=*/0,
                               /*fixed_mode=*/false,
                               /*use_pre_unstage_cmd=*/false);

        runStageTreeIterations(
            "Suite 1B - Buggy ~GrpcStagedDirectory (30ms + pre_unstage)",
            localcas_stub.get(), &stage_service, &stage_cfg, &stats, work_dir,
            20, /*delay_ms=*/30, /*fixed_mode=*/false,
            /*use_pre_unstage_cmd=*/true);

        runStageTreeIterations(
            "Suite 1C - Fixed ~GrpcStagedDirectory (30ms + pre_unstage)",
            localcas_stub.get(), &stage_service, &stage_cfg, &stats, work_dir,
            20, /*delay_ms=*/30, /*fixed_mode=*/true,
            /*use_pre_unstage_cmd=*/true);

        runStageTreeRestageCollisionTest(
            "Suite 1B-Restage - Buggy immediate reuse of same path (30ms)",
            localcas_stub.get(), &stage_service, &stage_cfg, work_dir, 20,
            /*delay_ms=*/30, /*fixed_mode=*/false);

        runStageTreeRestageCollisionTest(
            "Suite 1C-Restage - Fixed immediate reuse of same path (30ms)",
            localcas_stub.get(), &stage_service, &stage_cfg, work_dir, 20,
            /*delay_ms=*/30, /*fixed_mode=*/true);

        runStageTreeErrorLossTest(localcas_stub.get(), &stage_service, &stage_cfg,
                                  &stats);
    }

    if (mode == "all" || mode == "bytestream") {
        runUploadFdRegionLeakTests(bs_stub.get(), &bs_service, &stats, work_dir);
        runDownloadLeakTests(bs_stub.get(), &bs_service, &stats);
        runUploadStringControlTest(bs_stub.get(), &bs_service, &stats);
    }

    if (mode == "all" || mode == "offset") {
        runProcessWriteRequestOffsetTests(bs_stub.get(), &bs_service);
    }

    server->Shutdown();
    server->Wait();
    std::error_code ec;
    fs::remove_all(work_dir, ec);
    return 0;
}
