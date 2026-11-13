#ifndef DECODER_ASR_DECODER_H_
#define DECODER_ASR_DECODER_H_

// Lightweight AsrDecoder stub matching the interface used by
// wenet/runtime/core/grpc/grpc_server.cc.
// Produces deterministic multi-hypothesis DecodeResult entries when audio
// samples are received, enabling exact inspection of SerializeResult() & nbest_.

#include <atomic>
#include <chrono>
#include <memory>
#include <string>
#include <thread>
#include <utility>
#include <vector>

#include "frontend/feature_pipeline.h"
#include "utils/log.h"
#include "utils/utils.h"

namespace wenet {

struct DecodeOptions {
  int chunk_size = 16;
  int num_left_chunks = -1;
};

struct WordPiece {
  std::string word;
  int start = -1;
  int end = -1;

  WordPiece(std::string w, int s, int e)
      : word(std::move(w)), start(s), end(e) {}
};

struct DecodeResult {
  float score = 0.0f;
  std::string sentence;
  std::vector<WordPiece> word_pieces;
};

enum DecodeState {
  kEndBatch = 0x00,
  kEndpoint = 0x01,
  kEndFeats = 0x02,
  kWaitFeats = 0x03,
};

struct DecodeResource {
  int chunk_samples = 800;
  int decode_delay_ms = 0;
  std::vector<DecodeResult> mock_hypotheses;
};

class AsrDecoder {
 public:
  AsrDecoder(std::shared_ptr<FeaturePipeline> feature_pipeline,
             std::shared_ptr<DecodeResource> resource,
             const DecodeOptions& opts)
      : feature_pipeline_(std::move(feature_pipeline)),
        resource_(std::move(resource)),
        opts_(opts) {}

  DecodeState Decode(bool block = true) {
    (void)block;
    std::vector<int16_t> chunk;
    const int chunk_samples =
        (resource_ && resource_->chunk_samples > 0) ? resource_->chunk_samples
                                                    : 800;
    bool got_chunk = feature_pipeline_->ConsumeChunk(chunk_samples, &chunk);
    if (resource_ && resource_->decode_delay_ms > 0) {
      std::this_thread::sleep_for(
          std::chrono::milliseconds(resource_->decode_delay_ms));
    }

    if (got_chunk) {
      chunks_decoded_++;
      UpdateStubResults(/*final_pass=*/false);
      return DecodeState::kEndBatch;
    }

    UpdateStubResults(/*final_pass=*/true);
    return DecodeState::kEndFeats;
  }

  void Rescoring() {
    UpdateStubResults(/*final_pass=*/true);
    LOG(INFO) << "[DECODER_TELEMETRY] Rescoring accept_calls="
              << feature_pipeline_->accept_calls()
              << " total_samples=" << feature_pipeline_->total_samples()
              << " chunks_decoded=" << chunks_decoded_
              << " result_count=" << result_.size();
  }

  void ResetContinuousDecoding() { result_.clear(); }

  bool DecodedSomething() const {
    return !result_.empty() && !result_[0].sentence.empty();
  }

  const std::vector<DecodeResult>& result() const { return result_; }

 private:
  void UpdateStubResults(bool final_pass) {
    (void)final_pass;
    result_.clear();
    if (feature_pipeline_->total_samples() <= 0) {
      return;
    }
    if (resource_ && !resource_->mock_hypotheses.empty()) {
      result_ = resource_->mock_hypotheses;
      return;
    }
    DecodeResult r1;
    r1.score = -1.0f;
    r1.sentence = "hello world";
    r1.word_pieces.emplace_back("hello", 0, 400);
    r1.word_pieces.emplace_back("world", 400, 800);

    DecodeResult r2;
    r2.score = -2.0f;
    r2.sentence = "hello ward";
    r2.word_pieces.emplace_back("hello", 0, 400);
    r2.word_pieces.emplace_back("ward", 400, 800);

    DecodeResult r3;
    r3.score = -3.0f;
    r3.sentence = "halo world";
    r3.word_pieces.emplace_back("halo", 0, 400);
    r3.word_pieces.emplace_back("world", 400, 800);

    result_.push_back(std::move(r1));
    result_.push_back(std::move(r2));
    result_.push_back(std::move(r3));
  }

  std::shared_ptr<FeaturePipeline> feature_pipeline_;
  std::shared_ptr<DecodeResource> resource_;
  DecodeOptions opts_;
  int chunks_decoded_ = 0;
  std::vector<DecodeResult> result_;

 public:
  WENET_DISALLOW_COPY_AND_ASSIGN(AsrDecoder);
};

}  // namespace wenet

#endif  // DECODER_ASR_DECODER_H_
