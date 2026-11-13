#ifndef FRONTEND_FEATURE_PIPELINE_H_
#define FRONTEND_FEATURE_PIPELINE_H_

// Lightweight FeaturePipeline stub matching the interface used by
// wenet/runtime/core/grpc/grpc_server.cc and recording waveform telemetry.

#include <algorithm>
#include <atomic>
#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <vector>

#include "utils/log.h"

namespace wenet {

struct FeaturePipelineConfig {
  int num_bins = 80;
  int sample_rate = 16000;
  int frame_length = 400;
  int frame_shift = 160;

  FeaturePipelineConfig(int bins = 80, int sr = 16000)
      : num_bins(bins),
        sample_rate(sr),
        frame_length(sr / 1000 * 25),
        frame_shift(sr / 1000 * 10) {}
};

class FeaturePipeline {
 public:
  explicit FeaturePipeline(const FeaturePipelineConfig& config)
      : config_(config) {}

  void AcceptWaveform(const int16_t* pcm, const int size) {
    std::lock_guard<std::mutex> lock(mu_);
    accept_calls_++;
    if (size > 0 && pcm != nullptr) {
      pending_samples_.insert(pending_samples_.end(), pcm, pcm + size);
      total_samples_ += size;
    }
    cv_.notify_one();
  }

  void AcceptWaveform(const float* pcm, const int size) {
    std::lock_guard<std::mutex> lock(mu_);
    accept_calls_++;
    if (size > 0 && pcm != nullptr) {
      for (int i = 0; i < size; ++i) {
        pending_samples_.push_back(static_cast<int16_t>(pcm[i]));
      }
      total_samples_ += size;
    }
    cv_.notify_one();
  }

  void set_input_finished() {
    CHECK(!input_finished_);
    {
      std::lock_guard<std::mutex> lock(mu_);
      input_finished_ = true;
    }
    cv_.notify_one();
  }

  bool input_finished() const {
    std::lock_guard<std::mutex> lock(mu_);
    return input_finished_;
  }

  // Blocks until at least chunk_samples are buffered or input_finished_ is set.
  // Returns true if a chunk was consumed, false if input_finished_ and no
  // remaining samples exist.
  bool ConsumeChunk(int chunk_samples, std::vector<int16_t>* out) {
    out->clear();
    std::unique_lock<std::mutex> lock(mu_);
    cv_.wait(lock, [&] {
      return pending_samples_.size() >= static_cast<size_t>(chunk_samples) ||
             input_finished_;
    });
    if (pending_samples_.size() >= static_cast<size_t>(chunk_samples)) {
      out->assign(pending_samples_.begin(),
                  pending_samples_.begin() + chunk_samples);
      pending_samples_.erase(pending_samples_.begin(),
                             pending_samples_.begin() + chunk_samples);
      return true;
    }
    if (!pending_samples_.empty()) {
      out->swap(pending_samples_);
      // Consumed trailing partial chunk; next ConsumeChunk will see empty +
      // input_finished_ and return false.
      return true;
    }
    return false;
  }

  int accept_calls() const {
    std::lock_guard<std::mutex> lock(mu_);
    return accept_calls_;
  }

  int total_samples() const {
    std::lock_guard<std::mutex> lock(mu_);
    return total_samples_;
  }

  const FeaturePipelineConfig& config() const { return config_; }

 private:
  FeaturePipelineConfig config_;
  mutable std::mutex mu_;
  std::condition_variable cv_;
  std::vector<int16_t> pending_samples_;
  bool input_finished_ = false;
  int accept_calls_ = 0;
  int total_samples_ = 0;
};

}  // namespace wenet

#endif  // FRONTEND_FEATURE_PIPELINE_H_
