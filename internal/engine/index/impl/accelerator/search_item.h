/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include <condition_variable>
#include <mutex>

namespace vearch {
namespace accelerator {

/**
 * Search request item handed off from the search-thread side to the
 * accelerator (GPU / NPU) worker thread via a blocking concurrent queue.
 *
 * The worker thread fills `dis_` / `label_` and then calls `Notify()` to
 * release the original caller blocked inside `WaitForDone()`.
 *
 * Member fields are intentionally public to match the historical
 * `GPUSearchItem` / `NPUSearchItem` layout (worker threads read them
 * directly, e.g. `items[i]->nprobe_`).
 */
class AcceleratorSearchItem {
 public:
  AcceleratorSearchItem(int n, const float *x, int k, float *dis, long *label,
                        int nprobe)
      : n_(n),
        x_(x),
        k_(k),
        dis_(dis),
        label_(label),
        nprobe_(nprobe),
        done_(false) {}

  virtual ~AcceleratorSearchItem() = default;

  void Notify() {
    std::lock_guard<std::mutex> lock(mtx_);
    done_ = true;
    cv_.notify_one();
  }

  int WaitForDone() {
    std::unique_lock<std::mutex> lck(mtx_);
    while (!done_) {
      cv_.wait(lck);
    }
    return 0;
  }

  // Search parameters
  int n_;
  const float *x_;
  int k_;
  float *dis_;
  long *label_;
  int nprobe_;

 private:
  std::condition_variable cv_;
  std::mutex mtx_;
  bool done_;
};

}  // namespace accelerator
}  // namespace vearch
