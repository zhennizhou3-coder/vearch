/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#pragma once

#include "index/index_model.h"

namespace vearch {
namespace accelerator {

/**
 * Common retrieval-parameters base for accelerator (GPU / NPU) backed
 * IVF-style indexes. Carries the single shared knob `nprobe_` and forwards
 * the distance compute type to `RetrievalParameters`.
 *
 * The default value of `nprobe_` is platform-specific (e.g. GPU defaults to
 * 80, NPU defaults to 64); each platform therefore instantiates the template
 * with the appropriate compile-time default via the `DefaultNprobe`
 * non-type template parameter.
 *
 * Derived classes can still keep the historical names
 * (`GPURetrievalParametersBase` / `NPURetrievalParametersBase`) by aliasing
 * this template with the right default.
 */
template <int DefaultNprobe>
class AcceleratorRetrievalParams : public RetrievalParameters {
 public:
  AcceleratorRetrievalParams() : RetrievalParameters() {
    nprobe_ = DefaultNprobe;
  }

  AcceleratorRetrievalParams(int nprobe, DistanceComputeType type)
      : RetrievalParameters(type), nprobe_(nprobe) {}

  virtual ~AcceleratorRetrievalParams() = default;

  int Nprobe() const { return nprobe_; }
  void SetNprobe(int nprobe) { nprobe_ = nprobe; }

 protected:
  int nprobe_;
};

}  // namespace accelerator
}  // namespace vearch
