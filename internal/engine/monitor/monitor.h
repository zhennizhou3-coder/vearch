/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef ENGINE_MONITOR_MONITOR_H_
#define ENGINE_MONITOR_MONITOR_H_

// Engine-side metric declarations.
//
// All metrics are declared here with the DECLARE_PROMETHEUS_* macro and
// defined in prometheus_client.cc; adding a metric is a two-line change
// (DECLARE here, DEFINE there) confined to C++.

#ifdef ENGINE_METRICS_ENABLED

#include <prometheus/family.h>
#include <prometheus/histogram.h>

#include <map>
#include <string>
#include <vector>

#define DECLARE_PROMETHEUS_HISTOGRAM(NAME) \
  prometheus::Family<prometheus::Histogram>& NAME##_family();

namespace vearch {
namespace monitor {

using Labels = std::map<std::string, std::string>;
using Buckets = prometheus::Histogram::BucketBoundaries;

// Default bucket boundaries (milliseconds) for latency histograms.
inline const Buckets& DefaultLatencyBucketsMs() {
  static const Buckets b = {0.5, 1, 2, 5, 10, 20, 50, 100, 200, 500, 1000};
  return b;
}

DECLARE_PROMETHEUS_HISTOGRAM(engine_search_latency)
DECLARE_PROMETHEUS_HISTOGRAM(npu_search_latency)

}  // namespace monitor
}  // namespace vearch

#else  // no-op shims when metrics are disabled

#define DECLARE_PROMETHEUS_HISTOGRAM(NAME)

#endif  // ENGINE_METRICS_ENABLED

#endif  // ENGINE_MONITOR_MONITOR_H_
