/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include "monitor/prometheus_client.h"

#ifdef ENGINE_METRICS_ENABLED
#include <prometheus/text_serializer.h>
#endif

namespace vearch {
namespace monitor {

#ifdef ENGINE_METRICS_ENABLED

prometheus::Registry& GlobalRegistry() {
  // Meyers singleton: constructed on first use, never destroyed. Registry is
  // internally synchronized for concurrent Family creation and observation.
  static prometheus::Registry* registry = new prometheus::Registry();
  return *registry;
}

std::string GetEngineMetricsText() {
  prometheus::TextSerializer serializer;
  return serializer.Serialize(GlobalRegistry().Collect());
}

#else

std::string GetEngineMetricsText() { return std::string(); }

#endif  // ENGINE_METRICS_ENABLED

// Metric family definitions (one per DECLARE in monitor.h).
DEFINE_PROMETHEUS_HISTOGRAM(engine_search_latency,
                            "vearch_engine_search_latency_ms",
                            "Engine top-level search latency in milliseconds")
DEFINE_PROMETHEUS_HISTOGRAM(npu_search_latency,
                            "vearch_engine_npu_search_latency_ms",
                            "NPU search enqueue-to-done latency in milliseconds")

}  // namespace monitor
}  // namespace vearch
