/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef ENGINE_MONITOR_PROMETHEUS_CLIENT_H_
#define ENGINE_MONITOR_PROMETHEUS_CLIENT_H_

#include <string>

#include "monitor/monitor.h"

#ifdef ENGINE_METRICS_ENABLED
#include <prometheus/family.h>
#include <prometheus/histogram.h>
#include <prometheus/registry.h>
#endif

#ifdef ENGINE_METRICS_ENABLED

#define DEFINE_PROMETHEUS_HISTOGRAM(NAME, METRIC_NAME, HELP)          \
  prometheus::Family<prometheus::Histogram>& NAME##_family() {        \
    static prometheus::Family<prometheus::Histogram>& f =             \
        prometheus::BuildHistogram()                                  \
            .Name(METRIC_NAME)                                        \
            .Help(HELP)                                               \
            .Register(::vearch::monitor::GlobalRegistry());           \
    return f;                                                         \
  }

#else

#define DEFINE_PROMETHEUS_HISTOGRAM(NAME, METRIC_NAME, HELP)

#endif  // ENGINE_METRICS_ENABLED

namespace vearch {
namespace monitor {

// Prometheus client internals for the engine monitor.
//
// This header owns the process-global registry that every metric family
// registers into (via the DEFINE_PROMETHEUS_* macro above). Only the
// metric-definition TU and tests need it; instrumentation sites should rely on
// monitor.h / scope_metric.h instead.
#ifdef ENGINE_METRICS_ENABLED
// Process-global registry singleton.
prometheus::Registry& GlobalRegistry();
#endif

// Serialize all engine-side metrics into a Prometheus text exposition string.
std::string GetEngineMetricsText();

}  // namespace monitor
}  // namespace vearch

#endif  // ENGINE_MONITOR_PROMETHEUS_CLIENT_H_
