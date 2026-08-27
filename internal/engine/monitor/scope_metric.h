/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#ifndef ENGINE_MONITOR_SCOPE_METRIC_H_
#define ENGINE_MONITOR_SCOPE_METRIC_H_

#include "monitor/monitor.h"
#include "util/utils.h"

namespace vearch {
namespace monitor {

#ifdef ENGINE_METRICS_ENABLED

class ScopeMetric {
 public:
  explicit ScopeMetric(prometheus::Histogram& histogram)
      : histogram_(histogram), start_ms_(utils::getmillisecs()) {}

  ~ScopeMetric() { histogram_.Observe(utils::getmillisecs() - start_ms_); }

  ScopeMetric(const ScopeMetric&) = delete;
  ScopeMetric& operator=(const ScopeMetric&) = delete;

 private:
  prometheus::Histogram& histogram_;
  double start_ms_;
};

// Time the enclosing scope; HIST resolves the histogram once, then the
// hot path only Observes.
#define SCOPE_ENGINE_METRIC(NAME, LABELS, HIST)                         \
  if ((HIST) == nullptr) {                                              \
    (HIST) = &::vearch::monitor::NAME##_family().Add(                   \
        (LABELS), ::vearch::monitor::DefaultLatencyBucketsMs());        \
  }                                                                     \
  ::vearch::monitor::ScopeMetric _scope_metric_##NAME(*(HIST))

#else

#define SCOPE_ENGINE_METRIC(NAME, LABELS, HIST) ((void)0)

#endif 

}
}

#endif 
