/**
 * Copyright 2019 The Gamma Authors.
 *
 * This source code is licensed under the Apache License, Version 2.0 license
 * found in the LICENSE file in the root directory of this source tree.
 */

#include <gtest/gtest.h>

#include <string>

#include "monitor/monitor.h"
#include "monitor/prometheus_client.h"
#include "monitor/scope_metric.h"

#ifdef ENGINE_METRICS_ENABLED
#include <prometheus/client_metric.h>
#include <prometheus/histogram.h>

#include <cmath>
#include <cstdint>
#endif

namespace test {

#ifdef ENGINE_METRICS_ENABLED

// The latency bucket boundaries are a contract: the Go bridge and every P99
// dashboard depend on these exact `le` upper bounds. Pin them so a stray edit
// to DefaultLatencyBucketsMs() fails here instead of silently reshaping P99.
TEST(EngineMetrics, DefaultLatencyBucketsAreStable) {
  const vearch::monitor::Buckets &b = vearch::monitor::DefaultLatencyBucketsMs();
  const vearch::monitor::Buckets expected = {0.5, 1,   2,   5,   10,  20,
                                             50,  100, 200, 500, 1000};
  EXPECT_EQ(b, expected);
}

// Observe a known value and assert it lands in the correct cumulative buckets.
// 3.0ms must fall in le="5" (the first boundary >= 3.0), not le="2". This is
// exactly the bucketing the Go side reads back via histogram_quantile.
TEST(EngineMetrics, ObserveLandsInExpectedBucket) {
  prometheus::Histogram &h =
      vearch::monitor::engine_search_latency_family().Add(
          {{"space", "bucket_test"}}, vearch::monitor::DefaultLatencyBucketsMs());
  h.Observe(3.0);

  prometheus::ClientMetric cm = h.Collect();
  EXPECT_EQ(cm.histogram.sample_count, 1u);
  EXPECT_DOUBLE_EQ(cm.histogram.sample_sum, 3.0);

  uint64_t at_le_2 = 0, at_le_5 = 0, at_inf = 0;
  for (const auto &bucket : cm.histogram.bucket) {
    if (bucket.upper_bound == 2.0) at_le_2 = bucket.cumulative_count;
    if (bucket.upper_bound == 5.0) at_le_5 = bucket.cumulative_count;
    if (std::isinf(bucket.upper_bound)) at_inf = bucket.cumulative_count;
  }
  EXPECT_EQ(at_le_2, 0u);   // 3.0 is above le="2"
  EXPECT_EQ(at_le_5, 1u);   // and at/below le="5"
  EXPECT_EQ(at_inf, 1u);    // +Inf is cumulative == sample_count
}

// ScopeMetric records exactly one sample on destruction (RAII). Timing is
// nondeterministic, so pin only the count/sum contract, not the elapsed value.
TEST(EngineMetrics, ScopeMetricObservesOneSampleOnDestruct) {
  prometheus::Histogram &h =
      vearch::monitor::engine_search_latency_family().Add(
          {{"space", "scope_test"}}, vearch::monitor::DefaultLatencyBucketsMs());
  { vearch::monitor::ScopeMetric s(h); }

  prometheus::ClientMetric cm = h.Collect();
  EXPECT_EQ(cm.histogram.sample_count, 1u);
  EXPECT_GE(cm.histogram.sample_sum, 0.0);
}

// After an observation the text exposition must carry the metric name, HELP,
// and the +Inf bucket line — the exact surface the Go TextParser consumes.
TEST(EngineMetrics, TextExpositionContainsFamily) {
  vearch::monitor::engine_search_latency_family()
      .Add({{"space", "text_test"}}, vearch::monitor::DefaultLatencyBucketsMs())
      .Observe(1.0);

  std::string text = vearch::monitor::GetEngineMetricsText();
  EXPECT_NE(text.find("vearch_engine_search_latency_ms"), std::string::npos);
  EXPECT_NE(text.find("le=\"+Inf\""), std::string::npos);
  EXPECT_NE(text.find("vearch_engine_search_latency_ms_count"),
            std::string::npos);
}

#else  // metrics disabled -> the whole pipeline is a no-op

// With BUILD_WITH_ENGINE_METRICS=OFF the serializer must yield an empty string
// so the Go collector short-circuits (see engine_metrics.go: `if text == ""`).
TEST(EngineMetrics, DisabledYieldsEmptyText) {
  EXPECT_TRUE(vearch::monitor::GetEngineMetricsText().empty());
}

#endif  // ENGINE_METRICS_ENABLED

}  // namespace test
