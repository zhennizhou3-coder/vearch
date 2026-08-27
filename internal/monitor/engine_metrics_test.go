// Copyright 2026 The Vearch Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

package monitor

import (
	"math"
	"strings"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"
)

// TestEngineMetricsCollectorHistogramDropsPlusInfBucket reproduces the H1 bug:
// a C++ histogram serialized with a `le="+Inf"` bucket must not have that
// bucket passed to NewConstHistogram, otherwise the metric is rejected and the
// P99 histogram is silently dropped. After the fix, exactly one histogram
// metric is collected and its count survives.
func TestEngineMetricsCollectorHistogramDropsPlusInfBucket(t *testing.T) {
	// One observation at 1ms and one at 20ms: cumulative buckets 5ms=1,
	// 50ms=2, +Inf=2.
	text := `
# HELP vearch_engine_search_latency_ms Engine top-level search latency in milliseconds
# TYPE vearch_engine_search_latency_ms histogram
vearch_engine_search_latency_ms_bucket{space="s1",le="0.5"} 0
vearch_engine_search_latency_ms_bucket{space="s1",le="5"} 1
vearch_engine_search_latency_ms_bucket{space="s1",le="50"} 2
vearch_engine_search_latency_ms_bucket{space="s1",le="+Inf"} 2
vearch_engine_search_latency_ms_sum{space="s1"} 21
vearch_engine_search_latency_ms_count{space="s1"} 2
`
	collector := &engineMetricsCollector{}
	ch := make(chan prometheus.Metric, 16)

	// Sanity: the text parses into one histogram family before we collect.
	var parser expfmt.TextParser
	if _, err := parser.TextToMetricFamilies(strings.NewReader(text)); err != nil {
		t.Fatalf("fixture text does not parse: %v", err)
	}

	RegisterEngineMetricsProvider(func() string { return text })
	defer RegisterEngineMetricsProvider(nil)

	collector.Collect(ch)
	close(ch)

	var histograms []*dto.Metric
	for m := range ch {
		var dm dto.Metric
		if err := m.Write(&dm); err != nil {
			t.Fatalf("metric.Write: %v", err)
		}
		if dm.GetHistogram() != nil {
			histograms = append(histograms, &dm)
		}
	}

	if len(histograms) != 1 {
		t.Fatalf("expected 1 histogram metric, got %d (the +Inf bucket bug drops the whole histogram)", len(histograms))
	}
	h := histograms[0].GetHistogram()
	if h.GetSampleCount() != 2 {
		t.Errorf("sample count = %d, want 2", h.GetSampleCount())
	}
	if h.GetSampleSum() != 21 {
		t.Errorf("sample sum = %v, want 21", h.GetSampleSum())
	}
	// Finite buckets only (0.5, 5, 50); +Inf is carried by SampleCount.
	if len(h.GetBucket()) != 3 {
		t.Errorf("finite buckets = %d, want 3", len(h.GetBucket()))
	}
	for _, b := range h.GetBucket() {
		if math.IsInf(b.GetUpperBound(), 1) {
			t.Errorf("found an explicit +Inf bucket; it must be dropped")
		}
	}
	// Label must round-trip.
	var gotSpace bool
	for _, l := range histograms[0].GetLabel() {
		if l.GetName() == "space" && l.GetValue() == "s1" {
			gotSpace = true
		}
	}
	if !gotSpace {
		t.Errorf("space label missing")
	}
}

// TestSplitLabels checks the label pair splitting helper.
func TestSplitLabels(t *testing.T) {
	pairs := []*dto.LabelPair{
		{Name: strPtr("space"), Value: strPtr("db_s")},
		{Name: strPtr("field"), Value: strPtr("vec")},
	}
	names, values := splitLabels(pairs)
	if len(names) != 2 || len(values) != 2 {
		t.Fatalf("splitLabels returned %d names, %d values", len(names), len(values))
	}
	if names[0] != "space" || values[0] != "db_s" {
		t.Errorf("unexpected label %s=%s", names[0], values[0])
	}
}

func strPtr(s string) *string { return &s }
