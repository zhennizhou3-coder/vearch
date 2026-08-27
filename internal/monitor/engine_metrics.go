// Copyright 2019 The Vearch Authors.
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
	"sync"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/common/expfmt"

	"github.com/vearch/vearch/v3/internal/pkg/log"
)

// EngineMetricsProvider returns the C++ engine's Prometheus metrics as a text exposition string
type EngineMetricsProvider func() string

var (
	engineMetricsProviderMu sync.RWMutex
	engineMetricsProvider   EngineMetricsProvider
)

// RegisterEngineMetricsProvider wires the PS-side engine metrics source.
func RegisterEngineMetricsProvider(fn EngineMetricsProvider) {
	engineMetricsProviderMu.Lock()
	engineMetricsProvider = fn
	engineMetricsProviderMu.Unlock()
}

// engineMetricsCollector bridges the C++ engine's text-exposition output into the Go registry.
type engineMetricsCollector struct{}

func (c *engineMetricsCollector) Describe(ch chan<- *prometheus.Desc) {}

func (c *engineMetricsCollector) Collect(ch chan<- prometheus.Metric) {
	engineMetricsProviderMu.RLock()
	provider := engineMetricsProvider
	engineMetricsProviderMu.RUnlock()
	if provider == nil {
		return
	}

	text := provider()
	if text == "" {
		return
	}

	var parser expfmt.TextParser
	families, err := parser.TextToMetricFamilies(strings.NewReader(text))
	if err != nil {
		log.Error("parse engine metrics text failed: %v", err)
		return
	}

	for _, mf := range families {
		emitMetricFamily(ch, mf)
	}
}

// emitMetricFamily converts one parsed dto.MetricFamily into prometheus.Metric values
func emitMetricFamily(ch chan<- prometheus.Metric, mf *dto.MetricFamily) {
	name := mf.GetName()
	help := mf.GetHelp()

	for _, m := range mf.GetMetric() {
		labelNames, labelValues := splitLabels(m.GetLabel())
		desc := prometheus.NewDesc(name, help, labelNames, nil)

		var (
			metric prometheus.Metric
			err    error
		)
		switch mf.GetType() {
		case dto.MetricType_COUNTER:
			metric, err = prometheus.NewConstMetric(desc, prometheus.CounterValue,
				m.GetCounter().GetValue(), labelValues...)
		case dto.MetricType_GAUGE:
			metric, err = prometheus.NewConstMetric(desc, prometheus.GaugeValue,
				m.GetGauge().GetValue(), labelValues...)
		case dto.MetricType_HISTOGRAM:
			h := m.GetHistogram()
			buckets := make(map[float64]uint64, len(h.GetBucket()))
			for _, b := range h.GetBucket() {
				if math.IsInf(b.GetUpperBound(), 1) {
					continue
				}
				buckets[b.GetUpperBound()] = b.GetCumulativeCount()
			}
			metric, err = prometheus.NewConstHistogram(desc,
				h.GetSampleCount(), h.GetSampleSum(), buckets, labelValues...)
		default:
			continue
		}
		if err != nil {
			log.Error("build engine metric %s failed: %v", name, err)
			continue
		}
		ch <- metric
	}
}

func splitLabels(pairs []*dto.LabelPair) (names, values []string) {
	names = make([]string, 0, len(pairs))
	values = make([]string, 0, len(pairs))
	for _, p := range pairs {
		names = append(names, p.GetName())
		values = append(values, p.GetValue())
	}
	return names, values
}
