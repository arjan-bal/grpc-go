/*
 *
 * Copyright 2024 gRPC authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

// Package stats contains experimental metrics/stats API's.
package stats

import "google.golang.org/grpc/stats"

// MetricsRecorder records on metrics derived from metric registry.
type MetricsRecorder interface {
	// RecordInt64Count records the measurement alongside labels on the int
	// count associated with the provided handle.
	RecordInt64Count(handle *Int64CountHandle, incr int64, labels ...string)
	// RecordFloat64Count records the measurement alongside labels on the float
	// count associated with the provided handle.
	RecordFloat64Count(handle *Float64CountHandle, incr float64, labels ...string)
	// RecordInt64Histo records the measurement alongside labels on the int
	// histo associated with the provided handle.
	RecordInt64Histo(handle *Int64HistoHandle, incr int64, labels ...string)
	// RecordFloat64Histo records the measurement alongside labels on the float
	// histo associated with the provided handle.
	RecordFloat64Histo(handle *Float64HistoHandle, incr float64, labels ...string)
	// RecordInt64Gauge records the measurement alongside labels on the int
	// gauge associated with the provided handle.
	RecordInt64Gauge(handle *Int64GaugeHandle, value int64, labels ...string)
	// RegisterBatchCallback registers a callback to produce metric values for
	// only the listed descriptors. The returned function must be called when no
	// the metrics are no longer needed, which will remove the callback. The
	// returned method needs to be idempotent and concurrent safe.
	RegisterBatchCallback(callback Callback, descriptors ...AsyncMetric) func()
}

// Callback is a function registered with a MetricsRecorder that records metrics
// asynchronously for the set of descriptors it is registered with. The
// AsyncMetricsRecorder parameter is used to record values for these metrics.
//
// The function needs to make unique recording across all registered
// Callbacks. Meaning, it should not report values for a metric with the same
// attributes as another Callback will report.
//
// The function needs to be concurrent safe.
type Callback func(AsyncMetricsRecorder) error

// AsyncMetricsRecorder is a recorder for async metrics.
type AsyncMetricsRecorder interface {
	// RecordInt64Gauge records the measurement alongside labels on the int
	// gauge associated with the provided handle.
	RecordInt64Gauge(handle *Int64AsyncGaugeHandle, value int64, labels ...string)
}

// Metrics is an experimental legacy alias of the now-stable stats.MetricSet.
// Metrics will be deleted in a future release.
type Metrics = stats.MetricSet

// Metric was replaced by direct usage of strings.
type Metric = string

// NewMetrics is an experimental legacy alias of the now-stable
// stats.NewMetricSet.  NewMetrics will be deleted in a future release.
func NewMetrics(metrics ...Metric) *Metrics {
	return stats.NewMetricSet(metrics...)
}
