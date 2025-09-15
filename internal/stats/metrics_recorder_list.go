/*
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
 */

package stats

import (
	"errors"
	"fmt"

	estats "google.golang.org/grpc/experimental/stats"
	"google.golang.org/grpc/stats"
)

// MetricsRecorderList forwards Record calls to all of its metricsRecorders.
//
// It eats any record calls where the label values provided do not match the
// number of label keys.
type MetricsRecorderList struct {
	// metricsRecorders are the metrics recorders this list will forward to.
	metricsRecorders []estats.MetricsRecorder
}

// NewMetricsRecorderList creates a new metric recorder list with all the stats
// handlers provided which implement the MetricsRecorder interface.
// If no stats handlers provided implement the MetricsRecorder interface,
// the MetricsRecorder list returned is a no-op.
func NewMetricsRecorderList(shs []stats.Handler) *MetricsRecorderList {
	var mrs []estats.MetricsRecorder
	for _, sh := range shs {
		if mr, ok := sh.(estats.MetricsRecorder); ok {
			mrs = append(mrs, mr)
		}
	}
	return &MetricsRecorderList{
		metricsRecorders: mrs,
	}
}

func verifyLabels(desc *estats.MetricDescriptor, labelsRecv ...string) {
	if got, want := len(labelsRecv), len(desc.Labels)+len(desc.OptionalLabels); got != want {
		panic(fmt.Sprintf("Received %d labels in call to record metric %q, but expected %d.", got, desc.Name, want))
	}
}

// RecordInt64Count records the measurement alongside labels on the int
// count associated with the provided handle.
func (l *MetricsRecorderList) RecordInt64Count(handle *estats.Int64CountHandle, incr int64, labels ...string) {
	verifyLabels(handle.Descriptor(), labels...)

	for _, metricRecorder := range l.metricsRecorders {
		metricRecorder.RecordInt64Count(handle, incr, labels...)
	}
}

// RecordFloat64Count records the measurement alongside labels on the float
// count associated with the provided handle.
func (l *MetricsRecorderList) RecordFloat64Count(handle *estats.Float64CountHandle, incr float64, labels ...string) {
	verifyLabels(handle.Descriptor(), labels...)

	for _, metricRecorder := range l.metricsRecorders {
		metricRecorder.RecordFloat64Count(handle, incr, labels...)
	}
}

// RecordInt64Histo records the measurement alongside labels on the int
// histo associated with the provided handle.
func (l *MetricsRecorderList) RecordInt64Histo(handle *estats.Int64HistoHandle, incr int64, labels ...string) {
	verifyLabels(handle.Descriptor(), labels...)

	for _, metricRecorder := range l.metricsRecorders {
		metricRecorder.RecordInt64Histo(handle, incr, labels...)
	}
}

// RecordFloat64Histo records the measurement alongside labels on the float
// histo associated with the provided handle.
func (l *MetricsRecorderList) RecordFloat64Histo(handle *estats.Float64HistoHandle, incr float64, labels ...string) {
	verifyLabels(handle.Descriptor(), labels...)

	for _, metricRecorder := range l.metricsRecorders {
		metricRecorder.RecordFloat64Histo(handle, incr, labels...)
	}
}

// RecordInt64Gauge records the measurement alongside labels on the int
// gauge associated with the provided handle.
func (l *MetricsRecorderList) RecordInt64Gauge(handle *estats.Int64GaugeHandle, incr int64, labels ...string) {
	verifyLabels(handle.Descriptor(), labels...)

	for _, metricRecorder := range l.metricsRecorders {
		metricRecorder.RecordInt64Gauge(handle, incr, labels...)
	}
}

// RegisterBatchCallback TODO
func (l *MetricsRecorderList) RegisterBatchCallback(callback estats.Callback, descriptors ...*estats.MetricDescriptor) (estats.Unregister, error) {
	for _, desc := range descriptors {
		if got, want := desc.Type, estats.MetricTypeAsyncIntGauge; got != want {
			panic(fmt.Sprintf("Received synchronous metric %q of type %v in call to register callback, but expected async metrics of type %v.", desc.Name, got, want))
		}
	}
	unregisterFns := make([]func() error, 0, len(l.metricsRecorders))
	var returnError error

	for _, mr := range l.metricsRecorders {
		wrappedCallback := func(recorder estats.AsyncMetricsRecorder) error {
			wrappedRecorder := &asyncRecorderWrapper{
				delegate: recorder,
			}
			return callback(wrappedRecorder)
		}
		fn, err := mr.RegisterBatchCallback(wrappedCallback, descriptors...)
		returnError = errors.Join(returnError, err)
		if err != nil {
			break
		}
		unregisterFns = append(unregisterFns, fn)
	}

	if returnError != nil {
		for _, fn := range unregisterFns {
			fn()
		}
		return nil, returnError
	}

	return func() error {
		var err error
		for _, fn := range unregisterFns {
			err = errors.Join(err, fn())
		}
		return err
	}, nil
}

type asyncRecorderWrapper struct {
	delegate estats.AsyncMetricsRecorder
}

// RecordIntAsync64Gauge records the measurement alongside labels on the int
// gauge associated with the provided handle.
func (w *asyncRecorderWrapper) RecordInt64Gauge(handle *estats.Int64AsyncGaugeHandle, value int64, labels ...string) {
	verifyLabels(handle.Descriptor(), labels...)
	w.delegate.RecordInt64Gauge(handle, value, labels...)
}
