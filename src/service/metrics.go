package service

import (
	"time"

	"github.com/prometheus/client_golang/prometheus"

	m "github.com/linkit360/go-utils/metrics"
)

var (
	Success        m.Gauge
	Errors         m.Gauge
	dropped        m.Gauge
	empty          m.Gauge
	emptyPublisher m.Gauge
	emptySettings  m.Gauge
	publisherError m.Gauge
	Restore        *RestorePixelMetrics
)

type RestorePixelMetrics struct {
	Success                m.Gauge
	Errors                 m.Gauge
	Dropped                m.Gauge
	Empty                  m.Gauge
	BufferPixelNotFound    m.Gauge
	GetBufferPixelDuration prometheus.Summary
}

func initMetrics(appName string) {
	Success = m.NewGauge("", "", "success", "success")
	Errors = m.NewGauge("", "", "errors", "errors")

	empty = m.NewGauge("", appName, "empty", "Empty, invalid messages in queue")
	dropped = m.NewGauge("", appName, "dropped", "Dropped messages in queue")
	emptyPublisher = m.NewGauge("", appName, "empty_publisher", "Cannot determine publisher")
	emptySettings = m.NewGauge("", appName, "empty_settings", "No settings found for this publisher")
	publisherError = m.NewGauge("", appName, "publisher_error", "Request to publisher ended with error")
	Restore = &RestorePixelMetrics{
		Success:                m.NewGauge(appName, "buffer_pixel", "success", "bp success"),
		Errors:                 m.NewGauge(appName, "buffer_pixel", "errors", "bp errors"),
		Dropped:                m.NewGauge(appName, "buffer_pixel", "dropped", "bp dropped"),
		Empty:                  m.NewGauge(appName, "buffer_pixel", "empty", "bp empty"),
		BufferPixelNotFound:    m.NewGauge(appName, "buffer_pixel", "not_found", "bp not found"),
		GetBufferPixelDuration: m.NewSummary(appName+"_buffer_pixel_get_db_duration_seconds", "get buffer pixel duration seconds"),
	}
	go func() {
		for range time.Tick(time.Minute) {
			Success.Update()
			Errors.Update()
			empty.Update()
			dropped.Update()
			emptyPublisher.Update()
			emptySettings.Update()
			publisherError.Update()

			Restore.Success.Update()
			Restore.Errors.Update()
			Restore.Dropped.Update()
			Restore.Empty.Update()
			Restore.BufferPixelNotFound.Update()
		}
	}()
}
