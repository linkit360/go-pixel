package service

import (
	m "github.com/vostrok/utils/metrics"
	"time"
)

var (
	Success        m.Gauge
	Errors         m.Gauge
	dropped        m.Gauge
	empty          m.Gauge
	emptyPublisher m.Gauge
	emptySettings  m.Gauge
	publisherError m.Gauge
)

func initMetrics(appName string) {
	Success = m.NewGauge("", "", "success", "success")
	Errors = m.NewGauge("", "", "errors", "errors")

	empty = m.NewGauge("", appName, "empty", "Empty, invalid messages in queue")
	dropped = m.NewGauge("", appName, "dropped", "Dropped messages in queue")
	emptyPublisher = m.NewGauge("", appName, "empty_publisher", "Cannot determine publisher")
	emptySettings = m.NewGauge("", appName, "empty_settings", "No settings found for this publisher")
	publisherError = m.NewGauge("", appName, "publisher_error", "Request to publisher ended with error")

	go func() {
		for range time.Tick(time.Minute) {
			Success.Update()
			Errors.Update()
			empty.Update()
			dropped.Update()
			emptyPublisher.Update()
			emptySettings.Update()
			publisherError.Update()
		}
	}()
}
