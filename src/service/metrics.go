package service

import (
	m "github.com/vostrok/utils/metrics"
	"time"
)

var (
	dropped        m.Gauge
	empty          m.Gauge
	emptyPublisher m.Gauge
	emptySettings  m.Gauge
	publisherError m.Gauge
	Success        m.Gauge
)

func initMetrics() {
	empty = m.NewGauge("", "", "empty", "Empty, invalid messages in queue")
	dropped = m.NewGauge("", "", "dropped", "Dropped messages in queue")
	emptyPublisher = m.NewGauge("", "", "empty_publisher", "Cannot determine publisher")
	emptySettings = m.NewGauge("", "", "empty_settings", "No settings found for this publisher")
	publisherError = m.NewGauge("", "", "publisher_error", "Request to publisher ended with error")
	Success = m.NewGauge("", "", "success", "success")

	go func() {
		for range time.Tick(time.Minute) {
			empty.Update()
			dropped.Update()
			emptyPublisher.Update()
			emptySettings.Update()
			publisherError.Update()
			Success.Update()
		}
	}()
}
