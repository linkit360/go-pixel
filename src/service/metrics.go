package service

import (
	"time"

	m "github.com/vostrok/metrics"
)

var (
	dropped        m.Gauge
	empty          m.Gauge
	emptyPublisher m.Gauge
	emptySettings  m.Gauge
	publisherError m.Gauge
	dbError        m.Gauge
	success        m.Gauge
)

func newGauge(name, help string) m.Gauge {
	return m.NewGaugeMetric("", name, " "+help)
}
func initMetrics() {

	empty = newGauge("empty", "Empty, invalid messages in queue")
	dropped = newGauge("dropped", "Dropped messages in queue")
	emptyPublisher = newGauge("empty_publisher", "Cannot determine publisher")
	emptySettings = newGauge("empty_settings", "No settings found for this publisher")
	publisherError = newGauge("publisher_error", "Request to publisher ended with error")
	dbError = newGauge("db_error", "Any error connected with database occured")
	success = newGauge("success", "Count of success")

	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {
			empty.Update()
			dropped.Update()
			emptyPublisher.Update()
			emptySettings.Update()
			publisherError.Update()
			dbError.Update()
			success.Update()
		}
	}()
}
