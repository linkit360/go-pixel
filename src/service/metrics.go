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
	addToDBErrors  m.Gauge
	addToDbSuccess m.Gauge
)

func initMetrics() {
	empty = m.NewGauge("", "", "empty", "Empty, invalid messages in queue")
	dropped = m.NewGauge("", "", "dropped", "Dropped messages in queue")
	emptyPublisher = m.NewGauge("", "", "empty_publisher", "Cannot determine publisher")
	emptySettings = m.NewGauge("", "", "empty_settings", "No settings found for this publisher")
	publisherError = m.NewGauge("", "", "publisher_error", "Request to publisher ended with error")
	addToDBErrors = m.NewGauge("", "", "add_to_db_errors", "Any error connected with database occured")
	addToDbSuccess = m.NewGauge("", "", "add_to_db_success", "Count of success")

	go func() {
		// metrics in prometheus as for 15s (default)
		// so make for minute interval
		for range time.Tick(time.Minute) {
			dropped.Update()
			empty.Update()
			emptyPublisher.Update()
			emptySettings.Update()
			publisherError.Update()
			addToDBErrors.Update()
			addToDbSuccess.Update()
		}
	}()
}
