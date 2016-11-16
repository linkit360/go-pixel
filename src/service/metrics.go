package service

import (
	"github.com/prometheus/client_golang/prometheus"

	m "github.com/vostrok/metrics"
)

var (
	dropped        prometheus.Counter
	empty          prometheus.Counter
	emptyPublisher prometheus.Counter
	emptySettings  prometheus.Counter
	publisherError prometheus.Counter
	addToDBErrors  prometheus.Counter
	addToDbSuccess prometheus.Counter
)

func initMetrics() {
	empty = m.NewCounter("empty", "Empty, invalid messages in queue")
	dropped = m.NewCounter("dropped", "Dropped messages in queue")
	emptyPublisher = m.NewCounter("empty_publisher", "Cannot determine publisher")
	emptySettings = m.NewCounter("empty_settings", "No settings found for this publisher")
	publisherError = m.NewCounter("publisher_error", "Request to publisher ended with error")
	addToDBErrors = m.NewCounter("add_to_db_errors", "Any error connected with database occured")
	addToDbSuccess = m.NewCounter("add_to_db_success", "Count of success")
}
