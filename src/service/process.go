package service

import (
	"encoding/json"
	"fmt"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/streadway/amqp"
)

type Pixel struct {
	SubscriptionId int64  `json:"subscription_id"`
	Pixel          string `json:"pixel"`
	Publisher      string `json:"publisher"`
	Tid            string `json:"tid"`
	Msisdn         string `json:"msisdn"`
}
type Metrics struct {
	Dropped metrics.Gauge
	Empty   metrics.Gauge
}

func initMetrics() Metrics {
	return Metrics{
		Dropped: expvar.NewGauge("dropped"),
		Empty:   expvar.NewGauge("empty"),
	}
}

type EventNotifyUserActions struct {
	EventName string `json:"event_name,omitempty"`
	EventData Pixel  `json:"event_data,omitempty"`
}

func process(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {
		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyUserActions
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			svc.m.Dropped.Add(1)

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("consume pixel")
			msg.Ack(false)
			continue
		}

		t := e.EventData
		if t.Pixel == "" || t.Tid == "" {
			svc.m.Dropped.Add(1)
			svc.m.Empty.Add(1)

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("no pixel or tid, discarding")
			msg.Ack(false)
			continue
		}

		// send pixel

		pixel_sent := false
		var err error
		if err == nil {
			pixel_sent = true
		}
		query := fmt.Sprintf("UPDATE %ssubscriptions "+
			" SET pixel_sent = $1,  "+
			" SET pixel_sent_at = $2  "+
			" WHERE id = $3 ",
			svc.conf.db.TablePrefix)

		if _, err := svc.db.Exec(query,
			pixel_sent,
			time.Now(),
			t.SubscriptionId,
		); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("add pixel sent")
			msg.Nack(false, true)
			continue
		}

		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"pixel": t.Pixel,
		}).Info("processed successfully")
		msg.Ack(false)
	}
}
