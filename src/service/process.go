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

//CREATE TABLE public.xmp_pixel_transactions (
//id SERIAL PRIMARY KEY,
//created_at TIMESTAMP NOT NULL DEFAULT now(),
//tid CHARACTER VARYING(127) NOT NULL DEFAULT '',
//msisdn CHARACTER VARYING(32) NOT NULL DEFAULT '',
//id_campaign INTEGER NOT NULL DEFAULT 0,
//operator_code INTEGER NOT NULL DEFAULT 0,
//country_code INTEGER NOT NULL DEFAULT 0,
//publisher VARCHAR(511) NOT NULL DEFAULT '',
//response_code INT NOT NULL DEFAULT 0,
//took INT NOT NULL DEFAULT 0
//);
//

type Pixel struct {
	Tid            string  `json:"tid,omitempty"`
	Msisdn         string  `json:"msisdn,omitempty"`
	CampaignId     int64   `json:"campaign_id,omitempty"`
	SubscriptionId int64   `json:"subscription_id,omitempty"`
	OperatorCode   int     `json:"operator_code,omitempty"`
	CountryCode    int     `json:"country_code,omitempty"`
	Pixel          string  `json:"pixel,omitempty"`
	Publisher      string  `json:"publisher,omitempty"`
	ResponseCode   int     `json:"response_code,omitempty"`
	Took           float64 `json:"took,omitempty"`
	Sent           bool    `json:"sent"`
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
		pixelSetting, ok := memPixels.pixels.Map[t.CampaignId]
		if !ok {
			log.WithFields(log.Fields{
				"error":    "No settings",
				"pixel":    t.Pixel,
				"tid":      t.Tid,
				"campaign": t.CampaignId,
			}).Error("no pixel settings found")
			msg.Ack(false)
			continue
		}
		if !pixelSetting.Enabled {
			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
			}).Debug("send pixel disabled")
			msg.Ack(false)
			continue
		}

		log.WithFields(log.Fields{
			"pixel":    t.Pixel,
			"tid":      t.Tid,
			"campaign": t.CampaignId,
		}).Error("pixel send enabled")

		t.Sent = false
		var err error
		if err == nil {
			t.Sent = true
		}
		query := fmt.Sprintf("UPDATE %ssubscriptions "+
			" SET pixel_sent = $1,  "+
			" SET pixel_sent_at = $2  "+
			" WHERE id = $3 ",
			svc.conf.db.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Sent,
			time.Now(),
			t.SubscriptionId,
		); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"query": query,
				"msg":   "requeue",
				"error": err.Error(),
			}).Error("update subscription pixel sent")
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
