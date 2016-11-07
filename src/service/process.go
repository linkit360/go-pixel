package service

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/go-kit/kit/metrics"
	"github.com/go-kit/kit/metrics/expvar"
	"github.com/streadway/amqp"
)

type Pixel struct {
	Tid            string  `json:"tid,omitempty"`
	Msisdn         string  `json:"msisdn,omitempty"`
	CampaignId     int64   `json:"campaign_id,omitempty"`
	SubscriptionId int64   `json:"subscription_id,omitempty"`
	OperatorCode   int64   `json:"operator_code,omitempty"`
	CountryCode    int64   `json:"country_code,omitempty"`
	Pixel          string  `json:"pixel,omitempty"`
	Publisher      string  `json:"publisher,omitempty"`
	ResponseCode   int     `json:"response_code,omitempty"`
	Took           float64 `json:"took,omitempty"`
	Sent           bool    `json:"sent"`
}

type Metrics struct {
	Dropped  metrics.Gauge
	Empty    metrics.Gauge
	counters Counters
}

func initMetrics() Metrics {
	m := Metrics{
		Dropped: expvar.NewGauge("dropped"),
		Empty:   expvar.NewGauge("empty"),
	}
	go func() {
		for range time.Tick(60 * time.Second) {
			m.Empty.Set(m.counters.Empty)
			m.Dropped.Set(m.counters.Dropped)
			m.counters.Clear()
		}
	}()
	return m
}

type Counters struct {
	Dropped float64
	Empty   float64
}

func (c *Counters) Clear() {
	c.Dropped = .0
	c.Empty = .0
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
			svc.m.counters.Dropped++

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
			svc.m.counters.Dropped++
			svc.m.counters.Empty++

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("no pixel or tid, discarding")

			msg.Ack(false)
			continue
		}

		if len(t.Pixel) == 23 {
			t.Publisher = "Mobusi"
		}
		if len(t.Pixel) == 55 {
			t.Publisher = "Kimia"
		}
		if t.Publisher == "" {
			svc.m.counters.Dropped++
			svc.m.counters.Empty++
			log.WithFields(log.Fields{
				"error": "Empty publisher",
				"msg":   "dropped",
			}).Error("cannot determine publisher")
			msg.Ack(false)
			continue
		}
		// send pixel
		ps := PixelSetting{
			Publisher:    t.Publisher,
			OperatorCode: t.OperatorCode,
			CampaignId:   t.CampaignId,
		}
		pixelSetting, ok := memPixels.pixels.ByKey[ps.key()]
		if !ok {
			log.WithFields(log.Fields{
				"error": "No settings",
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
				"key":   ps.key(),
			}).Error("no pixel settings found")
			msg.Ack(false)
			continue
		}
		if !pixelSetting.Enabled {
			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
			}).Debug("send pixel disabled")
			msg.Ack(false)
			continue
		}
		if pixelSetting.Ignore() {
			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
				"ratio": pixelSetting.Ratio,
				"count": pixelSetting.count,
			}).Info("ratio: must skip")
			msg.Ack(false)
			continue
		}
		log.WithFields(log.Fields{
			"pixel": t.Pixel,
			"tid":   t.Tid,
			"ratio": pixelSetting.Ratio,
			"count": pixelSetting.count,
		}).Info("ratio rule: passed")

		t.Sent = false
		var err error
		endpoint := ""
		if svc.conf.server.Env == "dev" {
			endpoint = "http://localhost:50308/publisher?aff_sub=%pixel%"
		} else {
			endpoint = pixelSetting.Endpoint
		}
		endpoint = strings.Replace(endpoint, "%pixel%", t.Pixel, 1)
		client := &http.Client{
			Timeout: time.Duration(pixelSetting.Timeout) * time.Second,
		}

		resp, err := client.Get(endpoint)
		if err != nil {
			err = fmt.Errorf("client.Do: %s", err.Error())
			log.WithFields(log.Fields{
				"pixel":    t.Pixel,
				"tid":      t.Tid,
				"campaign": t.CampaignId,
				"url":      endpoint,
				"error":    err.Error(),
				"msg":      "dropped",
			}).Error("call publisher failed")
			msg.Ack(false)
			continue
		}
		log.WithFields(log.Fields{
			"pixel":    t.Pixel,
			"tid":      t.Tid,
			"campaign": t.CampaignId,
			"url":      endpoint,
			"code":     resp.Status,
		}).Debug("response")

		if err == nil && resp.StatusCode == 200 {
			t.Sent = true
		}
		query := fmt.Sprintf("INSERT INTO %spixel_transactions ( "+
			"tid, "+
			"msisdn, "+
			"id_campaign, "+
			"operator_code, "+
			"country_code, "+
			"publisher, "+
			"response_code "+
			") VALUES ( $1, $2, $3, $4, $5, $6, $7 )",
			svc.conf.db.TablePrefix)

		if _, err := svc.db.Exec(query,
			t.Tid,
			t.Msisdn,
			t.CampaignId,
			t.OperatorCode,
			t.CountryCode,
			t.Publisher,
			resp.StatusCode,
		); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"query": query,
				"error": err.Error(),
				"msg":   "dropped",
			}).Error("record pixel transaction failed")
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
			}).Info("updated pixel transaction")
		}

		query = fmt.Sprintf("UPDATE %ssubscriptions SET "+
			" pixel_sent = $1,  "+
			" pixel_sent_at = $2  "+
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
				"error": err.Error(),
				"msg":   "dropped",
			}).Error("update subscription pixel sent")
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
			}).Info("updated subscrption")
		}

		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"pixel": t.Pixel,
			"queue": "pixels",
		}).Info("processed")
		msg.Ack(false)
	}
}
