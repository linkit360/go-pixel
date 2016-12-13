package service

// get queue messages from pixels
// and send them to publisher
// ack all ok in case of any error

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	inmem_service "github.com/vostrok/inmem/service"
	"github.com/vostrok/pixels/src/notifier"
)

type EventNotifyPixel struct {
	EventName string         `json:"event_name,omitempty"`
	EventData notifier.Pixel `json:"event_data,omitempty"`
}

// ==========
// http://kbgames.net:10001/index.php?
// pixel=1480041599mb08436761119
// msisdn=923007001926
// trxid=1611250740056626
// trxtime=2016-11-25+07:40:23
// country=pakistan
// operator=mobilink

func processPixels(deliveries <-chan amqp.Delivery) {
	var client *http.Client
	var resp *http.Response

	for msg := range deliveries {

		var operator inmem_service.Operator
		var ps inmem_service.PixelSetting
		var cleintErr error
		var err error
		var statusCode int
		var t notifier.Pixel

		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyPixel
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			dropped.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("consume pixel")

			goto ack
		}

		t = e.EventData
		if t.Pixel == "" || t.Tid == "" {
			dropped.Inc()
			empty.Inc()

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("no pixel or tid, discarding")
			goto ack
		}

		if t.Publisher == "" {
			if len(t.Pixel) == 23 {
				t.Publisher = "Mobusi"
			}
			if len(t.Pixel) == 55 {
				t.Publisher = "Kimia"
			}
		}
		if t.Publisher == "" {
			dropped.Inc()
			empty.Inc()
			emptyPublisher.Inc()
			log.WithFields(log.Fields{
				"error": "Empty publisher",
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("cannot determine publisher")
			goto ack
		}
		// send pixel
		ps = inmem_service.PixelSetting{
			Publisher:    t.Publisher,
			OperatorCode: t.OperatorCode,
			CampaignId:   t.CampaignId,
		}
		ps, err = inmem_client.GetPixelSettingByKeyWithRatio(ps.Key())
		if err != nil {
			err = fmt.Errorf("GetPixelSettingByKey: %s", err.Error())
			dropped.Inc()
			emptySettings.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
				"key":   ps.Key(),
			}).Error("can't process pixel")
			goto ack
		}
		if !ps.Enabled {
			dropped.Inc()

			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
			}).Debug("send pixel disabled")
			goto ack
		}
		if ps.SkipPixelSend {
			dropped.Inc()
			log.WithFields(log.Fields{
				"pixel":   t.Pixel,
				"tid":     t.Tid,
				"msg":     "dropped",
				"key":     ps.Key(),
				"setting": fmt.Sprintf("%#v", ps),
			}).Info("ratio: must skip")
			goto ack
		}
		log.WithFields(log.Fields{
			"pixel": t.Pixel,
			"tid":   t.Tid,
			"ratio": ps.Ratio,
			"count": ps.Count,
			"key":   ps.Key(),
		}).Info("ratio rule: passed")

		t.Sent = false

		if svc.conf.server.Env == "dev" {
			t.Endpoint = "http://localhost:50309/publisher?" +
				"aff_sub=%pixel%&" +
				"msisdn=%msisdn%&" +
				"trxid=%trxid%&" +
				"trxtime=%time%&" +
				"country=%country_name%&" +
				"operator=%operator_name%"
		} else {
			t.Endpoint = ps.Endpoint
		}

		t.Endpoint = strings.Replace(t.Endpoint, "%pixel%", t.Pixel, 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%msisdn%", t.Msisdn, 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%trxid%", fmt.Sprintf("%d%s", time.Now().Unix(), t.Msisdn), 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%time%", time.Now().UTC().Format("2006-01-02+15:04:05"), 1)

		operator, err = inmem_client.GetOperatorByCode(t.OperatorCode)
		if err != nil {
			err = fmt.Errorf("GetPixelSettingByKey: %s", err.Error())
			dropped.Inc()
			emptySettings.Inc()

			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"error": err.Error(),
			}).Error("can't get operator name by code")
			goto ack
		}
		t.Endpoint = strings.Replace(t.Endpoint, "%operator_name%", operator.Name, 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%country_name%", operator.CountryName, 1)

		client = &http.Client{
			Timeout: time.Duration(ps.Timeout) * time.Second,
		}

		resp, cleintErr = client.Get(t.Endpoint)
		if cleintErr != nil || resp.StatusCode != 200 {
			publisherError.Inc()
		} else {
			Success.Inc()
		}
		log.WithFields(log.Fields{
			"resp": fmt.Sprintf("%#v", resp),
		}).Debug("response")
		if resp != nil {
			statusCode = resp.StatusCode
			t.ResponseCode = resp.StatusCode
		}

		if statusCode == 200 {
			t.Sent = true
		}
		log.WithFields(log.Fields{
			"pixel":    t.Pixel,
			"tid":      t.Tid,
			"campaign": t.CampaignId,
			"url":      t.Endpoint,
			"code":     statusCode,
			"sent":     t.Sent,
		}).Debug("response")

		if err := svc.n.PixelTransactionNotify(t); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"error": err.Error(),
			}).Error("cannot send pixel transaction")
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
			}).Info("sent pixel transaction")
		}

		if cleintErr != nil || statusCode != 200 {
			goto ack
		}

		if err := svc.n.PixelUpdateSubscriptionNotify(t); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"error": err.Error(),
			}).Error("cannot send pixel update subscription")
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
			}).Info("sent pixel update subscription")
		}
	ack:
		if err := msg.Ack(false); err != nil {
			log.WithFields(log.Fields{
				"tid":   e.EventData.Tid,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
