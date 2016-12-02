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

type Counters struct {
	Dropped float64
	Empty   float64
}

func (c *Counters) Clear() {
	c.Dropped = .0
	c.Empty = .0
}

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
	for msg := range deliveries {
		time.Sleep(time.Second)

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
			msg.Ack(false)
			continue
		}

		t := e.EventData
		if t.Pixel == "" || t.Tid == "" {
			dropped.Inc()
			empty.Inc()

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("no pixel or tid, discarding")

			msg.Ack(false)
			continue
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
			}).Error("cannot determine publisher")
			msg.Ack(false)
			continue
		}

		// send pixel
		ps := inmem_service.PixelSetting{
			Publisher:    t.Publisher,
			OperatorCode: t.OperatorCode,
			CampaignId:   t.CampaignId,
		}
		pixelSetting, err := inmem_client.GetPixelSettingByKeyWithRatio(ps.Key())
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
			msg.Ack(false)
			continue
		}
		if !pixelSetting.Enabled {
			dropped.Inc()

			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
			}).Debug("send pixel disabled")
			msg.Ack(false)
			continue
		}
		if pixelSetting.SkipPixelSend {
			dropped.Inc()
			log.WithFields(log.Fields{
				"pixel":   t.Pixel,
				"tid":     t.Tid,
				"msg":     "dropped",
				"key":     ps.Key(),
				"setting": fmt.Sprintf("%#v", pixelSetting),
			}).Info("ratio: must skip")
			msg.Ack(false)
			continue
		}
		log.WithFields(log.Fields{
			"pixel":   t.Pixel,
			"tid":     t.Tid,
			"skip":    pixelSetting.SkipPixelSend,
			"ratio":   pixelSetting.Ratio,
			"count":   pixelSetting.Count,
			"key":     ps.Key(),
			"setting": fmt.Sprintf("%#v", pixelSetting),
		}).Info("ratio rule: passed")

		t.Sent = false
		endpoint := ""
		if svc.conf.server.Env == "dev" {
			endpoint = "http://localhost:50309/publisher?" +
				"aff_sub=%pixel%&" +
				"msisdn=%msisdn%&" +
				"trxid=%trxid%&" +
				"trxtime=%time%&" +
				"country=%country_name%&" +
				"operator=%operator_name%"
		} else {
			endpoint = pixelSetting.Endpoint
		}

		endpoint = strings.Replace(endpoint, "%pixel%", t.Pixel, 1)
		endpoint = strings.Replace(endpoint, "%msisdn%", t.Msisdn, 1)
		endpoint = strings.Replace(endpoint, "%trxid%", fmt.Sprintf("%d%s", time.Now().Unix(), t.Msisdn), 1)
		endpoint = strings.Replace(endpoint, "%time%", time.Now().UTC().Format("2006-01-02+15:04:05"), 1)

		operator, err := inmem_client.GetOperatorByCode(t.OperatorCode)
		if err != nil {
			err = fmt.Errorf("GetPixelSettingByKey: %s", err.Error())
			dropped.Inc()
			emptySettings.Inc()

			msg.Ack(false)
			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"error": err.Error(),
			}).Error("can't get operator name by code")
			continue
		}
		endpoint = strings.Replace(endpoint, "%operator_name%", operator.Name, 1)
		endpoint = strings.Replace(endpoint, "%country_name%", operator.CountryName, 1)

		client := &http.Client{
			Timeout: time.Duration(pixelSetting.Timeout) * time.Second,
		}

		resp, err := client.Get(endpoint)
		if err != nil || resp.StatusCode != 200 {
			publisherError.Inc()
		}
		if err != nil {
			dropped.Inc()

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

		if resp.StatusCode == 200 {
			t.Sent = true
		}
		query := fmt.Sprintf("INSERT INTO %spixel_transactions ( "+
			"tid, "+
			"msisdn, "+
			"pixel, "+
			"endpoint, "+
			"id_campaign, "+
			"operator_code, "+
			"country_code, "+
			"publisher, "+
			"response_code "+
			") VALUES ( $1, $2, $3, $4, $5, $6, $7, $8, $9)",
			svc.conf.db.TablePrefix)

		begin := time.Now()
		if _, err = svc.db.Exec(query,
			t.Tid,
			t.Msisdn,
			t.Pixel,
			endpoint,
			t.CampaignId,
			t.OperatorCode,
			t.CountryCode,
			t.Publisher,
			resp.StatusCode,
		); err != nil {
			dbErrors.Inc()
			addToDBErrors.Inc()

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
				"took":  time.Since(begin),
			}).Info("added pixel transaction")
		}

		query = fmt.Sprintf("UPDATE %ssubscriptions SET "+
			" publisher = $1,  "+
			" pixel_sent = $2,  "+
			" pixel_sent_at = $3  "+
			" WHERE id = $4 ",
			svc.conf.db.TablePrefix)

		begin = time.Now()
		if _, err = svc.db.Exec(query,
			t.Publisher,
			t.Sent,
			time.Now(),
			t.SubscriptionId,
		); err != nil {
			dbErrors.Inc()
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
				"took":  time.Since(begin),
			}).Info("updated subscrption")
		}

		if err == nil {
			addToDbSuccess.Inc()
		}

		log.WithFields(log.Fields{
			"tid":   t.Tid,
			"pixel": t.Pixel,
			"queue": "pixels",
		}).Info("processed")
		msg.Ack(false)
	}
}
