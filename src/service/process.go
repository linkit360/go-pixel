package service

// get queue messages from pixels
// and send them to publisher
// ack all ok in case of any error

import (
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	mid "github.com/linkit360/go-mid/service"
	"github.com/linkit360/go-pixel/src/notifier"
	xmp_api_structs "github.com/linkit360/xmp-api/src/structs"
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

var slimSpotRe = regexp.MustCompile(`16122[0-9a-z]{3}_[0-9a-z]{2}_533_[0-9a-z]{16}`)

func processPixels(deliveries <-chan amqp.Delivery) {
	var client *http.Client
	var resp *http.Response

	for msg := range deliveries {

		var operator xmp_api_structs.Operator
		var ps mid.PixelSetting
		var clientErr error
		var err error
		var statusCode int
		var t notifier.Pixel
		var fields log.Fields
		var pixelKey string

		log.WithFields(log.Fields{
			"q":    svc.conf.service.Pixels.Name,
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyPixel
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			dropped.Inc()
			Errors.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"q":     svc.conf.service.Pixels.Name,
				"pixel": string(msg.Body),
			}).Error("consume pixel")

			goto ack
		}

		t = e.EventData
		if t.Pixel == "" || t.Tid == "" {
			dropped.Inc()
			empty.Inc()
			Errors.Inc()

			log.WithFields(log.Fields{
				"error": "Empty message",
				"msg":   "dropped",
				"q":     svc.conf.service.Pixels.Name,
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
			if slimSpotRe.MatchString(t.Pixel) {
				t.Publisher = "SlimSpot"
			}
			if t.Publisher == "" {
				publishers, err := mid_client.GetAllPublishers()
				if err != nil {
					log.WithFields(log.Fields{
						"error": err.Error(),
						"msg":   "dropped",
						"q":     svc.conf.service.Pixels.Name,
						"pixel": string(msg.Body),
					}).Error("cannot determine publisher")
					goto ack
				}
				for _, publisher := range publishers {
					if publisher.Regex.MatchString(t.Pixel) {
						t.Publisher = publisher.Name
						break
					}
				}
			}
		}

		if t.Publisher == "" {
			dropped.Inc()
			empty.Inc()
			emptyPublisher.Inc()
			Errors.Inc()

			log.WithFields(log.Fields{
				"error": "Empty publisher",
				"msg":   "dropped",
				"q":     svc.conf.service.Pixels.Name,
				"pixel": string(msg.Body),
			}).Error("cannot determine publisher")
			goto ack
		}

		ps = mid.PixelSetting{}
		ps.SetPublisher(t.Publisher)
		ps.SetCampaignCode(t.CampaignCode)
		ps.SetOperatorCode(t.OperatorCode)

		if svc.conf.service.SettingType == "operator" {
			pixelKey = ps.OperatorKey()
		} else if svc.conf.service.SettingType == "campaign_operator" {
			pixelKey = ps.CampaignOperatorKey()
		} else {
			pixelKey = ps.CampaignKey()
		}
		ps, err = mid_client.GetPixelSettingByKeyWithRatio(pixelKey)
		if err != nil {
			err = fmt.Errorf("GetPixelSettingByKey: %s", err.Error())
			dropped.Inc()
			emptySettings.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"msg":   "dropped",
			}).Error("can't process pixel")
			goto ack
		}
		if !ps.Enabled {
			dropped.Inc()

			log.WithFields(log.Fields{
				"pixel":    t.Pixel,
				"q":        svc.conf.service.Pixels.Name,
				"tid":      t.Tid,
				"pixelKey": pixelKey,
			}).Info("send pixel disabled!")
			goto ack
		}
		if ps.SkipPixelSend {
			dropped.Inc()
			log.WithFields(log.Fields{
				"pixel":   t.Pixel,
				"tid":     t.Tid,
				"q":       svc.conf.service.Pixels.Name,
				"msg":     "dropped",
				"setting": fmt.Sprintf("%#v", ps),
			}).Info("ratio: must skip")
			goto ack
		}
		log.WithFields(log.Fields{
			"pixel": t.Pixel,
			"tid":   t.Tid,
			"ratio": ps.Ratio,
			"q":     svc.conf.service.Pixels.Name,
			"count": ps.Count,
		}).Info("ratio rule: passed")

		t.Sent = false

		if svc.conf.server.Env == "dev" {
			t.Endpoint = "http://localhost:50309/publisher?" +
				"aff_sub=%pixel%&" +
				"msisdn=%msisdn%&" +
				"trxid=%trxid%&" +
				"trxtime=%time%&" +
				"country=%country_name%&" +
				"operator=%operator_name%&" +
				"service=%service_id%&" +
				"code=%operator_code%"
		} else {
			t.Endpoint = ps.Endpoint
		}

		t.Endpoint = strings.Replace(t.Endpoint, "%service_id%", t.ServiceCode, -1)
		t.Endpoint = strings.Replace(t.Endpoint, "%operator_code%", strconv.FormatInt(t.OperatorCode, 10), 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%pixel%", t.Pixel, 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%msisdn%", t.Msisdn, 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%trxid%", fmt.Sprintf("%d%s", time.Now().Unix(), t.Msisdn), 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%time%", time.Now().UTC().Format("2006-01-02+15:04:05"), 1)

		operator, err = mid_client.GetOperatorByCode(t.OperatorCode)
		if err != nil {
			err = fmt.Errorf("GetPixelSettingByKey: %s", err.Error())
			dropped.Inc()
			emptySettings.Inc()

			log.WithFields(log.Fields{
				"pixel": t.Pixel,
				"tid":   t.Tid,
				"q":     svc.conf.service.Pixels.Name,
				"error": err.Error(),
			}).Error("can't get operator name by code")
			goto ack
		}
		t.Endpoint = strings.Replace(t.Endpoint, "%operator_name%", operator.Name, 1)
		t.Endpoint = strings.Replace(t.Endpoint, "%country_name%", operator.CountryName, 1)

		client = &http.Client{
			Timeout: time.Duration(ps.Timeout) * time.Second,
		}
		resp, clientErr = client.Get(t.Endpoint)
		if clientErr != nil || resp.StatusCode != 200 {
			publisherError.Inc()
		} else {
			Success.Inc()
		}
		log.WithFields(log.Fields{
			"tid":  t.Tid,
			"q":    svc.conf.service.Pixels.Name,
			"url":  t.Endpoint,
			"resp": fmt.Sprintf("%#v", resp),
		}).Debug("response")

		fields = log.Fields{
			"pixel":    t.Pixel,
			"tid":      t.Tid,
			"campaign": t.CampaignCode,
			"url":      t.Endpoint,
			"q":        svc.conf.service.Pixels.Name,
		}
		if resp != nil {
			statusCode = resp.StatusCode
			t.ResponseCode = resp.StatusCode
			if resp.StatusCode == 200 {
				t.Sent = true
			}
			fields["code"] = resp.StatusCode
			fields["sent"] = t.Sent
		}

		if clientErr != nil {
			fields["error"] = clientErr.Error()
		}

		log.WithFields(fields).Debug("response")

		if err := svc.n.PixelTransactionNotify(t); err != nil {
			Errors.Inc()

			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"error": err.Error(),
				"q":     svc.conf.service.Pixels.Name,
			}).Error("cannot send pixel transaction")
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"q":     svc.conf.service.Pixels.Name,
			}).Info("sent pixel transaction")
		}

		if clientErr != nil || statusCode != 200 {
			goto ack
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

		if err := svc.n.PixelUpdateSubscriptionNotify(t); err != nil {
			Errors.Inc()

			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
				"q":     svc.conf.service.Pixels.Name,
				"error": err.Error(),
			}).Error("cannot send pixel update subscription")
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"q":     svc.conf.service.Pixels.Name,
				"pixel": t.Pixel,
			}).Info("sent pixel update subscription")
		}
	}
}
