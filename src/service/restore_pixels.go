package service

// get campaign id
// get pixel by campaign
// send pixel

import (
	"encoding/json"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/streadway/amqp"

	"github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/utils/rec"
)

func processRestorePixels(deliveries <-chan amqp.Delivery) {
	for msg := range deliveries {

		var r rec.Record
		var err error
		var t notifier.Pixel

		log.WithFields(log.Fields{
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyPixel
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			dropped.Inc()
			Errors.Inc()

			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"pixel": string(msg.Body),
			}).Error("consume pixel")

			goto ack
		}

		t = e.EventData
		if t.Msisdn == "" {
			log.WithFields(log.Fields{
				"error": "no msisdn",
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("cann't process")
			goto ack
		}
		if t.CampaignId == 0 {
			log.WithFields(log.Fields{
				"error": "no campaign id",
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("cann't process")
			goto ack
		}

		r, err = rec.GetSubscriptionByMsisdn(t.Msisdn)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("cann't process")
			goto ack
		}

		r, err = rec.GetBufferPixelByCampaignId(t.CampaignId)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"body":  string(msg.Body),
			}).Error("cann't process")
			goto ack
		}

		if err := svc.n.PixelNotify(notifier.Pixel{
			Tid:            r.Tid,
			Msisdn:         r.Msisdn,
			CampaignId:     r.CampaignId,
			SubscriptionId: r.SubscriptionId,
			OperatorCode:   r.OperatorCode,
			CountryCode:    r.CountryCode,
			Pixel:          t.Pixel,
		}); err != nil {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": err.Error(),
			}).Error("sent")
			msg.Nack(false, true)
			continue
		} else {
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"pixel": t.Pixel,
			}).Debug("sent")

			if err := svc.n.PixelRemoveBufferedNotify(notifier.Pixel{
				Tid:            r.Tid,
				Msisdn:         r.Msisdn,
				CampaignId:     r.CampaignId,
				SubscriptionId: r.SubscriptionId,
				OperatorCode:   r.OperatorCode,
				CountryCode:    r.CountryCode,
				Pixel:          t.Pixel,
			}); err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
					"tid":   t.Tid,
					"pixel": t.Pixel,
				}).Debug("remove sent")
			}
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
