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
		var restoredPixel notifier.Pixel
		var begin time.Time
		log.WithFields(log.Fields{
			"q":    svc.conf.service.RestorePixels.Name,
			"body": string(msg.Body),
		}).Debug("start process")

		var e EventNotifyPixel
		if err := json.Unmarshal(msg.Body, &e); err != nil {
			Restore.Dropped.Inc()
			Restore.Errors.Inc()
			Errors.Inc()
			log.WithFields(log.Fields{
				"error": err.Error(),
				"msg":   "dropped",
				"q":     svc.conf.service.RestorePixels.Name,
				"pixel": string(msg.Body),
			}).Error("consume pixel")

			goto ack
		}

		t = e.EventData
		if t.Msisdn == "" {
			Restore.Dropped.Inc()
			Restore.Empty.Inc()
			Restore.Errors.Inc()
			Errors.Inc()
			log.WithFields(log.Fields{
				"error": "no msisdn",
				"msg":   "dropped",
				"q":     svc.conf.service.RestorePixels.Name,
				"body":  string(msg.Body),
			}).Error("cann't process")
			goto ack
		}
		if t.CampaignId == 0 {
			Restore.Dropped.Inc()
			Restore.Empty.Inc()
			Restore.Errors.Inc()
			Errors.Inc()
			log.WithFields(log.Fields{
				"error": "no campaign id",
				"msg":   "dropped",
				"q":     svc.conf.service.RestorePixels.Name,
				"body":  string(msg.Body),
			}).Error("cann't process")
			goto ack
		}

		begin = time.Now()
		r, err = rec.GetBufferPixelByCampaignId(t.CampaignId)
		if err != nil {
			Restore.Dropped.Inc()
			Restore.BufferPixelNotFound.Inc()
			Restore.Errors.Inc()
			Errors.Inc()
			log.WithFields(log.Fields{
				"error":       err.Error(),
				"msg":         "dropped",
				"campaign_id": t.CampaignId,
				"service_id":  t.ServiceId,
				"q":           svc.conf.service.RestorePixels.Name,
				"body":        string(msg.Body),
			}).Warn("cann't process")
			goto ack
		} else {
			Restore.GetBufferPixelDuration.Observe(time.Since(begin).Seconds())
		}

		restoredPixel = notifier.Pixel{
			Tid:            t.Tid,
			Msisdn:         t.Msisdn,
			CampaignId:     t.CampaignId,
			ServiceId:      r.ServiceId,
			SubscriptionId: t.SubscriptionId,
			OperatorCode:   t.OperatorCode,
			CountryCode:    t.CountryCode,
			Pixel:          r.Pixel,
		}
		log.WithFields(log.Fields{
			"campaign_id": t.CampaignId,
			"service_id":  r.ServiceId,
			"q":           svc.conf.service.RestorePixels.Name,
		}).Debug("got restored pixel")

		if err := svc.n.PixelNotify(restoredPixel); err != nil {
			Restore.Errors.Inc()
			Errors.Inc()
			log.WithFields(log.Fields{
				"tid":   t.Tid,
				"error": err.Error(),
			}).Error("sent")
			msg.Nack(false, true)
			continue
		} else {
			Restore.Success.Inc()
			log.WithFields(log.Fields{
				"tid":    t.Tid,
				"oldtid": r.Tid,
			}).Debug("sent")

			if err := svc.n.PixelRemoveBufferedNotify(restoredPixel); err != nil {
				Restore.Errors.Inc()
				Errors.Inc()
				log.WithFields(log.Fields{
					"error": err.Error(),
					"tid":   t.Tid,
					"pixel": t.Pixel,
				}).Warn("remove sent")
			}
		}

	ack:
		if err := msg.Ack(false); err != nil {
			Errors.Inc()
			log.WithFields(log.Fields{
				"tid":   e.EventData.Tid,
				"error": err.Error(),
			}).Error("cannot ack")
			time.Sleep(time.Second)
			goto ack
		}
	}
}
