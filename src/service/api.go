package service

import (
	"strconv"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"

	"github.com/vostrok/pixels/src/notifier"
	rec "github.com/vostrok/utils/rec"
)

// does simple thing:
// selects all subscriptions form database where pixel was not sent to publisher
// and pushes to queue
func AddPixelsHandler(r *gin.Engine) {
	rg := r.Group("/api")
	rg.GET("", api)
}

type Params struct {
	Limit int
	Hours int
}

func api(c *gin.Context) {
	limitStr, _ := c.GetQuery("limit")
	limit, err := strconv.Atoi(limitStr)
	if err != nil || limitStr == "" {
		limit = svc.conf.service.Api.DefaultLimit
	}

	hoursStr, _ := c.GetQuery("hours")
	hours, err := strconv.Atoi(hoursStr)
	if err != nil || limitStr == "" {
		hours = svc.conf.service.Api.DefaultBeforeHours
	}

	params := Params{
		Limit: limit,
		Hours: hours,
	}

	records, err := rec.GetNotSentPixels(params.Hours, params.Limit)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	for _, v := range records {
		svc.n.PixelNotify(notifier.Pixel{
			Tid:            v.Tid,
			Msisdn:         v.Msisdn,
			CampaignId:     v.CampaignId,
			SubscriptionId: v.SubscriptionId,
			OperatorCode:   v.OperatorCode,
			CountryCode:    v.CountryCode,
			Pixel:          v.Pixel,
			Publisher:      v.Publisher,
		})
		log.WithFields(log.Fields{
			"tid":   v.Tid,
			"pixel": v.Pixel,
		}).Debug("sent")
	}

	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	c.JSON(200, len(records))
}
