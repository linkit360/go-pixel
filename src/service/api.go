package service

import (
	"fmt"
	"strconv"
	"time"

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

	count, err := GetPixels(params)
	if err != nil {
		c.JSON(500, err.Error())
		return
	}
	c.JSON(200, count)
}

// get not sent pixels
func GetPixels(p Params) (int, error) {
	var records []rec.Record

	log.WithFields(log.Fields{
		"count":  len(records),
		"params": fmt.Sprintf("%#v", p),
	}).Debug("get pixels")

	begin := time.Now()
	defer func() {
		log.WithFields(log.Fields{
			"took": time.Since(begin),
		}).Debug("get pixels")
	}()
	query := fmt.Sprintf("SELECT "+
		"tid, "+
		"msisdn, "+
		"id_campaign, "+
		"id, "+
		"operator_code, "+
		"country_code, "+
		"pixel, "+
		"publisher "+
		" FROM %ssubscriptions "+
		" WHERE pixel != '' "+
		" AND pixel_sent = false "+
		"AND result NOT IN ('', 'postpaid', 'blacklisted', 'rejected', 'canceled')",
		svc.conf.db.TablePrefix)

	if p.Hours > 0 {
		query = query +
			fmt.Sprintf(" AND (CURRENT_TIMESTAMP - %d * INTERVAL '1 hour' ) > created_at ", p.Hours)
	}
	query = query + fmt.Sprintf(" ORDER BY id ASC LIMIT %d", p.Limit)

	rows, err := svc.db.Query(query)
	if err != nil {
		return 0, fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
	}
	defer rows.Close()

	for rows.Next() {
		record := rec.Record{}

		if err := rows.Scan(
			&record.Tid,
			&record.Msisdn,
			&record.CampaignId,
			&record.SubscriptionId,
			&record.OperatorCode,
			&record.CountryCode,
			&record.Pixel,
			&record.Publisher,
		); err != nil {
			return 0, fmt.Errorf("rows.Scan: %s", err.Error())
		}
		records = append(records, record)
	}
	if rows.Err() != nil {
		return 0, fmt.Errorf("row.Err: %s", err.Error())
	}

	for _, v := range records {
		go svc.n.PixelNotify(notifier.Pixel{
			Tid:            v.Tid,
			Msisdn:         v.Msisdn,
			CampaignId:     v.CampaignId,
			SubscriptionId: v.SubscriptionId,
			OperatorCode:   v.OperatorCode,
			CountryCode:    v.CountryCode,
			Pixel:          v.Pixel,
			Publisher:      v.Publisher,
		})
	}

	return len(records), nil
}
