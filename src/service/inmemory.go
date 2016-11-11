package service

// im-memory pixel settings
// kept by key fmt.Sprintf("%d-%d-%s", ps.CampaignId, ps.OperatorCode, ps.Publisher)

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
	"strings"
)

var memPixels *inMemPixel

type inMemPixel struct {
	dbConn *sql.DB
	dbConf db.DataBaseConfig
	pixels *PixelSettings
}

func Get() *PixelSettings {
	return memPixels.pixels
}

func initInMemory(conf db.DataBaseConfig) {
	logrus.SetLevel(logrus.DebugLevel)

	memPixels = &inMemPixel{
		dbConn: db.Init(conf),
		dbConf: conf,
		pixels: &PixelSettings{},
	}

	if err := memPixels.pixels.Reload(); err != nil {
		logrus.WithField("error", err.Error()).Fatal("reload pixels failed")
	}

}

type PixelSettings struct {
	sync.RWMutex
	ByKey map[string]*PixelSetting
}

type PixelSetting struct {
	Id           int64
	CampaignId   int64
	OperatorCode int64
	Publisher    string
	Endpoint     string
	Timeout      int
	Enabled      bool
	Ratio        int
	count        int
}

func (ps *PixelSetting) Ignore() bool {
	ps.count = ps.count + 1
	if ps.count == ps.Ratio {
		ps.count = 0
		return false
	}
	return true
}

func (ps *PixelSetting) key() string {
	return strings.ToLower(fmt.Sprintf("%d-%d-%s", ps.CampaignId, ps.OperatorCode, ps.Publisher))
}

func (ps *PixelSettings) Reload() (err error) {
	ps.Lock()
	defer ps.Unlock()

	begin := time.Now()
	defer func(err error) {
		fields := log.Fields{
			"took": time.Since(begin),
		}
		if err != nil {
			fields["error"] = err.Error()
		}
		log.WithFields(fields).Debug("pixels reload")
	}(err)

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"id_campaign, "+
		"operator_code, "+
		"publisher, "+
		"endpoint, "+
		"timeout, "+
		"enabled, "+
		"ratio "+
		"FROM %spixel_settings "+
		"WHERE enabled = true",
		memPixels.dbConf.TablePrefix)

	var rows *sql.Rows
	rows, err = memPixels.dbConn.Query(query)
	if err != nil {
		err = fmt.Errorf("db.Query: %s, query: %s", err.Error(), query)
		return
	}
	defer rows.Close()

	var records []PixelSetting
	for rows.Next() {
		p := PixelSetting{}

		if err = rows.Scan(
			&p.Id,
			&p.CampaignId,
			&p.OperatorCode,
			&p.Publisher,
			&p.Endpoint,
			&p.Timeout,
			&p.Enabled,
			&p.Ratio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		p.count = 0
		records = append(records, p)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	ps.ByKey = make(map[string]*PixelSetting, len(records))
	for _, p := range records {
		memPixels.pixels.ByKey[p.key()] = &p
	}
	log.WithField("pixels", fmt.Sprintf("%#v", memPixels.pixels.ByKey)).Debug("loaded pixels")
	return nil
}
