package service

import (
	"database/sql"
	"fmt"
	"sync"
	"time"

	"github.com/Sirupsen/logrus"
	log "github.com/Sirupsen/logrus"

	"github.com/vostrok/db"
)

var memPixels *inMemPixel

type inMemPixel struct {
	dbConn *sql.DB
	dbConf db.DataBaseConfig
	pixels *PixelSettings
}

//CREATE TABLE public.xmp_pixel_settings (
//id SERIAL PRIMARY KEY ,
//operator_code INTEGER NOT NULL DEFAULT 0,
//country_code INTEGER NOT NULL DEFAULT 0,
//publisher VARCHAR(511) NOT NULL DEFAULT '',
//endpoint VARCHAR(2047) NOT NULL DEFAULT '',
//pixels_enabled BOOLEAN NOT NULL DEFAULT false,
//pixels_ratio INT NOT NULL DEFAULT 0
//);

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
	Map map[int64]PixelSetting
}
type PixelSetting struct {
	Id           int64
	OperatorCode int64
	Endpoint     string
	Enabled      bool
	Ratio        int
}

func (ps *PixelSettings) Reload() (err error) {
	ps.Lock()
	defer ps.Unlock()

	begin := time.Now()
	defer func(err error) {
		errStr := ""
		if err != nil {
			errStr = err.Error()
		}
		log.WithFields(log.Fields{
			"error": errStr,
			"took":  time.Since(begin),
		}).Debug("pixels reload")
	}(err)

	query := fmt.Sprintf("SELECT "+
		"id, "+
		"operator_code, "+
		"endpoint, "+
		"enabled, "+
		"ratio "+
		"FROM %spixel_settings",
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
			&p.OperatorCode,
			&p.Endpoint,
			&p.Enabled,
			&p.Ratio,
		); err != nil {
			err = fmt.Errorf("rows.Scan: %s", err.Error())
			return
		}
		records = append(records, p)
	}
	if rows.Err() != nil {
		err = fmt.Errorf("rows.Err: %s", err.Error())
		return
	}

	ps.Map = make(map[string]PixelSetting, len(records))
	for _, p := range records {
		memPixels.pixels.Map[p.Id] = p
	}
	return nil
}
