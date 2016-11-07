package service

// CQR handler for pixel_settings table

import (
	"errors"
	"net/http"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
)

type response struct {
	Success bool        `json:"success,omitempty"`
	Err     error       `json:"error,omitempty"`
	Data    interface{} `json:"data,omitempty"`
	Status  int         `json:"-"`
}

func AddCQRHandler(r *gin.Engine) {
	rg := r.Group("/cqr")
	rg.GET("", CQR)
}

func CQR(c *gin.Context) {
	var err error
	r := response{Err: err, Status: http.StatusOK}

	table, exists := c.GetQuery("table")
	if !exists || table == "" {
		table, exists = c.GetQuery("t")
		if !exists || table == "" {
			err := errors.New("Table name required")
			r.Status = http.StatusBadRequest
			r.Err = err
			render(r, c)
			return
		}
	}

	switch {
	case strings.Contains(table, "pixel_settings"):
		if err := memPixels.pixels.Reload(); err != nil {
			r.Success = false
			r.Status = http.StatusInternalServerError
			log.WithField("error", err.Error()).Error("pixels reload failed")
		} else {
			r.Success = true
		}
	default:
		r.Success = false
		r.Status = http.StatusInternalServerError
		log.WithField("table", table).Error("unknown table")
	}

	render(r, c)
	return
}

func render(msg response, c *gin.Context) {
	if msg.Err != nil {
		c.Header("Error", msg.Err.Error())
		c.Error(msg.Err)
	}
	c.JSON(msg.Status, msg)
}
