package service

// Pixels is a callback interface for CPA publishers
// * Get from queue pixels and send to publisher
// * API interface to get from database and send to publisher

import (
	"database/sql"

	"github.com/gin-gonic/gin"
	log "github.com/sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	mid_client "github.com/linkit360/go-mid/rpcclient"
	"github.com/linkit360/go-pixel/src/config"
	"github.com/linkit360/go-pixel/src/notifier"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/db"
	"github.com/linkit360/go-utils/rec"
)

var svc Service

type Service struct {
	consumer              *amqp.Consumer
	restorePixelsConsumer *amqp.Consumer
	n                     notifier.Notifier
	sendPixelsCh          <-chan amqp_driver.Delivery
	restorePixelsCh       <-chan amqp_driver.Delivery
	db                    *sql.DB
	conf                  Config
}
type Config struct {
	service  config.ServiceConfig
	server   config.ServerConfig
	db       db.DataBaseConfig
	consumer amqp.ConsumerConfig
	notifier notifier.NotifierConfig
}

func InitService(
	appName string,
	svcConf config.ServiceConfig,
	serverConfig config.ServerConfig,
	midConf mid_client.ClientConfig,
	dbConf db.DataBaseConfig,
	consumerConfig amqp.ConsumerConfig,
	notifierConf notifier.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	svc.conf = Config{
		service:  svcConf,
		server:   serverConfig,
		db:       dbConf,
		consumer: consumerConfig,
		notifier: notifierConf,
	}

	initMetrics(appName)

	rec.Init(dbConf)

	if err := mid_client.Init(midConf); err != nil {
		log.WithField("error", err.Error()).Fatal("mid dial error")
	}

	svc.n = notifier.NewNotifierService(notifierConf)

	// create consumer
	svc.consumer = amqp.NewConsumer(
		consumerConfig,
		svc.conf.service.Pixels.Name,
		svc.conf.service.Pixels.PrefetchCount,
	)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	// queue for pixels requests
	amqp.InitQueue(
		svc.consumer,
		svc.sendPixelsCh,
		processPixels,
		svc.conf.service.Pixels.ThreadsCount,
		svc.conf.service.Pixels.Name,
		svc.conf.service.Pixels.Name,
	)

	// restore pixels consumer
	svc.restorePixelsConsumer = amqp.NewConsumer(
		consumerConfig,
		svc.conf.service.RestorePixels.Name,
		svc.conf.service.RestorePixels.PrefetchCount,
	)
	if err := svc.restorePixelsConsumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	// queue for pixels requests
	amqp.InitQueue(
		svc.restorePixelsConsumer,
		svc.restorePixelsCh,
		processRestorePixels,
		svc.conf.service.RestorePixels.ThreadsCount,
		svc.conf.service.RestorePixels.Name,
		svc.conf.service.RestorePixels.Name,
	)
}

func AddPublisherHandler(r *gin.Engine) {
	rg := r.Group("/publisher")
	rg.GET("", status200ok)
}

func status200ok(c *gin.Context) {
	c.Writer.WriteHeader(200)
	c.Writer.Write([]byte("ok"))
}
