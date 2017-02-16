package service

// Pixels is a callback interface for CPA publishers
// * Get from queue pixels and send to publisher
// * API interface to get from database and send to publisher

import (
	"database/sql"

	log "github.com/Sirupsen/logrus"
	"github.com/gin-gonic/gin"
	amqp_driver "github.com/streadway/amqp"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/pixels/src/config"
	"github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/db"
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
	inMemConf inmem_client.RPCClientConfig,
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

	svc.db = db.Init(dbConf)

	if err := inmem_client.Init(inMemConf); err != nil {
		log.WithField("error", err.Error()).Fatal("inmem dial error")
	}

	svc.n = notifier.NewNotifierService(notifierConf)

	// create consumer
	svc.consumer = amqp.NewConsumer(
		consumerConfig,
		svc.conf.service.Queue.Name,
		svc.conf.service.Queue.PrefetchCount,
	)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	// queue for pixels requests
	amqp.InitQueue(
		svc.consumer,
		svc.sendPixelsCh,
		processPixels,
		svc.conf.service.Queue.ThreadsCount,
		svc.conf.service.Queue.Name,
		svc.conf.service.Queue.Name,
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
