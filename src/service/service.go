package service

// Pixels is a callback interface for CPA publishers
// * Get from queue pixels and send to publisher
// * API interface to get from database and send to publisher

import (
	"database/sql"

	log "github.com/Sirupsen/logrus"
	amqp_driver "github.com/streadway/amqp"

	"github.com/gin-gonic/gin"
	"github.com/vostrok/db"
	"github.com/vostrok/pixels/src/config"
	"github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/rabbit"
)

var svc Service

type Service struct {
	consumer *rabbit.Consumer
	records  <-chan amqp_driver.Delivery
	db       *sql.DB
	m        Metrics
	n        notifier.Notifier
	conf     Config
}
type Config struct {
	service  config.ServiceConfig
	server   config.ServerConfig
	db       db.DataBaseConfig
	consumer rabbit.ConsumerConfig
	notifier notifier.NotifierConfig
}

func InitService(
	svcConf config.ServiceConfig,
	serverConfig config.ServerConfig,
	dbConf db.DataBaseConfig,
	consumerConfig rabbit.ConsumerConfig,
	notifConf notifier.NotifierConfig,
) {
	log.SetLevel(log.DebugLevel)
	svc.conf = Config{
		service:  svcConf,
		server:   serverConfig,
		db:       dbConf,
		consumer: consumerConfig,
		notifier: notifConf,
	}

	svc.db = db.Init(dbConf)
	initInMemory(dbConf)
	svc.m = initMetrics()
	svc.n = notifier.NewNotifierService(notifConf)

	// process consumer
	svc.consumer = rabbit.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	var err error
	svc.records, err = svc.consumer.AnnounceQueue(serverConfig.Queue, serverConfig.Queue)
	if err != nil {
		log.WithFields(log.Fields{
			"queue": serverConfig.Queue,
			"error": err.Error(),
		}).Fatal("rbmq consumer: AnnounceQueue")
	}
	go svc.consumer.Handle(svc.records, process, serverConfig.ThreadsCount, serverConfig.Queue, serverConfig.Queue)
}

func AddPublisherHandler(r *gin.Engine) {
	rg := r.Group("/publisher")
	rg.GET("", status200ok)
}

func status200ok(c *gin.Context) {
	c.Writer.WriteHeader(200)
	c.Writer.Write([]byte("ok"))
}
