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
	consumer *amqp.Consumer
	n        notifier.Notifier
	records  <-chan amqp_driver.Delivery
	db       *sql.DB
	conf     Config
}
type Config struct {
	service  config.ServiceConfig
	server   config.ServerConfig
	db       db.DataBaseConfig
	consumer amqp.ConsumerConfig
	notifier notifier.NotifierConfig
}

func InitService(
	svcConf config.ServiceConfig,
	serverConfig config.ServerConfig,
	inMemConf inmem_client.RPCClientConfig,
	dbConf db.DataBaseConfig,
	consumerConfig amqp.ConsumerConfig,
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

	initMetrics()

	svc.db = db.Init(dbConf)

	inmem_client.Init(inMemConf)

	svc.n = notifier.NewNotifierService(notifConf)

	// create consumer
	svc.consumer = amqp.NewConsumer(consumerConfig)
	if err := svc.consumer.Connect(); err != nil {
		log.Fatal("rbmq consumer connect:", err.Error())
	}

	// queue for pixels requests
	amqp.InitQueue(
		svc.consumer,
		svc.records,
		processPixels,
		serverConfig.ThreadsCount,
		serverConfig.Queue,
		serverConfig.Queue,
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
