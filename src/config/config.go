package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	inmem_client "github.com/vostrok/inmem/rpcclient"
	"github.com/vostrok/pixels/src/notifier"
	"github.com/vostrok/utils/amqp"
	"github.com/vostrok/utils/config"
	"github.com/vostrok/utils/db"
)

type ServerConfig struct {
	Port string `default:"50308" yaml:"port"`
	Env  string `default:"dev" yaml:"env"`
}
type ServiceConfig struct {
	Delay int                       `default:"1" yaml:"delay"`
	Queue config.ConsumeQueueConfig `yaml:"queue"`
	Api   APIConfig                 `yaml:"api"`
}
type APIConfig struct {
	DefaultLimit       int `default:"500" yaml:"limit"`
	DefaultBeforeHours int `default:"0" yaml:"hours"`
}

type AppConfig struct {
	MetricInstancePrefix string                       `yaml:"metric_instance_prefix"`
	AppName              string                       `yaml:"app_name"`
	Service              ServiceConfig                `yaml:"service"`
	Server               ServerConfig                 `yaml:"server"`
	InMemClientConfig    inmem_client.RPCClientConfig `yaml:"inmem_client"`
	Consumer             amqp.ConsumerConfig          `yaml:"consumer"`
	DbConf               db.DataBaseConfig            `yaml:"db"`
	Notifier             notifier.NotifierConfig      `yaml:"notifier"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/appconfig.yml", "configuration yml file")
	flag.Parse()
	var appConfig AppConfig

	if *cfg != "" {
		if err := configor.Load(&appConfig, *cfg); err != nil {
			log.WithField("config", err.Error()).Fatal("config load error")
		}
	}
	if appConfig.AppName == "" {
		log.Fatal("app name must be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.AppName, "-") {
		log.Fatal("app name must be without '-' : it's not a valid metric name")
	}
	if appConfig.MetricInstancePrefix == "" {
		log.Fatal("metric_instance_prefix be defiled as <host>_<name>")
	}
	if strings.Contains(appConfig.MetricInstancePrefix, "-") {
		log.Fatal("metric_instance_prefix be without '-' : it's not a valid metric name")
	}

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Consumer.Conn.Host = envString("RBMQ_HOST", appConfig.Consumer.Conn.Host)

	log.WithField("config", fmt.Sprintf("%#v", appConfig)).Info("Config loaded")
	return appConfig
}

func envString(env, fallback string) string {
	e := os.Getenv(env)
	if e == "" {
		return fallback
	}
	return e
}
