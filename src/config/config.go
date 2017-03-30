package config

import (
	"flag"
	"fmt"
	"os"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	inmem_client "github.com/linkit360/go-inmem/rpcclient"
	"github.com/linkit360/go-pixel/src/notifier"
	"github.com/linkit360/go-utils/amqp"
	"github.com/linkit360/go-utils/config"
	"github.com/linkit360/go-utils/db"
)

type ServerConfig struct {
	Port string `default:"50308" yaml:"port"`
	Env  string `default:"dev" yaml:"env"`
}
type ServiceConfig struct {
	SettingType   string                    `yaml:"setting_type"`
	Pixels        config.ConsumeQueueConfig `yaml:"pixels"`
	RestorePixels config.ConsumeQueueConfig `yaml:"restore_pixels"`
	Api           APIConfig                 `yaml:"api"`
}
type APIConfig struct {
	DefaultLimit       int `default:"500" yaml:"limit"`
	DefaultBeforeHours int `default:"0" yaml:"hours"`
}

type AppConfig struct {
	AppName           string                    `yaml:"app_name"`
	Service           ServiceConfig             `yaml:"service"`
	Server            ServerConfig              `yaml:"server"`
	InMemClientConfig inmem_client.ClientConfig `yaml:"inmem_client"`
	Consumer          amqp.ConsumerConfig       `yaml:"consumer"`
	DbConf            db.DataBaseConfig         `yaml:"db"`
	Notifier          notifier.NotifierConfig   `yaml:"notifier"`
}

func LoadConfig() AppConfig {
	cfg := flag.String("config", "dev/pixels.yml", "configuration yml file")
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
