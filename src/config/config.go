package config

import (
	"flag"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/jinzhu/configor"

	"fmt"
	"github.com/vostrok/db"
	"github.com/vostrok/rabbit"
)

type ServerConfig struct {
	Port         string `default:"50308" yaml:"port"`
	Queue        string `default:"pixels" yaml:"queue"`
	ThreadsCount int    `default:"1" yaml:"threads_count"`
	Env          string `default:"dev" yaml:"env"`
}

type AppConfig struct {
	Server   ServerConfig          `yaml:"server"`
	Consumer rabbit.ConsumerConfig `yaml:"consumer"`
	DbConf   db.DataBaseConfig     `yaml:"db"`
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

	appConfig.Server.Port = envString("PORT", appConfig.Server.Port)
	appConfig.Consumer.Connection.Host = envString("RBMQ_HOST", appConfig.Consumer.Connection.Host)

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
