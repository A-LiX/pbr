package main

import (
	"os"
	"publisher-go/internal/services"
	"publisher-go/logger"
	"publisher-go/logging/applogger"
	"runtime"
	"strings"

	"github.com/go-redis/redis"
	"gopkg.in/yaml.v2"
)

type yamlConfig struct {
	Channels []string `yaml:"channels"`
	LogLevel int32    `yaml:"log_level"`
}

func initConfig(path string) (*yamlConfig, error) {
	conf := &yamlConfig{}
	if yumlFp, err := os.Open(path); err != nil {
		return nil, err
	} else {
		err := yaml.NewDecoder(yumlFp).Decode(conf)
		if err != nil {
			return nil, err
		}
	}
	return conf, nil
}

func main() {

	cpuNum := runtime.NumCPU()
	applogger.Info("Got Max CPU Num:%d", cpuNum)

	runtime.GOMAXPROCS(cpuNum)
	applogger.Info("Accupied CPU NUM:%d", cpuNum)

	server := redis.NewClient(&redis.Options{
		Addr: "ec2-35-75-136-145.ap-northeast-1.compute.amazonaws.com:16380",
		//Addr:     "127.0.0.1:6379",
		DB:       0,
		Password: "bitcoin666",
	})

	_, err := server.Ping().Result()
	if err != nil {
		applogger.Error("Redis server connect failed!")
		return
	}

	applogger.Info("Redis server connected!")
	conf, err := initConfig("./config.yaml")
	if err != nil {
		applogger.Debug("fail to open yaml file, please check your file path")
		return
	}
	logger.CurLogLevel = conf.LogLevel

	for _, channel := range conf.Channels {
		elems := strings.Split(channel, "_")
		exName := elems[0]
		symbol := elems[2] + "_" + elems[3]
		dataType := elems[4]
		switch exName {
		case "binance":
			{
				go services.BinanceWsService(server, symbol, dataType)
			}
		case "binanceperpetual":
			{
				go services.BinancePerpetualWsService(server, symbol, dataType)
			}
		case "crypto":
			{
				go services.CryptoWsService(server, symbol, dataType)
			}
		case "cryptoperpetual":
			{
				go services.CryptoPerpetualWsService(server, symbol, dataType)
			}
		}
	}

	select {}
}
