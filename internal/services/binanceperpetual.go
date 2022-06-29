package services

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"net/url"
	"publisher-go/internal/models"
	"publisher-go/logger"
	"publisher-go/logging/applogger"
	"time"

	"strconv"
	"strings"

	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

func BinancePerpetualWsService(server *redis.Client, symbol string, dataType string) {

	flag.Parse()
	tlsConfig := tls.Config{
		InsecureSkipVerify: true,
	}
	dial := websocket.Dialer{TLSClientConfig: &tlsConfig}

	urlSymbol := strings.ToLower(strings.ReplaceAll(symbol, "_", ""))
	wsUrl := url.URL{
		Scheme: "wss",
		Host:   "fstream.binance.com",
	}

	var channelName string
	if strings.Compare(dataType, "depth") == 0 {
		channelName = "binanceperpetualv2_rs_" + symbol + "_depth"
		wsUrl.Path = "/ws/" + urlSymbol + "@depth20@100ms"
	} else if strings.Compare(dataType, "ticker") == 0 {
		channelName = "binanceperpetualv2_ws_" + symbol + "_ticker"
		wsUrl.Path = "/ws/" + urlSymbol + "@aggTrade"
	}

	applogger.Info("connecting to %s:", wsUrl.String())

	wsClient, _, err := dial.Dial(wsUrl.String(), nil)
	if err != nil {
		applogger.Error("Dial tcp", err)
		return
	}
	defer wsClient.Close()

	msgChan := make(chan []byte)

	if strings.Compare(dataType, "depth") == 0 {
		applogger.Info("binanceperpetual %s orderbook stream connected!", symbol)
		go binanceperpetualDepthDataParser(server, msgChan, channelName, symbol, dataType)
	} else {
		applogger.Info("binanceperpetual %s transaction stream connected!", symbol)
		go binanceperpetualTickerDataParser(server, msgChan, channelName, symbol, dataType)
	}

	for {
		_, msgByte, err := wsClient.ReadMessage()
		if err != nil {
			applogger.Error("ReadMessage:%v", err)
		}

		msgChan <- msgByte
	}
}
func binanceperpetualDepthDataParser(server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("BinanceDepthDataParser %s started!", symbol)
	var bytes []byte
	data := models.DepthData{}
	data.Symbol = symbol
	data.ExName = "binanceperpetual"
	data.DataType = "depth"
	data.IO = "ws"

	for {
		bytes = <-msgChan
		data.Lts = time.Now().UnixMilli()
		data.DBts = data.Lts
		data.Data.Id = uint64(gjson.GetBytes(bytes, "E").Num)
		bids := gjson.GetBytes(bytes, "b").Array()
		for i, value := range bids {
			data.Data.Bids[i].Price, _ = strconv.ParseFloat(value.Array()[0].Str, 64)
			data.Data.Bids[i].Amount, _ = strconv.ParseFloat(value.Array()[1].Str, 64)
		}

		asks := gjson.GetBytes(bytes, "a").Array()
		for i, value := range asks {
			data.Data.Asks[i].Price, _ = strconv.ParseFloat(value.Array()[0].Str, 64)
			data.Data.Asks[i].Amount, _ = strconv.ParseFloat(value.Array()[1].Str, 64)
		}
		data.Data.DBns = uint64(time.Now().UnixNano())

		jsonBytes, _ := json.Marshal(data)
		logger.Print(logger.DEBUG, "binance", string(jsonBytes))
		server.Publish(channelName, jsonBytes)
	}
}

func binanceperpetualTickerDataParser(server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("BinanceTickerDataParser %s started!", symbol)
	var bytes []byte
	data := models.TickerData{}
	data.Symbol = symbol
	data.ExName = "binanceperpetual"
	data.DataType = "ticker"
	data.IO = "ws"

	for {
		bytes = <-msgChan
		dataMap := gjson.ParseBytes(bytes).Map()

		data.Lts = time.Now().UnixMilli()
		data.DBts = data.Lts

		data.Data.Tickers[0].Ts = uint64(dataMap["E"].Num)
		data.Data.Tickers[0].Price, _ = strconv.ParseFloat(dataMap["p"].Str, 64)
		data.Data.Tickers[0].Amount, _ = strconv.ParseFloat(dataMap["q"].Str, 64)
		data.Data.Tickers[0].Id = int64(gjson.GetBytes(bytes, "a").Num)
		data.Data.Tickers[0].DBns = uint64(time.Now().UnixNano())

		if dataMap["m"].Bool() {
			data.Data.Tickers[0].Direction = "sell"
		} else {
			data.Data.Tickers[0].Direction = "buy"
		}
		jsonBytes, _ := json.Marshal(data)
		logger.Print(logger.DEBUG, "binance", string(jsonBytes))
		server.Publish(channelName, jsonBytes)
	}
}
