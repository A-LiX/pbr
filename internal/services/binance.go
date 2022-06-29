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

func BinanceWsService(server *redis.Client, symbol string, dataType string) {

	flag.Parse()
	tlsConfig := tls.Config{
		InsecureSkipVerify: true,
	}
	dial := websocket.Dialer{TLSClientConfig: &tlsConfig}

	urlSymbol := strings.ToLower(strings.ReplaceAll(symbol, "_", ""))
	wsUrl := url.URL{
		Scheme: "wss",
		Host:   "stream.binance.com:9443",
	}

	var channelName string
	if strings.Compare(dataType, "depth") == 0 {
		channelName = "binancev2_rs_" + symbol + "_depth"
		wsUrl.Path = "/ws/" + urlSymbol + "@depth20@100ms"
	} else if strings.Compare(dataType, "ticker") == 0 {
		channelName = "binancev2_ws_" + symbol + "_ticker"
		wsUrl.Path = "/ws/" + urlSymbol + "@trade"
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
		applogger.Info("binance %s orderbook stream connected!", symbol)
		go binanceDepthDataParser(server, wsClient, msgChan, channelName, symbol, dataType)
	} else {
		applogger.Info("binance %s transaction stream connected!", symbol)
		go binanceTickerDataParser(server, msgChan, channelName, symbol, dataType)
	}

	for {
		msgType, msgByte, err := wsClient.ReadMessage()

		if err != nil {
			applogger.Error("ReadMessage:%v", err)
			wsClient, _, _ = dial.Dial(wsUrl.String(), nil)
			if strings.Compare(dataType, "depth") == 0 {
				applogger.Info("binance %s orderbook stream connected!", symbol)
				go binanceDepthDataParser(server, wsClient, msgChan, channelName, symbol, dataType)
			} else {
				applogger.Info("binance %s transaction stream connected!", symbol)
				go binanceTickerDataParser(server, msgChan, channelName, symbol, dataType)
			}
		}

		if msgType == websocket.PingMessage {
			applogger.Info("ping", string(msgByte))
			wsClient.WriteMessage(websocket.PongMessage, []byte{})

		} else {
			msgChan <- msgByte
		}
	}
}
func binanceDepthDataParser(server *redis.Client, wsClient *websocket.Conn, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("BinanceDepthDataParser %s started!", symbol)
	var bytes []byte
	data := models.DepthData{}
	data.Symbol = symbol
	data.ExName = "binance"
	data.DataType = "depth"
	data.IO = "ws"

	pingTimer := time.NewTimer(time.Minute * 10)

	for {
		select {
		case bytes = <-msgChan:
			{
				if !gjson.GetBytes(bytes, "lastUpdateId").Exists() {
					applogger.Info("lastUpdateId !exists", string(bytes))
					continue
				}

				data.Lts = time.Now().UnixMilli()
				data.DBts = data.Lts
				data.Data.Id = uint64(gjson.GetBytes(bytes, "lastUpdateId").Num)
				bids := gjson.GetBytes(bytes, "bids").Array()
				for i, value := range bids {
					data.Data.Bids[i].Price, _ = strconv.ParseFloat(value.Array()[0].Str, 64)
					data.Data.Bids[i].Amount, _ = strconv.ParseFloat(value.Array()[1].Str, 64)
				}

				asks := gjson.GetBytes(bytes, "asks").Array()
				for i, value := range asks {
					data.Data.Asks[i].Price, _ = strconv.ParseFloat(value.Array()[0].Str, 64)
					data.Data.Asks[i].Amount, _ = strconv.ParseFloat(value.Array()[1].Str, 64)
				}
				data.Data.DBns = uint64(time.Now().UnixNano())

				jsonBytes, _ := json.Marshal(data)
				logger.Print(logger.DEBUG, "binance", string(jsonBytes))
				server.Publish(channelName, jsonBytes)
			}
		case <-pingTimer.C:
			{
				wsClient.WriteMessage(websocket.PingMessage, []byte{})
				pingTimer.Reset(time.Minute * 10)
			}

		}

	}
}

func binanceTickerDataParser(server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("Binance Ticker Data Parser %s Started!", symbol)
	var bytes []byte
	data := models.TickerData{}
	data.Symbol = symbol
	data.ExName = "binance"
	data.DataType = "ticker"
	data.IO = "ws"

	for {
		bytes = <-msgChan

		data.Lts = time.Now().UnixMilli()
		data.DBts = data.Lts

		data.Data.Tickers[0].Ts = uint64(gjson.GetBytes(bytes, "T").Num)
		data.Data.Tickers[0].Price, _ = strconv.ParseFloat(gjson.GetBytes(bytes, "p").Str, 64)
		data.Data.Tickers[0].Amount, _ = strconv.ParseFloat(gjson.GetBytes(bytes, "q").Str, 64)
		data.Data.Tickers[0].Id = int64(gjson.GetBytes(bytes, "t").Num)
		data.Data.Tickers[0].DBns = uint64(time.Now().UnixNano())

		if gjson.GetBytes(bytes, "m").Bool() {
			data.Data.Tickers[0].Direction = "sell"
		} else {
			data.Data.Tickers[0].Direction = "buy"
		}
		jsonBytes, _ := json.Marshal(data)
		logger.Print(logger.DEBUG, "binance", string(jsonBytes))
		server.Publish(channelName, jsonBytes)
	}
}
