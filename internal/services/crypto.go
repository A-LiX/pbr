package services

import (
	"crypto/tls"
	"encoding/json"
	"flag"
	"net/url"
	"publisher-go/internal/models"
	"publisher-go/logger"
	"publisher-go/logging/applogger"
	"strings"
	"time"

	"github.com/go-redis/redis"
	"github.com/gorilla/websocket"
	"github.com/tidwall/gjson"
)

var CRYPTO_SPOT_WEBSOCKET_SCHEME string = "wss"
var CRYPTO_SPOT_WEBSOCKET_HOST string = "stream.crypto.com"
var CRYPTO_SPOT_WEBSOCKET_PATH string = "/v2/market"

func CryptoWsService(server *redis.Client, symbol string, dataType string) {

	flag.Parse()
	tlsConfig := tls.Config{
		InsecureSkipVerify: true,
	}
	dial := websocket.Dialer{TLSClientConfig: &tlsConfig}

	url := url.URL{
		Scheme: CRYPTO_SPOT_WEBSOCKET_SCHEME,
		Host:   CRYPTO_SPOT_WEBSOCKET_HOST,
		Path:   CRYPTO_SPOT_WEBSOCKET_PATH,
	}
	logger.Print(logger.INFO, "Crypto|WebSocket", "Connecting to "+url.String()+":")

	wsClient, _, err := dial.Dial(url.String(), nil)

	if err != nil {
		return
	}
	defer wsClient.Close()

	paramMap := make(map[string]interface{})
	subMap := make(map[string]interface{})
	subMap["id"] = 11
	subMap["method"] = "subscribe"
	if strings.Compare(dataType, "depth") == 0 {
		paramMap["channels"] = []string{"book." + symbol + ".10"}
	} else {
		paramMap["channels"] = []string{"trade." + symbol}
	}
	subMap["params"] = paramMap
	subMap["nonce"] = time.Now().UnixMilli()

	subBuf, _ := json.Marshal(subMap)
	err = wsClient.WriteMessage(websocket.TextMessage, subBuf)

	var channelName string

	msgChan := make(chan []byte)
	if strings.Compare(dataType, "depth") == 0 {
		applogger.Info("binance %s orderbook stream connected!", symbol)
		channelName = "cryptov2_rs_" + symbol + "_depth"
		go cryptoDepthDataParser(wsClient, server, msgChan, channelName, symbol, dataType)
	} else {
		applogger.Info("binance %s transaction stream connected!", symbol)
		channelName = "cryptov2_ws_" + symbol + "_ticker"
		go cryptoTickerDataParser(wsClient, server, msgChan, channelName, symbol, dataType)
	}

	for {
		_, msgByte, err := wsClient.ReadMessage()

		if err != nil {
			applogger.Error("ReadMessage:%v", err)
			wsClient, _, err = dial.Dial(url.String(), nil)

			if err != nil {
				applogger.Info("crypto %s  stream disconnected!", symbol)
				break
			}

			if strings.Compare(dataType, "depth") == 0 {
				applogger.Info("crypto %s orderbook stream re-connected!", symbol)
				go cryptoDepthDataParser(wsClient, server, msgChan, channelName, symbol, dataType)
			} else {
				applogger.Info("crypto %s transaction stream re-connected!", symbol)
				go cryptoTickerDataParser(wsClient, server, msgChan, channelName, symbol, dataType)
			}

			continue
		}

		msgChan <- msgByte
	}
}
func cryptoDepthDataParser(wsClient *websocket.Conn, server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("CryptoDepthDataParser %s started!", symbol)
	var bytes []byte
	data := models.DepthData{}
	data.Symbol = symbol
	data.ExName = "crypto"
	data.DataType = "depth"
	data.IO = "ws"

	respond := make(map[string]interface{})
	respond["method"] = "public/respond-heartbeat"

	for {
		bytes = <-msgChan
		jsonMap := gjson.ParseBytes(bytes).Map()
		if jsonMap["result"].Exists() {

			data.Lts = time.Now().UnixMilli()
			jsonData := gjson.GetBytes(bytes, "result").Map()["data"].Array()
			data.Data.Ts = uint64(jsonData[0].Map()["t"].Num)
			data.Data.Id = data.Data.Ts

			asks := jsonData[0].Map()["asks"].Array()
			for i, value := range asks {
				data.Data.Asks[i].Price = value.Array()[0].Float()
				data.Data.Asks[i].Amount = value.Array()[1].Float()
			}

			bids := jsonData[0].Map()["bids"].Array()
			for i, value := range bids {
				data.Data.Bids[i].Price = value.Array()[0].Float()
				data.Data.Bids[i].Amount = value.Array()[1].Float()
			}
			data.Data.DBns = uint64(time.Now().UnixNano())

			jsonBytes, _ := json.Marshal(data)
			logger.Print(logger.DEBUG, "crypto", string(jsonBytes))
			server.Publish(channelName, jsonBytes)
		} else if strings.Compare(jsonMap["method"].Str, "public/heartbeat") == 0 {
			respond["id"] = gjson.GetBytes(bytes, "id").Int()
			jBuf, _ := json.Marshal(respond)
			_ = wsClient.WriteMessage(websocket.TextMessage, jBuf)
		} else {
			applogger.Error("err maker:%s", string(bytes))
		}

	}
}

func cryptoTickerDataParser(wsClient *websocket.Conn, server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("CryptoTickerDataParser %s started!", symbol)
	var bytes []byte
	data := models.TickerData{}
	data.Symbol = symbol
	data.ExName = "crypto"
	data.DataType = "ticker"
	data.IO = "ws"

	respond := make(map[string]interface{})
	respond["method"] = "public/respond-heartbeat"
	for {
		bytes = <-msgChan
		jsonMap := gjson.ParseBytes(bytes).Map()
		if jsonMap["result"].Exists() {
			data.Lts = time.Now().UnixMilli()
			data.DBts = data.Lts
			trade := jsonMap["data"].Array()[0].Map()

			data.Data.Tickers[0].Ts = uint64(trade["t"].Num)
			data.Data.Tickers[0].Price = trade["p"].Float()
			data.Data.Tickers[0].Amount = trade["q"].Float()
			data.Data.Tickers[0].Direction = strings.ToLower(trade["s"].Str)
			data.Data.Tickers[0].Id = int64(trade["d"].Num)
			data.Data.Tickers[0].DBns = uint64(time.Now().UnixNano())

			jsonBytes, _ := json.Marshal(data)
			logger.Print(logger.DEBUG, "crypto", string(jsonBytes))
			server.Publish(channelName, jsonBytes)
		} else if strings.Compare(gjson.GetBytes(bytes, "method").Str, "public/heartbeat") == 0 {
			respond["id"] = gjson.GetBytes(bytes, "id").Int()
			jBuf, _ := json.Marshal(respond)
			_ = wsClient.WriteMessage(websocket.TextMessage, jBuf)
		} else {
			applogger.Error("trouble maker:%s", string(bytes))
		}
	}
}
