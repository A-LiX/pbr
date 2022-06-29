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

func CryptoPerpetualWsService(server *redis.Client, symbol string, dataType string) {

	flag.Parse()
	tlsConfig := tls.Config{
		InsecureSkipVerify: true,
	}
	dial := websocket.Dialer{TLSClientConfig: &tlsConfig}

	url := url.URL{
		Scheme: "wss",
		Host:   "deriv-stream.crypto.com",
		Path:   "/v1/market",
	}
	logger.Print(logger.INFO, "Crypto|WebSocket", "Connecting to "+url.String()+"...")

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
		paramMap["channels"] = []string{"book." + strings.ReplaceAll(symbol, "_", "") + "-PERP" + ".20"}
	} else {
		paramMap["channels"] = []string{"trade." + strings.ReplaceAll(symbol, "_", "") + "-PERP"}
	}
	subMap["params"] = paramMap

	subBuf, _ := json.Marshal(subMap)
	err = wsClient.WriteMessage(websocket.TextMessage, subBuf)

	var channelName string

	msgChan := make(chan []byte)
	if strings.Compare(dataType, "depth") == 0 {
		channelName = "cryptoperprtualv2_rs_" + symbol + "_depth"
		go cryptoperprtualDepthDataParser(wsClient, server, msgChan, channelName, symbol, dataType)
	} else {
		channelName = "cryptoperprtualv2_ws_" + symbol + "_ticker"
		go cryptoperprtualTickerDataParser(wsClient, server, msgChan, channelName, symbol, dataType)
	}

	for {
		_, msgByte, err := wsClient.ReadMessage()
		if err != nil {
			applogger.Error("ReadMessage:%v", err)
		}

		msgChan <- msgByte
	}
}

func cryptoperprtualDepthDataParser(wsClient *websocket.Conn, server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("CryptoPerprtualDepthDataParser %s started!", symbol)
	var bytes []byte
	data := models.DepthData{}
	data.Symbol = symbol
	data.ExName = "cryptoperprtual"
	data.DataType = "depth"
	data.IO = "ws"

	respond := make(map[string]interface{})
	respond["method"] = "public/respond-heartbeat"

	for {
		bytes = <-msgChan
		jsonMap := gjson.ParseBytes(bytes).Map()

		if jsonMap["result"].Exists() {

			dataMap := jsonMap["result"].Map()["data"].Array()[0].Map()
			data.Lts = time.Now().UnixMilli()
			data.Data.Ts = uint64(dataMap["t"].Num)
			data.Data.Id = uint64(dataMap["u"].Num)

			asks := dataMap["asks"].Array()
			for i, value := range asks {
				data.Data.Asks[i].Price = value.Array()[0].Float()
				data.Data.Asks[i].Amount = value.Array()[1].Float()
			}

			bids := dataMap["bids"].Array()
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
			applogger.Debug(string(bytes))
		}

	}
}

func cryptoperprtualTickerDataParser(wsClient *websocket.Conn, server *redis.Client, msgChan chan []byte, channelName string, symbol string, dataType string) {
	applogger.Info("CryptoPerpetual ticker data parser %s started!", symbol)
	var bytes []byte
	data := models.TickerData{}
	data.Symbol = symbol
	data.ExName = "cryptoperprtual"
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
			trade := jsonMap["result"].Map()["data"].Array()[0].Map()

			data.Data.Tickers[0].Ts = uint64(trade["t"].Num)
			data.Data.Tickers[0].Price = trade["p"].Float()
			data.Data.Tickers[0].Amount = trade["q"].Float()
			data.Data.Tickers[0].Direction = strings.ToLower(trade["s"].Str)
			data.Data.Tickers[0].Id = int64(trade["d"].Num)
			data.Data.Tickers[0].DBns = uint64(time.Now().UnixNano())

			jsonBytes, _ := json.Marshal(data)
			logger.Print(logger.DEBUG, "cryptoperprtual", string(jsonBytes))
			server.Publish(channelName, jsonBytes)
		} else if strings.Compare(gjson.GetBytes(bytes, "method").Str, "public/heartbeat") == 0 {
			respond["id"] = gjson.GetBytes(bytes, "id").Int()
			jBuf, _ := json.Marshal(respond)
			_ = wsClient.WriteMessage(websocket.TextMessage, jBuf)
		}

	}
}
