package models

type DepthData struct {
	Symbol   string          `json:"symbol"`
	DataType string          `json:"type"`
	ExName   string          `json:"exname"`
	IO       string          `json:"io"`
	Lts      int64           `json:"lts"`
	Cts      int64           `json:"cts"`
	DBts     int64           `json:"dbts"`
	Data     DepthDataDetail `json:"data"`
}

type DepthDataDetail struct {
	Asks [20]LevelData `json:"asks"`
	Bids [20]LevelData `json:"bids"`
	Ts   uint64        `json:"ts"`
	Id   uint64        `json:"id"`
	DBns uint64        `json:"dbns"`
}
type LevelData struct {
	Price  float64 `json:"price"`
	Amount float64 `json:"amount"`
}

type TickerData struct {
	Symbol   string           `json:"symbol"`
	DataType string           `json:"type"`
	ExName   string           `json:"exname"`
	IO       string           `json:"io"`
	Lts      int64            `json:"lts"`
	Cts      int64            `json:"cts"`
	DBts     int64            `json:"dbts"`
	Data     TickerDataDetail `json:"data"`
}

type TickerDataDetail struct {
	Tickers [1]Ticker `json:"tickers"`
}

type Ticker struct {
	Price     float64 `json:"price"`
	Amount    float64 `json:"amount"`
	Ts        uint64  `json:"ts"`
	Direction string  `json:"direction"`
	DBns      uint64  `json:"dbns"`
	Id        int64   `json:"id"`
}
