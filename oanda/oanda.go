package oanda

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"time"

	auto "github.com/fivemoreminix/autotrader"
)

const (
	oandaLiveURL     = "https://api-fxtrade.oanda.com"
	oandaPracticeURL = "https://api-fxpractice.oanda.com"
	TimeLayout       = time.RFC3339
)

var _ auto.Broker = (*OandaBroker)(nil) // Compile-time interface check.

type OandaBroker struct {
	*auto.SignalManager
	client    *http.Client
	token     string
	accountID string
	baseUrl   string // Either oandaLiveURL or oandaPracticeURL.
}

func NewOandaBroker(token, accountID string, practice bool) *OandaBroker {
	var baseUrl string
	if practice {
		baseUrl = oandaPracticeURL
	} else {
		baseUrl = oandaLiveURL
	}
	return &OandaBroker{
		SignalManager: &auto.SignalManager{},
		client:        &http.Client{},
		token:         token,
		accountID:     accountID,
		baseUrl:       baseUrl,
	}
}

// Price returns the ask price if wantToBuy is true and the bid price if wantToBuy is false.
func (b *OandaBroker) Price(symbol string, wantToBuy bool) float64 {
	if wantToBuy {
		return b.Ask(symbol)
	}
	return b.Bid(symbol)
}

func (b *OandaBroker) Bid(symbol string) float64 {
	return 0
}

func (b *OandaBroker) Ask(symbol string) float64 {
	return 0
}

func (b *OandaBroker) Candles(symbol, frequency string, count int) (*auto.Frame, error) {
	req, err := http.NewRequest("GET", b.baseUrl+"/v3/accounts/"+b.accountID+"/instruments/"+symbol+"/candles", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Authorization", "Bearer "+b.token)
	q := req.URL.Query()
	q.Add("granularity", frequency)
	q.Add("count", strconv.Itoa(auto.Min(count, 5000))) // API says max is 5000.
	req.URL.RawQuery = q.Encode()
	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var candlestickResponse *CandlestickResponse
	if err := json.NewDecoder(resp.Body).Decode(&candlestickResponse); err != nil {
		return nil, err
	}

	return newDataframe(candlestickResponse)
}

func (b *OandaBroker) Order(orderType auto.OrderType, symbol string, units, price, stopLoss, takeProfit float64) (auto.Order, error) {
	return nil, nil
}

func (b *OandaBroker) NAV() float64 {
	return 0
}

func (b *OandaBroker) PL() float64 {
	return 0
}

func (b *OandaBroker) OpenOrders() []auto.Order {
	return nil
}

func (b *OandaBroker) OpenPositions() []auto.Position {
	return nil
}

func (b *OandaBroker) Orders() []auto.Order {
	return nil
}

func (b *OandaBroker) Positions() []auto.Position {
	return nil
}

func (b *OandaBroker) fetchAccountUpdates() {
}

func newDataframe(candles *CandlestickResponse) (*auto.Frame, error) {
	if candles == nil {
		return nil, fmt.Errorf("candles is nil or empty")
	}
	data := auto.NewDOHLCVFrame()
	for _, candle := range candles.Candles {
		if candle.Mid == nil {
			return nil, fmt.Errorf("mid is nil or empty")
		}
		var o, h, l, c float64
		err := candle.Mid.Parse(&o, &h, &l, &c)
		if err != nil {
			return nil, fmt.Errorf("error parsing mid field of a candlestick: %w", err)
		}
		data.PushCandle(candle.Time, o, h, l, c, int64(candle.Volume))
	}
	return data, nil
}
