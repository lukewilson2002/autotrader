package autotrader

import (
	"errors"

	df "github.com/rocketlaunchr/dataframe-go"
)

var ErrNoData = errors.New("no data")

func Backtest(trader *Trader) {
	trader.Tick()
}

type TestBroker struct {
	DataBroker   Broker
	Data         *df.DataFrame
	Cash         float64
	Leverage     float64
	StartCandles int
	candles      int
}

func (b *TestBroker) Candles(symbol string, frequency string, count int) (*df.DataFrame, error) {
	if b.DataBroker != nil && b.Data == nil {
		// Fetch a lot of candles from the broker so we don't keep asking.
		candles, err := b.DataBroker.Candles(symbol, frequency, Max(count, 1000))
		if err != nil {
			return nil, err
		}
		b.Data = candles
	} else if b.Data == nil { // Both b.DataBroker and b.Data are nil.
		return nil, ErrNoData
	}

	// Check if we reached the end of the existing data.
	if b.candles >= b.Data.NRows() {
		return nil, nil
	}

	// Catch up to the start candles.
	if b.candles < b.StartCandles {
		b.candles = b.StartCandles
	} else {
		b.candles++
	}
	end := b.candles - 1
	start := Max(b.candles-count, 0)

	return b.Data.Copy(df.Range{Start: &start, End: &end}), nil
}

func (b *TestBroker) MarketOrder(symbol string, units float64, stopLoss, takeProfit float64) (Order, error) {
	return nil, nil
}

func (b *TestBroker) NAV() float64 {
	return b.Cash
}

func NewTestBroker(dataBroker Broker, data *df.DataFrame, cash, leverage float64, startCandles int) *TestBroker {
	return &TestBroker{
		DataBroker:   dataBroker,
		Data:         data,
		Cash:         cash,
		Leverage:     Max(leverage, 1),
		StartCandles: Max(startCandles-1, 0),
	}
}
