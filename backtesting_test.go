package autotrader

import (
	"strings"
	"testing"
	"time"
)

const testDataCSV = `date,open,high,low,close,volume
2022-01-01,1.1,1.2,1.0,1.15,100
2022-01-02,1.15,1.2,1.1,1.2,110
2022-01-03,1.2,1.3,1.15,1.25,120
2022-01-04,1.25,1.3,1.2,1.1,130
2022-01-05,1.1,1.2,1.0,1.15,110
2022-01-06,1.15,1.2,1.1,1.2,120
2022-01-07,1.2,1.3,1.15,1.25,140
2022-01-08,1.25,1.3,1.2,1.1,150
2022-01-09,1.1,1.4,1.0,1.3,220`

func newTestingDataframe() *DataFrame {
	data, err := DataFrameFromCSVReaderLayout(strings.NewReader(testDataCSV), DataCSVLayout{
		LatestFirst: false,
		DateFormat:  "2006-01-02",
		Date:        "date",
		Open:        "open",
		High:        "high",
		Low:         "low",
		Close:       "close",
		Volume:      "volume",
	})
	if err != nil {
		panic(err)
	}
	return data
}

func TestBacktestingBrokerCandles(t *testing.T) {
	data := newTestingDataframe()
	broker := NewTestBroker(nil, data, 0, 0, 0, 0)

	candles, err := broker.Candles("EUR_USD", "D", 3)
	if err != nil {
		t.Fatal(err)
	}
	if candles.Len() != 1 {
		t.Errorf("Expected 1 candle, got %d", candles.Len())
	}
	if candles.Date(0) != time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC) {
		t.Errorf("Expected first candle to be 2022-01-01, got %s", candles.Date(0))
	}

	broker.Advance()
	candles, err = broker.Candles("EUR_USD", "D", 3)
	if err != nil {
		t.Fatal(err)
	}
	if candles.Len() != 2 {
		t.Errorf("Expected 2 candles, got %d", candles.Len())
	}
	if candles.Date(1) != time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC) {
		t.Errorf("Expected second candle to be 2022-01-02, got %s", candles.Date(1))
	}

	for i := 0; i < 7; i++ { // 6 because we want to call broker.Candles 9 times total
		broker.Advance()
		candles, err = broker.Candles("EUR_USD", "D", 5)
		if err != nil && err != ErrEOF && i != 6 { // Allow ErrEOF on last iteration.
			t.Fatalf("Got an error on iteration %d: %v (called Advance() %d times)", i, err, broker.CandleIndex()+1)
		}
		if candles == nil {
			t.Errorf("Candles is nil on iteration %d", i+1)
		}
	}
	if candles.Len() != 5 {
		t.Errorf("Expected 5 candles, got %d", candles.Len())
	}
	if candles.Close(4) != 1.3 {
		t.Errorf("Expected the last closing price to be 1.3, got %f", candles.Close(4))
	}
}

func TestBacktestingBrokerFunctions(t *testing.T) {
	broker := NewTestBroker(nil, nil, 100_000, 20, 0, 0)

	if !EqualApprox(broker.NAV(), 100_000) {
		t.Errorf("Expected NAV to be 100_000, got %f", broker.NAV())
	}
}

func TestBacktestingBrokerOrders(t *testing.T) {
	data := newTestingDataframe()
	broker := NewTestBroker(nil, data, 100_000, 50, 0, 0)

	timeBeforeOrder := time.Now()
	order, err := broker.MarketOrder("EUR_USD", 50_000, 0, 0) // Buy 50,000 USD for 1000 EUR with no stop loss or take profit
	if err != nil {
		t.Fatal(err)
	}
	if order == nil {
		t.Fatal("Order is nil")
	}

	if order.Symbol() != "EUR_USD" {
		t.Errorf("Expected symbol to be EUR_USD, got %s", order.Symbol())
	}
	if order.Units() != 50_000 {
		t.Errorf("Expected units to be 50_000, got %f", order.Units())
	}
	if order.Price() != 1.15 {
		t.Errorf("Expected order price to be 1.15 (first close), got %f", order.Price())
	}
	if order.Fulfilled() != true {
		t.Error("Expected order to be fulfilled")
	}
	if order.Time().Before(timeBeforeOrder) {
		t.Error("Expected order time to be after timeBeforeOrder")
	}
	if order.Leverage() != 50 {
		t.Errorf("Expected leverage to be 50, got %f", order.Leverage())
	}
	if order.StopLoss() != 0 {
		t.Errorf("Expected stop loss to be 0, got %f", order.StopLoss())
	}
	if order.TakeProfit() != 0 {
		t.Errorf("Expected take profit to be 0, got %f", order.TakeProfit())
	}
	if order.Type() != MarketOrder {
		t.Errorf("Expected order type to be MarketOrder, got %s", order.Type())
	}

	position := order.Position()
	if position == nil {
		t.Fatal("Position is nil")
	}
	if position.Symbol() != "EUR_USD" {
		t.Errorf("Expected symbol to be EUR_USD, got %s", position.Symbol())
	}
	if position.Units() != 50_000 {
		t.Errorf("Expected units to be 50_000, got %f", position.Units())
	}
	if position.EntryPrice() != 1.15 {
		t.Errorf("Expected entry price to be 1.15 (first close), got %f", position.EntryPrice())
	}
	if position.Time().Before(timeBeforeOrder) {
		t.Error("Expected position time to be after timeBeforeOrder")
	}
	if position.Leverage() != 50 {
		t.Errorf("Expected leverage to be 50, got %f", position.Leverage())
	}
	if position.StopLoss() != 0 {
		t.Errorf("Expected stop loss to be 0, got %f", position.StopLoss())
	}
	if position.TakeProfit() != 0 {
		t.Errorf("Expected take profit to be 0, got %f", position.TakeProfit())
	}

	if !EqualApprox(broker.NAV(), 100_000) { // NAV should not change until the next candle
		t.Errorf("Expected NAV to be 100_000, got %f", broker.NAV())
	}

	broker.Advance()                       // Advance broker to the next candle
	if !EqualApprox(position.PL(), 2500) { // (1.2-1.15) * 50_000 = 2500
		t.Errorf("Expected position PL to be 2500, got %f", position.PL())
	}
	if !EqualApprox(broker.NAV(), 102_500) {
		t.Errorf("Expected NAV to be 102_500, got %f", broker.NAV())
	}

	// Test closing positions.
	position.Close()
	if position.Closed() != true {
		t.Error("Expected position to be closed")
	}
	broker.Advance()
	if !EqualApprox(broker.NAV(), 102_500) {
		t.Errorf("Expected NAV to still be 102_500, got %f", broker.NAV())
	}
	if !EqualApprox(broker.PL(), 2500) {
		t.Errorf("Expected broker PL to be 2500, got %f", broker.PL())
	}
}
