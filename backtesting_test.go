package autotrader

import (
	"testing"
	"time"
)

var testData = func() *IndexedFrame[UnixTime] {
	type candlestick struct {
		Date   time.Time
		Open   float64
		High   float64
		Low    float64
		Close  float64
		Volume float64
	}
	candlesticks := []candlestick{
		{time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC), 1.1, 1.2, 1.0, 1.15, 100},
		{time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC), 1.15, 1.2, 1.1, 1.2, 110},
		{time.Date(2022, 1, 3, 0, 0, 0, 0, time.UTC), 1.2, 1.3, 1.15, 1.25, 120},
		{time.Date(2022, 1, 4, 0, 0, 0, 0, time.UTC), 1.25, 1.3, 1.0, 1.1, 130},
		{time.Date(2022, 1, 5, 0, 0, 0, 0, time.UTC), 1.1, 1.2, 1.0, 1.15, 110},
		{time.Date(2022, 1, 6, 0, 0, 0, 0, time.UTC), 1.15, 1.2, 1.1, 1.2, 120},
		{time.Date(2022, 1, 7, 0, 0, 0, 0, time.UTC), 1.2, 1.3, 1.15, 1.25, 140},
		{time.Date(2022, 1, 8, 0, 0, 0, 0, time.UTC), 1.25, 1.3, 1.0, 1.1, 150},
		{time.Date(2022, 1, 9, 0, 0, 0, 0, time.UTC), 1.1, 1.4, 1.0, 1.3, 160},
	}
	frame := NewIndexedFrame(
		NewIndexedSeries[UnixTime, any]("Open", nil),
		NewIndexedSeries[UnixTime, any]("High", nil),
		NewIndexedSeries[UnixTime, any]("Low", nil),
		NewIndexedSeries[UnixTime, any]("Close", nil),
		NewIndexedSeries[UnixTime, any]("Volume", nil),
	)
	for _, c := range candlesticks {
		frame.Series("Open").Insert(UnixTime(c.Date.Unix()), c.Open)
		frame.Series("High").Insert(UnixTime(c.Date.Unix()), c.High)
		frame.Series("Low").Insert(UnixTime(c.Date.Unix()), c.Low)
		frame.Series("Close").Insert(UnixTime(c.Date.Unix()), c.Close)
		frame.Series("Volume").Insert(UnixTime(c.Date.Unix()), c.Volume)
	}
	return frame
}()

func TestBacktestingBrokerCandles(t *testing.T) {
	broker := NewTestBroker(nil, testData, 0, 0, 0, 0)

	candles, err := broker.Candles("EUR_USD", "D", 3)
	if err != nil {
		t.Fatal(err)
	}
	if candles.Len() != 1 {
		t.Errorf("Expected 1 candle, got %d", candles.Len())
	}
	expected := time.Date(2022, 1, 1, 0, 0, 0, 0, time.UTC)
	if !candles.Date(0).Time().Equal(expected) {
		t.Errorf("Expected first candle to be %s, got %s", expected, candles.Date(0))
	}

	broker.Advance()
	candles, err = broker.Candles("EUR_USD", "D", 3)
	if err != nil {
		t.Fatal(err)
	}
	if candles.Len() != 2 {
		t.Errorf("Expected 2 candles, got %d", candles.Len())
	}
	expected = time.Date(2022, 1, 2, 0, 0, 0, 0, time.UTC)
	if !candles.Date(1).Time().Equal(expected) {
		t.Errorf("Expected second candle to be %s, got %s", expected, candles.Date(1))
	}

	for i := 0; i < 7; i++ { // 6 because we want to call broker.Advance() 9 times total
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

func TestBacktestingBrokerMarketOrders(t *testing.T) {
	broker := NewTestBroker(nil, testData, 100_000, 50, 0, 0)
	broker.Slippage = 0

	timeBeforeOrder := time.Now()
	order, err := broker.Order(Market, "EUR_USD", 50_000, 0, 0, 0) // Buy 50,000 USD for 1000 EUR with no stop loss or take profit
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
	if order.Type() != Market {
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

func TestBacktestingBrokerLimitOrders(t *testing.T) {
	broker := NewTestBroker(nil, testData, 100_000, 50, 0, 0)
	broker.Slippage = 0

	order, err := broker.Order(Limit, "EUR_USD", -50_000, 1.3, 1.35, 1.1) // Sell limit 50,000 USD for 1000 EUR
	if err != nil {
		t.Fatal(err)
	}
	if order == nil {
		t.Fatal("Order is nil")
	}
	if order.Price() != 1.3 {
		t.Errorf("Expected order price to be 1.3, got %f", order.Price())
	}
	if order.Fulfilled() != false {
		t.Error("Expected order to not be fulfilled")
	}

	broker.Advance()
	broker.Advance() // Advance to the third candle where the order should be fulfilled

	if order.Fulfilled() != true {
		t.Error("Expected order to be fulfilled")
	}

	position := order.Position()
	if position == nil {
		t.Fatal("Position is nil")
	}
	if position.Closed() != false {
		t.Fatal("Expected position to not be closed")
	}

	broker.Advance() // Advance to the fourth candle which should hit our take profit

	if position.Closed() != true {
		t.Fatal("Expected position to be closed")
	}
	if position.ClosePrice() != 1.1 {
		t.Errorf("Expected position close price to be 1.1, got %f", position.ClosePrice())
	}
	if position.CloseType() != CloseTakeProfit {
		t.Errorf("Expected position close type to be TP, got %s", position.CloseType())
	}
	if !EqualApprox(position.PL(), 10_000) { // abs(1.1-1.3) * 50_000 = 10,000
		t.Errorf("Expected position PL to be 10000, got %f", position.PL())
	}
}

func TestBacktestingBrokerStopOrders(t *testing.T) {
	broker := NewTestBroker(nil, testData, 100_000, 50, 0, 0)
	broker.Slippage = 0

	order, err := broker.Order(Stop, "EUR_USD", 50_000, 1.2, 1, 1.3) // Buy stop 50,000 EUR for 1000 USD
	if err != nil {
		t.Fatal(err)
	}
	if order == nil {
		t.Fatal("Order is nil")
	}
	if order.Price() != 1.2 {
		t.Errorf("Expected order price to be 1.2, got %f", order.Price())
	}
	if order.Fulfilled() != false {
		t.Error("Expected order to not be fulfilled")
	}

	broker.Advance() // Advance to the second candle where the order should be fulfilled

	if order.Fulfilled() != true {
		t.Error("Expected order to be fulfilled")
	}

	position := order.Position()
	if position == nil {
		t.Fatal("Position is nil")
	}
	if position.Closed() != false {
		t.Fatal("Expected position to not be closed")
	}

	broker.Advance() // Advance to the third candle which should hit our take profit

	if position.Closed() != true {
		t.Fatal("Expected position to be closed")
	}
	if position.ClosePrice() != 1.3 {
		t.Errorf("Expected position close price to be 1.3, got %f", position.ClosePrice())
	}
	if position.CloseType() != CloseTakeProfit {
		t.Errorf("Expected position close type to be TP, got %s", position.CloseType())
	}
	if !EqualApprox(position.PL(), 5000) { // (1.3-1.2) * 50_000 = 5000
		t.Errorf("Expected position PL to be 5000, got %f", position.PL())
	}
}

func TestBacktestingBrokerStopLossTakeProfit(t *testing.T) {
	broker := NewTestBroker(nil, testData, 100_000, 50, 0, 0)
	broker.Slippage = 0

	order, err := broker.Order(Market, "", 10_000, 0, 1.05, 1.25)
	if err != nil {
		t.Fatal(err)
	}
	if order == nil {
		t.Fatal("Order is nil")
	}
	if order.StopLoss() != 1.05 {
		t.Errorf("Expected stop loss to be 1.1, got %f", order.StopLoss())
	}
	if order.TakeProfit() != 1.25 {
		t.Errorf("Expected take profit to be 1.25, got %f", order.TakeProfit())
	}

	position := order.Position()
	if position == nil {
		t.Fatal("Position is nil")
	}
	if position.StopLoss() != 1.05 {
		t.Errorf("Expected stop loss to be 1.1, got %f", position.StopLoss())
	}
	if position.TakeProfit() != 1.25 {
		t.Errorf("Expected take profit to be 1.25, got %f", position.TakeProfit())
	}

	broker.Advance()
	broker.Advance() // Now we're at the third candle which hits our take profit

	if position.Closed() != true {
		t.Error("Expected position to be closed")
	}
	if position.ClosePrice() != 1.25 {
		t.Errorf("Expected close price to be 1.25, got %f", position.ClosePrice())
	}
	if !EqualApprox(position.PL(), 1000) { // (1.25-1.15) * 10_000 = 1000
		t.Errorf("Expected position PL to be 1000, got %f", position.PL())
	}

	broker.Advance() // 4th candle

	order, err = broker.Order(Market, "", 10_000, 0, -0.2, 1.4) // Long position with trailing stop loss of 0.2.
	if err != nil {
		t.Fatal(err)
	}
	if order == nil {
		t.Fatal("Order is nil")
	}
	if order.StopLoss() != 0 {
		t.Errorf("Expected stop loss to be 0, got %f", order.StopLoss())
	}
	if order.TakeProfit() != 1.4 {
		t.Errorf("Expected take profit to be 1.4, got %f", order.TakeProfit())
	}
	if !EqualApprox(order.TrailingStop(), 0.2) { // Orders return the distance to the trailing stop loss.
		t.Errorf("Expected trailing stop to be 0.2, got %f", order.TrailingStop())
	}

	broker.Advance() // Cause the position to get updated.
	position = order.Position()
	if position == nil {
		t.Fatal("Position is nil")
	}
	if position.Closed() {
		t.Error("Expected position to be open")
	}
	if position.StopLoss() != 0 {
		t.Errorf("Expected stop loss to be 0, got %f", position.StopLoss())
	}
	if position.TakeProfit() != 1.4 {
		t.Errorf("Expected take profit to be 1.4, got %f", position.TakeProfit())
	}
	if !EqualApprox(position.TrailingStop(), 0.95) { // Positions return the actual trailing stop loss price.
		t.Errorf("Expected trailing stop to be 0.95, got %f", position.TrailingStop())
	}

	for !position.Closed() {
		broker.Advance() // Advance until position is closed.
	}

	if !EqualApprox(position.ClosePrice(), 1.05) {
		t.Errorf("Expected close price to be 1.05, got %f", position.ClosePrice())
	}
	if !EqualApprox(position.PL(), -500) { // (1.05-1.1) * 10_000 = -500
		t.Errorf("Expected position PL to be 1000, got %f", position.PL())
	}
	if position.CloseType() != CloseTrailingStop {
		t.Errorf("Expected close type to be %q, got %q", CloseTrailingStop, position.CloseType())
	}
}
