package autotrader

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/opts"
	"golang.org/x/exp/rand"
)

var (
	ErrEOF            = errors.New("end of the input data")
	ErrNoData         = errors.New("no data")
	ErrPositionClosed = errors.New("position closed")
)

func Backtest(trader *Trader) {
	switch broker := trader.Broker.(type) {
	case *TestBroker:
		trader.Init() // Initialize the trader and strategy.
		for !trader.EOF {
			trader.Tick()    // Allow the trader to process the current candlesticks.
			broker.Advance() // Give the trader access to the next candlestick.
		}
		log.Println("Backtest complete.")
		log.Println("Stats:")
		log.Println(trader.Stats().String())

		chart := charts.NewLine()
		chart.SetGlobalOptions(charts.WithTitleOpts(opts.Title{
			Title:    "Backtest",
			Subtitle: fmt.Sprintf("%s %s %T", trader.Symbol, trader.Frequency, trader.Strategy),
		}))
		chart.SetXAxis(seriesStringArray(trader.Stats().Dates())).
			AddSeries("Equity", lineDataFromSeries(trader.Stats().Series("Equity")))

		// Draw the chart to a file.
		f, err := os.Create("backtest.html")
		if err != nil {
			panic(err)
		}
		chart.Render(f)
		f.Close()

		// Open the chart in the default browser.
		if err := Open("backtest.html"); err != nil {
			panic(err)
		}
	default:
		log.Fatalf("Backtesting is only supported with a TestBroker. Got %T", broker)
	}
}

func lineDataFromSeries(s Series) []opts.LineData {
	data := make([]opts.LineData, s.Len())
	for i := 0; i < s.Len(); i++ {
		data[i] = opts.LineData{Value: s.Value(i)}
	}
	return data
}

func seriesStringArray(s Series) []string {
	data := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		switch val := s.Value(i).(type) {
		case time.Time:
			data[i] = val.Format(time.DateTime)
		case string:
			data[i] = fmt.Sprintf("%q", val)
		default:
			data[i] = fmt.Sprintf("%v", val)
		}
	}
	return data
}

// TestBroker is a broker that can be used for testing. It implements the Broker interface and fulfills orders
//
// Signals:
//   - Tick(nil) - Called when the broker ticks.
//   - OrderPlaced(Order) - Called when an order is placed.
//   - OrderFilled(Order) - Called when an order is filled.
//   - OrderCanceled(Order) - Called when an order is canceled.
//   - PositionClosed(Position) - Called when a position is closed.
//   - PositionModified(Position) - Called when a position changes.
type TestBroker struct {
	SignalManager
	DataBroker Broker
	Data       *DataFrame
	Cash       float64
	Leverage   float64
	Spread     float64 // Number of pips to add to the price when buying and subtract when selling. (Forex)

	candleCount int // The number of candles anyone outside this broker has seen. Also equal to the number of times Candles has been called.
	orders      []Order
	positions   []Position
}

// CandleIndex returns the index of the current candle.
func (b *TestBroker) CandleIndex() int {
	return Max(b.candleCount-1, 0)
}

// Advance advances the test broker to the next candle in the input data. This should be done at the end of the
// strategy loop.
func (b *TestBroker) Advance() {
	if b.candleCount < b.Data.Len() {
		b.candleCount++
	}
}

func (b *TestBroker) Candles(symbol string, frequency string, count int) (*DataFrame, error) {
	start := Max(Max(b.candleCount, 1)-count, 0)
	end := b.candleCount - 1

	if b.Data != nil && b.candleCount >= b.Data.Len() { // We have data and we are at the end of it.
		if count >= b.Data.Len() { // We are asking for more data than we have.
			return b.Data.Copy(0, -1).(*DataFrame), ErrEOF
		} else {
			return b.Data.Copy(start, -1).(*DataFrame), ErrEOF
		}
	} else if b.DataBroker != nil && b.Data == nil { // We have a data broker but no data.
		// Fetch a lot of candles from the broker so we don't keep asking.
		candles, err := b.DataBroker.Candles(symbol, frequency, Max(count, 1000))
		if err != nil {
			return nil, err
		}
		b.Data = candles
	} else if b.Data == nil { // Both b.DataBroker and b.Data are nil.
		return nil, ErrNoData
	}

	// TODO: check if count > our rows if we are using a data broker and then fetch more data if so.

	return b.Data.Copy(start, end).(*DataFrame), nil
}

func (b *TestBroker) MarketOrder(symbol string, units float64, stopLoss, takeProfit float64) (Order, error) {
	if b.Data == nil { // The DataBroker could have data but nobody has fetched it, yet.
		if b.DataBroker == nil {
			return nil, ErrNoData
		}
		_, err := b.Candles("", "", 1) // Fetch data from the DataBroker.
		if err != nil {
			return nil, err
		}
	}
	price := b.Data.Close(b.CandleIndex()) // Get the last close price.

	order := &TestOrder{
		id:         strconv.Itoa(rand.Int()),
		leverage:   b.Leverage,
		position:   nil,
		price:      price,
		symbol:     symbol,
		stopLoss:   stopLoss,
		takeProfit: takeProfit,
		time:       time.Now(),
		orderType:  MarketOrder,
		units:      units,
	}

	// Instantly fulfill the order.
	order.position = &TestPosition{
		broker:     b,
		closed:     false,
		entryPrice: price,
		id:         strconv.Itoa(rand.Int()),
		leverage:   b.Leverage,
		symbol:     symbol,
		stopLoss:   stopLoss,
		takeProfit: takeProfit,
		time:       time.Now(),
		units:      units,
	}
	b.Cash -= order.position.EntryValue()

	b.orders = append(b.orders, order)
	b.positions = append(b.positions, order.position)
	b.SignalEmit("OrderPlaced", order)

	return order, nil
}

func (b *TestBroker) NAV() float64 {
	nav := b.Cash
	// Add the value of open positions to our NAV.
	for _, position := range b.positions {
		if !position.Closed() {
			nav += position.Value()
		}
	}
	return nav
}

func (b *TestBroker) OpenOrders() []Order {
	orders := make([]Order, 0, len(b.orders))
	for _, order := range b.orders {
		if !order.Fulfilled() {
			orders = append(orders, order)
		}
	}
	return orders
}

func (b *TestBroker) OpenPositions() []Position {
	positions := make([]Position, 0, len(b.positions))
	for _, position := range b.positions {
		if !position.Closed() {
			positions = append(positions, position)
		}
	}
	return positions
}

func (b *TestBroker) Orders() []Order {
	return b.orders
}

func (b *TestBroker) Positions() []Position {
	return b.positions
}

func NewTestBroker(dataBroker Broker, data *DataFrame, cash, leverage, spread float64, startCandles int) *TestBroker {
	return &TestBroker{
		DataBroker:  dataBroker,
		Data:        data,
		Cash:        cash,
		Leverage:    Max(leverage, 1),
		Spread:      spread,
		candleCount: Max(startCandles, 1),
	}
}

type TestPosition struct {
	broker     *TestBroker
	closed     bool
	entryPrice float64
	closePrice float64 // If zero, then position has not been closed.
	id         string
	leverage   float64
	symbol     string
	stopLoss   float64
	takeProfit float64
	time       time.Time
	units      float64
}

func (p *TestPosition) Close() error {
	if p.closed {
		return ErrPositionClosed
	}
	p.closed = true
	p.closePrice = p.broker.Data.Close(p.broker.CandleIndex()) - p.broker.Spread // Get the last close price.
	p.broker.Cash += p.Value()                                                   // Return the value of the position to the broker.
	return nil
}

func (p *TestPosition) Closed() bool {
	return p.closed
}

func (p *TestPosition) EntryPrice() float64 {
	return p.entryPrice
}

func (p *TestPosition) ClosePrice() float64 {
	return p.closePrice
}

func (p *TestPosition) EntryValue() float64 {
	return p.entryPrice * p.units
}

func (p *TestPosition) Id() string {
	return p.id
}

func (p *TestPosition) Leverage() float64 {
	return p.leverage
}

func (p *TestPosition) PL() float64 {
	return p.Value() - p.EntryValue()
}

func (p *TestPosition) Symbol() string {
	return p.symbol
}

func (p *TestPosition) StopLoss() float64 {
	return p.stopLoss
}

func (p *TestPosition) TakeProfit() float64 {
	return p.takeProfit
}

func (p *TestPosition) Time() time.Time {
	return p.time
}

func (p *TestPosition) Units() float64 {
	return p.units
}

func (p *TestPosition) Value() float64 {
	if p.closed {
		return p.closePrice * p.units
	}
	bid := p.broker.Data.Close(p.broker.CandleIndex()) - p.broker.Spread
	return bid * p.units
}

type TestOrder struct {
	id         string
	leverage   float64
	position   *TestPosition
	price      float64
	symbol     string
	stopLoss   float64
	takeProfit float64
	time       time.Time
	orderType  OrderType
	units      float64
}

func (o *TestOrder) Cancel() error {
	return ErrCancelFailed
}

func (o *TestOrder) Fulfilled() bool {
	return o.position != nil
}

func (o *TestOrder) Id() string {
	return o.id
}

func (o *TestOrder) Leverage() float64 {
	return o.leverage
}

func (o *TestOrder) Position() Position {
	return o.position
}

func (o *TestOrder) Price() float64 {
	return o.price
}

func (o *TestOrder) Symbol() string {
	return o.symbol
}

func (o *TestOrder) StopLoss() float64 {
	return o.stopLoss
}

func (o *TestOrder) TakeProfit() float64 {
	return o.takeProfit
}

func (o *TestOrder) Time() time.Time {
	return o.time
}

func (o *TestOrder) Type() OrderType {
	return o.orderType
}

func (o *TestOrder) Units() float64 {
	return o.units
}
