package autotrader

import (
	"errors"
	"strconv"
	"time"

	"golang.org/x/exp/rand"
)

var (
	ErrEOF            = errors.New("end of the input data")
	ErrNoData         = errors.New("no data")
	ErrPositionClosed = errors.New("position closed")
)

func Backtest(trader *Trader) {
	trader.Tick()
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
	DataBroker   Broker
	Data         *DataFrame
	Cash         float64
	Leverage     float64
	Spread       float64 // Number of pips to add to the price when buying and subtract when selling. (Forex)
	StartCandles int

	candleCount int // The number of candles anyone outside this broker has seen. Also equal to the number of times Candles has been called.
	orders      []Order
	positions   []Position
}

func (b *TestBroker) Candles(symbol string, frequency string, count int) (*DataFrame, error) {
	// Check if we reached the end of the existing data.
	if b.Data != nil && b.candleCount >= b.Data.Len() {
		return b.Data.Copy(0, -1), ErrEOF
	}

	// Catch up to the start candles.
	if b.candleCount < b.StartCandles {
		b.candleCount = b.StartCandles
	} else {
		b.candleCount++
	}
	return b.candles(symbol, frequency, count)
}

// candles does the same as the public Candles except it doesn't increment b.candleCount so that it can be used
// internally to fetch candles without incrementing the count.
func (b *TestBroker) candles(symbol string, frequency string, count int) (*DataFrame, error) {
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

	// TODO: check if count > our rows if we are using a data broker and then fetch more data if so.

	// Catch up to the start candles.
	if b.candleCount < b.StartCandles {
		b.candleCount = b.StartCandles
	}

	// We use a Max(b.candleCount, 1) because we want to return at least 1 candle (even if b.candleCount is 0),
	// which may happen if we call this function before the first call to Candles.
	end := Max(b.candleCount, 1) - 1
	start := Max(Max(b.candleCount, 1)-count, 0)

	return b.Data.Copy(start, end), nil
}

func (b *TestBroker) MarketOrder(symbol string, units float64, stopLoss, takeProfit float64) (Order, error) {
	if b.Data == nil { // The dataBroker could have data but nobody has called Candles, yet.
		if b.DataBroker == nil {
			return nil, ErrNoData
		}
		_, err := b.candles("", "", 1) // Fetch 1 candle.
		if err != nil {
			return nil, err
		}
	}
	price := b.Data.Close(Max(b.candleCount-1, 0)) // Get the last close price.

	// Instantly fulfill the order.
	b.Cash -= price * units * LeverageToMargin(b.Leverage)
	position := &TestPosition{}

	order := &TestOrder{
		id:         strconv.Itoa(rand.Int()),
		leverage:   b.Leverage,
		position:   position,
		price:      price,
		symbol:     symbol,
		stopLoss:   stopLoss,
		takeProfit: takeProfit,
		time:       time.Now(),
		orderType:  MarketOrder,
		units:      units,
	}

	b.orders = append(b.orders, order)
	b.positions = append(b.positions, position)
	b.SignalEmit("OrderPlaced", order)

	return order, nil
}

func (b *TestBroker) NAV() float64 {
	return b.Cash
}

func (b *TestBroker) Orders() []Order {
	return b.orders
}

func (b *TestBroker) Positions() []Position {
	return b.positions
}

func NewTestBroker(dataBroker Broker, data *DataFrame, cash, leverage, spread float64, startCandles int) *TestBroker {
	return &TestBroker{
		DataBroker:   dataBroker,
		Data:         data,
		Cash:         cash,
		Leverage:     Max(leverage, 1),
		Spread:       spread,
		StartCandles: Max(startCandles-1, 0),
	}
}

type TestPosition struct {
	closed     bool
	entryPrice float64
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
	return nil
}

func (p *TestPosition) Closed() bool {
	return p.closed
}

func (p *TestPosition) EntryPrice() float64 {
	return p.entryPrice
}

func (p *TestPosition) Id() string {
	return p.id
}

func (p *TestPosition) Leverage() float64 {
	return p.leverage
}

func (p *TestPosition) PL() float64 {
	return 0
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
