package autotrader

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/go-echarts/go-echarts/v2/charts"
	"github.com/go-echarts/go-echarts/v2/components"
	"github.com/go-echarts/go-echarts/v2/opts"
	"golang.org/x/exp/rand"
	"golang.org/x/exp/slices"
)

var (
	ErrEOF            = errors.New("end of the input data")
	ErrNoData         = errors.New("no data")
	ErrPositionClosed = errors.New("position closed")
)

var _ Broker = (*TestBroker)(nil) // Compile-time interface check.

func Backtest(trader *Trader) {
	switch broker := trader.Broker.(type) {
	case *TestBroker:
		rand.Seed(uint64(time.Now().UnixNano()))
		trader.Init() // Initialize the trader and strategy.
		start := time.Now()
		for !trader.EOF {
			trader.Tick()    // Allow the trader to process the current candlesticks.
			broker.Advance() // Give the trader access to the next candlestick.
		}
		log.Printf("Backtest completed on %d candles. Opening report...\n", trader.Stats().Dated.Len())
		stats := trader.Stats()

		page := components.NewPage()

		// Create a new line balChart based on account equity and add it to the page.
		balChart := charts.NewLine()
		balChart.SetGlobalOptions(charts.WithTitleOpts(opts.Title{
			Title:    "Balance",
			Subtitle: fmt.Sprintf("%s %s %T  %s (took %.2f seconds)", trader.Symbol, trader.Frequency, trader.Strategy, time.Now().Format(time.DateTime), time.Since(start).Seconds()),
		}), charts.WithTooltipOpts(opts.Tooltip{
			Show:      true,
			Trigger:   "axis",
			TriggerOn: "mousemove|click",
		}), charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Show:      true,
				Formatter: "${value}",
			},
		}))
		balChart.SetXAxis(seriesStringArray(stats.Dated.Dates())).
			AddSeries("Equity", lineDataFromSeries(stats.Dated.Series("Equity")), func(s *charts.SingleSeries) {
			}).
			AddSeries("Profit", lineDataFromSeries(stats.Dated.Series("Profit")))
			// AddSeries("Drawdown", lineDataFromSeries(stats.Dated.Series("Drawdown")))

		// Sort Returns by value.
		// Plot returns as a bar chart.
		returnsSeries := stats.Dated.Series("Returns")
		returns := make([]float64, 0, returnsSeries.Len())
		// returns := stats.Dated.Series("Returns").Values()
		// Remove nil values.
		for i := 0; i < returnsSeries.Len(); i++ {
			r := returnsSeries.Value(i)
			if r != nil {
				returns = append(returns, r.(float64))
			}
		}
		// Sort the returns.
		slices.Sort(returns)
		// Create the X axis labels for the returns chart based on length of the returns slice.
		returnsLabels := make([]int, len(returns))
		for i := range returns {
			returnsLabels[i] = i + 1
		}
		returnsBars := make([]opts.BarData, len(returns))
		for i, r := range returns {
			returnsBars[i] = opts.BarData{Value: r}
		}
		var avg float64
		for _, r := range returns {
			avg += r
		}
		avg /= float64(len(returns))
		returnsAverage := make([]opts.LineData, len(returns))
		for i := range returnsAverage {
			returnsAverage[i] = opts.LineData{Value: avg}
		}

		returnsChart := charts.NewBar()
		returnsChart.SetGlobalOptions(charts.WithTitleOpts(opts.Title{
			Title:    "Returns",
			Subtitle: fmt.Sprintf("Average: $%.2f", avg),
		}), charts.WithYAxisOpts(opts.YAxis{
			AxisLabel: &opts.AxisLabel{
				Show:      true,
				Formatter: "${value}",
			},
		}))
		returnsChart.SetXAxis(returnsLabels).
			AddSeries("Returns", returnsBars)

		returnsChartAvg := charts.NewLine()
		returnsChartAvg.SetGlobalOptions(charts.WithTitleOpts(opts.Title{
			Title: "Average Returns",
		}))
		returnsChartAvg.SetXAxis(returnsLabels).
			AddSeries("Average", returnsAverage, func(s *charts.SingleSeries) {
				s.LineStyle = &opts.LineStyle{
					Width: 2,
				}
			})
		returnsChart.Overlap(returnsChartAvg)

		// TODO: Use Radar to display performance metrics.

		// Add all the charts in the desired order.
		page.PageTitle = "Backtest Report"
		page.AddCharts(balChart, returnsChart)

		// Draw the page to a file.
		f, err := os.Create("backtest.html")
		if err != nil {
			panic(err)
		}
		page.Render(f)
		f.Close()

		// Open the chart in the default browser.
		if err := Open("backtest.html"); err != nil {
			panic(err)
		}
	default:
		log.Fatalf("Backtesting is only supported with a TestBroker. Got %T", broker)
	}
}

// func barDataFromSeries(s Series) []opts.BarData {
// 	if s == nil || s.Len() == 0 {
// 		return []opts.BarData{}
// 	}
// 	data := make([]opts.BarData, s.Len())
// 	for i := 0; i < s.Len(); i++ {
// 		data[i] = opts.BarData{Value: s.Value(i)}
// 	}
// 	return data
// }

func lineDataFromSeries(s Series) []opts.LineData {
	if s == nil || s.Len() == 0 {
		return []opts.LineData{}
	}
	data := make([]opts.LineData, s.Len())
	for i := 0; i < s.Len(); i++ {
		data[i] = opts.LineData{Value: Round(s.Value(i).(float64), 2)}
	}
	return data
}

func seriesStringArray(s Series) []string {
	if s == nil || s.Len() == 0 {
		return []string{}
	}
	first := true
	data := make([]string, s.Len())
	var dateLayout string
	for i := 0; i < s.Len(); i++ {
		switch val := s.Value(i).(type) {
		case time.Time:
			if first {
				first = false
				dateHead := s.Value(0).(time.Time)
				dateTail := s.Value(-1).(time.Time)
				diff := dateTail.Sub(dateHead)
				if diff.Hours() > 24*365 {
					dateLayout = time.DateOnly
				} else {
					dateLayout = time.DateTime
				}
			}
			data[i] = val.Format(dateLayout)
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

func (b *TestBroker) PL() float64 {
	var pl float64
	for _, position := range b.positions {
		pl += position.PL()
	}
	return pl
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
	p.broker.SignalEmit("PositionClosed", p)
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
