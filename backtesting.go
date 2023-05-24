package autotrader

import (
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
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
	ErrPositionClosed = errors.New("position already closed")
	ErrZeroUnits      = errors.New("no amount of units specifed")
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
		trader.closeOrdersAndPositions() // Close any outstanding trades now.

		log.Printf("Backtest completed on %d candles. Opening report...\n", trader.Stats().Dated.Len())
		stats := trader.Stats()
		// log.Println(trader.Stats().Dated.String())

		// Divide net profit by maximum drawdown to get the profit factor.
		var maxDrawdown float64
		stats.Dated.Series("Drawdown").ForEach(func(i int, val any) {
			f := val.(float64)
			if f > maxDrawdown {
				maxDrawdown = f
			}
		})
		profit := stats.Dated.Float("Profit", -1)
		profitFactor := stats.Dated.Float("Profit", -1) / maxDrawdown
		maxDrawdownPct := 100 * maxDrawdown / stats.Dated.Float("Equity", 0)

		// Print a summary of the statistics to the console.
		{
			w := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
			fmt.Fprintln(w)
			fmt.Fprintf(w, "Timespan:\t%s\t\n", stats.Dated.Date(-1).Sub(stats.Dated.Date(0)).Round(time.Second))
			fmt.Fprintf(w, "Net Profit:\t$%.2f (%.2f%%)\t\n", profit, 100*profit/stats.Dated.Float("Equity", 0))
			fmt.Fprintf(w, "Profit Factor:\t%.2f\t\n", profitFactor)
			fmt.Fprintf(w, "Max Drawdown:\t$%.2f (%.2f%%)\t\n", maxDrawdown, maxDrawdownPct)
			fmt.Fprintf(w, "Spread collected:\t$%.2f\t\n", broker.spreadCollectedUSD)
			fmt.Fprintln(w)
			w.Flush()
		}

		// Pick a datetime layout based on the frequency.
		dateLayout := time.DateTime
		if strings.Contains(trader.Frequency, "S") { // Seconds
			dateLayout = "15:04:05"
		} else if strings.Contains(trader.Frequency, "H") { // Hours
			dateLayout = "2006-01-02 15:04"
		} else if strings.Contains(trader.Frequency, "D") || trader.Frequency == "W" { // Days or Weeks
			dateLayout = time.DateOnly
		} else if trader.Frequency == "M" { // Months
			dateLayout = "2006-01"
		} else if strings.Contains(trader.Frequency, "M") { // Minutes
			dateLayout = "01-02 15:04"
		}

		page := components.NewPage()

		// Create a new line balChart based on account equity and add it to the page.
		balChart := charts.NewLine()
		balChart.SetGlobalOptions(
			charts.WithTitleOpts(opts.Title{
				Title:    "Balance",
				Subtitle: fmt.Sprintf("%s %s %T  %s (took %.2f seconds)", trader.Symbol, trader.Frequency, trader.Strategy, time.Now().Format(time.DateTime), time.Since(start).Seconds()),
			}),
			charts.WithTooltipOpts(opts.Tooltip{
				Show:      true,
				Trigger:   "axis",
				TriggerOn: "mousemove|click",
			}),
			charts.WithYAxisOpts(opts.YAxis{
				AxisLabel: &opts.AxisLabel{
					Show:      true,
					Formatter: "${value}",
				},
			}),
			charts.WithLegendOpts(opts.Legend{
				Show:     true,
				Selected: map[string]bool{"Equity": false, "Profit": true},
			}))
		balChart.SetXAxis(seriesStringArray(stats.Dated.Dates(), dateLayout)).
			AddSeries("Equity", lineDataFromSeries(stats.Dated.Series("Equity"))).
			SetSeriesOptions(
				charts.WithMarkPointNameTypeItemOpts(
					opts.MarkPointNameTypeItem{Name: "Peak", Type: "max", ItemStyle: &opts.ItemStyle{
						Color: balChart.Colors[1],
					}},
					opts.MarkPointNameTypeItem{Name: "Drawdown", Type: "min", ItemStyle: &opts.ItemStyle{
						Color: balChart.Colors[3],
					}},
				),
			)
		balChart.AddSeries("Profit", lineDataFromSeries(stats.Dated.Series("Profit")))

		// Create a new kline chart based on the candlesticks and add it to the page.
		kline := newKline(trader.data, stats.Dated.Series("Trades"), dateLayout)

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
		returnsChart.SetGlobalOptions(
			charts.WithTitleOpts(opts.Title{
				Title:    "Returns",
				Subtitle: fmt.Sprintf("Average: $%.2f", avg),
			}),
			charts.WithYAxisOpts(opts.YAxis{
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
		page.AddCharts(balChart, kline, returnsChart)

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

func newKline(dohlcv *IndexedFrame[UnixTime], trades *Series, dateLayout string) *charts.Kline {
	kline := charts.NewKLine()

	x := make([]string, dohlcv.Len())
	y := make([]opts.KlineData, dohlcv.Len())
	for i := 0; i < dohlcv.Len(); i++ {
		x[i] = dohlcv.Date(i).Time().Format(dateLayout)
		y[i] = opts.KlineData{Value: [4]float64{
			dohlcv.Open(i),
			dohlcv.Close(i),
			dohlcv.Low(i),
			dohlcv.High(i),
		}}
	}

	marks := make([]opts.MarkPointNameCoordItem, 0)
	for i := 0; i < trades.Len(); i++ {
		if slice := trades.Value(i); slice != nil {
			for _, trade := range slice.([]TradeStat) {
				color := "green"
				rotation := float32(0)
				if trade.Units < 0 {
					color = "red"
					rotation = 180
				}
				if trade.Exit {
					color = "black"
				}
				marks = append(marks, opts.MarkPointNameCoordItem{
					Name:       "Trade",
					Value:      fmt.Sprintf("%v units", trade.Units),
					Coordinate: []interface{}{x[i], y[i].Value.([4]float64)[1]},
					Label: &opts.Label{
						Show:     true,
						Position: "inside",
					},
					ItemStyle: &opts.ItemStyle{
						Color: color,
					},
					Symbol:       "arrow",
					SymbolRotate: rotation,
					SymbolSize:   25,
				})
			}
		}
	}

	kline.SetGlobalOptions(
		charts.WithTitleOpts(opts.Title{
			Title:    "Trades",
			Subtitle: fmt.Sprintf("Showing %d candles", dohlcv.Len()),
		}),
		charts.WithXAxisOpts(opts.XAxis{
			SplitNumber: 20,
		}),
		charts.WithYAxisOpts(opts.YAxis{
			Scale: true,
		}),
		charts.WithTooltipOpts(opts.Tooltip{ // Enable seeing details on hover.
			Show:      true,
			Trigger:   "axis",
			TriggerOn: "mousemove|click",
		}),
		charts.WithDataZoomOpts(opts.DataZoom{ // Support zooming with scroll wheel.
			Type:       "inside",
			Start:      0,
			End:        100,
			XAxisIndex: []int{0},
		}),
		charts.WithDataZoomOpts(opts.DataZoom{ // Support zooming with bottom slider.
			Type:       "slider",
			Start:      0,
			End:        100,
			XAxisIndex: []int{0},
		}),
	)
	kline.SetXAxis(x).AddSeries("Price Action", y, charts.WithMarkPointNameCoordItemOpts(marks...))
	return kline
}

func lineDataFromSeries(s *Series) []opts.LineData {
	if s == nil || s.Len() == 0 {
		return []opts.LineData{}
	}
	data := make([]opts.LineData, s.Len())
	for i := 0; i < s.Len(); i++ {
		data[i] = opts.LineData{Value: Round(s.Value(i).(float64), 2)}
	}
	return data
}

func seriesStringArray(s *Series, dateLayout string) []string {
	if s == nil || s.Len() == 0 {
		return []string{}
	}
	data := make([]string, s.Len())
	for i := 0; i < s.Len(); i++ {
		switch val := s.Value(i).(type) {
		case time.Time:
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
	Data       *IndexedFrame[UnixTime]
	Cash       float64
	Leverage   float64
	Spread     float64 // Number of pips to add to the price when buying and subtract when selling. (Forex)
	Slippage   float64 // A percentage of the price to add when buying and subtract when selling.

	candleCount        int // The number of candles anyone outside this broker has seen. Also equal to the number of times Candles has been called.
	orders             []Order
	positions          []Position
	spreadCollectedUSD float64 // Total amount of spread collected from trades.
}

func NewTestBroker(dataBroker Broker, data *IndexedFrame[UnixTime], cash, leverage, spread float64, startCandles int) *TestBroker {
	return &TestBroker{
		DataBroker:  dataBroker,
		Data:        data,
		Cash:        cash,
		Leverage:    Max(leverage, 1),
		Spread:      spread,
		Slippage:    0.005, // Price +/- up to 0.5% by a random amount.
		candleCount: Max(startCandles, 1),
	}
}

// SpreadCollected returns the total amount of spread collected from trades, in USD.
func (b *TestBroker) SpreadCollected() float64 {
	return b.spreadCollectedUSD
}

// CandleIndex returns the index of the current candle.
func (b *TestBroker) CandleIndex() int {
	return Max(b.candleCount-1, 0)
}

// Advance advances the test broker to the next candle in the input data. This should be done at the end of the
// strategy loop. This will also call Tick() to update orders and positions.
func (b *TestBroker) Advance() {
	if b.candleCount < b.Data.Len() {
		b.candleCount++
	}
	b.Tick()
}

func (b *TestBroker) Tick() {
	// Check if the current candle's high and lows contain any take profits or stop losses.
	high, low := b.Data.High(b.CandleIndex()), b.Data.Low(b.CandleIndex())

	// Update orders.
	for _, any_o := range b.orders {
		if any_o.Fulfilled() {
			continue
		}
		o := any_o.(*TestOrder)

		if o.orderType == Limit {
			if o.price >= low && o.price <= high {
				o.fulfill(o.price)
			}
		} else if o.orderType == Stop {
			if o.price <= high && o.price >= low {
				o.fulfill(o.price)
			}
		} else {
			panic("the order type is either unknown or otherwise should not be market because those are fulfilled immediately")
		}
	}

	// Update positions.
	for _, any_p := range b.positions {
		if any_p.Closed() {
			continue
		}
		p := any_p.(*TestPosition)
		price := b.Price("", p.units < 0) // We want to buy if we are short, and vice versa.

		if p.trailingSLDist > 0 {
			p.trailingSL = Max(p.trailingSL, price-p.trailingSLDist)
		}

		// Check if the position should be closed.
		if p.takeProfit > 0 {
			if (p.units > 0 && p.takeProfit <= high) || (p.units < 0 && p.takeProfit >= low) {
				p.close(p.takeProfit, CloseTakeProfit)
				continue
			}
		}
		// stopLoss won't be set if trailingSL is set, and vice versa.
		if p.stopLoss > 0 {
			if (p.units > 0 && p.stopLoss >= low) || (p.units < 0 && p.stopLoss <= high) {
				p.close(p.stopLoss, CloseStopLoss)
			}
		} else if p.trailingSL > 0 {
			if (p.units > 0 && p.trailingSL >= low) || (p.units < 0 && p.trailingSL <= high) {
				p.close(p.trailingSL, CloseTrailingStop)
			}
		}
	}
}

// Price returns the ask price if wantToBuy is true and the bid price if wantToBuy is false.
func (b *TestBroker) Price(symbol string, wantToBuy bool) float64 {
	if wantToBuy {
		return b.Ask(symbol)
	}
	return b.Bid(symbol)
}

// Bid returns the price a seller receives for the current candle.
func (b *TestBroker) Bid(_ string) float64 {
	return b.Data.Close(b.CandleIndex())
}

// Ask returns the price a buyer pays for the current candle.
func (b *TestBroker) Ask(_ string) float64 {
	return b.Data.Close(b.CandleIndex()) + b.Spread
}

// Candles returns the last count candles for the given symbol and frequency. If count is greater than the number of candles, then a dataframe with zero rows is returned.
//
// If the TestBroker has a data broker set, then it will use that to get candles. Otherwise, it will return the candles from the data that was set. The first call to Candles will fetch candles from the data broker if it is set, so it is recommended to set the data broker before the first call to Candles and to call Candles the first time with the number of candles you want to fetch.
func (b *TestBroker) Candles(symbol string, frequency string, count int) (*IndexedFrame[UnixTime], error) {
	start := Max(Max(b.candleCount, 1)-count, 0)
	adjCount := b.candleCount - start

	if b.Data != nil && b.candleCount >= b.Data.Len() { // We have data and we are at the end of it.
		return b.Data.CopyRange(-count, -1), ErrEOF // Return the last count candles.
	} else if b.DataBroker != nil && b.Data == nil { // We have a data broker but no data.
		candles, err := b.DataBroker.Candles(symbol, frequency, count)
		if err != nil {
			return nil, err
		}
		b.Data = candles
	} else if b.Data == nil { // Both b.DataBroker and b.Data are nil.
		return nil, ErrNoData
	}
	return b.Data.CopyRange(start, adjCount), nil
}

func (b *TestBroker) Order(orderType OrderType, symbol string, units, price, stopLoss, takeProfit float64) (Order, error) {
	if units == 0 {
		return nil, ErrZeroUnits
	}
	if b.Data == nil { // The DataBroker could have data but nobody has fetched it, yet.
		if b.DataBroker == nil {
			return nil, ErrNoData
		}
		_, err := b.Candles("", "", 1) // Fetch data from the DataBroker.
		if err != nil {
			return nil, err
		}
	}

	var trailingSL float64
	if stopLoss < 0 {
		trailingSL = -stopLoss
	}

	marketPrice := b.Price("", units > 0)
	if orderType == Market {
		price = marketPrice
	}

	order := &TestOrder{
		broker:     b,
		id:         strconv.Itoa(rand.Int()),
		leverage:   b.Leverage,
		position:   nil,
		price:      price,
		symbol:     symbol,
		takeProfit: takeProfit,
		time:       time.Now(),
		orderType:  orderType,
		units:      units,
	}
	if trailingSL > 0 {
		order.trailingSL = trailingSL
	} else {
		order.stopLoss = stopLoss
	}

	// TODO: only instantly fulfill market orders or sometimes limit orders when requirements are met.
	if orderType == Market {
		order.fulfill(price)
	} else if orderType == Limit {
		if units > 0 && marketPrice <= order.price {
			order.fulfill(price)
		} else if units < 0 && marketPrice >= order.price {
			order.fulfill(price)
		}
	}

	b.orders = append(b.orders, order)
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

type TestPosition struct {
	broker         *TestBroker
	closed         bool
	entryPrice     float64
	closePrice     float64        // If zero, then position has not been closed.
	closeType      OrderCloseType // SL, TS, TP
	id             string
	leverage       float64
	symbol         string
	trailingSL     float64 // the price of the trailing stop loss as assigned by broker Tick().
	trailingSLDist float64 // serves to calculate the trailing stop loss at the broker.
	stopLoss       float64
	takeProfit     float64
	time           time.Time
	units          float64
}

func (p *TestPosition) Close() error {
	p.close(p.broker.Price("", p.units < 0), CloseMarket)
	return nil
}

func (p *TestPosition) close(atPrice float64, closeType OrderCloseType) {
	if p.closed {
		return
	}
	p.closed = true
	p.closePrice = atPrice
	p.closeType = closeType
	p.broker.Cash += p.Value() // Return the value of the position to the broker.
	p.broker.spreadCollectedUSD += p.broker.Spread * p.units
	p.broker.SignalEmit("PositionClosed", p)
}

func (p *TestPosition) Closed() bool {
	return p.closed
}

func (p *TestPosition) CloseType() OrderCloseType {
	return p.closeType
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

func (p *TestPosition) TrailingStop() float64 {
	return p.trailingSL
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
	return p.broker.Price("", p.units > 0) * p.units
}

type TestOrder struct {
	broker     *TestBroker
	id         string
	leverage   float64
	position   *TestPosition
	price      float64
	symbol     string
	trailingSL float64
	stopLoss   float64
	takeProfit float64
	time       time.Time
	orderType  OrderType
	units      float64
}

func (o *TestOrder) Cancel() error {
	return ErrCancelFailed
}

func (o *TestOrder) fulfill(atPrice float64) {
	slippage := rand.Float64() * o.broker.Slippage * atPrice
	atPrice += slippage - slippage/2 // Adjust price as +/- 50% of the slippage.

	o.position = &TestPosition{
		broker:     o.broker,
		closed:     false,
		entryPrice: atPrice,
		id:         strconv.Itoa(rand.Int()),
		leverage:   o.leverage,
		symbol:     o.symbol,
		takeProfit: o.takeProfit,
		time:       time.Now(),
		units:      o.units,
	}
	if o.trailingSL > 0 {
		o.position.trailingSLDist = o.trailingSL
	} else {
		o.position.stopLoss = o.stopLoss
	}
	o.broker.Cash -= o.position.EntryValue()

	o.broker.positions = append(o.broker.positions, o.position)
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

func (o *TestOrder) TrailingStop() float64 {
	return o.trailingSL
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
