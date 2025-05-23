package autotrader

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-co-op/gocron"
)

// Trader acts as the primary interface to the broker and strategy. To the strategy, it provides all the information
// about the current state of the market and the portfolio. To the broker, it provides the orders to be executed and
// requests for the current state of the portfolio.
type Trader struct {
	Broker        Broker
	Strategy      Strategy
	Symbol        string
	Frequency     string
	CandlesToKeep int
	Log           *log.Logger
	EOF           bool

	data  *IndexedFrame[UnixTime]
	sched *gocron.Scheduler
	stats *TraderStats
}

func (t *Trader) Data() *IndexedFrame[UnixTime] {
	return t.data
}

type TradeStat struct {
	Price float64 // Price is the price at which the trade was executed. If Exit is true, this is the exit price. Otherwise, this is the entry price.
	Units float64 // Units is the signed number of units bought or sold.
	Exit  bool    // Exit is true if the trade was to exit a previous position.
}

// Financial performance reporting and statistics.
type TraderStats struct {
	Dated             *Frame
	returnsThisCandle float64
	tradesThisCandle  []TradeStat
}

func (t *Trader) Stats() *TraderStats {
	return t.stats
}

// Run starts the trader. This is a blocking call.
func (t *Trader) Run() {
	t.sched = gocron.NewScheduler(time.UTC)
	capitalizedFreq := strings.ToUpper(t.Frequency)
	if strings.HasPrefix(capitalizedFreq, "S") {
		seconds, err := strconv.Atoi(t.Frequency[1:])
		if err != nil {
			panic(err)
		}
		t.sched.Every(seconds).Seconds()
	} else if strings.HasPrefix(capitalizedFreq, "M") {
		minutes, err := strconv.Atoi(t.Frequency[1:])
		if err != nil {
			panic(err)
		}
		t.sched.Every(minutes).Minutes()
	} else if strings.HasPrefix(capitalizedFreq, "H") {
		hours, err := strconv.Atoi(t.Frequency[1:])
		if err != nil {
			panic(err)
		}
		t.sched.Every(hours).Hours()
	} else {
		switch capitalizedFreq {
		case "D":
			t.sched.Every(1).Day()
		case "W":
			t.sched.Every(1).Day()
		case "M":
			t.sched.Every(1).Day()
		default:
			panic(fmt.Sprintf("invalid frequency: %s", t.Frequency))
		}
	}
	t.sched.Do(t.Tick) // Set the function to be run when the interval repeats.

	t.Init()
	t.sched.StartBlocking()
}

func (t *Trader) Init() {
	t.Strategy.Init(t)
	t.stats.Dated = NewFrame(
		NewSeries("Date"),
		NewSeries("Equity"),
		NewSeries("Profit"),
		NewSeries("Drawdown"),
		NewSeries("Returns"),
		NewSeries("Trades"), // []float64 representing the number of units traded positive for buy, negative for sell.
	)
	t.stats.tradesThisCandle = make([]TradeStat, 0, 2)
	t.Broker.SignalConnect(OrderFulfilled, t, func(a ...any) {
		order := a[0].(Order)
		tradeStat := TradeStat{order.Position().EntryPrice(), order.Units(), false}
		t.stats.tradesThisCandle = append(t.stats.tradesThisCandle, tradeStat)
	})
	t.Broker.SignalConnect("PositionClosed", t, func(args ...any) {
		position := args[0].(Position)
		tradeStat := TradeStat{position.ClosePrice(), position.Units(), true}
		t.stats.tradesThisCandle = append(t.stats.tradesThisCandle, tradeStat)
		t.stats.returnsThisCandle += position.PL()
	})
}

// Tick updates the current state of the market and runs the strategy.
func (t *Trader) Tick() {
	t.fetchData()      // Fetch the latest candlesticks from the broker.
	t.Strategy.Next(t) // Run the strategy.

	// Update the stats.
	err := t.stats.Dated.PushValues(map[string]any{
		"Date":   t.data.Date(-1).Time(),
		"Equity": t.Broker.NAV(),
		"Profit": t.Broker.PL(),
		"Drawdown": func() float64 {
			var bal float64
			if t.stats.Dated.Len() > 0 {
				bal = t.stats.Dated.Float("Equity", 0) // Take starting balance
			} else {
				bal = t.Broker.NAV() // Take current balance for first value
			}
			return Max(bal-t.Broker.NAV(), 0)
		}(),
		"Returns": func() any {
			if t.stats.returnsThisCandle != 0 {
				return t.stats.returnsThisCandle
			} else {
				return nil
			}
		}(),
		"Trades": func() any {
			if len(t.stats.tradesThisCandle) == 0 {
				return nil
			}
			trades := make([]TradeStat, len(t.stats.tradesThisCandle))
			copy(trades, t.stats.tradesThisCandle)
			t.stats.tradesThisCandle = t.stats.tradesThisCandle[:0]
			return trades
		}(),
	})
	if err != nil {
		log.Printf("error pushing values to stats dataframe: %v\n", err.Error())
	}
	t.stats.returnsThisCandle = 0
}

func (t *Trader) fetchData() {
	var err error
	t.data, err = t.Broker.Candles(t.Symbol, t.Frequency, t.CandlesToKeep)
	if err == ErrEOF {
		t.EOF = true
		t.Log.Println("End of data")
		if t.sched != nil && t.sched.IsRunning() {
			t.sched.Clear()
		}
	} else if err != nil {
		panic(err) // TODO: implement safe shutdown procedure
	}
}

func (t *Trader) Order(orderType OrderType, units, price, stopLoss, takeProfit float64) (Order, error) {
	var priceStr string
	if orderType != Market { // Price is ignored on market orders.
		priceStr = fmt.Sprintf(" @ $%.2f", price)
	} else {
		priceStr = fmt.Sprintf(" @ ~$%.2f", t.Broker.Price(t.Symbol, units > 0))
	}
	t.Log.Printf("%v %v units%v, stopLoss: %v, takeProfit: %v", orderType, units, priceStr, stopLoss, takeProfit)

	order, err := t.Broker.Order(orderType, t.Symbol, units, price, stopLoss, takeProfit)
	if err != nil {
		return order, err
	}

	// NOTE: Trade stats get added by handling an event by the broker
	return order, nil
}

// Buy creates a buy market order. Units must be greater than zero or ErrInvalidUnits is returned.
func (t *Trader) Buy(units, stopLoss, takeProfit float64) (Order, error) {
	if units <= 0 {
		return nil, ErrInvalidUnits
	}
	return t.Order(Market, units, 0, stopLoss, takeProfit)
}

// Sell creates a sell market order. Units must be greater than zero or ErrInvalidUnits is returned.
func (t *Trader) Sell(units, stopLoss, takeProfit float64) (Order, error) {
	if units <= 0 {
		return nil, ErrInvalidUnits
	}
	return t.Order(Market, -units, 0, stopLoss, takeProfit)
}

func (t *Trader) CloseOrdersAndPositions() {
	for _, order := range t.Broker.OpenOrders() {
		if order.Symbol() == t.Symbol {
			t.Log.Printf("Cancelling order: %v units", order.Units())
			order.Cancel()
		}
	}
	for _, position := range t.Broker.OpenPositions() {
		if position.Symbol() == t.Symbol {
			t.Log.Printf("Closing position: %v units, $%.2f PL, ($%.2f -> $%.2f)", position.Units(), position.PL(), position.EntryPrice(), position.ClosePrice())
			position.Close() // Event gets handled in the Init function
		}
	}
}

func (t *Trader) IsLong() bool {
	positions := t.Broker.OpenPositions()
	if len(positions) < 1 {
		return false
	} else if len(positions) > 1 {
		panic("cannot call IsLong with hedging enabled")
	}
	return positions[0].Units() > 0
}

func (t *Trader) IsShort() bool {
	positions := t.Broker.OpenPositions()
	if len(positions) < 1 {
		return false
	} else if len(positions) > 1 {
		panic("cannot call IsShort with hedging enabled")
	}
	return positions[0].Units() < 0
}

type TraderConfig struct {
	Broker        Broker
	Strategy      Strategy
	Symbol        string
	Frequency     string
	CandlesToKeep int
}

// NewTrader initializes a new Trader which can be used for live trading or backtesting.
func NewTrader(config TraderConfig) *Trader {
	logger := log.New(os.Stdout, "autotrader: ", log.LstdFlags|log.Lshortfile)
	return &Trader{
		Broker:        config.Broker,
		Strategy:      config.Strategy,
		Symbol:        config.Symbol,
		Frequency:     config.Frequency,
		CandlesToKeep: config.CandlesToKeep,
		Log:           logger,
		stats:         &TraderStats{},
	}
}
