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

	data  *DataFrame
	sched *gocron.Scheduler
	idx   int
}

func (t *Trader) Data() *DataFrame {
	return t.data
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
	t.sched.StartBlocking()
}

// Tick updates the current state of the market and runs the strategy.
func (t *Trader) Tick() {
	if t.idx == 0 {
		t.Strategy.Init(t)
	}
	t.fetchData()
	t.Strategy.Next(t)
	t.Log.Println("Tick")
}

func (t *Trader) fetchData() {
	var err error
	t.data, err = t.Broker.Candles(t.Symbol, t.Frequency, t.CandlesToKeep)
	if err != nil {
		panic(err) // TODO: implement safe shutdown procedure
	}
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
	}
}
