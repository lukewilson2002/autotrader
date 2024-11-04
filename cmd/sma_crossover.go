//go:build ignore

package main

import (
	"fmt"
	"os"

	auto "github.com/fivemoreminix/autotrader"
	"github.com/fivemoreminix/autotrader/oanda"
)

type SMAStrategy struct {
	period1, period2 int
}

func (s *SMAStrategy) Init(_ *auto.Trader) {
}

func (s *SMAStrategy) Next(t *auto.Trader) {
	sma1 := t.Data().Closes().Copy().Rolling(s.period1).Mean()
	sma2 := t.Data().Closes().Copy().Rolling(s.period2).Mean()

	// If the shorter SMA (sma1) crosses above the longer SMA (sma2), buy.
	if auto.CrossoverIndex(*t.Data().Date(-1), sma1, sma2) {
		t.CloseOrdersAndPositions()
		t.Buy(1000, 0, 0)
	} else if auto.CrossoverIndex(*t.Data().Date(-1), sma2, sma1) {
		t.CloseOrdersAndPositions()
		t.Sell(1000, 0, 0)
	}
}

func main() {
	/* data, err := auto.EURUSD()
	if err != nil {
		panic(err)
	}
	*/
	broker, err := oanda.NewOandaBroker(os.Getenv("OANDA_TOKEN"), os.Getenv("OANDA_ACCOUNT_ID"), true)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		Broker:        auto.NewTestBroker(broker, nil, 10000, 50, 0.0002, 0),
		Strategy:      &SMAStrategy{period1: 7, period2: 20},
		Symbol:        "EUR_USD",
		Frequency:     "M15",
		CandlesToKeep: 2500,
	}))
}
