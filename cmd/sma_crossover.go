//go:build ignore

package main

import (
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
	// If the shorter SMA crosses above the longer SMA, buy.
	if auto.CrossoverIndex(*t.Data().Date(-1), sma1, sma2) {
		t.Buy(1000)
	} else if auto.CrossoverIndex(*t.Data().Date(-1), sma2, sma1) {
		t.Sell(1000)
	}
}

func main() {
	/* data, err := auto.EURUSD()
	if err != nil {
		panic(err)
	}
	*/
	broker := oanda.NewOandaBroker(os.Getenv("OANDA_TOKEN"), os.Getenv("OANDA_ACCOUNT_ID"), true)

	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		Broker:        auto.NewTestBroker(broker /* data, */, nil, 10000, 50, 0.0002, 0),
		Strategy:      &SMAStrategy{period1: 10, period2: 25},
		Symbol:        "EUR_USD",
		Frequency:     "M15",
		CandlesToKeep: 2500,
	}))
}
