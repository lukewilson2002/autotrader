package main

import (
	auto "github.com/fivemoreminix/autotrader"
)

type SMAStrategy struct {
	period1, period2 int
}

func (s *SMAStrategy) Init(_ *auto.Trader) {
}

func (s *SMAStrategy) Next(t *auto.Trader) {
	sma1 := t.Data().Closes().Rolling(s.period1).Mean()
	sma2 := t.Data().Closes().Rolling(s.period2).Mean()
	// If the shorter SMA crosses above the longer SMA, buy.
	if crossover(sma1, sma2) {
		t.Buy(1000)
	} else if crossover(sma2, sma1) {
		t.Sell(1000)
	}
}

// crossover returns true if s1 crosses above s2 at the latest float.
func crossover(s1, s2 auto.Series) bool {
	return s1.Float(-1) > s2.Float(-1) && s1.Float(-2) <= s2.Float(-2)
}

func main() {
	data, err := auto.EURUSD()
	if err != nil {
		panic(err)
	}

	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		Broker:        auto.NewTestBroker(nil, data, 10000, 50, 0.0002, 0),
		Strategy:      &SMAStrategy{period1: 20, period2: 40},
		Symbol:        "EUR_USD",
		Frequency:     "D",
		CandlesToKeep: 1000,
	}))
}
