//go:build ignore

package main

import auto "github.com/fivemoreminix/autotrader"

type IchimokuStrategy struct {
	convPeriod, basePeriod, leadingPeriods int
}

func (s *IchimokuStrategy) Init(_ *auto.Trader) {
}

func (s *IchimokuStrategy) Next(t *auto.Trader) {
	ichimoku := auto.Ichimoku(t.Data().Closes(), s.convPeriod, s.basePeriod, s.leadingPeriods)
	time := t.Data().Date(-1)
	// If the price crosses above the Conversion Line, buy.
	if auto.CrossoverIndex(*time, t.Data().Closes(), ichimoku.Series("Conversion")) {
		t.Buy(1000)
	}
	// If the price crosses below the Conversion Line, sell.
	if auto.CrossoverIndex(*time, ichimoku.Series("Conversion"), t.Data().Closes()) {
		t.Sell(1000)
	}
}

func main() {
	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		Broker:        auto.NewTestBroker(nil, nil, 10000, 50, 0.0002, 0),
		Strategy:      &IchimokuStrategy{convPeriod: 9, basePeriod: 26, leadingPeriods: 52},
		Symbol:        "EUR_USD",
		Frequency:     "M15",
		CandlesToKeep: 2500,
	}))
}
