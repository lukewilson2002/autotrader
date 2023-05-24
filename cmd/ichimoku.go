//go:build ignore

package main

import (
	"os"
	"time"

	auto "github.com/fivemoreminix/autotrader"
	"github.com/fivemoreminix/autotrader/oanda"
)

type IchimokuStrategy struct {
	convPeriod, basePeriod, leadingPeriods int
}

func (s *IchimokuStrategy) Init(_ *auto.Trader) {
}

func (s *IchimokuStrategy) Next(t *auto.Trader) {
	data := t.Data()
	now := *data.Date(-1)
	laggingTime := data.Date(-s.leadingPeriods - 1)

	// Extract ichimoku elements
	ichimoku := auto.Ichimoku(data, s.convPeriod, s.basePeriod, s.leadingPeriods, time.Minute*15)
	conv := ichimoku.Series("Conversion")
	base := ichimoku.Series("Base")
	leadA := ichimoku.Series("LeadingA")
	leadB := ichimoku.Series("LeadingB")
	lagging := ichimoku.Series("Lagging")

	// Conditions to buy:
	//  - price closed above the cloud at the current time
	//  - conversion above baseline
	//  - future cloud must be green (LeadingA > LeadingB)

	// Oposite conditions for sell...

	if laggingTime == nil { // Not enough candles to see the lagging.
		return
	}

	if t.IsLong() {
		if data.CloseIndex(now) < base.FloatIndex(now) ||
			leadA.FloatIndex(now) < leadB.FloatIndex(now) {
			t.CloseOrdersAndPositions()
		}
	} else if t.IsShort() {
		if data.CloseIndex(now) > base.FloatIndex(now) ||
			leadA.FloatIndex(now) > leadB.FloatIndex(now) {
			t.CloseOrdersAndPositions()
		}
	} else {
		// Look to enter a trade
		if data.CloseIndex(now) > leadA.FloatIndex(now) &&
			leadA.FloatIndex(now) > leadB.FloatIndex(now) &&
			conv.FloatIndex(now) > base.FloatIndex(now) &&
			leadA.Float(-1) > leadB.Float(-1) &&
			lagging.FloatIndex(*laggingTime) > leadA.FloatIndex(*laggingTime) {
			t.Buy(10000, 0, 0)
		} else if data.CloseIndex(now) < leadA.FloatIndex(now) &&
			leadA.FloatIndex(now) < leadB.FloatIndex(now) &&
			conv.FloatIndex(now) < base.FloatIndex(now) &&
			leadA.Float(-1) < leadB.Float(-1) &&
			lagging.FloatIndex(*laggingTime) < leadA.FloatIndex(*laggingTime) {
			t.Sell(10000, 0, 0)
		}
	}
}

func main() {
	broker := oanda.NewOandaBroker(os.Getenv("OANDA_TOKEN"), os.Getenv("OANDA_ACCOUNT_ID"), true)
	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		Broker:        auto.NewTestBroker(broker, nil, 10000, 50, 0.0002, 0),
		Strategy:      &IchimokuStrategy{convPeriod: 9, basePeriod: 26, leadingPeriods: 52},
		Symbol:        "USD_JPY",
		Frequency:     "M15",
		CandlesToKeep: 2500,
	}))
}
