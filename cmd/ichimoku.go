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
	//  - lagging span above lagging cloud

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
		// v := a - b , then comparing v > 0 is the same as a > b
		closeToLeadA := data.CloseIndex(now) - leadA.FloatIndex(now)
		convToBase := conv.FloatIndex(now) - base.FloatIndex(now)
		leadAToLeadB := leadA.FloatIndex(now) - leadB.FloatIndex(now)
		futureLeadAToLeadB := leadA.Float(-1) - leadB.Float(-1)
		laggingToLeadA := lagging.FloatIndex(*laggingTime) - leadA.FloatIndex(*laggingTime)

		// tw := tabwriter.NewWriter(os.Stdout, 0, 0, 1, ' ', 0)
		// fmt.Fprintf(tw, "closeToLeadA\t%v\n", closeToLeadA)
		// fmt.Fprintf(tw, "convToBase\t%v\n", convToBase)
		// fmt.Fprintf(tw, "leadAToLeadB\t%v\n", leadAToLeadB)
		// fmt.Fprintf(tw, "futureLeadAToLeadB\t%v\n", futureLeadAToLeadB)
		// fmt.Fprintf(tw, "laggingToLeadA\t%v\n\n", laggingToLeadA)
		// tw.Flush()

		// Look to enter a trade
		if closeToLeadA > 0 &&
			leadAToLeadB > 0 &&
			convToBase > 0 &&
			futureLeadAToLeadB > 0 &&
			laggingToLeadA > 0 {
			t.Buy(10000, 0, 0)
		} else if closeToLeadA < 0 &&
			leadAToLeadB < 0 &&
			convToBase < 0 &&
			futureLeadAToLeadB < 0 &&
			laggingToLeadA < 0 {
			t.Sell(10000, 0, 0)
		}
	}
}

func main() {
	broker := oanda.NewOandaBroker(os.Getenv("OANDA_TOKEN"), os.Getenv("OANDA_ACCOUNT_ID"), true)
	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		Broker:        auto.NewTestBroker(broker, nil, 10000, 50, 0.0002, 0),
		Strategy:      &IchimokuStrategy{convPeriod: 9, basePeriod: 26, leadingPeriods: 52},
		Symbol:        "EUR_USD",
		Frequency:     "M15",
		CandlesToKeep: 2500,
	}))
}
