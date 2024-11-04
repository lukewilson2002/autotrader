//go:build ignore

package main

import (
	"fmt"
	"os"

	auto "github.com/fivemoreminix/autotrader"
	"github.com/fivemoreminix/autotrader/oanda"
)

type SpreadPayupStrategy struct {
}

func (s *SpreadPayupStrategy) Init(t *auto.Trader) {
	t.Broker.SignalConnect(auto.OrderFulfilled, s, func(a ...any) {
		order := a[0].(auto.Order)
		order.Position().Close() // Immediately close the position so we only pay spread.
	})
}

func (s *SpreadPayupStrategy) Next(t *auto.Trader) {
	_, err := t.Sell(1000, 0, 0)
	if err != nil {
		panic(err)
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
		Strategy:      &SpreadPayupStrategy{},
		Symbol:        "EUR_USD",
		Frequency:     "M15",
		CandlesToKeep: 1000,
	}))
}
