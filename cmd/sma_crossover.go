package main

import (
	"fmt"

	auto "github.com/fivemoreminix/autotrader"
)

type SMAStrategy struct {
	i int
}

func (s *SMAStrategy) Init(_trader *auto.Trader) {
	fmt.Println("Init")
	s.i = 0
}

func (s *SMAStrategy) Next(_trader *auto.Trader) {
	fmt.Println("Next " + fmt.Sprint(s.i))
	s.i++
}

func main() {
	// token := os.Environ["OANDA_TOKEN"]
	// accountId := os.Environ["OANDA_ACCOUNT_ID"]

	// if token == "" || accountId == "" {
	// 	fmt.Println("Please set OANDA_TOKEN and OANDA_ACCOUNT_ID environment variables")
	// 	os.Exit(1)
	// }

	data, err := auto.ReadDataCSV("./EUR_USD Historical Data.csv", auto.DataCSVLayout{
		LatestFirst: true,
		DateFormat:  "01/02/2006",
		Date:        "\ufeff\"Date\"",
		Open:        "Open",
		High:        "High",
		Low:         "Low",
		Close:       "Price",
		Volume:      "Vol.",
	})
	if err != nil {
		panic(err)
	}

	auto.Backtest(auto.NewTrader(auto.TraderConfig{
		// auto.NewOandaBroker(auto.OandaConfig{
		// 	Token:       "YOUR_TOKEN",
		// 	AccountID:   "101-001-14983263-001",
		// 	DemoAccount: true,
		// }),
		Broker:        auto.NewTestBroker(nil, data, 10000, 50, 0),
		Strategy:      &SMAStrategy{},
		Symbol:        "EUR_USD",
		Frequency:     "D",
		CandlesToKeep: 100,
	}))
}
