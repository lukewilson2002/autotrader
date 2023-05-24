//go:build ignore

package main

import (
	"os"

	"github.com/fivemoreminix/autotrader/oanda"
)

func main() {
	broker := oanda.NewOandaBroker(os.Getenv("OANDA_TOKEN"), os.Getenv("OANDA_ACCOUNT_ID"), true)
	candles, err := broker.Candles("EUR_USD", "D", 100)
	if err != nil {
		panic(err)
	}
	println(candles.String())
}
