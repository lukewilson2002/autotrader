//go:build ignore

package main

import (
	"fmt"
	"os"

	"github.com/fivemoreminix/autotrader/oanda"
)

func main() {
	broker, err := oanda.NewOandaBroker(os.Getenv("OANDA_TOKEN"), os.Getenv("OANDA_ACCOUNT_ID"), true)
	if err != nil {
		fmt.Println("error:", err)
		return
	}

	candles, err := broker.Candles("EUR_USD", "D", 100)
	if err != nil {
		panic(err)
	}
	println(candles.String())
}
