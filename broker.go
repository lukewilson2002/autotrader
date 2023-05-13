package autotrader

import (
	"errors"
	"time"

	df "github.com/rocketlaunchr/dataframe-go"
)

type OrderType string

const (
	MarketOrder OrderType = "MARKET"
	LimitOrder  OrderType = "LIMIT"
	StopOrder   OrderType = "STOP"
)

var (
	ErrSymbolNotFound    = errors.New("symbol not found")
	ErrInvalidStopLoss   = errors.New("invalid stop loss")
	ErrInvalidTakeProfit = errors.New("invalid take profit")
)

type Order interface {
	Cancel() error       // Cancel attempts to cancel the order and returns an error if it fails. If the error is nil, the order was canceled.
	Fulfilled() bool     // Fulfilled returns true if the order has been filled with the broker and a position is active.
	Id() string          // Id returns the unique identifier of the order by the broker.
	Leverage() float64   // Leverage returns the leverage of the order.
	Position() *Position // Position returns the position of the order. If the order has not been filled, nil is returned.
	Price() float64      // Price returns the price of the symbol at the time the order was placed.
	Symbol() string      // Symbol returns the symbol name of the order.
	StopLoss() float64   // StopLoss returns the stop loss price of the order.
	TakeProfit() float64 // TakeProfit returns the take profit price of the order.
	Time() time.Time     // Time returns the time the order was placed.
	Type() OrderType     // Type returns the type of order.
	Units() float64      // Units returns the number of units purchased or sold by the order.
}

type Position interface {
}

type Broker interface {
	// Candles returns a dataframe of candles for the given symbol, frequency, and count by querying the broker.
	Candles(symbol string, frequency string, count int) (*df.DataFrame, error)
	MarketOrder(symbol string, units float64, stopLoss, takeProfit float64) (Order, error)
	NAV() float64 // NAV returns the net asset value of the account.
}
