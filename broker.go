package autotrader

import (
	"errors"
	"time"
)

type OrderType string

const (
	MarketOrder OrderType = "MARKET"
	LimitOrder  OrderType = "LIMIT"
	StopOrder   OrderType = "STOP"
)

var (
	ErrCancelFailed      = errors.New("cancel failed")
	ErrSymbolNotFound    = errors.New("symbol not found")
	ErrInvalidStopLoss   = errors.New("invalid stop loss")
	ErrInvalidTakeProfit = errors.New("invalid take profit")
)

type Order interface {
	Cancel() error       // Cancel attempts to cancel the order and returns an error if it fails. If the error is nil, the order was canceled.
	Fulfilled() bool     // Fulfilled returns true if the order has been filled with the broker and a position is active.
	Id() string          // Id returns the unique identifier of the order by the broker.
	Leverage() float64   // Leverage returns the leverage of the order.
	Position() Position  // Position returns the position of the order. If the order has not been filled, nil is returned.
	Price() float64      // Price returns the price of the symbol at the time the order was placed.
	Symbol() string      // Symbol returns the symbol name of the order.
	StopLoss() float64   // StopLoss returns the stop loss price of the order.
	TakeProfit() float64 // TakeProfit returns the take profit price of the order.
	Time() time.Time     // Time returns the time the order was placed.
	Type() OrderType     // Type returns the type of order.
	Units() float64      // Units returns the number of units purchased or sold by the order.
}

type Position interface {
	Close() error        // Close attempts to close the position and returns an error if it fails. If the error is nil, the position was closed.
	Closed() bool        // Closed returns true if the position has been closed with the broker.
	ClosePrice() float64 // ClosePrice returns the price of the symbol at the time the position was closed. May be zero if the position is still open.
	EntryPrice() float64 // EntryPrice returns the price of the symbol at the time the position was opened.
	EntryValue() float64 // EntryValue returns the value of the position at the time it was opened.
	Id() string          // Id returns the unique identifier of the position by the broker.
	Leverage() float64   // Leverage returns the leverage of the position.
	PL() float64         // PL returns the profit or loss of the position.
	Symbol() string      // Symbol returns the symbol name of the position.
	StopLoss() float64   // StopLoss returns the stop loss price of the position.
	TakeProfit() float64 // TakeProfit returns the take profit price of the position.
	Time() time.Time     // Time returns the time the position was opened.
	Units() float64      // Units returns the number of units purchased or sold by the position.
	Value() float64      // Value returns the value of the position at the current price.
}

type Broker interface {
	// Candles returns a dataframe of candles for the given symbol, frequency, and count by querying the broker.
	Candles(symbol string, frequency string, count int) (*DataFrame, error)
	MarketOrder(symbol string, units float64, stopLoss, takeProfit float64) (Order, error)
	NAV() float64 // NAV returns the net asset value of the account.
	OpenOrders() []Order
	OpenPositions() []Position
	// Orders returns a slice of orders that have been placed with the broker. If an order has been canceled or
	// filled, it will not be returned.
	Orders() []Order
	// Positions returns a slice of positions that are currently open with the broker. If a position has been
	// closed, it will not be returned.
	Positions() []Position
}
