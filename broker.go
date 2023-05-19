package autotrader

import (
	"errors"
	"time"
)

type OrderCloseType string

const (
	CloseMarket       OrderCloseType = "M"
	CloseStopLoss     OrderCloseType = "SL"
	CloseTrailingStop OrderCloseType = "TS"
	CloseTakeProfit   OrderCloseType = "TP"
)

type OrderType string

const (
	Market OrderType = "MARKET" // Market means to buy or sell at the current market price, which may not be what you ask for.
	Limit  OrderType = "LIMIT"  // Limit means to buy or sell at a specific price or better.
	Stop   OrderType = "STOP"   // Stop means to buy or sell when the price reaches a specific price or worse.
)

var (
	ErrCancelFailed      = errors.New("cancel failed")
	ErrSymbolNotFound    = errors.New("symbol not found")
	ErrInvalidStopLoss   = errors.New("invalid stop loss")
	ErrInvalidTakeProfit = errors.New("invalid take profit")
)

type Order interface {
	Cancel() error         // Cancel attempts to cancel the order and returns an error if it fails. If the error is nil, the order was canceled.
	Fulfilled() bool       // Fulfilled returns true if the order has been filled with the broker and a position is active.
	Id() string            // Id returns the unique identifier of the order by the broker.
	Leverage() float64     // Leverage returns the leverage of the order.
	Position() Position    // Position returns the position of the order. If the order has not been filled, nil is returned.
	Price() float64        // Price returns the price of the symbol at the time the order was placed.
	Symbol() string        // Symbol returns the symbol name of the order.
	TrailingStop() float64 // TrailingStop returns the trailing stop loss distance of the order.
	StopLoss() float64     // StopLoss returns the stop loss price of the order.
	TakeProfit() float64   // TakeProfit returns the take profit price of the order.
	Time() time.Time       // Time returns the time the order was placed.
	Type() OrderType       // Type returns the type of order.
	Units() float64        // Units returns the number of units purchased or sold by the order.
}

type Position interface {
	Close() error              // Close attempts to close the position and returns an error if it fails. If the error is nil, the position was closed.
	Closed() bool              // Closed returns true if the position has been closed with the broker.
	CloseType() OrderCloseType // CloseType returns the type of order used to close the position.
	ClosePrice() float64       // ClosePrice returns the price of the symbol at the time the position was closed. May be zero if the position is still open.
	EntryPrice() float64       // EntryPrice returns the price of the symbol at the time the position was opened.
	EntryValue() float64       // EntryValue returns the value of the position at the time it was opened.
	Id() string                // Id returns the unique identifier of the position by the broker.
	Leverage() float64         // Leverage returns the leverage of the position.
	PL() float64               // PL returns the profit or loss of the position.
	Symbol() string            // Symbol returns the symbol name of the position.
	TrailingStop() float64     // TrailingStop returns the trailing stop loss price of the position.
	StopLoss() float64         // StopLoss returns the stop loss price of the position.
	TakeProfit() float64       // TakeProfit returns the take profit price of the position.
	Time() time.Time           // Time returns the time the position was opened.
	Units() float64            // Units returns the number of units purchased or sold by the position.
	Value() float64            // Value returns the value of the position at the current price.
}

// Broker is an interface that defines the methods that a broker must implement to report symbol data and place orders, etc. All Broker implementations must also implement the Signaler interface and emit the following functions when necessary:
//
//   - PositionClosed(Position) - Emitted after a position is closed either manually or automatically.
type Broker interface {
	Signaler
	Price(symbol string, wantToBuy bool) float64 // Price returns the ask price if wantToBuy is true and the bid price if wantToBuy is false.
	Bid(symbol string) float64                   // Bid returns the sell price of the symbol.
	Ask(symbol string) float64                   // Ask returns the buy price of the symbol, which is typically higher than the sell price.
	// Candles returns a dataframe of candles for the given symbol, frequency, and count by querying the broker.
	Candles(symbol, frequency string, count int) (*DataFrame, error)
	// Order places an order with orderType for the given symbol and returns an error if it fails. A short position has negative units. If the orderType is Market, the price argument will be ignored and the order will be fulfilled at current price. Otherwise, price is used to set the target price for Stop and Limit orders. If stopLoss or takeProfit are zero, they will not be set. If the stopLoss is greater than the current price for a long position or less than the current price for a short position, the order will fail. Likewise for takeProfit. If the stopLoss is a negative number, it is used as a trailing stop loss to represent how many price points away the stop loss should be from the current price.
	Order(orderType OrderType, symbol string, units, price, stopLoss, takeProfit float64) (Order, error)
	NAV() float64 // NAV returns the net asset value of the account.
	PL() float64  // PL returns the profit or loss of the account.
	OpenOrders() []Order
	OpenPositions() []Position
	// Orders returns a slice of orders that have been placed with the broker. If an order has been canceled or
	// filled, it will not be returned.
	Orders() []Order
	// Positions returns a slice of positions that are currently open with the broker. If a position has been
	// closed, it will not be returned.
	Positions() []Position
}
