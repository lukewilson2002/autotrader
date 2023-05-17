package oanda

import (
	"fmt"
	"strconv"
	"time"
)

// CandlestickResponse represents the response from the Oanda API for a request for candlestick data.
type CandlestickResponse struct {
	Instrument  string        `json:"instrument"`  // The instrument whose Prices are represented by the candlesticks.
	Granularity string        `json:"granularity"` // The granularity of the candlesticks provided.
	Candles     []Candlestick `json:"candles"`     // The list of candlesticks that satisfy the request.
}

// Candlestick represents a single candlestick.
type Candlestick struct {
	Time     time.Time        `json:"time"`     // The start time of the candlestick.
	Bid      *CandlestickData `json:"bid"`      // The candlestick data based on bids. Only provided if bid-based candles were requested.
	Ask      *CandlestickData `json:"ask"`      // The candlestick data based on asks. Only provided if ask-based candles were requested.
	Mid      *CandlestickData `json:"mid"`      // The candlestick data based on midpoints. Only provided if midpoint-based candles were requested.
	Volume   int              `json:"volume"`   // The number of prices created during the time-range represented by the candlestick.
	Complete bool             `json:"complete"` // A flag indicating if the candlestick is complete. A complete candlestick is one whose ending time is not in the future.
}

// CandlestickData represents the price information for a candlestick.
type CandlestickData struct {
	// The first (open) price in the time-range represented by the candlestick.
	O string `json:"o"`
	// The highest price in the time-range represented by the candlestick.
	H string `json:"h"`
	// The lowest price in the time-range represented by the candlestick.
	L string `json:"l"`
	// The last (closing) price in the time-range represented by the candlestick.
	C string `json:"c"`
}

func (d CandlestickData) Parse(o, h, l, c *float64) error {
	var err error
	*o, err = strconv.ParseFloat(d.O, 64)
	if err != nil {
		return fmt.Errorf("error parsing O field of CandlestickData: %w", err)
	}
	*h, err = strconv.ParseFloat(d.H, 64)
	if err != nil {
		return fmt.Errorf("error parsing H field of CandlestickData: %w", err)
	}
	*l, err = strconv.ParseFloat(d.L, 64)
	if err != nil {
		return fmt.Errorf("error parsing L field of CandlestickData: %w", err)
	}
	*c, err = strconv.ParseFloat(d.C, 64)
	if err != nil {
		return fmt.Errorf("error parsing C field of CandlestickData: %w", err)
	}
	return nil
}
