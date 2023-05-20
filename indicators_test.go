package autotrader

import (
	"testing"
)

func TestRSI(t *testing.T) {
	prices := NewFloatSeries("Prices", 1, 0, 2, 1, 3, 2, 4, 3, 5, 4, 6, 5, 7, 6)
	rsi := RSI(prices, 14)
	if rsi.Len() != 14 {
		t.Errorf("RSI length is %d, expected 14", rsi.Len())
	}
	if !EqualApprox(rsi.Value(0), 100) {
		t.Errorf("RSI[0] is %f, expected 100", rsi.Value(0))
	}
	// TODO: check the expected RSI
	if !EqualApprox(rsi.Value(-1), 63.157895) {
		t.Errorf("RSI[-1] is %f, expected 63.157895", rsi.Value(-1))
	}
}
