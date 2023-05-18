package autotrader

import (
	"testing"
	"time"
)

func TestDataFrame(t *testing.T) {
	data := NewDOHLCVDataFrame()
	if !data.ContainsDOHLCV() {
		t.Fatalf("Expected data to contain DOHLCV columns")
	}
	if data.Len() != 0 {
		t.Fatalf("Expected 0 rows, got %d", data.Len())
	}

	err := data.PushCandle(time.Date(2021, 5, 13, 0, 0, 0, 0, time.UTC), 0.8, 1.2, 0.6, 1.0, 1)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}
	err = data.PushCandle(time.Date(2023, 5, 14, 0, 0, 0, 0, time.UTC), 1.0, 1.4, 0.8, 1.2, 1)
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}
	if data.Len() != 2 {
		t.Fatalf("Expected 2 row, got %d", data.Len())
	}
	if data.Close(-1) != 1.2 {
		t.Fatalf("Expected latest close to be 1.2, got %f", data.Close(-1))
	}
}
