package autotrader

import (
	"testing"
	"time"
)

func TestDataFrameSeriesManagement(t *testing.T) {
	data := NewFrame(NewSeries("A"), NewSeries("B"))
	if data.Len() != 0 {
		t.Fatalf("Expected 0 rows, got %d", data.Len())
	}
	if data.Contains("A", "B") != true {
		t.Fatalf("Expected data to contain A and B columns")
	}

	err := data.PushSeries(NewSeries("C"))
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}
	if len(data.Names()) != 3 {
		t.Fatalf("Expected 3 columns, got %d", len(data.Names()))
	}
	if data.Contains("C") != true {
		t.Fatalf("Expected data to contain C column")
	}

	err = data.PushValues(map[string]any{"A": 1, "B": 2, "C": 3})
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}
	if data.Len() != 1 {
		t.Fatalf("Expected 1 row, got %d", data.Len())
	}
	if data.Int("B", -1) != 2 {
		t.Fatalf("Expected latest B to be 2, got %d", data.Int("B", -1))
	}

	err = data.PushValues(map[string]any{"A": 4, "B": 5, "C": 6})
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}
	if data.Len() != 2 {
		t.Fatalf("Expected 2 rows, got %d", data.Len())
	}
	if data.Int("B", -1) != 5 {
		t.Fatalf("Expected latest B to be 5, got %d", data.Int("B", -1))
	}

	selected := data.Select("A", "C")
	if len(selected.Names()) != 2 {
		t.Fatalf("Expected 2 selected columns, got %d", len(selected.Names()))
	}
	if selected.Int("A", -1) != 4 {
		t.Fatalf("Expected latest A to be 4, got %d", selected.Int("A", -1))
	}

	data.RemoveSeries("B")
	if data.Contains("B") != false {
		t.Fatalf("Expected data to not contain B column")
	}
	data.RemoveSeries("A", "C")
	if len(data.Names()) != 0 {
		t.Fatalf("Expected 0 columns, got %d", len(data.Names()))
	}
	if data.Len() != 0 {
		t.Fatalf("Expected 0 rows, got %d", data.Len())
	}
}

func TestDOHLCVDataFrame(t *testing.T) {
	data := NewDOHLCVFrame()
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
