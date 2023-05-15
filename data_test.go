package autotrader

import (
	"testing"
	"time"
)

func newTestingDataFrame() *DataFrame {
	data, err := EURUSD()
	if err != nil {
		panic(err)
	}
	return data
}

func TestDataSeries(t *testing.T) {
	data := newTestingDataFrame()

	dates, closes := data.Dates(), data.Closes()

	if dates.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", dates.Len())
	}
	if closes.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", closes.Len())
	}

	sma10 := closes.Rolling(10).Mean()
	if sma10.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", sma10.Len())
	}
	if sma10.Value(-1) != 1.10039 { // Latest closing price averaged over 10 periods.
		t.Fatalf("Expected 1.10039, got %f", sma10.Value(-1))
	}
}

func TestDataFrame(t *testing.T) {
	data := newTestingDataFrame()

	if data.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", data.Len())
	}
	if data.Close(-1) != 1.0967 {
		t.Fatalf("Expected 1.0967, got %f", data.Close(-1))
	}

	date := data.Date(2) // Get the 3rd earliest date from the Date column.
	if date.Year() != 2013 || date.Month() != 5 || date.Day() != 13 {
		t.Fatalf("Expected 2013-05-13, got %s", date.Format(time.DateOnly))
	}

	err := data.PushCandle(time.Date(2023, 5, 14, 0, 0, 0, 0, time.UTC), 1.0, 1.0, 1.0, 1.0, 1)
	if err != nil {
		t.Log(data.Names())
		t.Fatalf("Expected no error, got %s", err)
	}
	if data.Len() != 2611 {
		t.Fatalf("Expected 2611 rows, got %d", data.Len())
	}
	if data.Close(-1) != 1.0 {
		t.Fatalf("Expected latest close to be 1.0, got %f", data.Close(-1))
	}
}

func TestReadDataCSV(t *testing.T) {
	data := newTestingDataFrame()

	if data.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", data.Len())
	}
	if len(data.Names()) != 6 {
		t.Fatalf("Expected 6 columns, got %d", len(data.Names()))
	}
	if data.Series("Date") == nil {
		t.Fatalf("Expected Date column, got nil")
	}
	if data.Series("Open") == nil {
		t.Fatalf("Expected Open column, got nil")
	}
	if data.Series("High") == nil {
		t.Fatalf("Expected High column, got nil")
	}
	if data.Series("Low") == nil {
		t.Fatalf("Expected Low column, got nil")
	}
	if data.Series("Close") == nil {
		t.Fatalf("Expected Close column, got nil")
	}
	if data.Series("Volume") == nil {
		t.Fatalf("Expected Volume column, got nil")
	}

	if data.Series("Date").Time(0).Equal(time.Time{}) {
		t.Fatalf("Expected Date column to have type time.Time, got %s", data.Value("Date", 0))
	}
}
