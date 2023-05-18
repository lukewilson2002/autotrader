package autotrader

import (
	"testing"
	"time"
)

func TestReadDataCSV(t *testing.T) {
	data, err := EURUSD()
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}

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
