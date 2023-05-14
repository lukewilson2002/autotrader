package autotrader

import (
	"testing"
	"time"
)

func newTestingDataFrame() *DataFrame {
	_dataframe, err := ReadEURUSDDataCSV()
	if err != nil {
		return nil
	}
	return NewDataFrame(_dataframe)
}

func TestDataSeries(t *testing.T) {
	data := newTestingDataFrame()
	if data == nil {
		t.Fatal("Could not create DataFrame")
	}

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
	if data == nil {
		t.Fatal("Could not create DataFrame")
	}

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
}

func TestReadDataCSV(t *testing.T) {
	data, err := ReadEURUSDDataCSV()
	if err != nil {
		t.Fatal(err)
	}

	if data.NRows() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", data.NRows())
	}
	if len(data.Names()) != 6 {
		t.Fatalf("Expected 6 columns, got %d", len(data.Names()))
	}
	if data.Series[0].Name() != "Date" {
		t.Fatalf("Expected Date column, got %s", data.Series[0].Name())
	}
	if data.Series[1].Name() != "Open" {
		t.Fatalf("Expected Open column, got %s", data.Series[1].Name())
	}
	if data.Series[2].Name() != "High" {
		t.Fatalf("Expected High column, got %s", data.Series[2].Name())
	}
	if data.Series[3].Name() != "Low" {
		t.Fatalf("Expected Low column, got %s", data.Series[3].Name())
	}
	if data.Series[4].Name() != "Close" {
		t.Fatalf("Expected Close column, got %s", data.Series[4].Name())
	}
	if data.Series[5].Name() != "Volume" {
		t.Fatalf("Expected Volume column, got %s", data.Series[5].Name())
	}

	if data.Series[0].Type() != "time" {
		t.Fatalf("Expected Date column type time, got %s", data.Series[0].Type())
	}
}
