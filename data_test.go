package autotrader

import "testing"

func TestReadDataCSV(t *testing.T) {
	data, err := ReadDataCSV("./EUR_USD Historical Data.csv", DataCSVLayout{
		LatestFirst: true,
		DateFormat:  "01/02/2006",
		Date:        "\ufeff\"Date\"",
		Open:        "Open",
		High:        "High",
		Low:         "Low",
		Close:       "Price",
		Volume:      "Vol.",
	})
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
