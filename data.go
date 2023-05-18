package autotrader

import (
	"encoding/csv"
	"io"
	"os"
	"strconv"
	"time"
)

type DataCSVLayout struct {
	LatestFirst bool   // Whether the latest data is first in the dataframe. If false, the latest data is last.
	DateFormat  string // The format of the date column. Example: "03/22/2006". See https://pkg.go.dev/time#pkg-constants for more information.
	Date        string
	Open        string
	High        string
	Low         string
	Close       string
	Volume      string
}

func EURUSD() (*DataFrame, error) {
	return DataFrameFromCSVLayout("./EUR_USD Historical Data.csv", DataCSVLayout{
		LatestFirst: true,
		DateFormat:  "01/02/2006",
		Date:        "\ufeff\"Date\"",
		Open:        "Open",
		High:        "High",
		Low:         "Low",
		Close:       "Price",
		Volume:      "Vol.",
	})
}

func DataFrameFromCSVLayout(path string, layout DataCSVLayout) (*DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return DataFrameFromCSVReaderLayout(f, layout)
}

func DataFrameFromCSVReaderLayout(r io.Reader, layout DataCSVLayout) (*DataFrame, error) {
	data, err := DataFrameFromCSVReader(r, layout.DateFormat, layout.LatestFirst)
	if err != nil {
		return data, err
	}

	// Rename the columns and remove any columns that are not needed.
	for _, name := range data.Names() {
		var newName string
		switch name {
		case layout.Date:
			newName = "Date"
		case layout.Open:
			newName = "Open"
		case layout.High:
			newName = "High"
		case layout.Low:
			newName = "Low"
		case layout.Close:
			newName = "Close"
		case layout.Volume:
			newName = "Volume"
		default:
			data.RemoveSeries(name)
			continue
		}
		data.Series(name).SetName(newName)
	}

	return data, nil
}

func DataFrameFromCSVReader(r io.Reader, dateLayout string, readReversed bool) (*DataFrame, error) {
	csv := csv.NewReader(r)
	csv.LazyQuotes = true

	seriesSlice := make([]Series, 0, 12)

	// Read the CSV file.
	for {
		rec, err := csv.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}

		// Create the columns needed.
		if len(seriesSlice) == 0 {
			for _, val := range rec {
				seriesSlice = append(seriesSlice, NewDataSeries(val))
			}
			continue
		}

		// Add rows to the series.
		for j, val := range rec {
			series := seriesSlice[j]
			if f, err := strconv.ParseFloat(val, 64); err == nil {
				series.Push(f)
			} else if t, err := time.Parse(dateLayout, val); err == nil {
				series.Push(t)
			} else {
				series.Push(val)
			}
		}
	}

	// Reverse the series if needed.
	if readReversed {
		for _, series := range seriesSlice {
			series.Reverse()
		}
	}

	return NewDataFrame(seriesSlice...), nil
}
