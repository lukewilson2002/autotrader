package autotrader

import (
	"encoding/csv"
	"errors"
	"io"
	"os"
	"strconv"
	"time"

	df "github.com/rocketlaunchr/dataframe-go"
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

func ReadDataCSV(path string, layout DataCSVLayout) (*df.DataFrame, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	return ReadDataCSVFromReader(f, layout)
}

func ReadDataCSVFromReader(r io.Reader, layout DataCSVLayout) (*df.DataFrame, error) {
	data, err := ReadCSVFromReader(r, layout.DateFormat, layout.LatestFirst)
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
		idx, err := data.NameToColumn(name)
		if err != nil {
			panic(err)
		}
		data.Series[idx].Rename(newName)
	}

	err = data.ReorderColumns([]string{"Date", "Open", "High", "Low", "Close", "Volume"})
	if err != nil {
		return data, err
	}

	// TODO: Reverse the dataframe if the latest data is first.
	return data, nil
}

func ReadCSVFromReader(r io.Reader, dateLayout string, readReversed bool) (*df.DataFrame, error) {
	csv := csv.NewReader(r)
	csv.LazyQuotes = true
	records, err := csv.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, errors.New("csv file must have at least 2 rows")
	}

	seriesSlice := make([]df.Series, 0, 12)
	// TODO: change Capacity to Size.
	initOptions := &df.SeriesInit{Capacity: len(records) - 1}

	// Replace column names with standard ones.
	for j, val := range records[0] {
		// Check what type the next row is to determine the type of the series.
		nextRow := records[1][j]
		var series df.Series
		if _, err := strconv.ParseFloat(nextRow, 64); err == nil {
			series = df.NewSeriesFloat64(val, initOptions)
		} else if _, err := strconv.ParseInt(nextRow, 10, 64); err == nil {
			series = df.NewSeriesInt64(val, initOptions)
		} else if _, err := time.Parse(dateLayout, nextRow); err == nil {
			series = df.NewSeriesTime(val, initOptions)
		} else {
			series = df.NewSeriesString(val, initOptions)
		}

		// Create the series columns and label them.
		seriesSlice = append(seriesSlice, series)
	}

	// Set the direction to iterate the records.
	var startIdx, stopIdx, inc int
	if readReversed {
		startIdx = len(records) - 1
		stopIdx = 0 // Stop before the first row because it contains the column names.
		inc = -1
	} else {
		startIdx = 1 // Skip first row because it contains the column names.
		stopIdx = len(records)
		inc = 1
	}

	for i := startIdx; i != stopIdx; i += inc {
		rec := records[i]

		// Add rows to the series.
		for j, val := range rec {
			series := seriesSlice[j]
			switch series.Type() {
			case "float64":
				val, err := strconv.ParseFloat(val, 64)
				if err != nil {
					series.Append(nil)
				} else {
					series.Append(val)
				}
			case "int64":
				val, err := strconv.ParseInt(val, 10, 64)
				if err != nil {
					series.Append(nil)
				} else {
					series.Append(val)
				}
			case "time":
				val, err := time.Parse(dateLayout, val)
				if err != nil {
					series.Append(nil)
				} else {
					series.Append(val)
				}
			case "string":
				series.Append(val)
			}
			seriesSlice[j] = series
		}
	}

	// NOTE: we specifically construct the DataFrame at the end of the function because it likes to set
	// state like number of rows and columns at initialization and won't let you change it later.
	return df.NewDataFrame(seriesSlice...), nil
}
