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

type Series interface {
	Copy() Series
	Len() int
}

type Frame interface {
	Copy() Frame
	Len() int

	// Comparison functions.
	Equal(other Frame) bool
	NotEqual(other Frame) bool
	Less(other Frame) bool
	LessEqual(other Frame) bool
	Greater(other Frame) bool
	GreaterEqual(other Frame) bool

	// Easy access functions.
	Date(i int) time.Time
	Open(i int) float64
	High(i int) float64
	Low(i int) float64
	Close(i int) float64
	Volume(i int) float64
	Dates() Series
	Opens() Series
	Highs() Series
	Lows() Series
	Closes() Series
	Volumes() Series

	// Custom data columns
	Value(column string, i int) interface{}
	Float(column string, i int) float64
	Int(column string, i int) int
	String(column string, i int) string
	// Time returns the value of the column at index i. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
	Time(column string, i int) time.Time
}

type DataFrame struct {
	*df.DataFrame // DataFrame with a Date, Open, High, Low, Close, and Volume column.
}

func (o *DataFrame) Copy() *DataFrame {
	return &DataFrame{o.DataFrame.Copy()}
}

func (o *DataFrame) Len() int {
	if o.DataFrame == nil {
		return 0
	}
	return o.NRows()
}

// Date returns the value of the Date column at index i. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Time("Date", i).
func (o *DataFrame) Date(i int) time.Time {
	return o.Time("Date", i)
}

// Open returns the open price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Open", i).
func (o *DataFrame) Open(i int) float64 {
	return o.Float("Open", i)
}

// High returns the high price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("High", i).
func (o *DataFrame) High(i int) float64 {
	return o.Float("High", i)
}

// Low returns the low price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Low", i).
func (o *DataFrame) Low(i int) float64 {
	return o.Float("Low", i)
}

// Close returns the close price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Close", i).
func (o *DataFrame) Close(i int) float64 {
	return o.Float("Close", i)
}

// Volume returns the volume of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Volume", i).
func (o *DataFrame) Volume(i int) float64 {
	return o.Float("Volume", i)
}

// Value returns the value of the column at index i. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, nil is returned.
func (o *DataFrame) Value(column string, i int) interface{} {
	colIdx, err := o.DataFrame.NameToColumn(column)
	if err != nil {
		return nil
	} else if o.DataFrame == nil || i >= o.Len() {
		return 0
	} else if i < 0 {
		i = o.Len() - i
		if i < 0 {
			return 0
		}
		return o.Series[colIdx].Value(i)
	}
	return o.Series[colIdx].Value(i)
}

// Float returns the value of the column at index i casted to float64. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
func (o *DataFrame) Float(column string, i int) float64 {
	val := o.Value(column, i)
	if val == nil {
		return 0
	}
	switch val := val.(type) {
	case float64:
		return val
	default:
		return 0
	}
}

// Int returns the value of the column at index i casted to int. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
func (o *DataFrame) Int(column string, i int) int {
	val := o.Value(column, i)
	if val == nil {
		return 0
	}
	switch val := val.(type) {
	case int:
		return val
	default:
		return 0
	}
}

// String returns the value of the column at index i casted to string. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, "" is returned.
func (o *DataFrame) String(column string, i int) string {
	val := o.Value(column, i)
	if val == nil {
		return ""
	}
	switch val := val.(type) {
	case string:
		return val
	default:
		return ""
	}
}

// Time returns the value of the column at index i casted to time.Time. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, time.Time{} is returned.
func (o *DataFrame) Time(column string, i int) time.Time {
	val := o.Value(column, i)
	if val == nil {
		return time.Time{}
	}
	switch val := val.(type) {
	case time.Time:
		return val
	default:
		return time.Time{}
	}
}

func NewChartData(data *df.DataFrame) *DataFrame {
	return &DataFrame{data}
}

type RollingWindow struct {
	DataFrame
	Period int
}

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
