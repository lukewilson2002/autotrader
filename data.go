package autotrader

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"time"

	df "github.com/rocketlaunchr/dataframe-go"
	"golang.org/x/exp/slices"
)

// EasyIndex returns an index to the `n` -length object that allows for negative indexing. For example, EasyIndex(-1, 5) returns 4. This is similar to Python's negative indexing. The return value may be less than zero if (-i) > n.
func EasyIndex(i, n int) int {
	if i < 0 {
		return n + i
	}
	return i
}

type Series interface {
	Copy() Series
	Len() int

	// Statistical functions.
	Rolling(period int) *RollingSeries

	// Data access functions.
	Value(i int) interface{}
	Float(i int) float64
	Int(i int) int64
	String(i int) string
	Time(i int) time.Time
}

type Frame interface {
	Copy() Frame
	Len() int

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
	Series(name string) Series
	Value(column string, i int) interface{}
	Float(column string, i int) float64
	Int(column string, i int) int64
	String(column string, i int) string
	// Time returns the value of the column at index i. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
	Time(column string, i int) time.Time
}

// AppliedSeries is like Series, but it applies a function to each row of data before returning it.
type AppliedSeries struct {
	Series
	apply func(i int, val interface{}) interface{}
}

func (s *AppliedSeries) Value(i int) interface{} {
	return s.apply(EasyIndex(i, s.Len()), s.Series.Value(i))
}

func NewAppliedSeries(s Series, apply func(i int, val interface{}) interface{}) *AppliedSeries {
	return &AppliedSeries{
		Series: s,
		apply:  apply,
	}
}

type RollingSeries struct {
	Series
	period int
}

func (s *RollingSeries) Mean() *AppliedSeries {
	return &AppliedSeries{
		Series: s,
		apply: func(_ int, v interface{}) interface{} {
			switch v := v.(type) {
			case []interface{}:
				if len(v) == 0 {
					return nil
				}
				switch v[0].(type) {
				case float64:
					var sum float64
					for _, v := range v {
						sum += v.(float64)
					}
					return sum / float64(len(v))
				case int64:
					var sum int64
					for _, v := range v {
						sum += v.(int64)
					}
					return sum / int64(len(v))
				default:
					return v[len(v)-1] // Do nothing
				}
			default:
				panic(fmt.Sprintf("expected a slice of values, got %t", v))
			}
		},
	}
}

func (s *RollingSeries) EMA() *AppliedSeries {
	return &AppliedSeries{
		Series: s,
		apply: func(i int, v interface{}) interface{} {
			switch v := v.(type) {
			case []interface{}:
				if len(v) == 0 {
					return nil
				}
				switch v[0].(type) {
				case float64:
					ema := v[0].(float64)
					for _, v := range v[1:] {
						ema += (v.(float64) - ema) * 2 / (float64(s.period) + 1)
					}
					return ema
				case int64:
					ema := v[0].(int64)
					for _, v := range v[1:] {
						ema += (v.(int64) - ema) * 2 / (int64(s.period) + 1)
					}
					return ema
				default: // string, time.Time
					return v[len(v)-1] // Do nothing
				}
			default:
				panic(fmt.Sprintf("expected a slice of values, got %t", v))
			}
		},
	}
}

func (s *RollingSeries) Median() *AppliedSeries {
	return &AppliedSeries{
		Series: s,
		apply: func(_ int, v interface{}) interface{} {
			switch v := v.(type) {
			case []interface{}:
				if len(v) == 0 {
					return nil
				}
				switch v[0].(type) {
				case float64:
					if len(v) == 0 {
						return float64(0)
					}
					slices.SortFunc(v, func(a, b interface{}) bool {
						x, y := a.(float64), b.(float64)
						return x < y || (math.IsNaN(x) && !math.IsNaN(y))
					})
					if len(v)%2 == 0 {
						return (v[len(v)/2-1].(float64) + v[len(v)/2].(float64)) / 2
					}
					return v[len(v)/2]
				case int64:
					if len(v) == 0 {
						return int64(0)
					}
					slices.SortFunc(v, func(a, b interface{}) bool {
						x, y := a.(int64), b.(int64)
						return x < y
					})
					if len(v)%2 == 0 {
						return (v[len(v)/2-1].(int64) + v[len(v)/2].(int64)) / 2
					}
					return v[len(v)/2]
				default: // string, time.Time
					return v[len(v)-1] // Do nothing
				}
			default:
				panic(fmt.Sprintf("expected a slice of values, got %t", v))
			}
		},
	}
}

func (s *RollingSeries) StdDev() *AppliedSeries {
	return &AppliedSeries{
		Series: s,
		apply: func(i int, v interface{}) interface{} {
			switch v := v.(type) {
			case []interface{}:
				if len(v) == 0 {
					return nil
				}
				switch v[0].(type) {
				case float64:
					mean := s.Mean().Value(i).(float64) // Take the mean of the last period values for the current index
					var sum float64
					for _, v := range v {
						sum += (v.(float64) - mean) * (v.(float64) - mean)
					}
					return math.Sqrt(sum / float64(len(v)))
				case int64:
					mean := s.Mean().Value(i).(int64)
					var sum int64
					for _, v := range v {
						sum += (v.(int64) - mean) * (v.(int64) - mean)
					}
					return int64(math.Sqrt(float64(sum) / float64(len(v))))
				default: // A slice of something else, just return the last value
					return v[len(v)-1] // Do nothing
				}
			default:
				panic(fmt.Sprintf("expected a slice of values, got %t", v))
			}
		},
	}
}

// Value returns []interface{} up to `period` long. The last item in the slice is the item at i. If i is out of bounds, nil is returned.
func (s *RollingSeries) Value(i int) interface{} {
	items := make([]interface{}, 0, s.period)
	i = EasyIndex(i, s.Len())
	if i < 0 || i >= s.Len() {
		return items
	}
	for j := i; j > i-s.period && j >= 0; j-- {
		// items = append(items, s.Series.Value(j))
		items = slices.Insert(items, 0, s.Series.Value(j))
	}
	return items
}

// DataSeries is a Series that wraps a column of data. The data can be of the following types: float64, int64, string, or time.Time.
type DataSeries struct {
	data df.Series
}

type DataFrame struct {
	data *df.DataFrame // DataFrame with a Date, Open, High, Low, Close, and Volume column.
}

func (o *DataFrame) Copy() *DataFrame {
	return &DataFrame{o.data.Copy()}
}

// Len returns the number of rows in the DataFrame or 0 if the DataFrame is nil.
func (o *DataFrame) Len() int {
	if o.data == nil {
		return 0
	}
	return o.data.NRows()
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

// Dates returns a Series of all the dates in the DataFrame.
func (o *DataFrame) Dates() Series {
	return o.Series("Date")
}

// Opens returns a Series of all the open prices in the DataFrame.
func (o *DataFrame) Opens() Series {
	return o.Series("Open")
}

// Highs returns a Series of all the high prices in the DataFrame.
func (o *DataFrame) Highs() Series {
	return o.Series("High")
}

// Lows returns a Series of all the low prices in the DataFrame.
func (o *DataFrame) Lows() Series {
	return o.Series("Low")
}

// Closes returns a Series of all the close prices in the DataFrame.
func (o *DataFrame) Closes() Series {
	return o.Series("Close")
}

// Volumes returns a Series of all the volumes in the DataFrame.
func (o *DataFrame) Volumes() Series {
	return o.Series("Volume")
}

// Series returns a Series of the column with the given name. If the column does not exist, nil is returned.
func (o *DataFrame) Series(name string) Series {
	if o.data == nil {
		return nil
	}
	colIdx, err := o.data.NameToColumn(name)
	if err != nil {
		return nil
	}
	return &DataSeries{o.data.Series[colIdx]}
}

// Value returns the value of the column at index i. The first value is at index 0. A negative value for i can be used to get i values from the latest, like Python's negative indexing. If i is out of bounds, nil is returned.
func (o *DataFrame) Value(column string, i int) interface{} {
	if o.data == nil {
		return nil
	}
	i = EasyIndex(i, o.Len()) // Allow for negative indexing.
	colIdx, err := o.data.NameToColumn(column)
	if err != nil || i < 0 || i >= o.Len() { // Prevent out of bounds access.
		return nil
	}
	return o.data.Series[colIdx].Value(i)
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

func NewDataFrame(data *df.DataFrame) *DataFrame {
	return &DataFrame{data}
}

func (s *DataSeries) Copy() Series {
	return &DataSeries{s.data.Copy()}
}

func (s *DataSeries) Len() int {
	if s.data == nil {
		return 0
	}
	return s.data.NRows()
}

func (s *DataSeries) Rolling(period int) *RollingSeries {
	return &RollingSeries{s, period}
}

func (s *DataSeries) Value(i int) interface{} {
	if s.data == nil {
		return nil
	}
	i = EasyIndex(i, s.Len()) // Allow for negative indexing.
	return s.data.Value(i)
}

func (s *DataSeries) Float(i int) float64 {
	val := s.Value(i)
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

func (s *DataSeries) Int(i int) int64 {
	val := s.Value(i)
	if val == nil {
		return 0
	}
	switch val := val.(type) {
	case int64:
		return val
	default:
		return 0
	}
}

func (s *DataSeries) String(i int) string {
	val := s.Value(i)
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

func (s *DataSeries) Time(i int) time.Time {
	val := s.Value(i)
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

func ReadEURUSDDataCSV() (*df.DataFrame, error) {
	return ReadDataCSV("./EUR_USD Historical Data.csv", DataCSVLayout{
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
