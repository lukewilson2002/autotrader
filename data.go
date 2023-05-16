package autotrader

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	df "github.com/rocketlaunchr/dataframe-go"
	"golang.org/x/exp/maps"
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
	Signaler

	// Reading data.
	Copy(start, end int) Series
	Len() int
	Name() string // Name returns the immutable name of the Series.
	Float(i int) float64
	Int(i int) int64
	Str(i int) string
	Time(i int) time.Time
	Value(i int) interface{}
	ValueRange(start, end int) []interface{}
	Values() []interface{} // Values is the same as ValueRange(0, -1).

	// Writing data.
	SetName(name string) Series
	Push(val interface{}) Series

	// Statistical functions.
	Rolling(period int) *RollingSeries
}

type Frame interface {
	// Reading data.
	Contains(names ...string) bool // Contains returns true if the frame contains all the columns specified.
	Copy(start, end int) Frame
	Len() int
	Names() []string
	Series(name string) Series
	String() string
	Value(column string, i int) interface{}
	Float(column string, i int) float64
	Int(column string, i int) int64
	Str(column string, i int) string
	Time(column string, i int) time.Time

	// Writing data.
	PushSeries(s ...Series) error
	PushValues(values map[string]interface{}) error
	RemoveSeries(name string)

	// Easy access functions for common columns.
	ContainsDOHLCV() bool // ContainsDOHLCV returns true if the frame contains all the columns: Date, Open, High, Low, Close, and Volume.
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
	PushCandle(date time.Time, open, high, low, close, volume float64) error
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
//
// Signals:
//   - LengthChanged(int) - when the data is appended or an item is removed.
//   - NameChanged(string) - when the name is changed.
type DataSeries struct {
	SignalManager
	data df.Series
}

// Copy copies the Series from start to end (inclusive). If end is -1, it will copy to the end of the Series. If start is out of bounds, nil is returned.
func (s *DataSeries) Copy(start, end int) Series {
	var _end *int
	if start < 0 || start >= s.Len() {
		return nil
	} else if end >= 0 {
		if end < start {
			return nil
		}
		_end = &end
	}
	return &DataSeries{SignalManager{}, s.data.Copy(df.Range{Start: &start, End: _end})}
}

func (s *DataSeries) Name() string {
	return s.data.Name()
}

func (s *DataSeries) SetName(name string) Series {
	if name == s.Name() {
		return s
	}
	s.data.Rename(name)
	s.SignalEmit("NameChanged", name)
	return s
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

func (s *DataSeries) Push(value interface{}) Series {
	if s.data != nil {
		s.data.Append(value)
		s.SignalEmit("LengthChanged", s.Len())
	}
	return s
}

func (s *DataSeries) Value(i int) interface{} {
	if s.data == nil {
		return nil
	}
	i = EasyIndex(i, s.Len()) // Allow for negative indexing.
	return s.data.Value(i)
}

// ValueRange returns a slice of values from start to end, including start and end. The first value is at index 0. A negative value for start or end can be used to get values from the latest, like Python's negative indexing. If end is less than zero, it will be sliced from start to the last item. If start or end is out of bounds, nil is returned. If start is greater than end, nil is returned.
func (s *DataSeries) ValueRange(start, end int) []interface{} {
	if s.data == nil {
		return nil
	}
	start = EasyIndex(start, s.Len())
	if start < 0 || start >= s.Len() || end >= s.Len() || start > end {
		return nil
	} else if end < 0 {
		end = s.Len() - 1
	}

	items := make([]interface{}, end-start+1)
	for i := start; i <= end; i++ {
		items[i-start] = s.Value(i)
	}
	return items
}

func (s *DataSeries) Values() []interface{} {
	if s.data == nil {
		return nil
	}
	return s.ValueRange(0, -1)
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

func (s *DataSeries) Str(i int) string {
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

func NewDataSeries(data df.Series) *DataSeries {
	return &DataSeries{SignalManager{}, data}
}

type DataFrame struct {
	series    map[string]Series
	rowCounts map[string]int
	// data *df.DataFrame // DataFrame with a Date, Open, High, Low, Close, and Volume column.
}

// Copy copies the DataFrame from start to end (inclusive). If end is -1, it will copy to the end of the DataFrame. If start is out of bounds, nil is returned.
func (d *DataFrame) Copy(start, end int) Frame {
	out := &DataFrame{}
	for _, v := range d.series {
		newSeries := v.Copy(start, end)
		out.PushSeries(newSeries)
	}
	return out
}

// Len returns the number of rows in the DataFrame or 0 if the DataFrame is nil. A value less than zero means the
// DataFrame has Series of varying lengths.
func (d *DataFrame) Len() int {
	if len(d.series) == 0 {
		return 0
	}
	// Check if all the Series have the same length.
	var length int
	for _, v := range d.rowCounts {
		if length == 0 {
			length = v
		} else if length != v {
			return -1
		}
	}
	return length
}

func (d *DataFrame) String() string {
	names := d.Names() // Defines the order of the columns.
	series := make([]Series, len(names))
	for i, name := range names {
		series[i] = d.Series(name)
	}

	buffer := new(bytes.Buffer)
	t := tabwriter.NewWriter(buffer, 0, 0, 1, ' ', 0)
	fmt.Fprintf(t, "%T[%dx%d]\n", d, d.Len(), len(d.series))
	fmt.Fprintln(t, "\t", strings.Join(names, "\t"), "\t")

	printRow := func(i int) {
		row := make([]string, len(series))
		for j, s := range series {
			switch typ := s.Value(i).(type) {
			case time.Time:
				row[j] = typ.Format("2006-01-02 15:04:05")
			case string:
				row[j] = fmt.Sprintf("%q", typ)
			default:
				row[j] = fmt.Sprintf("%v", typ)
			}
		}
		fmt.Fprintln(t, strconv.Itoa(i), "\t", strings.Join(row, "\t"), "\t")
	}

	// Print the first ten rows and the last ten rows if the DataFrame has more than 20 rows.
	if d.Len() > 20 {
		for i := 0; i < 10; i++ {
			printRow(i)
		}
		fmt.Fprintf(t, "...\t")
		for range names {
			fmt.Fprint(t, "\t") // Keeps alignment.
		}
		fmt.Fprintln(t) // Print new line character.
		for i := 10; i > 0; i-- {
			printRow(d.Len() - i)
		}
	} else {
		for i := 0; i < d.Len(); i++ {
			printRow(i)
		}
	}

	t.Flush()
	return buffer.String()
}

// Date returns the value of the Date column at index i. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Time("Date", i).
func (d *DataFrame) Date(i int) time.Time {
	return d.Time("Date", i)
}

// Open returns the open price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Open", i).
func (d *DataFrame) Open(i int) float64 {
	return d.Float("Open", i)
}

// High returns the high price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("High", i).
func (d *DataFrame) High(i int) float64 {
	return d.Float("High", i)
}

// Low returns the low price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Low", i).
func (d *DataFrame) Low(i int) float64 {
	return d.Float("Low", i)
}

// Close returns the close price of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Close", i).
func (d *DataFrame) Close(i int) float64 {
	return d.Float("Close", i)
}

// Volume returns the volume of the candle at index i. The first candle is at index 0. A negative value for i (-n) can be used to get n candles from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
// This is the equivalent to calling Float("Volume", i).
func (d *DataFrame) Volume(i int) float64 {
	return d.Float("Volume", i)
}

// Dates returns a Series of all the dates in the DataFrame.
func (d *DataFrame) Dates() Series {
	return d.Series("Date")
}

// Opens returns a Series of all the open prices in the DataFrame.
func (d *DataFrame) Opens() Series {
	return d.Series("Open")
}

// Highs returns a Series of all the high prices in the DataFrame.
func (d *DataFrame) Highs() Series {
	return d.Series("High")
}

// Lows returns a Series of all the low prices in the DataFrame.
func (d *DataFrame) Lows() Series {
	return d.Series("Low")
}

// Closes returns a Series of all the close prices in the DataFrame.
func (d *DataFrame) Closes() Series {
	return d.Series("Close")
}

// Volumes returns a Series of all the volumes in the DataFrame.
func (d *DataFrame) Volumes() Series {
	return d.Series("Volume")
}

func (d *DataFrame) Contains(names ...string) bool {
	for _, name := range names {
		if _, ok := d.series[name]; !ok {
			return false
		}
	}
	return true
}

func (d *DataFrame) ContainsDOHLCV() bool {
	return d.Contains("Date", "Open", "High", "Low", "Close", "Volume")
}

func (d *DataFrame) PushCandle(date time.Time, open, high, low, close, volume float64) error {
	if len(d.series) == 0 {
		d.PushSeries([]Series{
			NewDataSeries(df.NewSeriesTime("Date", nil, date)),
			NewDataSeries(df.NewSeriesFloat64("Open", nil, open)),
			NewDataSeries(df.NewSeriesFloat64("High", nil, high)),
			NewDataSeries(df.NewSeriesFloat64("Low", nil, low)),
			NewDataSeries(df.NewSeriesFloat64("Close", nil, close)),
			NewDataSeries(df.NewSeriesFloat64("Volume", nil, volume)),
		}...)
		return nil
	}
	if !d.ContainsDOHLCV() {
		return fmt.Errorf("DataFrame does not contain Date, Open, High, Low, Close, Volume columns")
	}
	d.series["Date"].Push(date)
	d.series["Open"].Push(open)
	d.series["High"].Push(high)
	d.series["Low"].Push(low)
	d.series["Close"].Push(close)
	d.series["Volume"].Push(volume)
	return nil
}

func (d *DataFrame) PushValues(values map[string]interface{}) error {
	if len(d.series) == 0 {
		return fmt.Errorf("DataFrame has no columns") // TODO: could create the columns here.
	}
	for name, value := range values {
		if _, ok := d.series[name]; !ok {
			return fmt.Errorf("DataFrame does not contain column %q", name)
		}
		d.series[name].Push(value)
	}
	return nil
}

func (d *DataFrame) PushSeries(series ...Series) error {
	if d.series == nil {
		d.series = make(map[string]Series, len(series))
		d.rowCounts = make(map[string]int, len(series))
	}

	for _, s := range series {
		name := s.Name()
		s.SignalConnect("LengthChanged", d.onSeriesLengthChanged, name)
		s.SignalConnect("NameChanged", d.onSeriesNameChanged, name)
		d.series[name] = s
		d.rowCounts[name] = s.Len()
	}

	return nil
}

func (d *DataFrame) RemoveSeries(name string) {
	s, ok := d.series[name]
	if !ok {
		return
	}
	s.SignalDisconnect("LengthChanged", d.onSeriesLengthChanged)
	s.SignalDisconnect("NameChanged", d.onSeriesNameChanged)
	delete(d.series, name)
	delete(d.rowCounts, name)
}

func (d *DataFrame) onSeriesLengthChanged(args ...interface{}) {
	if len(args) != 2 {
		panic(fmt.Sprintf("expected two arguments, got %d", len(args)))
	}
	newLen := args[0].(int)
	name := args[1].(string)
	d.rowCounts[name] = newLen
}

func (d *DataFrame) onSeriesNameChanged(args ...interface{}) {
	if len(args) != 2 {
		panic(fmt.Sprintf("expected two arguments, got %d", len(args)))
	}
	newName := args[0].(string)
	oldName := args[1].(string)

	d.series[newName] = d.series[oldName]
	d.rowCounts[newName] = d.rowCounts[oldName]
	delete(d.series, oldName)
	delete(d.rowCounts, oldName)

	// Reconnect our signal handlers to update the name we use in the handlers.
	d.series[newName].SignalDisconnect("LengthChanged", d.onSeriesLengthChanged)
	d.series[newName].SignalDisconnect("NameChanged", d.onSeriesNameChanged)
	d.series[newName].SignalConnect("LengthChanged", d.onSeriesLengthChanged, newName)
	d.series[newName].SignalConnect("NameChanged", d.onSeriesNameChanged, newName)
}

func (d *DataFrame) Names() []string {
	return maps.Keys(d.series)
}

// Series returns a Series of the column with the given name. If the column does not exist, nil is returned.
func (d *DataFrame) Series(name string) Series {
	if len(d.series) == 0 {
		return nil
	}
	v, ok := d.series[name]
	if !ok {
		return nil
	}
	return v
}

// Value returns the value of the column at index i. The first value is at index 0. A negative value for i can be used to get i values from the latest, like Python's negative indexing. If i is out of bounds, nil is returned.
func (d *DataFrame) Value(column string, i int) interface{} {
	if len(d.series) == 0 {
		return nil
	}
	i = EasyIndex(i, d.Len())  // Allow for negative indexing.
	if i < 0 || i >= d.Len() { // Prevent out of bounds access.
		return nil
	}
	return d.series[column].Value(i)
}

// Float returns the value of the column at index i casted to float64. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, 0 is returned.
func (d *DataFrame) Float(column string, i int) float64 {
	val := d.Value(column, i)
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
func (d *DataFrame) Int(column string, i int) int64 {
	val := d.Value(column, i)
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

// String returns the value of the column at index i casted to string. The first value is at index 0. A negative value for i (-n) can be used to get n values from the latest, like Python's negative indexing. If i is out of bounds, "" is returned.
func (d *DataFrame) Str(column string, i int) string {
	val := d.Value(column, i)
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
func (d *DataFrame) Time(column string, i int) time.Time {
	val := d.Value(column, i)
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

func NewDataFrame(series ...Series) *DataFrame {
	d := &DataFrame{}
	d.PushSeries(series...)
	return d
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

	// err = data.ReorderColumns([]string{"Date", "Open", "High", "Low", "Close", "Volume"})
	// if err != nil {
	// 	return data, err
	// }

	// TODO: Reverse the dataframe if the latest data is first.
	return data, nil
}

func DataFrameFromCSVReader(r io.Reader, dateLayout string, readReversed bool) (*DataFrame, error) {
	csv := csv.NewReader(r)
	csv.LazyQuotes = true
	records, err := csv.ReadAll()
	if err != nil {
		return nil, err
	}

	if len(records) < 2 {
		return nil, errors.New("csv file must have at least 2 rows")
	}

	dfSeriesSlice := make([]df.Series, 0, 12)
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
		dfSeriesSlice = append(dfSeriesSlice, series)
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
			series := dfSeriesSlice[j]
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
			dfSeriesSlice[j] = series
		}
	}

	// NOTE: we specifically construct the DataFrame at the end of the function because it likes to set
	// state like number of rows and columns at initialization and won't let you change it later.
	seriesSlice := make([]Series, len(dfSeriesSlice))
	for i, series := range dfSeriesSlice {
		seriesSlice[i] = NewDataSeries(series)
	}
	return NewDataFrame(seriesSlice...), nil
}
