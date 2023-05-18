package autotrader

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	df "github.com/rocketlaunchr/dataframe-go"
	"golang.org/x/exp/maps"
)

type Frame interface {
	// Reading data.

	// Copy returns a new Frame with a copy of the original series. start is an EasyIndex and len is the number of rows to copy from start onward. If len is negative then all rows from start to the end of the frame are copied. If there are not enough rows to copy then the maximum amount is returned. If there are no items to copy then an empty frame will be returned with a length of zero.
	//
	// If start is out of bounds then nil is returned.
	//
	// Examples:
	//
	//  Copy(0, 10) - copy the first 10 items
	//  Copy(-1, 1) - copy the last item
	//  Copy(-10, -1) - copy the last 10 items
	Copy(start, len int) Frame
	Contains(names ...string) bool // Contains returns true if the frame contains all the columns specified.
	Len() int
	Names() []string
	Select(names ...string) Frame // Select returns a new Frame with only the specified columns.
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
	PushCandle(date time.Time, open, high, low, close float64, volume int64) error
}

type DataFrame struct {
	series    map[string]Series
	rowCounts map[string]int
	// data *df.DataFrame // DataFrame with a Date, Open, High, Low, Close, and Volume column.
}

func NewDataFrame(series ...Series) *DataFrame {
	d := &DataFrame{}
	d.PushSeries(series...)
	return d
}

// NewDOHLCVDataFrame returns a DataFrame with empty Date, Open, High, Low, Close, and Volume columns.
// Use the PushCandle method to add candlesticks in an easy and type-safe way.
func NewDOHLCVDataFrame() *DataFrame {
	return NewDataFrame(
		NewDataSeries(df.NewSeriesTime("Date", nil)),
		NewDataSeries(df.NewSeriesFloat64("Open", nil)),
		NewDataSeries(df.NewSeriesFloat64("High", nil)),
		NewDataSeries(df.NewSeriesFloat64("Low", nil)),
		NewDataSeries(df.NewSeriesFloat64("Close", nil)),
		NewDataSeries(df.NewSeriesInt64("Volume", nil)),
	)
}

// Copy returns a new DataFrame with a copy of the original series. start is an EasyIndex and len is the number of rows to copy from start onward. If len is negative then all rows from start to the end of the frame are copied. If there are not enough rows to copy then the maximum amount is returned. If there are no items to copy then an empty frame will be returned with a length of zero.
//
// If start is out of bounds then nil is returned.
//
// Examples:
//
//	Copy(0, 10) - copy the first 10 items
//	Copy(-1, 1) - copy the last item
//	Copy(-10, -1) - copy the last 10 items
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

// Select returns a new *DataFrame with the selected Series. The series are not copied so the returned Frame will be a reference to the current frame. If a Series name is not found, it is ignored.
func (d *DataFrame) Select(names ...string) Frame {
	out := &DataFrame{}
	for _, name := range names {
		out.PushSeries(d.Series(name))
	}
	return out
}

// String returns a string representation of the DataFrame. If the DataFrame is nil, it will return the string "*autotrader.DataFrame[nil]". Otherwise, it will return a string like:
//
//		*autotrader.DataFrame[2x6]
//		   Date        Open  High  Low  Close  Volume
//		1  2019-01-01  1     2     3    4      5
//	    2  2019-01-02  4     5     6    7      8
//
// The order of the columns is not defined.
//
// If the dataframe has more than 20 rows, the output will include the first ten rows and the last ten rows.
func (d *DataFrame) String() string {
	if d == nil {
		return fmt.Sprintf("%T[nil]", d)
	}
	names := d.Names() // Defines the order of the columns.
	series := make([]Series, len(names))
	for i, name := range names {
		series[i] = d.Series(name)
	}

	buffer := new(bytes.Buffer)
	t := tabwriter.NewWriter(buffer, 0, 0, 2, ' ', 0)
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

func (d *DataFrame) PushCandle(date time.Time, open, high, low, close float64, volume int64) error {
	if len(d.series) == 0 {
		d.PushSeries(
			NewDataSeries(df.NewSeriesTime("Date", nil, date)),
			NewDataSeries(df.NewSeriesFloat64("Open", nil, open)),
			NewDataSeries(df.NewSeriesFloat64("High", nil, high)),
			NewDataSeries(df.NewSeriesFloat64("Low", nil, low)),
			NewDataSeries(df.NewSeriesFloat64("Close", nil, close)),
			NewDataSeries(df.NewSeriesInt64("Volume", nil, volume)),
		)
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
