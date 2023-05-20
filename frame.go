package autotrader

import (
	"bytes"
	"fmt"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/exp/maps"
)

type Frame struct {
	series    map[string]*Series
	rowCounts map[string]int
}

func NewFrame(series ...*Series) *Frame {
	d := &Frame{}
	d.PushSeries(series...)
	return d
}

// NewDOHLCVFrame returns a Frame with empty Date, Open, High, Low, Close, and Volume columns.
// Use the PushCandle method to add candlesticks in an easy and type-safe way.
func NewDOHLCVFrame() *Frame {
	return NewFrame(
		NewSeries("Date"),
		NewSeries("Open"),
		NewSeries("High"),
		NewSeries("Low"),
		NewSeries("Close"),
		NewSeries("Volume"),
	)
}

// Copy is the same as CopyRange(0, -1)
func (d *Frame) Copy() *Frame {
	return d.CopyRange(0, -1)
}

// Copy returns a new Frame with a copy of the original series. start is an EasyIndex and count is the number of rows to copy from start onward. If count is negative then all rows from start to the end of the frame are copied. If there are not enough rows to copy then the maximum amount is returned. If there are no items to copy then a Frame will be returned with a length of zero but with the same column names as the original.
//
// Examples:
//
//	Copy(0, 10) - copy the first 10 rows
//	Copy(-1, 1) - copy the last row
//	Copy(-10, -1) - copy the last 10 rows
func (d *Frame) CopyRange(start, count int) *Frame {
	out := &Frame{}
	for _, s := range d.series {
		out.PushSeries(s.CopyRange(start, count))
	}
	return out
}

// Len returns the number of rows in the Frame or 0 if the Frame has no rows. If the Frame has series of different lengths, then the longest length series is returned.
func (d *Frame) Len() int {
	if len(d.series) == 0 {
		return 0
	}
	var length int
	for _, v := range d.rowCounts {
		if v > length {
			length = v
		}
	}
	return length
}

// Select returns a new Frame with the selected Series. The series are not copied so the returned frame will be a reference to the current frame. If a series name is not found, it is ignored.
func (d *Frame) Select(names ...string) *Frame {
	out := &Frame{}
	for _, name := range names {
		if s := d.Series(name); s != nil {
			out.PushSeries(s)
		}
	}
	return out
}

// String returns a string representation of the Frame. If the Frame is nil, it will return the string "*autotrader.Frame[nil]". Otherwise, it will return a string like:
//
//		*autotrader.Frame[2x6]
//		   Date        Open  High  Low  Close  Volume
//		1  2019-01-01  1     2     3    4      5
//	    2  2019-01-02  4     5     6    7      8
//
// The order of the columns is not defined.
//
// If the Frame has more than 20 rows, the output will include the first ten rows and the last ten rows.
func (d *Frame) String() string {
	if d == nil {
		return fmt.Sprintf("%T[nil]", d)
	}
	names := d.Names() // Defines the order of the columns.
	series := make([]*Series, len(names))
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

	// Print the first ten rows and the last ten rows if the Frame has more than 20 rows.
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

// Date returns the value of the Date column at index i. i is an EasyIndex. If i is out of bounds, time.Time{} is returned. This is equivalent to calling Time("Date", i).
func (d *Frame) Date(i int) time.Time {
	return d.Time("Date", i)
}

// Open returns the open price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Open", i).
func (d *Frame) Open(i int) float64 {
	return d.Float("Open", i)
}

// High returns the high price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("High", i).
func (d *Frame) High(i int) float64 {
	return d.Float("High", i)
}

// Low returns the low price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Low", i).
func (d *Frame) Low(i int) float64 {
	return d.Float("Low", i)
}

// Close returns the close price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Close", i).
func (d *Frame) Close(i int) float64 {
	return d.Float("Close", i)
}

// Volume returns the volume of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Volume", i).
func (d *Frame) Volume(i int) int {
	return d.Int("Volume", i)
}

// Dates returns a Series of all the dates in the Frame. This is equivalent to calling Series("Date").
func (d *Frame) Dates() *Series {
	return d.Series("Date")
}

// Opens returns a Series of all the open prices in the Frame. This is equivalent to calling Series("Open").
func (d *Frame) Opens() *Series {
	return d.Series("Open")
}

// Highs returns a Series of all the high prices in the Frame. This is equivalent to calling Series("High").
func (d *Frame) Highs() *Series {
	return d.Series("High")
}

// Lows returns a Series of all the low prices in the Frame. This is equivalent to calling Series("Low").
func (d *Frame) Lows() *Series {
	return d.Series("Low")
}

// Closes returns a Series of all the close prices in the Frame. This is equivalent to calling Series("Close").
func (d *Frame) Closes() *Series {
	return d.Series("Close")
}

// Volumes returns a Series of all the volumes in the Frame. This is equivalent to calling Series("Volume").
func (d *Frame) Volumes() *Series {
	return d.Series("Volume")
}

// Contains returns true if the Frame contains all the given series names. Remember that names are case sensitive.
func (d *Frame) Contains(names ...string) bool {
	for _, name := range names {
		if _, ok := d.series[name]; !ok {
			return false
		}
	}
	return true
}

// ContainsDOHLCV returns true if the Frame contains the series "Date", "Open", "High", "Low", "Close", and "Volume". This is equivalent to calling Contains("Date", "Open", "High", "Low", "Close", "Volume").
func (d *Frame) ContainsDOHLCV() bool {
	return d.Contains("Date", "Open", "High", "Low", "Close", "Volume")
}

// PushCandle pushes a candlestick to the Frame. If the Frame does not contain the series "Date", "Open", "High", "Low", "Close", and "Volume", an error is returned.
func (d *Frame) PushCandle(date time.Time, open, high, low, close float64, volume int64) error {
	if !d.ContainsDOHLCV() {
		return fmt.Errorf("Frame does not contain Date, Open, High, Low, Close, Volume columns")
	}
	d.series["Date"].Push(date)
	d.series["Open"].Push(open)
	d.series["High"].Push(high)
	d.series["Low"].Push(low)
	d.series["Close"].Push(close)
	d.series["Volume"].Push(volume)
	return nil
}

// PushValues uses the keys of the values map as the names of the series to push the values to. If the Frame does not contain a series with a given name, an error is returned.
func (d *Frame) PushValues(values map[string]any) error {
	if len(d.series) == 0 {
		return fmt.Errorf("Frame has no columns")
	}
	for name, value := range values {
		if _, ok := d.series[name]; !ok {
			return fmt.Errorf("Frame does not contain column %q", name)
		}
		d.series[name].Push(value)
	}
	return nil
}

// PushSeries adds the given series to the Frame. If the Frame already contains a series with the same name, an error is returned.
func (d *Frame) PushSeries(series ...*Series) error {
	if d.series == nil {
		d.series = make(map[string]*Series, len(series))
		d.rowCounts = make(map[string]int, len(series))
	}

	for _, s := range series {
		name := s.Name()
		if _, ok := d.series[name]; ok {
			return fmt.Errorf("Frame already contains column %q", name)
		}
		s.SignalConnect("LengthChanged", d, d.onSeriesLengthChanged, name)
		s.SignalConnect("NameChanged", d, d.onSeriesNameChanged, name)
		d.series[name] = s
		d.rowCounts[name] = s.Len()
	}

	return nil
}

// RemoveSeries removes the given series from the Frame. If the Frame does not contain a series with a given name, nothing happens.
func (d *Frame) RemoveSeries(names ...string) {
	for _, name := range names {
		s, ok := d.series[name]
		if !ok {
			return
		}
		s.SignalDisconnect("LengthChanged", d, d.onSeriesLengthChanged)
		s.SignalDisconnect("NameChanged", d, d.onSeriesNameChanged)
		delete(d.series, name)
		delete(d.rowCounts, name)
	}
}

func (d *Frame) onSeriesLengthChanged(args ...any) {
	if len(args) != 2 {
		panic(fmt.Sprintf("expected two arguments, got %d", len(args)))
	}
	newLen := args[0].(int)
	name := args[1].(string)
	d.rowCounts[name] = newLen
}

func (d *Frame) onSeriesNameChanged(args ...any) {
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
	d.series[newName].SignalDisconnect("LengthChanged", d, d.onSeriesLengthChanged)
	d.series[newName].SignalDisconnect("NameChanged", d, d.onSeriesNameChanged)
	d.series[newName].SignalConnect("LengthChanged", d, d.onSeriesLengthChanged, newName)
	d.series[newName].SignalConnect("NameChanged", d, d.onSeriesNameChanged, newName)
}

// Names returns a slice of the names of the series in the Frame.
func (d *Frame) Names() []string {
	return maps.Keys(d.series)
}

// Series returns a Series of the column with the given name. If the column does not exist, nil is returned.
func (d *Frame) Series(name string) *Series {
	if len(d.series) == 0 {
		return nil
	}
	v, ok := d.series[name]
	if !ok {
		return nil
	}
	return v
}

// Value returns the value of the column at index i. i is an EasyIndex. If i is out of bounds, nil is returned.
func (d *Frame) Value(column string, i int) any {
	if len(d.series) == 0 {
		return nil
	}
	if s, ok := d.series[column]; ok {
		return s.Value(i)
	}
	return nil
}

// Float returns the float64 value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not a float64, then 0 is returned.
func (d *Frame) Float(column string, i int) float64 {
	val, ok := d.Value(column, i).(float64)
	if !ok {
		return 0
	}
	return val
}

// Int returns the int value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not an int, then 0 is returned.
func (d *Frame) Int(column string, i int) int {
	val, ok := d.Value(column, i).(int)
	if !ok {
		return 0
	}
	return val
}

// Str returns the string value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not a string, then the empty string "" is returned.
func (d *Frame) Str(column string, i int) string {
	val, ok := d.Value(column, i).(string)
	if !ok {
		return ""
	}
	return val
}

// Time returns the time.Time value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not a Time, then time.Time{} is returned. Use Time.IsZero() to check if the value was valid.
func (d *Frame) Time(column string, i int) time.Time {
	val, ok := d.Value(column, i).(time.Time)
	if !ok {
		return time.Time{}
	}
	return val
}
