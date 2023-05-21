package autotrader

import (
	"bytes"
	"fmt"
	"strings"
	"text/tabwriter"
	"time"

	"golang.org/x/exp/maps"
)

type UnixTime int64

func (t UnixTime) Time() time.Time {
	return time.Unix(int64(t), 0)
}

func (t UnixTime) String() string {
	return t.Time().String()
}

// It is worth mentioning that if you want to use time.Time as an index type, then you should use the public UnixTime as a Unix int64 time which can be converted back into a time.Time easily. See [time.Time](https://pkg.go.dev/time#Time) for more information on why you should not compare Time with == (or a map, which is what the IndexedFrame uses).
type IndexedFrame[I comparable] struct {
	*SignalManager
	series map[string]*IndexedSeries[I]
}

// It is worth mentioning that if you want to use time.Time as an index type, then you should use int64 as a Unix time. See [time.Time](https://pkg.go.dev/time#Time) for more information on why you should not compare Time with == (or a map, which is what the IndexedFrame uses).
func NewIndexedFrame[I comparable](series ...*IndexedSeries[I]) *IndexedFrame[I] {
	d := &IndexedFrame[I]{
		&SignalManager{},
		make(map[string]*IndexedSeries[I], len(series)),
	}
	d.PushSeries(series...)
	return d
}

// NewDOHLCVIndexedFrame returns a IndexedFrame with empty Date, Open, High, Low, Close, and Volume columns.
// Use the PushCandle method to add candlesticks in an easy and type-safe way.
//
// It is worth mentioning that if you want to use time.Time as an index type, then you should use int64 as a Unix time. See [time.Time](https://pkg.go.dev/time#Time) for more information on why you should not compare Time with == (or a map, which is what the IndexedFrame uses).
func NewDOHLCVIndexedFrame[I comparable]() *IndexedFrame[I] {
	frame := NewIndexedFrame[I]()
	for _, name := range []string{"Open", "High", "Low", "Close", "Volume"} {
		frame.PushSeries(NewIndexedSeries[I](name, nil))
	}
	return frame
}

// Copy is the same as CopyRange(0, -1)
func (d *IndexedFrame[I]) Copy() *IndexedFrame[I] {
	return d.CopyRange(0, -1)
}

// Copy returns a new IndexedFrame with a copy of the original series. start is an EasyIndex and count is the number of rows to copy from start onward. If count is negative then all rows from start to the end of the IndexedFrame are copied. If there are not enough rows to copy then the maximum amount is returned. If there are no items to copy then a IndexedFrame will be returned with a length of zero but with the same column names as the original.
//
// Examples:
//
//	Copy(0, 10) - copy the first 10 rows
//	Copy(-1, 1) - copy the last row
//	Copy(-10, -1) - copy the last 10 rows
func (d *IndexedFrame[I]) CopyRange(start, count int) *IndexedFrame[I] {
	out := &IndexedFrame[I]{SignalManager: &SignalManager{}}
	for _, s := range d.series {
		out.PushSeries(s.CopyRange(start, count))
	}
	return out
}

// Len returns the number of rows in the IndexedFrame or 0 if the IndexedFrame has no rows. If the IndexedFrame has series of different lengths, then the longest length series is returned.
func (d *IndexedFrame[I]) Len() int {
	if len(d.series) == 0 {
		return 0
	}
	var length int
	for _, s := range d.series {
		if s.Len() > length {
			length = s.Len()
		}
	}
	return length
}

// Select returns a new IndexedFrame with the selected Series. The series are not copied so the returned IndexedFrame will be a reference to the current IndexedFrame. If a series name is not found, it is ignored.
func (d *IndexedFrame[I]) Select(names ...string) *IndexedFrame[I] {
	out := &IndexedFrame[I]{SignalManager: &SignalManager{}}
	for _, name := range names {
		if s := d.Series(name); s != nil {
			out.PushSeries(s)
		}
	}
	return out
}

// String returns a string representation of the IndexedFrame. If the IndexedFrame is nil, it will return the string "*autotrader.IndexedFrame[nil]". Otherwise, it will return a string like:
//
//		*autotrader.IndexedFrame[2x6]
//		   Date        Open  High  Low  Close  Volume
//		1  2019-01-01  1     2     3    4      5
//	    2  2019-01-02  4     5     6    7      8
//
// The order of the columns is not defined.
//
// If the IndexedFrame has more than 20 rows, the output will include the first ten rows and the last ten rows.
func (d *IndexedFrame[I]) String() string {
	if d == nil {
		return fmt.Sprintf("%T[nil]", d)
	}
	names := d.Names() // Defines the order of the columns.
	series := make([]*IndexedSeries[I], len(names))
	for i, name := range names {
		series[i] = d.Series(name)
	}

	buffer := new(bytes.Buffer)
	t := tabwriter.NewWriter(buffer, 0, 0, 2, ' ', 0)
	fmt.Fprintf(t, "%T[%dx%d]\n", d, d.Len(), len(series))
	fmt.Fprintf(t, "[Row]\t[Index]\t%s\t\n", strings.Join(names, "\t"))

	printRow := func(row int, index I) {
		seriesVals := make([]string, len(series))
		// For every IndexedSeries in the series slice...
		for j, s := range series {
			// Get the value at the row i.
			i := s.Row(index)
			switch typ := s.Value(i).(type) {
			case string:
				seriesVals[j] = fmt.Sprintf("%q", typ)
			default:
				seriesVals[j] = fmt.Sprintf("%v", typ)
			}
		}
		fmt.Fprintf(t, "%d\t%v\t%s\t\n", row, index, strings.Join(seriesVals, "\t"))
	}

	indexes := maps.Keys(series[0].index)
	// Print the first ten rows and the last ten rows if the IndexedFrame has more than 20 rows.
	if d.Len() > 20 {
		for i := 0; i < 10; i++ {
			printRow(i, indexes[i])
		}
		fmt.Fprintf(t, "...\t")
		for range names {
			fmt.Fprint(t, "\t") // Keeps alignment.
		}
		fmt.Fprintln(t) // Print new line character.
		for i := 10; i > 0; i-- {
			printRow(i, indexes[len(indexes)-i])
		}
	} else {
		for i := 0; i < d.Len(); i++ {
			printRow(i, indexes[i])
		}
	}

	t.Flush()
	return buffer.String()
}

func (d *IndexedFrame[I]) Index(row int) *I {
	var index *I
	d.ForEachSeries(func(s *IndexedSeries[I]) {
		if index == nil {
			index = s.Index(row)
		} else if i := s.Index(row); i == nil || *index != *i {
			panic(fmt.Errorf("autotrader: IndexedFrame has inconsistent indexes, expected %v but got %v", index, i))
		}
	})
	return index
}

// Date returns the value of the Date column at index i. i is an EasyIndex. If i is out of bounds, time.Time{} is returned. This is equivalent to calling Index(i).
func (d *IndexedFrame[I]) Date(i int) *I {
	return d.Index(i)
}

// Open returns the open price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Open", i).
func (d *IndexedFrame[I]) Open(i int) float64 {
	return d.Float("Open", i)
}

func (d *IndexedFrame[I]) OpenIndex(index I) float64 {
	return d.FloatIndex("Open", index)
}

// High returns the high price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("High", i).
func (d *IndexedFrame[I]) High(i int) float64 {
	return d.Float("High", i)
}

func (d *IndexedFrame[I]) HighIndex(index I) float64 {
	return d.FloatIndex("High", index)
}

// Low returns the low price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Low", i).
func (d *IndexedFrame[I]) Low(i int) float64 {
	return d.Float("Low", i)
}

func (d *IndexedFrame[I]) LowIndex(index I) float64 {
	return d.FloatIndex("Low", index)
}

// Close returns the close price of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Close", i).
func (d *IndexedFrame[I]) Close(i int) float64 {
	return d.Float("Close", i)
}

func (d *IndexedFrame[I]) CloseIndex(index I) float64 {
	return d.FloatIndex("Close", index)
}

// Volume returns the volume of the candle at index i. i is an EasyIndex. If i is out of bounds, 0 is returned. This is the equivalent to calling Float("Volume", i).
func (d *IndexedFrame[I]) Volume(i int) int {
	return d.Int("Volume", i)
}

func (d *IndexedFrame[I]) VolumeIndex(index I) int {
	return d.IntIndex("Volume", index)
}

// Dates returns a Series of all the dates in the IndexedFrame. This is equivalent to calling Series("Date").
func (d *IndexedFrame[I]) Dates() *IndexedSeries[I] {
	return d.Series("Date")
}

// Opens returns a FloatSeries of all the open prices in the IndexedFrame. This is equivalent to calling Series("Open").
func (d *IndexedFrame[I]) Opens() *IndexedSeries[I] {
	return d.Series("Open")
}

// Highs returns a FloatSeries of all the high prices in the IndexedFrame. This is equivalent to calling Series("High").
func (d *IndexedFrame[I]) Highs() *IndexedSeries[I] {
	return d.Series("High")
}

// Lows returns a FloatSeries of all the low prices in the IndexedFrame. This is equivalent to calling Series("Low").
func (d *IndexedFrame[I]) Lows() *IndexedSeries[I] {
	return d.Series("Low")
}

// Closes returns a FloatSeries of all the close prices in the IndexedFrame. This is equivalent to calling Series("Close").
func (d *IndexedFrame[I]) Closes() *IndexedSeries[I] {
	return d.Series("Close")
}

// Volumes returns a Series of all the volumes in the IndexedFrame. This is equivalent to calling Series("Volume").
func (d *IndexedFrame[I]) Volumes() *IndexedSeries[I] {
	return d.Series("Volume")
}

// Contains returns true if the IndexedFrame contains all the given series names. Remember that names are case sensitive.
func (d *IndexedFrame[I]) Contains(names ...string) bool {
	for _, name := range names {
		if _, ok := d.series[name]; !ok {
			return false
		}
	}
	return true
}

// ContainsDOHLCV returns true if the IndexedFrame contains the series "Date", "Open", "High", "Low", "Close", and "Volume". This is equivalent to calling Contains("Date", "Open", "High", "Low", "Close", "Volume").
func (d *IndexedFrame[I]) ContainsDOHLCV() bool {
	return d.Contains("Open", "High", "Low", "Close", "Volume")
}

// PushCandle pushes a candlestick to the IndexedFrame. If the IndexedFrame does not contain the series "Date", "Open", "High", "Low", "Close", and "Volume", an error is returned.
func (d *IndexedFrame[I]) PushCandle(date I, open, high, low, close float64, volume int64) error {
	if !d.ContainsDOHLCV() {
		return fmt.Errorf("IndexedFrame does not contain Open, High, Low, Close, Volume columns")
	}
	d.series["Open"].Push(date, open)
	d.series["High"].Push(date, high)
	d.series["Low"].Push(date, low)
	d.series["Close"].Push(date, close)
	d.series["Volume"].Push(date, volume)
	return nil
}

// PushSeries adds the given series to the IndexedFrame. If the IndexedFrame already contains a series with the same name, an error is returned.
func (d *IndexedFrame[I]) PushSeries(series ...*IndexedSeries[I]) error {
	if d.series == nil {
		d.series = make(map[string]*IndexedSeries[I], len(series))
	}

	for _, s := range series {
		name := s.Name()
		if _, ok := d.series[name]; ok {
			return fmt.Errorf("IndexedFrame already contains column %q", name)
		}
		s.SignalConnect("NameChanged", d, d.onSeriesNameChanged, name)
		d.series[name] = s
	}

	return nil
}

// RemoveSeries removes the given series from the IndexedFrame. If the IndexedFrame does not contain a series with a given name, nothing happens.
func (d *IndexedFrame[I]) RemoveSeries(names ...string) {
	for _, name := range names {
		s, ok := d.series[name]
		if !ok {
			return
		}
		s.SignalDisconnect("NameChanged", d, d.onSeriesNameChanged)
		delete(d.series, name)
	}
}

func (d *IndexedFrame[I]) onSeriesNameChanged(args ...any) {
	if len(args) != 2 {
		panic(fmt.Sprintf("expected two arguments, got %d", len(args)))
	}
	newName := args[0].(string)
	oldName := args[1].(string)

	d.series[newName] = d.series[oldName]
	delete(d.series, oldName)

	// Reconnect our signal handlers to update the name we use in the handlers.
	d.series[newName].SignalDisconnect("NameChanged", d, d.onSeriesNameChanged)
	d.series[newName].SignalConnect("NameChanged", d, d.onSeriesNameChanged, newName)
}

// Names returns a slice of the names of the series in the IndexedFrame.
func (d *IndexedFrame[I]) Names() []string {
	return maps.Keys(d.series)
}

// Series returns a Series of the column with the given name. If the column does not exist, nil is returned.
func (d *IndexedFrame[I]) Series(name string) *IndexedSeries[I] {
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
func (d *IndexedFrame[I]) Value(column string, i int) any {
	if len(d.series) == 0 {
		return nil
	}
	if s, ok := d.series[column]; ok {
		return s.Value(i)
	}
	return nil
}

func (d *IndexedFrame[I]) ValueIndex(column string, index I) any {
	if len(d.series) == 0 {
		return nil
	}
	if s, ok := d.series[column]; ok {
		return s.ValueIndex(index)
	}
	return nil
}

// Float returns the float64 value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not a float64, then 0 is returned.
func (d *IndexedFrame[I]) Float(column string, i int) float64 {
	val, ok := d.Value(column, i).(float64)
	if !ok {
		return 0
	}
	return val
}

func (d *IndexedFrame[I]) FloatIndex(column string, index I) float64 {
	val, ok := d.ValueIndex(column, index).(float64)
	if !ok {
		return 0
	}
	return val
}

// Int returns the int value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not an int, then 0 is returned.
func (d *IndexedFrame[I]) Int(column string, i int) int {
	val, ok := d.Value(column, i).(int)
	if !ok {
		return 0
	}
	return val
}

func (d *IndexedFrame[I]) IntIndex(column string, index I) int {
	val, ok := d.ValueIndex(column, index).(int)
	if !ok {
		return 0
	}
	return val
}

// Str returns the string value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not a string, then the empty string "" is returned.
func (d *IndexedFrame[I]) Str(column string, i int) string {
	val, ok := d.Value(column, i).(string)
	if !ok {
		return ""
	}
	return val
}

func (d *IndexedFrame[I]) StrIndex(column string, index I) string {
	val, ok := d.ValueIndex(column, index).(string)
	if !ok {
		return ""
	}
	return val
}

// Time returns the time.Time value of the column at index i. i is an EasyIndex. If i is out of bounds or the value was not a Time, then time.Time{} is returned. Use Time.IsZero() to check if the value was valid.
func (d *IndexedFrame[I]) Time(column string, i int) time.Time {
	val, ok := d.Value(column, i).(time.Time)
	if !ok {
		return time.Time{}
	}
	return val
}

func (d *IndexedFrame[I]) TimeIndex(column string, index I) time.Time {
	val, ok := d.ValueIndex(column, index).(time.Time)
	if !ok {
		return time.Time{}
	}
	return val
}

func (d *IndexedFrame[I]) ForEachSeries(f func(*IndexedSeries[I])) {
	for _, s := range d.series {
		f(s)
	}
}
