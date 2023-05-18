package autotrader

import (
	"fmt"
	"math"
	"time"

	df "github.com/rocketlaunchr/dataframe-go"
	"golang.org/x/exp/slices"
)

type Series interface {
	Signaler

	// Reading data.

	// Copy returns a new Series with a copy of the original data and Series name. start is an EasyIndex and len is the number of items to copy from start onward. If len is negative then all items from start to the end of the series are copied. If there are not enough items to copy then the maximum amount is returned. If there are no items to copy then an empty DataSeries is returned.
	//
	// If start is out of bounds then nil is returned.
	//
	// Examples:
	//
	//  Copy(0, 10) - copy the first 10 items
	//  Copy(-1, 1) - copy the last item
	//  Copy(-10, -1) - copy the last 10 items
	//
	// All signals are disconnected from the copy. The copy has its value function reset to its own Value.
	Copy(start, len int) Series
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
	SetValue(i int, val interface{}) Series
	Push(val interface{}) Series

	// Functional.

	Filter(f func(i int, val interface{}) bool) Series            // Where returns a new Series with only the values that return true for the given function.
	Map(f func(i int, val interface{}) interface{}) Series        // Map returns a new Series with the values modified by the given function.
	MapReverse(f func(i int, val interface{}) interface{}) Series // MapReverse is the same as Map but it starts from the last item and works backwards.

	// Statistical functions.

	Rolling(period int) *RollingSeries

	// WithValueFunc is used to implement other types of Series that may modify the values by applying a function before returning them, for example. This returns a Series that is a copy of the original with the new value function used whenever a value is requested outside of the Value() method, which will still return the original value.
	WithValueFunc(value func(i int) interface{}) Series
}

var _ Series = (*AppliedSeries)(nil) // Compile-time interface check.

// AppliedSeries is like Series, but it applies a function to each row of data before returning it.
type AppliedSeries struct {
	Series
	apply func(s *AppliedSeries, i int, val interface{}) interface{}
}

func NewAppliedSeries(s Series, apply func(s *AppliedSeries, i int, val interface{}) interface{}) *AppliedSeries {
	appliedSeries := &AppliedSeries{apply: apply}
	appliedSeries.Series = s.WithValueFunc(appliedSeries.Value)
	return appliedSeries
}

func (s *AppliedSeries) Copy(start, len int) Series {
	return NewAppliedSeries(s.Series.Copy(start, len), s.apply)
}

// Value returns the value of the underlying Series item after applying the function.
//
// See also: ValueUnapplied()
func (s *AppliedSeries) Value(i int) interface{} {
	return s.apply(s, EasyIndex(i, s.Series.Len()), s.Series.Value(i))
}

// ValueUnapplied returns the value of the underlying Series item without applying the function.
//
// This is equivalent to:
//
//	s.Series.Value(i)
func (s *AppliedSeries) ValueUnapplied(i int) interface{} {
	return s.Series.Value(i)
}

// SetValue sets the value of the underlying Series item without applying the function.
//
// This may give unexpected results, as the function will still be applied when the value is requested.
//
// For example:
//
//	series := NewSeries(1, 2, 3) // Pseudo-code.
//	applied := NewAppliedSeries(series, func(_ *AppliedSeries, _ int, val interface{}) interface{} {
//	    return val.(int) * 2
//	})
//	applied.SetValue(0, 10)
//	applied.Value(0) // 20
//	series.Value(0)  // 1
func (s *AppliedSeries) SetValue(i int, val interface{}) Series {
	_ = s.Series.SetValue(i, val)
	return s
}

func (s *AppliedSeries) Push(val interface{}) Series {
	_ = s.Series.Push(val)
	return s
}

func (s *AppliedSeries) Filter(f func(i int, val interface{}) bool) Series {
	return NewAppliedSeries(s.Series.Filter(f), s.apply)
}

func (s *AppliedSeries) Map(f func(i int, val interface{}) interface{}) Series {
	return NewAppliedSeries(s.Series.Map(f), s.apply)
}

func (s *AppliedSeries) MapReverse(f func(i int, val interface{}) interface{}) Series {
	return NewAppliedSeries(s.Series.MapReverse(f), s.apply)
}

func (s *AppliedSeries) WithValueFunc(value func(i int) interface{}) Series {
	return &AppliedSeries{Series: s.Series.WithValueFunc(value), apply: s.apply}
}

var _ Series = (*RollingSeries)(nil) // Compile-time interface check.

type RollingSeries struct {
	Series
	period int
}

func NewRollingSeries(s Series, period int) *RollingSeries {
	series := &RollingSeries{period: period}
	series.Series = s.WithValueFunc(series.Value)
	return series
}

func (s *RollingSeries) Copy(start, len int) Series {
	return NewRollingSeries(s.Series.Copy(start, len), s.period)
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

func (s *RollingSeries) SetValue(i int, val interface{}) Series {
	_ = s.Series.SetValue(i, val)
	return s
}

func (s *RollingSeries) Push(val interface{}) Series {
	_ = s.Series.Push(val)
	return s
}

func (s *RollingSeries) Filter(f func(i int, val interface{}) bool) Series {
	return NewRollingSeries(s.Series.Filter(f), s.period)
}

func (s *RollingSeries) Map(f func(i int, val interface{}) interface{}) Series {
	return NewRollingSeries(s.Series.Map(f), s.period)
}

func (s *RollingSeries) MapReverse(f func(i int, val interface{}) interface{}) Series {
	return NewRollingSeries(s.Series.MapReverse(f), s.period)
}

// Average is an alias for Mean.
func (s *RollingSeries) Average() *AppliedSeries {
	return s.Mean()
}

func (s *RollingSeries) Mean() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v interface{}) interface{} {
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
	})
}

func (s *RollingSeries) EMA() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, i int, v interface{}) interface{} {
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
	})
}

func (s *RollingSeries) Median() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v interface{}) interface{} {
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
	})
}

func (s *RollingSeries) StdDev() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, i int, v interface{}) interface{} {
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
	})
}

func (s *RollingSeries) WithValueFunc(value func(i int) interface{}) Series {
	return &RollingSeries{Series: s.Series.WithValueFunc(value), period: s.period}
}

// DataSeries is a Series that wraps a column of data. The data can be of the following types: float64, int64, string, or time.Time.
//
// Signals:
//   - LengthChanged(int) - when the data is appended or an item is removed.
//   - NameChanged(string) - when the name is changed.
type DataSeries struct {
	SignalManager
	data  df.Series
	value func(i int) interface{}
}

func NewDataSeries(data df.Series) *DataSeries {
	dataSeries := &DataSeries{
		SignalManager: SignalManager{},
		data:          data,
	}
	dataSeries.value = dataSeries.Value
	return dataSeries
}

// Copy returns a new DataSeries with a copy of the original data and Series name. start is an EasyIndex and len is the number of items to copy from start onward. If len is negative then all items from start to the end of the series are copied. If there are not enough items to copy then the maximum amount is returned. If there are no items to copy then an empty DataSeries is returned.
//
// If start is out of bounds then nil is returned.
//
// Examples:
//
//	Copy(0, 10) - copy the first 10 items
//	Copy(-1, 1) - copy the last item
//	Copy(-10, -1) - copy the last 10 items
//
// All signals are disconnected from the copy. The copy has its value function reset to its own Value.
func (s *DataSeries) Copy(start, len int) Series {
	start = EasyIndex(start, s.Len())
	var _end *int
	if start < 0 || start >= s.Len() {
		return nil
	} else if len >= 0 {
		end := start + len
		if end < s.Len() {
			if end < start {
				copy := s.data.Copy()
				copy.Reset()
				series := &DataSeries{SignalManager{}, copy, nil}
				series.value = series.Value
				return series
			}
			_end = &end
		}
	}
	return &DataSeries{
		SignalManager: SignalManager{},
		data:          s.data.Copy(df.Range{Start: &start, End: _end}),
		value:         s.value,
	}
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

func (s *DataSeries) Push(value interface{}) Series {
	if s.data != nil {
		s.data.Append(value)
		s.SignalEmit("LengthChanged", s.Len())
	}
	return s
}

func (s *DataSeries) SetValue(i int, val interface{}) Series {
	if s.data != nil {
		s.data.Update(EasyIndex(i, s.Len()), val)
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
	if end < 0 {
		end = s.Len() - 1
	}
	if start < 0 || start >= s.Len() || end >= s.Len() || start > end {
		return nil
	}

	items := make([]interface{}, end-start+1)
	for i := start; i <= end; i++ {
		items[i-start] = s.value(i)
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
	val := s.value(i)
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
	val := s.value(i)
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
	val := s.value(i)
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
	val := s.value(i)
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

func (s *DataSeries) Filter(f func(i int, val interface{}) bool) Series {
	if s.data == nil {
		return nil
	}
	series := &DataSeries{SignalManager{}, df.NewSeriesGeneric(s.data.Name(), (interface{})(nil), nil), s.value}
	for i := 0; i < s.Len(); i++ {
		if val := series.value(i); f(i, val) {
			series.Push(val)
		}
	}
	return series
}

func (s *DataSeries) Map(f func(i int, val interface{}) interface{}) Series {
	if s.data == nil {
		return nil
	}
	series := &DataSeries{SignalManager{}, s.data.Copy(), s.value}
	for i := 0; i < s.Len(); i++ {
		series.SetValue(i, f(i, series.value(i)))
	}
	return series
}

func (s *DataSeries) MapReverse(f func(i int, val interface{}) interface{}) Series {
	if s.data == nil {
		return nil
	}
	series := &DataSeries{SignalManager{}, s.data.Copy(), s.value}
	for i := s.Len() - 1; i >= 0; i-- {
		series.SetValue(i, f(i, series.value(i)))
	}
	return series
}

func (s *DataSeries) Rolling(period int) *RollingSeries {
	return NewRollingSeries(s, period)
}

func (s *DataSeries) WithValueFunc(value func(i int) interface{}) Series {
	copy := s.Copy(0, -1).(*DataSeries)
	copy.value = value
	return copy
}
