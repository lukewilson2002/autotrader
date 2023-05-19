package autotrader

import (
	"fmt"
	"math"
	"sort"
	"time"

	"golang.org/x/exp/slices"
)

type Series interface {
	Signaler

	// Reading data.

	// Copy returns a new Series with a copy of the original data and Series name. start is an EasyIndex and count is the number of items to copy from start onward. If count is negative then all items from start to the end of the series are copied. If there are not enough items to copy then the maximum amount is returned. If there are no items to copy then an empty DataSeries is returned.
	//
	// Examples:
	//
	//  Copy(0, 10) - copy the first 10 items
	//  Copy(-1, 1) - copy the last item
	//  Copy(-10, -1) - copy the last 10 items
	//
	// All signals are disconnected from the copy. The copy has its value function reset to its own Value.
	Copy(start, count int) Series
	Len() int
	Name() string // Name returns the immutable name of the Series.
	Float(i int) float64
	Int(i int) int
	Str(i int) string
	Time(i int) time.Time
	Value(i int) any
	ValueRange(start, end int) []any
	Values() []any // Values is the same as ValueRange(0, -1).

	// Writing data.

	Reverse() Series
	SetName(name string) Series
	SetValue(i int, val any) Series
	Push(val any) Series

	// Functional.

	Filter(f func(i int, val any) bool) Series    // Where returns a new Series with only the values that return true for the given function.
	Map(f func(i int, val any) any) Series        // Map returns a new Series with the values modified by the given function.
	MapReverse(f func(i int, val any) any) Series // MapReverse is the same as Map but it starts from the last item and works backwards.
	ForEach(f func(i int, val any)) Series        // ForEach calls f for each item in the Series.

	// Statistical functions.

	Rolling(period int) *RollingSeries

	// WithValueFunc is used to implement other types of Series that may modify the values by applying a function before returning them, for example. This returns a Series that is a copy of the original with the new value function used whenever a value is requested outside of the Value() method, which will still return the original value.
	WithValueFunc(value func(i int) any) Series
}

var _ Series = (*AppliedSeries)(nil) // Compile-time interface check.

// AppliedSeries is like Series, but it applies a function to each row of data before returning it.
type AppliedSeries struct {
	Series
	apply func(s *AppliedSeries, i int, val any) any
}

func NewAppliedSeries(s Series, apply func(s *AppliedSeries, i int, val any) any) *AppliedSeries {
	appliedSeries := &AppliedSeries{apply: apply}
	appliedSeries.Series = s.WithValueFunc(appliedSeries.Value)
	return appliedSeries
}

func (s *AppliedSeries) Copy(start, count int) Series {
	return NewAppliedSeries(s.Series.Copy(start, count), s.apply)
}

// Value returns the value of the underlying Series item after applying the function.
//
// See also: ValueUnapplied()
func (s *AppliedSeries) Value(i int) any {
	return s.apply(s, EasyIndex(i, s.Series.Len()), s.Series.Value(i))
}

// ValueUnapplied returns the value of the underlying Series item without applying the function.
//
// This is equivalent to:
//
//	s.Series.Value(i)
func (s *AppliedSeries) ValueUnapplied(i int) any {
	return s.Series.Value(i)
}

func (s *AppliedSeries) Reverse() Series {
	return NewAppliedSeries(s.Series.Reverse(), s.apply)
}

// SetValue sets the value of the underlying Series item without applying the function.
//
// This may give unexpected results, as the function will still be applied when the value is requested.
//
// For example:
//
//	series := NewSeries(1, 2, 3) // Pseudo-code.
//	applied := NewAppliedSeries(series, func(_ *AppliedSeries, _ int, val any) any {
//	    return val.(int) * 2
//	})
//	applied.SetValue(0, 10)
//	applied.Value(0) // 20
//	series.Value(0)  // 1
func (s *AppliedSeries) SetValue(i int, val any) Series {
	_ = s.Series.SetValue(i, val)
	return s
}

func (s *AppliedSeries) Push(val any) Series {
	_ = s.Series.Push(val)
	return s
}

func (s *AppliedSeries) Filter(f func(i int, val any) bool) Series {
	return NewAppliedSeries(s.Series.Filter(f), s.apply)
}

func (s *AppliedSeries) Map(f func(i int, val any) any) Series {
	return NewAppliedSeries(s.Series.Map(f), s.apply)
}

func (s *AppliedSeries) MapReverse(f func(i int, val any) any) Series {
	return NewAppliedSeries(s.Series.MapReverse(f), s.apply)
}

func (s *AppliedSeries) ForEach(f func(i int, val any)) Series {
	_ = s.Series.ForEach(f)
	return s
}

func (s *AppliedSeries) WithValueFunc(value func(i int) any) Series {
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

func (s *RollingSeries) Copy(start, count int) Series {
	return NewRollingSeries(s.Series.Copy(start, count), s.period)
}

// Value returns []any up to `period` long. The last item in the slice is the item at i. If i is out of bounds, nil is returned.
func (s *RollingSeries) Value(i int) any {
	items := make([]any, 0, s.period)
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

func (s *RollingSeries) Reverse() Series {
	return NewRollingSeries(s.Series.Reverse(), s.period)
}

func (s *RollingSeries) SetValue(i int, val any) Series {
	_ = s.Series.SetValue(i, val)
	return s
}

func (s *RollingSeries) Push(val any) Series {
	_ = s.Series.Push(val)
	return s
}

func (s *RollingSeries) Filter(f func(i int, val any) bool) Series {
	return NewRollingSeries(s.Series.Filter(f), s.period)
}

func (s *RollingSeries) Map(f func(i int, val any) any) Series {
	return NewRollingSeries(s.Series.Map(f), s.period)
}

func (s *RollingSeries) MapReverse(f func(i int, val any) any) Series {
	return NewRollingSeries(s.Series.MapReverse(f), s.period)
}

func (s *RollingSeries) ForEach(f func(i int, val any)) Series {
	_ = s.Series.ForEach(f)
	return s
}

// Average is an alias for Mean.
func (s *RollingSeries) Average() *AppliedSeries {
	return s.Mean()
}

func (s *RollingSeries) Mean() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v any) any {
		switch v := v.(type) {
		case []any:
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
	return NewAppliedSeries(s, func(_ *AppliedSeries, i int, v any) any {
		switch v := v.(type) {
		case []any:
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
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return nil
			}
			switch v[0].(type) {
			case float64:
				if len(v) == 0 {
					return float64(0)
				}
				slices.SortFunc(v, func(a, b any) bool {
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
				slices.SortFunc(v, func(a, b any) bool {
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
	return NewAppliedSeries(s, func(_ *AppliedSeries, i int, v any) any {
		switch v := v.(type) {
		case []any:
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

func (s *RollingSeries) WithValueFunc(value func(i int) any) Series {
	return &RollingSeries{Series: s.Series.WithValueFunc(value), period: s.period}
}

// DataSeries is a Series that wraps a column of data. The data can be of the following types: float64, int64, string, or time.Time.
//
// Signals:
//   - LengthChanged(int) - when the data is appended or an item is removed.
//   - NameChanged(string) - when the name is changed.
//   - ValueChanged(int, any) - when a value is changed.
type DataSeries struct {
	SignalManager
	name  string
	data  []any
	value func(i int) any
}

func NewDataSeries(name string, vals ...any) *DataSeries {
	dataSeries := &DataSeries{
		SignalManager: SignalManager{},
		name:          name,
		data:          vals,
	}
	dataSeries.value = dataSeries.Value
	return dataSeries
}

func NewDataSeriesFloat(name string, vals ...float64) *DataSeries {
	anyVals := make([]any, len(vals))
	for i, v := range vals {
		anyVals[i] = v
	}
	return NewDataSeries(name, anyVals...)
}

func NewDataSeriesInt(name string, vals ...int) *DataSeries {
	anyVals := make([]any, len(vals))
	for i, v := range vals {
		anyVals[i] = v
	}
	return NewDataSeries(name, anyVals...)
}

// Copy returns a new DataSeries with a copy of the original data and Series name. start is an EasyIndex and count is the number of items to copy from start onward. If count is negative then all items from start to the end of the series are copied. If there are not enough items to copy then the maximum amount is returned. If there are no items to copy then an empty DataSeries is returned.
//
// Examples:
//
//	Copy(0, 10) - copy the first 10 items
//	Copy(-1, 1) - copy the last item
//	Copy(-10, -1) - copy the last 10 items
//
// All signals are disconnected from the copy. The copy has its value function reset to its own Value.
func (s *DataSeries) Copy(start, count int) Series {
	if s.Len() == 0 {
		return NewDataSeries(s.name)
	}
	start = EasyIndex(start, s.Len())
	var end int
	start = Max(Min(start, s.Len()), 0)
	if count < 0 {
		end = s.Len()
	} else {
		end = Min(start+count, s.Len())
	}
	if end <= start {
		return NewDataSeries(s.name) // Return an empty series.
	}
	data := make([]any, end-start)
	copy(data, s.data[start:end])
	return NewDataSeries(s.name, data...)
}

func (s *DataSeries) Name() string {
	return s.name
}

func (s *DataSeries) SetName(name string) Series {
	if name == s.name {
		return s
	}
	s.name = name
	s.SignalEmit("NameChanged", name)
	return s
}

func (s *DataSeries) Len() int {
	return len(s.data)
}

func (s *DataSeries) Reverse() Series {
	if len(s.data) != 0 {
		sort.Slice(s.data, func(i, j int) bool {
			return i > j
		})
		for i, v := range s.data {
			s.SignalEmit("ValueChanged", i, v)
		}
	}
	return s
}

func (s *DataSeries) Push(value any) Series {
	s.data = append(s.data, value)
	s.SignalEmit("LengthChanged", s.Len())
	return s
}

func (s *DataSeries) SetValue(i int, val any) Series {
	if i = EasyIndex(i, s.Len()); i < s.Len() && i >= 0 {
		s.data[i] = val
		s.SignalEmit("ValueChanged", i, val)
	}
	return s
}

func (s *DataSeries) Value(i int) any {
	i = EasyIndex(i, s.Len())
	if i >= s.Len() || i < 0 {
		return nil
	}
	return s.data[i]
}

// ValueRange returns a copy of values from start to start+count. If count is negative then all items from start to the end of the series are returned. If there are not enough items to return then the maximum amount is returned. If there are no items to return then an empty slice is returned.
func (s *DataSeries) ValueRange(start, count int) []any {
	start = EasyIndex(start, s.Len())
	start = Max(Min(start, s.Len()), 0)
	if count < 0 {
		count = s.Len() - start
	} else {
		count = Min(count, s.Len()-start)
	}
	if count <= 0 {
		return []any{}
	}

	end := start + count
	items := make([]any, count)
	copy(items, s.data[start:end])
	return items
}

// Values returns a copy of all values. If there are no values, an empty slice is returned.
//
// Same as:
//
//	ValueRange(0, -1)
func (s *DataSeries) Values() []any {
	return s.ValueRange(0, -1)
}

// Float returns the value at index i as a float64. If the value is not a float64 then NaN is returned.
func (s *DataSeries) Float(i int) float64 {
	val := s.value(i)
	switch val := val.(type) {
	case float64:
		return val
	default:
		return math.NaN()
	}
}

// Int returns the value at index i as an int64. If the value is not an int64 then 0 is returned.
func (s *DataSeries) Int(i int) int {
	val := s.value(i)
	switch val := val.(type) {
	case int:
		return val
	default:
		return 0
	}
}

// Str returns the value at index i as a string. If the value is not a string then "" is returned.
func (s *DataSeries) Str(i int) string {
	val := s.value(i)
	switch val := val.(type) {
	case string:
		return val
	default:
		return ""
	}
}

// Time returns the value at index i as a time.Time. If the value is not a time.Time then time.Time{} is returned.
func (s *DataSeries) Time(i int) time.Time {
	val := s.value(i)
	switch val := val.(type) {
	case time.Time:
		return val
	default:
		return time.Time{}
	}
}

func (s *DataSeries) Filter(f func(i int, val any) bool) Series {
	series := NewDataSeries(s.name, make([]any, 0, s.Len())...)
	for i := 0; i < s.Len(); i++ {
		if val := s.value(i); f(i, val) {
			series.Push(val)
		}
	}
	return series
}

// Map returns a new series with the same length as the original series. The value at each index is replaced by the value returned by the function f.
func (s *DataSeries) Map(f func(i int, val any) any) Series {
	series := s.Copy(0, -1)
	for i := 0; i < s.Len(); i++ {
		series.SetValue(i, f(i, s.value(i)))
	}
	return series
}

// MapReverse returns a new series with the same length as the original series. The value at each index is replaced by the value returned by the function f. The values are processed in reverse order.
func (s *DataSeries) MapReverse(f func(i int, val any) any) Series {
	series := s.Copy(0, -1)
	for i := s.Len() - 1; i >= 0; i-- {
		series.SetValue(i, f(i, s.value(i)))
	}
	return series
}

func (s *DataSeries) ForEach(f func(i int, val any)) Series {
	for i := 0; i < s.Len(); i++ {
		f(i, s.value(i))
	}
	return s
}

func (s *DataSeries) Rolling(period int) *RollingSeries {
	return NewRollingSeries(s, period)
}

func (s *DataSeries) WithValueFunc(value func(i int) any) Series {
	copy := s.Copy(0, -1).(*DataSeries)
	copy.value = value
	return copy
}
