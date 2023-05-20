package autotrader

import (
	"math"
	"sort"
	"time"

	anymath "github.com/spatialcurrent/go-math/pkg/math"
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

	// Operations.

	// Add returns a new Series with the values of the original Series added to the values of the other Series. It will add each value up to the length of the original Series or the other Series, whichever contains fewer values. The number of values in the new Series will remain equal to the number of values in the original Series.
	Add(other Series) Series
	// Sub returns a new Series with the values of the original Series subtracted from the values of the other Series. It will subtract each value up to the length of the original Series or the other Series, whichever contains fewer values. The number of values in the new Series will remain equal to the number of values in the original Series.
	Sub(other Series) Series
	// Mul returns a new Series with the values of the original Series multiplied by the values of the other Series. It will multiply each value up to the length of the original Series or the other Series, whichever contains fewer values. The number of values in the new Series will remain equal to the number of values in the original Series.
	Mul(other Series) Series
	// Div returns a new Series with the values of the original Series divided by the values of the other Series. It will divide each value up to the length of the original Series or the other Series, whichever contains fewer values. The number of values in the new Series will remain equal to the number of values in the original Series.
	Div(other Series) Series

	// Functional.

	Filter(f func(i int, val any) bool) Series    // Where returns a new Series with only the values that return true for the given function.
	Map(f func(i int, val any) any) Series        // Map returns a new Series with the values modified by the given function.
	MapReverse(f func(i int, val any) any) Series // MapReverse is the same as Map but it starts from the last item and works backwards.
	ForEach(f func(i int, val any)) Series        // ForEach calls f for each item in the Series.
	MaxFloat() float64                            // MaxFloat returns the maximum of all floats and integers as a float64.
	MaxInt() int                                  // MaxInt returns the maximum of all integers as an int.
	MinFloat() float64                            // MinFloat returns the minimum of all floats and integers as a float64.
	MinInt() int                                  // MinInt returns the minimum of all integers as an int.

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

func (s *AppliedSeries) Add(other Series) Series {
	return NewAppliedSeries(s.Series.Add(other), s.apply)
}

func (s *AppliedSeries) Sub(other Series) Series {
	return NewAppliedSeries(s.Series.Sub(other), s.apply)
}

func (s *AppliedSeries) Mul(other Series) Series {
	return NewAppliedSeries(s.Series.Mul(other), s.apply)
}

func (s *AppliedSeries) Div(other Series) Series {
	return NewAppliedSeries(s.Series.Div(other), s.apply)
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

func (s *RollingSeries) Add(other Series) Series {
	return NewRollingSeries(s.Series.Add(other), s.period)
}

func (s *RollingSeries) Sub(other Series) Series {
	return NewRollingSeries(s.Series.Sub(other), s.period)
}

func (s *RollingSeries) Mul(other Series) Series {
	return NewRollingSeries(s.Series.Mul(other), s.period)
}

func (s *RollingSeries) Div(other Series) Series {
	return NewRollingSeries(s.Series.Div(other), s.period)
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

// Max returns an AppliedSeries that returns the maximum value of the rolling period as a float64 or 0 if the requested period is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) Max() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return nil
			}
			max := math.Inf(-1)
			for _, v := range v {
				switch v := v.(type) {
				case float64:
					if v > max {
						max = v
					}
				case float32:
					if float64(v) > max {
						max = float64(v)
					}
				case int:
					if float64(v) > max {
						max = float64(v)
					}
				case int64:
					if float64(v) > max {
						max = float64(v)
					}
				case int32:
					if float64(v) > max {
						max = float64(v)
					}
				case int16:
					if float64(v) > max {
						max = float64(v)
					}
				case int8:
					if float64(v) > max {
						max = float64(v)
					}
				}
				return max
			}
		}
		panic("unreachable")
	})
}

// Min returns an AppliedSeries that returns the minimum value of the rolling period as a float64 or 0 if the requested period is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) Min() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return nil
			}
			min := math.Inf(1)
			for _, v := range v {
				switch v := v.(type) {
				case float64:
					if v < min {
						min = v
					}
				case float32:
					if float64(v) < min {
						min = float64(v)
					}
				case int:
					if float64(v) < min {
						min = float64(v)
					}
				case int64:
					if float64(v) < min {
						min = float64(v)
					}
				case int32:
					if float64(v) < min {
						min = float64(v)
					}
				case int16:
					if float64(v) < min {
						min = float64(v)
					}
				case int8:
					if float64(v) < min {
						min = float64(v)
					}
				}
				return min
			}
		}
		panic("unreachable")
	})
}

// Average is an alias for Mean.
func (s *RollingSeries) Average() *AppliedSeries {
	return s.Mean()
}

// Mean returns the mean of the rolling period as a float64 or 0 if the period requested is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) Mean() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return 0
			}
			var sum float64
			for _, v := range v {
				switch v := v.(type) {
				case float64:
					sum += v
				case float32:
					sum += float64(v)
				case int:
					sum += float64(v)
				case int64:
					sum += float64(v)
				case int32:
					sum += float64(v)
				case int16:
					sum += float64(v)
				case int8:
					sum += float64(v)
				}
			}
			return sum / float64(len(v))
		}
		panic("unreachable")
	})
}

// EMA returns the exponential moving average of the period as a float64 or 0 if the period requested is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) EMA() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, i int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return 0
			}
			var ema float64
			period := float64(s.period)
			first := true
			for _, v := range v {
				var f float64
				switch v := v.(type) {
				case float64:
					f = v
				case float32:
					f = float64(v)
				case int:
					f = float64(v)
				case int64:
					f = float64(v)
				case int32:
					f = float64(v)
				case int16:
					f = float64(v)
				case int8:
					f = float64(v)
				default:
					continue
				}
				if first { // Set as first value
					ema = f
					first = false
					continue
				}
				ema += (f - ema) * 2 / (period + 1)
			}
			return ema
		}
		panic("unreachable")
	})
}

// Median returns the median of the period as a float64 or 0 if the period requested is empty.
//
// Will work with float64 and int. Ignores all other values.
func (s *RollingSeries) Median() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, _ int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return 0
			}

			var offenders int
			slices.SortFunc(v, func(a, b any) bool {
				less, offender := LessAny(a, b)
				// Sort offenders to the end.
				if offender == a {
					offenders++
					return false
				} else if offender == b {
					offenders++
					return true
				}
				return less
			})
			v = v[:len(v)-offenders] // Cut out the offenders.

			v1 := v[len(v)/2-1]
			v2 := v[len(v)/2]
			if len(v)%2 == 0 {
				switch n1 := v1.(type) {
				case float64:
					switch n2 := v2.(type) {
					case float64:
						return (n1 + n2) / 2
					case int:
						return (n1 + float64(n2)) / 2
					}
				case int:
					switch n2 := v2.(type) {
					case float64:
						return (float64(n1) + n2) / 2
					case int:
						return (float64(n1) + float64(n2)) / 2
					}
				default:
					return 0
				}
			}
			switch vMid := v[len(v)/2].(type) {
			case float64:
				return vMid
			case int:
				return float64(vMid)
			default:
				panic("unreachable") // Offenders are pushed to the back of the slice and ignored.
			}
		}
		panic("unreachable")
	})
}

// StdDev returns the standard deviation of the period as a float64 or 0 if the period requested is empty.
func (s *RollingSeries) StdDev() *AppliedSeries {
	return NewAppliedSeries(s, func(_ *AppliedSeries, i int, v any) any {
		switch v := v.(type) {
		case []any:
			if len(v) == 0 {
				return nil
			}

			mean := s.Mean().Value(i).(float64) // Take the mean of the last period values for the current index
			var sum float64
			var ignored int
			for _, v := range v {
				switch v := v.(type) {
				case float64:
					sum += (v - mean) * (v - mean)
				case float32:
					sum += (float64(v) - mean) * (float64(v) - mean)
				case int:
					sum += (float64(v) - mean) * (float64(v) - mean)
				case int64:
					sum += (float64(v) - mean) * (float64(v) - mean)
				case int32:
					sum += (float64(v) - mean) * (float64(v) - mean)
				case int16:
					sum += (float64(v) - mean) * (float64(v) - mean)
				case int8:
					sum += (float64(v) - mean) * (float64(v) - mean)
				default:
					ignored++
				}
			}
			if ignored >= len(v) {
				return 0
			}
			return math.Sqrt(sum / float64(len(v)-ignored))
		}
		panic("unreachable")
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

func (s *DataSeries) Add(other Series) Series {
	rows := make([]any, 0, s.Len())
	copy(rows, s.data)
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Add(s.value(i), other.Value(i))
		if err != nil {
			continue
		}
		rows[i] = val
	}
	return NewDataSeries(s.name, rows...)
}

func (s *DataSeries) Sub(other Series) Series {
	rows := make([]any, 0, s.Len())
	copy(rows, s.data)
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Subtract(s.value(i), other.Value(i))
		if err != nil {
			continue
		}
		rows[i] = val
	}
	return NewDataSeries(s.name, rows...)
}

func (s *DataSeries) Mul(other Series) Series {
	rows := make([]any, 0, s.Len())
	copy(rows, s.data)
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Multiply(s.value(i), other.Value(i))
		if err != nil {
			continue
		}
		rows[i] = val
	}
	return NewDataSeries(s.name, rows...)
}

func (s *DataSeries) Div(other Series) Series {
	rows := make([]any, 0, s.Len())
	copy(rows, s.data)
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Divide(s.value(i), other.Value(i))
		if err != nil {
			continue
		}
		rows[i] = val
	}
	return NewDataSeries(s.name, rows...)
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

func (s *DataSeries) MaxFloat() float64 {
	if s.Len() == 0 {
		return 0
	}
	max := math.Inf(-1)
	for i := 0; i < s.Len(); i++ {
		switch val := s.value(i).(type) {
		case float64:
			if val > max {
				max = val
			}
		case int:
			if float64(val) > max {
				max = float64(val)
			}
		}
	}
	return max
}

func (s *DataSeries) MinFloat() float64 {
	if s.Len() == 0 {
		return 0
	}
	min := math.Inf(1)
	for i := 0; i < s.Len(); i++ {
		switch val := s.value(i).(type) {
		case float64:
			if val < min {
				min = val
			}
		case int:
			if float64(val) < min {
				min = float64(val)
			}
		}
	}
	return min
}

func (s *DataSeries) MaxInt() int {
	if s.Len() == 0 {
		return 0
	}
	max := math.MinInt64
	for i := 0; i < s.Len(); i++ {
		switch val := s.value(i).(type) {
		case int:
			if val > max {
				max = val
			}
		case float64:
			if int(val) > max {
				max = int(val)
			}
		}
	}
	return max
}

func (s *DataSeries) MinInt() int {
	if s.Len() == 0 {
		return 0
	}
	min := math.MaxInt64
	for i := 0; i < s.Len(); i++ {
		switch val := s.value(i).(type) {
		case int:
			if val < min {
				min = val
			}
		case float64:
			if int(val) < min {
				min = int(val)
			}
		}
	}
	return min
}

func (s *DataSeries) Rolling(period int) *RollingSeries {
	return NewRollingSeries(s, period)
}

func (s *DataSeries) WithValueFunc(value func(i int) any) Series {
	copy := s.Copy(0, -1).(*DataSeries)
	copy.value = value
	return copy
}
