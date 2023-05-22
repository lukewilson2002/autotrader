package autotrader

import (
	"math"
	"sort"
	"time"

	anymath "github.com/spatialcurrent/go-math/pkg/math"
	"golang.org/x/exp/slices"
)

// TODO:
//  - IndexedSeries type with an 'any' index value that can be set on each row. Each index must be unique.
//  - TimeIndexedSeries type with a time.Time index value that can be set on each row. Each index must be unique. Composed of an IndexedSeries.

// Series is a slice of any values with a name. It is used to represent a column in a DataFrame. The type contains various functions to perform mutating operations on the data. All mutating operations return a pointer to the Series so that they can be chained together. To create a copy of a Series before applying operations, use the Copy() or CopyRange() functions.
//
// Signals:
//   - LengthChanged(int) - when the data is appended or an item is removed.
//   - NameChanged(string) - when the name is changed.
//   - ValueChanged(int, any) - when a value is changed.
type Series struct {
	SignalManager
	name string
	data []any
}

func NewSeries(name string, vals ...any) *Series {
	return &Series{
		SignalManager: SignalManager{},
		name:          name,
		data:          vals,
	}
}

func (s *Series) ISetName(name string) {
	s.SetName(name)
}

// Copy is equivalent to CopyRange(0, -1).
func (s *Series) Copy() *Series {
	return s.CopyRange(0, -1)
}

// CopyRange returns a new Series with a copy of the original data and name. start is an EasyIndex and count is the number of items to copy from start onward. If count is negative then all items from start to the end of the series are copied. If there are not enough items to copy then the maximum amount is returned. If there are no items to copy then an empty DataSeries is returned.
//
// Examples:
//
//	CopyRange(0, 10) - copy the first 10 items
//	CopyRange(-1, 1) - copy the last item
//	CopyRange(-10, -1) - copy the last 10 items
//
// All signals are disconnected from the copy.
func (s *Series) CopyRange(start, count int) *Series {
	if s.Len() == 0 {
		return NewSeries(s.name)
	}
	start, end := s.Range(start, count)
	if start == end {
		return NewSeries(s.name)
	}
	data := make([]any, end-start)
	copy(data, s.data[start:end])
	return NewSeries(s.name, data...)
}

// Range takes an EasyIndex start and a number of items to select with count, and returns a range from begin to end, exclusive. If count is negative then the range spans to the end of the series. begin will always be between 0 and len-1. end will always be between start and len. If the range is empty then begin and end will be the same value.
func (s *Series) Range(start, count int) (begin, end int) {
	start = EasyIndex(start, s.Len())   // Allow for negative indexing.
	start = Max(Min(start, s.Len()), 0) // Clamp start between 0 and len-1.
	if count < 0 {
		count = s.Len() - start
	}
	end = Min(start+count, s.Len()) // Clamp end between start and len.
	return start, end
}

// Name returns the name of the Series.
func (s *Series) Name() string {
	return s.name
}

// SetName sets the name of the series to name and emits a NameChanged signal.
func (s *Series) SetName(name string) *Series {
	if name == s.name {
		return s
	}
	s.name = name
	s.SignalEmit("NameChanged", name)
	return s
}

// Len returns the number of rows in the Series.
func (s *Series) Len() int {
	return len(s.data)
}

// Reverse will reverse the order of the values in the Series and emit a ValueChanged signal for each value.
func (s *Series) Reverse() *Series {
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

// Remove removes and returns the value at index i and emits a LengthChanged signal. If i is out of bounds then nil is returned.
func (s *Series) Remove(i int) any {
	if i = EasyIndex(i, s.Len()); i < s.Len() && i >= 0 {
		value := s.data[i]
		s.data = append(s.data[:i], s.data[i+1:]...)
		s.SignalEmit("LengthChanged", s.Len())
		return value
	}
	return nil
}

// RemoveRange removes count items starting at index start and emits a LengthChanged signal.
func (s *Series) RemoveRange(start, count int) *Series {
	start, end := s.Range(start, count)
	if start == end {
		return s
	}
	s.data = append(s.data[:start], s.data[end:]...)
	s.SignalEmit("LengthChanged", s.Len())
	return s
}

// Push will append a value to the end of the Series and emit a LengthChanged signal.
func (s *Series) Push(value any) *Series {
	s.data = append(s.data, value)
	s.SignalEmit("LengthChanged", s.Len())
	return s
}

// Pop will remove the last value from the Series and emit a LengthChanged signal.
func (s *Series) Pop() any {
	if len(s.data) != 0 {
		value := s.data[len(s.data)-1]
		s.data = s.data[:len(s.data)-1]
		s.SignalEmit("LengthChanged", s.Len())
		return value
	}
	return s
}

func (s *Series) SetValue(i int, val any) *Series {
	if i = EasyIndex(i, s.Len()); i < s.Len() && i >= 0 {
		s.data[i] = val
		s.SignalEmit("ValueChanged", i, val)
	}
	return s
}

func (s *Series) Value(i int) any {
	i = EasyIndex(i, s.Len())
	if i >= s.Len() || i < 0 {
		return nil
	}
	return s.data[i]
}

// ValueRange returns a copy of values from start to start+count. If count is negative then all items from start to the end of the series are returned. If there are not enough items to return then the maximum amount is returned. If there are no items to return then an empty slice is returned.
func (s *Series) ValueRange(start, count int) []any {
	start, end := s.Range(start, count)
	if start == end {
		return []any{}
	}
	items := make([]any, end-start)
	copy(items, s.data[start:end])
	return items
}

// Values returns a copy of all values. If there are no values, an empty slice is returned.
//
// Same as:
//
//	ValueRange(0, -1)
func (s *Series) Values() []any {
	return s.ValueRange(0, -1)
}

// Float returns the value at index i as a float64. If the value is not a float64 then 0 is returned.
func (s *Series) Float(i int) float64 {
	val := s.Value(i)
	switch val := val.(type) {
	case float64:
		return val
	default:
		return 0
	}
}

// Int returns the value at index i as an int64. If the value is not an int64 then 0 is returned.
func (s *Series) Int(i int) int {
	val := s.Value(i)
	switch val := val.(type) {
	case int:
		return val
	default:
		return 0
	}
}

// Str returns the value at index i as a string. If the value is not a string then "" is returned.
func (s *Series) Str(i int) string {
	val := s.Value(i)
	switch val := val.(type) {
	case string:
		return val
	default:
		return ""
	}
}

// Time returns the value at index i as a time.Time. If the value is not a time.Time then time.Time{} is returned. Use Time.IsZero() to check if the value returned was not a Time.
func (s *Series) Time(i int) time.Time {
	val := s.Value(i)
	switch val := val.(type) {
	case time.Time:
		return val
	default:
		return time.Time{}
	}
}

func (s *Series) Add(other *Series) *Series {
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Add(s.Value(i), other.Value(i))
		if err != nil {
			continue
		}
		s.data[i] = val
		s.SignalEmit("ValueChanged", i, val)
	}
	return s
}

func (s *Series) Sub(other *Series) *Series {
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Subtract(s.Value(i), other.Value(i))
		if err != nil {
			continue
		}
		s.data[i] = val
		s.SignalEmit("ValueChanged", i, val)
	}
	return s
}

func (s *Series) Mul(other *Series) *Series {
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Multiply(s.Value(i), other.Value(i))
		if err != nil {
			continue
		}
		s.data[i] = val
		s.SignalEmit("ValueChanged", i, val)
	}
	return s
}

func (s *Series) Div(other *Series) *Series {
	for i := 0; i < s.Len() && i < other.Len(); i++ {
		val, err := anymath.Divide(s.Value(i), other.Value(i))
		if err != nil {
			continue
		}
		s.data[i] = val
		s.SignalEmit("ValueChanged", i, val)
	}
	return s
}

func (s *Series) Filter(f func(i int, val any) bool) *Series {
	for i := 0; i < s.Len(); i++ {
		if val := s.data[i]; !f(i, val) {
			s.data = append(s.data[:i], s.data[i+1:]...)
			i--
		}
	}
	return s
}

func (s *Series) Map(f func(i int, val any) any) *Series {
	for i := 0; i < s.Len(); i++ {
		if val := f(i, s.data[i]); val != s.data[i] {
			s.data[i] = val
			s.SignalEmit("ValueChanged", i, val)
		}
	}
	return s
}

// MapReverse is equivalent to Map except that it iterates over the series in reverse order.
// This is useful when you want to retrieve values before i that are not modified by the map function,
// for example when calculating a moving average.
func (s *Series) MapReverse(f func(i int, val any) any) *Series {
	for i := s.Len() - 1; i >= 0; i-- {
		if val := f(i, s.data[i]); val != s.data[i] {
			s.data[i] = val
			s.SignalEmit("ValueChanged", i, val)
		}
	}
	return s
}

func (s *Series) ForEach(f func(i int, val any)) *Series {
	for i := 0; i < s.Len(); i++ {
		f(i, s.data[i])
	}
	return s
}

func (s *Series) MaxFloat() float64 {
	if s.Len() == 0 {
		return 0
	}
	max := math.Inf(-1)
	for i := 0; i < s.Len(); i++ {
		switch val := s.data[i].(type) {
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

func (s *Series) MinFloat() float64 {
	if s.Len() == 0 {
		return 0
	}
	min := math.Inf(1)
	for i := 0; i < s.Len(); i++ {
		switch val := s.data[i].(type) {
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

func (s *Series) MaxInt() int {
	if s.Len() == 0 {
		return 0
	}
	max := math.MinInt64
	for i := 0; i < s.Len(); i++ {
		switch val := s.data[i].(type) {
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

func (s *Series) MinInt() int {
	if s.Len() == 0 {
		return 0
	}
	min := math.MaxInt64
	for i := 0; i < s.Len(); i++ {
		switch val := s.data[i].(type) {
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

func (s *Series) Rolling(period int) *RollingSeries {
	return NewRollingSeries(s, period)
}

func (s *Series) Shift(periods int, nilVal any) *Series {
	if periods == 0 {
		return s
	} else if periods > 0 {
		// Shift values forward.
		for i := s.Len() - 1; i >= periods; i-- {
			s.data[i] = s.data[i-periods]
		}
		// Fill in nil values.
		for i := 0; i < periods; i++ {
			s.data[i] = nilVal
		}
	} else {
		periods = -periods
		// Shift values backward.
		for i := 0; i < periods; i++ {
			s.data[i] = s.data[periods-i]
		}
		// Fill in nil values.
		for i := periods; i < s.Len(); i++ {
			s.data[i] = nilVal
		}
	}
	return s
}

type RollingSeries struct {
	series *Series
	period int
}

func NewRollingSeries(series *Series, period int) *RollingSeries {
	return &RollingSeries{series, period}
}

// Period returns a slice of 'any' values with a length up to the period of the RollingSeries. The last item in the slice is the item at row. If row is out of bounds, nil is returned.
func (s *RollingSeries) Period(row int) []any {
	items := make([]any, 0, s.period)
	row = EasyIndex(row, s.series.Len())
	if row < 0 || row >= s.series.Len() {
		return items
	}
	for j := row; j > row-s.period && j >= 0; j-- {
		items = slices.Insert(items, 0, s.series.Value(j))
	}
	return items
}

// Max returns the underlying series with each value mapped to the maximum of its period as a float64 or 0 if the requested period is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) Max() *Series {
	return s.series.Map(func(i int, _ any) any {
		period := s.Period(i)
		if len(period) == 0 {
			return 0
		}
		max := math.Inf(-1)
		for _, v := range period {
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
		}
		return max
	})
}

// Min returns an AppliedSeries that returns the minimum value of the rolling period as a float64 or 0 if the requested period is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) Min() *Series {
	return s.series.Map(func(i int, _ any) any {
		period := s.Period(i)
		if len(period) == 0 {
			return 0
		}
		min := math.Inf(1)
		for _, v := range period {
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
		}
		return min
	})
}

// Average is an alias for Mean.
func (s *RollingSeries) Average() *Series {
	return s.Mean()
}

// Mean returns the mean of the rolling period as a float64 or 0 if the period requested is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) Mean() *Series {
	return s.series.MapReverse(func(i int, _ any) any {
		period := s.Period(i)
		var sum float64
		for _, v := range period {
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
		return sum / float64(len(period))
	})
}

// EMA returns the exponential moving average of the period as a float64 or 0 if the period requested is empty.
//
// Will work with all signed int and float types. Ignores all other values.
func (s *RollingSeries) EMA() *Series {
	return s.series.MapReverse(func(i int, _ any) any {
		period := s.Period(i)
		fPeriod := float64(s.period)
		var ema float64
		first := true
		for _, v := range period {
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
			ema += (f - ema) * 2 / (fPeriod + 1)
		}
		return ema
	})
}

// Median returns the median of the period as a float64 or 0 if the period requested is empty.
//
// Will work with float64 and int. Ignores all other values.
func (s *RollingSeries) Median() *Series {
	return s.series.MapReverse(func(i int, _ any) any {
		period := s.Period(i)
		if len(period) == 0 {
			return 0
		}

		var offenders int
		slices.SortFunc(period, func(a, b any) bool {
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
		period = period[:len(period)-offenders] // Cut out the offenders.

		v1 := period[len(period)/2-1]
		v2 := period[len(period)/2]
		if len(period)%2 == 0 {
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
		switch vMid := period[len(period)/2].(type) {
		case float64:
			return vMid
		case int:
			return float64(vMid)
		default:
			panic("unreachable") // Offenders are pushed to the back of the slice and ignored.
		}
	})
}

// StdDev returns the standard deviation of the period as a float64 or 0 if the period requested is empty.
func (s *RollingSeries) StdDev() *Series {
	return s.series.MapReverse(func(i int, _ any) any {
		period := s.Period(i)
		if len(period) == 0 {
			return 0
		}

		mean := s.Mean().Value(i).(float64) // Take the mean of the last period values for the current index
		period = s.Period(i)
		var sum float64
		var ignored int
		for _, v := range period {
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
		if ignored >= len(period) {
			return 0
		}
		return math.Sqrt(sum / float64(len(period)-ignored))
	})
}
