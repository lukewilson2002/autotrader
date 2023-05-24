package autotrader

import (
	"bytes"
	"fmt"
	"text/tabwriter"
	"time"

	anymath "github.com/spatialcurrent/go-math/pkg/math"
	"golang.org/x/exp/constraints"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
)

type ErrIndexExists struct {
	any
}

func (e ErrIndexExists) Error() string {
	return fmt.Sprintf("index already exists: %v", e.any)
}

// UnixTime is a wrapper over the number of milliseconds since January 1, 1970, AKA Unix time.
type UnixTime int64

// Time converts the UnixTime to a time.Time.
func (t UnixTime) Time() time.Time {
	return time.Unix(int64(t), 0)
}

// String returns the string representation of the UnixTime.
func (t UnixTime) String() string {
	return t.Time().UTC().String()
}

// UnixTimeStep returns a function that adds a number of increments to a UnixTime.
func UnixTimeStep(frequency time.Duration) func(UnixTime, int) UnixTime {
	return func(t UnixTime, amt int) UnixTime {
		return UnixTime(t.Time().Add(frequency * time.Duration(amt)).Unix())
	}
}

type Index interface {
	comparable
	constraints.Ordered
}

// IndexedSeries is a Series with a custom index type.
type IndexedSeries[I Index] struct {
	*SignalManager
	series  *Series
	indexes []I // Sorted slice of indexes.
	index   map[I]int
}

// NewIndexedSeries returns a new IndexedSeries with the given name and index type.
func NewIndexedSeries[I Index, V any](name string, vals map[I]V) *IndexedSeries[I] {
	out := &IndexedSeries[I]{
		&SignalManager{},
		NewSeries(name),
		make([]I, 0),
		make(map[I]int),
	}
	for index, val := range vals {
		out.Insert(index, val)
	}
	return out
}

// Add adds the values of the other series to the values of this series. The other series must have the same index type. The values are added by comparing their indexes. For example, adding two IndexedSeries that share no indexes will result in no change of values.
func (s *IndexedSeries[I]) Add(other *IndexedSeries[I]) *IndexedSeries[I] {
	// For each index in self, add the corresponding value of the other series.
	for index, row := range s.index {
		if otherRow, ok := other.index[index]; ok {
			val, err := anymath.Add(s.series.Value(row), other.series.Value(otherRow))
			if err != nil {
				panic(fmt.Errorf("error adding values at index %v: %w", index, err))
			}
			s.series.SetValue(row, val)
		}
	}
	return s
}

func (s *IndexedSeries[I]) AddFloat(num float64) *IndexedSeries[I] {
	for index, row := range s.index {
		newValue, err := anymath.Add(s.series.Value(row), num)
		if err != nil {
			panic(fmt.Errorf("error adding values at index %v: %w", index, err))
		}
		s.series.SetValue(row, newValue)
	}
	return s
}

// Copy returns a copy of this series.
func (s *IndexedSeries[I]) Copy() *IndexedSeries[I] {
	return s.CopyRange(0, -1)
}

// CopyRange returns a copy of this series with the given range.
func (s *IndexedSeries[I]) CopyRange(start, count int) *IndexedSeries[I] {
	start, end := s.series.Range(start, count)
	if start == end {
		return NewIndexedSeries[I, any](s.Name(), nil)
	}
	count = end - start

	// Copy the index values over.
	indexes := make([]I, count)
	copy(indexes, s.indexes[start:end])
	index := make(map[I]int, count)
	for i, _index := range indexes {
		index[_index] = i
	}
	return &IndexedSeries[I]{
		&SignalManager{},
		s.series.CopyRange(start, count),
		indexes,
		index,
	}
}

// Div divides this series values with the other series values. The other series must have the same index type. The values are divided by comparing their indexes. For example, dividing two IndexedSeries that share no indexes will result in no change of values.
func (s *IndexedSeries[I]) Div(other *IndexedSeries[I]) *IndexedSeries[I] {
	for index, row := range s.index {
		if otherRow, ok := other.index[index]; ok {
			val, err := anymath.Divide(s.series.Value(row), other.series.Value(otherRow))
			if err != nil {
				panic(fmt.Errorf("error dividing values at index %v: %w", index, err))
			}
			s.series.SetValue(row, val)
		}
	}
	return s
}

func (s *IndexedSeries[I]) DivFloat(num float64) *IndexedSeries[I] {
	for index, row := range s.index {
		newValue, err := anymath.Divide(s.series.Value(row), num)
		if err != nil {
			panic(fmt.Errorf("error dividing values at index %v: %w", index, err))
		}
		s.series.SetValue(row, newValue)
	}
	return s
}

func (s *IndexedSeries[I]) Filter(f func(i int, val any) bool) *IndexedSeries[I] {
	_ = s.series.Filter(f)
	return s
}

func (s *IndexedSeries[I]) Float(i int) float64 {
	return s.series.Float(i)
}

func (s *IndexedSeries[I]) FloatIndex(index I) float64 {
	row := s.Row(index)
	if row < 0 {
		return 0.0
	}
	return s.series.Float(row)
}

func (s *IndexedSeries[I]) ForEach(f func(i int, val any)) *IndexedSeries[I] {
	_ = s.series.ForEach(f)
	return s
}

// Index returns the index of the given row or nil if the row is out of bounds. row is an EasyIndex.
//
// The performance of this operation is O(1).
func (s *IndexedSeries[I]) Index(row int) *I {
	row = EasyIndex(row, s.series.Len())
	if row < 0 || row >= len(s.indexes) {
		return nil
	}
	return &s.indexes[row]
}

// Row returns the row of the given index or -1 if the index does not exist.
//
// The performance of this operation is O(1).
func (s *IndexedSeries[I]) Row(index I) int {
	if i, ok := s.index[index]; ok {
		return i
	}
	return -1
}

// Len returns the number of rows in the series.
func (s *IndexedSeries[I]) Len() int {
	return s.series.Len()
}

func (s *IndexedSeries[I]) Map(f func(index I, row int, val any) any) *IndexedSeries[I] {
	_ = s.series.Map(func(i int, val any) any {
		index := s.Index(i)
		return f(*index, i, val)
	})
	return s
}

func (s *IndexedSeries[I]) MapReverse(f func(index I, row int, val any) any) *IndexedSeries[I] {
	_ = s.series.MapReverse(func(i int, val any) any {
		index := s.Index(i)
		return f(*index, i, val)
	})
	return s
}

// Mul multiplies this series values with the other series values. The other series must have the same index type. The values are multiplied by comparing their indexes. For example, multiplying two IndexedSeries that share no indexes will result in no change of values.
func (s *IndexedSeries[I]) Mul(other *IndexedSeries[I]) *IndexedSeries[I] {
	for index, row := range s.index {
		if otherRow, ok := other.index[index]; ok {
			val, err := anymath.Multiply(s.series.Value(row), other.series.Value(otherRow))
			if err != nil {
				panic(fmt.Errorf("error multiplying values at index %v: %w", index, err))
			}
			s.series.SetValue(row, val)
		}
	}
	return s
}

func (s *IndexedSeries[I]) MulFloat(num float64) *IndexedSeries[I] {
	for index, row := range s.index {
		newValue, err := anymath.Multiply(s.series.Value(row), num)
		if err != nil {
			panic(fmt.Errorf("error multiplying values at index %v: %w", index, err))
		}
		s.series.SetValue(row, newValue)
	}
	return s
}

// Name returns the name of the series.
func (s *IndexedSeries[I]) Name() string {
	return s.series.Name()
}

// insertIndex will insert the provided index somewhere in the sorted slice of indexes. If the index already exists, the existing index will be returned.
func (s *IndexedSeries[I]) insertIndex(index I) (row int, exists bool) {
	// Sort the indexes.
	idx, found := slices.BinarySearch(s.indexes, index)
	if found {
		return idx, true
	}
	s.index[index] = idx // Create the index to row mapping.
	// Check if we're just appending the index. Just an optimization.
	if idx >= len(s.indexes) {
		s.indexes = append(s.indexes, index) // Append the index to our sorted slice of indexes.
		return idx, false
	}
	s.indexes = slices.Insert(s.indexes, idx, index)
	// Shift the row values of all indexes after the inserted index.
	for i := idx + 1; i < len(s.indexes); i++ {
		s.index[s.indexes[i]]++
	}
	return idx, false
}

// Insert adds a value to the series at the given index. If the index already exists, the value will be overwritten. The indexes are sorted using comparison operators.
func (s *IndexedSeries[I]) Insert(index I, val any) *IndexedSeries[I] {
	row, exists := s.insertIndex(index)
	if exists {
		s.series.SetValue(row, val)
		return s
	}
	s.series.Insert(row, val)
	return s
}

// Remove deletes the row at the given index and returns it.
func (s *IndexedSeries[I]) Remove(index I) any {
	row, ok := s.index[index]
	if !ok {
		return nil
	}
	delete(s.index, index)
	// Shift each index after the removed index down by one.
	for key, j := range s.index {
		if j > row {
			s.index[key] = j - 1
		}
	}
	// Remove the value from the series.
	return s.series.Remove(row)
}

// RemoveRange deletes the rows in the given range and returns the series.
//
// The operation is O(n) where n is the number of rows in the series.
func (s *IndexedSeries[I]) RemoveRange(start, count int) *IndexedSeries[I] {
	start, end := s.series.Range(start, count)
	if start == end {
		return s
	}
	count = end - start
	// Remove the indexes from the map.
	for index, i := range s.index {
		if i >= start && i < end {
			idx := slices.Index(s.indexes, index)
			slices.Delete(s.indexes, idx, idx+1)
			delete(s.index, index)
		}
	}
	// Shift each index after the removed index down by count.
	for key, i := range s.index {
		if i >= end {
			s.index[key] = i - count
		}
	}
	// Remove the values from the series.
	_ = s.series.RemoveRange(start, count)
	return s
}

// Reverse reverses the rows of the series.
func (s *IndexedSeries[I]) Reverse() *IndexedSeries[I] {
	// Reverse the values.
	_ = s.series.Reverse()
	return s
}

func (s *IndexedSeries[I]) Rolling(period int) *IndexedRollingSeries[I] {
	return NewIndexedRollingSeries(s, period)
}

func (s *IndexedSeries[I]) SetName(name string) *IndexedSeries[I] {
	_ = s.series.SetName(name)
	return s
}

func (s *IndexedSeries[I]) SetValue(row int, val any) *IndexedSeries[I] {
	_ = s.series.SetValue(row, val)
	return s
}

// SetValueIndex is like SetValue but uses the index instead of the row.
func (s *IndexedSeries[I]) SetValueIndex(index I, val any) *IndexedSeries[I] {
	row := s.Row(index)
	if row < 0 {
		return s
	}
	return s.SetValue(row, val)
}

func (s *IndexedSeries[I]) Shift(periods int, nilValue any) *IndexedSeries[I] {
	_ = s.series.Shift(periods, nilValue)
	return s
}

func (s *IndexedSeries[I]) ShiftIndex(periods int, step func(prev I, amt int) I) *IndexedSeries[I] {
	if periods == 0 {
		return s
	}
	// Update the index values.
	for index, i := range s.index {
		s.indexes[i] = step(index, periods)
	}

	// Reassign the index map.
	maps.Clear(s.index)
	for i, index := range s.indexes {
		s.index[index] = i
	}

	// Shift the indexes.
	newIndexes := make(map[I]int, len(s.index))
	for index, i := range s.index {
		newIndexes[step(index, periods)] = i
	}
	s.index = newIndexes
	return s
}

func (s *IndexedSeries[I]) String() string {
	if s == nil {
		return fmt.Sprintf("%T[nil]", s)
	}

	buffer := new(bytes.Buffer)
	t := tabwriter.NewWriter(buffer, 0, 0, 2, ' ', 0)
	fmt.Fprintf(t, "%T[%d]\n", s, s.Len())
	fmt.Fprintf(t, "[Row]\t[Index]\t%s\t\n", s.series.Name())

	for i, index := range s.indexes {
		fmt.Fprintf(t, "%d\t%v\t%v\t\n", i, index, s.series.Value(i))
	}
	_ = t.Flush()
	return buffer.String()
}

// Sub subtracts the other series values from this series values. The other series must have the same index type. The values are subtracted by comparing their indexes. For example, subtracting two IndexedSeries that share no indexes will result in no change of values.
func (s *IndexedSeries[I]) Sub(other *IndexedSeries[I]) *IndexedSeries[I] {
	for index, row := range s.index {
		if otherRow, ok := other.index[index]; ok {
			val, err := anymath.Divide(s.series.Value(row), other.series.Value(otherRow))
			if err != nil {
				panic(fmt.Errorf("error subtracting values at index %v: %w", index, err))
			}
			s.series.SetValue(row, val)
		}
	}
	return s
}

func (s *IndexedSeries[I]) SubFloat(num float64) *IndexedSeries[I] {
	for index, row := range s.index {
		newValue, err := anymath.Subtract(s.series.Value(row), num)
		if err != nil {
			panic(fmt.Errorf("error subtracting values at index %v: %w", index, err))
		}
		s.series.SetValue(row, newValue)
	}
	return s
}

// Value returns the value at the given row.
func (s *IndexedSeries[I]) Value(i int) any {
	return s.series.Value(i)
}

// ValueIndex returns the value at the given index or nil if the index does not exist.
func (s *IndexedSeries[I]) ValueIndex(index I) any {
	row := s.Row(index)
	if row < 0 {
		return nil
	}
	return s.Value(row)
}

// Values returns a copy of the values in the series.
func (s *IndexedSeries[I]) Values() []any {
	return s.series.ValueRange(0, -1)
}

// ValueRange returns a copy of the values in the given range. start is an EasyIndex. count is the number of values to return. If count is -1, all values after start are returned. See Series.ValueRange() for more information.
func (s *IndexedSeries[I]) ValueRange(start, count int) []any {
	return s.series.ValueRange(start, count)
}

type IndexedRollingSeries[I Index] struct {
	rolling *RollingSeries
	series  *IndexedSeries[I]
}

func NewIndexedRollingSeries[I Index](series *IndexedSeries[I], period int) *IndexedRollingSeries[I] {
	return &IndexedRollingSeries[I]{NewRollingSeries(series.series, period), series}
}

func (s *IndexedRollingSeries[I]) Period(row int) []any {
	return s.rolling.Period(row)
}

func (s *IndexedRollingSeries[I]) Max() *IndexedSeries[I] {
	_ = s.rolling.Max() // Mutate the underlying series.
	return s.series
}

func (s *IndexedRollingSeries[I]) Min() *IndexedSeries[I] {
	_ = s.rolling.Min() // Mutate the underlying series.
	return s.series
}

func (s *IndexedRollingSeries[I]) Average() *IndexedSeries[I] {
	_ = s.rolling.Average() // Mutate the underlying series.
	return s.series
}

func (s *IndexedRollingSeries[I]) Mean() *IndexedSeries[I] {
	_ = s.rolling.Mean() // Mutate the underlying series.
	return s.series
}

func (s *IndexedRollingSeries[I]) Median() *IndexedSeries[I] {
	_ = s.rolling.Median() // Mutate the underlying series.
	return s.series
}

func (s *IndexedRollingSeries[I]) EMA() *IndexedSeries[I] {
	_ = s.rolling.EMA() // Mutate the underlying series.
	return s.series
}

func (s *IndexedRollingSeries[I]) StdDev() *IndexedSeries[I] {
	_ = s.rolling.StdDev() // Mutate the underlying series.
	return s.series
}
