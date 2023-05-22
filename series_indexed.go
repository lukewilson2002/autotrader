package autotrader

import (
	"fmt"

	anymath "github.com/spatialcurrent/go-math/pkg/math"
)

type ErrIndexExists struct {
	any
}

func (e ErrIndexExists) Error() string {
	return fmt.Sprintf("index already exists: %v", e.any)
}

// IndexedSeries is a Series with a custom index type.
type IndexedSeries[I comparable] struct {
	*SignalManager
	series *Series
	index  map[I]int
}

func NewIndexedSeries[I comparable](name string, vals map[I]any) *IndexedSeries[I] {
	out := &IndexedSeries[I]{
		&SignalManager{},
		NewSeries(name),
		make(map[I]int, len(vals)),
	}
	for key, val := range vals {
		// Check that the key is not already in the map.
		if _, ok := out.index[key]; ok {
			panic(ErrIndexExists{key})
		}
		out.index[key] = out.series.Len()
		out.series.Push(val)
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

func (s *IndexedSeries[I]) Copy() *IndexedSeries[I] {
	return s.CopyRange(0, -1)
}

func (s *IndexedSeries[I]) CopyRange(start, count int) *IndexedSeries[I] {
	// Copy the index values over.
	index := make(map[I]int, len(s.index))
	for key, val := range s.index {
		index[key] = val
	}
	return &IndexedSeries[I]{
		&SignalManager{},
		s.series.CopyRange(start, count),
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
func (s *IndexedSeries[I]) Index(row int) *I {
	row = EasyIndex(row, s.series.Len())
	for key, val := range s.index {
		if val == row {
			return &key
		}
	}
	return nil
}

// Row returns the row of the given index or -1 if the index does not exist.
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

// Name returns the name of the series.
func (s *IndexedSeries[I]) Name() string {
	return s.series.Name()
}

// Push adds a value to the end of the series and returns the series or an error if the index already exists. The error is of type ErrIndexExists.
func (s *IndexedSeries[I]) Push(index I, val any) (*IndexedSeries[I], error) {
	// Check that the key is not already in the map.
	if _, ok := s.index[index]; ok {
		return nil, ErrIndexExists{index}
	}
	s.index[index] = s.series.Len()
	s.series.Push(val)
	return s, nil
}

func (s *IndexedSeries[I]) Pop() any {
	return s.Remove(s.series.Len() - 1)
}

// Remove deletes the row at the given index and returns it.
func (s *IndexedSeries[I]) Remove(row int) any {
	// Remove the index from the map.
	for index, j := range s.index {
		if j == row {
			delete(s.index, index)
			break
		}
	}
	// Shift each index after the removed index down by one.
	for key, j := range s.index {
		if j > row {
			s.index[key] = j - 1
		}
	}
	// Remove the value from the series.
	return s.series.Remove(row)
}

// RemoveIndex deletes the row at the given index and returns it. If index does not exist, nil is returned.
func (s *IndexedSeries[I]) RemoveIndex(index I) any {
	// Check that the key is in the map.
	if i, ok := s.index[index]; ok {
		return s.Remove(i)
	}
	return nil
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
	// Reverse the indexes.
	s.ReverseIndexes()
	// Reverse the values.
	_ = s.series.Reverse()
	return s
}

// ReverseIndexes reverses the indexes of the series but not the rows.
func (s *IndexedSeries[I]) ReverseIndexes() *IndexedSeries[I] {
	seriesLen := s.series.Len()
	for key, i := range s.index {
		s.index[key] = seriesLen - i - 1
	}
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
	// Shift the indexes.
	newIndexes := make(map[I]int, len(s.index))
	for index, i := range s.index {
		newIndexes[step(index, periods)] = i
	}
	s.index = newIndexes
	return s
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

type IndexedRollingSeries[I comparable] struct {
	rolling *RollingSeries
	series  *IndexedSeries[I]
}

func NewIndexedRollingSeries[I comparable](series *IndexedSeries[I], period int) *IndexedRollingSeries[I] {
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
