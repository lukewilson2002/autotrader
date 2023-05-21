package autotrader

// FloatSeries is a wrapper of a Series where all items are float64 values. This is done by always casting values to and from float64
type FloatSeries struct {
	// NOTE: We embed the Series struct to get all of its methods. BUT! We want to make sure that we override the methods that set values or return a pointer to the Series.

	*Series // The underlying Series which contains the data. Accessing this directly will not provide the type safety of FloatSeries and may cause panics.
}

func NewFloatSeries(name string, vals ...float64) *FloatSeries {
	anyVals := make([]any, len(vals))
	for i, val := range vals {
		anyVals[i] = val
	}
	return &FloatSeries{NewSeries(name, anyVals...)}
}

func (s *FloatSeries) Add(other *FloatSeries) *FloatSeries {
	_ = s.Series.Add(other.Series)
	return s
}

func (s *FloatSeries) Copy() *FloatSeries {
	return s.CopyRange(0, -1)
}

func (s *FloatSeries) CopyRange(start, count int) *FloatSeries {
	return &FloatSeries{s.Series.CopyRange(start, count)}
}

func (s *FloatSeries) Div(other *FloatSeries) *FloatSeries {
	_ = s.Series.Div(other.Series)
	return s
}

func (s *FloatSeries) Filter(f func(i int, val float64) bool) *FloatSeries {
	_ = s.Series.Filter(func(i int, val any) bool {
		return f(i, val.(float64))
	})
	return s
}

func (s *FloatSeries) ForEach(f func(i int, val float64)) {
	s.Series.ForEach(func(i int, val any) {
		f(i, val.(float64))
	})
}

func (s *FloatSeries) Map(f func(i int, val float64) float64) *FloatSeries {
	_ = s.Series.Map(func(i int, val any) any {
		return f(i, val.(float64))
	})
	return s
}

func (s *FloatSeries) MapReverse(f func(i int, val float64) float64) *FloatSeries {
	_ = s.Series.MapReverse(func(i int, val any) any {
		return f(i, val.(float64))
	})
	return s
}

// Max returns the maximum value in the series or 0 if the series is empty. This should be used over Series.MaxFloat() because this function contains optimizations that assume all the values are of float64.
func (s *FloatSeries) Max() float64 {
	if s.Series.Len() == 0 {
		return 0
	}
	max := s.Series.data[0].(float64)
	for i := 1; i < s.Series.Len(); i++ {
		v := s.Series.data[i].(float64)
		if v > max {
			max = v
		}
	}
	return max
}

// Min returns the minimum value in the series or 0 if the series is empty. This should be used over Series.MinFloat() because this function contains optimizations that assume all the values are of float64.
func (s *FloatSeries) Min() float64 {
	if s.Series.Len() == 0 {
		return 0
	}
	min := s.Series.data[0].(float64)
	for i := 1; i < s.Series.Len(); i++ {
		v := s.Series.data[i].(float64)
		if v < min {
			min = v
		}
	}
	return min
}

func (s *FloatSeries) Mul(other *FloatSeries) *FloatSeries {
	_ = s.Series.Mul(other.Series)
	return s
}

func (s *FloatSeries) Push(val float64) *FloatSeries {
	_ = s.Series.Push(val)
	return s
}

func (s *FloatSeries) Pop() float64 {
	if v := s.Series.Pop(); v != nil {
		return v.(float64)
	}
	return 0
}

// Remove deletes the value at the given index and returns it. If the index is out of bounds, it returns 0.
func (s *FloatSeries) Remove(i int) float64 {
	if v := s.Series.Remove(i); v != nil {
		return v.(float64)
	}
	return 0
}

func (s *FloatSeries) RemoveRange(start, count int) *FloatSeries {
	_ = s.Series.RemoveRange(start, count)
	return s
}

func (s *FloatSeries) Reverse() *FloatSeries {
	_ = s.Series.Reverse()
	return s
}

func (s *FloatSeries) SetName(name string) *FloatSeries {
	_ = s.Series.SetName(name)
	return s
}

func (s *FloatSeries) SetValue(i int, val float64) *FloatSeries {
	_ = s.Series.SetValue(i, val)
	return s
}

func (s *FloatSeries) Sub(other *FloatSeries) *FloatSeries {
	_ = s.Series.Sub(other.Series)
	return s
}

func (s *FloatSeries) Value(i int) float64 {
	return s.Series.Value(i).(float64)
}

func (s *FloatSeries) Values() []float64 {
	return s.ValueRange(0, -1)
}

func (s *FloatSeries) ValueRange(start, count int) []float64 {
	start, end := s.Series.Range(start, count)
	if start == end {
		return []float64{}
	}
	vals := make([]float64, end-start)
	for i := start; i < end; i++ {
		vals[i] = s.Series.data[i].(float64)
	}
	return vals
}
