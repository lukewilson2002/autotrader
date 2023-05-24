package autotrader

import (
	"math"
	"testing"
	"time"
)

func TestSeries(t *testing.T) {
	series := NewFloatSeries("test", 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)
	if series.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", series.Len())
	}
	series.Reverse()
	if series.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", series.Len())
	}
	for i := 0; i < 10; i++ {
		if val := series.Value(i); val != float64(10-i) {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(10-i), val)
		}
	}

	last5 := series.CopyRange(-5, -1)
	if last5.Len() != 5 {
		t.Fatalf("Expected 5 rows, got %d", last5.Len())
	}
	for i := 0; i < 5; i++ {
		if val := last5.Value(i); val != float64(5-i) {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(5-i), val)
		}
	}
	last5.SetValue(-1, 0.0)
	if series.Value(-1) == 0.0 {
		t.Errorf("Expected data to be copied, not referenced")
	}

	outOfBounds := series.CopyRange(10, -1)
	if outOfBounds == nil {
		t.Fatal("Expected non-nil series, got nil")
	}
	if outOfBounds.Len() != 0 {
		t.Fatalf("Expected 0 rows, got %d", outOfBounds.Len())
	}

	valueRange := series.ValueRange(-1, 0) // Out of bounds should result in an empty slice.
	if valueRange == nil || len(valueRange) != 0 {
		t.Fatalf("Expected a slice with 0 items, got %d", len(valueRange))
	}
	valueRange = series.ValueRange(0, 5) // Take the first 5 items.
	if len(valueRange) != 5 {
		t.Fatalf("Expected a slice with 5 items, got %d", len(valueRange))
	}
	for i := 0; i < 5; i++ {
		if val := valueRange[i]; val != float64(10-i) {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(10-i), val)
		}
	}

	values := series.Values()
	if len(values) != 10 {
		t.Fatalf("Expected a slice with 10 items, got %d", len(values))
	}
	for i := 0; i < 10; i++ {
		if val := values[i]; val != float64(10-i) {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(10-i), val)
		}
	}
}

func TestSeriesFunctional(t *testing.T) {
	series := NewSeries("test", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)
	doubled := series.Copy().Map(func(_ int, val any) any {
		return val.(float64) * 2
	})
	if doubled.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", doubled.Len())
	}
	for i := 0; i < 10; i++ {
		if val := doubled.Float(i); val != float64(i+1)*2 {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(i+1)*2, val)
		}
	}
	series.SetValue(0, 100.0)
	if doubled.Float(0) == 100.0 {
		t.Error("Expected data to be copied, not referenced")
	}
	series.SetValue(0, 1.0) // Reset the value.

	evens := series.Copy().Filter(func(_ int, val any) bool {
		return EqualApprox(math.Mod(val.(float64), 2), 0)
	})
	if evens.Len() != 5 {
		t.Fatalf("Expected 5 rows, got %d", evens.Len())
	}
	for i := 0; i < 5; i++ {
		if val := evens.Float(i); val != float64(i+1)*2 {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(i+1)*2, val)
		}
	}
	if series.Len() != 10 {
		t.Fatalf("Expected series to still have 10 rows, got %d", series.Len())
	}

	diffed := series.Copy().Map(func(i int, v any) any {
		if i == 0 {
			return 0.0
		}
		return v.(float64) - series.Float(i-1)
	})
	if diffed.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", diffed.Len())
	}
	if diffed.Float(0) != 0.0 {
		t.Errorf("Expected first value to be 0.0, got %v", diffed.Float(0))
	}
	for i := 1; i < 10; i++ {
		if val := diffed.Float(i); val != 1.0 {
			t.Errorf("(%d)\tExpected 1.0, got %v", i, val)
		}
	}
}

func TestRollingSeries(t *testing.T) {
	// Test rolling average.
	series := NewSeries("test", 1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0)

	sma5Expected := []float64{1, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8}
	sma5 := series.Copy().Rolling(5).Average() // Take the 5 period moving average and cast it to Series.
	if sma5.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", sma5.Len())
	}
	for i := 0; i < 10; i++ {
		// Calling Float instead of Value is very important. Value will call the AppliedSeries.Value method
		// while Float calls Series.Float which is what most people will use and is the most likely to be
		// problematic as it is supposed to route through the DataSeries.value method.
		if val := sma5.Float(i); !EqualApprox(val, sma5Expected[i]) {
			t.Errorf("(%d)\tExpected %f, got %v", i, sma5Expected[i], val)
		}
	}

	ema5Expected := []float64{1, 1.3333333333333333, 1.8888888888888888, 2.5925925925925926, 3.3950617283950617, 4.395061728395062, 5.395061728395062, 6.395061728395062, 7.395061728395062, 8.395061728395062}
	ema5 := series.Rolling(5).EMA() // Take the 5 period exponential moving average.
	if ema5.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", ema5.Len())
	}
	for i := 0; i < 10; i++ {
		if val := ema5.Float(i); !EqualApprox(val, ema5Expected[i]) {
			t.Errorf("(%d)\tExpected %f, got %v", i, ema5Expected[i], val)
		}
	}
}

func TestIndexedSeriesInsert(t *testing.T) {
	indexed := NewIndexedSeries("test", map[UnixTime]float64{
		UnixTime(0):  1.0,
		UnixTime(2):  2.0,
		UnixTime(4):  3.0,
		UnixTime(6):  4.0,
		UnixTime(8):  5.0,
		UnixTime(10): 6.0,
	})
	if indexed.Len() != 6 {
		t.Fatalf("Expected 6 rows, got %d", indexed.Len())
	}
	for i := 0; i < 6; i++ {
		if val := indexed.Float(i); val != float64(i+1) {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(i+1), val)
		}
	}
	for i := 0; i < 6; i++ {
		index := UnixTime(i * 2)
		if val := indexed.ValueIndex(index); val != float64(i+1) {
			t.Errorf("(%v)\tExpected %f, got %v", index, float64(i+1), val)
		}
	}
	indexed.Insert(UnixTime(5), -1.0)
	if indexed.Len() != 7 {
		t.Fatalf("Expected 7 rows, got %d", indexed.Len())
	}
	if indexed.ValueIndex(UnixTime(5)) != -1.0 {
		t.Errorf("Expected value at index 5 to be -1.0, got %v", indexed.ValueIndex(UnixTime(5)))
	}
}

func TestIndexedSeries(t *testing.T) {
	intIndexed := NewIndexedSeries("test", map[int]float64{
		0:  1.0,
		2:  2.0,
		4:  3.0,
		6:  4.0,
		8:  5.0,
		10: 6.0,
	})

	if intIndexed.Len() != 6 {
		t.Fatalf("Expected 6 rows, got %d", intIndexed.Len())
	}
	if intIndexed.ValueIndex(4).(float64) != 3.0 {
		t.Errorf("Expected value at index 4 to be 3.0, got %v", intIndexed.ValueIndex(4))
	}

	floatIndexed := NewIndexedSeries("test", map[float64]float64{
		0.0: 1.0,
		2.0: 2.0,
		4.0: 3.0,
	})
	if floatIndexed.Len() != 3 {
		t.Fatalf("Expected 3 rows, got %d", floatIndexed.Len())
	}
	if floatIndexed.ValueIndex(4.0).(float64) != 3.0 {
		t.Errorf("Expected value at index 4.0 to be 3.0, got %v", floatIndexed.ValueIndex(4.0))
	}

	timeIndexed := NewIndexedSeries("test", map[UnixTime]float64{
		UnixTime(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).Unix()): 1.0,
		UnixTime(time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC).Unix()): 2.0,
		UnixTime(time.Date(2018, 1, 3, 0, 0, 0, 0, time.UTC).Unix()): 3.0,
	})
	if timeIndexed.Len() != 3 {
		t.Fatalf("Expected 3 rows, got %d", timeIndexed.Len())
	}
	if index := UnixTime(time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC).Unix()); timeIndexed.ValueIndex(index).(float64) != 2.0 {
		t.Errorf("Expected value at index 2018-01-02 to be 2.0, got %v", timeIndexed.ValueIndex(index))
	}

	doubledTimeIndexed := timeIndexed.Copy().Add(timeIndexed)
	if doubledTimeIndexed.Len() != 3 {
		t.Fatalf("Expected 3 rows, got %d", doubledTimeIndexed.Len())
	}
	if index := UnixTime(time.Date(2018, 1, 2, 0, 0, 0, 0, time.UTC).Unix()); doubledTimeIndexed.ValueIndex(index).(float64) != 4.0 {
		t.Errorf("Expected value at index 2018-01-02 to be 4.0, got %v", doubledTimeIndexed.ValueIndex(index))
	}

	// Test that the Copy function works.
	index := UnixTime(time.Date(2018, 1, 1, 0, 0, 0, 0, time.UTC).Unix())
	doubledTimeIndexed.SetValueIndex(index, 100.0)
	if timeIndexed.ValueIndex(index).(float64) != 1.0 {
		t.Errorf("Expected value at index 2018-01-01 to be 1.0, got %v", timeIndexed.ValueIndex(index))
	}
}
