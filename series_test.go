package autotrader

import (
	"testing"

	"github.com/rocketlaunchr/dataframe-go"
)

func TestAppliedSeries(t *testing.T) {
	underlying := NewDataSeries(dataframe.NewSeriesFloat64("test", nil, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))
	applied := NewAppliedSeries(underlying, func(_ *AppliedSeries, _ int, val interface{}) interface{} {
		return val.(float64) * 2
	})

	if applied.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", applied.Len())
	}
	for i := 0; i < 10; i++ {
		if val := applied.Float(i); val != float64(i+1)*2 {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(i+1)*2, val)
		}
	}

	// Test that the underlying series is not modified.
	if underlying.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", underlying.Len())
	}
	for i := 0; i < 10; i++ {
		if val := underlying.Float(i); val != float64(i+1) {
			t.Errorf("(%d)\tExpected %f, got %v", i, float64(i+1), val)
		}
	}

	// Test that the underlying series is not modified when the applied series is modified.
	applied.SetValue(0, 100)
	if underlying.Float(0) != 1 {
		t.Errorf("Expected 1, got %v", underlying.Float(0))
	}
	if applied.Float(0) != 200 {
		t.Errorf("Expected 200, got %v", applied.Float(0))
	}
}

func TestRollingAppliedSeries(t *testing.T) {
	// Test rolling average.
	series := NewDataSeries(dataframe.NewSeriesFloat64("test", nil, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10))

	sma5Expected := []float64{1, 1.5, 2, 2.5, 3, 4, 5, 6, 7, 8}
	sma5 := (Series)(series.Rolling(5).Average()) // Take the 5 period moving average and cast it to Series.
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
	ema5 := (Series)(series.Rolling(5).EMA()) // Take the 5 period exponential moving average.
	if ema5.Len() != 10 {
		t.Fatalf("Expected 10 rows, got %d", ema5.Len())
	}
	for i := 0; i < 10; i++ {
		if val := ema5.Float(i); !EqualApprox(val, ema5Expected[i]) {
			t.Errorf("(%d)\tExpected %f, got %v", i, ema5Expected[i], val)
		}
	}
}

func TestDataSeries(t *testing.T) {
	data, err := EURUSD()
	if err != nil {
		t.Fatalf("Expected no error, got %s", err)
	}

	dates, closes := data.Dates(), data.Closes()

	if dates.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", dates.Len())
	}
	if closes.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", closes.Len())
	}

	sma10 := closes.Rolling(10).Mean()
	if sma10.Len() != 2610 {
		t.Fatalf("Expected 2610 rows, got %d", sma10.Len())
	}
	if sma10.Value(-1) != 1.10039 { // Latest closing price averaged over 10 periods.
		t.Fatalf("Expected 1.10039, got %f", sma10.Value(-1))
	}
}
