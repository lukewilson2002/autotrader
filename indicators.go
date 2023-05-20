package autotrader

import "math"

// RSI calculates the Relative Strength Index for a given Series. Typically, the input series is the Close column of a DataFrame. Returns a Series of RSI values of the same length as the input.
//
// Traditionally, an RSI reading of 70 or above indicates an overbought condition, and a reading of 30 or below indicates an oversold condition.
//
// Typically, the RSI is calculated with a period of 14 days.
func RSI(series *Series, periods int) *Series {
	// Calculate the difference between each day's close and the previous day's close.
	delta := series.Copy().Map(func(i int, v interface{}) interface{} {
		if i == 0 {
			return float64(0)
		}
		return v.(float64) - series.Value(i-1).(float64)
	})
	// Calculate the average gain and average loss.
	avgGain := delta.Copy().
		Map(func(i int, val interface{}) interface{} { return math.Max(val.(float64), 0) }).
		Rolling(periods).Average()
	avgLoss := delta.Copy().
		Map(func(i int, val interface{}) interface{} { return math.Abs(math.Min(val.(float64), 0)) }).
		Rolling(periods).Average()
	// Calculate the RSI.
	return avgGain.Map(func(i int, val interface{}) interface{} {
		loss := avgLoss.Float(i)
		if loss == 0 {
			return float64(100)
		}
		return float64(100. - 100./(1.+val.(float64)/loss))
	})
}

// Ichimoku calculates the Ichimoku Cloud for a given Series. Returns a DataFrame of the same length as the input with float64 values. The series input must contain only float64 values, which are traditionally the close prices.
//
// The standard values:
//   - convPeriod:     9
//   - basePeriod:     26
//   - leadingPeriods: 52
//
// DataFrame columns:
//   - Conversion
//   - Base
//   - LeadingA
//   - LeadingB
//   - Lagging
func Ichimoku(series *Series, convPeriod, basePeriod, leadingPeriods int) *Frame {
	// Calculate the Conversion Line.
	conv := series.Copy().Rolling(convPeriod).Max().Add(series.Copy().Rolling(convPeriod).Min()).
		Map(func(i int, val any) any {
			return val.(float64) / float64(2)
		})
	// Calculate the Base Line.
	base := series.Copy().Rolling(basePeriod).Max().Add(series.Copy().Rolling(basePeriod).Min()).
		Map(func(i int, val any) any {
			return val.(float64) / float64(2)
		})
	// Calculate the Leading Span A.
	leadingA := conv.Copy().Rolling(leadingPeriods).Max().Add(base.Copy().Rolling(leadingPeriods).Max()).
		Map(func(i int, val any) any {
			return val.(float64) / float64(2)
		})
	// Calculate the Leading Span B.
	leadingB := series.Copy().Rolling(leadingPeriods).Max().Add(series.Copy().Rolling(leadingPeriods).Min()).
		Map(func(i int, val any) any {
			return val.(float64) / float64(2)
		})
	// Calculate the Lagging Span.
	// lagging := series.Shift(-leadingPeriods)
	// Return a DataFrame of the results.
	return NewFrame(conv, base, leadingA, leadingB)
}
