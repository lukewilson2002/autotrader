package autotrader

import (
	"math"
	"time"
)

// RSI calculates the Relative Strength Index for a given Series. Typically, the input series is the Close column of a DataFrame. Returns a Series of RSI values of the same length as the input.
//
// Traditionally, an RSI reading of 70 or above indicates an overbought condition, and a reading of 30 or below indicates an oversold condition.
//
// Typically, the RSI is calculated with a period of 14 days.
func RSI(series *FloatSeries, periods int) *FloatSeries {
	// Calculate the difference between each day's close and the previous day's close.
	delta := series.Copy().MapReverse(func(i int, v float64) float64 {
		if i == 0 {
			return 0
		}
		return v - series.Value(i-1)
	})
	// Calculate the average gain and average loss.
	avgGain := &FloatSeries{delta.Copy().
		Map(func(i int, val float64) float64 { return math.Max(val, 0) }).
		Rolling(periods).Average()}
	avgLoss := &FloatSeries{delta.Copy().
		Map(func(i int, val float64) float64 { return math.Abs(math.Min(val, 0)) }).
		Rolling(periods).Average()}

	// Calculate the RSI.
	return avgGain.Map(func(i int, val float64) float64 {
		loss := avgLoss.Float(i)
		if loss == 0 {
			return float64(100)
		}
		return float64(100 - 100/(1+val/loss))
	}).SetName("RSI")
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
func Ichimoku(price *IndexedFrame[UnixTime], convPeriod, basePeriod, leadingPeriods int, frequency time.Duration) *IndexedFrame[UnixTime] {
	// TODO: make this run concurrently.

	conv := price.Highs().Copy().Rolling(convPeriod).Max().Add(price.Lows().Copy().Rolling(convPeriod).Min()).DivFloat(2)
	base := price.Highs().Copy().Rolling(basePeriod).Max().Add(price.Lows().Copy().Rolling(basePeriod).Min()).DivFloat(2)
	lagging := price.Closes().Copy()
	leadingA := conv.Copy().Add(base).DivFloat(2)
	leadingB := price.Highs().Copy().Rolling(leadingPeriods).Max().Add(price.Lows().Copy().Rolling(leadingPeriods).Min()).DivFloat(2)

	// Return a DataFrame of the results.
	return NewIndexedFrame(
		conv.SetName("Conversion"),
		base.SetName("Base"),
		leadingA.SetName("LeadingA").ShiftIndex(leadingPeriods, UnixTimeStep(frequency)),
		leadingB.SetName("LeadingB").ShiftIndex(leadingPeriods, UnixTimeStep(frequency)),
		lagging.SetName("Lagging").ShiftIndex(-leadingPeriods, UnixTimeStep(frequency)),
	)
}
