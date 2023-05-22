package autotrader

import (
	"errors"
	"math"
	"os/exec"
	"runtime"

	"golang.org/x/exp/constraints"
)

const float64Tolerance = float64(1e-6)

var ErrNotASignedNumber = errors.New("not a signed number")

// Crossover returns true if the latest a value crosses above the latest b value, but only if it just happened. For example, if a series is [1, 2, 3, 4, 5] and b series is [1, 2, 3, 4, 3], then Crossover(a, b) returns false because the latest a value is 5 and the latest b value is 3. However, if a series is [1, 2, 3, 4, 5] and b series is [1, 2, 3, 4, 6], then Crossover(a, b) returns true because the latest a value is 5 and the latest b value is 6
func Crossover(a, b *Series) bool {
	return a.Float(-1) > b.Float(-1) && a.Float(-2) <= b.Float(-2)
}

func CrossoverIndex[I comparable](index I, a, b *IndexedSeries[I]) bool {
	aRow, bRow := a.Row(index), b.Row(index)
	if aRow < 1 || bRow < 1 {
		return false
	}
	return a.Float(aRow) > b.Float(bRow) && a.Float(aRow-1) <= b.Float(bRow-1)
}

// EasyIndex returns an index to the `n` -length object that allows for negative indexing. For example, EasyIndex(-1, 5) returns 4. This is similar to Python's negative indexing. The return value may be less than zero if (-i) > n.
func EasyIndex(i, n int) int {
	if i < 0 {
		return n + i
	}
	return i
}

// EqualApprox returns true if a and b are approximately equal. NaN and Inf are handled correctly. The tolerance is 1e-6 or 0.0000001.
func EqualApprox(a, b float64) bool {
	if math.IsNaN(a) || math.IsNaN(b) {
		return math.IsNaN(a) && math.IsNaN(b)
	} else if math.IsInf(a, 1) || math.IsInf(b, 1) {
		return math.IsInf(a, 1) && math.IsInf(b, 1)
	} else if math.IsInf(a, -1) || math.IsInf(b, -1) {
		return math.IsInf(a, -1) && math.IsInf(b, -1)
	}
	return math.Abs(a-b) <= float64Tolerance
}

// Round returns f rounded to d decimal places. d may be negative to round to the left of the decimal point.
//
// Examples:
//
//	Round(123.456, 0) // 123.0
//	Round(123.456, 1) // 123.5
//	Round(123.456, -1) // 120.0
func Round(f float64, d int) float64 {
	ratio := math.Pow10(d)
	return math.Round(f*ratio) / ratio
}

func Abs[T constraints.Integer | constraints.Float](a T) T {
	if a < T(0) {
		return -a
	}
	return a
}

func Min[T constraints.Ordered](a, b T) T {
	if a < b {
		return a
	}
	return b
}

func Max[T constraints.Ordered](a, b T) T {
	if a > b {
		return a
	}
	return b
}

func LeverageToMargin(leverage float64) float64 {
	return 1 / leverage
}

func MarginToLeverage(margin float64) float64 {
	return 1 / margin
}

// Open opens the specified URL in the default browser of the user.
func Open(url string) error {
	var cmd string
	var args []string

	switch runtime.GOOS {
	case "windows":
		cmd = "cmd"
		args = []string{"/c", "start"}
	case "darwin":
		cmd = "open"
	default: // "linux", "freebsd", "openbsd", "netbsd"
		cmd = "xdg-open"
	}
	args = append(args, url)
	return exec.Command(cmd, args...).Start()
}

// LessAny returns true if a < b. a and b must be signed numbers. If a or b is not a signed number, then the function returns false, and the value that was first identified as not a signed number as the interface{} alias 'any'. The order of checking is a -> b. If a is not a signed number, then a is returned as the offender. Else if b is not a signed number, then b is returned as the offender. Else, nil is returned as the offender.
//
// A signed number is any of the following types:
//
//   - float64
//   - float32
//   - int
//   - int64
//   - int32
//   - int16
//   - int8
func LessAny(a, b any) (less bool, offender any) {
	switch a := a.(type) {
	case float64:
		switch b := b.(type) {
		case float64:
			return a < b, nil
		case float32:
			return a < float64(b), nil
		case int:
			return a < float64(b), nil
		case int64:
			return a < float64(b), nil
		case int32:
			return a < float64(b), nil
		case int16:
			return a < float64(b), nil
		case int8:
			return a < float64(b), nil
		default:
			return false, b
		}
	case float32:
		switch b := b.(type) {
		case float64:
			return float64(a) < b, nil
		case float32:
			return a < b, nil
		case int:
			return float64(a) < float64(b), nil
		case int64:
			return float64(a) < float64(b), nil
		case int32:
			return a < float32(b), nil
		case int16:
			return a < float32(b), nil
		case int8:
			return a < float32(b), nil
		default:
			return false, b
		}
	case int:
		switch b := b.(type) {
		case float64:
			return float64(a) < b, nil
		case float32:
			return float64(a) < float64(b), nil
		case int:
			return a < b, nil
		case int64:
			return int64(a) < b, nil
		case int32:
			return a < int(b), nil
		case int16:
			return a < int(b), nil
		case int8:
			return a < int(b), nil
		default:
			return false, b
		}
	case int64:
		switch b := b.(type) {
		case float64:
			return float64(a) < b, nil
		case float32:
			return float64(a) < float64(b), nil
		case int:
			return a < int64(b), nil
		case int64:
			return a < b, nil
		case int32:
			return a < int64(b), nil
		case int16:
			return a < int64(b), nil
		case int8:
			return a < int64(b), nil
		default:
			return false, b
		}
	case int32:
		switch b := b.(type) {
		case float64:
			return float64(a) < b, nil
		case float32:
			return float32(a) < float32(b), nil
		case int:
			return int(a) < b, nil
		case int64:
			return int64(a) < b, nil
		case int32:
			return a < b, nil
		case int16:
			return a < int32(b), nil
		case int8:
			return a < int32(b), nil
		default:
			return false, b
		}
	case int16:
		switch b := b.(type) {
		case float64:
			return float64(a) < b, nil
		case float32:
			return float32(a) < b, nil
		case int:
			return int(a) < b, nil
		case int64:
			return int64(a) < b, nil
		case int32:
			return int32(a) < b, nil
		case int16:
			return a < b, nil
		case int8:
			return a < int16(b), nil
		default:
			return false, b
		}
	case int8:
		switch b := b.(type) {
		case float64:
			return float64(a) < b, nil
		case float32:
			return float32(a) < b, nil
		case int:
			return int(a) < b, nil
		case int64:
			return int64(a) < b, nil
		case int32:
			return int32(a) < b, nil
		case int16:
			return int16(a) < b, nil
		case int8:
			return a < b, nil
		default:
			return false, b
		}
	}
	return false, a
}
