package autotrader

import (
	"math"
	"os/exec"
	"runtime"

	"golang.org/x/exp/constraints"
)

const float64Tolerance = float64(1e-6)

// Crossover returns true if the latest a value crosses above the latest b value, but only if it just happened. For example, if a series is [1, 2, 3, 4, 5] and b series is [1, 2, 3, 4, 3], then Crossover(a, b) returns false because the latest a value is 5 and the latest b value is 3. However, if a series is [1, 2, 3, 4, 5] and b series is [1, 2, 3, 4, 6], then Crossover(a, b) returns true because the latest a value is 5 and the latest b value is 6
func Crossover(a, b Series) bool {
	return a.Float(-1) > b.Float(-1) && a.Float(-2) <= b.Float(-2)
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
