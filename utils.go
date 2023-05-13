package autotrader

import "golang.org/x/exp/constraints"

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
