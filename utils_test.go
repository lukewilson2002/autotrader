package autotrader

import (
	"math"
	"testing"
)

func TestEqualApprox(t *testing.T) {
	if !EqualApprox(0.0000000, float64Tolerance/10) { // 1e-6
		t.Error("Expected 0.0000000 to be approximately equal to 0.0000001")
	}
	if EqualApprox(0.0000000, float64Tolerance+float64Tolerance/10) {
		t.Error("Expected 0.0000000 to not be approximately equal to 0.0000011")
	}
	if !EqualApprox(math.NaN(), math.NaN()) {
		t.Error("Expected NaN to be approximately equal to NaN")
	}
	if EqualApprox(math.NaN(), 0) {
		t.Error("Expected NaN to not be approximately equal to 0")
	}
	if !EqualApprox(math.Inf(1), math.Inf(1)) {
		t.Error("Expected Inf to be approximately equal to Inf")
	}
	if EqualApprox(math.Inf(-1), math.Inf(1)) {
		t.Error("Expected -Inf to not be approximately equal to Inf")
	}
	if EqualApprox(1, 2) {
		t.Error("Expected 1 to not be approximately equal to 2")
	}
	if !EqualApprox(0.3, 0.6/2) {
		t.Errorf("Expected 0.3 to be approximately equal to %f", 6.0/2)
	}
}

func TestRound(t *testing.T) {
	if Round(0.1234567, 0) != 0 {
		t.Error("Expected 0.1234567 to round to 0")
	}
	if Round(0.1234567, 1) != 0.1 {
		t.Error("Expected 0.1234567 to round to 0.1")
	}
	if Round(0.1234567, 2) != 0.12 {
		t.Error("Expected 0.1234567 to round to 0.12")
	}
	if Round(0.128, 2) != 0.13 {
		t.Error("Expected 0.128 to round to 0.13")
	}
	if Round(12.34, -1) != 10 {
		t.Error("Expected 12.34 to round to 10")
	}
}
