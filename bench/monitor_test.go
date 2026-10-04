package main

import (
	"math"
	"testing"
)

func TestProcessCPUUsesPerCoreScale(t *testing.T) {
	for _, tc := range []struct {
		ticks             int64
		hz, seconds, want float64
	}{
		{100, 100, 1, 100},
		{1200, 100, 1, 1200},
		{120, 100, 0.1, 1200},
		{-1, 100, 1, 0},
		{100, 100, 0, 0},
	} {
		if got := processCPUPercent(tc.ticks, tc.hz, tc.seconds); math.Abs(got-tc.want) > 1e-9 {
			t.Fatalf("CPU percent = %v, want %v", got, tc.want)
		}
	}
}
