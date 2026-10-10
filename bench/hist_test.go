package main

import (
	"testing"
	"time"
)

func TestHistPercentilesWithinResolution(t *testing.T) {
	var h hist
	for i := 1; i <= 100_000; i++ {
		h.record(time.Duration(i) * time.Microsecond)
	}
	for _, c := range []struct {
		p    float64
		want time.Duration
	}{{50, 50 * time.Millisecond}, {99, 99 * time.Millisecond}, {99.9, 99900 * time.Microsecond}} {
		got := h.percentile(c.p)
		if got < c.want || float64(got) > float64(c.want)*1.04 {
			t.Fatalf("p%.1f = %v, want ~%v", c.p, got, c.want)
		}
	}
	if h.percentile(100) != 100*time.Millisecond {
		t.Fatalf("max = %v", h.percentile(100))
	}
}

func TestHistIndexMonotonic(t *testing.T) {
	prev := -1
	for v := uint64(0); v < 1<<22; v += 7 {
		i := histIndex(v)
		if i < prev || histValue(i) < v {
			t.Fatalf("v=%d idx=%d prev=%d upper=%d", v, i, prev, histValue(i))
		}
		prev = i
	}
}
