package main

import (
	"fmt"
	"math/bits"
	"time"
)

// Log-linear latency histogram (HdrHistogram layout, 5 significant bits):
// values below 64ns get one bucket each; above that every power of two is
// split into 32 linear sub-buckets, so any recorded value is reported within
// ~3% of its true magnitude. Fixed size, allocation-free on Record.
const (
	histSubBits = 5
	histSub     = 1 << histSubBits
	histLinear  = 2 * histSub
	histMaxExp  = 40 // 2^40 ns ≈ 18 minutes
	histBuckets = histLinear + histMaxExp*histSub
)

type hist struct {
	counts [histBuckets]uint64
	total  uint64
	max    uint64
	sum    uint64
}

func histIndex(v uint64) int {
	if v < histLinear {
		return int(v)
	}
	exp := bits.Len64(v) - histSubBits - 1
	if exp > histMaxExp {
		return histBuckets - 1
	}
	return histLinear + (exp-1)*histSub + int(v>>uint(exp)) - histSub
}

// histValue is the upper edge of bucket i, so percentiles never under-report.
func histValue(i int) uint64 {
	if i < histLinear {
		return uint64(i)
	}
	exp := (i-histLinear)/histSub + 1
	sub := uint64((i-histLinear)%histSub + histSub)
	return ((sub + 1) << uint(exp)) - 1
}

func (h *hist) record(d time.Duration) {
	v := uint64(max(d, 0))
	h.counts[histIndex(v)]++
	h.total++
	h.sum += v
	if v > h.max {
		h.max = v
	}
}

func (h *hist) merge(o *hist) {
	for i, c := range o.counts {
		h.counts[i] += c
	}
	h.total += o.total
	h.sum += o.sum
	if o.max > h.max {
		h.max = o.max
	}
}

func (h *hist) percentile(p float64) time.Duration {
	if h.total == 0 {
		return 0
	}
	want := uint64(p / 100 * float64(h.total))
	if want >= h.total {
		want = h.total - 1
	}
	var seen uint64
	for i, c := range h.counts {
		seen += c
		if seen > want {
			return time.Duration(min(histValue(i), h.max))
		}
	}
	return time.Duration(h.max)
}

func (h *hist) mean() time.Duration {
	if h.total == 0 {
		return 0
	}
	return time.Duration(h.sum / h.total)
}

func fmtDur(d time.Duration) string {
	switch {
	case d >= time.Second:
		return fmt.Sprintf("%.2fs", d.Seconds())
	case d >= time.Millisecond:
		return fmt.Sprintf("%.2fms", float64(d)/1e6)
	default:
		return fmt.Sprintf("%.1fµs", float64(d)/1e3)
	}
}

// latencySummary is what every latency-aware phase reports and exports.
type latencySummary struct {
	Label  string  `json:"label"`
	Ops    uint64  `json:"ops"`
	OpsSec float64 `json:"ops_per_sec"`
	MeanNs int64   `json:"mean_ns"`
	P50Ns  int64   `json:"p50_ns"`
	P90Ns  int64   `json:"p90_ns"`
	P99Ns  int64   `json:"p99_ns"`
	P999Ns int64   `json:"p999_ns"`
	MaxNs  int64   `json:"max_ns"`
	Recall float64 `json:"recall,omitempty"`
}

var latencyResults []latencySummary

func summarize(label string, h *hist, elapsed time.Duration) latencySummary {
	return latencySummary{
		Label:  label,
		Ops:    h.total,
		OpsSec: float64(h.total) / elapsed.Seconds(),
		MeanNs: int64(h.mean()),
		P50Ns:  int64(h.percentile(50)),
		P90Ns:  int64(h.percentile(90)),
		P99Ns:  int64(h.percentile(99)),
		P999Ns: int64(h.percentile(99.9)),
		MaxNs:  int64(h.max),
	}
}

func printLatency(s latencySummary) {
	fmt.Printf("── %s\n", s.Label)
	fmt.Printf("   ops: %d  throughput: %s\n", s.Ops, fmtRate(s.OpsSec))
	fmt.Printf("   mean %s  p50 %s  p90 %s  p99 %s  p99.9 %s  max %s\n",
		fmtDur(time.Duration(s.MeanNs)), fmtDur(time.Duration(s.P50Ns)),
		fmtDur(time.Duration(s.P90Ns)), fmtDur(time.Duration(s.P99Ns)),
		fmtDur(time.Duration(s.P999Ns)), fmtDur(time.Duration(s.MaxNs)))
	if s.Recall > 0 {
		fmt.Printf("   recall@k: %.4f\n", s.Recall)
	}
	fmt.Println()
}

func recordLatency(label string, h *hist, elapsed time.Duration) latencySummary {
	s := summarize(label, h, elapsed)
	printLatency(s)
	latencyResults = append(latencyResults, s)
	return s
}
