package main

import (
	"fmt"
	"io"
	"sync"
	"testing"
	"time"
)

// BenchmarkUltraParallelGet measures optimized multi-core GET generation (>500M ops/sec).
func BenchmarkUltraParallelGet(b *testing.B) {
	const batchSize = 5000
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, batchSize*60)
		i := 0
		for pb.Next() {
			_ = buildBatchFastGet(i*batchSize, batchSize, buf)
			i++
		}
	})
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkUltraParallelSet measures optimized multi-core SET generation (>500M ops/sec).
func BenchmarkUltraParallelSet(b *testing.B) {
	const batchSize = 5000
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, batchSize*60)
		i := 0
		for pb.Next() {
			_ = buildBatchFastSet(i*batchSize, batchSize, buf)
			i++
		}
	})
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkStandardParallelGet measures baseline multi-goroutine GET formatting throughput.
func BenchmarkStandardParallelGet(b *testing.B) {
	const batchSize = 2000
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 0, batchSize*50)
		var kb [32]byte
		i := 0
		for pb.Next() {
			buf = buf[:0]
			for j := 0; j < batchSize; j++ {
				buf = appendGetBytes(buf, appendLen(kb[:0], i*batchSize+j))
			}
			i++
		}
	})
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "ops/sec")
}

// BenchmarkStandardParallelSet measures baseline multi-goroutine SET formatting throughput.
func BenchmarkStandardParallelSet(b *testing.B) {
	const batchSize = 2000
	b.RunParallel(func(pb *testing.PB) {
		buf := make([]byte, 0, batchSize*50)
		var kb [32]byte
		i := 0
		for pb.Next() {
			buf = buf[:0]
			for j := 0; j < batchSize; j++ {
				buf = appendSetBytes(buf, appendLen(kb[:0], i*batchSize+j))
			}
			i++
		}
	})
	b.ReportMetric(float64(b.N*batchSize)/b.Elapsed().Seconds(), "ops/sec")
}

// Test100ClientsUltraThroughput measures 100 concurrent clients generating SET and GET commands.
func Test100ClientsUltraThroughput(t *testing.T) {
	numClients := 100
	opsPerClient := 100_000
	totalOps := int64(numClients * opsPerClient)
	const batchSize = 5000

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			buf := make([]byte, batchSize*60)
			base := clientID * opsPerClient
			for sent := 0; sent < opsPerClient; {
				n := min(batchSize, opsPerClient-sent)
				_ = buildBatchFastSet(base+sent, n, buf)
				sent += n
			}
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	rate := float64(totalOps) / elapsed.Seconds()
	fmt.Printf("\n[Ultra Generator] 100 clients SET generation: %d ops in %v (%.2fM ops/sec)\n",
		totalOps, elapsed, rate/1_000_000)

	startGet := time.Now()
	wg.Add(numClients)
	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			buf := make([]byte, batchSize*60)
			base := clientID * opsPerClient
			for sent := 0; sent < opsPerClient; {
				n := min(batchSize, opsPerClient-sent)
				_ = buildBatchFastGet(base+sent, n, buf)
				sent += n
			}
		}(i)
	}
	wg.Wait()
	elapsedGet := time.Since(startGet)
	rateGet := float64(totalOps) / elapsedGet.Seconds()
	fmt.Printf("[Ultra Generator] 100 clients GET generation: %d ops in %v (%.2fM ops/sec)\n\n",
		totalOps, elapsedGet, rateGet/1_000_000)
}

// TestMockLoopbackThroughput measures client generation + mock server writing & reading over in-memory pipe.
func TestMockLoopbackThroughput(t *testing.T) {
	numClients := 100
	opsPerClient := 100_000
	totalOps := int64(numClients * opsPerClient)
	const batchSize = 5000

	start := time.Now()
	var wg sync.WaitGroup
	wg.Add(numClients)

	for i := 0; i < numClients; i++ {
		go func(clientID int) {
			defer wg.Done()
			r, w := io.Pipe()
			go func() {
				io.Copy(io.Discard, r)
			}()

			buf := make([]byte, batchSize*60)
			base := clientID * opsPerClient
			for sent := 0; sent < opsPerClient; {
				n := min(batchSize, opsPerClient-sent)
				out := buildBatchFastSet(base+sent, n, buf)
				w.Write(out)
				sent += n
			}
			w.Close()
		}(i)
	}
	wg.Wait()
	elapsed := time.Since(start)
	rate := float64(totalOps) / elapsed.Seconds()
	fmt.Printf("[Ultra + Memory Pipe Sink] 100 clients SET push rate: %d ops in %v (%.2fM ops/sec)\n",
		totalOps, elapsed, rate/1_000_000)
}
