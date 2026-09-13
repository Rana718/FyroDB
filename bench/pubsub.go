package main

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const PUB_PIPE_SIZE = 200

func runPubSub(addr string) {
	channel := "bench:pubsub"

	totalMsgs := int64(PUB_PUBLISHERS * PUB_MSGS_EACH)
	totalExpected := int64(PUB_SUBSCRIBERS) * totalMsgs

	fmt.Printf("── Pub/Sub Benchmark (%s) ────────────────────\n", addr)
	fmt.Printf("subscribers=%d  publishers=%d  msgs/publisher=%d  pipe_size=%d\n",
		PUB_SUBSCRIBERS, PUB_PUBLISHERS, PUB_MSGS_EACH, PUB_PIPE_SIZE)
	fmt.Printf("total publishes=%d  total deliveries expected=%d\n\n",
		totalMsgs, totalExpected)

	msgPayload := "hello-pubsub-bench-msg"
	messageFrameBytes := int64(len(buildMessageFrame(channel, msgPayload)))

	var received int64

	startSignal := make(chan struct{})
	var subReady sync.WaitGroup
	var recvDone sync.WaitGroup

	subCmd := buildSubCmd(channel)

	subConns := make([]net.Conn, PUB_SUBSCRIBERS)
	for i := range PUB_SUBSCRIBERS {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("sub connect error: %v\n", err)
			return
		}
		tc := conn.(*net.TCPConn)
		tc.SetNoDelay(true)
		tc.SetReadBuffer(2 << 20)
		subConns[i] = conn

		conn.Write(subCmd)
		conn.SetReadDeadline(time.Now().Add(5 * time.Second))

		r := bufio.NewReaderSize(conn, 512)
		consumeSubConfirm(r)
		conn.SetReadDeadline(time.Time{})
		if r.Buffered() > 0 {
			tmp := make([]byte, r.Buffered())
			r.Read(tmp)
		}

		subReady.Add(1)
		recvDone.Go(func() {
			subReady.Done()
			<-startSignal

			bytesRead, err := io.CopyN(io.Discard, conn, totalMsgs*messageFrameBytes)
			localCount := bytesRead / messageFrameBytes
			if err == nil {
				localCount = totalMsgs
			}
			atomic.AddInt64(&received, localCount)
		})
	}

	subReady.Wait()

	pubConns := make([]*net.TCPConn, PUB_PUBLISHERS)
	for i := range PUB_PUBLISHERS {
		conn, err := net.Dial("tcp", addr)
		if err != nil {
			fmt.Printf("pub connect error: %v\n", err)
			return
		}
		tc := conn.(*net.TCPConn)
		tc.SetNoDelay(true)
		tc.SetWriteBuffer(1 << 19)
		tc.SetReadBuffer(1 << 18)
		pubConns[i] = tc
	}

	pubCmd := buildPubCmd(channel, msgPayload)

	close(startSignal)
	pubStart := time.Now()

	var published int64
	var pubWg sync.WaitGroup

	for i := range PUB_PUBLISHERS {
		pubWg.Go(func() {
			conn := pubConns[i]
			defer conn.Close()

			conn.SetDeadline(time.Now().Add(30 * time.Second))
			r := bufio.NewReaderSize(conn, 128<<10)
			requests := make([]byte, 0, PUB_PIPE_SIZE*len(pubCmd))

			sent := 0
			for sent < PUB_MSGS_EACH {
				batch := min(PUB_PIPE_SIZE, PUB_MSGS_EACH-sent)
				requests = requests[:0]
				for range batch {
					requests = append(requests, pubCmd...)
				}
				writeFull(conn, requests)
				skipLines(r, batch)
				sent += batch
			}
			atomic.AddInt64(&published, int64(sent))
		})
	}
	pubWg.Wait()
	pubElapsed := time.Since(pubStart)

	done := make(chan struct{})
	go func() {
		recvDone.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
	}
	recvElapsed := time.Since(pubStart)

	for _, c := range subConns {
		c.Close()
	}

	pubRate := rate(published, pubElapsed)
	deliveryRate := rate(atomic.LoadInt64(&received), recvElapsed)

	mixResults = append(mixResults,
		benchResult{"Pub/Sub publish", published, pubElapsed},
		benchResult{"Pub/Sub delivery", atomic.LoadInt64(&received), recvElapsed},
	)

	fmt.Printf("── Publish throughput\n")
	fmt.Printf("   published: %d\n", published)
	fmt.Printf("   elapsed:   %s\n", pubElapsed.Round(time.Millisecond))
	fmt.Printf("   ops/sec:   %s\n\n", fmtRate(pubRate))

	fmt.Printf("── End-to-end delivery\n")
	fmt.Printf("   delivered:      %d / %d\n", atomic.LoadInt64(&received), totalExpected)
	fmt.Printf("   e2e elapsed:    %s\n", recvElapsed.Round(time.Millisecond))
	fmt.Printf("   delivery rate:  %s\n\n", fmtRate(deliveryRate))

	fmt.Println("── Pub/Sub Summary ─────────────────────────────")
	fmt.Printf("publish throughput:   %s\n", fmtRate(pubRate))
	fmt.Printf("delivery throughput:  %s\n", fmtRate(deliveryRate))
	fmt.Printf("fan-out factor:       %dx  (%d subs × %d msgs)\n",
		PUB_SUBSCRIBERS, PUB_SUBSCRIBERS, totalMsgs)
}

func buildMessageFrame(channel, message string) []byte {
	b := make([]byte, 0, 32+len(channel)+len(message))
	b = append(b, "*3\r\n$7\r\nmessage\r\n$"...)
	b = strconv.AppendInt(b, int64(len(channel)), 10)
	b = append(b, "\r\n"...)
	b = append(b, channel...)
	b = append(b, "\r\n$"...)
	b = strconv.AppendInt(b, int64(len(message)), 10)
	b = append(b, "\r\n"...)
	b = append(b, message...)
	b = append(b, "\r\n"...)
	return b
}

func consumeSubConfirm(r *bufio.Reader) {
	r.ReadSlice('\n')
	r.ReadSlice('\n')
	r.ReadSlice('\n')
	line, _ := r.ReadSlice('\n')
	chanLen := 0
	for _, c := range line {
		if c >= '0' && c <= '9' {
			chanLen = chanLen*10 + int(c-'0')
		} else if c == '$' {
			continue
		} else {
			break
		}
	}
	discardN(r, chanLen+2)
	r.ReadSlice('\n')
}

func buildSubCmd(channel string) []byte {
	b := make([]byte, 0, 64)
	b = append(b, "*2\r\n$9\r\nSUBSCRIBE\r\n$"...)
	b = strconv.AppendInt(b, int64(len(channel)), 10)
	b = append(b, "\r\n"...)
	b = append(b, channel...)
	b = append(b, "\r\n"...)
	return b
}

func buildPubCmd(channel, message string) []byte {
	b := make([]byte, 0, 64+len(channel)+len(message))
	b = append(b, "*3\r\n$7\r\nPUBLISH\r\n$"...)
	b = strconv.AppendInt(b, int64(len(channel)), 10)
	b = append(b, "\r\n"...)
	b = append(b, channel...)
	b = append(b, "\r\n$"...)
	b = strconv.AppendInt(b, int64(len(message)), 10)
	b = append(b, "\r\n"...)
	b = append(b, message...)
	b = append(b, "\r\n"...)
	return b
}
