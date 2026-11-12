package main

import (
	"bufio"
	"bytes"
	"fmt"
	"net"
	"testing"
)

func TestANNSearchOptionsMatchTheIndexAlgorithm(t *testing.T) {
	for _, tc := range []struct {
		ef     int
		filter string
		want   string
	}{
		{0, "", "*=>[KNN 10 @vec $v AS score]"},
		{0, "@bucket:[0 9]", "(@bucket:[0 9])=>[KNN 10 @vec $v AS score]"},
		{80, "", "*=>[KNN 10 @vec $v EF_RUNTIME 80 AS score]"},
	} {
		wire := annSearchCmd("idx", 10, tc.ef, tc.filter, []byte{0})
		value := readValue(bufio.NewReader(bytes.NewReader(wire))).([]any)
		if value[2] != tc.want {
			t.Fatalf("query = %q, want %q", value[2], tc.want)
		}
	}
}

func TestANNReadinessRejectsPartialResultsAndActiveIndexing(t *testing.T) {
	t.Setenv("ANN_READY_RECALL", "1")
	listener, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	done := make(chan int, 1)
	go func() {
		conn, err := listener.AcceptTCP()
		if err != nil {
			done <- -1
			return
		}
		defer conn.Close()
		r := bufio.NewReader(conn)
		queries := 0
		for {
			if _, err := r.Peek(1); err != nil {
				done <- queries
				return
			}
			cmd := readValue(r).([]any)
			if cmd[0] == "FT.SEARCH" {
				queries++
				id := 7
				if queries == 1 {
					id = 8
				}
				fmt.Fprintf(conn, "*2\r\n:1\r\n$5\r\nann:%d\r\n", id)
			} else {
				indexing := 0
				if queries == 2 {
					indexing = 1
				}
				fmt.Fprintf(conn, "*2\r\n$8\r\nindexing\r\n:%d\r\n", indexing)
			}
		}
	}()
	conn, err := net.DialTCP("tcp", nil, listener.Addr().(*net.TCPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	verifyANNReady(conn, bufio.NewReader(conn), "idx", annConfig{k: 1}, [][]byte{{0}}, [][]int{{7}})
	conn.Close()
	if queries := <-done; queries != 5 {
		t.Fatalf("readiness completed after %d queries, want 5", queries)
	}
}
