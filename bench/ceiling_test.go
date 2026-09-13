//go:build !race

package main

import "testing"

func TestClientCeiling(t *testing.T) {
	if testing.Short() {
		t.Skip("client ceiling measurement")
	}
	srv := startFakeServer(t)
	setVar(t, &addrs, []string{srv.addr()})
	setVar(t, &CLIENTS, 100)
	setVar(t, &OPS_CLIENT, 100_000)
	setVar(t, &MIX_OPS_CLIENT, 50_000)
	setVar(t, &MIX_HOTKEY_OPS, 100_000)
	setVar(t, &MIX_QUEUE_OPS, 50_000)
	setVar(t, &PUB_SUBSCRIBERS, 5)
	setVar(t, &PUB_PUBLISHERS, 2)
	setVar(t, &PUB_MSGS_EACH, 20_000)

	warmup()
	runKV()
	runMix()
	runPubSub(srv.addr())
}
