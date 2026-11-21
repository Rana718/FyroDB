package main

import (
	"bufio"
	"fmt"
	"net"
	"time"
)

var slotOwners []int

func clusterQuery(addr, subcommand string) (any, error) {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	if err := conn.SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
		return nil, err
	}
	if _, err := fmt.Fprintf(conn, "*2\r\n$7\r\nCLUSTER\r\n$%d\r\n%s\r\n", len(subcommand), subcommand); err != nil {
		return nil, err
	}
	resp, err := readRESPValue(bufio.NewReader(conn))
	if err != nil {
		return nil, err
	}
	if err, ok := resp.(error); ok {
		return nil, err
	}
	return resp, nil
}

func discoverSlotOwners(addrs []string) ([]int, error) {
	ids := make(map[string]int, len(addrs))
	for i, addr := range addrs {
		reply, err := clusterQuery(addr, "MYID")
		if err != nil {
			return nil, err
		}
		id, ok := reply.(string)
		if !ok || id == "" {
			return nil, fmt.Errorf("invalid CLUSTER MYID from %s", addr)
		}
		if _, exists := ids[id]; exists {
			return nil, fmt.Errorf("duplicate cluster node %q", id)
		}
		ids[id] = i
	}
	reply, err := clusterQuery(addrs[0], "SLOTS")
	if err != nil {
		return nil, err
	}
	return indexSlotOwners(reply, ids)
}

func indexSlotOwners(reply any, ids map[string]int) ([]int, error) {
	ranges, ok := reply.([]any)
	if !ok {
		return nil, fmt.Errorf("unexpected CLUSTER SLOTS reply")
	}
	owners := make([]int, 16384)
	for i := range owners {
		owners[i] = -1
	}
	counts := make([]int, len(ids))
	for _, item := range ranges {
		r, ok := item.([]any)
		if !ok || len(r) < 3 {
			return nil, fmt.Errorf("invalid cluster slot range")
		}
		start, startOK := r[0].(int64)
		end, endOK := r[1].(int64)
		node, nodeOK := r[2].([]any)
		if !startOK || !endOK || start < 0 || end < start || end >= int64(len(owners)) || !nodeOK || len(node) < 3 {
			return nil, fmt.Errorf("invalid slot bounds or primary in CLUSTER SLOTS")
		}
		id, _ := node[2].(string)
		index, exists := ids[id]
		if !exists {
			return nil, fmt.Errorf("slot owner %q is missing from -cluster", id)
		}
		for slot := start; slot <= end; slot++ {
			if owners[slot] != -1 {
				return nil, fmt.Errorf("multiple owners for slot %d", slot)
			}
			owners[slot] = index
			counts[index]++
		}
	}
	for i, n := range counts {
		if n == 0 {
			return nil, fmt.Errorf("node %d owns no slots", i)
		}
	}
	for i, owner := range owners {
		if owner < 0 {
			return nil, fmt.Errorf("slot %d has no owner", i)
		}
	}
	return owners, nil
}
