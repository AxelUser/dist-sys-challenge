package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastSvc struct {
	seen  map[int]struct{}
	m     []int
	t     map[string]int
	mLock sync.RWMutex
	tLock sync.RWMutex
}

func createBroadcastSvc() *broadcastSvc {
	return &broadcastSvc{
		seen: make(map[int]struct{}),
		t:    make(map[string]int),
		m:    make([]int, 0),
	}
}

func (svc *broadcastSvc) add(v int) bool {
	svc.mLock.Lock()
	var seen bool
	if _, seen = svc.seen[v]; !seen {
		svc.seen[v] = struct{}{}
		svc.m = append(svc.m, v)
	}
	svc.mLock.Unlock()
	return !seen
}

func (svc *broadcastSvc) values() []int {
	svc.mLock.RLock()
	cp := make([]int, len(svc.m))
	copy(cp, svc.m)
	svc.mLock.RUnlock()
	return cp
}

func (svc *broadcastSvc) updateTopology(nodes []string) {
	svc.tLock.Lock()
	for _, n := range nodes {
		if _, ok := svc.t[n]; !ok {
			svc.t[n] = 0
		}
	}
	svc.tLock.Unlock()
}

func (svc *broadcastSvc) unsent() map[string][]int {
	svc.tLock.Lock()
	seen := svc.values()
	len := len(seen)
	unsent := make(map[string][]int)
	for node, sent := range svc.t {
		unsent[node] = seen[sent:]
		svc.t[node] = len
	}
	svc.tLock.Unlock()
	return unsent
}

func main() {
	n := maelstrom.NewNode()
	svc := createBroadcastSvc()

	type broadcastBody struct {
		Type    string `json:"type"`
		Message int    `json:"message"`
	}

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if svc.add(body.Message) {
			unsent := svc.unsent()
			for node, values := range unsent {
				if node == msg.Src {
					continue
				}
				for _, v := range values {
					n.RPC(node, broadcastBody{
						Type:    "broadcast",
						Message: v,
					}, func(msg maelstrom.Message) error { return nil })
				}
			}
		}

		res := make(map[string]any)
		res["type"] = "broadcast_ok"
		return n.Reply(msg, res)
	})

	n.Handle("read", func(msg maelstrom.Message) error {
		body := make(map[string]any)

		body["type"] = "read_ok"
		body["messages"] = svc.values()

		return n.Reply(msg, body)
	})

	type topologyBody struct {
		Topology map[string][]string `json:"topology"`
	}

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body topologyBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		svc.updateTopology(body.Topology[n.ID()])
		res := make(map[string]any)
		res["type"] = "topology_ok"

		return n.Reply(msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
