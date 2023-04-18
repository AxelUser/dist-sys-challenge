package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastSvc struct {
	seen    map[int]struct{}
	m       []int
	pending map[string]map[int]struct{}
	msgLock sync.RWMutex
	brLock  sync.RWMutex
}

func createBroadcastSvc() *broadcastSvc {
	return &broadcastSvc{
		pending: make(map[string]map[int]struct{}),
		seen:    make(map[int]struct{}),
		m:       make([]int, 0),
	}
}

func (svc *broadcastSvc) add(v int) bool {
	svc.msgLock.Lock()
	var seen bool
	if _, seen = svc.seen[v]; !seen {
		svc.seen[v] = struct{}{}
		svc.m = append(svc.m, v)
	}
	svc.msgLock.Unlock()
	return !seen
}

func (svc *broadcastSvc) values() []int {
	svc.msgLock.RLock()
	cp := make([]int, len(svc.m))
	copy(cp, svc.m)
	svc.msgLock.RUnlock()
	return cp
}

func (svc *broadcastSvc) updateTopology(nodes []string) {
	svc.brLock.Lock()
	for _, n := range nodes {
		if _, ok := svc.pending[n]; !ok {
			svc.pending[n] = make(map[int]struct{})
		}
	}
	svc.brLock.Unlock()
}

func (svc *broadcastSvc) unsent() map[string][]int {
	unsent := make(map[string][]int)
	svc.brLock.Lock()
	for node, nPending := range svc.pending {
		ls := make([]int, len(nPending))
		for k, _ := range nPending {
			ls = append(ls, k)
		}
		unsent[node] = ls
	}
	svc.brLock.Unlock()
	return unsent
}

func (svc *broadcastSvc) startSend(msg int) {
	svc.brLock.Lock()
	for node, _ := range svc.pending {
		svc.pending[node][msg] = struct{}{}
	}
	svc.brLock.Unlock()
}

func (svc *broadcastSvc) commitSent(node string, msg int) {
	svc.brLock.Lock()
	if nPending, ok := svc.pending[node]; ok {
		delete(nPending, msg)
	}
	svc.brLock.Unlock()
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
			svc.startSend(body.Message)
			unsent := svc.unsent()
			for node, nodeMessages := range unsent {
				for message := range nodeMessages {
					func(nd string, m int) {
						n.RPC(nd, broadcastBody{
							Type:    "broadcast",
							Message: message,
						}, func(msg maelstrom.Message) error {
							svc.commitSent(node, m)
							return nil
						})
					}(node, message)
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
