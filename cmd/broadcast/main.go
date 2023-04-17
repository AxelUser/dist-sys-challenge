package main

import (
	"encoding/json"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type broadcastSvc struct {
	m     []any
	mLock sync.RWMutex
}

func createBroadcastSvc() *broadcastSvc {
	return &broadcastSvc{
		m: make([]any, 0),
	}
}

func (svc *broadcastSvc) add(v any) {
	svc.mLock.Lock()
	svc.m = append(svc.m, v)
	svc.mLock.Unlock()
}

func (svc *broadcastSvc) values() []any {
	svc.mLock.RLock()
	cp := make([]any, len(svc.m))
	copy(cp, svc.m)
	svc.mLock.RUnlock()
	return cp
}

func main() {
	n := maelstrom.NewNode()
	svc := createBroadcastSvc()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		svc.add(body["message"])
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

	n.Handle("topology", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := make(map[string]any)
		res["type"] = "topology_ok"

		return n.Reply(msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
