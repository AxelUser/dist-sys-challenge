package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type workItem struct {
	mid string
	dst string
	msg broadcastBody
}

type broadcastBody struct {
	Type    string `json:"type"`
	Message int    `json:"message"`
}

type broadcastSvc struct {
	seen    map[int]struct{}
	pending map[string]map[int]struct{}
	nbrs    []string
	msgLock sync.RWMutex
}

func createBroadcastSvc() *broadcastSvc {
	return &broadcastSvc{
		pending: make(map[string]map[int]struct{}),
		seen:    make(map[int]struct{}),
		nbrs:    make([]string, 0),
	}
}

func (svc *broadcastSvc) add(v int) bool {
	svc.msgLock.Lock()
	var seen bool
	if _, seen = svc.seen[v]; !seen {
		svc.seen[v] = struct{}{}
	}
	svc.msgLock.Unlock()
	return !seen
}

func (svc *broadcastSvc) values() []int {
	svc.msgLock.RLock()
	cp := make([]int, 0)
	for v := range svc.seen {
		cp = append(cp, v)
	}
	svc.msgLock.RUnlock()
	return cp
}

func (svc *broadcastSvc) setNeighbors(nodes []string) {
	svc.nbrs = nodes
}

func (svc *broadcastSvc) neighbors() []string {
	return svc.nbrs
}

func workers(n *maelstrom.Node, count int, work chan workItem) {
	acks := make(map[string]struct{})
	var ackLock sync.RWMutex

	log.Printf("Starting %d senders", count)

	for i := 0; i < count; i++ {
		senderId := i
		go func() {
			for {
				item, open := <-work
				if !open {
					return
				}

				ackLock.RLock()
				_, acked := acks[item.mid]
				ackLock.RUnlock()

				if acked {
					ackLock.Lock()
					delete(acks, item.mid)
					ackLock.Unlock()
					continue
				}

				log.Printf("Sender %d: sending %d to node %s", senderId, item.msg.Message, item.dst)
				n.RPC(item.dst, item.msg, func(msg maelstrom.Message) error {
					log.Printf("Sender %d: acknowledged %d from node %s", senderId, item.msg.Message, item.dst)
					ackLock.Lock()
					acks[item.mid] = struct{}{}
					ackLock.Unlock()
					return nil
				})
				work <- item
			}
		}()
	}
}

func main() {
	n := maelstrom.NewNode()
	svc := createBroadcastSvc()

	work := make(chan workItem, 100_000)
	workers(n, 1, work)

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if svc.add(body.Message) {
			log.Printf("Received %d from node %s", body.Message, msg.Src)

			neighbors := svc.neighbors()

			for _, dst := range neighbors {
				if dst == msg.Src {
					continue
				}

				log.Printf("Scheduling message %d to %s", body.Message, dst)
				select {
				case work <- workItem{
					mid: fmt.Sprintf("%s-%d", dst, body.Message),
					dst: dst,
					msg: body,
				}:
				default:
					fmt.Println("Channel full. Discarding message")
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

		svc.setNeighbors(body.Topology[msg.Dest])
		res := make(map[string]any)
		res["type"] = "topology_ok"

		return n.Reply(msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
	close(work)
}
