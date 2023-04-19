package main

import (
	"encoding/json"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

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

func send(n *maelstrom.Node, receivers map[string]bool, body broadcastBody, delay time.Duration) {
	exit := make(chan bool, 1)

	count := len(receivers)
	log.Printf("Broadcasting %d to %d receivers", body.Message, count)
	var wg sync.WaitGroup
	wg.Add(count)

	go func() {
		wg.Wait()
		exit <- true
	}()

	var sendLock sync.RWMutex
	broadcast := func() {
		for dst := range receivers {
			sendLock.RLock()
			received := receivers[dst]
			sendLock.RUnlock()
			if received {
				continue
			}

			log.Printf("Sending %d to node %s", body.Message, dst)
			n.RPC(dst, body, func(msg maelstrom.Message) error {
				log.Printf("Acknowledged %d from node %s", body.Message, msg.Src)
				sendLock.Lock()
				receivers[msg.Src] = true
				sendLock.Unlock()
				wg.Done()
				return nil
			})
		}
	}

	go func() {
		broadcast()
		for {
			select {
			case <-exit:
				log.Printf("Broadcasting %d completed", body.Message)
				return
			case <-time.After(delay): // retry after delay
				log.Printf("Retry broadcasting %d after %v", body.Message, delay)
				broadcast()
			}
		}
	}()

}

const RETRY_MILL = 2000

func main() {
	n := maelstrom.NewNode()
	svc := createBroadcastSvc()

	n.Handle("broadcast", func(msg maelstrom.Message) error {
		var body broadcastBody
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		if svc.add(body.Message) {
			log.Printf("Received %d from node %s", body.Message, msg.Src)

			neighbors := svc.neighbors()

			receivers := make(map[string]bool)
			for _, nbr := range neighbors {
				if nbr == msg.Src {
					continue
				}
				receivers[nbr] = false
			}

			send(n, receivers, body, time.Duration(time.Millisecond*RETRY_MILL))
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
}
