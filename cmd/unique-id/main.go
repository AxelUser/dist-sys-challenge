package main

import (
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type uniqueIdSvc struct {
	count uint64
}

func createUniqueIdSvc() *uniqueIdSvc {
	return &uniqueIdSvc{
		count: 0,
	}
}

func (svc *uniqueIdSvc) generate(node string) string {
	return fmt.Sprintf("%s-%d", node, atomic.AddUint64(&svc.count, 1))
}

func main() {
	svc := createUniqueIdSvc()
	n := maelstrom.NewNode()

	n.Handle("generate", func(msg maelstrom.Message) error {
		var body map[string]any
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		body["type"] = "generate_ok"
		body["id"] = svc.generate(n.ID())

		return n.Reply(msg, body)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
