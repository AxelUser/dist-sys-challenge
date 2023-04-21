package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync/atomic"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type addCmd struct {
	Type  string `json:"type"`
	Delta int    `json:"delta"`
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewSeqKV(n)

	reads := int64(0)

	n.Handle("add", func(msg maelstrom.Message) error {
		var body addCmd
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		for {
			cur, err := kv.ReadInt(context.Background(), "c")
			new := 0
			if err != nil {
				log.Println("Empty counter")
				new = body.Delta
			} else {
				new = cur + body.Delta
			}

			if err = kv.CompareAndSwap(context.Background(), "c", cur, new, true); err == nil {
				log.Printf("Added %d", body.Delta)
				break
			}
			log.Printf("Failed to add %d, retrying...", body.Delta)
		}

		return n.Reply(msg, map[string]string{
			"type": "add_ok",
		})
	})

	const STALE_WAIT = 50

	n.Handle("read", func(msg maelstrom.Message) error {
		// trick to converge kv state
		// https://github.com/jepsen-io/maelstrom/issues/39#issuecomment-1445414521
		atomic.AddInt64(&reads, 1)
		kv.Write(context.Background(), fmt.Sprintf("%s-read-%v", n.ID(), reads), reads)

		cur, _ := kv.ReadInt(context.Background(), "c")
		log.Printf("Read %d", cur)
		return n.Reply(msg, map[string]any{
			"type":  "read_ok",
			"value": cur,
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
