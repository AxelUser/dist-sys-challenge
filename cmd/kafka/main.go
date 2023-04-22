package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type sendReq struct {
	Type string `json:"type"`
	Key  string `json:"key"`
	Msg  int    `json:"msg"`
}

type sendRes struct {
	Type   string `json:"type"`
	Offset int    `json:"offset"`
}

type pollReq struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type pollRes struct {
	Type string             `json:"type"`
	Msgs map[string][][]int `json:"msgs"`
}

type commitOffsetsReq struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type listCommittedOffsetsReq struct {
	Type string   `json:"type"`
	Keys []string `json:"keys"`
}

type listCommittedOffsetsRes struct {
	Type    string         `json:"type"`
	Offsets map[string]int `json:"offsets"`
}

type kafkaSvc struct {
	n  *maelstrom.Node
	kv *maelstrom.KV
}

func createKafka(n *maelstrom.Node, kv *maelstrom.KV) *kafkaSvc {
	return &kafkaSvc{
		n:  n,
		kv: kv,
	}
}

func lock(k *kafkaSvc, key string, wrapped func()) {
	lockKey := fmt.Sprintf("%s-lock", key)
	for {
		if k.kv.CompareAndSwap(context.Background(), lockKey, nil, k.nodeID(), true) == nil {
			log.Printf("Acquired lock on key %s for node %s", key, k.nodeID())
			break
		}
	}

	wrapped()

	k.kv.Write(context.Background(), lockKey, nil)
}

func (k *kafkaSvc) nodeID() string {
	return k.n.ID()
}

func (k *kafkaSvc) send(key string, msg int) int {
	var offset int

	lock(k, "log", func() {

		stored, err := k.kv.Read(context.Background(), "log")

		var parsed map[string][]int

		if err != nil {
			parsed = map[string][]int{}
		} else {
			json.Unmarshal([]byte(stored.(string)), &parsed)
		}

		if _, ok := parsed[key]; !ok {
			// init key log
			offset = 0
			parsed[key] = []int{msg}
		} else {
			// append message to key log
			offset = len(parsed[key])
			parsed[key] = append(parsed[key], msg)
		}

		bytes, _ := json.Marshal(parsed)
		k.kv.Write(context.Background(), "log", string(bytes))
	})

	log.Printf("Appended %d to key %s with offset %d", msg, key, offset)
	return offset
}

func (k *kafkaSvc) poll(offsets map[string]int) map[string][][]int {
	var msgsWithOffsets map[string][][]int

	lock(k, "log", func() {
		stored, err := k.kv.Read(context.Background(), "log")

		var parsed map[string][]int

		if err != nil {
			parsed = map[string][]int{}
		} else {
			json.Unmarshal([]byte(stored.(string)), &parsed)
		}

		msgsWithOffsets = make(map[string][][]int)

		for key, offset := range offsets {
			if m, ok := parsed[key]; !ok {
				msgsWithOffsets[key] = [][]int{}
			} else {
				mo := [][]int{}
				count := len(m)
				for i := offset; i < count; i++ {
					mo = append(mo, []int{i, m[i]})
				}
				msgsWithOffsets[key] = mo
			}
		}
	})

	return msgsWithOffsets
}

func (k *kafkaSvc) commit(offsets map[string]int) {
	lock(k, "commits", func() {
		stored, err := k.kv.Read(context.Background(), "commits")

		var parsed map[string]int

		if err != nil {
			parsed = map[string]int{}
		} else {
			json.Unmarshal([]byte(stored.(string)), &parsed)
		}

		for key, offset := range offsets {
			parsed[key] = offset
		}

		bytes, _ := json.Marshal(parsed)
		k.kv.Write(context.Background(), "commits", string(bytes))
	})
}

func (k *kafkaSvc) listCommitted(keys []string) map[string]int {
	offsets := make(map[string]int)

	lock(k, "commits", func() {
		stored, err := k.kv.Read(context.Background(), "commits")

		var parsed map[string]int

		if err != nil {
			parsed = map[string]int{}
		} else {
			json.Unmarshal([]byte(stored.(string)), &parsed)
		}

		for key, offset := range parsed {
			offsets[key] = offset
		}
	})

	return offsets
}

func main() {
	n := maelstrom.NewNode()
	kv := maelstrom.NewLinKV(n)
	kafka := createKafka(n, kv)

	n.Handle("send", func(msg maelstrom.Message) error {
		var body sendReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := sendRes{
			Type:   "send_ok",
			Offset: kafka.send(body.Key, body.Msg),
		}

		return n.Reply(msg, res)
	})

	n.Handle("poll", func(msg maelstrom.Message) error {
		var body pollReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := pollRes{
			Type: "poll_ok",
			Msgs: kafka.poll(body.Offsets),
		}

		return n.Reply(msg, res)
	})

	n.Handle("commit_offsets", func(msg maelstrom.Message) error {
		var body commitOffsetsReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		kafka.commit(body.Offsets)

		return n.Reply(msg, map[string]string{
			"type": "commit_offsets_ok",
		})
	})

	n.Handle("list_committed_offsets", func(msg maelstrom.Message) error {
		var body listCommittedOffsetsReq
		if err := json.Unmarshal(msg.Body, &body); err != nil {
			return err
		}

		res := listCommittedOffsetsRes{
			Type:    "list_committed_offsets_ok",
			Offsets: kafka.listCommitted(body.Keys),
		}

		return n.Reply(msg, res)
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
