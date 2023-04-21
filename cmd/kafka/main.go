package main

import (
	"encoding/json"
	"log"
	"sync"

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
	msgs        map[string][]int
	msgsLock    sync.RWMutex
	commits     map[string]int
	commitsLock sync.RWMutex
}

func createKafka() *kafkaSvc {
	return &kafkaSvc{
		msgs:    map[string][]int{},
		commits: map[string]int{},
	}
}

func (k *kafkaSvc) send(key string, msg int) int {
	var offset int

	k.msgsLock.Lock()
	if msgs, ok := k.msgs[key]; !ok {
		k.msgs[key] = []int{msg}
		offset = 0
	} else {
		k.msgs[key] = append(msgs, msg)
		offset = len(msgs)
	}
	k.msgsLock.Unlock()

	return offset
}

func (k *kafkaSvc) poll(offsets map[string]int) map[string][][]int {
	msgs := make(map[string][][]int)

	k.msgsLock.RLock()
	for key, offset := range offsets {
		if m, ok := k.msgs[key]; !ok {
			msgs[key] = [][]int{}
		} else {
			mo := [][]int{}
			count := len(m)
			for i := offset; i < count; i++ {
				mo = append(mo, []int{i, m[i]})
			}
			msgs[key] = mo
		}
	}
	k.msgsLock.RUnlock()

	return msgs
}

func (k *kafkaSvc) commit(offsets map[string]int) {
	k.commitsLock.Lock()
	for key, offset := range offsets {
		k.commits[key] = offset
	}
	k.commitsLock.Unlock()
}

func (k *kafkaSvc) listCommitted(keys []string) map[string]int {
	offsets := make(map[string]int)

	k.commitsLock.RLock()
	for _, key := range keys {
		offsets[key] = k.commits[key]
	}
	k.commitsLock.RUnlock()

	return offsets
}

func main() {
	n := maelstrom.NewNode()
	kafka := createKafka()

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
