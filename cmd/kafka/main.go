package main

import (
	"encoding/json"
	"log"
	"strconv"
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
	n           *maelstrom.Node
	msgs        map[string][]int
	msgsLock    sync.RWMutex
	commits     map[string]int
	commitsLock sync.RWMutex
}

func createKafka(n *maelstrom.Node) *kafkaSvc {
	return &kafkaSvc{
		n:       n,
		msgs:    make(map[string][]int),
		commits: make(map[string]int),
	}
}

func (k *kafkaSvc) location(key string) string {
	// assume all keys are integers for simplicity
	hash, _ := strconv.Atoi(key)
	nodes := k.n.NodeIDs()
	return nodes[hash%len(nodes)]
}

func (k *kafkaSvc) send(key string, msg int) int {
	var offset int

	master := k.location(key)
	if master == k.n.ID() {
		// append to local state
		k.msgsLock.Lock()
		if msgs, ok := k.msgs[key]; !ok {
			// init key log
			offset = 0
			k.msgs[key] = []int{msg}
		} else {
			// append message to key log
			offset = len(msgs)
			k.msgs[key] = append(msgs, msg)
		}
		k.msgsLock.Unlock()
		log.Printf("Appended %d to key %s with offset %d", msg, key, offset)
	} else {
		// send to other node
		var wg sync.WaitGroup
		wg.Add(1)
		k.n.RPC(master, sendReq{
			Type: "send",
			Key:  key,
			Msg:  msg,
		}, func(msg maelstrom.Message) error {
			var body sendRes
			json.Unmarshal(msg.Body, &body)
			offset = body.Offset
			wg.Done()
			return nil
		})
		wg.Wait()
	}

	return offset
}

func (k *kafkaSvc) poll(offsets map[string]int) map[string][][]int {
	msgsWithOffsets := make(map[string][][]int)

	offsetPerLocation := make(map[string]map[string]int)
	for key, offset := range offsets {
		msgsWithOffsets[key] = make([][]int, 0)
		location := k.location(key)
		if _, ok := offsetPerLocation[location]; !ok {
			offsetPerLocation[location] = map[string]int{key: offset}
		} else {
			offsetPerLocation[location][key] = offset
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(offsetPerLocation))

	for location, keyOffsets := range offsetPerLocation {
		go func(location string, keyOffsets map[string]int) {
			// search in local storage
			if location == k.n.ID() {
				k.msgsLock.RLock()
				for key, offset := range keyOffsets {
					if m, ok := k.msgs[key]; !ok {
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
				k.msgsLock.RUnlock()
				wg.Done()
			} else {
				// search in other nodes
				k.n.RPC(location, pollReq{Type: "poll", Offsets: keyOffsets}, func(msg maelstrom.Message) error {
					var body pollRes
					json.Unmarshal(msg.Body, &body)
					for key, msgs := range body.Msgs {
						msgsWithOffsets[key] = msgs
					}
					wg.Done()
					return nil
				})
			}
		}(location, keyOffsets)
	}

	wg.Wait()

	return msgsWithOffsets
}

func (k *kafkaSvc) commit(offsets map[string]int) {
	offsetPerLocation := make(map[string]map[string]int)
	for key, offset := range offsets {
		location := k.location(key)
		if _, ok := offsetPerLocation[location]; !ok {
			offsetPerLocation[location] = map[string]int{key: offset}
		} else {
			offsetPerLocation[location][key] = offset
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(offsetPerLocation))

	for location, offsetPerKey := range offsetPerLocation {
		go func(location string, offsetPerKey map[string]int) {
			if location == k.n.ID() {
				// commit to local storage
				k.commitsLock.Lock()
				for key, offset := range offsetPerKey {
					k.commits[key] = offset
					log.Printf("Committed offset %d for key %s", offset, key)
				}
				k.commitsLock.Unlock()
				wg.Done()
			} else {
				// commit to other node
				k.n.RPC(location, commitOffsetsReq{Type: "commit_offsets", Offsets: offsetPerKey}, func(msg maelstrom.Message) error {
					wg.Done()
					return nil
				})
			}
		}(location, offsetPerKey)
	}

	wg.Wait()
}

func (k *kafkaSvc) listCommitted(keys []string) map[string]int {
	offsets := make(map[string]int)
	keysPerLocation := make(map[string][]string)
	for _, key := range keys {
		offsets[key] = 0
		location := k.location(key)
		if lk, ok := keysPerLocation[location]; !ok {
			keysPerLocation[location] = []string{key}
		} else {
			keysPerLocation[location] = append(lk, key)
		}
	}

	var wg sync.WaitGroup
	wg.Add(len(keysPerLocation))

	for location, locationKeys := range keysPerLocation {
		go func(location string, locationKeys []string) {
			if location == k.n.ID() {
				// get local storage
				k.commitsLock.RLock()
				for _, key := range locationKeys {
					offsets[key] = k.commits[key]
				}
				k.commitsLock.RUnlock()
				wg.Done()
			} else {
				// get from another node
				k.n.RPC(location, listCommittedOffsetsReq{Type: "list_committed_offsets", Keys: locationKeys}, func(msg maelstrom.Message) error {
					var body listCommittedOffsetsRes
					json.Unmarshal(msg.Body, &body)
					for key, offset := range body.Offsets {
						offsets[key] = offset
					}
					wg.Done()
					return nil
				})
			}
		}(location, locationKeys)
	}

	wg.Wait()

	return offsets
}

func main() {
	n := maelstrom.NewNode()
	kafka := createKafka(n)

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
