package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"time"

	maelstrom "github.com/jepsen-io/maelstrom/demo/go"
)

type txn struct {
	Op    string
	Key   int
	Value *int
}

func readTxns(raw [][]any) []txn {
	txns := make([]txn, len(raw))
	for i, rawTxn := range raw {
		txns[i].Op = rawTxn[0].(string)
		txns[i].Key = int(rawTxn[1].(float64))
		switch v := rawTxn[2].(type) {
		case float64:
			value := int(v)
			txns[i].Value = &value
		}
	}

	return txns
}

func toBody(txns []txn) [][]any {
	body := make([][]any, len(txns))

	for i, txn := range txns {
		body[i] = []any{txn.Op, txn.Key, txn.Value}
	}

	return body
}

type txnSvc struct {
	n    *maelstrom.Node
	mu   sync.RWMutex
	keys map[int]int
}

func createTxnSvc(n *maelstrom.Node) *txnSvc {
	return &txnSvc{
		n:    n,
		keys: make(map[int]int),
	}
}

func (t *txnSvc) runTxn(txns []txn) ([]txn, error) {
	final := make(map[int]int)

	for _, txn := range txns {
		switch txn.Op {
		case "r":
			t.mu.RLock()
			if v, ok := t.keys[txn.Key]; ok {
				txn.Value = &v
			}
			t.mu.RUnlock()
		case "w":
			final[txn.Key] = *txn.Value
		}
	}

	go func() {
		body := applyReq{
			Type:   "apply",
			Values: final,
		}
		for _, dst := range t.n.NodeIDs() {
			go func(dst string) {
				err := t.sendApply(dst, body, 5)
				if err != nil {
					log.Println(err.Error())
				}
			}(dst)
		}
	}()

	t.mu.Lock()
	for k, v := range final {
		t.keys[k] = v
	}
	t.mu.Unlock()

	return txns, nil
}

func (t *txnSvc) sendApply(dst string, body applyReq, retry int) error {
	for i := 0; i < retry; i++ {
		err := t.applyWithTimeout(dst, body, time.Second)
		if err == nil {
			return nil
		}
		time.Sleep(time.Millisecond)
	}

	return fmt.Errorf("fail apply to %s: %v", dst, body.Values)
}

func (t *txnSvc) applyWithTimeout(dst string, body applyReq, timeout time.Duration) error {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err := t.n.SyncRPC(ctx, dst, body)
	if err != nil {
		return err
	}

	return nil
}

func (t *txnSvc) apply(values map[int]int) {
	t.mu.Lock()
	for k, v := range values {
		t.keys[k] = v
	}
	t.mu.Unlock()
}

type txnReq struct {
	Txn [][]any `json:"txn"`
}

type applyReq struct {
	Type   string      `json:"type"`
	Values map[int]int `json:"values"`
}

const TX_TIMEOUT_MILLS = 2000

func main() {
	n := maelstrom.NewNode()
	t := createTxnSvc(n)

	n.Handle("txn", func(msg maelstrom.Message) error {
		var req txnReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		txns := readTxns(req.Txn)
		txns, err := t.runTxn(txns)
		if err != nil {
			return n.Reply(msg, map[string]any{
				"type": "error",
				"code": maelstrom.TxnConflict,
				"text": err.Error(),
			})
		}

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  toBody(txns),
		})
	})

	n.Handle("apply", func(msg maelstrom.Message) error {
		var req applyReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		t.apply(req.Values)

		return n.Reply(msg, map[string]string{
			"type": "apply_ok",
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
