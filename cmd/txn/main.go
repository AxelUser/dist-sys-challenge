package main

import (
	"encoding/json"
	"log"
	"sync"

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
	storage     map[int]int
	storageLock sync.Mutex
}

func createTxnSvc() *txnSvc {
	return &txnSvc{
		storage: make(map[int]int),
	}
}

func (t *txnSvc) apply(txns []txn) []txn {
	t.storageLock.Lock()
	for i, txn := range txns {
		switch txn.Op {
		case "r":
			if val, ok := t.storage[txn.Key]; ok {
				txns[i].Value = &val
			}
		case "w":
			t.storage[txn.Key] = *txn.Value
		}
	}
	t.storageLock.Unlock()
	return txns
}

type txnReq struct {
	Txn [][]any `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()
	t := createTxnSvc()

	n.Handle("txn", func(msg maelstrom.Message) error {
		var req txnReq
		if err := json.Unmarshal(msg.Body, &req); err != nil {
			return err
		}

		txns := readTxns(req.Txn)
		txns = t.apply(txns)

		return n.Reply(msg, map[string]any{
			"type": "txn_ok",
			"txn":  toBody(txns),
		})
	})

	if err := n.Run(); err != nil {
		log.Fatal(err)
	}
}
