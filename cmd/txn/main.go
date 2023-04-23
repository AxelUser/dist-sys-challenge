package main

import (
	"context"
	"encoding/json"
	"log"
	"strconv"

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
	kv *maelstrom.KV
}

func createTxnSvc(kv *maelstrom.KV) *txnSvc {
	return &txnSvc{
		kv: kv,
	}
}

func (t *txnSvc) apply(txns []txn) []txn {
TX_START:
	for i, txn := range txns {
		switch txn.Op {
		case "r":
			val, err := t.kv.ReadInt(context.Background(), strconv.Itoa(txn.Key))
			if err == nil {
				txns[i].Value = &val
			}
		case "w":
			curVal, err := t.kv.ReadInt(context.Background(), strconv.Itoa(txn.Key))
			// if err != nil, create key, else update
			create := err != nil
			if t.kv.CompareAndSwap(context.Background(), strconv.Itoa(txn.Key), curVal, *txn.Value, create) != nil {
				// dirty write, retry
				goto TX_START
			}
		}
	}
	return txns
}

type txnReq struct {
	Txn [][]any `json:"txn"`
}

func main() {
	n := maelstrom.NewNode()
	t := createTxnSvc(maelstrom.NewLinKV(n))

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
