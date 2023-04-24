maelstrom_deps:
	sudo apt-get update
	sudo apt-get install graphviz gnuplot
maelstrom_fetch:
	wget -P ./third-party https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 && bzip2 -d ./third-party/maelstrom.tar.bz2 && tar -xvf ./third-party/maelstrom.tar -C ./third-party && rm ./third-party/maelstrom.tar

build_echo:
	go build -o ./bin/maelstrom-echo ./cmd/echo/main.go
	chmod +x ./bin/maelstrom-echo

run_echo:
	./third-party/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom-echo --node-count 1 --time-limit 10

build_uniqueid:
	go build -o ./bin/maelstrom-unique-id ./cmd/unique-id/main.go
	chmod +x ./bin/maelstrom-unique-id

run_uniqueid:
	./third-party/maelstrom/maelstrom test -w unique-ids --bin ./bin/maelstrom-unique-id --time-limit 30 --rate 1000 --node-count 3 --availability total --nemesis partition

build_broadcast:
	go build -o ./bin/maelstrom-broadcast ./cmd/broadcast/main.go
	chmod +x ./bin/maelstrom-broadcast

build_gcounter:
	go build -o ./bin/maelstrom-gcounter ./cmd/g-counter/main.go
	chmod +x ./bin/maelstrom-gcounter

build_kafka:
	go build -o ./bin/maelstrom-kafka ./cmd/kafka/main.go
	chmod +x ./bin/maelstrom-kafka

build_txn:
	go build -o ./bin/maelstrom-txn ./cmd/txn/main.go
	chmod +x ./bin/maelstrom-txn

run_broadcast_singlenode:
	./third-party/maelstrom/maelstrom test -w broadcast --bin ./bin/maelstrom-broadcast --node-count 1 --time-limit 20 --rate 10

run_broadcast_multinode:
	./third-party/maelstrom/maelstrom test -w broadcast --bin ./bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10

run_broadcast_partition:
	./third-party/maelstrom/maelstrom test -w broadcast --bin ./bin/maelstrom-broadcast --node-count 5 --time-limit 20 --rate 10 --nemesis partition

run_broadcast_efficient:
	./third-party/maelstrom/maelstrom test -w broadcast --bin ./bin/maelstrom-broadcast --node-count 25 --time-limit 20 --rate 100 --latency 100

run_gcounter:
	./third-party/maelstrom/maelstrom test -w g-counter --bin ./bin/maelstrom-gcounter --node-count 3 --rate 100 --time-limit 20 --nemesis partition

run_kafka_single:
	./third-party/maelstrom/maelstrom test -w kafka --bin ./bin/maelstrom-kafka --node-count 1 --concurrency 2n --time-limit 20 --rate 1000

run_kafka_multi:
	./third-party/maelstrom/maelstrom test -w kafka --bin ./bin/maelstrom-kafka --node-count 2 --concurrency 2n --time-limit 20 --rate 1000

run_txn_single:
	./third-party/maelstrom/maelstrom test -w txn-rw-register --bin ./bin/maelstrom-txn --node-count 1 --time-limit 20 --rate 1000 --concurrency 2n --consistency-models read-uncommitted --availability total

run_txn_read_uncommitted:
	./third-party/maelstrom/maelstrom test -w txn-rw-register --bin ./bin/maelstrom-txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-uncommitted --availability total --nemesis partition

run_txn_read_committed:
	./third-party/maelstrom/maelstrom test -w txn-rw-register --bin ./bin/maelstrom-txn --node-count 2 --concurrency 2n --time-limit 20 --rate 1000 --consistency-models read-committed --availability total –-nemesis partition

maelstrom_serve:
	./third-party/maelstrom/maelstrom serve