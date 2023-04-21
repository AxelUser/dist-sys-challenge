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

maelstrom_serve:
	./third-party/maelstrom/maelstrom serve