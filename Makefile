install_maelstrom:
	sudo apt-get update
	sudo apt-get install graphviz gnuplot
	wget -P ./third-party https://github.com/jepsen-io/maelstrom/releases/download/v0.2.3/maelstrom.tar.bz2 && bzip2 -d ./third-party/maelstrom.tar.bz2 && tar -xvf ./third-party/maelstrom.tar -C ./third-party && rm ./third-party/maelstrom.tar

build_echo:
	go build -o ./bin/maelstrom-echo ./cmd/echo/main.go
	chmod +x ./bin/maelstrom-echo

run_echo:
	./third-party/maelstrom/maelstrom test -w echo --bin ./bin/maelstrom-echo --node-count 1 --time-limit 10