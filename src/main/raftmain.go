package main

import (
	"ddn/raft"
	"os"
)

func main() {
	r := raft.NewRaft("127.0.0.1",os.Args[1], "127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002")	
	r.Init()

	r.ListenToClients()
}
