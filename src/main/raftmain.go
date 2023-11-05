package main

import (
	"ddn/raft"
	"os"
	"time"
)

func main() {
	applyMsg := make(chan raft.ApplyMsg)
	r := raft.NewRaft("127.0.0.1",os.Args[1], "127.0.0.1:5000,127.0.0.1:5001,127.0.0.1:5002", applyMsg)	

	go r.Start("SET start true")

	time.Sleep(100 * time.Millisecond)
	r.ListenToClients()
}
