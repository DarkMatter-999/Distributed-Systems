package main

import (
	"fmt"
	"os"
	"time"

	"ddn/mr"
)

func main() {

	if len(os.Args) < 2 {
		fmt.Fprintf(os.Stderr, "Usage: mrmaster /path/to/inputfiles\n")
		os.Exit(1)
	}

	server := mr.StartMaster(os.Args[1:], 10)

	for server.Done() == false {
		time.Sleep(time.Second)
	}
	time.Sleep(1 * time.Second)
}
