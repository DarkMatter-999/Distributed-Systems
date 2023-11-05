#/bin/bash

go run raftmain.go 5000 -race &  go run raftmain.go 5001 -race &  go run raftmain.go 5002 -race
