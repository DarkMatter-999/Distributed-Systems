package raft

import (
	"fmt"
	"hash/adler32"
	"io"
	"math/rand"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Connection struct {
	ip   string
	port string
}

type Raft struct {
	ip               string
	port             string
	ht               HashTable
	commitlog        CommitLog
	partitions       [][]string
	conns            [][]*Connection
	clusterIndex     int
	serverIndex      int
	currentTerm      int
	votedFor         int
	votes            map[int]bool
	state            string
	leaderID         int
	commitIndex      int
	nextIndices      []int
	matchIndices     []int
	electionPeriodMS int
	rpcPeriodMS      int
	electionTimeout  time.Time
	rpcTimeout       []int
	lock             sync.Mutex
}

func hash(input string) uint32 {
	hasher := adler32.New()
	io.WriteString(hasher, input)
	return hasher.Sum32()
}

func NewRaft(ip, port, partitions string) *Raft {
	inputpartitions := parsePartitions(partitions)

	raft := Raft{
		ip:               ip,
		port:             port,
		ht:               *newHashTable(),
		commitlog:        *newCommitLog(fmt.Sprintf("commit-log-%s-%s.txt", ip, port)),
		partitions:       inputpartitions,
		conns:            make([][]*Connection, len(strings.Fields(partitions))),
		clusterIndex:     -1,
		serverIndex:      -1,
		currentTerm:      1,
		votedFor:         -1,
		votes:            make(map[int]bool),
		state:            "FOLLOWER",
		leaderID:         -1,
		commitIndex:      0,
		electionPeriodMS: rand.Intn(4000) + 1000,
		rpcPeriodMS:      3000,
		electionTimeout:  time.Now(),
	}

	raft.commitlog.Init()

	raft.initConnections()

	fmt.Println("Ready....")
	return &raft
}

func (r *Raft) initConnections() {
	for i := 0; i < len(r.partitions); i++ {
		cluster := r.partitions[i]
		// fmt.Printf("%v", cluster)
		r.conns[i] = make([]*Connection, len(cluster))

		for j := 0; j < len(cluster); j++ {
			host := strings.Split(cluster[j], ":")
			if host[0] == r.ip && host[1] == r.port {
				r.clusterIndex = i
				r.serverIndex = j
			} else {
				r.conns[i][j] = &Connection{
					ip:   host[0],
					port: host[1],
				}
			}
		}
	}

	u := len(r.partitions[r.clusterIndex])

	if len(r.partitions[r.clusterIndex]) > 1 {
		r.state = "FOLLOWER"
	} else {
		r.state = "LEADER"
	}
	r.leaderID = -1
	r.commitIndex = 0
	r.nextIndices = make([]int, u)
	r.matchIndices = make([]int, u)
	r.electionPeriodMS = rand.Intn(4000) + 1000
	r.rpcPeriodMS = 3000
	r.electionTimeout = time.Now()
	r.rpcTimeout = make([]int, u)

	for i := 0; i < u; i++ {
		r.matchIndices[i] = -1
		r.rpcTimeout[i] = -1
	}
}

func parsePartitions(input string) [][]string {
	ips := strings.Split(input, ",")

	partitions := make([][]string, 1)

	partitions[0] = make([]string, len(ips))

	for idx, ip := range ips {
		partitions[0][idx] = ip
	}

	return partitions
}

func (r *Raft) Init() {
	r.setRandomElectionTimeout()

	go r.onElectionTimeout()

	go r.leaderSendAppendEntries()
}

func (r *Raft) setElectionTimeout(timeout time.Time) {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.electionTimeout = timeout
}

func (r *Raft) setRandomElectionTimeout() {
	r.lock.Lock()
	defer r.lock.Unlock()

	r.electionTimeout = time.Now().Add(time.Millisecond * time.Duration(rand.Intn(r.electionPeriodMS)))
}

func (r *Raft) onElectionTimeout() {
	for {
		if time.Now().After(r.electionTimeout) && (r.state == "FOLLOWER" || r.state == "CANDIDATE") {
			fmt.Println("Timeout....")
			r.setRandomElectionTimeout()
			go r.startElection()
		}
		time.Sleep(500 * time.Millisecond)
	}
}

func (r *Raft) startElection() {
	r.lock.Lock()
	defer r.lock.Unlock()

	fmt.Println("Starting election...")

	r.state = "CANDIDATE"
	r.votedFor = r.serverIndex
	r.currentTerm++
	r.votes = make(map[int]bool)
	r.votes[r.serverIndex] = true

	var wg sync.WaitGroup
	for j := 0; j < len(r.partitions[r.clusterIndex]); j++ {
		if j != r.serverIndex {
			wg.Add(1)
			go func(server int) {
				defer wg.Done()
				r.requestVote(server)
			}(j)
		}
	}

	wg.Wait()
}

func (r *Raft) requestVote(server int) {
	lastIndex, lastTerm := r.commitlog.getLastItemTerm()

	fmt.Printf("Requesting vote from %d\n", server)
	for {
		fmt.Printf("Requesting vote from %d\n", server)

		if r.state == "CANDIDATE" && time.Now().Before(r.electionTimeout) {
			host := r.conns[r.clusterIndex][server]
			msg := fmt.Sprintf("VOTE-REQ %d %d %d %d", r.serverIndex, r.currentTerm, lastTerm, lastIndex)
			fmt.Printf("%v %v\n", host, msg)
			resp := sendAndRecvNoRetry(msg, host.ip, host.port, time.Duration(r.rpcPeriodMS))

			if len(resp) > 0 && strings.Compare(resp[:8], "VOTE-REP") == 0 {
				res := strings.Split(resp[8:], " ")
				server, _ := strconv.Atoi(res[1])
				currTerm, _ := strconv.Atoi(res[2])
				votedFor, _ := strconv.Atoi(res[3])

				fmt.Printf("%s\n", resp)
				r.processVoteReply(server, currTerm, votedFor)
				break
			}
		} else {
			break
		}
	}
}

func (r *Raft) processVoteReply(server, term, votedFor int) {
	fmt.Printf("Processing vote reply from server: %d term: %d..\n", server, term)
	if term > r.currentTerm {
		r.stepDown(term)
	}
	if term <= r.currentTerm && r.state == "CANDIDATE" {
		if votedFor == r.serverIndex {
			r.votes[server] = true
		}
		if len(r.votes) > len(r.partitions[r.clusterIndex])/2 {
			r.state = "LEADER"
			r.leaderID = r.serverIndex
			fmt.Printf("%d %d became Leader\n", r.clusterIndex, r.serverIndex)
			fmt.Printf("%v - %d\t", r.votes, r.currentTerm)
		}
	}
}

func (r *Raft) stepDown(term int) {
	fmt.Printf("Stepping down\n")
	r.currentTerm = term
	r.state = "FOLLOWER"
	r.votedFor = -1
	r.setRandomElectionTimeout()
}

func (r *Raft) processVoteRequest(server, term, lastTerm, lastIndex int) string {
	fmt.Printf("Processing vote request from %d %d\n", server, term)
	if term > r.currentTerm {
		r.stepDown(term)
	}
	rlastIndex, rlastTerm := r.commitlog.getLastItemTerm()
	if (term == r.currentTerm) && (r.votedFor == server || r.votedFor == -1) && (lastTerm > rlastTerm || (lastTerm == rlastTerm && lastIndex >= rlastIndex)) {
		r.votedFor = server
		r.state = "FOLLOWER"
		r.setRandomElectionTimeout()
	}
	return fmt.Sprintf("VOTE-REP %d %d %d", r.serverIndex, r.currentTerm, r.votedFor)
}

func (r *Raft) leaderSendAppendEntries() {
	fmt.Println("Sending Append entries from leader")
	for {
		if r.state == "LEADER" {
			r.appendEntries()
			lastIndex, _ := r.commitlog.getLastItemTerm()
			r.commitIndex = lastIndex
		}
	}
}

func (r *Raft) appendEntries() {
	var wg sync.WaitGroup

	for j := 0; j < len(r.partitions[r.clusterIndex]); j++ {
		if j != r.serverIndex {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				r.sendAppendEntriesRequest(j, nil)
			}(j)
		}
	}

	if len(r.partitions[r.clusterIndex]) > 1 {
		var cnts int
		done := make(chan struct{}, len(r.partitions[r.clusterIndex])/2)

		for i := 0; i < len(r.partitions[r.clusterIndex])/2; i++ {
			<-done
			cnts++
			if cnts > (len(r.partitions[r.clusterIndex])/2)-1 {
				return
			}
		}
	} else {
		return
	}
}

func (r *Raft) sendAppendEntriesRequest(server int, res chan string) {
	fmt.Printf("Sending append entries to %d...\n", server)

	prevIdx := r.nextIndices[server] - 1

	logSlice := r.commitlog.ReadLogsStartEnd(prevIdx, nil)

	var prevTerm int
	if prevIdx == -1 {
		prevTerm = 0
	} else {
		if len(logSlice) > 0 {
			prevTerm, _ = strconv.Atoi(logSlice[0][0])
			logSlice = logSlice[1:]
		} else {
			prevTerm = 0
		}
	}

	logSliceStr := ""
	for _, log := range logSlice {
		logSliceStr += fmt.Sprintf("(%s,%s);", log[0], log[1])
	}

	msg := fmt.Sprintf("APPEND-REQ %d %d %d %d [%s] %d", r.serverIndex, r.currentTerm, prevIdx, prevTerm, logSliceStr, r.commitIndex)

	for {
		if r.state == "LEADER" {
			host := r.conns[r.clusterIndex][server]

			resp := sendAndRecvNoRetry(msg, host.ip, host.port, time.Duration(r.rpcPeriodMS))

			if resp != "" {
				appendRep := strings.Fields(resp)

				if len(appendRep) >= 4 && appendRep[0] == "APPEND-REP" {
					server, currTermStr, flagStr, indexStr := appendRep[1], appendRep[2], appendRep[3], appendRep[4]

					serverInt, _ := strconv.Atoi(server)
					currTermInt, _ := strconv.Atoi(currTermStr)
					flagInt, _ := strconv.Atoi(flagStr)
					success := 0
					if flagInt == 1 {
						success = 1
					}
					indexInt, _ := strconv.Atoi(indexStr)

					r.processAppendReply(serverInt, currTermInt, success, indexInt)
					break
				}
			}
		} else {
			break
		}
	}

	if res != nil {
		res <- "ok"
	}
}

func (r *Raft) processAppendRequests(server int, term int, prevIdx int, prevTerm int, logs [][]string, commitIndex int) string {
	fmt.Printf("Processing append request from %d %d...\n", server, term)

	r.setRandomElectionTimeout()

	flag, index := 0, 0

	if term > r.currentTerm {
		r.stepDown(term)
	}

	if term == r.currentTerm {
		r.leaderID = server
		success := false
		selfLogs := make([][]string, 0)
		if prevIdx != -1 {
			selfLogs = r.commitlog.ReadLogsStartEnd(prevIdx, &prevIdx)
		} else {
			success = true
		}

		if len(selfLogs) > 0 {
			init, _ := strconv.Atoi(selfLogs[0][0])

			success = (len(selfLogs) > 0 && init == prevTerm)

			if success {
				lastIndex, lastTerm := r.commitlog.getLastItemTerm()

				last, _ := strconv.Atoi(logs[len(logs)-1][0])
				if len(logs) > 0 && lastTerm == last && lastIndex == r.commitIndex {
					index = r.commitIndex
				} else {
					index = r.storeEntries(prevIdx, logs)
				}
			}
		}
		if success {
			flag = 1
		}

	}

	fmt.Println(fmt.Sprintf("APPEND-REP %d %d %d %d", r.serverIndex, r.currentTerm, flag, index))

	return fmt.Sprintf("APPEND-REP %d %d %d %d", r.serverIndex, r.currentTerm, flag, index)
}

func (r *Raft) processAppendReply(server, term, success, index int) {
	fmt.Printf("Processing append reply from %d %d %d", server, term, success)
	if term > r.currentTerm {
		r.stepDown(term)
	}

	if r.state == "LEADER" && term == r.currentTerm {
		if success > 0 {
			r.nextIndices[server] = index + 1
		} else {
			r.nextIndices[server] = max(0, r.nextIndices[server]-1)
			r.sendAppendEntriesRequest(server, nil)
		}
	}
}

func (r *Raft) storeEntries(prevIdx int, leaderLogs [][]string) int {
	cmd := make([]string, len(leaderLogs))
	for i := 0; i < len(leaderLogs); i++ {
		cmd[i] = leaderLogs[i][1]
	}
	lastIndex, _ := r.commitlog.logReplace(r.currentTerm, cmd, prevIdx+1)
	r.commitIndex = lastIndex
	return lastIndex
}

func (r *Raft) updateStateMachine(cmd string) {
	if cmd[:3] == "SET" {
		cmd := strings.Split(cmd[3:], " ")
		key := cmd[0]
		val := cmd[1]
		req_id, _ := strconv.Atoi(cmd[2])

		r.ht.set(key, val, req_id)
	}
}

func (r *Raft) processRequest(conn net.Conn) {
	defer conn.Close()

	for {
		buffer := make([]byte, 2048)
		_, err := conn.Read(buffer)
		if err != nil {
			// fmt.Println("Error reading from client:", err)
			conn.Close()
			break
		}

		msg := string(buffer)
		msg = strings.TrimSpace(msg)
		if msg != "" {
			// fmt.Printf("%s received\n", msg)
			output := r.handleCommands(msg, conn)
			conn.Write([]byte(output))
		}
	}
}

func (r *Raft) ListenToClients() {
	listenAddr := fmt.Sprintf("0.0.0.0:%s", r.port)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		fmt.Println("Error binding to address:", err)
		os.Exit(1)
	}
	defer listener.Close()

	fmt.Printf("Server listening on %s\n", listenAddr)

	var wg sync.WaitGroup

	for {
		clientConn, err := listener.Accept()
		if err != nil {
			fmt.Println("Error accepting connection:", err)
			continue
		}

		fmt.Printf("Connected to new client at address %s\n", clientConn.RemoteAddr())

		wg.Add(1)
		go func(conn net.Conn) {
			defer wg.Done()
			r.processRequest(conn)
		}(clientConn)
	}

	wg.Wait()
}

func (r *Raft) handleCommands(msg string, conn net.Conn) string {
	var output string

	if strings.HasPrefix(msg, "SET") {
		parts := strings.Fields(msg)
		if len(parts) == 4 {
			key := parts[1]
			value := parts[2]
			reqID, _ := strconv.Atoi(parts[3])

			output = "ko"
			node := hash(key) % uint32(len(r.partitions))

			if uint32(r.clusterIndex) == node {
				for {
					if r.state == "LEADER" {
						lastIndex, err := r.commitlog.log(r.currentTerm, msg)
						if err != 0 {
							break
						}
						for {
							if lastIndex == r.commitIndex {
								break
							}
						}
						r.ht.set(key, value, reqID)
					} else {
						if r.leaderID != -1 && r.leaderID != r.serverIndex {
							output = sendAndRecvNoRetry(msg, r.conns[node][r.leaderID].ip, r.conns[node][r.leaderID].port, time.Duration(r.rpcPeriodMS))
							if len(output) > 0 {
								break
							}
						} else {
							output = "ko"
							break
						}
					}
				}
			} else {
				output = sendAndRecvNoRetry(msg, r.conns[node][r.leaderID].ip, r.conns[node][r.leaderID].port, time.Duration(r.rpcPeriodMS))
				if len(output) == 0 {
					output = "ko"
				}
			}
		} else {
			fmt.Println("Invalid SET command format")
			return "Error: Invalid command"
		}

	} else if strings.HasPrefix(msg, "GET") {
		parts := strings.Fields(msg)
		if len(parts) == 3 {
			key := parts[1]
			node := hash(key) % uint32(len(r.partitions))

			if r.clusterIndex == int(node) {
				for {
					if r.state == "LEADER" {
						val := r.ht.get(key)
						if str, ok := val.(string); ok {
							output = str
						} else {
							output = "Error: Non existent key"
							break
						}
					} else {
						if r.leaderID != -1 && r.leaderID != r.serverIndex {
							output = sendAndRecvNoRetry(msg, r.conns[node][r.leaderID].ip, r.conns[node][r.leaderID].port, time.Duration(r.rpcPeriodMS))
							if len(output) > 0 {
								break
							}
						} else {
							output = "ko"
							break
						}
					}
				}
			} else {
				output = sendAndRecvNoRetry(msg, r.conns[node][r.leaderID].ip, r.conns[node][r.leaderID].port, time.Duration(r.rpcPeriodMS))
				if len(output) == 0 {
					output = "ko"
				}
			}
		} else {
			fmt.Println("Invalid GET command format")
			output = "Error: Invalid command"
		}
	} else if strings.HasPrefix(msg, "VOTE-REQ") {
		parts := strings.Fields(msg)
		fmt.Println(parts, len(parts))
		if len(parts) == 5 {
			server, _ := strconv.Atoi(parts[1])
			currTerm, _ := strconv.Atoi(parts[2])
			lastTerm, _ := strconv.Atoi(parts[3])
			lastIndex, _ := strconv.Atoi(parts[4])

			output = r.processVoteRequest(server, currTerm, lastTerm, lastIndex)
		}
	} else if strings.HasPrefix(msg, "APPEND-REQ") {
		parts := strings.Fields(msg)
		fmt.Println(parts)
		if len(parts) == 7 {
			server, _ := strconv.Atoi(parts[1])
			currTerm, _ := strconv.Atoi(parts[2])
			prevIndex, _ := strconv.Atoi(parts[3])
			prevTerm, _ := strconv.Atoi(parts[4])
			logs := r.parseLogs(parts[5])
			commitIndex, _ := strconv.Atoi(parts[6])
			output = r.processAppendRequests(server, currTerm, prevIndex, prevTerm, logs, commitIndex)
		}
	} else {
		fmt.Println("Invalid command:", msg)
		output = "Error: Invalid command"
	}
	return output
}

func (r *Raft) parseLogs(input string) [][]string {
	input = input[1 : len(input)-1]
	if len(input) > 0 {
		inputsplit := strings.Split(input[1:len(input)-1], ";")
		output := make([][]string, len(inputsplit))

		for i := 0; i < len(inputsplit); i++ {
			keyval := strings.Split(inputsplit[i][1:len(inputsplit[i])], ",")
			output[i] = keyval
		}
		fmt.Println(output)
		return output
	} else {
		return make([][]string, 0)
	}

}
