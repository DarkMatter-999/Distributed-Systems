package raft

import (
	"fmt"
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

const MinElectionTimeout = 500
const MaxElectionTimeout = 1000
const RPCTimeout = 500

const (
	Follower = iota
	Candidate
	Leader
)

type CommandTerm struct {
	Command interface{}
	Term    int
}

type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entry        []CommandTerm
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
	Xterm   int
	XIndex  int
	XLen    int
}

type Raft struct {
	ip   string
	port string
	ht   HashTable
	//commitlog CommitLog
	lock sync.Mutex

	peers []*Connection

	me           int   // this peer's index into peers[]
	dead         int32 // set by Kill()
	status       int
	currentTerm  int
	votedFor     int
	log          []CommandTerm
	commitIndex  int
	lastApplied  int
	lastLogIndex int
	nextIndex    []int
	matchIndex   []int
	lastAccessed time.Time
	applyMsg     chan ApplyMsg
}

func NewRaft(ip, port, peers string, applyCh chan ApplyMsg) *Raft {
	inputpeers := parsePartitions(peers)

	r := &Raft{}

	r.lock.Lock()
	r.lock.Unlock()

	r.ip = ip
	r.port = port
	r.ht = *newHashTable()
	//r.commitlog = *newCommitLog(fmt.Sprintf("commit-log-%s-%s.txt", ip, port))
	r.peers = make([]*Connection, len(inputpeers))

	for i := 0; i < len(inputpeers); i++ {
		host := strings.Split(inputpeers[i], ":")
		if !(host[0] == r.ip && host[1] == r.port) {
			r.peers[i] = &Connection{
				ip:   host[0],
				port: host[1],
			}
		} else {
			r.me = i
		}
	}

	r.status = Follower
	r.log = []CommandTerm{
		{
			Command: nil,
			Term:    0,
		},
	}
	r.votedFor = -1
	r.nextIndex = make([]int, len(r.peers))
	r.matchIndex = make([]int, len(r.peers))
	r.applyMsg = applyCh

	//r.commitlog.Init()

	fmt.Println("Ready....")

	go r.Init()

	return r
}

func randomTimeout() time.Duration {
	timeout := MinElectionTimeout + rand.Intn(MaxElectionTimeout-MinElectionTimeout)
	return time.Duration(timeout) * time.Millisecond
}

func parsePartitions(input string) []string {
	ips := strings.Split(input, ",")

	peers := make([]string, len(ips))

	for idx, ip := range ips {
		peers[idx] = ip
	}

	return peers
}

func (r *Raft) Init() {
	for {
		r.lock.Lock()
		status := r.status
		r.lock.Unlock()

		if status == Follower {
			r.handleFollower()
		} else if status == Candidate {
			r.handleCandidate()
		} else if status == Leader {
			r.handleLeader()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (r *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	r.lock.Lock()
	defer r.lock.Unlock()
	isLeader = r.status == Leader
	if !isLeader {
		return 0, 0, false
	}

	term = r.currentTerm
	r.log = append(r.log, CommandTerm{
		Command: command,
		Term:    term,
	})
	r.lastLogIndex = len(r.log) - 1
	index = r.lastLogIndex
	//r.persist()
	return index, term, isLeader
}

func (r *Raft) handleFollower() {
	dur := randomTimeout()
	time.Sleep(dur)
	r.lock.Lock()
	lastAccessed := r.lastAccessed
	r.lock.Unlock()

	if time.Now().Sub(lastAccessed).Milliseconds() >= dur.Milliseconds() {
		r.lock.Lock()
		r.status = Candidate
		r.currentTerm++
		r.votedFor = -1
		//rf.persist()
		r.lock.Unlock()
	}
}

func (r *Raft) handleCandidate() {
	dur := randomTimeout()

	start := time.Now()
	r.lock.Lock()
	peers := r.peers
	me := r.me
	term := r.currentTerm
	lastLogIndex := r.lastLogIndex
	lastLogTerm := r.log[lastLogIndex].Term
	r.lock.Unlock()
	count := 0
	total := len(peers)
	finished := 0
	majority := (total / 2) + 1

	for peer := range peers {
		if me == peer {
			r.lock.Lock()
			count++
			finished++
			r.lock.Unlock()
			continue
		}

		go func(peer int) {
			args := RequestVoteArgs{}
			args.Term = term
			args.CandidateId = me
			args.LastLogIndex = lastLogIndex
			args.LastLogTerm = lastLogTerm
			reply := RequestVoteReply{}
			ok := r.requestVote(peer, &args, &reply)
			r.lock.Lock()
			defer r.lock.Unlock()
			if !ok {
				finished++
				return
			}
			if reply.VoteGranted {
				finished++
				count++
			} else {
				finished++
				if args.Term < reply.Term {
					r.status = Follower
					//r.persist()
				}
			}
		}(peer)
	}

	for {
		r.lock.Lock()
		if count >= majority || finished == total || time.Now().Sub(start).Milliseconds() >= dur.Milliseconds() {
			break
		}
		r.lock.Unlock()
		time.Sleep(50 * time.Millisecond)
	}

	if time.Now().Sub(start).Milliseconds() >= dur.Milliseconds() {
		r.status = Follower
		r.lock.Unlock()
		return
	}

	if r.status == Candidate && count >= majority {
		r.status = Leader
		for peer := range peers {
			r.nextIndex[peer] = r.lastLogIndex + 1
		}
	} else {
		r.status = Follower
	}
	//r.persist()
	r.lock.Unlock()
}

func (r *Raft) handleLeader() {
	r.lock.Lock()
	me := r.me
	term := r.currentTerm
	commitIndex := r.commitIndex
	peers := r.peers
	nextIndex := r.nextIndex

	lastLogIndex := r.lastLogIndex
	matchIndex := r.matchIndex
	nextIndex[me] = lastLogIndex + 1
	matchIndex[me] = lastLogIndex
	log := r.log
	r.lock.Unlock()
	for n := commitIndex + 1; n <= lastLogIndex; n++ {
		count := 0
		total := len(peers)
		majority := (total / 2) + 1
		for peer := range peers {
			if matchIndex[peer] >= n && log[n].Term == term {
				count++
			}
		}

		if count >= majority {
			r.lock.Lock()
			i := r.commitIndex + 1
			for ; i <= n; i++ {
				r.applyMsg <- ApplyMsg{
					CommandValid: true,
					Command:      log[i].Command,
					CommandIndex: i,
				}
				r.commitIndex = r.commitIndex + 1
			}
			r.lock.Unlock()
		}
	}

	for peer := range peers {
		if peer == me {
			continue
		}

		args := AppendEntriesArgs{}
		reply := AppendEntriesReply{}
		r.lock.Lock()
		args.Term = r.currentTerm
		prevLogIndex := nextIndex[peer] - 1
		args.PrevLogIndex = prevLogIndex
		args.PrevLogTerm = r.log[prevLogIndex].Term
		args.LeaderCommit = r.commitIndex
		args.LeaderId = r.me
		if nextIndex[peer] <= lastLogIndex {
			args.Entry = r.log[prevLogIndex+1 : lastLogIndex+1]
		}
		r.lock.Unlock()

		go func(peer int) {
			ok := r.AppendEntriesRequest(peer, &args, &reply)
			if !ok {
				return
			}

			r.lock.Lock()
			if reply.Success {
				r.nextIndex[peer] = min(r.nextIndex[peer]+len(args.Entry), r.lastLogIndex+1)
				r.matchIndex[peer] = prevLogIndex + len(args.Entry)
			} else {
				if reply.Term > args.Term {
					r.status = Follower
					r.lock.Unlock()
					return
				}
				if reply.Xterm == -1 {
					r.nextIndex[peer] = reply.XLen
					r.lock.Unlock()
					return
				}
				index := -1
				for i, v := range r.log {
					if v.Term == reply.Xterm {
						index = i
					}
				}
				if index == -1 {
					r.nextIndex[peer] = reply.XIndex
				} else {
					r.nextIndex[peer] = index
				}
			}
			r.lock.Unlock()
		}(peer)
	}
}

func (r *Raft) requestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	fmt.Printf("Requesting vote from %d\n", server)
	host := r.peers[server]
	msg := fmt.Sprintf("VOTE-REQ %d %d %d %d", args.Term, args.CandidateId, args.LastLogTerm, args.LastLogIndex)
	//fmt.Printf("%v %v\n", host, msg)
	resp := sendAndRecvNoRetry(msg, host.ip, host.port, RPCTimeout)

	if len(resp) > 0 && strings.Compare(resp[:8], "VOTE-REP") == 0 {
		res := strings.Split(resp[8:], " ")
		//server, _ := strconv.Atoi(res[1])
		currTerm, _ := strconv.Atoi(res[2])
		vote, _ := strconv.Atoi(res[3])

		//fmt.Printf("%s\n", resp)
		reply.Term = currTerm
		reply.VoteGranted = vote > 0
		return true
	}
	return false
}

func (r *Raft) processVoteRequest(term, server, lastTerm, lastIndex int) string {
	fmt.Printf("Processing vote request from %d %d\n", server, term)

	r.lock.Lock()
	defer r.lock.Unlock()
	if r.currentTerm == term && r.votedFor == server {
		return fmt.Sprintf("VOTE-REP %d %d %d", server, r.currentTerm, 1)
	}
	if r.currentTerm > term ||
		(r.currentTerm == term && r.votedFor != -1) {
		return fmt.Sprintf("VOTE-REP %d %d %d", server, r.currentTerm, 0)
	}
	if term > r.currentTerm {
		r.currentTerm, r.votedFor = term, -1
		r.status = Follower
	}
	if r.lastLogIndex-1 >= 0 {
		lastLogTerm := r.log[r.lastLogIndex-1].Term
		if lastLogTerm > lastTerm ||
			(lastLogTerm == lastTerm && r.lastLogIndex > lastIndex) {
			return fmt.Sprintf("VOTE-REP %d %d %d", server, term, 0)
		}
	}
	r.status = Follower
	r.lastAccessed = time.Now()
	r.votedFor = server

	return fmt.Sprintf("VOTE-REP %d %d %d", server, term, 1)
}

func (r *Raft) AppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	fmt.Printf("Sending append entries to %d...\n", server)

	logSliceStr := ""
	for _, log := range args.Entry {
		logSliceStr += fmt.Sprintf("(%s,%d);", log.Command, log.Term)
	}

	msg := fmt.Sprintf("APPEND-REQ %d %d %d %d [%s] %d", args.Term, args.LeaderId, args.PrevLogIndex, args.PrevLogTerm, logSliceStr, args.LeaderCommit)

	host := r.peers[server]

	resp := sendAndRecvNoRetry(msg, host.ip, host.port, time.Duration(RPCTimeout*time.Millisecond))

	if resp != "" {
		appendRep := strings.Fields(resp)

		if len(appendRep) >= 4 && appendRep[0] == "APPEND-REP" {
			success, term, xlen, xterm, xindex := appendRep[1], appendRep[2], appendRep[3], appendRep[4], appendRep[5]

			successInt, _ := strconv.Atoi(success)
			termInt, _ := strconv.Atoi(term)
			xlenInt, _ := strconv.Atoi(xlen)
			xtermInt, _ := strconv.Atoi(xterm)
			xindexInt, _ := strconv.Atoi(xindex)

			reply.Success = successInt == 1
			reply.Term = termInt
			reply.XLen = xlenInt
			reply.Xterm = xtermInt
			reply.XIndex = xindexInt

			return true
		}
	}
	return false
}

func (r *Raft) processAppendRequests(server int, term int, prevIdx int, prevTerm int, logs []CommandTerm, leaderCommit int) string {
	fmt.Printf("Processing append request from %d %d...\n", server, term)

	r.lock.Lock()
	defer r.lock.Unlock()

	Xterm := -1
	XIndex := -1
	XLen := len(r.log)
	Success := 0
	Term := r.currentTerm
	if term < r.currentTerm {
		return fmt.Sprintf("APPEND-REP %d %d %d %d %d", Success, Term, Xterm, XLen, XIndex)
	}

	if len(r.log) < prevIdx+1 {
		return fmt.Sprintf("APPEND-REP %d %d %d %d %d", Success, Term, Xterm, XLen, XIndex)
	}

	if r.log[prevIdx].Term != prevTerm {
		Xterm = r.log[prevIdx].Term
		for i, v := range r.log {
			if v.Term == Xterm {
				XIndex = i
				break
			}
		}

		return fmt.Sprintf("APPEND-REP %d %d %d %d %d", Success, Term, Xterm, XLen, XIndex)
	}

	index := 0
	for ; index < len(logs); index++ {
		currentIndex := prevIdx + 1 + index
		if currentIndex > len(r.log)-1 {
			break
		}
		if r.log[currentIndex].Term != logs[index].Term {
			r.log = r.log[:currentIndex]
			r.lastLogIndex = len(r.log) - 1
			//rf.persist()
			break
		}
	}

	Success = 1
	r.lastAccessed = time.Now()
	if len(logs) > 0 {
		r.log = append(r.log, logs[index:]...)
		r.lastLogIndex = len(r.log) - 1
		//rf.persist()
	}
	if leaderCommit > r.commitIndex {
		min := min(leaderCommit, r.lastLogIndex)
		for i := r.commitIndex + 1; i <= min; i++ {
			r.commitIndex = i
			r.applyMsg <- ApplyMsg{
				CommandValid: true,
				Command:      r.log[i].Command,
				CommandIndex: i,
			}
		}
	}

	//fmt.Println(fmt.Sprintf("APPEND-REP %d %d %d %d %d", Success, Term, Xterm, XLen, XIndex))

	return fmt.Sprintf("APPEND-REP %d %d %d %d %d", Success, Term, Xterm, XLen, XIndex)
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

		//fmt.Printf("Connected to new client at address %s\n", clientConn.RemoteAddr())

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

			for {
				if r.status == Leader {
					r.ht.set(key, value, reqID)
					output = "ok"
					break
				} else {
					if r.status == Follower {
						output = sendAndRecvNoRetry(msg, r.peers[r.votedFor].ip, r.peers[r.votedFor].port, time.Duration(RPCTimeout*time.Millisecond))
						if len(output) > 0 {
							break
						} else {
							output = "ko"
							break
						}
					}
				}
			}
		} else {
			fmt.Println("Invalid SET command format")
			return "Error: Invalid command"
		}
	} else if strings.HasPrefix(msg, "GET") {
		parts := strings.Fields(msg)
		fmt.Println(parts)
		if len(parts) >= 3 {
			key := parts[1]

			for {
				if r.status == Leader {
					val := r.ht.get(key)
					if str, ok := val.(string); ok {
						output = str
					} else {
						output = "Error: Non-existent key"
						break
					}
				} else {
					if r.status == Follower {
						output = sendAndRecvNoRetry(msg, r.peers[r.votedFor].ip, r.peers[r.votedFor].port, time.Duration(RPCTimeout*time.Millisecond))
						if len(output) > 0 {
							break
						} else {
							output = "ko"
							break
						}
					}
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
			term, _ := strconv.Atoi(parts[1])
			server, _ := strconv.Atoi(parts[2])
			lastTerm, _ := strconv.Atoi(parts[3])
			lastIndex, _ := strconv.Atoi(parts[4])

			output = r.processVoteRequest(term, server, lastTerm, lastIndex)
		}
	} else if strings.HasPrefix(msg, "APPEND-REQ") {
		fmt.Println(msg)
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
	//fmt.Println(r.me, len(r.ht.table))
	//fmt.Println(r.log)
	return output
}

func (r *Raft) parseLogs(input string) []CommandTerm {
	input = input[1 : len(input)-1]
	if len(input) > 0 {
		inputsplit := strings.Split(input[1:len(input)-1], ";")
		output := make([]CommandTerm, len(inputsplit))

		for i := 0; i < len(inputsplit); i++ {
			keyval := strings.Split(inputsplit[i][1:len(inputsplit[i])], ",")
			cmd := keyval[0]
			term, _ := strconv.Atoi(keyval[1])
			output[i] = CommandTerm{
				Command: cmd,
				Term:    term,
			}
		}
		fmt.Println(output)
		return output
	} else {
		return make([]CommandTerm, 0)
	}

}
