package raft

import (
	"bufio"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type CommitLog struct {
	file      string
	lock      sync.Mutex
	lastTerm  int
	lastIndex int
}

func newCommitLog(file string) *CommitLog {
	return &CommitLog{
		file:      file,
		lastTerm:  0,
		lastIndex: -1,
	}
}

func (cl *CommitLog) Init() {
	f, err := os.OpenFile(cl.file, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		fmt.Println(err)
	}
	f.Close()
}

func (cl *CommitLog) truncate() {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	file, err := os.OpenFile(cl.file, os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error truncating file:", err)
		return
	}
	defer file.Close()

	cl.lastTerm = 0
	cl.lastIndex = -1
}

func (cl *CommitLog) getLastItemTerm() (int, int) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	return cl.lastIndex, cl.lastTerm
}

func (cl *CommitLog) log(term int, command string) (int, int) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	file, err := os.OpenFile(cl.file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file for append:", err)
		return cl.lastIndex, cl.lastTerm
	}
	defer file.Close()

	now := time.Now().Format("02/01/2006 15:04:05")
	message := fmt.Sprintf("%s,%d,%s\n", now, term, command)
	_, err = file.WriteString(message)
	if err != nil {
		fmt.Println("Error writing to file:", err)
		return cl.lastIndex, cl.lastTerm
	}

	cl.lastTerm = term
	cl.lastIndex++

	return cl.lastIndex, cl.lastTerm
}

func (cl *CommitLog) readLog() []struct {
	term    int
	command string
} {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	var output []struct {
		term    int
		command string
	}

	file, err := os.Open(cl.file)
	if err != nil {
		fmt.Println("Error opening file for read:", err)
		return output
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		parts := strings.Split(scanner.Text(), ",")
		if len(parts) == 3 {
			term, _ := strconv.Atoi(parts[1])
			output = append(output, struct {
				term    int
				command string
			}{
				term:    term,
				command: parts[2],
			})
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return output
	}

	return output
}

func (cl *CommitLog) ReadLogsStartEnd(start int, end *int) [][]string {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	var output [][]string
	index := 0

	file, err := os.Open(cl.file)
	if err != nil {
		fmt.Println("Error opening file:", err)
		return output
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if index >= start {
			parts := strings.Split(line, ",")
			if len(parts) == 3 {
				term := parts[1]
				command := parts[2]
				output = append(output, []string{term, command})
			}
		}

		index++
		if end != nil && index > *end {
			break
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
	}

	return output
}

func (cl *CommitLog) WriteLogFromSock(sock net.Conn) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	file, err := os.OpenFile(cl.file, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file for append:", err)
		return
	}
	defer file.Close()

	sock.Write([]byte("commitlog"))

	BUFFER_SIZE := 4096
	buffer := make([]byte, BUFFER_SIZE)

	progress := 0
	for {
		n, err := sock.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error reading from socket:", err)
			return
		}

		_, err = file.Write(buffer[:n])
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return
		}

		progress += n
		fmt.Printf("\rReceiving %s: %d B", cl.file, progress)
	}

	fmt.Println()
}

func (cl *CommitLog) SendLogToSock(sock net.Conn) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	file, err := os.Open(cl.file)
	if err != nil {
		fmt.Println("Error opening file for read:", err)
		return
	}
	defer file.Close()

	sock.Write([]byte("commitlog"))

	BUFFER_SIZE := 4096
	buffer := make([]byte, BUFFER_SIZE)

	fileInfo, err := file.Stat()
	if err != nil {
		fmt.Println("Error getting file info:", err)
		return
	}
	fileSize := fileInfo.Size()

	progress := int64(0)
	for {
		n, err := file.Read(buffer)
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Error reading from file:", err)
			return
		}

		_, err = sock.Write(buffer[:n])
		if err != nil {
			fmt.Println("Error writing to socket:", err)
			return
		}

		progress += int64(n)
		fmt.Printf("\rSending %s: %d B / %d B", cl.file, progress, fileSize)
	}

	fmt.Println()
}

func (cl *CommitLog) logReplace(term int, commands []string, start int) (int, int) {
	cl.lock.Lock()
	defer cl.lock.Unlock()

	file, err := os.OpenFile(cl.file, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		fmt.Println("Error opening file for read/write:", err)
		return cl.lastIndex, cl.lastTerm
	}
	defer file.Close()

	var logs []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		logs = append(logs, scanner.Text())
	}

	if err := scanner.Err(); err != nil {
		fmt.Println("Error reading file:", err)
		return cl.lastIndex, cl.lastTerm
	}

	for i := start; i < len(commands); i++ {
		if i < len(logs) {
			logs[i] = fmt.Sprintf("%s,%d,%s", time.Now().Format("02/01/2006 15:04:05"), term, commands[i])
		} else {
			logs = append(logs, fmt.Sprintf("%s,%d,%s", time.Now().Format("02/01/2006 15:04:05"), term, commands[i]))
		}
	}

	if err := file.Truncate(0); err != nil {
		fmt.Println("Error truncating file:", err)
		return cl.lastIndex, cl.lastTerm
	}

	_, err = file.Seek(0, 0)
	if err != nil {
		fmt.Println("Error seeking to the beginning of the file:", err)
		return cl.lastIndex, cl.lastTerm
	}

	writer := bufio.NewWriter(file)
	for _, logEntry := range logs {
		_, err := writer.WriteString(logEntry + "\n")
		if err != nil {
			fmt.Println("Error writing to file:", err)
			return cl.lastIndex, cl.lastTerm
		}
	}

	if err := writer.Flush(); err != nil {
		fmt.Println("Error flushing writer:", err)
		return cl.lastIndex, cl.lastTerm
	}

	cl.lastTerm = term
	cl.lastIndex = len(logs) - 1

	return cl.lastIndex, cl.lastTerm
}
