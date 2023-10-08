package raft

import (
	"fmt"
	"net"
	"time"
)

func waitUntilServerStartup(ip string, port string) net.Conn {
    for {
        conn, err := net.Dial("tcp", fmt.Sprintf("%s:%s", ip, port))
        if err == nil {
            return conn
        }
        time.Sleep(1 * time.Second)
    }
}

func sendAndRecvNoRetry(msg string, ip string, port string, timeout time.Duration) string {
    conn := waitUntilServerStartup(ip, port)
    defer conn.Close()

	timeout = timeout * time.Millisecond

    _, err := conn.Write([]byte(msg))
    if err != nil {
        fmt.Println(err)
        return ""
    }

    if timeout > 0 {
        conn.SetReadDeadline(time.Now().Add(timeout))
    }

    buf := make([]byte, 2048)
    n, err := conn.Read(buf)
    if err != nil {
        fmt.Println(err)
        return ""
    }

    return string(buf[:n])
}

func sendAndRecv(msg string, ip string, port string, res chan<- string, timeout time.Duration) {
    for {
        resp := sendAndRecvNoRetry(msg, ip, port, timeout)
        if resp != "" {
            res <- resp
            break
        }
    }
}


