package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
)

type Robot struct {
	ID int `json:"id"`
	X  int `json:"x"`
	Y  int `json:"y"`
}

type State struct {
	mu     sync.RWMutex
	nextID int
	robots map[int]Robot
	width  int
	height int
}

var st = &State{
	nextID: 1,
	robots: make(map[int]Robot),
	width:  20, // grid size for now (≥20x20)
	height: 20,
}

func main() {
	addr := ":8080"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("coordinator listening on", addr)

	// UDP listener for robot positions
	go udpListen(":9001")

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go handle(conn)
	}
}

func handle(c net.Conn) {
	defer c.Close()
	r := bufio.NewReader(c)

	// Request line: METHOD PATH HTTP/1.1
	line, err := r.ReadString('\n')
	if err != nil {
		return
	}
	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	if len(parts) < 3 {
		writeText(c, 400, "Bad Request")
		return
	}
	method, path := parts[0], parts[1]

	// Headers
	headers := map[string]string{}
	for {
		h, _ := r.ReadString('\n')
		h = strings.TrimRight(h, "\r\n")
		if h == "" {
			break
		}
		if i := strings.Index(h, ":"); i > 0 {
			k := strings.ToLower(strings.TrimSpace(h[:i]))
			v := strings.TrimSpace(h[i+1:])
			headers[k] = v
		}
	}

	// Body (only if Content-Length)
	var body string
	if cl, ok := headers["content-length"]; ok {
		n, _ := strconv.Atoi(cl)
		if n > 0 {
			buf := make([]byte, n)
			_, _ = r.Read(buf)
			body = string(buf)
		}
	}

	switch {
	case method == "GET" && path == "/status":
		st.mu.RLock()
		count := len(st.robots)
		st.mu.RUnlock()
		writeJSON(c, 200, fmt.Sprintf(`{"ok":true,"robots":%d}`, count))

	case method == "GET" && path == "/map":
		// snapshot state
		st.mu.RLock()
		type robotView struct{ ID, X, Y int }
		robots := make([]robotView, 0, len(st.robots))
		for _, rb := range st.robots {
			robots = append(robots, robotView{ID: rb.ID, X: rb.X, Y: rb.Y})
		}
		resp := map[string]any{
			"width":  st.width,
			"height": st.height,
			"robots": robots,
		}
		st.mu.RUnlock()
		b, _ := json.Marshal(resp)
		writeJSON(c, 200, string(b))

	case method == "POST" && path == "/robot":
		// Optional body: {"x":0,"y":0}
		var req struct{ X, Y *int }
		if strings.TrimSpace(body) != "" {
			_ = json.Unmarshal([]byte(body), &req)
		}

		st.mu.Lock()
		id := st.nextID
		st.nextID++
		x, y := 0, 0
		if req.X != nil {
			x = *req.X
		}
		if req.Y != nil {
			y = *req.Y
		}
		st.robots[id] = Robot{ID: id, X: x, Y: y}
		resp := map[string]any{
			"id":     id,
			"width":  st.width,
			"height": st.height,
			"start":  map[string]int{"x": x, "y": y},
		}
		b, _ := json.Marshal(resp)
		st.mu.Unlock()
		writeJSON(c, 200, string(b))

	case method == "GET" || method == "POST":
		writeText(c, 404, "Not Found")
	default:
		writeText(c, 405, "Method Not Allowed")
	}

}

func writeText(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func writeJSON(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func udpListen(addr string) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("udp listening on", addr)

	buf := make([]byte, 256)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			continue
		}
		msg := strings.TrimSpace(string(buf[:n])) // expected: "id,x,y"
		parts := strings.Split(msg, ",")
		if len(parts) != 3 {
			continue
		}

		id, err1 := strconv.Atoi(parts[0])
		x, err2 := strconv.Atoi(parts[1])
		y, err3 := strconv.Atoi(parts[2])
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}

		st.mu.Lock()
		if rb, ok := st.robots[id]; ok {
			if x < 0 {
				x = 0
			} else if x >= st.width {
				x = st.width - 1
			}
			if y < 0 {
				y = 0
			} else if y >= st.height {
				y = st.height - 1
			}
			rb.X, rb.Y = x, y
			st.robots[id] = rb
		}
		st.mu.Unlock()
	}
}
