package coordinator

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

// ---------- Datenstrukturen ----------

type Robot struct {
	ID int `json:"id"`
	X  int `json:"x"`
	Y  int `json:"y"`
}

type State struct {
	Mu     sync.RWMutex
	NextID int
	Robots map[int]Robot
	Width  int
	Height int
}

var St = &State{
	NextID: 1,
	Robots: make(map[int]Robot),
	Width:  20,
	Height: 20,
}

// ---------- Öffentliche Funktionen ----------

// HandleHTTP ist public, damit Tests HTTP-Requests simulieren können
func HandleHTTP(c net.Conn, st *State) {
	handle(c, st)
}

// UDPListen ist public, damit Tests UDP-Nachrichten senden können
func UDPListen(addr string, st *State) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	conn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Close()
	log.Println("udp listening on", addr)

	buf := make([]byte, 256)
	for {
		n, _, err := conn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			return
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

		st.Mu.Lock()
		if rb, ok := st.Robots[id]; ok {
			if x < 0 {
				x = 0
			} else if x >= st.Width {
				x = st.Width - 1
			}
			if y < 0 {
				y = 0
			} else if y >= st.Height {
				y = st.Height - 1
			}
			rb.X, rb.Y = x, y
			st.Robots[id] = rb
			fmt.Printf("Robot %d moved to (%d, %d)\n", id, x, y)
		}
		st.Mu.Unlock()
	}
}

// ---------- Private Funktionen ----------

func handle(c net.Conn, st *State) {
	defer c.Close()
	r := bufio.NewReader(c)

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
		st.Mu.RLock()
		type robotView struct{ ID, X, Y int }
		robots := make([]robotView, 0, len(st.Robots))
		for _, rb := range st.Robots {
			robots = append(robots, robotView(rb))
		}
		resp := map[string]any{
			"ok":     true,
			"robots": robots,
		}
		st.Mu.RUnlock()
		b, _ := json.MarshalIndent(resp, "", "  ")
		writeJSON(c, 200, string(b)+"\n")

	case method == "GET" && path == "/map":
		st.Mu.RLock()
		type robotView struct{ ID, X, Y int }
		robots := make([]robotView, 0, len(st.Robots))
		for _, rb := range st.Robots {
			robots = append(robots, robotView(rb))
		}
		resp := map[string]any{
			"width":  st.Width,
			"height": st.Height,
			"robots": robots,
		}
		st.Mu.RUnlock()
		b, _ := json.MarshalIndent(resp, "", "  ")
		writeJSON(c, 200, string(b)+"\n")

	case method == "POST" && path == "/robot":
		var req struct{ X, Y *int }
		if strings.TrimSpace(body) != "" {
			_ = json.Unmarshal([]byte(body), &req)
		}

		st.Mu.Lock()
		id := st.NextID
		st.NextID++
		x, y := 0, 0
		if req.X != nil {
			x = *req.X
		}
		if req.Y != nil {
			y = *req.Y
		}
		st.Robots[id] = Robot{ID: id, X: x, Y: y}
		resp := map[string]any{
			"id":     id,
			"width":  st.Width,
			"height": st.Height,
			"start":  map[string]int{"x": x, "y": y},
		}
		b, _ := json.Marshal(resp)
		st.Mu.Unlock()
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
