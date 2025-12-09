package world

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
)

// ---------- Data Structures ----------

type Problem struct {
	X    int    `json:"x"`
	Y    int    `json:"y"`
	Type string `json:"type"`
}

type State struct {
	Mu       sync.RWMutex
	Problems map[string]Problem
	Width    int
	Height   int
}

var St = &State{
	Problems: make(map[string]Problem),
	Width:    20,
	Height:   20,
}

// ---------- Public Functions ----------

func HandleHTTPRequest(c net.Conn, st *State) {
	defer c.Close()
	r := bufio.NewReader(c)

	method, path, headerLines, err := readHTTPHeader(r)
	if err != nil {
		writeText(c, 400, "Bad Request")
		return
	}

	var body string
	if cl, ok := headerLines["content-length"]; ok {
		body = readHTTPBody(r, cl)
	}

	switch {
	case method == "GET" && path == "/world-map":
		handleWorldMapRequest(c, st)
	case method == "GET" && path == "/live-map":
		handleLiveMapRequest(c)
	case method == "POST" && path == "/problem":
		handleProblemUpsert(c, st, body)
	case method == "POST" && path == "/problem-at":
		handleProblemAt(c, st, body)
	case method == "GET" || method == "POST":
		writeText(c, 404, "Not Found")
	default:
		writeText(c, 405, "Method Not Allowed")
	}
}

// ---------- Private Functions ----------

func writeText(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: text/plain\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func writeJSON(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: application/json\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func writeHTML(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: text/html; charset=utf-8\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func coordKey(x, y int) string {
	return fmt.Sprintf("%d,%d", x, y)
}

func readHTTPHeader(r *bufio.Reader) (string, string, map[string]string, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return "", "", nil, err
	}
	line = strings.TrimRight(line, "\r\n")
	parts := strings.Split(line, " ")
	if len(parts) < 3 {
		return "", "", nil, fmt.Errorf("bad request")
	}
	method, path := parts[0], parts[1]

	headerLines := map[string]string{}
	for {
		h, _ := r.ReadString('\n')
		h = strings.TrimRight(h, "\r\n")
		if h == "" {
			break
		}
		if i := strings.Index(h, ":"); i > 0 {
			k := strings.ToLower(strings.TrimSpace(h[:i]))
			v := strings.TrimSpace(h[i+1:])
			headerLines[k] = v
		}
	}
	return method, path, headerLines, nil
}

func readHTTPBody(r *bufio.Reader, contentLength string) string {
	n, _ := strconv.Atoi(contentLength)
	if n <= 0 {
		return ""
	}

	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	if err != nil {
		log.Printf("Error reading body: %v", err)
		return ""
	}
	return string(buf)
}

func handleWorldMapRequest(c net.Conn, st *State) {
	st.Mu.RLock()
	problems := make([]Problem, 0, len(st.Problems))
	for _, p := range st.Problems {
		problems = append(problems, p)
	}
	resp := map[string]any{
		"width":    st.Width,
		"height":   st.Height,
		"problems": problems,
	}
	st.Mu.RUnlock()
	b, _ := json.MarshalIndent(resp, "", "  ")
	writeJSON(c, 200, string(b)+"\n")
}

func handleLiveMapRequest(c net.Conn) {
	path := filepath.Join("internal", "world", "live_map.html")
	b, err := os.ReadFile(path)
	if err != nil {
		writeText(c, 500, "could not load live_map.html")
		return
	}
	writeHTML(c, 200, string(b))
}

func handleProblemUpsert(c net.Conn, st *State, body string) {
	var req Problem
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		writeText(c, 400, "invalid json")
		return
	}
	if req.X < 0 || req.X >= st.Width || req.Y < 0 || req.Y >= st.Height {
		writeText(c, 400, "out of bounds")
		return
	}
	typ := strings.ToLower(strings.TrimSpace(req.Type))
	if typ != "dirt" && typ != "defect" {
		writeText(c, 400, "invalid type")
		return
	}

	st.Mu.Lock()
	var stateProb, found = st.Problems[coordKey(req.X, req.Y)]
	if found && stateProb.Type == typ {
		// Bereits vorhanden, nichts zu tun
		st.Mu.Unlock()
		writeText(c, 200, "already exists")
		return
	} else if found && stateProb.Type != typ {
		st.Mu.Unlock()
		writeText(c, 400, "conflicting problem type at location")
		return
	}
	st.Problems[coordKey(req.X, req.Y)] = Problem{X: req.X, Y: req.Y, Type: typ}
	st.Mu.Unlock()
	log.Printf("problem placed: %s at (%d,%d)", typ, req.X, req.Y)
	writeText(c, 200, "stored")
}

func handleProblemAt(c net.Conn, st *State, body string) {
	var req struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		writeText(c, 400, "invalid json")
		return
	}
	if req.X < 0 || req.X >= st.Width || req.Y < 0 || req.Y >= st.Height {
		writeText(c, 400, "out of bounds")
		return
	}
	st.Mu.RLock()
	pb, ok := st.Problems[coordKey(req.X, req.Y)]
	st.Mu.RUnlock()
	resp := map[string]any{
		"present": ok,
		"type":    "",
	}
	if ok {
		resp["type"] = pb.Type
	}
	b, _ := json.Marshal(resp)
	writeJSON(c, 200, string(b))
}
