package coordinator

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// ---------- Datenstrukturen ----------

type Robot struct {
	ID     int    `json:"id"`
	X      int    `json:"x"`
	Y      int    `json:"y"`
	Type   string `json:"type"`   // "detector", "cleaner", "repair"
	Status string `json:"status"` // "idle", "busy"
}

type State struct {
	Mu       sync.RWMutex
	NextID   int
	Robots   map[int]Robot
	Width    int
	Height   int
	Problems map[string]Problem
}

type Problem struct {
	X    int    `json:"x"`
	Y    int    `json:"y"`
	Type string `json:"type"`
}

var St = &State{
	NextID:   1,
	Robots:   make(map[int]Robot),
	Problems: make(map[string]Problem),
	Width:    20,
	Height:   20,
}

func init() {
	log.SetFlags(0)
}

// ---------- Öffentliche Funktionen ----------

func StartUDPListener(addr string, st *State) {
	udpAddr, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		log.Fatal(err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer udpConn.Close()
	log.Println("udp listening on", addr)
	HandleUDPMessages(st, udpConn)
}

// HandleUDPMessages ist public, damit Tests UDP-Nachrichten senden können
func HandleUDPMessages(st *State, udpConn *net.UDPConn) {
	buf := make([]byte, 256)
	for {
		// UDP-Nachricht lesen
		n, _, err := udpConn.ReadFromUDP(buf)
		if err != nil {
			log.Printf("UDP read error: %v", err)
			return
		}
		// Nachricht parsen
		msg := strings.TrimSpace(string(buf[:n])) // expected: "id,x,y"
		parts := strings.Split(msg, ",")
		if len(parts) != 3 {
			continue
		}
		// ID und Koordinaten extrahieren
		id, err1 := strconv.Atoi(parts[0])
		x, err2 := strconv.Atoi(parts[1])
		y, err3 := strconv.Atoi(parts[2])
		if err1 != nil || err2 != nil || err3 != nil {
			continue
		}
		// Koordinaten begrenzen
		x = clamp(x, 0, st.Width-1)
		y = clamp(y, 0, st.Height-1)
		// Roboter-Position aktualisieren
		st.Mu.Lock()
		if rb, ok := st.Robots[id]; ok {
			rb.X = x
			rb.Y = y
			st.Robots[id] = rb
			fmt.Printf("Robot %d moved to (%d, %d)\n", id, x, y)
		}
		st.Mu.Unlock()
	}
}

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
	case method == "GET" && path == "/status":
		handleStatusRequest(c, st)
	case method == "GET" && path == "/map":
		handleMapRequest(c, st)
	case method == "GET" && path == "/live-map":
		handleLiveMapRequest(c)
	case method == "POST" && path == "/problem":
		handleProblemUpsert(c, st, body)
	case method == "POST" && path == "/problem-at":
		handleProblemAt(c, st, body)
	case method == "POST" && path == "/robot":
		handleRobotRegistration(c, st, body)
	case method == "POST" && path == "/event":
		handleEvent(c, st, body)
	case method == "POST" && path == "/repair-robot":
		handleRepairRobotRegistration(c, st, body)
	case method == "POST" && path == "/cleaner-robot":
		handleCleanerRobotRegistration(c, st, body)

	case method == "GET" || method == "POST":
		writeText(c, 404, "Not Found")
	default:
		writeText(c, 405, "Method Not Allowed")
	}
}

// ---------- Private Funktionen ----------

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

func clamp(val, min, max int) int {
	if val < min {
		return min
	} else if val > max {
		return max
	} else {
		return val
	}
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
		log.Printf("Fehler beim Lesen des Bodys: %v", err)
		return ""
	}
	body := string(buf)
	return body
}

func handleStatusRequest(c net.Conn, st *State) {
	st.Mu.RLock()
	type robotView struct {
		ID     int    `json:"ID"`
		X      int    `json:"X"`
		Y      int    `json:"Y"`
		Type   string `json:"type"`
		Status string `json:"status"`
	}
	robots := make([]robotView, 0, len(st.Robots))
	for _, rb := range st.Robots {
		robots = append(robots, robotView{ID: rb.ID, X: rb.X, Y: rb.Y, Type: rb.Type, Status: rb.Status})
	}
	resp := map[string]any{
		"ok":     true,
		"robots": robots,
	}
	st.Mu.RUnlock()
	b, _ := json.MarshalIndent(resp, "", "  ")
	writeJSON(c, 200, string(b)+"\n")
}

func handleMapRequest(c net.Conn, st *State) {
	st.Mu.RLock()
	type robotView struct {
		ID     int    `json:"ID"`
		X      int    `json:"X"`
		Y      int    `json:"Y"`
		Type   string `json:"type"`
		Status string `json:"status"`
	}
	robots := make([]robotView, 0, len(st.Robots))
	for _, rb := range st.Robots {
		robots = append(robots, robotView{ID: rb.ID, X: rb.X, Y: rb.Y, Type: rb.Type, Status: rb.Status})
	}
	type problemView struct {
		X    int    `json:"x"`
		Y    int    `json:"y"`
		Type string `json:"type"`
	}
	problems := make([]problemView, 0, len(st.Problems))
	for _, p := range st.Problems {
		problems = append(problems, problemView(p))
	}
	resp := map[string]any{
		"width":    st.Width,
		"height":   st.Height,
		"robots":   robots,
		"problems": problems,
	}
	st.Mu.RUnlock()
	b, _ := json.MarshalIndent(resp, "", "  ")
	writeJSON(c, 200, string(b)+"\n")
}

func handleLiveMapRequest(c net.Conn) {
	path := filepath.Join("internal", "coordinator", "live_map.html")
	b, err := os.ReadFile(path)
	if err != nil {
		writeText(c, 500, "could not load live_map.html")
		return
	}
	writeHTML(c, 200, string(b))
}

func handleRobotRegistration(c net.Conn, st *State, body string) {
	var req struct{ X, Y *int }
	if strings.TrimSpace(body) != "" {
		_ = json.Unmarshal([]byte(body), &req)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	st.Mu.Lock()
	id := st.NextID
	st.NextID++
	x, y := 0, 0
	if req.X != nil {
		x = *req.X
	} else {
		x = rng.Intn(st.Width)
	}
	if req.Y != nil {
		y = *req.Y
	} else {
		y = rng.Intn(st.Height)
	}
	st.Robots[id] = Robot{ID: id, X: x, Y: y, Type: "detector", Status: "busy"}
	resp := map[string]any{
		"id":     id,
		"type":   "detector",
		"width":  st.Width,
		"height": st.Height,
		"start":  map[string]int{"x": x, "y": y},
	}
	b, _ := json.Marshal(resp)
	st.Mu.Unlock()
	writeJSON(c, 200, string(b))
}

func handleEvent(c net.Conn, st *State, body string) {
	var req struct {
		Event string `json:"event"`
		X     int    `json:"x"`
		Y     int    `json:"y"`
	}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		writeText(c, 400, "invalid json")
		return
	}
	log.Printf("problem reported: %s at (%d,%d)", req.Event, req.X, req.Y)
	writeText(c, 200, "Event received")
}

func handleProblemUpsert(c net.Conn, st *State, body string) {
	var req struct {
		X    int    `json:"x"`
		Y    int    `json:"y"`
		Type string `json:"type"`
	}
	if err := json.Unmarshal([]byte(body), &req); err != nil {
		writeText(c, 400, "invalid json")
		return
	}
	if req.X < 0 || req.X >= st.Width || req.Y < 0 || req.Y >= st.Height {
		writeText(c, 400, "out of bounds")
		return
	}
	typ := strings.ToLower(strings.TrimSpace(req.Type))
	if typ == "clear" || typ == "" {
		st.Mu.Lock()
		delete(st.Problems, coordKey(req.X, req.Y))
		st.Mu.Unlock()
		writeText(c, 200, "cleared")
		return
	}
	if typ != "dirt" && typ != "defect" {
		writeText(c, 400, "invalid type")
		return
	}
	st.Mu.Lock()
	st.Problems[coordKey(req.X, req.Y)] = Problem{X: req.X, Y: req.Y, Type: typ}
	st.Mu.Unlock()
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

func handleRepairRobotRegistration(c net.Conn, st *State, body string) {
	registerServiceBot(c, st, body, "repair")
}

func handleCleanerRobotRegistration(c net.Conn, st *State, body string) {
	registerServiceBot(c, st, body, "cleaner")
}

func registerServiceBot(c net.Conn, st *State, body string, robotType string) {
	var req struct{ X, Y *int }
	if strings.TrimSpace(body) != "" {
		_ = json.Unmarshal([]byte(body), &req)
	}

	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	st.Mu.Lock()
	id := st.NextID
	st.NextID++
	x, y := 0, 0
	if req.X != nil {
		x = *req.X
	} else {
		x = rng.Intn(st.Width)
	}
	if req.Y != nil {
		y = *req.Y
	} else {
		y = rng.Intn(st.Height)
	}
	st.Robots[id] = Robot{ID: id, X: x, Y: y, Type: robotType, Status: "idle"}
	resp := map[string]any{
		"id":     id,
		"type":   robotType,
		"width":  st.Width,
		"height": st.Height,
		"start":  map[string]int{"x": x, "y": y},
	}
	b, _ := json.Marshal(resp)
	st.Mu.Unlock()
	log.Printf("%s robot registered: id=%d at (%d,%d)", robotType, id, x, y)
	writeJSON(c, 200, string(b))
}
