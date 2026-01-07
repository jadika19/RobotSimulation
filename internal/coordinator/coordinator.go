package coordinator

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/taskpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// ---------- Datenstrukturen ----------

type Robot struct {
	ID       int    `json:"id"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
	Type     string `json:"type"`     // "detector", "cleaner", "repair"
	Status   string `json:"status"`   // "idle", "busy"
	GRPCAddr string `json:"grpcAddr"` // gRPC address for task assignment
	TaskID   string `json:"taskId"`   // Current task ID if busy
}

type Task struct {
	ID        string `json:"id"`
	X         int    `json:"x"`
	Y         int    `json:"y"`
	Type      string `json:"type"`    // "dirt" or "defect"
	RobotID   int    `json:"robotId"` // Assigned robot
	Status    string `json:"status"`  // "pending", "assigned", "completed"
	CreatedAt time.Time
}

type State struct {
	Mu            sync.RWMutex
	NextID        int
	NextTaskID    int
	Robots        map[int]Robot
	Width         int
	Height        int
	KnownProblems map[string]Problem // Only problems reported by detectors
	Tasks         map[string]*Task   // Active tasks
	WorldAddr     string             // World service address for cleanup
}

type Problem struct {
	X    int    `json:"x"`
	Y    int    `json:"y"`
	Type string `json:"type"`
}

var St = &State{
	NextID:        1,
	NextTaskID:    1,
	Robots:        make(map[int]Robot),
	KnownProblems: make(map[string]Problem),
	Tasks:         make(map[string]*Task),
	Width:         20,
	Height:        20,
	WorldAddr:     "http://world:8081",
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
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: text/plain\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func writeJSON(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: application/json\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: %d\r\n\r\n%s",
		code, len(body), body)
}

func writeHTML(c net.Conn, code int, body string) {
	fmt.Fprintf(c, "HTTP/1.1 %d \r\nContent-Type: text/html; charset=utf-8\r\nAccess-Control-Allow-Origin: *\r\nContent-Length: %d\r\n\r\n%s",
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
		TaskID string `json:"taskId,omitempty"`
	}
	robots := make([]robotView, 0, len(st.Robots))
	for _, rb := range st.Robots {
		robots = append(robots, robotView{ID: rb.ID, X: rb.X, Y: rb.Y, Type: rb.Type, Status: rb.Status, TaskID: rb.TaskID})
	}
	type problemView struct {
		X    int    `json:"x"`
		Y    int    `json:"y"`
		Type string `json:"type"`
	}
	problems := make([]problemView, 0, len(st.KnownProblems))
	for _, p := range st.KnownProblems {
		problems = append(problems, problemView{X: p.X, Y: p.Y, Type: p.Type})
	}
	type taskView struct {
		ID      string `json:"id"`
		X       int    `json:"x"`
		Y       int    `json:"y"`
		Type    string `json:"type"`
		RobotID int    `json:"robotId"`
		Status  string `json:"status"`
	}
	tasks := make([]taskView, 0, len(st.Tasks))
	for _, t := range st.Tasks {
		tasks = append(tasks, taskView{ID: t.ID, X: t.X, Y: t.Y, Type: t.Type, RobotID: t.RobotID, Status: t.Status})
	}
	resp := map[string]any{
		"width":    st.Width,
		"height":   st.Height,
		"robots":   robots,
		"problems": problems,
		"tasks":    tasks,
	}
	st.Mu.RUnlock()
	b, _ := json.MarshalIndent(resp, "", "  ")
	writeJSON(c, 200, string(b)+"\n")
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

	// Check if problem already known (avoid duplicates)
	key := coordKey(req.X, req.Y)
	st.Mu.Lock()
	if _, exists := st.KnownProblems[key]; exists {
		st.Mu.Unlock()
		writeText(c, 200, "Problem already known")
		return
	}
	// Add to KnownProblems
	st.KnownProblems[key] = Problem{X: req.X, Y: req.Y, Type: req.Event}
	st.Mu.Unlock()

	// Trigger task assignment in background
	go AssignTask(st, req.X, req.Y, req.Event)

	writeText(c, 200, "Event received")
}

func handleRepairRobotRegistration(c net.Conn, st *State, body string) {
	registerServiceBot(c, st, body, "repair")
}

func handleCleanerRobotRegistration(c net.Conn, st *State, body string) {
	registerServiceBot(c, st, body, "cleaner")
}

func registerServiceBot(c net.Conn, st *State, body string, robotType string) {
	var req struct {
		X        *int   `json:"x"`
		Y        *int   `json:"y"`
		GRPCAddr string `json:"grpcAddr"`
	}
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
	st.Robots[id] = Robot{ID: id, X: x, Y: y, Type: robotType, Status: "idle", GRPCAddr: req.GRPCAddr}
	resp := map[string]any{
		"id":     id,
		"type":   robotType,
		"width":  st.Width,
		"height": st.Height,
		"start":  map[string]int{"x": x, "y": y},
	}
	b, _ := json.Marshal(resp)
	st.Mu.Unlock()
	log.Printf("%s robot registered: id=%d at (%d,%d) grpc=%s", robotType, id, x, y, req.GRPCAddr)
	writeJSON(c, 200, string(b))
}

// ---------- Task Assignment ----------

func AssignTask(st *State, x, y int, problemType string) {
	// Determine which robot type can handle this problem
	var requiredType string
	if problemType == "dirt" {
		requiredType = "cleaner"
	} else if problemType == "defect" {
		requiredType = "repair"
	} else {
		log.Printf("unknown problem type: %s", problemType)
		return
	}

	st.Mu.Lock()
	// Find nearest idle robot of the required type
	var bestRobot *Robot
	bestDistance := -1
	for id, robot := range st.Robots {
		if robot.Type != requiredType || robot.Status != "idle" || robot.GRPCAddr == "" {
			continue
		}
		distance := abs(robot.X-x) + abs(robot.Y-y) // Manhattan distance
		if bestDistance < 0 || distance < bestDistance {
			r := st.Robots[id]
			bestRobot = &r
			bestDistance = distance
		}
	}

	if bestRobot == nil {
		// No idle robot available, create pending task
		taskID := fmt.Sprintf("task-%d", st.NextTaskID)
		st.NextTaskID++
		task := &Task{
			ID:        taskID,
			X:         x,
			Y:         y,
			Type:      problemType,
			RobotID:   -1, // No robot assigned yet
			Status:    "pending",
			CreatedAt: time.Now(),
		}
		st.Tasks[taskID] = task
		st.Mu.Unlock()
		log.Printf("no idle %s robot available for problem at (%d,%d), created pending task %s", requiredType, x, y, taskID)
		return
	}

	// Create task
	taskID := fmt.Sprintf("task-%d", st.NextTaskID)
	st.NextTaskID++
	task := &Task{
		ID:        taskID,
		X:         x,
		Y:         y,
		Type:      problemType,
		RobotID:   bestRobot.ID,
		Status:    "assigned",
		CreatedAt: time.Now(),
	}
	st.Tasks[taskID] = task

	// Mark robot as busy
	robot := st.Robots[bestRobot.ID]
	robot.Status = "busy"
	robot.TaskID = taskID
	st.Robots[bestRobot.ID] = robot
	grpcAddr := robot.GRPCAddr
	robotID := robot.ID
	st.Mu.Unlock()

	log.Printf("gRPC AssignTask -> robot=%d addr=%s task=%s problem=%s target=(%d,%d)", robotID, grpcAddr, taskID, problemType, x, y)

	// Call robot via gRPC
	go callRobotGRPC(grpcAddr, robotID, taskID, x, y, problemType)
}

func callRobotGRPC(addr string, robotID int, taskID string, x, y int, problemType string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("gRPC dial -> TaskService addr=%s robot=%d task=%s", addr, robotID, taskID)

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("gRPC dial failed addr=%s robot=%d task=%s err=%v", addr, robotID, taskID, err)
		return
	}
	defer conn.Close()

	client := taskpb.NewTaskServiceClient(conn)
	resp, err := client.AssignTask(ctx, &taskpb.TaskRequest{
		TaskId:      taskID,
		X:           int32(x),
		Y:           int32(y),
		ProblemType: problemType,
	})
	if err != nil {
		log.Printf("gRPC AssignTask failed addr=%s robot=%d task=%s err=%v", addr, robotID, taskID, err)
		return
	}
	log.Printf("gRPC AssignTask ok addr=%s robot=%d task=%s accepted=%v", addr, robotID, taskID, resp.Accepted)
}

func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// ---------- gRPC Callback Server ----------

type TaskCallbackServer struct {
	taskpb.UnimplementedTaskCallbackServiceServer
	St *State
}

func (s *TaskCallbackServer) ReportCompletion(ctx context.Context, req *taskpb.CompletionRequest) (*taskpb.CompletionResponse, error) {
	log.Printf("gRPC TaskCallback.ReportCompletion robot=%d task=%s success=%v", req.RobotId, req.TaskId, req.Success)

	s.St.Mu.Lock()
	task, ok := s.St.Tasks[req.TaskId]
	if !ok {
		s.St.Mu.Unlock()
		return &taskpb.CompletionResponse{Acknowledged: false}, nil
	}

	// Remove problem from KnownProblems
	key := coordKey(task.X, task.Y)
	delete(s.St.KnownProblems, key)

	// Mark task completed
	task.Status = "completed"

	// Mark robot as idle
	if robot, ok := s.St.Robots[int(req.RobotId)]; ok {
		robot.Status = "idle"
		robot.TaskID = ""
		s.St.Robots[int(req.RobotId)] = robot
	}

	// Clean up completed task
	delete(s.St.Tasks, req.TaskId)
	worldAddr := s.St.WorldAddr
	s.St.Mu.Unlock()

	// Try to assign any pending tasks now that a robot is idle
	go tryAssignPendingTasks(s.St)

	// Delete problem from World service
	go deleteProblemFromWorld(worldAddr, task.X, task.Y)

	return &taskpb.CompletionResponse{Acknowledged: true}, nil
}

// tryAssignPendingTasks attempts to assign any pending tasks to newly available robots
func tryAssignPendingTasks(st *State) {
	st.Mu.Lock()
	defer st.Mu.Unlock()

	for taskID, task := range st.Tasks {
		if task.Status != "pending" {
			continue
		}

		// Determine which robot type can handle this problem
		var requiredType string
		if task.Type == "dirt" {
			requiredType = "cleaner"
		} else if task.Type == "defect" {
			requiredType = "repair"
		} else {
			log.Printf("unknown pending task type: %s", task.Type)
			continue
		}

		var bestRobot *Robot
		var bestDist float64
		for _, robot := range st.Robots {
			if robot.Type != requiredType || robot.Status != "idle" {
				continue
			}
			dist := math.Sqrt(float64((robot.X-task.X)*(robot.X-task.X) + (robot.Y-task.Y)*(robot.Y-task.Y)))
			if bestRobot == nil || dist < bestDist {
				bestRobot = &robot
				bestDist = dist
			}
		}

		if bestRobot == nil {
			continue // Still no robot available for this task
		}

		// Assign the task
		task.RobotID = bestRobot.ID
		task.Status = "assigned"
		st.Tasks[taskID] = task

		// Mark robot as busy
		robot := st.Robots[bestRobot.ID]
		robot.Status = "busy"
		robot.TaskID = taskID
		st.Robots[bestRobot.ID] = robot
		grpcAddr := robot.GRPCAddr
		robotID := robot.ID

		log.Printf("assigned pending task %s to robot %d at (%d,%d)", taskID, robotID, task.X, task.Y)

		// Call robot via gRPC (unlock mutex during gRPC call)
		st.Mu.Unlock()
		go callRobotGRPC(grpcAddr, robotID, taskID, task.X, task.Y, task.Type)
		st.Mu.Lock()
	}
}

func deleteProblemFromWorld(worldAddr string, x, y int) {
	body := fmt.Sprintf(`{"x":%d,"y":%d}`, x, y)
	req, err := http.NewRequest("DELETE", worldAddr+"/problem", strings.NewReader(body))
	if err != nil {
		log.Printf("failed to create delete request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("failed to delete problem from world: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("deleted problem at (%d,%d) from world, status=%d", x, y, resp.StatusCode)
}

func StartGRPCCallbackServer(addr string, st *State) {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("failed to listen on %s: %v", addr, err)
	}
	logger := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		log.Printf("gRPC srv=TaskCallback addr=%s method=%s dur=%s req=%+v resp=%+v err=%v", addr, info.FullMethod, dur, req, resp, err)
		return resp, err
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(logger))
	taskpb.RegisterTaskCallbackServiceServer(s, &TaskCallbackServer{St: st})
	log.Printf("gRPC callback server listening on %s", addr)
	if err := s.Serve(lis); err != nil {
		log.Fatalf("failed to serve gRPC: %v", err)
	}
}
