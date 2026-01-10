package servicebots

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"sync"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/taskpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// RegResp is the response from coordinator on registration
type RegResp struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Start  struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"start"`
}

// ServiceBot represents a cleaner or repair robot
type ServiceBot struct {
	taskpb.UnimplementedTaskServiceServer
	ID             int
	Type           string // "cleaner" or "repair"
	Status         string // "idle" or "busy"
	X, Y           int
	Width          int
	Height         int
	mqttClient     *mqtt.Client
	CoordGRPCAddr  string // Coordinator gRPC callback address
	WorldAddr      string // World HTTP address
	Mu             sync.Mutex
	CurrentTask    *taskpb.TaskRequest
	GRPCPort       int
	GRPCListenAddr string

	// --- Leader Election State ---
	LeaderState        string       // "follower", "candidate", "leader"
	LeaderID           int          // ID of current leader (0 if unknown)
	Term               int          // Current election term
	ElectionTimeout    *time.Timer  // Triggers new election
	HeartbeatTicker    *time.Ticker // Leader sends heartbeats
	LastHeartbeat      time.Time    // Last heartbeat received
	ElectionInProgress bool         // Prevents duplicate elections
	VotesReceived      int          // Answers received during election

	// --- Leader Task Management State ---
	KnownBots     map[int]*BotInfo           // All registered bots (from metadata)
	PendingTasks  map[string]*TaskInfo       // Tasks waiting assignment
	AssignedTasks map[string]*TaskInfo       // Tasks currently being executed
	TaskEventLog  []mqtt.TaskAssignmentEvent // Event sourcing log
	TaskMu        sync.RWMutex               // Protects task state
}

// BotInfo tracks information about a bot in the system
type BotInfo struct {
	ID       int
	Type     string
	GRPCAddr string
	X        int
	Y        int
	Status   string
	TaskID   string
	LastSeen time.Time
	Term     int
}

// TaskInfo tracks a task in the system
type TaskInfo struct {
	ID         string
	X          int
	Y          int
	Type       string // "dirt" or "defect"
	RobotID    int    // -1 if pending
	Status     string // "pending", "assigned", "completed", "failed"
	CreatedAt  time.Time
	AssignedAt time.Time
}

// New creates a new service bot with the given type
func New(botType string) *ServiceBot {
	return &ServiceBot{
		Type:          botType,
		Status:        "idle",
		LeaderState:   StateFollower,
		LeaderID:      0,
		Term:          0,
		KnownBots:     make(map[int]*BotInfo),
		PendingTasks:  make(map[string]*TaskInfo),
		AssignedTasks: make(map[string]*TaskInfo),
		TaskEventLog:  make([]mqtt.TaskAssignmentEvent, 0),
	}
}

// Register registers the bot with the coordinator via HTTP
func (bot *ServiceBot) Register(coordAddr string, grpcAddr string) error {
	endpoint := "/" + bot.Type + "-robot"
	body := fmt.Sprintf(`{"grpcAddr":"%s"}`, grpcAddr)
	req, err := http.NewRequest("POST", coordAddr+endpoint, bytes.NewBufferString(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration failed: %s", resp.Status)
	}

	var regResp RegResp
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return err
	}

	bot.ID = regResp.ID
	bot.X = regResp.Start.X
	bot.Y = regResp.Start.Y
	bot.Width = regResp.Width
	bot.Height = regResp.Height
	bot.Status = "idle"

	return nil
}

// Close closes the MQTT connection
func (bot *ServiceBot) Close() {
	if bot.mqttClient != nil {
		// Publish offline status before disconnecting
		bot.PublishStatus("offline")
		bot.mqttClient.Disconnect(1000)
	}
}

// AssignTask is called by the coordinator via gRPC
func (bot *ServiceBot) AssignTask(ctx context.Context, req *taskpb.TaskRequest) (*taskpb.TaskResponse, error) {
	bot.Mu.Lock()
	if bot.Status == "busy" {
		bot.Mu.Unlock()
		return &taskpb.TaskResponse{Accepted: false, Message: "robot is busy"}, nil
	}
	bot.Status = "busy"
	bot.CurrentTask = req
	bot.Mu.Unlock()
	// Publish updated metadata (now busy)
	go bot.PublishMetadata()
	log.Printf("gRPC TaskService.AssignTask bot=%d type=%s addr=%s task=%s target=(%d,%d) problem=%s status=accepted", bot.ID, bot.Type, bot.GRPCListenAddr, req.TaskId, req.X, req.Y, req.ProblemType)

	// Execute task in background
	go bot.executeTask(req)

	return &taskpb.TaskResponse{Accepted: true, Message: "task accepted"}, nil
}

func (bot *ServiceBot) executeTask(task *taskpb.TaskRequest) {
	// MQTT mode execution
	bot.executeTaskMQTT(task)
}

// reportTaskCompletion reports completion to both leader (via callback) and coordinator (for monitoring)
func (bot *ServiceBot) reportTaskCompletion(taskID string, success bool, x, y int) {
	// Notify leader via MQTT event (works for all bots, including leader itself)
	bot.Mu.Lock()
	leaderID := bot.LeaderID
	term := bot.Term
	robotID := bot.ID
	bot.Mu.Unlock()

	if leaderID != 0 {
		// Publish completion event for leader to pick up
		event := mqtt.TaskAssignmentEvent{
			TaskID:    taskID,
			RobotID:   robotID,
			EventType: "completed",
			LeaderID:  leaderID,
			Term:      term,
			Timestamp: time.Now().Format(time.RFC3339),
		}
		if err := bot.mqttClient.PublishTaskEvent(event); err != nil {
			log.Printf("[TASK] ERROR: Failed to publish completion event: %v", err)
		}
	}

	// Also report to coordinator for monitoring (keep old behavior)
	bot.reportCompletion(taskID, success, x, y)
}

func (bot *ServiceBot) reportCompletion(taskID string, success bool, x, y int) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("gRPC TaskCallback.ReportCompletion dialing=%s bot=%d task=%s at (%d,%d)", bot.CoordGRPCAddr, bot.ID, taskID, x, y)

	conn, err := grpc.DialContext(ctx, bot.CoordGRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("failed to connect to coordinator gRPC: %v", err)
		return
	}
	defer conn.Close()

	client := taskpb.NewTaskCallbackServiceClient(conn)
	resp, err := client.ReportCompletion(ctx, &taskpb.CompletionRequest{
		TaskId:  taskID,
		RobotId: int32(bot.ID),
		Success: success,
		X:       int32(x),
		Y:       int32(y),
	})
	if err != nil {
		log.Printf("failed to report completion: %v", err)
		return
	}
	log.Printf("gRPC TaskCallback.ReportCompletion bot=%d task=%s success=%v acknowledged=%v", bot.ID, taskID, success, resp.Acknowledged)
}

// StartGRPCServer starts the gRPC server for receiving tasks
func (bot *ServiceBot) StartGRPCServer(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	// GRPCListenAddr is already set in main.go with the full hostname:port
	// Don't overwrite it with the local listen address
	logger := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		log.Printf("gRPC srv=TaskService bot=%d type=%s addr=%s method=%s dur=%s req=%+v resp=%+v err=%v", bot.ID, bot.Type, bot.GRPCListenAddr, info.FullMethod, dur, req, resp, err)
		return resp, err
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(logger))
	taskpb.RegisterTaskServiceServer(s, bot)
	log.Printf("gRPC server listening on %s", addr)
	return s.Serve(lis)
}

// ---------- MQTT Mode Functions ----------

// ConnectMQTT establishes MQTT connection with Last Will Testament
func (bot *ServiceBot) ConnectMQTT(brokerURL string) error {
	config := mqtt.Config{
		BrokerURL:   brokerURL,
		ClientID:    fmt.Sprintf("servicebot-%d", bot.ID),
		WillEnabled: true,
		WillTopic:   fmt.Sprintf("devices/servicebot/%d/status", bot.ID),
		WillPayload: "offline",
		WillQoS:     1,
		WillRetain:  true,
	}

	client, err := mqtt.NewClient(config)
	if err != nil {
		return fmt.Errorf("mqtt connect: %w", err)
	}

	bot.mqttClient = client
	return nil
}

// PublishPosition publishes current position to MQTT broker
func (bot *ServiceBot) PublishPosition() {
	if bot.mqttClient == nil {
		return
	}

	posMsg := mqtt.PositionMessage{
		ID:        bot.ID,
		X:         bot.X,
		Y:         bot.Y,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if err := bot.mqttClient.PublishPosition("servicebot", posMsg); err != nil {
		log.Printf("Failed to publish position: %v", err)
	}
}

// PublishStatus publishes online/offline status to MQTT broker
func (bot *ServiceBot) PublishStatus(status string) {
	if bot.mqttClient == nil {
		return
	}

	if err := bot.mqttClient.PublishStatus("servicebot", bot.ID, status); err != nil {
		log.Printf("Failed to publish status: %v", err)
	}
}

// PublishMetadata publishes bot metadata as retained message
func (bot *ServiceBot) PublishMetadata() {
	if bot.mqttClient == nil {
		return
	}

	bot.Mu.Lock()
	meta := mqtt.BotMetadata{
		ID:        bot.ID,
		Type:      bot.Type,
		GRPCAddr:  bot.GRPCListenAddr,
		X:         bot.X,
		Y:         bot.Y,
		Status:    bot.Status,
		TaskID:    "",
		Term:      bot.Term,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	if bot.CurrentTask != nil {
		meta.TaskID = bot.CurrentTask.TaskId
	}
	bot.Mu.Unlock()

	if err := bot.mqttClient.PublishBotMetadata(meta); err != nil {
		log.Printf("Failed to publish metadata: %v", err)
	}
}

// executeTaskMQTT performs task execution with MQTT position updates
func (bot *ServiceBot) executeTaskMQTT(task *taskpb.TaskRequest) {
	targetX := int(task.X)
	targetY := int(task.Y)

	log.Printf("moving from (%d,%d) to (%d,%d)", bot.X, bot.Y, targetX, targetY)

	// Move to target position step by step (N4 neighborhood only - one step per iteration)
	for bot.X != targetX || bot.Y != targetY {
		// Prioritize X-axis movement, then Y-axis (one step only per iteration)
		if bot.X < targetX {
			bot.X++
		} else if bot.X > targetX {
			bot.X--
		} else if bot.Y < targetY {
			bot.Y++
		} else if bot.Y > targetY {
			bot.Y--
		}

		bot.PublishPosition()              // MQTT instead of UDP
		time.Sleep(500 * time.Millisecond) // Same speed as detectors
	}

	log.Printf("arrived at (%d,%d), fixing %s...", bot.X, bot.Y, task.ProblemType)

	// Simulate fixing the problem (2 seconds)
	time.Sleep(2 * time.Second)

	log.Printf("fixed %s at (%d,%d)", task.ProblemType, bot.X, bot.Y)

	// Delete problem from world service
	bot.deleteProblemFromWorld(bot.X, bot.Y)

	// Set status to idle BEFORE reporting completion
	// (so leader can assign next pending task to this bot)
	bot.Mu.Lock()
	bot.Status = "idle"
	bot.CurrentTask = nil
	bot.Mu.Unlock()

	// Publish updated metadata (now idle)
	bot.PublishMetadata()

	// Report completion - notify leader and coordinator
	// (must be AFTER setting status to idle to avoid race condition)
	bot.reportTaskCompletion(task.TaskId, true, bot.X, bot.Y)

	log.Printf("task complete; staying at (%d,%d) and set to idle", bot.X, bot.Y)
}

// deleteProblemFromWorld sends HTTP DELETE to world service to remove fixed problem
func (bot *ServiceBot) deleteProblemFromWorld(x, y int) {
	if bot.WorldAddr == "" {
		log.Printf("WorldAddr not set, cannot delete problem from world")
		return
	}

	body := fmt.Sprintf(`{"x":%d,"y":%d}`, x, y)
	req, err := http.NewRequest("DELETE", bot.WorldAddr+"/problem", bytes.NewReader([]byte(body)))
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
