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
	CoordGRPCAddr  string // Coordinator gRPC callback address (deprecated, kept for compatibility)
	Mu             sync.Mutex
	CurrentTask    *taskpb.TaskRequest
	GRPCPort       int
	GRPCListenAddr string

	// Election state for decentralized coordination
	Election *ElectionState
}

// New creates a new service bot with the given type
func New(botType string) *ServiceBot {
	return &ServiceBot{
		Type:   botType,
		Status: "idle",
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

	log.Printf("gRPC TaskService.AssignTask bot=%d type=%s addr=%s task=%s target=(%d,%d) problem=%s status=accepted", bot.ID, bot.Type, bot.GRPCListenAddr, req.TaskId, req.X, req.Y, req.ProblemType)

	// Execute task in background
	go bot.executeTask(req)

	return &taskpb.TaskResponse{Accepted: true, Message: "task accepted"}, nil
}

func (bot *ServiceBot) executeTask(task *taskpb.TaskRequest) {
	// MQTT mode execution
	bot.executeTaskMQTT(task)
}

func (bot *ServiceBot) reportCompletion(taskID string, success bool) {
	// Report completion to the current leader via MQTT
	bot.Election.mu.RLock()
	isLeader := bot.Election.IsLeader
	currentLeaderID := bot.Election.CurrentLeaderID
	bot.Election.mu.RUnlock()

	// If I am the leader, handle completion locally
	if isLeader {
		bot.HandleTaskCompletion(taskID, bot.ID, success)
		return
	}

	// Find leader's gRPC address and report
	bot.Election.mu.RLock()
	leaderInfo, exists := bot.Election.KnownBots[currentLeaderID]
	bot.Election.mu.RUnlock()

	if !exists || leaderInfo.GRPCAddr == "" {
		log.Printf("[Bot %d] Cannot report completion: leader %d not found or no gRPC address", bot.ID, currentLeaderID)
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("[Bot %d] Reporting task %s completion to leader %d at %s", bot.ID, taskID, currentLeaderID, leaderInfo.GRPCAddr)

	conn, err := grpc.DialContext(ctx, leaderInfo.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Bot %d] Failed to connect to leader gRPC: %v", bot.ID, err)
		return
	}
	defer conn.Close()

	client := taskpb.NewTaskCallbackServiceClient(conn)
	resp, err := client.ReportCompletion(ctx, &taskpb.CompletionRequest{
		TaskId:  taskID,
		RobotId: int32(bot.ID),
		Success: success,
	})
	if err != nil {
		log.Printf("[Bot %d] Failed to report completion: %v", bot.ID, err)
		return
	}
	log.Printf("[Bot %d] Task %s completion reported, acknowledged=%v", bot.ID, taskID, resp.Acknowledged)
}

// StartGRPCServer starts the gRPC server for receiving tasks and (if leader) task completions
func (bot *ServiceBot) StartGRPCServer(addr string) error {
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	bot.GRPCListenAddr = addr
	logger := func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		start := time.Now()
		resp, err := handler(ctx, req)
		dur := time.Since(start)
		log.Printf("gRPC srv=TaskService bot=%d type=%s addr=%s method=%s dur=%s req=%+v resp=%+v err=%v", bot.ID, bot.Type, bot.GRPCListenAddr, info.FullMethod, dur, req, resp, err)
		return resp, err
	}
	s := grpc.NewServer(grpc.UnaryInterceptor(logger))
	taskpb.RegisterTaskServiceServer(s, bot)
	// Also register callback service so leader can receive completions
	taskpb.RegisterTaskCallbackServiceServer(s, &BotCallbackServer{Bot: bot})
	log.Printf("gRPC server listening on %s", addr)
	return s.Serve(lis)
}

// BotCallbackServer implements TaskCallbackService for receiving completion reports (when leader)
type BotCallbackServer struct {
	taskpb.UnimplementedTaskCallbackServiceServer
	Bot *ServiceBot
}

// ReportCompletion handles task completion reports (called when this bot is leader)
func (s *BotCallbackServer) ReportCompletion(ctx context.Context, req *taskpb.CompletionRequest) (*taskpb.CompletionResponse, error) {
	log.Printf("[Leader] Bot %d: received completion report for task %s from bot %d", s.Bot.ID, req.TaskId, req.RobotId)

	if s.Bot.Election == nil || !s.Bot.Election.IsLeader {
		log.Printf("[Leader] Bot %d: not the leader, ignoring completion report", s.Bot.ID)
		return &taskpb.CompletionResponse{Acknowledged: false}, nil
	}

	s.Bot.HandleTaskCompletion(req.TaskId, int(req.RobotId), req.Success)
	return &taskpb.CompletionResponse{Acknowledged: true}, nil
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

// executeTaskMQTT performs task execution with MQTT position updates
func (bot *ServiceBot) executeTaskMQTT(task *taskpb.TaskRequest) {
	targetX := int(task.X)
	targetY := int(task.Y)

	log.Printf("moving from (%d,%d) to (%d,%d)", bot.X, bot.Y, targetX, targetY)

	// Move to target position step by step (cardinal directions only - no diagonal)
	for bot.X != targetX || bot.Y != targetY {
		if bot.X < targetX {
			bot.X++
		} else if bot.X > targetX {
			bot.X--
		} else if bot.Y < targetY {
			bot.Y++
		} else if bot.Y > targetY {
			bot.Y--
		}
		bot.PublishPosition() // MQTT instead of UDP
		time.Sleep(500 * time.Millisecond)
	}

	log.Printf("arrived at (%d,%d), fixing %s...", bot.X, bot.Y, task.ProblemType)

	// Simulate fixing the problem (2 seconds)
	time.Sleep(2 * time.Second)

	log.Printf("fixed %s at (%d,%d)", task.ProblemType, bot.X, bot.Y)

	// Report completion to coordinator
	bot.reportCompletion(task.TaskId, true)

	bot.Mu.Lock()
	bot.Status = "idle"
	bot.CurrentTask = nil
	bot.Mu.Unlock()

	log.Printf("task complete; staying at (%d,%d) and set to idle", bot.X, bot.Y)
}
