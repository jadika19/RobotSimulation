package servicebots

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/taskpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Election MQTT topics
const (
	TopicElectionRequest  = "servicebots/election/request"
	TopicElectionAnswer   = "servicebots/election/answer"
	TopicElectionAnnounce = "servicebots/election/announce"
	TopicProblems         = "events/problems"
)

// Election timeouts
const (
	ElectionTimeout   = 3 * time.Second
	HeartbeatInterval = 1 * time.Second
	LeaderTimeout     = 4 * time.Second
)

// BotInfo stores information about known service bots
type BotInfo struct {
	ID       int
	Type     string // "cleaner" or "repair"
	X, Y     int
	Status   string // "idle", "busy", "offline"
	GRPCAddr string
	LastSeen time.Time
}

// ElectionMessage is sent when a bot initiates an election
type ElectionMessage struct {
	SenderID int    `json:"senderId"`
	Type     string `json:"type"` // "request", "answer", "announce"
}

// LeaderAnnouncement is sent by the leader as heartbeat
type LeaderAnnouncement struct {
	LeaderID  int    `json:"leaderId"`
	Timestamp string `json:"timestamp"`
}

// BotStatusMessage includes gRPC address for task assignment
type BotStatusMessage struct {
	ID       int    `json:"id"`
	Type     string `json:"type"`
	Status   string `json:"status"`
	GRPCAddr string `json:"grpcAddr"`
	X        int    `json:"x"`
	Y        int    `json:"y"`
}

// TaskInfo tracks active tasks (for leader)
type TaskInfo struct {
	ID         string
	X, Y       int
	Type       string // "dirt" or "defect"
	AssignedTo int
	Status     string // "pending", "assigned", "completed"
	CreatedAt  time.Time
}

// ElectionState holds all election-related state for a bot
type ElectionState struct {
	IsLeader        bool
	CurrentLeaderID int
	LastHeartbeat   time.Time

	// Election process state
	electionInProgress bool
	electionCancel     context.CancelFunc

	// Leader state (only used when IsLeader is true)
	KnownBots    map[int]*BotInfo
	PendingTasks []*TaskInfo
	ActiveTasks  map[string]*TaskInfo
	NextTaskID   int

	// World service address for problem cleanup
	WorldAddr string

	mu sync.RWMutex
}

// Lock acquires write lock (exported for testing)
func (s *ElectionState) Lock() {
	s.mu.Lock()
}

// Unlock releases write lock (exported for testing)
func (s *ElectionState) Unlock() {
	s.mu.Unlock()
}

// RLock acquires read lock (exported for testing)
func (s *ElectionState) RLock() {
	s.mu.RLock()
}

// RUnlock releases read lock (exported for testing)
func (s *ElectionState) RUnlock() {
	s.mu.RUnlock()
}

// NewElectionState creates a new election state
func NewElectionState() *ElectionState {
	return &ElectionState{
		KnownBots:    make(map[int]*BotInfo),
		ActiveTasks:  make(map[string]*TaskInfo),
		PendingTasks: make([]*TaskInfo, 0),
		NextTaskID:   1,
		WorldAddr:    "http://world:8081",
	}
}

// InitializeElection sets up MQTT subscriptions for election and starts monitoring
func (bot *ServiceBot) InitializeElection() error {
	if bot.Election == nil {
		bot.Election = NewElectionState()
	}

	// Subscribe to election topics
	if err := bot.mqttClient.Subscribe(TopicElectionRequest, bot.handleElectionRequest); err != nil {
		return fmt.Errorf("subscribe election request: %w", err)
	}
	if err := bot.mqttClient.Subscribe(TopicElectionAnswer, bot.handleElectionAnswer); err != nil {
		return fmt.Errorf("subscribe election answer: %w", err)
	}
	if err := bot.mqttClient.Subscribe(TopicElectionAnnounce, bot.handleLeaderAnnouncement); err != nil {
		return fmt.Errorf("subscribe election announce: %w", err)
	}

	// Subscribe to all service bot positions and statuses
	if err := bot.mqttClient.Subscribe("devices/servicebot/+/position", bot.handleBotPosition); err != nil {
		return fmt.Errorf("subscribe bot positions: %w", err)
	}
	if err := bot.mqttClient.Subscribe("devices/servicebot/+/status", bot.handleBotStatus); err != nil {
		return fmt.Errorf("subscribe bot statuses: %w", err)
	}

	// Add self to known bots
	bot.Election.mu.Lock()
	bot.Election.KnownBots[bot.ID] = &BotInfo{
		ID:       bot.ID,
		Type:     bot.Type,
		X:        bot.X,
		Y:        bot.Y,
		Status:   bot.Status,
		GRPCAddr: bot.GRPCAdvertise,
		LastSeen: time.Now(),
	}
	bot.Election.mu.Unlock()

	log.Printf("[Election] Bot %d initialized election system", bot.ID)
	return nil
}

// StartElection initiates a Bully algorithm election
func (bot *ServiceBot) StartElection() {
	bot.Election.mu.Lock()
	if bot.Election.electionInProgress {
		bot.Election.mu.Unlock()
		log.Printf("[Election] Bot %d: election already in progress", bot.ID)
		return
	}
	bot.Election.electionInProgress = true
	ctx, cancel := context.WithCancel(context.Background())
	bot.Election.electionCancel = cancel
	bot.Election.mu.Unlock()

	log.Printf("[Election] Bot %d starting election", bot.ID)

	// Send election request to all bots with higher IDs
	msg := ElectionMessage{
		SenderID: bot.ID,
		Type:     "request",
	}
	if err := bot.mqttClient.Publish(TopicElectionRequest, msg); err != nil {
		log.Printf("[Election] Bot %d failed to publish election request: %v", bot.ID, err)
	}

	// Wait for answers from higher-ID bots
	select {
	case <-ctx.Done():
		// Election was cancelled (received answer from higher ID)
		log.Printf("[Election] Bot %d: election cancelled, waiting for leader announcement", bot.ID)
		bot.Election.mu.Lock()
		bot.Election.electionInProgress = false
		bot.Election.mu.Unlock()
		return
	case <-time.After(ElectionTimeout):
		// No answer received - I am the leader!
		bot.Election.mu.Lock()
		bot.Election.electionInProgress = false
		bot.Election.mu.Unlock()
		bot.becomeLeader()
	}
}

// becomeLeader transitions this bot to leader state
func (bot *ServiceBot) becomeLeader() {
	bot.Election.mu.Lock()
	bot.Election.IsLeader = true
	bot.Election.CurrentLeaderID = bot.ID
	bot.Election.mu.Unlock()

	log.Printf("[Election] Bot %d: I AM THE LEADER NOW!", bot.ID)

	// Announce leadership
	bot.announceLeadership()

	// Subscribe to problem events (only leader handles this)
	if err := bot.mqttClient.Subscribe(TopicProblems, bot.handleProblemEvent); err != nil {
		log.Printf("[Election] Bot %d failed to subscribe to problems: %v", bot.ID, err)
	}

	// Start heartbeat goroutine
	go bot.leaderHeartbeat()

	// Periodically retry pending tasks while leader
	go bot.pendingRetryLoop()

	// Try to assign any pending tasks
	go bot.tryAssignPendingTasks()
}

// stepDownAsLeader transitions this bot from leader to follower
func (bot *ServiceBot) stepDownAsLeader() {
	bot.Election.mu.Lock()
	wasLeader := bot.Election.IsLeader
	bot.Election.IsLeader = false
	bot.Election.mu.Unlock()

	if wasLeader {
		log.Printf("[Election] Bot %d: stepping down as leader", bot.ID)
		// Unsubscribe from problem events
		bot.mqttClient.Unsubscribe(TopicProblems)
	}
}

// announceLeadership broadcasts leader announcement
func (bot *ServiceBot) announceLeadership() {
	announcement := LeaderAnnouncement{
		LeaderID:  bot.ID,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	if err := bot.mqttClient.Publish(TopicElectionAnnounce, announcement); err != nil {
		log.Printf("[Election] Bot %d failed to announce leadership: %v", bot.ID, err)
	}
}

// leaderHeartbeat sends periodic heartbeats while this bot is leader
func (bot *ServiceBot) leaderHeartbeat() {
	ticker := time.NewTicker(HeartbeatInterval)
	defer ticker.Stop()

	for {
		bot.Election.mu.RLock()
		isLeader := bot.Election.IsLeader
		bot.Election.mu.RUnlock()

		if !isLeader {
			log.Printf("[Election] Bot %d: no longer leader, stopping heartbeat", bot.ID)
			return
		}

		bot.announceLeadership()
		<-ticker.C
	}
}

// MonitorLeader monitors the leader and starts election if leader times out
func (bot *ServiceBot) MonitorLeader() {
	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	for {
		<-ticker.C

		bot.Election.mu.RLock()
		isLeader := bot.Election.IsLeader
		lastHeartbeat := bot.Election.LastHeartbeat
		currentLeaderID := bot.Election.CurrentLeaderID
		bot.Election.mu.RUnlock()

		if isLeader {
			continue // I am the leader, no need to monitor
		}

		// Check for leader timeout
		if time.Since(lastHeartbeat) > LeaderTimeout && currentLeaderID != 0 {
			log.Printf("[Election] Bot %d: leader %d timeout detected! Starting election...", bot.ID, currentLeaderID)
			bot.Election.mu.Lock()
			bot.Election.CurrentLeaderID = 0
			bot.Election.mu.Unlock()
			go bot.StartElection()
		}
	}
}

// handleElectionRequest handles incoming election requests (Bully algorithm)
func (bot *ServiceBot) handleElectionRequest(topic string, payload []byte) {
	var msg ElectionMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		log.Printf("[Election] Bot %d: failed to unmarshal election request: %v", bot.ID, err)
		return
	}

	// Ignore our own messages
	if msg.SenderID == bot.ID {
		return
	}

	log.Printf("[Election] Bot %d: received election request from bot %d", bot.ID, msg.SenderID)

	// If sender has lower ID, send answer and start our own election
	if msg.SenderID < bot.ID {
		log.Printf("[Election] Bot %d: I have higher ID than %d, sending answer", bot.ID, msg.SenderID)
		answer := ElectionMessage{
			SenderID: bot.ID,
			Type:     "answer",
		}
		if err := bot.mqttClient.Publish(TopicElectionAnswer, answer); err != nil {
			log.Printf("[Election] Bot %d: failed to send answer: %v", bot.ID, err)
		}

		// Start our own election (we might be the new leader)
		go bot.StartElection()
	}
}

// handleElectionAnswer handles incoming election answers (Bully algorithm)
func (bot *ServiceBot) handleElectionAnswer(topic string, payload []byte) {
	var msg ElectionMessage
	if err := json.Unmarshal(payload, &msg); err != nil {
		return
	}

	// Ignore our own messages
	if msg.SenderID == bot.ID {
		return
	}

	// Only care about answers from higher IDs
	if msg.SenderID > bot.ID {
		log.Printf("[Election] Bot %d: received answer from higher-ID bot %d, cancelling my election", bot.ID, msg.SenderID)
		bot.Election.mu.Lock()
		if bot.Election.electionCancel != nil {
			bot.Election.electionCancel()
		}
		bot.Election.mu.Unlock()
	}
}

// handleLeaderAnnouncement handles leader heartbeats/announcements
func (bot *ServiceBot) handleLeaderAnnouncement(topic string, payload []byte) {
	var announcement LeaderAnnouncement
	if err := json.Unmarshal(payload, &announcement); err != nil {
		return
	}

	bot.Election.mu.Lock()
	previousLeader := bot.Election.CurrentLeaderID
	bot.Election.CurrentLeaderID = announcement.LeaderID
	bot.Election.LastHeartbeat = time.Now()

	// If a new leader is announced and it's not me, step down
	if bot.Election.IsLeader && announcement.LeaderID != bot.ID {
		// Someone with higher ID became leader
		if announcement.LeaderID > bot.ID {
			bot.Election.IsLeader = false
			log.Printf("[Election] Bot %d: stepping down, new leader is %d", bot.ID, announcement.LeaderID)
		}
	}
	bot.Election.mu.Unlock()

	if previousLeader != announcement.LeaderID {
		log.Printf("[Election] Bot %d: leader is now bot %d", bot.ID, announcement.LeaderID)
	}
}

// handleBotPosition updates known bot positions
func (bot *ServiceBot) handleBotPosition(topic string, payload []byte) {
	var pos mqtt.PositionMessage
	if err := json.Unmarshal(payload, &pos); err != nil {
		return
	}

	bot.Election.mu.Lock()
	defer bot.Election.mu.Unlock()

	info, exists := bot.Election.KnownBots[pos.ID]
	if !exists {
		// Discover new bot from position if status message has not arrived yet
		info = &BotInfo{ID: pos.ID, Status: "idle", LastSeen: time.Now()}
		bot.Election.KnownBots[pos.ID] = info
	}
	info.X = pos.X
	info.Y = pos.Y
	info.LastSeen = time.Now()
}

// handleBotStatus updates known bot statuses
func (bot *ServiceBot) handleBotStatus(topic string, payload []byte) {
	// Extract bot ID from topic: devices/servicebot/{id}/status
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		return
	}
	botID, err := strconv.Atoi(parts[2])
	if err != nil {
		return
	}

	status := strings.TrimSpace(string(payload))

	bot.Election.mu.Lock()
	defer bot.Election.mu.Unlock()

	if status == "offline" {
		// Bot went offline
		if info, exists := bot.Election.KnownBots[botID]; exists {
			info.Status = "offline"
			log.Printf("[Election] Bot %d: bot %d went offline", bot.ID, botID)
		}

		// If the leader went offline, start election
		if botID == bot.Election.CurrentLeaderID && !bot.Election.IsLeader {
			log.Printf("[Election] Bot %d: leader %d went offline! Starting election...", bot.ID, botID)
			bot.Election.CurrentLeaderID = 0
			go bot.StartElection()
		}
		return
	}

	// Try to parse as BotStatusMessage (JSON)
	var statusMsg BotStatusMessage
	if err := json.Unmarshal(payload, &statusMsg); err != nil {
		// Simple status string (online/idle/busy)
		if info, exists := bot.Election.KnownBots[botID]; exists {
			if status == "online" {
				info.Status = "idle"
			} else {
				info.Status = status
			}
			info.LastSeen = time.Now()
		}
		return
	}

	// Full status message with gRPC address
	if info, exists := bot.Election.KnownBots[statusMsg.ID]; exists {
		info.Status = statusMsg.Status
		info.GRPCAddr = statusMsg.GRPCAddr
		info.X = statusMsg.X
		info.Y = statusMsg.Y
		info.LastSeen = time.Now()
	} else {
		// New bot discovered
		bot.Election.KnownBots[statusMsg.ID] = &BotInfo{
			ID:       statusMsg.ID,
			Type:     statusMsg.Type,
			X:        statusMsg.X,
			Y:        statusMsg.Y,
			Status:   statusMsg.Status,
			GRPCAddr: statusMsg.GRPCAddr,
			LastSeen: time.Now(),
		}
		log.Printf("[Election] Bot %d: discovered new bot %d (type=%s)", bot.ID, statusMsg.ID, statusMsg.Type)
	}
}

// handleProblemEvent handles problem events (only called when leader)
func (bot *ServiceBot) handleProblemEvent(topic string, payload []byte) {
	bot.Election.mu.RLock()
	isLeader := bot.Election.IsLeader
	bot.Election.mu.RUnlock()

	if !isLeader {
		return // Only leader handles problems
	}

	var event mqtt.EventMessage
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("[Leader] Bot %d: failed to unmarshal problem event: %v", bot.ID, err)
		return
	}

	log.Printf("[Leader] Bot %d: received problem %s at (%d,%d) from detector %d",
		bot.ID, event.Type, event.X, event.Y, event.DetectorID)

	// Assign task
	go bot.assignTask(event.X, event.Y, event.Type)
}

// assignTask assigns a task to the best available bot (leader function)
func (bot *ServiceBot) assignTask(x, y int, problemType string) {

	// log the problem type
	log.Printf("[DIANA] Bot %d: assigning task for problem type: %s", bot.ID, problemType)

	// Determine required robot type
	var requiredType string
	if problemType == "dirt" {
		requiredType = "cleaner"
	} else if problemType == "defect" {
		requiredType = "repair"
	} else {
		log.Printf("[Leader] Bot %d: unknown problem type: %s", bot.ID, problemType)
		return
	}

	// log the requiredType
	log.Printf("[DIANA] Bot %d: required robot type for problem type %s is %s", bot.ID, problemType, requiredType)

	bot.Election.mu.Lock()

	// Find nearest idle robot of the required type
	var bestBot *BotInfo
	bestDistance := float64(-1)

	for _, info := range bot.Election.KnownBots {
		if info.Type != requiredType {
			continue
		}
		if info.Status != "idle" {
			continue
		}
		if info.GRPCAddr == "" {
			continue
		}

		distance := math.Sqrt(float64((info.X-x)*(info.X-x) + (info.Y-y)*(info.Y-y)))
		if bestDistance < 0 || distance < bestDistance {
			bestBot = info
			bestDistance = distance
		}
	}

	// log bestBot found and if not found log that as well
	if bestBot != nil {
		log.Printf("[DIANA] Bot %d: best bot found for problem type %s is bot %d at (%d,%d)", bot.ID, problemType, bestBot.ID, bestBot.X, bestBot.Y)
	} else {
		log.Printf("[DIANA] Bot %d: no suitable bot found for problem type %s", bot.ID, problemType)
	}

	if bestBot == nil {
		// No idle robot available, create pending task
		taskID := fmt.Sprintf("task-%d", bot.Election.NextTaskID)
		bot.Election.NextTaskID++
		task := &TaskInfo{
			ID:         taskID,
			X:          x,
			Y:          y,
			Type:       problemType,
			AssignedTo: -1,
			Status:     "pending",
			CreatedAt:  time.Now(),
		}
		bot.Election.PendingTasks = append(bot.Election.PendingTasks, task)
		bot.Election.mu.Unlock()
		log.Printf("[Leader] Bot %d: no idle %s robot for problem at (%d,%d), queued as %s",
			bot.ID, requiredType, x, y, taskID)
		return
	}

	// Create and assign task
	taskID := fmt.Sprintf("task-%d", bot.Election.NextTaskID)
	bot.Election.NextTaskID++
	task := &TaskInfo{
		ID:         taskID,
		X:          x,
		Y:          y,
		Type:       problemType,
		AssignedTo: bestBot.ID,
		Status:     "assigned",
		CreatedAt:  time.Now(),
	}
	bot.Election.ActiveTasks[taskID] = task

	// Mark bot as busy
	bestBot.Status = "busy"
	grpcAddr := bestBot.GRPCAddr
	targetBotID := bestBot.ID

	bot.Election.mu.Unlock()

	log.Printf("[Leader] Bot %d: assigning task %s to bot %d for %s at (%d,%d)",
		bot.ID, taskID, targetBotID, problemType, x, y)

	// Call robot via gRPC
	go bot.callRobotGRPC(grpcAddr, targetBotID, taskID, x, y, problemType)
}

// callRobotGRPC sends task assignment via gRPC
func (bot *ServiceBot) callRobotGRPC(addr string, robotID int, taskID string, x, y int, problemType string) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	log.Printf("[Leader] Bot %d: gRPC dial -> bot %d at %s for task %s", bot.ID, robotID, addr, taskID)

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Printf("[Leader] Bot %d: gRPC dial failed addr=%s: %v", bot.ID, addr, err)
		// Mark task as pending for retry
		bot.markTaskPending(taskID)
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
		log.Printf("[Leader] Bot %d: gRPC AssignTask failed: %v", bot.ID, err)
		bot.markTaskPending(taskID)
		return
	}

	if !resp.Accepted {
		log.Printf("[Leader] Bot %d: task %s rejected by bot %d: %s", bot.ID, taskID, robotID, resp.Message)
		bot.markTaskPending(taskID)
		return
	}

	log.Printf("[Leader] Bot %d: task %s accepted by bot %d", bot.ID, taskID, robotID)
}

// markTaskPending moves a task back to pending queue
func (bot *ServiceBot) markTaskPending(taskID string) {
	bot.Election.mu.Lock()
	defer bot.Election.mu.Unlock()

	if task, exists := bot.Election.ActiveTasks[taskID]; exists {
		assignedBot := task.AssignedTo
		task.Status = "pending"
		task.AssignedTo = -1

		// If we had marked a bot busy, return it to idle so it can be selected again
		if info, ok := bot.Election.KnownBots[assignedBot]; ok {
			info.Status = "idle"
		}
		bot.Election.PendingTasks = append(bot.Election.PendingTasks, task)
		delete(bot.Election.ActiveTasks, taskID)
		log.Printf("[Leader] Bot %d: task %s moved to pending queue", bot.ID, taskID)
	}
}

// tryAssignPendingTasks attempts to assign pending tasks to available bots
func (bot *ServiceBot) tryAssignPendingTasks() {
	bot.Election.mu.Lock()
	if len(bot.Election.PendingTasks) == 0 {
		bot.Election.mu.Unlock()
		return
	}

	pendingCopy := make([]*TaskInfo, len(bot.Election.PendingTasks))
	copy(pendingCopy, bot.Election.PendingTasks)
	bot.Election.PendingTasks = nil
	bot.Election.mu.Unlock()

	for _, task := range pendingCopy {
		// Try to assign each pending task
		bot.assignTask(task.X, task.Y, task.Type)
	}
}

// pendingRetryLoop reattempts pending tasks on a short interval while this bot is leader
func (bot *ServiceBot) pendingRetryLoop() {
	ticker := time.NewTicker(3 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		bot.Election.mu.RLock()
		isLeader := bot.Election.IsLeader
		bot.Election.mu.RUnlock()

		if !isLeader {
			return
		}

		bot.tryAssignPendingTasks()
	}
}

// HandleTaskCompletion handles task completion reports (for leader)
func (bot *ServiceBot) HandleTaskCompletion(taskID string, robotID int, success bool) {
	bot.Election.mu.Lock()
	task, exists := bot.Election.ActiveTasks[taskID]
	if !exists {
		bot.Election.mu.Unlock()
		return
	}

	// Remove completed task
	delete(bot.Election.ActiveTasks, taskID)

	// Mark robot as idle
	if info, exists := bot.Election.KnownBots[robotID]; exists {
		info.Status = "idle"
	}

	worldAddr := bot.Election.WorldAddr
	bot.Election.mu.Unlock()

	log.Printf("[Leader] Bot %d: task %s completed by bot %d", bot.ID, taskID, robotID)

	// Delete problem from world
	go deleteProblemFromWorld(worldAddr, task.X, task.Y)

	// Publish problem solved message so coordinator can update its view
	go bot.publishProblemSolved(task.X, task.Y)

	// Try to assign pending tasks
	go bot.tryAssignPendingTasks()
}

// publishProblemSolved notifies coordinator that a problem has been solved
func (bot *ServiceBot) publishProblemSolved(x, y int) {
	if bot.mqttClient == nil {
		return
	}

	msg := struct {
		X int `json:"x"`
		Y int `json:"y"`
	}{
		X: x,
		Y: y,
	}

	if err := bot.mqttClient.Publish("events/problems/solved", msg); err != nil {
		log.Printf("[Leader] Failed to publish problem solved: %v", err)
	} else {
		log.Printf("[Leader] Published problem solved at (%d,%d)", x, y)
	}
}

// PublishFullStatus publishes complete status including gRPC address
func (bot *ServiceBot) PublishFullStatus() {
	if bot.mqttClient == nil {
		return
	}

	statusMsg := BotStatusMessage{
		ID:       bot.ID,
		Type:     bot.Type,
		Status:   bot.Status,
		GRPCAddr: bot.GRPCAdvertise,
		X:        bot.X,
		Y:        bot.Y,
	}

	topic := fmt.Sprintf("devices/servicebot/%d/status", bot.ID)
	if err := bot.mqttClient.Publish(topic, statusMsg); err != nil {
		log.Printf("Failed to publish full status: %v", err)
	}
}

// deleteProblemFromWorld removes a problem from the world service
func deleteProblemFromWorld(worldAddr string, x, y int) {
	body := fmt.Sprintf(`{"x":%d,"y":%d}`, x, y)
	req, err := http.NewRequest("DELETE", worldAddr+"/problem", strings.NewReader(body))
	if err != nil {
		log.Printf("[Leader] Failed to create delete request: %v", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		log.Printf("[Leader] Failed to delete problem from world: %v", err)
		return
	}
	defer resp.Body.Close()
	log.Printf("[Leader] Deleted problem at (%d,%d) from world, status=%d", x, y, resp.StatusCode)
}
