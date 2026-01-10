package servicebots

import (
	"context"
	"encoding/json"
	"log"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/taskpb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

const (
	TaskTimeout = 30 * time.Second // Task reassignment timeout
)

// StartTaskManagement begins the leader's task management loop
func (bot *ServiceBot) StartTaskManagement() {
	// Subscribe to new tasks from detectors
	if err := bot.mqttClient.Subscribe("tasks/new", bot.handleNewTask); err != nil {
		log.Printf("[LEADER] ERROR: Failed to subscribe to tasks/new: %v", err)
		return
	}

	// Subscribe to task events for state recovery
	if err := bot.mqttClient.Subscribe("tasks/events", bot.handleTaskEvent); err != nil {
		log.Printf("[LEADER] ERROR: Failed to subscribe to tasks/events: %v", err)
		return
	}

	log.Printf("[LEADER] Bot %d started task management", bot.ID)

	// Start task timeout monitor
	go bot.monitorTaskTimeouts()
}

// handleNewTask processes new task announcements from detectors
func (bot *ServiceBot) handleNewTask(topic string, payload []byte) {
	var task mqtt.TaskMessage
	if err := json.Unmarshal(payload, &task); err != nil {
		log.Printf("[LEADER] ERROR: Failed to unmarshal task: %v", err)
		return
	}

	// Only leader processes tasks
	bot.Mu.Lock()
	isLeader := bot.LeaderState == StateLeader
	bot.Mu.Unlock()

	if !isLeader {
		return
	}

	log.Printf("[LEADER] Bot %d received new task: %s at (%d,%d) type=%s", bot.ID, task.TaskID, task.X, task.Y, task.Type)

	// Check if task already exists (deduplication)
	bot.TaskMu.Lock()
	if _, exists := bot.PendingTasks[task.TaskID]; exists {
		bot.TaskMu.Unlock()
		return
	}
	if _, exists := bot.AssignedTasks[task.TaskID]; exists {
		bot.TaskMu.Unlock()
		return
	}

	// Create task info
	taskInfo := &TaskInfo{
		ID:        task.TaskID,
		X:         task.X,
		Y:         task.Y,
		Type:      task.Type,
		RobotID:   -1,
		Status:    "pending",
		CreatedAt: time.Now(),
	}
	bot.PendingTasks[task.TaskID] = taskInfo
	bot.TaskMu.Unlock()

	// Try to assign immediately
	bot.tryAssignTask(taskInfo)
}

// tryAssignTask attempts to assign a task to the nearest idle bot
func (bot *ServiceBot) tryAssignTask(task *TaskInfo) {
	bot.TaskMu.Lock()
	defer bot.TaskMu.Unlock()

	// Determine required bot type
	requiredType := "cleaner"
	if task.Type == "defect" {
		requiredType = "repair"
	}

	// Find nearest idle bot of correct type using Manhattan distance
	var bestBot *BotInfo
	bestDistance := -1

	// Check all bots in KnownBots (including leader itself if it's there)
	for _, botInfo := range bot.KnownBots {
		// Skip if wrong type
		if botInfo.Type != requiredType {
			continue
		}

		// Skip if not idle
		if botInfo.Status != "idle" {
			continue
		}

		// Skip if bot is stale (not seen in 10 seconds)
		if time.Since(botInfo.LastSeen) > 10*time.Second {
			continue
		}

		// Calculate Manhattan distance
		distance := abs(botInfo.X-task.X) + abs(botInfo.Y-task.Y)

		if bestDistance < 0 || distance < bestDistance {
			bestBot = botInfo
			bestDistance = distance
		}
	}

	if bestBot == nil {
		log.Printf("[LEADER] Bot %d: No idle %s bot available for task %s, keeping pending", bot.ID, requiredType, task.ID)
		return
	}

	// Assign task to bot
	log.Printf("[LEADER] Bot %d assigning task %s to bot %d (distance=%d)", bot.ID, task.ID, bestBot.ID, bestDistance)

	task.RobotID = bestBot.ID
	task.Status = "assigned"
	task.AssignedAt = time.Now()

	// Move from pending to assigned
	delete(bot.PendingTasks, task.ID)
	bot.AssignedTasks[task.ID] = task

	// Update bot status
	bestBot.Status = "busy"
	bestBot.TaskID = task.ID

	// Publish task assignment event (event sourcing)
	event := mqtt.TaskAssignmentEvent{
		TaskID:    task.ID,
		RobotID:   bestBot.ID,
		EventType: "assigned",
		LeaderID:  bot.ID,
		Term:      bot.Term,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	if err := bot.mqttClient.PublishTaskEvent(event); err != nil {
		log.Printf("[LEADER] ERROR: Failed to publish task event: %v", err)
	}

	// Record in event log
	bot.TaskEventLog = append(bot.TaskEventLog, event)

	// Call bot via gRPC to assign task (even if it's the leader itself)
	go bot.assignTaskViaGRPC(bestBot, task)
}

// assignTaskViaGRPC sends task assignment to bot via gRPC
func (bot *ServiceBot) assignTaskViaGRPC(botInfo *BotInfo, task *TaskInfo) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx, botInfo.GRPCAddr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		log.Printf("[LEADER] ERROR: Failed to connect to bot %d gRPC: %v", botInfo.ID, err)
		bot.handleTaskAssignmentFailure(task.ID)
		return
	}
	defer conn.Close()

	client := taskpb.NewTaskServiceClient(conn)
	resp, err := client.AssignTask(ctx, &taskpb.TaskRequest{
		TaskId:      task.ID,
		X:           int32(task.X),
		Y:           int32(task.Y),
		ProblemType: task.Type,
	})

	if err != nil {
		log.Printf("[LEADER] ERROR: Failed to assign task to bot %d: %v", botInfo.ID, err)
		bot.handleTaskAssignmentFailure(task.ID)
		return
	}

	if !resp.Accepted {
		log.Printf("[LEADER] Bot %d rejected task %s: %s", botInfo.ID, task.ID, resp.Message)
		bot.handleTaskAssignmentFailure(task.ID)
		return
	}

	log.Printf("[LEADER] Bot %d accepted task %s", botInfo.ID, task.ID)
}

// handleTaskAssignmentFailure handles failed task assignment
func (bot *ServiceBot) handleTaskAssignmentFailure(taskID string) {
	bot.TaskMu.Lock()
	defer bot.TaskMu.Unlock()

	task, exists := bot.AssignedTasks[taskID]
	if !exists {
		return
	}

	log.Printf("[LEADER] Task %s assignment failed, moving back to pending", taskID)

	// Move back to pending
	delete(bot.AssignedTasks, taskID)
	task.Status = "pending"
	task.RobotID = -1
	bot.PendingTasks[taskID] = task

	// Update bot status if it was marked busy
	if task.RobotID != -1 {
		if botInfo, exists := bot.KnownBots[task.RobotID]; exists {
			botInfo.Status = "idle"
			botInfo.TaskID = ""
		}
	}

	// Publish failure event
	event := mqtt.TaskAssignmentEvent{
		TaskID:    taskID,
		RobotID:   task.RobotID,
		EventType: "failed",
		LeaderID:  bot.ID,
		Term:      bot.Term,
		Timestamp: time.Now().Format(time.RFC3339),
	}
	bot.mqttClient.PublishTaskEvent(event)
	bot.TaskEventLog = append(bot.TaskEventLog, event)

	// Try to reassign to another bot
	taskCopy := task
	go func() {
		time.Sleep(1 * time.Second)
		bot.tryAssignTask(taskCopy)
	}()
}

// monitorTaskTimeouts monitors assigned tasks for timeouts
func (bot *ServiceBot) monitorTaskTimeouts() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for range ticker.C {
		bot.Mu.Lock()
		isLeader := bot.LeaderState == StateLeader
		bot.Mu.Unlock()

		if !isLeader {
			continue
		}

		bot.TaskMu.Lock()
		now := time.Now()

		for taskID, task := range bot.AssignedTasks {
			if now.Sub(task.AssignedAt) > TaskTimeout {
				log.Printf("[LEADER] Task %s timed out (assigned to bot %d), reassigning", taskID, task.RobotID)

				// Mark bot as potentially offline
				if botInfo, exists := bot.KnownBots[task.RobotID]; exists {
					botInfo.Status = "idle"
					botInfo.TaskID = ""
				}

				// Move back to pending
				delete(bot.AssignedTasks, taskID)
				task.Status = "pending"
				task.RobotID = -1
				bot.PendingTasks[taskID] = task

				// Publish timeout event
				event := mqtt.TaskAssignmentEvent{
					TaskID:    taskID,
					RobotID:   task.RobotID,
					EventType: "timeout",
					LeaderID:  bot.ID,
					Term:      bot.Term,
					Timestamp: time.Now().Format(time.RFC3339),
				}
				bot.mqttClient.PublishTaskEvent(event)
				bot.TaskEventLog = append(bot.TaskEventLog, event)

				// Try to reassign
				go func(t *TaskInfo) {
					time.Sleep(1 * time.Second)
					bot.tryAssignTask(t)
				}(task)
			}
		}

		bot.TaskMu.Unlock()
	}
}

// handleTaskEvent processes task events (event sourcing for state recovery)
func (bot *ServiceBot) handleTaskEvent(topic string, payload []byte) {
	var event mqtt.TaskAssignmentEvent
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("[LEADER] ERROR: Failed to unmarshal task event: %v", err)
		return
	}

	// Only new leader processes events for state recovery
	bot.Mu.Lock()
	isLeader := bot.LeaderState == StateLeader
	bot.Mu.Unlock()

	if !isLeader {
		return
	}

	// Record event in log
	bot.TaskMu.Lock()
	bot.TaskEventLog = append(bot.TaskEventLog, event)
	bot.TaskMu.Unlock()

	log.Printf("[LEADER] Bot %d received task event: %s type=%s robot=%d", bot.ID, event.TaskID, event.EventType, event.RobotID)

	// Process the event
	switch event.EventType {
	case "completed":
		// Task was completed by a bot - update state and try to assign pending tasks
		bot.OnTaskCompletion(event.TaskID, event.RobotID, true)
	case "failed":
		// Task failed - could trigger reassignment
		bot.OnTaskCompletion(event.TaskID, event.RobotID, false)
	case "timeout":
		// Task timed out - handle timeout
		bot.handleTaskAssignmentFailure(event.TaskID)
	}
}

// OnTaskCompletion is called when a bot completes a task (via handleTaskEvent from MQTT)
// Events are published by the bot itself, not here, to avoid duplicates
func (bot *ServiceBot) OnTaskCompletion(taskID string, robotID int, success bool) {
	bot.Mu.Lock()
	isLeader := bot.LeaderState == StateLeader
	bot.Mu.Unlock()

	if !isLeader {
		return
	}

	bot.TaskMu.Lock()
	defer bot.TaskMu.Unlock()

	task, exists := bot.AssignedTasks[taskID]
	if !exists {
		log.Printf("[LEADER] Received completion for unknown task %s from bot %d", taskID, robotID)
		return
	}

	log.Printf("[LEADER] Bot %d completed task %s, success=%v", robotID, taskID, success)

	// Remove from assigned tasks
	delete(bot.AssignedTasks, taskID)
	task.Status = "completed"

	// Update bot status in KnownBots
	if botInfo, exists := bot.KnownBots[robotID]; exists {
		botInfo.Status = "idle"
		botInfo.TaskID = ""
	}

	// Try to assign pending tasks to newly freed bot
	go bot.tryAssignPendingTasks()
}

// tryAssignPendingTasks attempts to assign all pending tasks
func (bot *ServiceBot) tryAssignPendingTasks() {
	bot.TaskMu.Lock()
	pendingTasks := make([]*TaskInfo, 0, len(bot.PendingTasks))
	for _, task := range bot.PendingTasks {
		pendingTasks = append(pendingTasks, task)
	}
	bot.TaskMu.Unlock()

	for _, task := range pendingTasks {
		bot.tryAssignTask(task)
	}
}

// abs returns absolute value of an integer
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}

// GetTaskStats returns current task statistics (for debugging/monitoring)
func (bot *ServiceBot) GetTaskStats() map[string]interface{} {
	bot.TaskMu.RLock()
	defer bot.TaskMu.RUnlock()

	return map[string]interface{}{
		"pendingTasks":  len(bot.PendingTasks),
		"assignedTasks": len(bot.AssignedTasks),
		"knownBots":     len(bot.KnownBots),
		"eventLogSize":  len(bot.TaskEventLog),
	}
}
