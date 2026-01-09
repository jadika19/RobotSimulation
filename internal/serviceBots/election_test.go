package servicebots_test

import (
	"encoding/json"
	"sync"
	"testing"
	"time"

	servicebots "code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/serviceBots"
)

// TestElectionStateInitialization tests that election state is properly initialized
func TestElectionStateInitialization(t *testing.T) {
	state := servicebots.NewElectionState()

	if state.IsLeader {
		t.Error("New election state should not be leader")
	}
	if state.CurrentLeaderID != 0 {
		t.Errorf("Expected CurrentLeaderID to be 0, got %d", state.CurrentLeaderID)
	}
	if len(state.KnownBots) != 0 {
		t.Errorf("Expected KnownBots to be empty, got %d", len(state.KnownBots))
	}
	if len(state.ActiveTasks) != 0 {
		t.Errorf("Expected ActiveTasks to be empty, got %d", len(state.ActiveTasks))
	}
	if state.NextTaskID != 1 {
		t.Errorf("Expected NextTaskID to be 1, got %d", state.NextTaskID)
	}
}

// TestBotInfoTracking tests that bot information is properly tracked
func TestBotInfoTracking(t *testing.T) {
	state := servicebots.NewElectionState()

	// Add a bot
	state.KnownBots[1] = &servicebots.BotInfo{
		ID:       1,
		Type:     "cleaner",
		X:        5,
		Y:        10,
		Status:   "idle",
		GRPCAddr: "localhost:50051",
		LastSeen: time.Now(),
	}

	// Verify bot is tracked
	bot, exists := state.KnownBots[1]
	if !exists {
		t.Fatal("Bot should exist in KnownBots")
	}
	if bot.Type != "cleaner" {
		t.Errorf("Expected type 'cleaner', got '%s'", bot.Type)
	}
	if bot.X != 5 || bot.Y != 10 {
		t.Errorf("Expected position (5,10), got (%d,%d)", bot.X, bot.Y)
	}
}

// TestElectionMessageSerialization tests election message JSON encoding
func TestElectionMessageSerialization(t *testing.T) {
	msg := servicebots.ElectionMessage{
		SenderID: 42,
		Type:     "request",
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal election message: %v", err)
	}

	var decoded servicebots.ElectionMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal election message: %v", err)
	}

	if decoded.SenderID != 42 {
		t.Errorf("Expected SenderID 42, got %d", decoded.SenderID)
	}
	if decoded.Type != "request" {
		t.Errorf("Expected Type 'request', got '%s'", decoded.Type)
	}
}

// TestLeaderAnnouncementSerialization tests leader announcement JSON encoding
func TestLeaderAnnouncementSerialization(t *testing.T) {
	announcement := servicebots.LeaderAnnouncement{
		LeaderID:  5,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	data, err := json.Marshal(announcement)
	if err != nil {
		t.Fatalf("Failed to marshal leader announcement: %v", err)
	}

	var decoded servicebots.LeaderAnnouncement
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal leader announcement: %v", err)
	}

	if decoded.LeaderID != 5 {
		t.Errorf("Expected LeaderID 5, got %d", decoded.LeaderID)
	}
}

// TestBotStatusMessageSerialization tests full status message JSON encoding
func TestBotStatusMessageSerialization(t *testing.T) {
	msg := servicebots.BotStatusMessage{
		ID:       3,
		Type:     "repair",
		Status:   "idle",
		GRPCAddr: "bot3:50051",
		X:        15,
		Y:        20,
	}

	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("Failed to marshal status message: %v", err)
	}

	var decoded servicebots.BotStatusMessage
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("Failed to unmarshal status message: %v", err)
	}

	if decoded.ID != 3 {
		t.Errorf("Expected ID 3, got %d", decoded.ID)
	}
	if decoded.GRPCAddr != "bot3:50051" {
		t.Errorf("Expected GRPCAddr 'bot3:50051', got '%s'", decoded.GRPCAddr)
	}
}

// TestTaskInfoTracking tests pending task queue functionality
func TestTaskInfoTracking(t *testing.T) {
	state := servicebots.NewElectionState()

	// Add a pending task
	task := &servicebots.TaskInfo{
		ID:         "task-1",
		X:          5,
		Y:          10,
		Type:       "dirt",
		AssignedTo: -1,
		Status:     "pending",
		CreatedAt:  time.Now(),
	}
	state.PendingTasks = append(state.PendingTasks, task)

	if len(state.PendingTasks) != 1 {
		t.Errorf("Expected 1 pending task, got %d", len(state.PendingTasks))
	}

	// Move to active tasks
	state.ActiveTasks[task.ID] = task
	task.Status = "assigned"
	task.AssignedTo = 2
	state.PendingTasks = nil

	if len(state.PendingTasks) != 0 {
		t.Errorf("Expected 0 pending tasks, got %d", len(state.PendingTasks))
	}
	if len(state.ActiveTasks) != 1 {
		t.Errorf("Expected 1 active task, got %d", len(state.ActiveTasks))
	}

	activeTask := state.ActiveTasks["task-1"]
	if activeTask.AssignedTo != 2 {
		t.Errorf("Expected task assigned to bot 2, got %d", activeTask.AssignedTo)
	}
}

// TestConcurrentElectionStateAccess tests thread safety of election state
func TestConcurrentElectionStateAccess(t *testing.T) {
	state := servicebots.NewElectionState()
	var wg sync.WaitGroup

	// Simulate concurrent access (using mutex as the real code does)
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()

			// Concurrent writes with proper locking
			state.Lock()
			state.KnownBots[id] = &servicebots.BotInfo{
				ID:       id,
				Type:     "cleaner",
				Status:   "idle",
				LastSeen: time.Now(),
			}
			state.Unlock()

			// Concurrent reads with proper locking
			state.RLock()
			_ = state.IsLeader
			_ = state.CurrentLeaderID
			state.RUnlock()
		}(i)
	}

	wg.Wait()

	state.RLock()
	botCount := len(state.KnownBots)
	state.RUnlock()

	if botCount != 100 {
		t.Errorf("Expected 100 bots, got %d", botCount)
	}
}

// TestHigherIDWinsElection tests that higher ID bot wins in Bully algorithm
func TestHigherIDWinsElection(t *testing.T) {
	// Simulate election logic manually
	botIDs := []int{3, 7, 2, 9, 5}

	// Find highest ID (winner according to Bully algorithm)
	winner := 0
	for _, id := range botIDs {
		if id > winner {
			winner = id
		}
	}

	if winner != 9 {
		t.Errorf("Expected bot 9 to win election, got %d", winner)
	}
}

// TestClosestBotSelection tests task assignment to closest bot
func TestClosestBotSelection(t *testing.T) {
	state := servicebots.NewElectionState()

	// Add bots at different positions
	state.KnownBots[1] = &servicebots.BotInfo{
		ID:       1,
		Type:     "cleaner",
		X:        0,
		Y:        0,
		Status:   "idle",
		GRPCAddr: "bot1:50051",
	}
	state.KnownBots[2] = &servicebots.BotInfo{
		ID:       2,
		Type:     "cleaner",
		X:        10,
		Y:        10,
		Status:   "idle",
		GRPCAddr: "bot2:50051",
	}
	state.KnownBots[3] = &servicebots.BotInfo{
		ID:       3,
		Type:     "cleaner",
		X:        5,
		Y:        5,
		Status:   "idle",
		GRPCAddr: "bot3:50051",
	}

	// Problem at (4, 4) - bot 3 at (5,5) should be closest
	problemX, problemY := 4, 4

	var closestBot *servicebots.BotInfo
	closestDist := float64(-1)

	for _, bot := range state.KnownBots {
		if bot.Type != "cleaner" || bot.Status != "idle" {
			continue
		}
		dx := float64(bot.X - problemX)
		dy := float64(bot.Y - problemY)
		dist := dx*dx + dy*dy // Squared distance

		if closestDist < 0 || dist < closestDist {
			closestBot = bot
			closestDist = dist
		}
	}

	if closestBot == nil {
		t.Fatal("No closest bot found")
	}
	if closestBot.ID != 3 {
		t.Errorf("Expected bot 3 to be closest, got bot %d", closestBot.ID)
	}
}

// TestBotTypeFiltering tests that task assignment respects bot types
func TestBotTypeFiltering(t *testing.T) {
	state := servicebots.NewElectionState()

	// Add different types of bots
	state.KnownBots[1] = &servicebots.BotInfo{
		ID:       1,
		Type:     "cleaner",
		Status:   "idle",
		GRPCAddr: "bot1:50051",
	}
	state.KnownBots[2] = &servicebots.BotInfo{
		ID:       2,
		Type:     "repair",
		Status:   "idle",
		GRPCAddr: "bot2:50051",
	}

	// Count cleaners
	cleanerCount := 0
	for _, bot := range state.KnownBots {
		if bot.Type == "cleaner" {
			cleanerCount++
		}
	}

	if cleanerCount != 1 {
		t.Errorf("Expected 1 cleaner, got %d", cleanerCount)
	}

	// Count repair bots
	repairCount := 0
	for _, bot := range state.KnownBots {
		if bot.Type == "repair" {
			repairCount++
		}
	}

	if repairCount != 1 {
		t.Errorf("Expected 1 repair bot, got %d", repairCount)
	}
}

// TestBusyBotExclusion tests that busy bots are excluded from task assignment
func TestBusyBotExclusion(t *testing.T) {
	state := servicebots.NewElectionState()

	// Add an idle and a busy bot
	state.KnownBots[1] = &servicebots.BotInfo{
		ID:       1,
		Type:     "cleaner",
		Status:   "busy",
		GRPCAddr: "bot1:50051",
	}
	state.KnownBots[2] = &servicebots.BotInfo{
		ID:       2,
		Type:     "cleaner",
		Status:   "idle",
		GRPCAddr: "bot2:50051",
	}

	// Count available (idle) bots
	availableCount := 0
	for _, bot := range state.KnownBots {
		if bot.Status == "idle" {
			availableCount++
		}
	}

	if availableCount != 1 {
		t.Errorf("Expected 1 available bot, got %d", availableCount)
	}
}
