package servicebots

import (
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
)

// Election state constants
const (
	StateFollower  = "follower"
	StateCandidate = "candidate"
	StateLeader    = "leader"
)

// Election timeouts and intervals
const (
	ElectionTimeoutMin = 5 * time.Second
	ElectionTimeoutMax = 10 * time.Second
	HeartbeatInterval  = 2 * time.Second
	HeartbeatDeadline  = 6 * time.Second // 3x heartbeat interval
	AnswerWaitTime     = 3 * time.Second
)

// StartElectionLoop begins the election monitoring goroutine
func (bot *ServiceBot) StartElectionLoop() {
	// Add startup jitter to prevent simultaneous elections
	jitter := time.Duration(rand.Intn(2000)) * time.Millisecond
	time.Sleep(jitter)

	log.Printf("[ELECTION] Bot %d starting election loop with jitter %v", bot.ID, jitter)

	// Initialize as follower
	bot.becomeFollower(0, 0)

	// Subscribe to election topics
	if err := bot.subscribeToElectionTopics(); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to subscribe to election topics: %v", err)
		return
	}

	// Subscribe to bot metadata for state recovery
	if err := bot.subscribeToBotMetadata(); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to subscribe to bot metadata: %v", err)
		return
	}

	// Start periodic metadata publishing
	metadataTicker := time.NewTicker(5 * time.Second)
	defer metadataTicker.Stop()

	// Main election loop
	for {
		select {
		case <-bot.ElectionTimeout.C:
			// Election timeout expired - start new election
			log.Printf("[ELECTION] Bot %d election timeout expired, starting election", bot.ID)
			bot.startElection()
		case <-metadataTicker.C:
			// Periodically publish metadata to keep leader updated
			go bot.PublishMetadata()
		}
	}
}

// subscribeToElectionTopics subscribes to all election-related MQTT topics
func (bot *ServiceBot) subscribeToElectionTopics() error {
	// Only service bots (cleaner/repair) participate in elections
	if bot.Type != "cleaner" && bot.Type != "repair" {
		log.Printf("[ELECTION] Bot %d type=%s does not participate in elections", bot.ID, bot.Type)
		return nil
	}

	// Subscribe to heartbeats from leader
	heartbeatTopic := "election/heartbeat"
	if err := bot.mqttClient.Subscribe(heartbeatTopic, bot.handleHeartbeat); err != nil {
		return fmt.Errorf("subscribe to heartbeat: %w", err)
	}

	// Subscribe to election messages (wildcard to see all elections)
	electionTopic := "election/election/+"
	if err := bot.mqttClient.Subscribe(electionTopic, bot.handleElection); err != nil {
		return fmt.Errorf("subscribe to election: %w", err)
	}

	// Subscribe to answer messages directed to this bot
	answerTopic := fmt.Sprintf("election/answer/%d", bot.ID)
	if err := bot.mqttClient.Subscribe(answerTopic, bot.handleAnswer); err != nil {
		return fmt.Errorf("subscribe to answer: %w", err)
	}

	// Subscribe to victory announcements
	victoryTopic := "election/victory/+"
	if err := bot.mqttClient.Subscribe(victoryTopic, bot.handleVictory); err != nil {
		return fmt.Errorf("subscribe to victory: %w", err)
	}

	log.Printf("[ELECTION] Bot %d subscribed to election topics", bot.ID)
	return nil
}

// subscribeToBotMetadata subscribes to bot metadata for state recovery
func (bot *ServiceBot) subscribeToBotMetadata() error {
	// Subscribe to all bot metadata (wildcard)
	metadataTopic := "bots/metadata/+"
	if err := bot.mqttClient.Subscribe(metadataTopic, bot.handleBotMetadata); err != nil {
		return fmt.Errorf("subscribe to bot metadata: %w", err)
	}

	// Also subscribe to position updates to track bot movements
	positionTopic := "devices/servicebot/+/position"
	if err := bot.mqttClient.Subscribe(positionTopic, bot.handlePositionUpdate); err != nil {
		return fmt.Errorf("subscribe to position updates: %w", err)
	}

	log.Printf("[ELECTION] Bot %d subscribed to bot metadata and position updates", bot.ID)
	return nil
}

// startElection initiates a new leader election (Bully Algorithm)
func (bot *ServiceBot) startElection() {
	bot.Mu.Lock()
	if bot.ElectionInProgress {
		bot.Mu.Unlock()
		return
	}
	bot.ElectionInProgress = true
	bot.Term++
	currentTerm := bot.Term
	bot.Mu.Unlock()

	log.Printf("[ELECTION] Bot %d FOLLOWER → CANDIDATE, term=%d", bot.ID, currentTerm)
	bot.LeaderState = StateCandidate
	bot.VotesReceived = 0

	// Send ELECTION message to all higher-ID bots (they will see it via wildcard)
	electionMsg := mqtt.ElectionMessage{
		CandidateID: bot.ID,
		Term:        currentTerm,
		Timestamp:   time.Now().Format(time.RFC3339),
	}

	if err := bot.mqttClient.PublishElection(electionMsg); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to publish election: %v", err)
		bot.Mu.Lock()
		bot.ElectionInProgress = false
		bot.Mu.Unlock()
		return
	}

	log.Printf("[ELECTION] Bot %d sent ELECTION message, term=%d", bot.ID, currentTerm)

	// Wait for ANSWER messages
	time.Sleep(AnswerWaitTime)

	bot.Mu.Lock()
	receivedAnswers := bot.VotesReceived
	inProgress := bot.ElectionInProgress
	bot.Mu.Unlock()

	if !inProgress {
		// Election was cancelled (received ANSWER or VICTORY)
		log.Printf("[ELECTION] Bot %d election cancelled by higher bot", bot.ID)
		return
	}

	if receivedAnswers == 0 {
		// No higher-ID bots responded - we are the leader!
		bot.becomeLeader()
	} else {
		// Higher-ID bots responded - step down
		log.Printf("[ELECTION] Bot %d received %d answers, stepping down", bot.ID, receivedAnswers)
		bot.becomeFollower(bot.LeaderID, currentTerm)
	}

	bot.Mu.Lock()
	bot.ElectionInProgress = false
	bot.Mu.Unlock()
}

// becomeLeader transitions bot to leader state
func (bot *ServiceBot) becomeLeader() {
	bot.Mu.Lock()
	bot.LeaderState = StateLeader
	bot.LeaderID = bot.ID
	currentTerm := bot.Term
	bot.Mu.Unlock()

	log.Printf("[ELECTION] Bot %d CANDIDATE → LEADER, term=%d", bot.ID, currentTerm)

	// Send VICTORY announcement
	victoryMsg := mqtt.VictoryMessage{
		LeaderID:  bot.ID,
		Term:      currentTerm,
		Timestamp: time.Now().Format(time.RFC3339),
	}

	if err := bot.mqttClient.PublishVictory(victoryMsg); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to publish victory: %v", err)
	}

	// Start sending heartbeats
	bot.startHeartbeat()

	// Request all bots to re-publish their metadata (state recovery)
	log.Printf("[ELECTION] Bot %d became leader, requesting metadata from all bots", bot.ID)

	// Publish a metadata request message (all bots will respond by publishing their metadata)
	// This is done via the victory message - bots re-publish on victory
}

// becomeFollower transitions bot to follower state
func (bot *ServiceBot) becomeFollower(leaderID int, term int) {
	bot.Mu.Lock()
	oldState := bot.LeaderState
	bot.LeaderState = StateFollower
	bot.LeaderID = leaderID
	if term > bot.Term {
		bot.Term = term
	}
	bot.LastHeartbeat = time.Now()
	bot.Mu.Unlock()

	if oldState == StateLeader {
		log.Printf("[ELECTION] Bot %d LEADER → FOLLOWER, new leader=%d, term=%d", bot.ID, leaderID, term)
		bot.stopHeartbeat()
	} else if oldState == StateCandidate {
		log.Printf("[ELECTION] Bot %d CANDIDATE → FOLLOWER, leader=%d, term=%d", bot.ID, leaderID, term)
	} else {
		log.Printf("[ELECTION] Bot %d accepted leader=%d, term=%d", bot.ID, leaderID, term)
	}

	// Reset election timeout
	bot.resetElectionTimeout()
}

// startHeartbeat begins sending periodic heartbeats as leader
func (bot *ServiceBot) startHeartbeat() {
	bot.stopHeartbeat() // Stop any existing ticker

	ticker := time.NewTicker(HeartbeatInterval)
	bot.HeartbeatTicker = ticker

	go func(t *time.Ticker) {
		for range t.C {
			bot.Mu.Lock()
			if bot.LeaderState != StateLeader {
				bot.Mu.Unlock()
				return
			}
			currentTerm := bot.Term
			bot.Mu.Unlock()

			heartbeatMsg := mqtt.HeartbeatMessage{
				LeaderID:  bot.ID,
				Term:      currentTerm,
				Timestamp: time.Now().Format(time.RFC3339),
			}

			if err := bot.mqttClient.PublishHeartbeat(heartbeatMsg); err != nil {
				log.Printf("[ELECTION] ERROR: Failed to publish heartbeat: %v", err)
			}
		}
	}(ticker)
}

// stopHeartbeat stops sending heartbeats
func (bot *ServiceBot) stopHeartbeat() {
	if bot.HeartbeatTicker != nil {
		bot.HeartbeatTicker.Stop()
		bot.HeartbeatTicker = nil
	}
}

// resetElectionTimeout resets the election timeout with randomization
func (bot *ServiceBot) resetElectionTimeout() {
	if bot.ElectionTimeout != nil {
		bot.ElectionTimeout.Stop()
	}

	// Random timeout between min and max
	timeout := ElectionTimeoutMin + time.Duration(rand.Int63n(int64(ElectionTimeoutMax-ElectionTimeoutMin)))
	bot.ElectionTimeout = time.NewTimer(timeout)
}

// --- MQTT Message Handlers ---

// handleHeartbeat processes heartbeat messages from leader
func (bot *ServiceBot) handleHeartbeat(topic string, payload []byte) {
	var hb mqtt.HeartbeatMessage
	if err := json.Unmarshal(payload, &hb); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to unmarshal heartbeat: %v", err)
		return
	}

	bot.Mu.Lock()
	defer bot.Mu.Unlock()

	// Ignore stale heartbeats from old terms
	if hb.Term < bot.Term {
		return
	}

	// Update term if heartbeat has higher term
	if hb.Term > bot.Term {
		bot.Term = hb.Term
	}

	// If we are leader and see heartbeat from different leader with >= term, step down
	if bot.LeaderState == StateLeader && hb.LeaderID != bot.ID && hb.Term >= bot.Term {
		log.Printf("[ELECTION] Bot %d saw heartbeat from leader %d (term %d), stepping down", bot.ID, hb.LeaderID, hb.Term)
		bot.Mu.Unlock()
		bot.becomeFollower(hb.LeaderID, hb.Term)
		bot.Mu.Lock()
		return
	}

	// Accept leader if we are follower or candidate
	if bot.LeaderState != StateLeader {
		if bot.LeaderID != hb.LeaderID {
			log.Printf("[ELECTION] Bot %d received heartbeat from leader %d, term=%d", bot.ID, hb.LeaderID, hb.Term)
		}
		bot.LeaderID = hb.LeaderID
		bot.LastHeartbeat = time.Now()
		bot.resetElectionTimeout()
	}
}

// handleElection processes election messages (Bully Algorithm)
func (bot *ServiceBot) handleElection(topic string, payload []byte) {
	var elec mqtt.ElectionMessage
	if err := json.Unmarshal(payload, &elec); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to unmarshal election: %v", err)
		return
	}

	// Ignore own election messages
	if elec.CandidateID == bot.ID {
		return
	}

	bot.Mu.Lock()
	myID := bot.ID
	currentTerm := bot.Term
	bot.Mu.Unlock()

	// If candidate has higher ID than us, ignore (they should become leader)
	if elec.CandidateID > myID {
		return
	}

	// If candidate has lower ID, send ANSWER and start own election
	log.Printf("[ELECTION] Bot %d received ELECTION from lower bot %d, sending ANSWER", myID, elec.CandidateID)

	answerMsg := mqtt.AnswerMessage{
		RespondingID: myID,
		ToCandidate:  elec.CandidateID,
		Term:         currentTerm,
		Timestamp:    time.Now().Format(time.RFC3339),
	}

	if err := bot.mqttClient.PublishAnswer(answerMsg); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to publish answer: %v", err)
	}

	// Start own election if not already leader or in election
	bot.Mu.Lock()
	if !bot.ElectionInProgress && bot.LeaderState != StateLeader {
		bot.Mu.Unlock()
		go bot.startElection()
	} else {
		bot.Mu.Unlock()
	}
}

// handleAnswer processes answer messages
func (bot *ServiceBot) handleAnswer(topic string, payload []byte) {
	var ans mqtt.AnswerMessage
	if err := json.Unmarshal(payload, &ans); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to unmarshal answer: %v", err)
		return
	}

	bot.Mu.Lock()
	defer bot.Mu.Unlock()

	// Only process if we are in election
	if !bot.ElectionInProgress || bot.LeaderState != StateCandidate {
		return
	}

	log.Printf("[ELECTION] Bot %d received ANSWER from bot %d, cancelling election", bot.ID, ans.RespondingID)

	// Higher-ID bot responded - cancel our election
	bot.VotesReceived++
	bot.ElectionInProgress = false
}

// handleVictory processes victory announcements
func (bot *ServiceBot) handleVictory(topic string, payload []byte) {
	var vic mqtt.VictoryMessage
	if err := json.Unmarshal(payload, &vic); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to unmarshal victory: %v", err)
		return
	}

	// Ignore own victory messages
	if vic.LeaderID == bot.ID {
		return
	}

	bot.Mu.Lock()
	myID := bot.ID
	currentState := bot.LeaderState
	bot.Mu.Unlock()

	log.Printf("[ELECTION] Bot %d received VICTORY from bot %d, term=%d", myID, vic.LeaderID, vic.Term)

	// Accept new leader
	bot.becomeFollower(vic.LeaderID, vic.Term)

	// If we were candidate, cancel election
	if currentState == StateCandidate {
		bot.Mu.Lock()
		bot.ElectionInProgress = false
		bot.Mu.Unlock()
	}

	// If new leader, re-publish our metadata for state recovery
	go bot.PublishMetadata()
}

// handleBotMetadata processes bot metadata messages (state recovery)
func (bot *ServiceBot) handleBotMetadata(topic string, payload []byte) {
	var meta mqtt.BotMetadata
	if err := json.Unmarshal(payload, &meta); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to unmarshal bot metadata: %v", err)
		return
	}

	bot.TaskMu.Lock()
	defer bot.TaskMu.Unlock()

	// Update or add bot info (including own metadata so leader can assign tasks to itself)
	botInfo, exists := bot.KnownBots[meta.ID]
	if !exists {
		botInfo = &BotInfo{}
		bot.KnownBots[meta.ID] = botInfo
		if meta.ID == bot.ID {
			log.Printf("[ELECTION] Bot %d added itself to KnownBots (type=%s)", bot.ID, meta.Type)
		} else {
			log.Printf("[ELECTION] Bot %d discovered new bot %d (type=%s)", bot.ID, meta.ID, meta.Type)
		}
	}

	botInfo.ID = meta.ID
	botInfo.Type = meta.Type
	botInfo.GRPCAddr = meta.GRPCAddr
	botInfo.X = meta.X
	botInfo.Y = meta.Y
	botInfo.Status = meta.Status
	botInfo.TaskID = meta.TaskID
	botInfo.LastSeen = time.Now()
	botInfo.Term = meta.Term
}

// handlePositionUpdate processes position updates from other service bots
func (bot *ServiceBot) handlePositionUpdate(topic string, payload []byte) {
	var pos mqtt.PositionMessage
	if err := json.Unmarshal(payload, &pos); err != nil {
		log.Printf("[ELECTION] ERROR: Failed to unmarshal position update: %v", err)
		return
	}

	// Ignore own position updates
	if pos.ID == bot.ID {
		return
	}

	bot.TaskMu.Lock()
	defer bot.TaskMu.Unlock()

	// Update bot position if we know this bot
	if botInfo, exists := bot.KnownBots[pos.ID]; exists {
		botInfo.X = pos.X
		botInfo.Y = pos.Y
		botInfo.LastSeen = time.Now()
	}
}
