package servicebots

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
)

type subscription struct {
	topic    string
	handler  func(string, []byte)
	clientID string
}

type fakeBroker struct {
	mu   sync.Mutex
	subs []subscription
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{subs: make([]subscription, 0)}
}

func (b *fakeBroker) addSub(clientID, topic string, handler func(string, []byte)) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.subs = append(b.subs, subscription{topic: topic, handler: handler, clientID: clientID})
}

func (b *fakeBroker) removeClient(clientID string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	filtered := b.subs[:0]
	for _, s := range b.subs {
		if s.clientID != clientID {
			filtered = append(filtered, s)
		}
	}
	b.subs = filtered
}

func (b *fakeBroker) publish(topic string, payload []byte) {
	b.mu.Lock()
	handlers := make([]subscription, len(b.subs))
	copy(handlers, b.subs)
	b.mu.Unlock()

	for _, s := range handlers {
		if topicMatches(s.topic, topic) {
			s.handler(topic, payload)
		}
	}
}

func topicMatches(sub, topic string) bool {
	subParts := strings.Split(sub, "/")
	topicParts := strings.Split(topic, "/")
	if len(subParts) != len(topicParts) {
		return false
	}

	for i := range subParts {
		if subParts[i] == "+" {
			continue
		}
		if subParts[i] != topicParts[i] {
			return false
		}
	}
	return true
}

type fakeMQTTClient struct {
	id     string
	broker *fakeBroker
	alive  int32
}

func (c *fakeMQTTClient) isAlive() bool {
	return atomic.LoadInt32(&c.alive) == 1
}

func (c *fakeMQTTClient) Subscribe(topic string, handler func(topic string, payload []byte)) error {
	if !c.isAlive() {
		return nil
	}
	c.broker.addSub(c.id, topic, handler)
	return nil
}

func (c *fakeMQTTClient) Disconnect(waitMillis uint) {
	if atomic.SwapInt32(&c.alive, 0) == 0 {
		return
	}
	c.broker.removeClient(c.id)
}

func (c *fakeMQTTClient) publish(topic string, payload interface{}) error {
	if !c.isAlive() {
		return nil
	}
	switch v := payload.(type) {
	case string:
		c.broker.publish(topic, []byte(v))
	case []byte:
		c.broker.publish(topic, v)
	default:
		data, err := json.Marshal(v)
		if err != nil {
			return err
		}
		c.broker.publish(topic, data)
	}
	return nil
}

func (c *fakeMQTTClient) PublishElection(msg mqtt.ElectionMessage) error {
	return c.publish(fmt.Sprintf("election/election/%d", msg.CandidateID), msg)
}

func (c *fakeMQTTClient) PublishAnswer(msg mqtt.AnswerMessage) error {
	return c.publish(fmt.Sprintf("election/answer/%d", msg.ToCandidate), msg)
}

func (c *fakeMQTTClient) PublishVictory(msg mqtt.VictoryMessage) error {
	return c.publish(fmt.Sprintf("election/victory/%d", msg.LeaderID), msg)
}

func (c *fakeMQTTClient) PublishHeartbeat(msg mqtt.HeartbeatMessage) error {
	return c.publish("election/heartbeat", msg)
}

func (c *fakeMQTTClient) PublishBotMetadata(meta mqtt.BotMetadata) error {
	return c.publish(fmt.Sprintf("bots/metadata/%d", meta.ID), meta)
}

func (c *fakeMQTTClient) PublishPosition(botType string, pos mqtt.PositionMessage) error {
	return c.publish(fmt.Sprintf("devices/%s/%d/position", botType, pos.ID), pos)
}

func (c *fakeMQTTClient) PublishStatus(botType string, botID int, status string) error {
	return c.publish(fmt.Sprintf("devices/%s/%d/status", botType, botID), status)
}

func (c *fakeMQTTClient) PublishTaskEvent(event mqtt.TaskAssignmentEvent) error {
	return c.publish("tasks/events", event)
}

func newFakeMQTTClient(broker *fakeBroker, id int) *fakeMQTTClient {
	return &fakeMQTTClient{
		id:     fmt.Sprintf("bot-%d", id),
		broker: broker,
		alive:  1,
	}
}

func newTestBot(id int, broker *fakeBroker) (*ServiceBot, *fakeMQTTClient) {
	client := newFakeMQTTClient(broker, id)
	bot := New("cleaner")
	bot.ID = id
	bot.mqttClient = client
	bot.LastHeartbeat = time.Now()
	return bot, client
}

func runElection(t *testing.T, bot *ServiceBot) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		bot.startElection()
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(AnswerWaitTime + time.Second):
		t.Fatalf("election did not finish for bot %d", bot.ID)
	}
}

func TestElectionPromotesFollowerWhenLeaderDies(t *testing.T) {
	broker := newFakeBroker()

	leaderBot, leaderClient := newTestBot(2, broker)
	followerBot, _ := newTestBot(1, broker)

	if err := leaderBot.subscribeToElectionTopics(); err != nil {
		t.Fatalf("leader subscribe failed: %v", err)
	}
	if err := followerBot.subscribeToElectionTopics(); err != nil {
		t.Fatalf("follower subscribe failed: %v", err)
	}

	// First election with higher-ID bot alive should keep follower as follower.
	runElection(t, followerBot)
	if followerBot.LeaderState == StateLeader {
		t.Fatalf("bot %d should not become leader while higher-ID bot is alive", followerBot.ID)
	}

	// Allow the higher-ID bot to complete its own election and declare victory.
	time.Sleep(AnswerWaitTime + 200*time.Millisecond)
	if leaderBot.LeaderState != StateLeader {
		t.Fatalf("higher-ID bot %d should have become leader, state=%s", leaderBot.ID, leaderBot.LeaderState)
	}

	// Simulate leader death (disconnect from broker).
	leaderBot.stopHeartbeat()
	leaderClient.Disconnect(0)

	// Run another election; with no answers expected, follower should promote itself.
	runElection(t, followerBot)
	if followerBot.LeaderState != StateLeader {
		t.Fatalf("bot %d did not become leader after leader death", followerBot.ID)
	}
	if followerBot.LeaderID != followerBot.ID {
		t.Fatalf("expected bot %d to set itself as leader, got %d", followerBot.ID, followerBot.LeaderID)
	}

	followerBot.stopHeartbeat()
}

func TestSimultaneousElectionsSelectHighestID(t *testing.T) {
	broker := newFakeBroker()

	bot1, _ := newTestBot(1, broker)
	bot2, _ := newTestBot(2, broker)
	bot3, _ := newTestBot(3, broker)
	bots := []*ServiceBot{bot1, bot2, bot3}

	for _, b := range bots {
		if err := b.subscribeToElectionTopics(); err != nil {
			t.Fatalf("subscribe failed for bot %d: %v", b.ID, err)
		}
	}

	var wg sync.WaitGroup
	for _, b := range bots {
		wg.Add(1)
		go func(bot *ServiceBot) {
			defer wg.Done()
			runElection(t, bot)
		}(b)
	}
	wg.Wait()

	// Give time for victory propagation
	time.Sleep(AnswerWaitTime + 200*time.Millisecond)

	if bot3.LeaderState != StateLeader {
		t.Fatalf("highest-ID bot should be leader, got state=%s", bot3.LeaderState)
	}
	if bot3.LeaderID != bot3.ID {
		t.Fatalf("leader ID should be %d, got %d", bot3.ID, bot3.LeaderID)
	}

	for _, b := range []*ServiceBot{bot1, bot2} {
		if b.LeaderState != StateFollower {
			t.Fatalf("bot %d should be follower, got %s", b.ID, b.LeaderState)
		}
		if b.LeaderID != bot3.ID {
			t.Fatalf("bot %d should follow leader %d, got %d", b.ID, bot3.ID, b.LeaderID)
		}
		b.stopHeartbeat()
	}
	bot3.stopHeartbeat()
}

func TestTaskRequeuedWhenAssignedBotDies(t *testing.T) {
	broker := newFakeBroker()
	leaderBot, _ := newTestBot(99, broker)
	leaderBot.LeaderState = StateLeader
	leaderBot.mqttClient = newFakeMQTTClient(broker, 99)

	t.Log("[TEST] setup: leader=99, deadBot=2, task=task-dead-bot")

	// Set up maps
	leaderBot.AssignedTasks = make(map[string]*TaskInfo)
	leaderBot.PendingTasks = make(map[string]*TaskInfo)
	leaderBot.KnownBots[2] = &BotInfo{ID: 2, Type: "cleaner", Status: "busy", LastSeen: time.Now()}

	task := &TaskInfo{
		ID:         "task-dead-bot",
		X:          5,
		Y:          5,
		Type:       "dirt",
		RobotID:    2, // assigned to the bot that will "die"
		Status:     "assigned",
		AssignedAt: time.Now().Add(-1 * time.Minute),
	}

	leaderBot.AssignedTasks[task.ID] = task

	t.Logf("[TEST] assign: task %s -> bot %d (status=%s)", task.ID, task.RobotID, leaderBot.KnownBots[2].Status)

	// Simulate bot death while task is in-flight: leader should detect and re-queue via handleBotDeath.
	t.Log("[TEST] simulate death: bot 2 goes offline")
	leaderBot.handleBotDeath(2)

	if _, exists := leaderBot.AssignedTasks[task.ID]; exists {
		t.Fatalf("task %s should have been removed from assigned list", task.ID)
	}

	pending, exists := leaderBot.PendingTasks[task.ID]
	if !exists {
		t.Fatalf("task %s should be re-queued in pending tasks", task.ID)
	}
	if pending.Status != "pending" {
		t.Fatalf("task %s should be marked pending, got %s", task.ID, pending.Status)
	}
	if pending.RobotID != -1 {
		t.Fatalf("task %s should not be assigned after bot death, got robot %d", task.ID, pending.RobotID)
	}

	if botInfo := leaderBot.KnownBots[2]; botInfo != nil && botInfo.Status != "offline" {
		t.Fatalf("dead bot should be marked offline, got %s", botInfo.Status)
	}

	t.Logf("[TEST] requeue OK: task=%s now pending and bot 2 marked %s", task.ID, leaderBot.KnownBots[2].Status)
}
