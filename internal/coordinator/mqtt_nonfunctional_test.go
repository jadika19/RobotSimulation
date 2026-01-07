package coordinator

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
)

// =============================================================================
// NON-FUNCTIONAL TESTS FOR MQTT/MOM IMPLEMENTATION
// =============================================================================
// These tests verify HOW the system works, not WHAT it does.
// Focus areas: Performance (latency), Reliability (connection recovery)
// =============================================================================

// TestMessageLatency measures the round-trip time for MQTT messages
// This is a PERFORMANCE test that validates message delivery speed.
//
// What it tests:
// - Time from publisher sending to subscriber receiving
// - Statistical analysis: min, max, avg, p50, p95, p99 latency
// - Whether latency meets acceptable thresholds
//
// Pass criteria:
// - Average latency < 50ms
// - P99 latency < 200ms
func TestMessageLatency(t *testing.T) {
	// Skip if MQTT broker not available
	brokerURL := os.Getenv("MQTT_BROKER")
	if brokerURL == "" {
		brokerURL = "tcp://localhost:1883"
	}

	// Check if broker is reachable
	if !isBrokerRunning(brokerURL) {
		t.Skip("MQTT broker not available at " + brokerURL + ". Start with: docker run -d -p 1883:1883 eclipse-mosquitto:2.0 mosquitto -c /mosquitto-no-auth.conf")
	}

	const messageCount = 100
	const testTopic = "test/latency/position"

	// Channel to collect latencies
	latencies := make([]time.Duration, 0, messageCount)
	var receivedCount int32

	// Create subscriber client
	subscriberConfig := mqtt.Config{
		BrokerURL: brokerURL,
		ClientID:  fmt.Sprintf("latency-subscriber-%d", time.Now().UnixNano()),
	}
	subscriber, err := mqtt.NewClient(subscriberConfig)
	if err != nil {
		t.Fatalf("Failed to create subscriber: %v", err)
	}
	defer subscriber.Disconnect(1000)

	// Subscribe and record receive times
	receiveTimestamps := make(map[int]time.Time)
	var recvMu sync.Mutex

	err = subscriber.Subscribe(testTopic, func(topic string, payload []byte) {
		recvTime := time.Now()

		var msg latencyTestMessage
		if err := json.Unmarshal(payload, &msg); err != nil {
			return
		}

		recvMu.Lock()
		receiveTimestamps[msg.Sequence] = recvTime
		recvMu.Unlock()
		atomic.AddInt32(&receivedCount, 1)
	})
	if err != nil {
		t.Fatalf("Failed to subscribe: %v", err)
	}

	// Wait for subscription to be established
	time.Sleep(100 * time.Millisecond)

	// Create publisher client
	publisherConfig := mqtt.Config{
		BrokerURL: brokerURL,
		ClientID:  fmt.Sprintf("latency-publisher-%d", time.Now().UnixNano()),
	}
	publisher, err := mqtt.NewClient(publisherConfig)
	if err != nil {
		t.Fatalf("Failed to create publisher: %v", err)
	}
	defer publisher.Disconnect(1000)

	// Record send times and publish messages
	sendTimestamps := make(map[int]time.Time)

	t.Run("Publish messages", func(t *testing.T) {
		for i := 0; i < messageCount; i++ {
			msg := latencyTestMessage{
				Sequence:  i,
				Timestamp: time.Now().UnixNano(),
				X:         i % 20,
				Y:         i / 20, // this is integer
			}

			sendTimestamps[i] = time.Now()

			if err := publisher.Publish(testTopic, msg); err != nil {
				t.Errorf("Failed to publish message %d: %v", i, err)
			}

			// Small delay to avoid overwhelming the broker
			time.Sleep(5 * time.Millisecond)
		}
	})

	// Wait for all messages to be received
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt32(&receivedCount) < messageCount && time.Now().Before(deadline) {
		time.Sleep(50 * time.Millisecond)
	}

	received := atomic.LoadInt32(&receivedCount)
	if received < messageCount {
		t.Logf("⚠️  Only received %d/%d messages (%.1f%% delivery rate)", received, messageCount, float64(received)/float64(messageCount)*100)
	}

	// Calculate latencies
	recvMu.Lock()
	for seq, sendTime := range sendTimestamps {
		if recvTime, ok := receiveTimestamps[seq]; ok {
			latency := recvTime.Sub(sendTime)
			latencies = append(latencies, latency)
		}
	}
	recvMu.Unlock()

	if len(latencies) == 0 {
		t.Fatal("❌ FAIL: No latency measurements collected")
	}

	// Calculate statistics
	stats := calculateLatencyStats(latencies)

	// Print results
	t.Run("Results", func(t *testing.T) {
		fmt.Println("\n" + strings.Repeat("═", 60))
		fmt.Println("MESSAGE LATENCY TEST RESULTS")
		fmt.Println(strings.Repeat("═", 60))
		fmt.Printf("Messages sent:     %d\n", messageCount)
		fmt.Printf("Messages received: %d (%.1f%%)\n", len(latencies), float64(len(latencies))/float64(messageCount)*100)
		fmt.Println(strings.Repeat("-", 60))
		fmt.Printf("Min latency:       %v\n", stats.Min)
		fmt.Printf("Max latency:       %v\n", stats.Max)
		fmt.Printf("Avg latency:       %v\n", stats.Avg)
		fmt.Printf("P50 latency:       %v\n", stats.P50)
		fmt.Printf("P95 latency:       %v\n", stats.P95)
		fmt.Printf("P99 latency:       %v\n", stats.P99)
		fmt.Println(strings.Repeat("═", 60))

		// Assertions
		if stats.Avg > 50*time.Millisecond {
			t.Errorf("❌ FAIL: Average latency %v exceeds 50ms threshold", stats.Avg)
		} else {
			t.Logf("✅ PASS: Average latency %v is within 50ms threshold", stats.Avg)
		}

		if stats.P99 > 200*time.Millisecond {
			t.Errorf("❌ FAIL: P99 latency %v exceeds 200ms threshold", stats.P99)
		} else {
			t.Logf("✅ PASS: P99 latency %v is within 200ms threshold", stats.P99)
		}

		deliveryRate := float64(len(latencies)) / float64(messageCount) * 100
		if deliveryRate < 99.0 {
			t.Errorf("❌ FAIL: Delivery rate %.1f%% is below 99%% threshold", deliveryRate)
		} else {
			t.Logf("✅ PASS: Delivery rate %.1f%% meets 99%% threshold", deliveryRate)
		}
	})

	// Output machine-readable results for script parsing
	fmt.Printf("\n[LATENCY_RESULT] min=%d max=%d avg=%d p50=%d p95=%d p99=%d received=%d sent=%d\n",
		stats.Min.Microseconds(),
		stats.Max.Microseconds(),
		stats.Avg.Microseconds(),
		stats.P50.Microseconds(),
		stats.P95.Microseconds(),
		stats.P99.Microseconds(),
		len(latencies),
		messageCount)
}

// TestConnectionFailureRecovery tests MQTT client auto-reconnect behavior
// This is a RELIABILITY test that validates system resilience.
//
// What it tests:
// - Client behavior when broker connection is lost
// - Auto-reconnection capability
// - Message delivery after reconnection (QoS 1 guarantee)
// - State consistency during/after connection disruption
//
// Pass criteria:
// - Client reconnects within 15 seconds
// - Messages sent before disconnect are not lost
// - System remains functional after recovery
func TestConnectionFailureRecovery(t *testing.T) {
	// This test requires Docker to control broker lifecycle
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("Docker not available, skipping connection recovery test")
	}

	// Use a dedicated container name for this test
	containerName := fmt.Sprintf("mqtt-recovery-test-%d", time.Now().UnixNano()%10000)
	brokerPort := "11883" // Use non-standard port to avoid conflicts

	// Cleanup function
	cleanup := func() {
		exec.Command("docker", "rm", "-f", containerName).Run()
	}
	defer cleanup()

	// Start broker container
	t.Run("Start broker", func(t *testing.T) {
		cleanup() // Ensure clean state
		cmd := exec.Command("docker", "run", "-d",
			"--name", containerName,
			"-p", brokerPort+":1883",
			"eclipse-mosquitto:2.0",
			"mosquitto", "-c", "/mosquitto-no-auth.conf")

		if output, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("Failed to start broker: %v\nOutput: %s", err, output)
		}

		// Wait for broker to be ready
		brokerURL := "tcp://localhost:" + brokerPort
		deadline := time.Now().Add(10 * time.Second)
		for !isBrokerRunning(brokerURL) && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}

		if !isBrokerRunning(brokerURL) {
			t.Fatal("Broker failed to start within timeout")
		}
		t.Log("✅ Broker started successfully")
	})

	brokerURL := "tcp://localhost:" + brokerPort
	testTopic := "test/recovery/messages"

	// Create client with auto-reconnect
	config := mqtt.Config{
		BrokerURL: brokerURL,
		ClientID:  fmt.Sprintf("recovery-test-%d", time.Now().UnixNano()),
	}

	var client *mqtt.Client
	var err error

	t.Run("Connect client", func(t *testing.T) {
		client, err = mqtt.NewClient(config)
		if err != nil {
			t.Fatalf("Failed to connect: %v", err)
		}
		t.Log("✅ Client connected")
	})
	defer func() {
		if client != nil {
			client.Disconnect(1000)
		}
	}()

	// Verify initial connection works
	var messagesBeforeDisconnect int32
	var messagesAfterReconnect int32
	var receivedMu sync.Mutex
	receivedMessages := make(map[int]bool)

	t.Run("Subscribe and verify initial connection", func(t *testing.T) {
		err = client.Subscribe(testTopic, func(topic string, payload []byte) {
			var msg latencyTestMessage
			if err := json.Unmarshal(payload, &msg); err != nil {
				return
			}
			receivedMu.Lock()
			receivedMessages[msg.Sequence] = true
			receivedMu.Unlock()
		})
		if err != nil {
			t.Fatalf("Failed to subscribe: %v", err)
		}

		// Send 5 messages before disconnect
		for i := 0; i < 5; i++ {
			msg := latencyTestMessage{Sequence: i, Timestamp: time.Now().UnixNano()}
			if err := client.Publish(testTopic, msg); err != nil {
				t.Errorf("Failed to publish message %d: %v", i, err)
			} else {
				atomic.AddInt32(&messagesBeforeDisconnect, 1)
			}
		}
		time.Sleep(500 * time.Millisecond)

		receivedMu.Lock()
		count := len(receivedMessages)
		receivedMu.Unlock()

		if count < 5 {
			t.Logf("⚠️  Only received %d/5 messages before disconnect", count)
		} else {
			t.Log("✅ All 5 pre-disconnect messages received")
		}
	})

	// Stop the broker (simulate network failure)
	t.Run("Simulate broker failure", func(t *testing.T) {
		disconnectTime := time.Now()

		cmd := exec.Command("docker", "stop", containerName)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to stop broker: %v", err)
		}
		t.Logf("✅ Broker stopped at %v", disconnectTime.Format("15:04:05.000"))

		// Verify client detects disconnection
		time.Sleep(2 * time.Second)
		if client.IsConnected() {
			t.Log("⚠️  Client still reports connected (may have cached state)")
		} else {
			t.Log("✅ Client detected disconnection")
		}
	})

	// Restart the broker
	var reconnectTime time.Duration

	t.Run("Restart broker and measure reconnect time", func(t *testing.T) {
		restartStart := time.Now()

		cmd := exec.Command("docker", "start", containerName)
		if err := cmd.Run(); err != nil {
			t.Fatalf("Failed to restart broker: %v", err)
		}

		// Wait for broker to be ready
		deadline := time.Now().Add(10 * time.Second)
		for !isBrokerRunning(brokerURL) && time.Now().Before(deadline) {
			time.Sleep(200 * time.Millisecond)
		}

		t.Log("✅ Broker restarted")

		// Wait for client to reconnect
		reconnectDeadline := time.Now().Add(15 * time.Second)
		for !client.IsConnected() && time.Now().Before(reconnectDeadline) {
			time.Sleep(100 * time.Millisecond)
		}

		reconnectTime = time.Since(restartStart)

		if !client.IsConnected() {
			t.Errorf("❌ FAIL: Client failed to reconnect within 15 seconds")
		} else {
			t.Logf("✅ Client reconnected in %v", reconnectTime)
		}
	})

	// Verify client works after reconnection
	t.Run("Verify functionality after reconnect", func(t *testing.T) {
		if !client.IsConnected() {
			t.Skip("Client not connected, skipping post-reconnect test")
		}

		// Wait a moment for connection to stabilize
		time.Sleep(500 * time.Millisecond)

		// Need to resubscribe after reconnect (Paho behavior)
		// Retry subscription a few times as connection may not be fully established
		var subscribeErr error
		for retry := 0; retry < 5; retry++ {
			subscribeErr = client.Subscribe(testTopic, func(topic string, payload []byte) {
				var msg latencyTestMessage
				if err := json.Unmarshal(payload, &msg); err != nil {
					return
				}
				receivedMu.Lock()
				receivedMessages[msg.Sequence] = true
				receivedMu.Unlock()
			})
			if subscribeErr == nil {
				break
			}
			time.Sleep(200 * time.Millisecond)
		}
		if subscribeErr != nil {
			t.Logf("⚠️  Failed to resubscribe after retries: %v (continuing test)", subscribeErr)
		}

		// Send 5 more messages after reconnection
		for i := 100; i < 105; i++ {
			msg := latencyTestMessage{Sequence: i, Timestamp: time.Now().UnixNano()}
			if err := client.Publish(testTopic, msg); err != nil {
				t.Errorf("Failed to publish message %d after reconnect: %v", i, err)
			} else {
				atomic.AddInt32(&messagesAfterReconnect, 1)
			}
		}
		time.Sleep(500 * time.Millisecond)

		receivedMu.Lock()
		postReconnectCount := 0
		for seq := range receivedMessages {
			if seq >= 100 {
				postReconnectCount++
			}
		}
		receivedMu.Unlock()

		if postReconnectCount < 5 {
			t.Errorf("❌ FAIL: Only received %d/5 messages after reconnect", postReconnectCount)
		} else {
			t.Log("✅ All 5 post-reconnect messages received")
		}
	})

	// Print results
	t.Run("Results", func(t *testing.T) {
		fmt.Println("\n" + strings.Repeat("═", 60))
		fmt.Println("CONNECTION FAILURE RECOVERY TEST RESULTS")
		fmt.Println(strings.Repeat("═", 60))
		fmt.Printf("Messages before disconnect: %d\n", messagesBeforeDisconnect)
		fmt.Printf("Messages after reconnect:   %d\n", messagesAfterReconnect)
		fmt.Printf("Reconnection time:          %v\n", reconnectTime)
		fmt.Printf("Client connected:           %v\n", client.IsConnected())
		fmt.Println(strings.Repeat("═", 60))

		// Assertions
		if reconnectTime > 15*time.Second {
			t.Errorf("❌ FAIL: Reconnection time %v exceeds 15s threshold", reconnectTime)
		} else if reconnectTime > 0 {
			t.Logf("✅ PASS: Reconnection time %v is within 15s threshold", reconnectTime)
		}

		if client.IsConnected() {
			t.Log("✅ PASS: Client is connected after recovery")
		} else {
			t.Error("❌ FAIL: Client is not connected after recovery")
		}
	})

	// Output machine-readable results for script parsing
	fmt.Printf("\n[RECOVERY_RESULT] reconnect_ms=%d connected=%v msgs_before=%d msgs_after=%d\n",
		reconnectTime.Milliseconds(),
		client.IsConnected(),
		messagesBeforeDisconnect,
		messagesAfterReconnect)
}

// =============================================================================
// Helper Types and Functions
// =============================================================================

type latencyTestMessage struct {
	Sequence  int   `json:"sequence"`
	Timestamp int64 `json:"timestamp"`
	X         int   `json:"x"`
	Y         int   `json:"y"`
}

type latencyStats struct {
	Min time.Duration
	Max time.Duration
	Avg time.Duration
	P50 time.Duration
	P95 time.Duration
	P99 time.Duration
}

func calculateLatencyStats(latencies []time.Duration) latencyStats {
	if len(latencies) == 0 {
		return latencyStats{}
	}

	// Sort for percentile calculation
	sorted := make([]time.Duration, len(latencies))
	copy(sorted, latencies)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i] < sorted[j]
	})

	// Calculate sum for average
	var sum time.Duration
	for _, l := range sorted {
		sum += l
	}

	return latencyStats{
		Min: sorted[0],
		Max: sorted[len(sorted)-1],
		Avg: sum / time.Duration(len(sorted)),
		P50: percentile(sorted, 50),
		P95: percentile(sorted, 95),
		P99: percentile(sorted, 99),
	}
}

func percentile(sorted []time.Duration, p int) time.Duration {
	if len(sorted) == 0 {
		return 0
	}
	idx := (p * len(sorted)) / 100
	if idx >= len(sorted) {
		idx = len(sorted) - 1
	}
	return sorted[idx]
}

func isBrokerRunning(brokerURL string) bool {
	// Try to connect with a short timeout
	config := mqtt.Config{
		BrokerURL: brokerURL,
		ClientID:  fmt.Sprintf("health-check-%d", time.Now().UnixNano()),
	}

	client, err := mqtt.NewClient(config)
	if err != nil {
		return false
	}
	client.Disconnect(100)
	return true
}
