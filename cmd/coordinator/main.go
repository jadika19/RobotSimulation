package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"os"
	"strconv"
	"strings"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/coordinator"
	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
)

func main() {
	addr := ":8080"

	// Allow overriding via environment
	if v := os.Getenv("WORLD_ADDR"); v != "" {
		coordinator.St.WorldAddr = v
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("coordinator listening on", addr)
	log.Println("NOTE: Task assignment is now handled by elected service bot leader")

	// Start MQTT subscriber for position updates only (no task assignment)
	log.Println("Starting in MQTT mode (position tracking only)")
	go startMQTTSubscriber()

	// No gRPC callback server needed - service bots handle this now

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go coordinator.HandleHTTPRequest(conn, coordinator.St)
	}
}

func startMQTTSubscriber() {
	brokerURL := os.Getenv("MQTT_BROKER")
	if brokerURL == "" {
		brokerURL = "tcp://localhost:1884"
	}

	config := mqtt.Config{
		BrokerURL: brokerURL,
		ClientID:  "coordinator",
	}

	client, err := mqtt.NewClient(config)
	if err != nil {
		log.Fatalf("Failed to connect to MQTT broker: %v", err)
	}

	log.Println("MQTT client connected to broker:", brokerURL)

	// Subscribe to position updates from all devices
	err = client.Subscribe("devices/+/+/position", handlePositionMessage)
	if err != nil {
		log.Fatalf("Failed to subscribe to position topics: %v", err)
	}

	// Subscribe to problem events (for tracking/display only - leader handles assignment)
	err = client.Subscribe("events/problems", handleEventMessage)
	if err != nil {
		log.Fatalf("Failed to subscribe to events/problems: %v", err)
	}

	// Subscribe to bot status updates (online/offline)
	err = client.Subscribe("devices/+/+/status", handleStatusMessage)
	if err != nil {
		log.Fatalf("Failed to subscribe to status topics: %v", err)
	}

	// Subscribe to election announcements to track current leader
	err = client.Subscribe("servicebots/election/announce", handleLeaderAnnouncement)
	if err != nil {
		log.Fatalf("Failed to subscribe to election announcements: %v", err)
	}

	// Subscribe to problem solved events to update coordinator view
	err = client.Subscribe("events/problems/solved", handleProblemSolved)
	if err != nil {
		log.Fatalf("Failed to subscribe to problem solved: %v", err)
	}

	log.Println("Subscribed to MQTT topics for position/status tracking (task assignment handled by leader bot)")

	// Keep the subscriber running
	select {}
}

func handlePositionMessage(topic string, payload []byte) {
	var pos mqtt.PositionMessage
	if err := json.Unmarshal(payload, &pos); err != nil {
		log.Printf("Failed to unmarshal position message: %v", err)
		return
	}

	// Clamp coordinates to grid bounds
	x := clamp(pos.X, 0, coordinator.St.Width-1)
	y := clamp(pos.Y, 0, coordinator.St.Height-1)

	// Update robot position in state
	coordinator.St.Mu.Lock()
	if rb, ok := coordinator.St.Robots[pos.ID]; ok {
		rb.X = x
		rb.Y = y
		coordinator.St.Robots[pos.ID] = rb
		fmt.Printf("Robot %d moved to (%d, %d)\n", pos.ID, x, y)
	}
	coordinator.St.Mu.Unlock()
}

func handleEventMessage(topic string, payload []byte) {
	var event mqtt.EventMessage
	if err := json.Unmarshal(payload, &event); err != nil {
		log.Printf("Failed to unmarshal event message: %v", err)
		return
	}

	log.Printf("problem reported: %s at (%d,%d) by detector %d (leader bot will handle assignment)",
		event.Type, event.X, event.Y, event.DetectorID)

	// Track problem for display purposes only
	key := coordKey(event.X, event.Y)
	coordinator.St.Mu.Lock()
	if _, exists := coordinator.St.KnownProblems[key]; !exists {
		coordinator.St.KnownProblems[key] = coordinator.Problem{X: event.X, Y: event.Y, Type: event.Type}
	}
	coordinator.St.Mu.Unlock()

	// NOTE: Task assignment is now handled by the elected leader bot
}

func handleStatusMessage(topic string, payload []byte) {
	status := strings.TrimSpace(string(payload))

	// Parse topic to extract device type and ID: devices/{type}/{id}/status
	parts := strings.Split(topic, "/")
	if len(parts) != 4 {
		log.Printf("Invalid status topic format: %s", topic)
		return
	}

	deviceType := parts[1] // "detector", "servicebot"
	deviceID, err := strconv.Atoi(parts[2])
	if err != nil {
		log.Printf("Invalid device ID in topic %s: %v", topic, err)
		return
	}

	coordinator.St.Mu.Lock()
	if rb, ok := coordinator.St.Robots[deviceID]; ok {
		if status == "offline" {
			log.Printf("Robot %d (%s) went offline", deviceID, deviceType)
			// Mark as offline but keep in map for now
			// Could implement cleanup logic here if needed
		} else if status == "online" {
			log.Printf("Robot %d (%s) is online", deviceID, deviceType)
		}
		// Could update robot status field here if needed
		coordinator.St.Robots[deviceID] = rb
	}
	coordinator.St.Mu.Unlock()
}

func clamp(val, min, max int) int {
	if val < min {
		return min
	} else if val > max {
		return max
	}
	return val
}

func coordKey(x, y int) string {
	return fmt.Sprintf("%d,%d", x, y)
}

func handleProblemSolved(topic string, payload []byte) {
	var msg struct {
		X int `json:"x"`
		Y int `json:"y"`
	}
	if err := json.Unmarshal(payload, &msg); err != nil {
		log.Printf("Failed to unmarshal problem solved message: %v", err)
		return
	}

	key := coordKey(msg.X, msg.Y)
	coordinator.St.Mu.Lock()
	if _, exists := coordinator.St.KnownProblems[key]; exists {
		delete(coordinator.St.KnownProblems, key)
		log.Printf("Problem at (%d,%d) solved and removed from coordinator view", msg.X, msg.Y)
	}
	coordinator.St.Mu.Unlock()
}

func handleLeaderAnnouncement(topic string, payload []byte) {
	var announcement struct {
		LeaderID  int    `json:"leaderId"`
		Timestamp string `json:"timestamp"`
	}
	if err := json.Unmarshal(payload, &announcement); err != nil {
		return
	}

	coordinator.St.Mu.Lock()
	previousLeader := coordinator.St.LeaderID
	coordinator.St.LeaderID = announcement.LeaderID
	coordinator.St.Mu.Unlock()

	if announcement.LeaderID != previousLeader {
		log.Printf("Service bot leader changed: bot %d is now the leader", announcement.LeaderID)
	}
}
