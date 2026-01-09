# Praktikum 5: Coordinator Dismissal - Decentralized Service Bot Coordination

## Overview

In this praktikum, we relieve the coordinator of task assignment responsibility. The service robots (cleaner and repair bots) now coordinate themselves autonomously using a **leader election mechanism**. The coordinator only receives status messages via MQTT for monitoring purposes.

## Goals

1. **Decentralized Coordination**: Service bots elect a leader who handles task assignment
2. **Leader Election**: Implement the Bully Algorithm for fault-tolerant leader selection
3. **Robustness**: Handle leader failures gracefully with automatic re-election
4. **Conflict Resolution**: Ensure no conflicts when problems occur simultaneously

## Architecture Changes

### Before (Centralized)

```
┌─────────────┐     ┌─────────────┐     ┌─────────────┐
│  Detector   │────▶│ Coordinator │────▶│ ServiceBot  │
│             │     │  (assigns)  │     │             │
└─────────────┘     └─────────────┘     └─────────────┘
                          │
                          ▼
                    Task Assignment
```

### After (Decentralized)

```
┌─────────────┐                        ┌─────────────┐
│  Detector   │──────MQTT──────────────│  Leader Bot │
│  (reports)  │    events/problems     │  (assigns)  │
└─────────────┘                        └─────────────┘
                                              │
                                              ▼ gRPC
                                       ┌─────────────┐
                                       │ Worker Bots │
                                       └─────────────┘

┌─────────────┐
│ Coordinator │ ← Monitors only (no task assignment)
│ (passive)   │
└─────────────┘
```

## MQTT Topics

### Election Topics

| Topic                           | Purpose                  | Publisher      | Subscribers        |
| ------------------------------- | ------------------------ | -------------- | ------------------ |
| `servicebots/election/request`  | Start election           | Any bot        | All service bots   |
| `servicebots/election/answer`   | Higher-ID bot responding | Higher-ID bots | Election initiator |
| `servicebots/election/announce` | Leader heartbeat         | Leader only    | All service bots   |

### Data Topics

| Topic                              | Purpose                                            |
| ---------------------------------- | -------------------------------------------------- |
| `events/problems`                  | Problem reports from detectors (leader subscribes) |
| `devices/servicebot/{id}/position` | Bot position updates                               |
| `devices/servicebot/{id}/status`   | Bot status (includes gRPC address)                 |

## Bully Algorithm Implementation

### How It Works

The **Bully Algorithm** is a leader election algorithm where the process with the highest ID becomes the coordinator:

1. **Election Trigger**: When a bot detects leader failure (heartbeat timeout) or starts up
2. **Election Request**: Bot sends request to all bots with higher IDs
3. **Election Answer**: Higher-ID bots respond with "I'm alive"
4. **Become Leader**: If no answer received within timeout → become leader
5. **Leader Announcement**: New leader broadcasts announcement (also serves as heartbeat)

### State Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                    Bot Startup                               │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│  Wait 3s for other bots, then Start Election                │
└──────────────────────┬──────────────────────────────────────┘
                       │
         ┌─────────────┴─────────────┐
         │                           │
         ▼                           ▼
┌─────────────────┐        ┌─────────────────┐
│ Received Answer │        │ No Answer       │
│ from Higher ID  │        │ (Timeout 3s)    │
└────────┬────────┘        └────────┬────────┘
         │                          │
         ▼                          ▼
┌─────────────────┐        ┌─────────────────┐
│ Wait for Leader │        │  BECOME LEADER  │
│  Announcement   │        │ - Announce      │
└────────┬────────┘        │ - Start HB      │
         │                 │ - Subscribe     │
         ▼                 │   problems      │
┌─────────────────┐        └────────┬────────┘
│   FOLLOWER      │                 │
│ - Monitor HB    │◀────────────────┘
│ - Execute tasks │
└─────────────────┘
```

### Failure Detection

1. **Heartbeat**: Leader publishes announcement every 1 second
2. **Timeout**: Followers start election if no heartbeat for 4 seconds
3. **Last Will Testament (LWT)**: MQTT broker publishes "offline" if bot disconnects unexpectedly

## Implementation Details

### New Files

- `internal/serviceBots/election.go` - Election state and Bully algorithm implementation
- `internal/serviceBots/election_test.go` - Unit tests for election logic

### Key Structures

```go
// ElectionState holds all election-related state
type ElectionState struct {
    IsLeader        bool
    CurrentLeaderID int
    LastHeartbeat   time.Time
    KnownBots       map[int]*BotInfo    // Registry of all service bots
    PendingTasks    []*TaskInfo         // Queue for unassigned tasks
    ActiveTasks     map[string]*TaskInfo
}

// BotInfo stores information about known service bots
type BotInfo struct {
    ID       int
    Type     string  // "cleaner" or "repair"
    X, Y     int
    Status   string  // "idle", "busy", "offline"
    GRPCAddr string  // For task assignment via gRPC
}
```

### Leader Responsibilities

1. **Subscribe to `events/problems`** - Receive problem reports
2. **Maintain bot registry** - Track all service bot positions/statuses
3. **Task Assignment** - Find nearest idle bot of correct type
4. **Heartbeat** - Publish leadership announcement every second
5. **Handle completions** - Update task status, try pending tasks

### Follower Responsibilities

1. **Monitor heartbeat** - Start election on leader timeout
2. **Execute tasks** - Receive via gRPC, move to location, fix problem
3. **Publish status** - Position and availability for leader's registry
4. **Report completion** - Notify leader (via gRPC) when task done

## Coordinator Changes

The coordinator is now **passive**:

- ✅ HTTP registration endpoints (unchanged)
- ✅ Position tracking via MQTT (unchanged)
- ✅ Live map visualization (unchanged)
- ❌ Task assignment (removed)
- ❌ gRPC callback server (removed)

```go
// cmd/coordinator/main.go - Task assignment removed
func handleEventMessage(topic string, payload []byte) {
    // Track problem for display purposes only
    // NOTE: Task assignment is now handled by the elected leader bot
}
```

## Testing

### Unit Tests (`election_test.go`)

| Test                                | Description                                 |
| ----------------------------------- | ------------------------------------------- |
| `TestElectionStateInitialization`   | Verify state initializes correctly          |
| `TestBotInfoTracking`               | Test bot registry updates                   |
| `TestHigherIDWinsElection`          | Verify Bully algorithm: highest ID wins     |
| `TestClosestBotSelection`           | Test distance-based task assignment         |
| `TestBotTypeFiltering`              | Verify dirt→cleaner, defect→repair matching |
| `TestBusyBotExclusion`              | Busy bots excluded from assignment          |
| `TestConcurrentElectionStateAccess` | Thread safety verification                  |

### Integration Test Scenarios

1. **Normal Election**: Start 3 bots, verify highest ID becomes leader
2. **Leader Failure**: Kill leader, verify re-election within 4s
3. **Simultaneous Problems**: Two problems at same time, both assigned
4. **Worker Failure**: Worker dies mid-task, task re-queued

## Running the System

### Start with Docker Compose

```bash
cd deployments/compose
docker-compose up --build
```

### Expected Logs

```
# Service bot startup
servicebot-1: Starting servicebot in MQTT mode with decentralized coordination
servicebot-1: Bot 2 initialized election system
servicebot-1: Bot 2 starting initial election

# Leader election
servicebot-2: [Election] Bot 3 starting election
servicebot-1: [Election] Bot 2: received election request from bot 3
servicebot-1: [Election] Bot 2: I have higher ID than 3, sending answer
servicebot-2: [Election] Bot 3: received answer from higher-ID bot 2, cancelling my election

# Leadership established
servicebot-1: [Election] Bot 2: I AM THE LEADER NOW!
servicebot-2: [Election] Bot 3: leader is now bot 2

# Task handling
servicebot-1: [Leader] Bot 2: received problem dirt at (5,10) from detector 1
servicebot-1: [Leader] Bot 2: assigning task task-1 to bot 3 for dirt at (5,10)
```

## Robustness Features

### 1. Leader Failure Recovery

- Heartbeat every 1 second
- 4-second timeout triggers re-election
- MQTT LWT provides immediate notification

### 2. Pending Task Queue

- Tasks queued when no suitable bot available
- Automatically assigned when bot becomes idle
- Persists across leader changes (leader subscribes to problem topic)

### 3. Worker Failure Handling

- gRPC timeout on task assignment → task re-queued
- Bot status "offline" via MQTT LWT
- Leader updates registry, excludes from future assignments

## Configuration

### Environment Variables

| Variable      | Default                   | Description                       |
| ------------- | ------------------------- | --------------------------------- |
| `MQTT_BROKER` | `tcp://mosquitto:1884`    | MQTT broker URL                   |
| `COORD_ADDR`  | `http://coordinator:8080` | Coordinator for registration      |
| `WORLD_ADDR`  | `http://world:8081`       | World service for problem cleanup |
| `BOT_TYPE`    | `cleaner`                 | Bot type: `cleaner` or `repair`   |
| `GRPC_PORT`   | `50051`                   | gRPC server port                  |

### Timing Constants

```go
const (
    ElectionTimeout   = 3 * time.Second   // Wait for election answers
    HeartbeatInterval = 1 * time.Second   // Leader heartbeat frequency
    LeaderTimeout     = 4 * time.Second   // Detect leader failure
)
```

## Summary

This praktikum demonstrates:

1. **Decentralized Coordination** - No single point of failure for task assignment
2. **Consensus Building** - Bully algorithm ensures single leader election
3. **Fault Tolerance** - Automatic leader re-election on failure
4. **Message-Oriented Middleware** - MQTT for loose coupling between components

The coordinator is now a passive observer, while service bots self-organize to handle problems autonomously.
