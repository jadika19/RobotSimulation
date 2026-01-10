# Bully-Algorithmus Implementation für Service-Roboter

## Übersicht

Dieses Dokument beschreibt die Implementierung des Bully-Algorithmus für dezentrale Koordination von Service-Robotern (Cleaner und Repair Bots). Die Service-Bots wählen automatisch einen Leader, der die Task-Zuweisung übernimmt.

## Architektur

### Komponenten-Rollen

| Komponente | Rolle | Verantwortlichkeiten |
|------------|-------|---------------------|
| **Detector-Bots** | Problem-Scanner | Scannen Grid, publizieren neue Tasks via `tasks/new` |
| **Service-Bots** | Worker + Coordinator | Führen Tasks aus, partizipieren an Leader-Election |
| **Leader-Bot** | Aktiver Coordinator | Weist Tasks zu, trackt Bot-Status, verwaltet Event-Log |
| **Coordinator** | Passiver Observer | Bot-Registration, Monitoring, Live-Map-Visualisierung |

### Neue Kommunikationsflüsse

```
Detector → MQTT (tasks/new) → Leader → gRPC (AssignTask) → Service-Bot
         → MQTT (events/problems) → Coordinator (Monitoring)

Service-Bot → MQTT (bots/metadata/{id}) → Leader (State Discovery)
           → MQTT (tasks/events) → Leader + Coordinator (Event Sourcing)

Leader → MQTT (election/heartbeat) → Followers (Leader Presence)
```

## Bully-Algorithmus Details

### Election States

- **FOLLOWER**: Wartet auf Heartbeats vom Leader
- **CANDIDATE**: Startet Election, wartet auf Antworten
- **LEADER**: Sendet Heartbeats, weist Tasks zu

### Election Flow

1. **Timeout**: Follower empfängt 6s keine Heartbeats → startet Election
2. **ELECTION Message**: Candidate published `election/election/{candidateID}`
3. **Higher-ID Bots**: Antworten mit ANSWER, starten eigene Election
4. **No ANSWER**: Nach 3s ohne Antwort → Candidate wird Leader
5. **VICTORY**: Leader announced via `election/victory/{leaderID}`
6. **Heartbeats**: Leader sendet alle 2s `election/heartbeat`

### Term Numbers

Jede Election erhöht den Term-Counter. Bots akzeptieren nur Messages mit aktuellem/höherem Term. Verhindert Race Conditions bei simultanen Elections.

## MQTT-Topics

### Election Topics

| Topic | Publisher | Payload | Zweck |
|-------|-----------|---------|-------|
| `election/heartbeat` | Leader | `{leaderId, term, timestamp}` | Leader Presence |
| `election/election/{id}` | Candidate | `{candidateId, term, timestamp}` | Start Election |
| `election/answer/{id}` | Higher Bot | `{respondingId, toCandidate, term}` | Election Response |
| `election/victory/{id}` | New Leader | `{leaderId, term, timestamp}` | Leader Announcement |

### Task Management Topics

| Topic | Publisher | Payload | Zweck |
|-------|-----------|---------|-------|
| `tasks/new` | Detectors | `{taskId, x, y, type, timestamp}` | New Task Discovery |
| `tasks/events` | Leader | `{taskId, robotId, eventType, leaderId, term}` | Event Sourcing (retained) |
| `bots/metadata/{id}` | Service-Bots | `{id, type, grpcAddr, x, y, status, taskId, term}` | State Discovery (retained) |

## Leader Task-Assignment

### Manhattan Distance Algorithm

Leader verwendet denselben Algorithmus wie alter Coordinator:

```go
distance := abs(botX - taskX) + abs(botY - taskY)
bestBot := bot mit kleinstem distance UND status == "idle" UND type == requiredType
```

### Task Lifecycle

1. **Pending**: Task wartet auf idle Bot
2. **Assigned**: Leader weist via gRPC zu, startet Timeout (30s)
3. **Completed**: Bot meldet Erfolg via `tasks/events`
4. **Timeout**: Nach 30s ohne Completion → Reassignment
5. **Failed**: gRPC-Fehler → zurück zu Pending

### Event Sourcing

Alle Task-Zuweisungen werden als MQTT-Events (QoS 1, retained) published:

```json
{
  "taskId": "task-1-5-3",
  "robotId": 4,
  "eventType": "assigned",  // or "completed", "failed", "timeout"
  "leaderId": 4,
  "term": 2,
  "timestamp": "2026-01-09T14:23:45Z"
}
```

Neuer Leader rekonstruiert State aus Event-Log.

## State Recovery nach Leader-Crash

1. **Election**: Follower wählt neuen Leader (höchste ID)
2. **Metadata Request**: Neuer Leader empfängt retained `bots/metadata/*`
3. **Event Log Replay**: Leader liest `tasks/events` (retained)
4. **Task Timeout**: Timed-out Tasks werden reassigned

## Robustheit-Features

### Bot-Ausfall während Task

- Task-Timeout (30s) erkennt crashed Bots
- Leader reassignt Task an anderen Bot
- MQTT Last Will Testament markiert Bot als "offline"

### Leader-Ausfall

- Follower timeout (6s) triggert neue Election
- Höchste verbleibende ID wird neuer Leader
- State-Recovery via retained Messages

### Simultane Elections

- Term-Counter verhindert Konflikte
- Höhere ID gewinnt automatisch (Bully-Prinzip)
- ANSWER-Messages stoppen niedrigere Candidates

### Network-Partition

- **Split-Brain möglich**: Zwei Leader in getrennten Netzwerken
- **Auflösung**: Nach Partition-Heal sehen beide gegenseitig Heartbeats
- Niedriger-ID-Leader steppt down (empfängt HEARTBEAT von höherer ID)

## Konfiguration

### Timeouts

```go
ElectionTimeoutMin = 5s   // Minimum Wartezeit für Election
ElectionTimeoutMax = 10s  // Maximum (randomisiert)
HeartbeatInterval  = 2s   // Leader Heartbeat Frequenz
HeartbeatDeadline  = 6s   // Follower Timeout (3x Heartbeat)
AnswerWaitTime     = 3s   // Candidate wartet auf ANSWER
TaskTimeout        = 30s  // Task Reassignment Timeout
```

### Environment Variables

Keine neuen Environment-Variables nötig. Bestehende bleiben:

- `BOT_TYPE`: "cleaner" oder "repair"
- `MQTT_BROKER`: MQTT Broker URL
- `COORD_ADDR`: Coordinator HTTP-Endpoint (nur für Registration)
- `COORD_GRPC`: Coordinator gRPC (nur für Monitoring-Callbacks)
- `GRPC_PORT`: Bot's gRPC-Port (default: 50051)

## Testing

### Manuelle Tests

**Test 1: Normal Election**
```bash
docker compose up
# Erwarte: Höchste Bot-ID wird Leader, sendet Heartbeats
# Check Logs: "[ELECTION] Bot X CANDIDATE → LEADER"
```

**Test 2: Leader Crash**
```bash
docker compose up
# Warte bis Leader gewählt
docker compose stop cleaner  # Stoppe Leader-Container
# Erwarte: Neue Election, zweithöchste ID wird Leader
```

**Test 3: Task Assignment**
```bash
docker compose up
# Warte bis Leader gewählt
# Detektoren melden Tasks
# Erwarte: Leader weist Tasks zu via gRPC
# Check Logs: "[LEADER] Bot X assigning task Y to bot Z"
```

**Test 4: Simultane Probleme**
```bash
# Mehrere Detektoren melden gleichzeitig
# Erwarte: Keine doppelten Zuweisungen
# Check: tasks/events für TaskID-Duplikate
```

### Unit Tests (TODO)

Siehe `internal/serviceBots/election_test.go`:
- `TestElectionTimeout`
- `TestHigherIDRespondsToElection`
- `TestCandidateReceivesAnswer`
- `TestLeaderSendsHeartbeats`

### Integration Tests (TODO)

Siehe `internal/serviceBots/election_integration_test.go`:
- `TestTwoBotsElection`
- `TestLeaderCrashRecovery`
- `TestTaskAssignment`

## Monitoring

### Logs

**Leader Election:**
```
[ELECTION] Bot 4 FOLLOWER → CANDIDATE, term=1
[ELECTION] Bot 4 sent ELECTION message, term=1
[ELECTION] Bot 4 CANDIDATE → LEADER, term=1
```

**Task Assignment:**
```
[LEADER] Bot 4 received new task: task-1-5-3 at (5,3) type=dirt
[LEADER] Bot 4 assigning task task-1-5-3 to bot 2 (distance=7)
[TASK] Event: assigned - task=task-1-5-3 robot=2 leader=4
```

**Coordinator (Observer Mode):**
```
Coordinator now operating in PASSIVE OBSERVER mode - Leader handles task assignment
[ELECTION] Current leader: Bot 4, term=1
[TASK] Event: completed - task=task-1-5-3 robot=2 leader=4
```

### Coordinator Live-Map

Live-Map zeigt weiterhin:
- Bot-Positionen (via MQTT position updates)
- Problem-Locations (via MQTT events/problems)
- **Neu**: Leader-Status (via election/heartbeat)

## Bekannte Limitierungen

1. **Split-Brain**: Network-Partition kann temporär zwei Leader erzeugen
   - Auflösung: Automatisch nach Partition-Heal (niedriger ID steppt down)
   
2. **Event-Log-Größe**: `tasks/events` wächst unbegrenzt
   - Mitigation: Periodisches Cleanup alter Events (nicht implementiert)

3. **Stateless Leader**: Kein persistenter State bei Leader-Restart
   - Mitigation: Event-Sourcing + Retained Messages rekonstruieren State

4. **Globaler Leader**: Ein Leader für beide Bot-Typen (cleaner + repair)
   - Alternative: Separate Leaders pro Typ (nicht implementiert)

## Vergleich: Alt vs. Neu

| Feature | Alte Architektur (Praktikum 4) | Neue Architektur (Praktikum 5) |
|---------|--------------------------------|--------------------------------|
| Task-Assignment | Zentraler Coordinator (Single Point of Failure) | Dezentraler Leader (Fault-Tolerant) |
| Bot-Discovery | HTTP-Registration beim Coordinator | MQTT Retained Metadata |
| Task-Discovery | Detector → MQTT → Coordinator | Detector → MQTT → Leader |
| State-Management | In-Memory im Coordinator | Event-Sourcing via MQTT |
| Failure-Recovery | Manuell (Coordinator-Restart verliert State) | Automatisch (Leader-Election + State-Replay) |
| Scalability | Coordinator-Bottleneck | Leader kann Tasks parallel verwalten |

## Erweiterungsmöglichkeiten

1. **Separate Leaders pro Bot-Typ**: `election/cleaner/*` und `election/repair/*`
2. **Raft-Konsens**: Statt Bully → garantiert ein Leader zur Zeit
3. **Load-Balancing**: Mehrere Leader für verschiedene Grid-Regionen
4. **Persistent Event-Log**: MQTT-Broker mit Persistence-Backend
5. **Task-Prioritäten**: High-Priority Tasks bevorzugt zuweisen
6. **Bot-Health-Monitoring**: Leader tracked Bot-Metriken (CPU, Batterie)

## Implementierte Dateien

### Neue Dateien
- `internal/serviceBots/election.go` - Bully-Algorithmus Kern-Logik
- `internal/serviceBots/task_assignment.go` - Leader Task-Management
- `BULLY_ALGORITHM.md` - Dieses Dokument

### Modifizierte Dateien
- `internal/mqtt/client.go` - Neue Message-Typen, PublishRetained
- `internal/serviceBots/servicebot.go` - Election-State, Metadata-Publishing
- `internal/detector/detector.go` - Tasks via MQTT publishen
- `cmd/servicebot/main.go` - StartElectionLoop, StartTaskManagement
- `cmd/coordinator/main.go` - Passiver Observer-Modus

---

**Status**: ✅ Implementierung komplett, bereit für Tests
**Datum**: 9. Januar 2026
**Autor**: GitHub Copilot (Claude Sonnet 4.5)
