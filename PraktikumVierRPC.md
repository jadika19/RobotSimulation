# Praktikum Vier – RPC & Auftragsvergabe

## Zielsetzung / Plan

1. **Weltzustand trennen**: Welt-Service hält Ground-Truth-Probleme; Koordinator kennt nur gemeldete Probleme (Detected).
2. **Problemerkennung**: Detektoren laufen über das Grid, fragen die Welt nach Problemen und melden Funde an den Koordinator (`POST /event`).
3. **Auftragsvergabe per RPC**: Koordinator wählt passenden Service-Bot (Cleaner/Repair) und weist ihn via gRPC an (`AssignTask`).
4. **Bot-Lifecycle**: Service-Bot wechselt von IDLE → BUSY, bewegt sich zum Problem, "repariert"/reinigt, meldet Abschluss via gRPC (`ReportCompletion`), geht wieder auf IDLE (bleibt am Ziel-Ort).
5. **Synchronisation der Zustände**: Nach Abschluss Problem aus Koordinator (`KnownProblems`) und Welt (`Problems`) entfernen; Map/UI zeigen Aufgabenstatus.

## Implementierung

### Dienste & Ports

- **World Service** (HTTP 8081): `/world-map`, `/problem`, `/problem-at`, `/problem/delete`, `/live-map`
- **Coordinator** (HTTP 8080, UDP 9001, gRPC 9002): `/map`, `/event`, `/robot`, `/cleaner-robot`, `/repair-robot`, TaskCallback gRPC
- **Service-Bots** (per-Container gRPC, default 50051): TaskService gRPC Server
- **Detector**: REST + UDP Client (fragt Welt, meldet an Koordinator)

### Datenstrukturen (Koordinator)

- `Robots`: ID, Position, Type (`detector|cleaner|repair`), Status (`idle|busy`), `GRPCAddr`, `CurrentTaskID`
- `KnownProblems`: nur gemeldete Probleme (Key: "x,y")
- `Task` (implizit): TaskID, Problemtyp, Koordinate, zugewiesener Bot

### gRPC Schnittstellen (`proto/task.proto`)

- **TaskService.AssignTask(TaskRequest)** → TaskResponse
  - Request: `task_id`, `x`, `y`, `problem_type`
  - Response: `accepted`, `message`
- **TaskCallbackService.ReportCompletion(CompletionRequest)** → CompletionResponse
  - Request: `task_id`, `robot_id`, `success`
  - Response: `acknowledged`

### Ablauf (Happy Path)

1. User platziert Problem im World-Service (`POST /problem`).
2. Detector läuft, fragt Welt (`POST /problem-at`), findet Problem, meldet an Koordinator (`POST /event`).
3. Koordinator legt KnownProblem an, wählt passenden Service-Bot:
   - Typfilter (Cleaner für `dirt`, Repair für `defect`).
   - Nur `idle` Bots.
   - Nächstes via Tehran-Distanz |Xa–Xb| + |Ya–Yb| (Tie-Break: kleinere ID).
4. Koordinator ruft gRPC `AssignTask` beim Bot auf, markiert Bot `busy`, speichert TaskID.
5. Service-Bot bewegt sich schrittweise zum Ziel, sendet UDP-Positionsupdates, simuliert Fix (2s), meldet per `ReportCompletion` zurück, setzt Status `idle` **am Ziel-Ort**.
6. Koordinator löscht Problem aus `KnownProblems` und ruft World `/problem/delete` auf.
7. UI (Live-Map) zeigt Bots (beide Grids), Probleme: links Ground Truth, rechts nur entdeckte.

### Wichtige Details / Designentscheidungen

- Welt ist separat: Koordinator erfährt Probleme nur via Detektor-Report, nicht direkt vom User.
- Bots bleiben nach Erledigung am Ziel (kein Zurück zum Ursprung), um unnötige Bewegung zu sparen.
- CORS für Koordinator-HTTP aktiviert (damit Live-Map im Browser von World-Service abrufen kann).
- Problem-De-Duplikation: KnownProblems keyed by Koordinate; erneute Reports überschreiben nicht zugewiesene Tasks.
- Timeout/Retry (TODO): Für Robustheit könnte ein Task-Timeout hinzugefügt werden, um hängende Bots neu zuzuweisen.

### Endpunkte (Kurzfassung)

- **World**: `GET /world-map`, `POST /problem`, `POST /problem-at`, `DELETE /problem`, `GET /live-map`
- **Coordinator**: `GET /map`, `POST /event`, `POST /robot`, `POST /cleaner-robot`, `POST /repair-robot`, UDP `9001`, gRPC `9002`
- **Service-Bot**: gRPC TaskService (AssignTask)

### Wie getestet

- Lokaler Lauf: `docker compose up` und Live-Map unter `http://localhost:8081/live-map`
- Sichtprüfung: Bots bewegen sich, Probleme erscheinen erst nach Detektor-Report rechts, Service-Bots bleiben nach Abschluss am Ziel und werden idle.

## FAQ

**Frage:** Alle Service-Bot-Container lauschen auf Port 50051. Bekommen dann alle Bots den gRPC-Aufruf?

**Antwort:** Nein. Jeder Container hat einen eigenen Hostnamen (z. B. `compose-cleaner-1`) und teilt nur die Portnummer, nicht die Adresse. Beim Registrieren sendet jeder Bot seine eindeutige gRPC-Adresse (`hostname:50051`) an den Koordinator. Der Koordinator speichert diese Adresse und wählt genau einen Bot aus (z. B. den nächsten freien Cleaner) und ruft per gRPC genau diese Adresse auf (`grpc.Dial("compose-cleaner-1:50051")`). Docker DNS löst den Hostnamen auf die IP des jeweiligen Containers auf. Dadurch erhält nur der ausgewählte Bot den Aufruf; die anderen Container hören nichts davon.

Great question! Let me trace through exactly how the gRPC addressing works:

## How Service Bot gRPC Addressing Works

### Step 1: Each Bot Gets a Unique Address (Hostname:Port)

When a service bot container starts, it constructs its gRPC address like this (in main.go):

```go
hostname, _ := os.Hostname()           // e.g., "compose-cleaner-1"
grpcAddr := fmt.Sprintf("%s:%s", hostname, grpcPort)  // e.g., "compose-cleaner-1:50051"
```

Each Docker container has a **unique hostname** (the container name). So even though they all listen on port `50051` **inside their container**, from the network perspective they are:

| Container         | Hostname          | gRPC Address              |
| ----------------- | ----------------- | ------------------------- |
| compose-cleaner-1 | compose-cleaner-1 | `compose-cleaner-1:50051` |
| compose-cleaner-2 | compose-cleaner-2 | `compose-cleaner-2:50051` |
| compose-repair-1  | compose-repair-1  | `compose-repair-1:50051`  |
| compose-repair-2  | compose-repair-2  | `compose-repair-2:50051`  |

### Step 2: Bot Registers Its Unique Address with Coordinator

When the bot registers via HTTP (`POST /cleaner-robot`), it sends its unique gRPC address:

```go
body := fmt.Sprintf(`{"grpcAddr":"%s"}`, grpcAddr)  // {"grpcAddr":"compose-cleaner-1:50051"}
```

The coordinator stores this in the `Robot` struct:

```go
st.Robots[id] = Robot{
    ID:       id,
    GRPCAddr: req.GRPCAddr,  // "compose-cleaner-1:50051"
    ...
}
```

### Step 3: Coordinator Selects a Specific Bot

When a problem needs fixing, the coordinator picks the **nearest idle bot** of the right type:

```go
for id, robot := range st.Robots {
    if robot.Type == requiredType && robot.Status == "idle" {
        // Calculate Manhattan distance, pick closest
    }
}
```

### Step 4: Coordinator Dials the Exact Bot

The coordinator uses the **stored gRPC address** to call only that specific bot:

```go
grpcAddr := robot.GRPCAddr  // e.g., "compose-cleaner-1:50051"

// This connects to ONLY that container
conn, err := grpc.DialContext(ctx, grpcAddr, ...)
client := taskpb.NewTaskServiceClient(conn)
resp, err := client.AssignTask(ctx, &taskpb.TaskRequest{...})
```

### Step 5: Only the Target Bot Receives the Call

Docker's internal DNS resolves `compose-cleaner-1` to that specific container's IP address. The gRPC call goes **directly and only** to that container:

```
Coordinator → DNS lookup "compose-cleaner-1" → 172.18.0.5:50051 → Only cleaner-1 receives it
```

**Other bots (cleaner-2, repair-1, etc.) never see this message.**

---

## Summary Flow

```
┌─────────────────────────────────────────────────────────────────┐
│                         COORDINATOR                              │
│                                                                   │
│  Robots map:                                                      │
│  ┌─────────────────────────────────────────────────────────────┐ │
│  │ ID=1: cleaner, GRPCAddr="compose-cleaner-1:50051"           │ │
│  │ ID=2: cleaner, GRPCAddr="compose-cleaner-2:50051"           │ │
│  │ ID=3: repair,  GRPCAddr="compose-repair-1:50051"            │ │
│  └─────────────────────────────────────────────────────────────┘ │
│                                                                   │
│  Problem detected: dirt at (5,5)                                  │
│  → Find nearest idle cleaner → Robot ID=1                         │
│  → grpc.Dial("compose-cleaner-1:50051")  ← SPECIFIC ADDRESS      │
└─────────────────────────────────────────────────────────────────┘
                           │
                           │ gRPC AssignTask
                           ▼
┌─────────────────────────────────────────────────────────────────┐
│  compose-cleaner-1 (listening on :50051)                         │
│  ← ONLY this container receives the call                         │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│  compose-cleaner-2 (listening on :50051)                         │
│  ← Does NOT receive anything (different hostname/IP)             │
└─────────────────────────────────────────────────────────────────┘
```

---

## Key Points

1. **Port 50051 is not shared** - each container has its own network namespace, so `cleaner-1:50051` and `cleaner-2:50051` are completely separate endpoints.

2. **Docker DNS does the routing** - when coordinator dials `compose-cleaner-1:50051`, Docker resolves it to that container's internal IP.

3. **Only the chosen bot receives the gRPC call** - this is standard TCP/IP networking; there's no broadcast or multicast.

4. **The stored `GRPCAddr` is the key** - each bot tells the coordinator its unique address at registration, and the coordinator uses exactly that address later.
