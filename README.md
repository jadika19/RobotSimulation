# Roboter-Simulation (World + Coordinator + Detector + Service Bots)

Dieses Projekt simuliert ein 2D-Gitter mit getrenntem Weltzustand (Ground Truth), Koordinator, Detektoren und Service-Bots (Cleaner/Repair). Detektoren entdecken Probleme, melden sie dem Koordinator, der den nächstgelegenen Service-Bot per gRPC zum Aufräumen/ Reparieren schickt. Eine Live-Map (Dual View) zeigt sowohl die Welt (alle Probleme) als auch die Koordinator-Sicht (nur entdeckte Probleme).

---

## Projektstruktur

```bash
projekt/
	cmd/
		coordinator/       # HTTP + UDP + gRPC callback server
		detector/          # Detektor-Bots
		servicebot/        # Cleaner/Repair Bots (gRPC Server)
		world/             # Welt-Service (Ground Truth)
	deployments/
		compose/
			docker-compose.yml
		docker/
			coordinator/
			detector/
			servicebot/
			world/
	internal/
		coordinator/       # Koordinator-Logik
		detector/          # Detektor-Logik
		serviceBots/       # Service-Bot-Logik (gRPC)
		world/             # Weltzustand + Live-Map
		taskpb/            # gRPC Stubs (TaskService, TaskCallback)
	proto/
		task.proto         # gRPC Definitionen
	go.mod
	Makefile
	README.md
```

## Voraussetzungen

- [Go 1.24+](https://go.dev/dl/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

---

## Projekt starten

1. **Build der Container:**

```bash
cd deployments/compose
docker compose build
```

2. **Starten aller Services:**

```bash
docker compose up
```

- World Service: HTTP 8081 (`/world-map`, `/problem`, `/problem-at`, `/problem/delete`, `/live-map`)
- Coordinator: HTTP 8080 (`/map`, `/event`, `/robot`, `/cleaner-robot`, `/repair-robot`), UDP 9001 (Positionsupdates), gRPC 9002 (TaskCallback)
- Service-Bots: gRPC Server (per-Container Port `GRPC_PORT`, default 50051) für TaskService
- Detectors verbinden sich automatisch über interne Docker-Namen und melden Funde an den Koordinator

3. **Live-Map öffnen:**

- `http://localhost:8081/live-map` (wenn lokal) oder `http://<host-ip>:8081/live-map`
- Linke Ansicht: Welt (alle Probleme), rechte Ansicht: Koordinator (nur entdeckte Probleme + Bots)

4. **Zusätzliche Detektoren starten:**

```bash
docker run --rm --network compose_default compose-detector
```

oder skalieren:

```bash
docker compose up -d --scale detector=4
```

5. **Stoppen aller Services:**

```bash
docker compose down
```

## Lokale Entwicklung (ohne Docker)

1. **Build der Binaries:**

```bash
make build
```

2. **World Service starten:**

```bash
go run ./cmd/world
```

3. **Coordinator starten:**

```bash
go run ./cmd/coordinator
```

4. **Detector starten:**

```bash
go run ./cmd/detector
```

5. **Service-Bot starten (Cleaner oder Repair):**

```bash
BOT_TYPE=cleaner go run ./cmd/servicebot
```

6. **Tests ausführen:**

```bash
make test
```

## HTTP/UDP/gRPC Endpunkte

### World Service (HTTP 8081)

- `GET /world-map` – liefert Ground-Truth-Probleme (alle Probleme)
- `POST /problem` – Problem hinzufügen (`{x,y,type}`)
- `POST /problem-at` – Detektoren fragen nach Problem an Position
- `DELETE /problem` – Problem entfernen (`{x,y}`) nach erfolgreicher Bereinigung
- `GET /live-map` – Dual-View HTML (World vs Coordinator)

### Coordinator (HTTP 8080, UDP 9001, gRPC 9002)

- `GET /map` – Roboter + entdeckte Probleme
- `GET /status` – Statusübersicht
- `POST /event` – Detektor meldet gefundenes Problem (`{event,x,y}`)
- `POST /robot` – Detektor registrieren
- `POST /cleaner-robot` – Service-Bot (Cleaner) registrieren (`{grpcAddr}` optional x/y)
- `POST /repair-robot` – Service-Bot (Repair) registrieren (`{grpcAddr}` optional x/y)
- UDP :9001 – Positionsupdates im Format `id,x,y`
- gRPC :9002 – TaskCallbackService (ReportCompletion)

### Service-Bots (per Container gRPC, default Port 50051)

- TaskService.AssignTask – Koordinator ruft auf, um Aufgabe zuzuweisen

### Detector Bot

- Läuft eigenständig, ruft `POST /event` beim Koordinator, fragt Welt mit `/problem-at` ab

## API testen mit curl

Nachdem der Coordinator läuft (lokal oder in Docker):

- **Status abrufen:**

```bash
curl -X GET http://localhost:8080/status
```

- **Karte mit Roboter-Positionen abrufen:**

```bash
curl -X GET http://localhost:8080/map
```

- **Neuen Roboter registrieren:**

```bash
curl -X POST http://localhost:8080/robot -H "Content-Type: application/json" -d '{"x":2,"y":3}'
```

Hinweis: Detectors laufen für 100 Schritte (~50 Sekunden) und senden Updates alle 500ms.

## Tests

1. **Coordinator-Tests ausführen:**

```bash
go test ./internal/coordinator -v
```

```bash
go test ./internal/coordinator -bench=.
```

2. **Detector-Tests ausführen:**

```bash
go test ./internal/detector -v
```

```bash
go test ./internal/detector -bench=.
```

3. **Viertes Praktikums Tests ausführen:**

```bash
go test -v -run "TestRobotFailure" ./internal/coordinator/
```

---

## Nicht-Funktionale Tests (Non-Functional Tests)

Die folgenden Tests prüfen **WIE** das System arbeitet, nicht was es tut. Sie validieren Performance und Zuverlässigkeit der MQTT/MOM-Implementierung.

### Test 1: Message Latency (Performance)

Misst die Round-Trip-Zeit für MQTT-Nachrichten vom Publisher zum Subscriber.

**Ausführung:**

```bash
go test -v -run TestMessageLatency ./internal/coordinator/
```

**Was wird getestet:**

- Zeit von Publish bis Receive (100 Nachrichten)
- Statistische Analyse: Min, Max, Avg, P50, P95, P99
- Delivery Rate (sollte 99%+ sein)

**Pass-Kriterien:**

- Average Latency < 50ms
- P99 Latency < 200ms
- Delivery Rate ≥ 99%

### Test 2: Connection Failure Recovery (Reliability)

Testet das Auto-Reconnect-Verhalten des MQTT-Clients bei Verbindungsabbruch.

**Ausführung:**

```bash
go test -v -run TestConnectionFailureRecovery ./internal/coordinator/
```

**Was wird getestet:**

- Client-Verhalten bei Broker-Ausfall
- Automatische Wiederverbindung
- Nachrichtenübertragung nach Recovery (QoS 1)
- State Consistency nach Disruption

**Pass-Kriterien:**

- Reconnection Time < 15 Sekunden
- Client reconnected = true
- Nachrichten nach Recovery erfolgreich

### Batch-Test Runner (100 Iterationen)

Führt beide Tests mehrfach aus und aggregiert die Ergebnisse:

```bash
# 100 Iterationen (Standard)
./scripts/run_nonfunctional_tests.sh

# Oder mit anderer Anzahl
./scripts/run_nonfunctional_tests.sh 50
```

**Voraussetzung:** MQTT Broker muss laufen (lokal auf Port 1883 oder via Docker).

### Test-Ergebnisse (Beispiel)

Die folgende Tabelle zeigt typische Ergebnisse nach 100 Testdurchläufen:

#### Message Latency Test Results

| Metric              | Value      | Unit | Threshold    | Status  |
| ------------------- | ---------- | ---- | ------------ | ------- |
| **Average Latency** | ~500-700   | μs   | < 50,000 μs  | ✅ PASS |
| **P50 Latency**     | ~500-600   | μs   | -            | -       |
| **P95 Latency**     | ~800-1000  | μs   | -            | -       |
| **P99 Latency**     | ~1000-1500 | μs   | < 200,000 μs | ✅ PASS |
| **Min Latency**     | ~100-200   | μs   | -            | -       |
| **Max Latency**     | ~1000-2000 | μs   | -            | -       |
| **Delivery Rate**   | 100        | %    | ≥ 99%        | ✅ PASS |

#### Connection Recovery Test Results

| Metric                   | Value    | Unit | Threshold   | Status  |
| ------------------------ | -------- | ---- | ----------- | ------- |
| **Avg Reconnect Time**   | ~100-200 | ms   | < 15,000 ms | ✅ PASS |
| **Min Reconnect Time**   | ~50-100  | ms   | -           | -       |
| **Max Reconnect Time**   | ~200-500 | ms   | -           | -       |
| **Reconnection Success** | 100      | %    | 100%        | ✅ PASS |

### Interpretation

- **Message Latency:** MQTT liefert zuverlässige Nachrichtenübermittlung mit QoS 1. Latenzen im Sub-Millisekunden- bis niedrigen Millisekundenbereich demonstrieren effiziente Broker-Performance, geeignet für Echtzeit-Positionsupdates.

- **Connection Recovery:** Die Auto-Reconnect-Funktion des Paho MQTT-Clients gewährleistet Systemresilienz. Bei Broker-Neustart verbinden sich Clients automatisch innerhalb des konfigurierten Timeouts wieder, was die Systemverfügbarkeit aufrechterhält.

---

## Troubleshooting

### Containers stop not properly (permission denied)

If `docker compose down` fails with "permission denied" when stopping containers (especially the coordinator), this is typically due to AppArmor profiles interfering with Docker on Ubuntu.

**Symptoms:**

- `docker compose down` shows: `Error response from daemon: cannot stop container: ... permission denied`
- Containers remain running despite stop attempts.

**Solution:**

1. Clean up unknown AppArmor profiles:
   ```bash
   sudo aa-remove-unknown
   ```
2. Restart AppArmor and Docker:
   ```bash
   sudo systemctl restart apparmor
   sudo systemctl restart docker
   ```
3. Clean up and restart the stack:
   ```bash
   cd deployments/compose
   docker compose down -v --remove-orphans
   docker compose up
   ```

This should be a one-time fix. If the issue persists frequently, check for Docker or kernel updates, or investigate what is creating conflicting AppArmor profiles.

**Code-level fixes applied:**

- The coordinator's UDP listener now exits gracefully on network errors instead of busy-looping.
- Detectors listen for termination signals and stop cleanly.
