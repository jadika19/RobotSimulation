# Roboter-Simulation (Coordinator + Detector)

Dieses Projekt simuliert Roboter, die sich auf einem 2D-Gitter bewegen.  
Es besteht aus zwei Komponenten:

1. **Coordinator** – verwaltet den Serverzustand, HTTP-API, UDP-Listener
2. **Detector** – simuliert Roboter, die sich zufällig bewegen und ihre Positionen an den Coordinator senden

---

## Projektstruktur

```bash
projekt/
	cmd/
		coordinator/
			main.go
		detector/
			main.go
	deployments/
		compose/
			docker-compose.yml
		docker/
			coordinator/
				Dockerfile
			detector/
				Dockerfile
	internal/
		coordinator/
			coordinator.go
			coordinator_test.go
		detector/
			detector.go
			detector_test.go
	go.mod
	Makefile
	README.md
```

## Voraussetzungen

- [Go 1.23+](https://go.dev/dl/)
- [Docker](https://www.docker.com/)
- [Docker Compose](https://docs.docker.com/compose/)

---

## Projekt starten

1. **Build der Container:**

```bash
cd deployments/compose
docker-compose build
```

2. **Starten aller Services:**

```bash
docker-compose up
```

- Coordinator läuft auf TCP 8080 (HTTP)
- Coordinator lauscht auf UDP 9001 für Positionsupdates
- Detector verbindet sich automatisch über interne Docker-Namen

3. **Zusätzliche Roboter starten:**

Um einen zusätzlichen Roboter zu starten, während die Services bereits laufen:

```bash
docker run --rm --network compose_default compose-detector
```

Dies startet einen neuen Detector-Container, der sich automatisch registriert und beginnt, sich zu bewegen.

4. **Stoppen aller Services:**

```bash
docker-compose down
```

## Lokale Entwicklung

Für lokale Tests ohne Docker:

1. **Build der Binaries:**

```bash
make build
```

2. **Coordinator starten (in einem Terminal):**

```bash
make coordinator
```

- Coordinator läuft auf `localhost:8080` (HTTP) und lauscht auf UDP `9001`.

3. **Detector starten (in einem anderen Terminal):**

```bash
make detector
```

- Detector registriert sich beim Coordinator und sendet Positionsupdates.

4. **Tests ausführen:**

```bash
make test
```

## HTTP-Endpunkte des Coordinators

| Methode | Pfad    | Beschreibung                      |
| ------- | ------- | --------------------------------- |
| GET     | /status | Anzahl der registrierten Roboter  |
| GET     | /map    | Aktuelle Positionen aller Roboter |
| POST    | /robot  | Roboter registrieren              |

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

Hinweis: Detectors laufen für 100 Schritte (~20 Sekunden) und senden Updates alle 200ms.

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
