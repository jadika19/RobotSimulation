# Roboter-Simulation (Coordinator + Detector)  

Dieses Projekt simuliert Roboter, die sich auf einem 2D-Gitter bewegen.  
Es besteht aus zwei Komponenten:

1. **Coordinator** – verwaltet den Serverzustand, HTTP-API, UDP-Listener  
2. **Detector** – simuliert Roboter, die sich zufällig bewegen und ihre Positionen an den Coordinator senden  

---

## Projektstruktur

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

3. **Stoppen aller Services:**

```bash
docker-compose down
```

## HTTP-Endpunkte des Coordinators

| Methode | Pfad    | Beschreibung                      |
| ------- | ------- | --------------------------------- |
| GET     | /status | Anzahl der registrierten Roboter  |
| GET     | /map    | Aktuelle Positionen aller Roboter |
| POST    | /robot  | Roboter registrieren              |
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
