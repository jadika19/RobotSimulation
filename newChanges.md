# Recent Changes (Coordinator, Detector & Service Bots)

## Coordinator (`internal/coordinator/coordinator.go`)

- Added problem storage on the grid via `Problems` map and `Problem` struct.
- New endpoint `POST /problem` handled by `handleProblemUpsert`: upserts or clears a problem at given `x`,`y` with `type` (`dirt`/`defect`/`clear`).
- New endpoint `POST /problem-at` handled by `handleProblemAt`: returns whether a problem is present at `x`,`y` and its type.
- `/map` response now includes `problems` array so the live map can render dirt/defect cells.
- `/event` now parses JSON and logs incoming reports as `problem reported: <type> at (x,y)` (timestamps removed via `log.SetFlags(0)`).
- Extended `Robot` struct with `Type` (detector/cleaner/repair) and `Status` (idle/busy) fields.
- `/map` and `/status` responses now include robot type and status.
- New endpoints `POST /cleaner-robot` and `POST /repair-robot` for service bot registration (via shared `registerServiceBot` helper).

## Detector (`internal/detector/detector.go`)

- Removed random event generation; detectors now query the coordinator for real problems.
- Added `CheckForProblem`: calls `POST /problem-at` with current `x`,`y` to see if a problem exists; returns type and presence.
- In `Walk`, when the detector steps on a problem cell (dirt/defect) and hasn't reported that coordinate yet, it sends `POST /event` with `{"event":"<type>","x":<x>,"y":<y>}`.
- Added deduping per coordinate so each problem location is reported only once per detector run.

## Service Bots (`internal/serviceBots/servicebot.go`)

- New `ServiceBot` struct with `Type` (cleaner/repair), `Status` (idle/busy), position, and UDP connection.
- `New(botType)`: creates a service bot of given type.
- `Register(coordAddr)`: registers with coordinator via `POST /<type>-robot`.
- `ConnectUDP(udpAddr)` / `Close()`: manages UDP connection for position updates.
- `SendPosition()`: sends `id,x,y` to coordinator (same format as detectors).
- Service bots initialize in idle mode and wait for tasks (movement/work logic to be implemented later).

## Live Map (`internal/coordinator/live_map.html`)

- Left-click places dirt (dark green), right-click places defect (red).
- Robots color-coded by type: blue=detector, purple=cleaner, orange=repair.
- Problems and robots rendered from `/map` response.

## Docker Compose (`deployments/compose/docker-compose.yml`)

- Added `cleaner` service (2 replicas, `BOT_TYPE=cleaner`).
- Added `repair` service (2 replicas, `BOT_TYPE=repair`).
- New Dockerfile for servicebot at `deployments/docker/servicebot/Dockerfile`.
