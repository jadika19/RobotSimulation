# Praktikum Three Architecture

## Overview

This document outlines the architectural design for the robot coordination system, focusing on simulating a realistic sensor-based environment where the coordinator only learns about problems (dirt/defect) through detector reports, not directly from user placements.

## Implemented Architecture: Separate Services

The system is now implemented with **complete service separation**:

### Services

1. **World Service** (Port 8081): Owns the ground-truth state of problems
2. **Coordinator Service** (Port 8080): Manages robots and only knows reported problems
3. **Detector Robots**: Query world, report to coordinator
4. **Service Bots** (Cleaner/Repair): Register with coordinator, await tasks

### Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────┐
│                      WORLD SERVICE (:8081)                       │
│  State:                                                          │
│  └── Problems map[coord]Problem  (user-placed, ground truth)    │
│                                                                  │
│  Endpoints:                                                      │
│  - POST /problem (user clicks → adds to Problems)               │
│  - POST /problem-at (detector queries → reads Problems)         │
│  - GET /world-map (returns all Problems for user view)          │
│  - GET /live-map (serves dual-view HTML)                        │
└─────────────────────────────────────────────────────────────────┘
       ↑
  User clicks
  (place problems)
       ↑
┌──────┴──────────────────────────────────────────────────────────┐
│                         DETECTOR                                 │
│  1. Registers with coordinator → gets ID, start position        │
│  2. Walks grid, sends position via UDP to coordinator           │
│  3. Queries world service /problem-at at each position          │
│  4. If problem found → reports to coordinator via /event        │
└─────────────────────────────────────────────────────────────────┘
                                    │
                                    │ POST /event
                                    ↓
┌─────────────────────────────────────────────────────────────────┐
│                    COORDINATOR SERVICE (:8080)                   │
│  State:                                                          │
│  ├── Robots map[id]Robot        (all robots, positions)         │
│  └── KnownProblems map[coord]Problem  (ONLY reported problems)  │
│                                                                  │
│  Endpoints:                                                      │
│  - POST /robot, /cleaner-robot, /repair-robot (registration)    │
│  - POST /event (detector reports → adds to KnownProblems)       │
│  - GET /map (returns robots + KnownProblems only)               │
│  - GET /status (robot status)                                   │
│  - UDP :9001 (robot position updates)                           │
└─────────────────────────────────────────────────────────────────┘
```

### Data Flow

1. **User places problem**: Clicks on World View grid → `POST /problem` to World Service → Stored in `Problems` map
2. **Detector discovers**: Walks grid → `POST /problem-at` to World Service → Gets problem info
3. **Detector reports**: If problem found → `POST /event` to Coordinator → Added to `KnownProblems`
4. **Live map displays**: Left grid shows World View (all problems), Right grid shows Coordinator View (only discovered)

### Key Design Decisions

#### Why Separate Services?

- **True isolation**: Coordinator process has NO knowledge of world problems
- **Realistic simulation**: Problems only enter coordinator knowledge via detector reports
- **Scalability**: Services can be scaled independently
- **Clear boundaries**: Each service has single responsibility

#### Service Responsibilities

| Service     | Owns                        | Endpoints                                            |
| ----------- | --------------------------- | ---------------------------------------------------- |
| World       | Ground-truth problems       | `/problem`, `/problem-at`, `/world-map`, `/live-map` |
| Coordinator | Robots, discovered problems | `/robot`, `/event`, `/map`, `/status`                |

### Docker Compose Services

```yaml
services:
  world: # Ground truth, user interaction
  coordinator: # Robot management, discovered problems
  detector: # Queries world, reports to coordinator
  cleaner: # Service bot (idle until tasked)
  repair: # Service bot (idle until tasked)
```

### Live Map (Dual View)

The live map now shows two side-by-side grids:

- **Left (World View)**: Fetches from World Service `/world-map`, shows ALL problems
- **Right (Coordinator View)**: Fetches from Coordinator `/map`, shows only DISCOVERED problems

User can click on World View to place problems. They appear immediately in World View, but only appear in Coordinator View after a detector walks over them and reports.

### Previous Architecture (Superseded)

The previous implementation had both `WorldProblems` and `KnownProblems` in the same coordinator process. While logically separated, this violated the principle that the coordinator should have no knowledge of undiscovered problems.

### Future Extensions

- Add detector range limitations (problems only discoverable within certain distance)
- Implement problem aging or cleanup mechanics
- Add task assignment from coordinator to service bots
- Implement problem resolution (service bots clean/repair and remove from KnownProblems)
