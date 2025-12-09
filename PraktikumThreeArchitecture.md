# Praktikum Three Architecture

## Overview

This document outlines the architectural design for the robot coordination system, focusing on simulating a realistic sensor-based environment where the coordinator only learns about problems (dirt/defect) through detector reports, not directly from user placements.

## Current Architecture Issues

In the initial implementation, problems placed by the user via the live map are immediately stored in the coordinator's state. This breaks the simulation because:

- The coordinator has perfect knowledge of all problems, unlike a real system where sensors must discover issues.
- Detectors report problems they "discover," but the coordinator already knows about them.
- This violates the principle of distributed sensing where information is gathered incrementally.

## Proposed Architecture: Two-State Separation

### Core Concept

Maintain two separate views of the world:

1. **World State**: The complete, ground-truth state including all user-placed problems.
2. **Coordinator State**: The coordinator's partial knowledge, containing only problems that have been reported by detectors.

### Data Structures

- **WorldProblems**: A map storing all problems placed by the user, regardless of whether they've been discovered.
- **KnownProblems**: A map in the coordinator containing only problems that detectors have reported via events.

### Data Flow

1. User clicks on the live map to place a problem → Stored in WorldProblems.
2. Detector moves to a position → Queries WorldProblems to check if there's a problem at that location.
3. If problem exists and hasn't been reported → Detector sends an event to coordinator.
4. Coordinator receives event → Adds problem to KnownProblems.
5. Live map can display both views: full world view and coordinator's limited view.

### Implementation Options

#### Option 1: Separate Maps

```go
type State struct {
    Robots map[string]*Robot
    WorldProblems map[string]*Problem  // All user-placed problems
    KnownProblems map[string]*Problem  // Only reported problems
}
```

#### Option 2: Problem Flags

```go
type Problem struct {
    X, Y int
    Type string  // "dirt" or "defect"
    IsReported bool  // Flag indicating if coordinator knows about it
}
```

Use a single map, but filter based on IsReported flag for coordinator view.

### Endpoints

- `/world-map`: Returns the complete world state (for debugging/full view).
- `/map`: Returns the coordinator's known state (current behavior, but filtered).
- `/problem`: Places a problem in WorldProblems (user placement).
- `/problem-at`: Queries WorldProblems for detector checks.
- `/event`: Reports discovered problems to KnownProblems.

### Benefits

- Accurate simulation of sensor-based discovery.
- Clear separation of concerns between world truth and coordinator knowledge.
- Enables side-by-side visualization of both views in the live map.
- Maintains event-driven architecture for problem reporting.

### Previous Ideas Considered

#### Idea A: Three Views

- World View: Complete state.
- Coordinator View: Known problems.
- Detector View: Problems visible to detectors (could include range limitations).
  Rejected due to complexity without clear benefits.

#### Idea B: Two Views with Refinements

- World View: User-placed problems.
- Coordinator View: Reported problems.
  Adopted with separate maps for cleaner implementation.

### Future Extensions

- Add detector range limitations (problems only discoverable within certain distance).
- Implement problem aging or cleanup mechanics.
- Add multiple detector types with different sensing capabilities.

```
┌─────────────────────────────────────────────────────────────────┐
│                         COORDINATOR                              │
│  State:                                                          │
│  ├── Robots map[id]Robot        (all robots, positions)         │
│  └── KnownProblems map[coord]Problem  (ONLY reported problems)  │
│                                                                  │
│  Endpoints:                                                      │
│  - POST /robot, /cleaner-robot, /repair-robot (registration)    │
│  - POST /event (detector reports problem → adds to KnownProblems)│
│  - GET /map (returns robots + KnownProblems only)               │
│  - UDP :9001 (robot position updates)                           │
└─────────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────────┐
│                         WORLD STATE                              │
│  (separate from coordinator - could be same process, different  │
│   data structure, or even separate service)                      │
│                                                                  │
│  State:                                                          │
│  └── WorldProblems map[coord]Problem  (user-placed, hidden)     │
│                                                                  │
│  Endpoints:                                                      │
│  - POST /problem (user clicks → adds to WorldProblems)          │
│  - POST /problem-at (detector queries → reads WorldProblems)    │
│  - GET /world-map (returns WorldProblems for user view)         │
└─────────────────────────────────────────────────────────────────┘
```
