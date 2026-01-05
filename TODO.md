# TODO

- Decide MQTT broker setup
  - Pick broker (e.g., Mosquitto) and add to compose
  - Configure default creds and persistence

- Define MQTT topics and QoS
  - devices/detector/{id}/position (QoS 1, retain=false)
  - devices/servicebot/{id}/position (QoS 1, retain=false)
  - events/problems (QoS 1, retain=false) payload {x,y,type,detectorId}
  - devices/{id}/status (QoS 1, retain=true) with Last Will offline

- ServiceBot migration (internal/serviceBots/servicebot.go)
  - Add MQTT client init (opts, will, connect, reconnect)
  - Replace UDP SendPosition with MQTT publish to devices/servicebot/{id}/position
  - Add status online/offline publishes and graceful disconnect

- Detector migration (internal/detector/detector.go)
  - Add MQTT client init (opts, will, connect)
  - Replace UDP position send with MQTT publish to devices/detector/{id}/position
  - Replace HTTP /event with MQTT publish to events/problems

- Coordinator ingestion (internal/coordinator/coordinator.go)
  - Add MQTT client subscribe to devices/+/position and events/problems
  - Update state on position messages; enforce bounds
  - On problem events: deduplicate, trigger task assignment
  - Add handling of status topics to mark bots offline/online

- Keep gRPC task flow (for now)
  - Retain gRPC AssignTask and ReportCompletion; no change needed
  - Revisit only if full async needed

- World service integration
  - Optionally mirror world/map or deltas to MQTT for UI live updates
  - Keep HTTP world cleanup as-is unless real-time updates are required

- Configuration and env wiring
  - Add broker URL, creds, client IDs to config/env
  - Wire docker-compose services to broker network

- Observability
  - Add structured logging around MQTT connects/publishes/subscribes
  - Add metrics or counters for received position/events

- Testing
  - Update coordinator UDP tests to MQTT-based equivalents
  - Add integration test: detector + coordinator + broker, ensure positions/events processed
  - Add test for Last Will/offline handling

- Cleanup
  - Remove UDP sockets and related config once MQTT path is stable
  - Update docs/README to describe MQTT flow and topics
