# Praktikum 5 - Änderungen und Bugfixes

## Übersicht
Implementierung des Bully-Algorithmus für Leader-Election mit verteilter Task-Zuweisung wurde um mehrere kritische Bugfixes erweitert.

---

## 1. Problem-Löschung aus beiden Views

### Problem
Probleme verschwanden nach Behebung nur aus der World-View, nicht aus der Coordinator-View.

### Ursache
Der Coordinator versuchte im `ReportCompletion` Handler die Task in seiner `Tasks` Map zu finden. Da der Leader nun die Tasks verwaltet, gab der Lookup `false` zurück und die Funktion beendete früh ohne das Problem aus `KnownProblems` zu löschen.

### Lösung
- **proto/task.proto**: `CompletionRequest` um `x` und `y` Felder erweitert
- **internal/taskpb/**: Proto-Dateien neu generiert
- **internal/serviceBots/servicebot.go**: `reportTaskCompletion()` und `reportCompletion()` übergeben nun X,Y Koordinaten
- **internal/coordinator/coordinator.go**: `ReportCompletion` nutzt `req.X` und `req.Y` statt Task-Lookup, löscht Problem auch wenn Task nicht existiert

### Ergebnis
Probleme verschwinden nun sowohl aus World-View als auch aus Coordinator-View.

---

## 2. Leader weist sich selbst keine Tasks zu

### Problem
Der Leader wies sich selbst keine Aufgaben zu, obwohl er näher am Problem war als andere Bots.

### Ursache
- In `election.go` ignoriert jeder Bot seine eigenen Metadaten (`if meta.ID == bot.ID { return }`)
- Leader erscheint nicht in seiner eigenen `KnownBots` Map
- In `tryAssignTask()` wurde nur über `KnownBots` iteriert → Leader nie als Kandidat berücksichtigt

### Lösung (ursprünglich)
- **internal/serviceBots/task_assignment.go**: `tryAssignTask()` prüft zuerst den Leader selbst als Kandidat
- Wenn Leader idle und richtiger Typ: wird als `bestBot` mit Manhattan-Distanz gesetzt
- Dann werden andere Bots in `KnownBots` geprüft
- Bot mit kürzester Distanz gewinnt

### Spätere Verbesserung
Siehe Punkt 7 - Leader wird nun komplett wie andere Bots behandelt.

---

## 3. Roboter-Bewegung: N4-Nachbarschaft und Geschwindigkeit

### Problem
- Service Bots bewegten sich diagonal (zu schnell)
- Service Bots bewegten sich schneller als Detektoren

### Ursache
**Service Bot Bewegung** (vorher):
- Bewegte sich ZUERST in X-Richtung (`bot.X++`)
- Sleep 200ms
- DANN in Y-Richtung (`bot.Y++`)
- Sleep 200ms
- **Ergebnis**: 2 Schritte in 400ms = diagonal

**Detector Bewegung**:
- Bewegt sich nur in EINE Richtung pro Iteration (entweder x++ ODER y++)
- Sleep 500ms
- **Ergebnis**: 1 Schritt in 500ms = N4-Nachbarschaft

### Lösung
- **internal/serviceBots/servicebot.go**: `executeTaskMQTT()` geändert
- Nur EIN Schritt pro Iteration (entweder X ODER Y)
- Priorisiert X-Achse, dann Y-Achse
- Sleep 500ms pro Schritt

### Ergebnis
Service Bots bewegen sich nun im gleichen Tempo wie Detektoren und nur in N4-Nachbarschaft (kein diagonales Laufen).

---

## 4. Pending Tasks werden nicht abgearbeitet

### Problem
Wenn mehr Probleme entdeckt wurden als Bots verfügbar (z.B. 4 defects, 2 repair bots), wurden nur die ersten beiden gelöst. Weitere Tasks blieben pending, auch wenn Bots idle wurden.

### Ursache - Race Condition
**Vorher**:
1. Bot beendet Task
2. `reportTaskCompletion()` wird aufgerufen ← Status ist noch "busy"
3. Leader erhält Completion → ruft `tryAssignPendingTasks()` auf
4. `tryAssignTask()` prüft Bot-Status → findet Bot "busy" → kann keine neue Task zuweisen
5. DANN wird Status auf "idle" gesetzt (zu spät!)

### Lösung
- **internal/serviceBots/servicebot.go**: Reihenfolge in `executeTaskMQTT()` korrigiert
1. Bot beendet Task
2. Status wird auf "idle" gesetzt
3. Metadata wird publiziert
4. `reportTaskCompletion()` wird aufgerufen ← Status ist jetzt "idle"
5. Leader kann pending Tasks sofort zuweisen

### Ergebnis
Pending Tasks werden korrekt zugewiesen, sobald ein Bot idle wird.

---

## 5. HandleTaskEvent verarbeitet Events nicht

### Problem
Events wurden empfangen aber nicht verarbeitet - Tasks wurden nie zugewiesen, wenn andere Bots idle wurden.

### Ursache
`handleTaskEvent` empfing empfangene Task-Events (insbesondere "completed" Events) nur geloggt, aber NICHT verarbeitet. Dadurch wurde `OnTaskCompletion` nie aufgerufen, wenn ein nicht-Leader Bot eine Task abschloss.

**Was passierte**:
1. Bot beendet Task → publiziert MQTT Event "completed"
2. Leader empfängt Event in `handleTaskEvent`
3. Event wird nur geloggt, NICHTS passiert
4. Pending Tasks werden NIE zugewiesen

### Lösung
- **internal/serviceBots/task_assignment.go**: `handleTaskEvent` verarbeitet Events aktiv
  - "completed" → ruft `OnTaskCompletion()` auf
  - "failed" → ruft `OnTaskCompletion()` auf
  - "timeout" → ruft `handleTaskAssignmentFailure()` auf

### Ergebnis
Pending Tasks werden zugewiesen, sobald ein Bot eine Task abschließt.

---

## 6. Doppelte Event-Verarbeitung

### Problem
Tasks vom Typ des Leaders wurden nicht ordentlich abgearbeitet - "unknown task" Fehler, inkonsistenter State.

### Ursache - Doppeltes Event-Publishing
**Flow**:
1. Bot schließt Task ab
2. `OnTaskCompletion` wird aufgerufen → löscht Task aus `AssignedTasks` → publiziert MQTT Event
3. Leader empfängt sein eigenes MQTT Event
4. `handleTaskEvent` ruft `OnTaskCompletion` NOCHMAL auf
5. Task existiert nicht mehr → "Received completion for unknown task"
6. State ist inkonsistent

### Erste Lösungsversuche
- Check ob Task existiert bevor Event verarbeitet wird (rückgängig gemacht)
- `publishEvent` Parameter für `OnTaskCompletion` (funktionierte teilweise)

### Endgültige Lösung
Siehe Punkt 7 - komplette Umstrukturierung.

---

## 7. Leader-Behandlung vereinfacht

### Problem
Leader bewegte sich sinnlos herum, Tasks wurden nicht ordentlich verteilt, viele Spezialfälle im Code.

### Ursache
- Leader war nicht in `KnownBots`
- Leader hatte Spezialbehandlung für Task-Zuweisung
- Leader rief `OnTaskCompletion` direkt auf statt MQTT Events
- Inkonsistente Status-Verwaltung

### Lösung - Leader wird wie jeder andere Bot behandelt

#### 1. Leader fügt sich selbst in KnownBots ein
**internal/serviceBots/election.go**:
- Entfernt `if meta.ID == bot.ID { return }`
- Leader verarbeitet seine eigenen Metadaten
- Fügt sich selbst in `KnownBots` ein

#### 2. Keine Spezialprüfung in tryAssignTask
**internal/serviceBots/task_assignment.go**:
- Entfernt spezielle Leader-Selbstprüfung
- Leader ist jetzt in `KnownBots` wie alle anderen
- Wird automatisch bei Manhattan-Distanz-Berechnung berücksichtigt

#### 3. Leader erhält Tasks über gRPC
**internal/serviceBots/task_assignment.go**:
- Entfernt `if bestBot.ID == bot.ID { ... direkter Aufruf }`
- Alle Bots (einschließlich Leader) erhalten Tasks über `assignTaskViaGRPC()`

#### 4. Alle Bots publizieren MQTT Events
**internal/serviceBots/servicebot.go**:
- Entfernt direkten `OnTaskCompletion` Callback für Leader
- Alle Bots publizieren MQTT Events bei Completion
- Leader empfängt eigene Events über MQTT wie andere Events auch

#### 5. OnTaskCompletion vereinfacht
**internal/serviceBots/task_assignment.go**:
- Entfernt `publishEvent` Parameter
- Publiziert NIE Events (Events werden von Bots publiziert)
- Wird nur noch von `handleTaskEvent` aufgerufen

### Ergebnis
- Leader wird konsistent wie jeder andere Service Bot behandelt
- Erhält Tasks über gRPC
- Status wird in `KnownBots` verwaltet
- Keine Sonderfälle mehr
- Deutlich einfacherer und wartbarerer Code

---

## Technische Verbesserungen

### Event Flow (final)
1. Bot beendet Task
2. Bot setzt Status auf "idle"
3. Bot publiziert Metadata-Update
4. Bot publiziert "completed" Event via MQTT
5. Bot meldet Completion an Coordinator
6. Leader empfängt "completed" Event
7. `handleTaskEvent` ruft `OnTaskCompletion` auf
8. `OnTaskCompletion` aktualisiert State, ruft `tryAssignPendingTasks()`
9. Pending Tasks werden an idle Bots zugewiesen

### Manhattan-Distanz Berechnung
- Alle Bots (einschließlich Leader) in `KnownBots`
- Iteration über alle Bots
- Filter: richtiger Typ, idle, nicht stale (>10s)
- Berechnung: `abs(botX - taskX) + abs(botY - taskY)`
- Auswahl: Bot mit kleinster Distanz

### Konsistenz-Garantien
- Status-Updates vor Event-Publishing
- Kein direkter State-Zugriff zwischen Komponenten
- Alle State-Änderungen über MQTT Events
- Event Sourcing für Task-Historie
- Keine Race Conditions durch korrekte Reihenfolge

---

## Zusammenfassung der geänderten Dateien

1. **proto/task.proto**: X,Y zu CompletionRequest hinzugefügt
2. **internal/taskpb/**: Neu generierte Proto-Dateien
3. **internal/serviceBots/servicebot.go**: 
   - Bewegungslogik (N4, 500ms)
   - Status vor Completion setzen
   - Alle Bots publizieren Events
4. **internal/serviceBots/task_assignment.go**:
   - Leader in KnownBots
   - Keine Spezialbehandlung für Leader
   - Event-Processing in handleTaskEvent
   - OnTaskCompletion vereinfacht
5. **internal/serviceBots/election.go**: Leader ignoriert eigene Metadaten nicht mehr
6. **internal/coordinator/coordinator.go**: Problem-Löschung ohne Task-Lookup

---

## Testing-Erkenntnisse

- System funktioniert mit 2 Cleaners, 2 Repairs, 3 Detectors
- Leader-Election stabil
- Task-Zuweisung nach Manhattan-Distanz effizient
- Alle Tasks werden abgearbeitet, auch bei Überlast
- Keine "unknown task" Fehler mehr
- Probleme verschwinden korrekt aus allen Views
- Bots bewegen sich korrekt in N4-Nachbarschaft
