# Aufgaben 3.3 RPC

## Neue Bots (Cleaner, Repair)

- [ ] Neuer Status BUSY/IDLE
- [ ] Eigene Position + Status bei jedem Schritt an Koordinator übermitteln
- [ ] Nur bewegen, wenn Verschmutzung/Defekt bearbeitet wird
- [ ] Bei Start über REST-API beim Koordinator registrieren
  - [ ] Endpoint `/robot-cleaner`
  - [ ] Endpoint `/robot-repair`
- [ ] Nach Erhalt des Auftrags zur Stelle bewegen
  - [ ] Defekt/Schmutz beseitigen (Zeitspanne beliebig wählbar)
  - [ ] Abschluss der Aufgabe über RPC an Koordinator melden
  - [ ] Stehen bleiben (Status wechseln)

## Detector Bot

- [x] Soll zufällig Probleme auf der Karte entdecken
- [ ] Probleme an Koordinator über neue REST-Schnittstelle melden (z. B. `POST /event`)

## Koordinator

- [ ] Nach Erhalt von Problem geeigneten und verfügbaren Service-Bot auswählen
- [ ] Aufgaben an Service-Bots vergeben über RPC (gRPC/Apache Thrift)
- [ ] Mögliche Bestandteile: Aufgaben-ID, Ort des Problems, Art des Problems

## Dashboard

- [x] Neue Probleme anzeigen
- [ ] Welcher Bot erledigt welche Aufgabe
- [ ] Welche Aufgabe wurde erledigt
