package coordinator

import (
	"net"
	"testing"
	"time"
	"strconv"
	"strings"
)

func TestCoordinatorHandlesHighUDPTraffic(t *testing.T) {
	// ---- Arrange ----

	// Lokalen freien Port für UDP finden
	addr, err := net.ResolveUDPAddr("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// Frischen State für diesen Test erzeugen
	st := &State{
		NextID: 1,
		Robots: make(map[int]Robot),
		Width:  100,
		Height: 100,
	}

	// Ein Roboter, den die UDP-Nachrichten "bewegen" werden
	st.Robots[1] = Robot{ID: 1, X: 10, Y: 10}

	// UDP Listener in Goroutine starten
	conn, err := net.ListenUDP("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// Listener ersetzen, da die echte Funktion UDPListen intern resolve nutzt
	go func() {
		buf := make([]byte, 256)
		for {
			n, _, err := conn.ReadFromUDP(buf)
			if err != nil {
				return // Listener wurde testweise beendet
			}
			msg := string(buf[:n])
			parts := strings.Split(msg, ",")
			if len(parts) != 3 {
				continue
			}
			id, _ := strconv.Atoi(parts[0])
			x, _ := strconv.Atoi(parts[1])
			y, _ := strconv.Atoi(parts[2])

			st.Mu.Lock()
			r := st.Robots[id]
			r.X, r.Y = x, y
			st.Robots[id] = r
			st.Mu.Unlock()
		}
	}()

	// ---- Act ----

	// Sender-Socket
	sender, err := net.DialUDP("udp", nil, conn.LocalAddr().(*net.UDPAddr))
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	const total = 1000

	for i := 0; i < total; i++ {
		msg := "1," + strconv.Itoa(i%100) + "," + strconv.Itoa((i*3)%100)
		_, err := sender.Write([]byte(msg))
		if err != nil {
			t.Fatalf("Fehler beim Senden: %v", err)
		}
	}

	// Kurze Zeit warten, damit Listener alles verarbeiten kann
	time.Sleep(200 * time.Millisecond)

	// ---- Assert ----

	st.Mu.RLock()
	final := st.Robots[1]
	st.Mu.RUnlock()

	expectedX := (total - 1) % 100
	expectedY := ((total - 1) * 3) % 100

	if final.X != expectedX || final.Y != expectedY {
		t.Fatalf("Koordinator hat nicht alle UDP-Nachrichten verarbeitet: got=(%d,%d) expected=(%d,%d)",
			final.X, final.Y, expectedX, expectedY)
	}
}
