package coordinator_test

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/coordinator"
)

// ---------- Globaler Test-State ----------

var testState = &coordinator.State{
	NextID: 1,
	Robots: make(map[int]coordinator.Robot),
	Width:  20,
	Height: 20,
	Mu:     sync.RWMutex{},
}

// ---------- Helper: TCP-Verbindung simulieren ----------

func handleTestRequest(request string, t *testing.T) string {
	client, server := net.Pipe()
	defer client.Close()
	defer server.Close()

	// Schreibe Request
	go func() {
		client.Write([]byte(request))
	}()

	// Handler
	go coordinator.HandleHTTPRequest(server, testState)

	// Lese Response
	buf := make([]byte, 1024)
	n, _ := client.Read(buf)
	return string(buf[:n])
}

// ---------- Funktionale Tests (HTTP) ----------

func TestRegisterRobotWithBody(t *testing.T) {
	testState.Mu.Lock()
	testState.NextID = 1
	testState.Robots = make(map[int]coordinator.Robot)
	testState.Mu.Unlock()

	body := `{"x":2,"y":3}`
	req := "POST /robot HTTP/1.1\r\nContent-Length: " +
		strconv.Itoa(len(body)) + "\r\n\r\n" + body
	resp := handleTestRequest(req, t)

	// Prüfen, dass die ID korrekt ist
	if !strings.Contains(resp, `"id":1`) {
		t.Errorf("expected ID 1 in response, got: %s", resp)
	}

	// Prüfen, dass die Koordinaten im State korrekt gesetzt wurden
	testState.Mu.RLock()
	rb := testState.Robots[1]
	testState.Mu.RUnlock()
	if rb.X != 2 || rb.Y != 3 {
		t.Errorf("expected robot at (2,3), got (%d,%d)", rb.X, rb.Y)
	}
}

func TestGetStatus(t *testing.T) {
	testState.Mu.Lock()
	testState.Robots = map[int]coordinator.Robot{
		1: {ID: 1, X: 0, Y: 0},
		2: {ID: 2, X: 5, Y: 5},
	}
	testState.Mu.Unlock()

	req := "GET /status HTTP/1.1\r\n\r\n"
	resp := handleTestRequest(req, t)

	var out map[string]interface{}
	json.Unmarshal([]byte(resp[strings.Index(resp, "{"):]), &out)

	robots, ok := out["robots"].([]interface{})
	if !ok {
		t.Fatalf("expected robots to be an array, got %T", out["robots"])
	}
	if len(robots) != 2 {
		t.Errorf("expected 2 robots, got %d", len(robots))
	}
}

// ---------- Funktionale Tests (UDP) ----------

func TestUDPUpdate(t *testing.T) {
	testState.Mu.Lock()
	testState.NextID = 1
	testState.Robots = map[int]coordinator.Robot{
		1: {ID: 1, X: 0, Y: 0},
	}
	testState.Mu.Unlock()

	go coordinator.StartUDPListener(":9999", testState)
	time.Sleep(100 * time.Millisecond) // Listener starten

	conn, err := net.Dial("udp", "127.0.0.1:9999")
	if err != nil {
		t.Fatalf("cannot connect UDP: %v", err)
	}
	defer conn.Close()

	conn.Write([]byte("1,5,7"))
	time.Sleep(50 * time.Millisecond)

	testState.Mu.RLock()
	rb := testState.Robots[1]
	testState.Mu.RUnlock()

	if rb.X != 5 || rb.Y != 7 {
		t.Errorf("expected (5,7), got (%d,%d)", rb.X, rb.Y)
	}
}

// ---------- Benchmark für Robot Registration ----------

func BenchmarkRobotRegistration(b *testing.B) {
	testState.Mu.Lock()
	testState.NextID = 1
	testState.Robots = make(map[int]coordinator.Robot)
	testState.Mu.Unlock()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		testState.Mu.Lock()
		id := testState.NextID
		testState.NextID++
		testState.Robots[id] = coordinator.Robot{ID: id, X: 0, Y: 0}
		testState.Mu.Unlock()
	}
}
