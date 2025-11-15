package detector_test

import (
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/detector"
)

func TestRegister(t *testing.T) {
	// Mock Coordinator HTTP Server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		resp := detector.RegResp{
			ID:     1,
			Width:  10,
			Height: 10,
		}
		resp.Start.X = 5
		resp.Start.Y = 5
		json.NewEncoder(w).Encode(resp)
	}))
	defer server.Close()

	out, err := detector.Register(server.URL)
	if err != nil {
		t.Fatalf("Register failed: %v", err)
	}
	if out.ID != 1 || out.Start.X != 5 || out.Start.Y != 5 {
		t.Errorf("unexpected register output: %+v", out)
	}
}

func TestRunRandomWalk_UDP(t *testing.T) {
	// Einfacher UDP-Server zum Auffangen der Nachrichten
	addr := "127.0.0.1:0" 
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer pc.Close()
	udpAddr := pc.LocalAddr().String()

	// RegResp Startpunkt
	r := &detector.RegResp{
		ID:     1,
		Width:  5,
		Height: 5,
	}
	r.Start.X = 2
	r.Start.Y = 2

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		pc.SetDeadline(time.Now().Add(3 * time.Second))
		n, _, _ := pc.ReadFrom(buf)
		if n == 0 {
			t.Error("no UDP message received")
		}
	}()

	conn, err := detector.ConnectUDP(udpAddr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	detector.RunRandomWalk(r, conn)

	wg.Wait()
}

func BenchmarkRandomWalkStep(b *testing.B) {
	r := &detector.RegResp{
    ID:     1,
    Width:  5,
    Height: 5,
    Start: struct {
        X int `json:"x"`
        Y int `json:"y"`
    }{
        X: 2,
        Y: 2,
    },
}

	addr := "127.0.0.1:0"
	pc, err := net.ListenPacket("udp", addr)
	if err != nil {
		b.Fatal(err)
	}
	defer pc.Close()
	udpAddr := pc.LocalAddr().String()

	conn, err := detector.ConnectUDP(udpAddr)
	if err != nil {
		b.Fatal(err)
	}
	defer conn.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		detector.StepRandom(r.Start.X, r.Start.Y, r.Width, r.Height)
	}
}
