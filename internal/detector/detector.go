package detector

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ---------- Datenstrukturen ----------

type RegResp struct {
	ID     int `json:"id"`
	Width  int `json:"width"`
	Height int `json:"height"`
	Start  struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"start"`
}

// ---------- Öffentliche API ----------

// Run startet den gesamten Detektor-Workflow
// Diese Funktion wird aus main.go aufgerufen
func Run(coordHTTP, udpAddr string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	out, err := Register(coordHTTP)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	log.Printf("detector id=%d grid=%dx%d start=(%d,%d)",
		out.ID, out.Width, out.Height, out.Start.X, out.Start.Y)

	udpConn, err := ConnectUDP(udpAddr)
	if err != nil {
		return err
	}
	defer udpConn.Close()

	RunRandomWalk(ctx, out, udpConn)
	return nil
}

// ---------- Registrierung ----------

func Register(coordHTTP string) (*RegResp, error) {
	var out RegResp
	err := postJSON(coordHTTP+"/robot", nil, &out)
	return &out, err
}

// ---------- UDP ----------

func ConnectUDP(addr string) (*net.UDPConn, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return conn.(*net.UDPConn), nil
}

// ---------- Walking + Senden ----------

func RunRandomWalk(ctx context.Context, r *RegResp, conn *net.UDPConn) {
	x, y := r.Start.X, r.Start.Y

	for i := 0; i < 100; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		x, y = StepRandom(x, y, r.Width, r.Height)
		fmt.Fprintf(conn, "%d,%d,%d", r.ID, x, y)

		select {
		case <-ctx.Done():
			return
		case <-time.After(200 * time.Millisecond):
		}
	}
}

func StepRandom(x, y, width, height int) (int, int) {
	switch rand.Intn(4) {
	case 0:
		if y > 0 {
			y--
		}
	case 1:
		if y < height-1 {
			y++
		}
	case 2:
		if x > 0 {
			x--
		}
	case 3:
		if x < width-1 {
			x++
		}
	}
	return x, y
}

// ---------- Hilfsfunktion für POST ----------

func postJSON(url string, body any, out any) error {
	var buf *bytes.Buffer
	if body != nil {
		b, _ := json.Marshal(body)
		buf = bytes.NewBuffer(b)
	} else {
		buf = bytes.NewBuffer(nil)
	}

	req, _ := http.NewRequest("POST", url, buf)
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()
	return json.NewDecoder(resp.Body).Decode(out)
}
