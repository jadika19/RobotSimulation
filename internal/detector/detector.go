package detector

import (
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

// ---------- Öffentliche Funktionen ----------

func Run(coordHTTP, udpAddr string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	regResp, err := SendHTTPRegistrationRequest(coordHTTP)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	log.Printf("detector id=%d grid=%dx%d start=(%d,%d)",
		regResp.ID, regResp.Width, regResp.Height, regResp.Start.X, regResp.Start.Y)

	udpConn, err := OpenUDPConnection(udpAddr)
	if err != nil {
		return err
	}
	defer udpConn.Close()

	Walk(ctx, regResp, udpConn)
	return nil
}

func SendHTTPRegistrationRequest(addr string) (*RegResp, error) {
	// HTTP-Request erstellen
	req, err := http.NewRequest("POST", addr+"/robot", nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	// TCP-Verbindung aufbauen und Request senden
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("unexpected status: %s", resp.Status)
	}
	// Response parsen
	var regResp RegResp
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return nil, err
	}
	return &regResp, nil
}

func OpenUDPConnection(addr string) (*net.UDPConn, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return conn.(*net.UDPConn), nil
}

func Walk(ctx context.Context, r *RegResp, conn *net.UDPConn) {
	x, y := r.Start.X, r.Start.Y

	for i := 0; i < 100; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		x, y = TakeOneStep(x, y, r.Width, r.Height)
		fmt.Fprintf(conn, "%d,%d,%d", r.ID, x, y)

		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}
	}
}

func TakeOneStep(x, y, width, height int) (int, int) {
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
