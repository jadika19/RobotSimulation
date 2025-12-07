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
	"strings"
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

	Walk(ctx, regResp, udpConn, coordHTTP)
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

func Walk(ctx context.Context, r *RegResp, conn *net.UDPConn, tcpAddr string) {
	x, y := r.Start.X, r.Start.Y
	width, height := r.Width, r.Height
	reported := make(map[string]bool)

	forward := true // Richtung des gesamten Ablaufs

	for i := 0; i < 100; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		// Position über UDP senden
		fmt.Fprintf(conn, "%d,%d,%d", r.ID, x, y)

		// Prüfen, ob an dieser Stelle ein Problem liegt
		if eventType, found := CheckForProblem(tcpAddr, x, y); found {
			key := fmt.Sprintf("%d,%d", x, y)
			if !reported[key] {
				reported[key] = true
				go func(px, py int, et string) {
					if err := SendHTTPEvent(tcpAddr, et, px, py); err != nil {
						log.Printf("failed to send event %s at (%d,%d): %v", et, px, py, err)
					}
				}(x, y, eventType)
			}
		}

		// Warteintervall
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}

		// Bewegung je nach Richtung
		if forward {
			// Zick-Zack nach unten
			if y%2 == 0 { // gerade Zeile -> rechts laufen
				if x < width-1 {
					x++
				} else { // rechts angekommen
					if y < height-1 {
						y++
					} else {
						// Unten rechts angekommen -> Richtung umkehren
						forward = false
					}
				}
			} else { // ungerade Zeile -> links laufen
				if x > 0 {
					x--
				} else { // links angekommen
					if y < height-1 {
						y++
					} else {
						forward = false
					}
				}
			}

		} else {
			// Zick-Zack nach oben (Rückweg)
			if y%2 == 0 { // gerade Zeile -> rechts laufen (umgekehrt Muster)
				if x > 0 {
					x--
				} else {
					if y > 0 {
						y--
					} else {
						// Oben links angekommen -> wieder vorwärts
						forward = true
					}
				}
			} else { // ungerade Zeile -> links laufen
				if x < width-1 {
					x++
				} else {
					if y > 0 {
						y--
					} else {
						forward = true
					}
				}
			}
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

func SendHTTPEvent(addr, event string, x, y int) error {
	reqBody := fmt.Sprintf(`{"event":"%s","x":%d,"y":%d}`, event, x, y)
	req, err := http.NewRequest("POST", addr+"/event", strings.NewReader(reqBody))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(reqBody)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %s", resp.Status)
	}
	return nil
}

func CheckForProblem(addr string, x, y int) (string, bool) {
	reqBody := fmt.Sprintf(`{"x":%d,"y":%d}`, x, y)
	req, err := http.NewRequest("POST", addr+"/problem-at", strings.NewReader(reqBody))
	if err != nil {
		return "", false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Length", fmt.Sprintf("%d", len(reqBody)))

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", false
	}
	var res struct {
		Present bool   `json:"present"`
		Type    string `json:"type"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&res); err != nil {
		return "", false
	}
	return res.Type, res.Present
}
