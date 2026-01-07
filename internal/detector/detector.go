package detector

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/mqtt"
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

// ---------- MQTT Mode Functions ----------

// RunMQTT runs the detector in MQTT mode (publishes positions and events to MQTT broker)
func RunMQTT(coordHTTP, worldHTTP, mqttBroker string) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-sigChan
		cancel()
	}()

	// Register with coordinator via HTTP to get ID and grid info
	regResp, err := SendHTTPRegistrationRequest(coordHTTP)
	if err != nil {
		return fmt.Errorf("register: %w", err)
	}

	log.Printf("detector id=%d grid=%dx%d start=(%d,%d)",
		regResp.ID, regResp.Width, regResp.Height, regResp.Start.X, regResp.Start.Y)

	// Initialize MQTT client with Last Will Testament
	config := mqtt.Config{
		BrokerURL:   mqttBroker,
		ClientID:    fmt.Sprintf("detector-%d", regResp.ID),
		WillEnabled: true,
		WillTopic:   fmt.Sprintf("devices/detector/%d/status", regResp.ID),
		WillPayload: "offline",
		WillQoS:     1,
		WillRetain:  true,
	}

	mqttClient, err := mqtt.NewClient(config)
	if err != nil {
		return fmt.Errorf("mqtt connect: %w", err)
	}
	defer mqttClient.Disconnect(1000)

	// Publish online status
	if err := mqttClient.PublishStatus("detector", regResp.ID, "online"); err != nil {
		log.Printf("Failed to publish online status: %v", err)
	}

	// Start walking and publishing positions/events via MQTT
	WalkMQTT(ctx, regResp, mqttClient, worldHTTP)

	// Publish offline status on graceful shutdown
	if err := mqttClient.PublishStatus("detector", regResp.ID, "offline"); err != nil {
		log.Printf("Failed to publish offline status: %v", err)
	}

	return nil
}

// WalkMQTT performs the same zigzag walking pattern but publishes via MQTT
func WalkMQTT(ctx context.Context, r *RegResp, client *mqtt.Client, worldAddr string) {
	x, y := r.Start.X, r.Start.Y
	width, height := r.Width, r.Height
	reported := make(map[string]bool)

	forward := true // Direction of overall movement

	for i := 0; i < 1000; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}

		// Publish position via MQTT
		posMsg := mqtt.PositionMessage{
			ID:        r.ID,
			X:         x,
			Y:         y,
			Timestamp: time.Now().Format(time.RFC3339),
		}
		if err := client.PublishPosition("detector", posMsg); err != nil {
			log.Printf("Failed to publish position: %v", err)
		}

		// Check for problem at current position (query world service)
		if eventType, found := CheckForProblem(worldAddr, x, y); found {
			key := fmt.Sprintf("%d,%d", x, y)
			if !reported[key] {
				reported[key] = true
				// Publish event via MQTT
				go func(px, py int, et string) {
					eventMsg := mqtt.EventMessage{
						DetectorID: r.ID,
						Type:       et,
						X:          px,
						Y:          py,
						Timestamp:  time.Now().Format(time.RFC3339),
					}
					if err := client.PublishEvent(eventMsg); err != nil {
						log.Printf("Failed to publish event %s at (%d,%d): %v", et, px, py, err)
					}
				}(x, y, eventType)
			}
		}

		// Wait interval
		select {
		case <-ctx.Done():
			return
		case <-time.After(500 * time.Millisecond):
		}

		// Movement logic (same zigzag pattern as UDP mode)
		if forward {
			// Zigzag downward
			if y%2 == 0 { // even row -> move right
				if x < width-1 {
					x++
				} else { // reached right edge
					if y < height-1 {
						y++
					} else {
						// Reached bottom right -> reverse direction
						forward = false
					}
				}
			} else { // odd row -> move left
				if x > 0 {
					x--
				} else { // reached left edge
					if y < height-1 {
						y++
					} else {
						forward = false
					}
				}
			}

		} else {
			// Zigzag upward (return path)
			if y%2 == 0 { // even row -> move left (reverse pattern)
				if x > 0 {
					x--
				} else {
					if y > 0 {
						y--
					} else {
						// Reached top left -> forward again
						forward = true
					}
				}
			} else { // odd row -> move right
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
