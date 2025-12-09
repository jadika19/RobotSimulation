package servicebots

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
)

// RegResp is the response from coordinator on registration
type RegResp struct {
	ID     int    `json:"id"`
	Type   string `json:"type"`
	Width  int    `json:"width"`
	Height int    `json:"height"`
	Start  struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"start"`
}

// ServiceBot represents a cleaner or repair robot
type ServiceBot struct {
	ID      int
	Type    string // "cleaner" or "repair"
	Status  string // "idle" or "busy"
	X, Y    int
	Width   int
	Height  int
	udpConn *net.UDPConn
}

// New creates a new service bot with the given type
func New(botType string) *ServiceBot {
	return &ServiceBot{
		Type:   botType,
		Status: "idle",
	}
}

// Register registers the bot with the coordinator via HTTP
func (bot *ServiceBot) Register(coordAddr string) error {
	endpoint := "/" + bot.Type + "-robot"
	req, err := http.NewRequest("POST", coordAddr+endpoint, nil)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("registration failed: %s", resp.Status)
	}

	var regResp RegResp
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return err
	}

	bot.ID = regResp.ID
	bot.X = regResp.Start.X
	bot.Y = regResp.Start.Y
	bot.Width = regResp.Width
	bot.Height = regResp.Height
	bot.Status = "idle"

	return nil
}

// ConnectUDP opens a UDP connection for position updates
func (bot *ServiceBot) ConnectUDP(udpAddr string) error {
	conn, err := net.Dial("udp", udpAddr)
	if err != nil {
		return err
	}
	bot.udpConn = conn.(*net.UDPConn)
	return nil
}

// Close closes the UDP connection
func (bot *ServiceBot) Close() {
	if bot.udpConn != nil {
		bot.udpConn.Close()
	}
}

// SendPosition sends current position to coordinator via UDP
// This is to be implemented with RPC later
func (bot *ServiceBot) SendPosition() {
	if bot.udpConn == nil {
		return
	}
	// Same format as detector: "id,x,y"
	fmt.Fprintf(bot.udpConn, "%d,%d,%d", bot.ID, bot.X, bot.Y)
}

func (bot *ServiceBot) MoveTo(x, y int, udpAddr string) error {
	conn, err := OpenUDPConnection(udpAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	bot.Status = "busy"

	// Bewegung abwechselnd in X- und Y-Richtung
	for bot.X != x && bot.Y != y {
		if bot.X < x {
			bot.X++
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.Status)
		} else if bot.X > x {
			bot.X--
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.Status)
		}

		if bot.Y < y {
			bot.Y++
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.Status)
		} else if bot.Y > y {
			bot.Y--
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.Status)
		}
	}
	return nil
}

func OpenUDPConnection(addr string) (*net.UDPConn, error) {
	conn, err := net.Dial("udp", addr)
	if err != nil {
		return nil, err
	}
	return conn.(*net.UDPConn), nil
}