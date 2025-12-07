package servicebots

import (
	"encoding/json"
	"net/http"
	"fmt"
	"net"
	"time"
)

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

type Map struct {
	Width  int
	Height int
}

type ServiceBot struct {
	ID        int
	RobotType string
	State     string
	X, Y      int
	Map       *Map
}

// NewServiceBot erzeugt einen neuen Bot mit Typ ("cleaner-robot" oder "repair-robot")
func NewServiceBot(robotType string) *ServiceBot {
	return &ServiceBot{
		RobotType: robotType,
		State:     "idle",
	}
}

// Register registriert den Bot beim Server und initialisiert seine Map
func (bot *ServiceBot) Register(tcpAddr string) error {
	regResp, err := sendHTTPRegistrationRequest(tcpAddr, bot.RobotType)
	if err != nil {
		return err
	}

	bot.ID = regResp.ID
	bot.X = regResp.Start.X
	bot.Y = regResp.Start.Y
	bot.State = "idle"
	bot.Map = &Map{
		Width:  regResp.Width,
		Height: regResp.Height,
	}

	return nil
}

// sendHTTPRegistrationRequest sendet die POST-Anfrage zum Server
func sendHTTPRegistrationRequest(tcpAddr, robotType string) (*RegResp, error) {
	req, err := http.NewRequest("POST", tcpAddr+"/"+robotType, nil)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var regResp RegResp
	if err := json.NewDecoder(resp.Body).Decode(&regResp); err != nil {
		return nil, err
	}
	return &regResp, nil
}

func (bot *ServiceBot) MoveTo(x, y int, udpAddr string) error {
	conn, err := OpenUDPConnection(udpAddr)
	if err != nil {
		return err
	}
	defer conn.Close()

	bot.State = "busy"

	// Bewegung abwechselnd in X- und Y-Richtung
	for bot.X != x && bot.Y != y {
		if bot.X < x {
			bot.X++
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.State)
		} else if bot.X > x {
			bot.X--
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.State)
		}

		if bot.Y < y {
			bot.Y++
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.State)
		} else if bot.Y > y {
			bot.Y--
			fmt.Fprintf(conn, "%d,%d,%d,%s", bot.ID, bot.X, bot.Y, bot.State)
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

func (bot *ServiceBot) Work() {
	// Simuliere Arbeit durch Schlafen lol 
	time.Sleep(2 * time.Second)
}