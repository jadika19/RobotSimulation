package servicebots

import (
	"encoding/json"
	"net/http"
)

type RegResp struct {
	ID     int `json:"id"`
	Type   string `json:"type"`
	Width  int `json:"width"`
	Height int `json:"height"`
	Start  struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"start"`
}

type State struct {
	State  string
	X      int
	Y      int
}

var St = &State{}

func sendHTTPRegistrationRequest(coordHTTP string, robotType string) (*RegResp, error) {
	req, err := http.NewRequest("POST", coordHTTP+"/"+robotType, nil)
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