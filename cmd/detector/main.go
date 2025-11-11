package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"time"
)

type regResp struct {
	ID     int `json:"id"`
	Width  int `json:"width"`
	Height int `json:"height"`
	Start  struct {
		X int `json:"x"`
		Y int `json:"y"`
	} `json:"start"`
}

func main() {
	coordHTTP := "http://localhost:8080"
	udpAddr := "localhost:9001"

	// 1) register
	var out regResp
	if err := postJSON(coordHTTP+"/robot", nil, &out); err != nil {
		log.Fatal("register:", err)
	}
	log.Printf("detector id=%d grid=%dx%d start=(%d,%d)", out.ID, out.Width, out.Height, out.Start.X, out.Start.Y)

	// 2) UDP dial
	udpConn, err := net.Dial("udp", udpAddr)
	if err != nil {
		log.Fatal(err)
	}
	defer udpConn.Close()

	// 3) random N4 walk + send id,x,y
	x, y := out.Start.X, out.Start.Y
	for range 100 {
		switch rand.Intn(4) {
		case 0:
			if y > 0 {
				y--
			} // up
		case 1:
			if y < out.Height-1 {
				y++
			} // down
		case 2:
			if x > 0 {
				x--
			} // left
		case 3:
			if x < out.Width-1 {
				x++
			} // right
		}

		fmt.Fprintf(udpConn, "%d,%d,%d", out.ID, x, y)
		time.Sleep(200 * time.Millisecond)
	}
}

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
