package main

import (
	"log"
	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/detector"
)

func main() {
	coordHTTP := "http://coordinator:8080"
	udpAddr := "coordinator:9001"

	if err := detector.Run(coordHTTP, udpAddr); err != nil {
		log.Fatal(err)
	}
}
