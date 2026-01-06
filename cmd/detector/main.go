package main

import (
	"log"
	"os"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/detector"
)

// Mode switch: true = MQTT mode, false = UDP/HTTP mode
const useMQTT = true

func main() {
	coordHTTP := "http://coordinator:8080"
	worldHTTP := "http://world:8081"
	udpAddr := "coordinator:9001"
	mqttBroker := "tcp://mosquitto:1884"

	// Allow override via environment variables
	if env := os.Getenv("COORDINATOR_ADDR"); env != "" {
		coordHTTP = env
	}
	if env := os.Getenv("WORLD_ADDR"); env != "" {
		worldHTTP = env
	}
	if env := os.Getenv("UDP_ADDR"); env != "" {
		udpAddr = env
	}
	if env := os.Getenv("MQTT_BROKER"); env != "" {
		mqttBroker = env
	}

	if useMQTT {
		log.Println("Starting detector in MQTT mode")
		if err := detector.RunMQTT(coordHTTP, worldHTTP, mqttBroker); err != nil {
			log.Fatal(err)
		}
	} else {
		log.Println("Starting detector in UDP/HTTP mode")
		if err := detector.Run(coordHTTP, worldHTTP, udpAddr); err != nil {
			log.Fatal(err)
		}
	}
}
