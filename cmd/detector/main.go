package main

import (
	"log"
	"os"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/detector"
)

func main() {
	coordHTTP := "http://coordinator:8080"
	worldHTTP := "http://world:8081"
	mqttBroker := "tcp://mosquitto:1884"

	// Allow override via environment variables
	if env := os.Getenv("COORDINATOR_ADDR"); env != "" {
		coordHTTP = env
	}
	if env := os.Getenv("WORLD_ADDR"); env != "" {
		worldHTTP = env
	}
	if env := os.Getenv("MQTT_BROKER"); env != "" {
		mqttBroker = env
	}

	log.Println("Starting detector in MQTT mode")
	if err := detector.RunMQTT(coordHTTP, worldHTTP, mqttBroker); err != nil {
		log.Fatal(err)
	}
}
