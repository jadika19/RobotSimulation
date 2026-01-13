package main

import (
	"log"
	"net"
	"os"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/world"
)

func main() {
	addr := os.Getenv("WORLD_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	// Initialize MQTT connection
	mqttBroker := os.Getenv("MQTT_BROKER")
	if mqttBroker == "" {
		mqttBroker = "tcp://mosquitto:1883"
	}

	if err := world.St.InitializeMQTT(mqttBroker); err != nil {
		log.Printf("Warning: Failed to initialize MQTT: %v", err)
		log.Println("Continuing without MQTT - leader status will not be available")
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	defer ln.Close()
	log.Println("world service listening on", addr)

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Println("accept error:", err)
			continue
		}
		go world.HandleHTTPRequest(conn, world.St)
	}
}
