package main

import (
	"fmt"
	"log"
	"os"
	"strconv"

	servicebots "code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/serviceBots"
)

func main() {
	coordHTTP := os.Getenv("COORD_ADDR")
	if coordHTTP == "" {
		coordHTTP = "http://coordinator:8080"
	}

	coordGRPC := os.Getenv("COORD_GRPC")
	if coordGRPC == "" {
		coordGRPC = "coordinator:9002"
	}

	mqttBroker := os.Getenv("MQTT_BROKER")
	if mqttBroker == "" {
		mqttBroker = "tcp://mosquitto:1884"
	}

	botType := os.Getenv("BOT_TYPE")
	if botType == "" {
		botType = "cleaner"
	}

	// Each bot needs a unique gRPC port - use GRPC_PORT env or default based on hostname
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	// Get hostname for unique identification
	hostname, _ := os.Hostname()
	grpcAddr := fmt.Sprintf("%s:%s", hostname, grpcPort)

	bot := servicebots.New(botType)
	bot.CoordGRPCAddr = coordGRPC

	// Parse port for local server
	port, _ := strconv.Atoi(grpcPort)
	bot.GRPCPort = port

	if err := bot.Register(coordHTTP, grpcAddr); err != nil {
		log.Fatalf("failed to register %s bot: %v", botType, err)
	}
	log.Printf("%s bot registered: id=%d at (%d,%d)", botType, bot.ID, bot.X, bot.Y)

	log.Println("Starting servicebot in MQTT mode")
	if err := bot.ConnectMQTT(mqttBroker); err != nil {
		log.Fatalf("failed to connect MQTT: %v", err)
	}
	defer bot.Close()

	// Publish online status and initial position
	bot.PublishStatus("online")
	bot.PublishPosition()

	log.Printf("%s bot id=%d is idle, waiting for tasks on gRPC %s...", botType, bot.ID, grpcAddr)

	// Start gRPC server to receive tasks (blocks forever)
	localAddr := fmt.Sprintf(":%s", grpcPort)
	if err := bot.StartGRPCServer(localAddr); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
