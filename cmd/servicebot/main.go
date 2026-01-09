package main

import (
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	servicebots "code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/serviceBots"
)

func main() {
	coordHTTP := os.Getenv("COORD_ADDR")
	if coordHTTP == "" {
		coordHTTP = "http://coordinator:8080"
	}

	mqttBroker := os.Getenv("MQTT_BROKER")
	if mqttBroker == "" {
		mqttBroker = "tcp://mosquitto:1884"
	}

	botType := os.Getenv("BOT_TYPE")
	if botType == "" {
		botType = "cleaner"
	}

	worldAddr := os.Getenv("WORLD_ADDR")
	if worldAddr == "" {
		worldAddr = "http://world:8081"
	}

	// Each bot needs a unique gRPC port - use GRPC_PORT env or default based on hostname
	grpcPort := os.Getenv("GRPC_PORT")
	if grpcPort == "" {
		grpcPort = "50051"
	}

	// Advertised gRPC address: allow explicit override for compose/service DNS
	grpcAdvertise := os.Getenv("GRPC_ADVERTISE")
	if grpcAdvertise == "" {
		hostname, _ := os.Hostname()
		grpcAdvertise = fmt.Sprintf("%s:%s", hostname, grpcPort)
	}
	grpcAddr := grpcAdvertise

	bot := servicebots.New(botType)

	// Parse port for local server
	port, _ := strconv.Atoi(grpcPort)
	bot.GRPCPort = port
	bot.GRPCAdvertise = grpcAdvertise

	// Register with coordinator (for ID assignment only - no task assignment)
	if err := bot.Register(coordHTTP, grpcAddr); err != nil {
		log.Fatalf("failed to register %s bot: %v", botType, err)
	}
	log.Printf("%s bot registered: id=%d at (%d,%d)", botType, bot.ID, bot.X, bot.Y)

	// Connect to MQTT with LWT for leader detection
	log.Println("Starting servicebot in MQTT mode with decentralized coordination")
	if err := bot.ConnectMQTT(mqttBroker); err != nil {
		log.Fatalf("failed to connect MQTT: %v", err)
	}
	defer bot.Close()

	// Initialize election system
	bot.Election = servicebots.NewElectionState()
	bot.Election.WorldAddr = worldAddr
	if err := bot.InitializeElection(); err != nil {
		log.Fatalf("failed to initialize election: %v", err)
	}

	// Publish full status (including gRPC address) for other bots
	bot.PublishFullStatus()
	bot.PublishPosition()

	// Periodically publish full status so leader knows about all bots
	go func() {
		ticker := time.NewTicker(2 * time.Second)
		defer ticker.Stop()
		for range ticker.C {
			bot.PublishFullStatus()
		}
	}()

	// Start leader monitoring in background
	go bot.MonitorLeader()

	// Wait a bit for other bots to come online, then start election
	log.Printf("%s bot id=%d waiting for other bots before starting election...", botType, bot.ID)
	time.Sleep(3 * time.Second)

	// Start election to determine initial leader
	log.Printf("%s bot id=%d starting initial election", botType, bot.ID)
	go bot.StartElection()

	log.Printf("%s bot id=%d is ready, waiting for tasks on gRPC %s...", botType, bot.ID, grpcAddr)

	// Start gRPC server to receive tasks (blocks forever)
	localAddr := fmt.Sprintf(":%s", grpcPort)
	if err := bot.StartGRPCServer(localAddr); err != nil {
		log.Fatalf("gRPC server failed: %v", err)
	}
}
