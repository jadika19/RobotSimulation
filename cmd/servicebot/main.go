package main

import (
	"log"
	"os"

	servicebots "code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/serviceBots"
)

func main() {
	coordHTTP := "http://coordinator:8080"
	udpAddr := "coordinator:9001"

	botType := os.Getenv("BOT_TYPE")
	if botType == "" {
		botType = "cleaner"
	}

	bot := servicebots.New(botType)

	if err := bot.Register(coordHTTP); err != nil {
		log.Fatalf("failed to register %s bot: %v", botType, err)
	}
	log.Printf("%s bot registered: id=%d at (%d,%d)", botType, bot.ID, bot.X, bot.Y)

	if err := bot.ConnectUDP(udpAddr); err != nil {
		log.Fatalf("failed to connect UDP: %v", err)
	}
	defer bot.Close()

	// Send initial position
	bot.SendPosition()

	log.Printf("%s bot id=%d is idle, waiting for tasks...", botType, bot.ID)

	// Idle loop - service bots wait for commands (to be implemented via RPC later)
	select {} // block forever for now
}
