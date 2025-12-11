package main

import (
	"log"
	"net"
	"os"

	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/coordinator"
)

func main() {
	addr := ":8080"
	grpcAddr := ":9002" // gRPC callback server port

	// Allow overriding via environment
	if v := os.Getenv("WORLD_ADDR"); v != "" {
		coordinator.St.WorldAddr = v
	}

	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("coordinator listening on", addr)

	go coordinator.StartUDPListener(":9001", coordinator.St)
	go coordinator.StartGRPCCallbackServer(grpcAddr, coordinator.St)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go coordinator.HandleHTTPRequest(conn, coordinator.St)
	}
}
