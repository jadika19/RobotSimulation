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
