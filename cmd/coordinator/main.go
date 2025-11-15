package main

import (
	"log"
	"net"
	"code.fbi.h-da.de/distributed-systems/praktika/lab-for-distributed-systems-ws-2526/burchard/Di1y_2/internal/coordinator"
)

func main() {
	addr := ":8080"
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("coordinator listening on", addr)

	go coordinator.UDPListen(":9001", coordinator.St)

	for {
		conn, err := ln.Accept()
		if err != nil {
			continue
		}
		go coordinator.HandleHTTP(conn, coordinator.St)
	}
}
