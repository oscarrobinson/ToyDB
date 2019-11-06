package main

import (
	"io"
	"net"
	"log"
	"os"
	"fmt"
)

func main() {
	l, err := net.Listen("tcp", ":2000")
	if err != nil {
		log.Fatal(err)
	}
	defer l.Close()

	for {
		// Wait for a connection.
		conn, err := l.Accept()
		if err != nil {
			log.Fatal(err)
		}
		// Handle the connection in a new goroutine.
		// The loop then returns to accepting, so that
		// multiple connections may be served concurrently.
		go func(c net.Conn) {
			fmt.Println("New connection")
			// Echo all incoming data.
			io.Copy(os.Stdout, c)
			// Shut down the connection.
			c.Close()
		}(conn)
	}
}
