package main

import (
	"log"
	"net/http"
)

func main() {
	// Set the directory to serve
	fs := http.FileServer(http.Dir("./"))

	// Handle all requests by serving a file of the same name
	http.Handle("/", fs)

	// Define the port to listen on
	port := "3000"
	log.Printf("Listening on http://localhost:%s/", port)

	// Start the server
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		log.Fatal(err)
	}
}
