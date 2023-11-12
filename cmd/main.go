package main

import (
	"fmt"
	"os"
)

func usage() {
	fmt.Printf("Usage: %s [-i <index filename>] <data filename>\n", os.Args[0])
	fmt.Println("Options:")
	fmt.Println("\t-i <index filename> - specify the existing index of the file to be opened, writing to stdout")
	fmt.Println("\t-I <index filename> - specify the existing index of the file to be opened, mutating it in-place if it exists")
	os.Exit(1)
}

func main() {
	if len(os.Args) < 2 {
		usage()
	}

	var index string
	var filename string

	if len(os.Args) == 2 {
		filename = os.Args[1]
	} else if len(os.Args) == 4 {
		if os.Args[1] != "-i" {
			usage()
		}
		filename = os.Args[3]
		index = os.Args[2]
	} else {
		usage()
	}

	// Open the file
	file, err := os.Open(filename)
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}

	// Open the index file

}
