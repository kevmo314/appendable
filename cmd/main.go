package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/kevmo314/appendable/pkg/appendable"
)

func main() {

	var jsonlFlag bool
	var csvFlag bool

	flag.BoolVar(&jsonlFlag, "jsonl", false, "Use JSONL handler")
	flag.BoolVar(&csvFlag, "csv", false, "Use CSV handler")

	var showTimings bool
	flag.BoolVar(&showTimings, "t", false, "Show time-related metrics")

	var totalStart, readStart, writeStart time.Time
	if showTimings {
		totalStart = time.Now()
	}

	// index := flag.String("i", "", "Specify the existing index of the file to be opened, writing to stdout")
	flag.Usage = func() {
		fmt.Printf("Usage: %s [-t] [-i index] [-I index] filename\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}
	flag.Parse()

	args := flag.Args()

	if len(args) != 1 {
		flag.Usage()
	}

	// Open the file
	file, err := os.Open(args[0])
	if err != nil {
		panic(err)
	}


	var dataHandler appendable.DataHandler

	switch {
	case jsonlFlag:
		dataHandler = appendable.JSONLHandler{ReadSeeker: file}
	case csvFlag:
		dataHandler = appendable.CSVHandler{ReadSeeker: file}
	default:
		fmt.Println("Please specify the file type with -jsonl or -csv.")
		os.Exit(1)
	}
  if showTimings {
		readStart = time.Now()
	}
	// Open the index file
	indexFile, err := appendable.NewIndexFile(dataHandler)


	if showTimings {
		readDuration := time.Since(readStart)
		log.Printf("Opening + synchronizing index file took: %s", readDuration)
	}

	if err != nil {
		panic(err)
	}

	// Write the index file
	if showTimings {
		writeStart = time.Now()
	}
	of, err := os.Create(args[0] + ".index")
	if err != nil {
		panic(err)
	}
	log.Printf("Writing index file to %s", args[0]+".index")
	bufof := bufio.NewWriter(of)
	if err := indexFile.Serialize(bufof); err != nil {
		panic(err)
	}
	if err := bufof.Flush(); err != nil {
		panic(err)
	}
	if err := of.Close(); err != nil {
		panic(err)
	}

	if showTimings {
		writeDuration := time.Since(writeStart)
		log.Printf("Writing index file took: %s", writeDuration)

		totalDuration := time.Since(totalStart)
		log.Printf("Total execution time: %s", totalDuration)
	}

	log.Printf("Done!")
}
