package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"os"

	"github.com/kevmo314/appendable/pkg/appendable"
)

func main() {

	var jsonlFlag bool
	var csvFlag bool

	flag.BoolVar(&jsonlFlag, "jsonl", false, "Use JSONL handler")
	flag.BoolVar(&csvFlag, "csv", false, "Use CSV handler")

	// index := flag.String("i", "", "Specify the existing index of the file to be opened, writing to stdout")
	flag.Usage = func() {
		fmt.Printf("Usage: %s [-i index] [-I index] [-jsonl or -csv] filename\n", os.Args[0])
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

	// Open the index file
	indexFile, err := appendable.NewIndexFile(dataHandler)
	if err != nil {
		panic(err)
	}

	// Write the index file
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
	log.Printf("Done!")
}
