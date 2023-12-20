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
	// index := flag.String("i", "", "Specify the existing index of the file to be opened, writing to stdout")
	flag.Usage = func() {
		fmt.Printf("Usage: %s [-i index] [-I index] filename\n", os.Args[0])
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

	// Determine file type, assign correct handler
	handler, err := appendable.DetermineDataHandler(args[0])
	if err != nil {
		panic(err)
	}

	// Open the index file
	indexFile, err := appendable.NewIndexFile(file, handler)
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
