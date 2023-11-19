package main

import (
	"flag"
	"fmt"
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

	// Open the index file
	indexFile, err := appendable.NewIndexFile(file)
	if err != nil {
		panic(err)
	}

	// Write the index file
	of, err := os.Create(args[0] + ".index")
	if err != nil {
		panic(err)
	}
	if err := indexFile.Serialize(of); err != nil {
		panic(err)
	}
}
