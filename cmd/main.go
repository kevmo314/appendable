package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/kevmo314/appendable/pkg/appendable"
	"go.uber.org/zap"
)

func main() {
	var debugFlag, jsonlFlag, csvFlag, showTimings bool

	flag.BoolVar(&debugFlag, "debug", false, "Use logger that prints at the debug-level")
	flag.BoolVar(&jsonlFlag, "jsonl", false, "Use JSONL handler")
	flag.BoolVar(&csvFlag, "csv", false, "Use CSV handler")
	flag.BoolVar(&showTimings, "t", false, "Show time-related metrics")

	flag.Parse()

	var logger *zap.Logger
	var err error

	if debugFlag {
		logger, err = zap.NewDevelopment()

		if err != nil {
			panic("cannot initialize zap logger: " + err.Error())
		}
	} else {
		logger, err = zap.NewProduction()

		if err != nil {
			panic("cannot initialize zap logger: " + err.Error())
		}
	}

	defer logger.Sync()
	sugar := logger.Sugar()

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
		dataHandler = appendable.JSONLHandler{
			ReadSeeker: file,
		}
	case csvFlag:
		dataHandler = appendable.CSVHandler{
			ReadSeeker: file,
		}
	default:
		sugar.Fatal("Please specify the file type with -jsonl or -csv.")
	}
	if showTimings {
		readStart = time.Now()
	}
	// Open the index file
	indexFile, err := appendable.NewIndexFile(dataHandler, sugar)

	if err != nil {
		sugar.Panic(err)
	}

	if showTimings {
		readDuration := time.Since(readStart)
		sugar.Infof("Opening + synchronizing index file took: %s", readDuration)
	}

	// Write the index file
	if showTimings {
		writeStart = time.Now()
	}
	of, err := os.Create(args[0] + ".index")
	if err != nil {
		sugar.Panic(err)
	}
	sugar.Infof("Writing index file to %s", args[0]+".index")
	bufof := bufio.NewWriter(of)
	if err := indexFile.Serialize(bufof); err != nil {
		sugar.Panic(err)
	}
	if err := bufof.Flush(); err != nil {
		sugar.Panic(err)
	}
	if err := of.Close(); err != nil {
		sugar.Panic(err)
	}

	if showTimings {
		writeDuration := time.Since(writeStart)
		sugar.Infof("Writing index file took: %s", writeDuration)

		totalDuration := time.Since(totalStart)
		sugar.Infof("Total execution time: %s", totalDuration)
	}

	sugar.Info("Done!")
}
