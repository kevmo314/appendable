package main

import (
	"bufio"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"time"

	"github.com/kevmo314/appendable/pkg/appendable"
)

func main() {
	var debugFlag, jsonlFlag, csvFlag, showTimings bool

	flag.BoolVar(&debugFlag, "debug", false, "Use logger that prints at the debug-level")
	flag.BoolVar(&jsonlFlag, "jsonl", false, "Use JSONL handler")
	flag.BoolVar(&csvFlag, "csv", false, "Use CSV handler")
	flag.BoolVar(&showTimings, "t", false, "Show time-related metrics")

	flag.Parse()

	logLevel := &slog.LevelVar{}

	if debugFlag {
		logLevel.Set(slog.LevelDebug)
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

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
		logger.Error("Please specify the file type with -jsonl or -csv.")
		os.Exit(1)
	}
	if showTimings {
		readStart = time.Now()
	}
	// Open the index file
	indexFile, err := appendable.NewIndexFile(dataHandler)

	if err != nil {
		panic(err)
	}

	if showTimings {
		readDuration := time.Since(readStart)
		logger.Info("Opening + synchronizing index file took", slog.Duration("duration", readDuration))
	}

	var indexHeaders []string

	for _, index := range indexFile.Indexes {
		indexHeaders = append(indexHeaders, index.FieldName)
	}

	fmt.Printf("index headers: %v, len: %v\n", indexHeaders, len(indexHeaders))

	// Write the index file
	if showTimings {
		writeStart = time.Now()
	}
	of, err := os.Create(args[0] + ".index")
	if err != nil {
		panic(err)
	}
	logger.Info("Writing index file to", slog.String("path", args[0]+".index"))
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
		logger.Info("Writing index file took", slog.Duration("duration", writeDuration))

		totalDuration := time.Since(totalStart)
		logger.Info("Total execution time", slog.Duration("duration", totalDuration))
	}

	logger.Info("Done!")
}
