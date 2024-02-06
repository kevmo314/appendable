package main

import (
	"flag"
	"fmt"
	"log"
	"log/slog"
	"os"
	"runtime/pprof"
	"time"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/handlers"
	"github.com/kevmo314/appendable/pkg/mmap"
)

func main() {
	var debugFlag, jsonlFlag, csvFlag, showTimings bool
	var indexFilename string

	flag.BoolVar(&debugFlag, "debug", false, "Use logger that prints at the debug-level")
	flag.BoolVar(&jsonlFlag, "jsonl", false, "Use JSONL handler")
	flag.BoolVar(&csvFlag, "csv", false, "Use CSV handler")
	flag.BoolVar(&showTimings, "t", false, "Show time-related metrics")
	flag.StringVar(&indexFilename, "i", "", "Specify the existing index of the file to be opened, writing to stdout")

	flag.Parse()
	logLevel := &slog.LevelVar{}

	if debugFlag {
		logLevel.Set(slog.LevelDebug)
	}

	f, err := os.Create("pprof.out")
	if err != nil {
		log.Fatal("could not create CPU profile: ", err)
	}
	defer f.Close() // error handling omitted for example
	if err := pprof.StartCPUProfile(f); err != nil {
		log.Fatal("could not start CPU profile: ", err)
	}
	defer pprof.StopCPUProfile()

	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: logLevel}))
	slog.SetDefault(logger)

	var totalStart, readStart, writeStart time.Time
	if showTimings {
		totalStart = time.Now()
	}

	flag.Usage = func() {
		fmt.Printf("Usage: %s [-t] [-i index] [-I index] filename\n", os.Args[0])
		flag.PrintDefaults()
		os.Exit(1)
	}

	args := flag.Args()

	if len(args) != 1 {
		flag.Usage()
	}

	// Open the data df
	df, err := mmap.OpenFile(args[0], os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer df.Close()

	var dataHandler appendable.DataHandler

	switch {
	case jsonlFlag:
		dataHandler = handlers.JSONLHandler{}
	// case csvFlag:
	// 	dataHandler = handlers.CSVHandler{}
	default:
		logger.Error("Please specify the file type with -jsonl or -csv.")
		os.Exit(1)
	}
	if showTimings {
		readStart = time.Now()
	}
	mmpif, err := mmap.OpenFile(indexFilename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		panic(err)
	}
	defer mmpif.Close()

	// Open the index file
	i, err := appendable.NewIndexFile(mmpif, dataHandler)
	if err != nil {
		panic(err)
	}

	if err := i.Synchronize(df.Bytes()); err != nil {
		panic(err)
	}

	if showTimings {
		readDuration := time.Since(readStart)
		logger.Info("Opening + synchronizing index file took", slog.Duration("duration", readDuration))
	}

	// Write the index file
	if showTimings {
		writeStart = time.Now()
	}

	if err := mmpif.Close(); err != nil {
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
