package main

import (
	"os"
	"testing"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/handlers"
	"github.com/kevmo314/appendable/pkg/mmap"
)

func TestIndexFile(t *testing.T) {

	// Open the data df
	df, err := mmap.OpenFile("../examples/workspace/green_tripdata_2023-01.jsonl", os.O_RDONLY, 0)
	if err != nil {
		panic(err)
	}
	defer df.Close()

	var dataHandler = handlers.JSONLHandler{}

	mmpif, err := mmap.OpenFile("../pkg/mocks/mock.index", os.O_RDWR|os.O_CREATE, 0666)
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

}
