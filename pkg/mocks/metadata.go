package main

import (
	"log"
	"os"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/metapage"
	"github.com/kevmo314/appendable/pkg/pagefile"
)

func generateFilledMetadata() {
	b := buftest.NewSeekableBuffer()
	p, err := pagefile.NewPageFile(b)
	if err != nil {
		log.Fatalf("%v", err)
	}
	tree, err := metapage.NewMultiBPTree(p, 0)
	if err != nil {
		log.Fatalf("%v", err)
	}
	node, err := tree.AddNext()
	if err != nil {
		log.Fatalf("%v", err)
	}
	if err := node.SetMetadata([]byte("hello")); err != nil {
		log.Fatalf("%v", err)
	}

	b.WriteToDisk("filled_metadata.bin")
}

func writeByteToFile(data []byte, filename string) error {
	if err := os.WriteFile(filename, data, 0644); err != nil {
		return err
	}
	return nil
}

func generateFileMeta() {
	fm := appendable.FileMeta{}
	fm.Format = 1
	fm.Version = 1
	fm.ReadOffset = 4096
	fm.Entries = 34

	b, err := fm.MarshalBinary()
	if err != nil {
		log.Fatalf("failed to write file meta to disk")
	}

	if err := writeByteToFile(b, "filemeta.bin"); err != nil {
		log.Fatalf("failed to write bytes to disk")
	}
}

func generateIndexMeta() {
	im := appendable.IndexMeta{}
	im.FieldName = "howdydo"
	im.FieldType = appendable.FieldTypeBoolean
	im.Width = appendable.DetermineType(appendable.FieldTypeBoolean)
	im.TotalFieldValueLength = 773424601

	b, err := im.MarshalBinary()
	if err != nil {
		log.Fatal("failed to write index meta to disk")
	}

	if err := writeByteToFile(b, "indexmeta.bin"); err != nil {
		log.Fatalf("failed to write bytes to disk")
	}
}
