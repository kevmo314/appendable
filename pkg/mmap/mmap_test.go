package mmap

import (
	"fmt"
	"log"
	"os"
	"testing"
)

func ExampleMemoryMappedFile() {
	// Open a file.
	f, err := os.CreateTemp("", "example")
	if err != nil {
		log.Fatal(err)
	}
	defer f.Close()

	// Create a memory-mapped file.
	m, err := NewMemoryMappedFile(f)
	if err != nil {
		log.Fatal(err)
	}
	defer m.Close()

	// Write to the memory-mapped file.
	if _, err := m.WriteAt([]byte("Hello, world!"), 0); err != nil {
		log.Fatal(err)
	}

	// Read from the memory-mapped file.
	b := make([]byte, 13)
	if _, err := m.ReadAt(b, 0); err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(b))
}

func TestMemoryMappedFile_Read(t *testing.T) {
	t.Run("Read", func(t *testing.T) {
		f, err := os.CreateTemp("", "read")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		// write some data to the file
		if _, err := f.Write([]byte("Hello, world!")); err != nil {
			log.Fatal(err)
		}

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		b := make([]byte, 13)
		if _, err := m.Read(b); err != nil {
			log.Fatal(err)
		}
		if string(b) != "Hello, world!" {
			t.Fatalf("expected %s, got %s", "Hello, world!", string(b))
		}
	})

	t.Run("ReadAt", func(t *testing.T) {
		f, err := os.CreateTemp("", "readat")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		// write some data to the file
		if _, err := f.Write([]byte("Hello, world!")); err != nil {
			log.Fatal(err)
		}

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		b := make([]byte, 12)
		if _, err := m.ReadAt(b, 1); err != nil {
			log.Fatal(err)
		}
		if string(b) != "ello, world!" {
			t.Fatalf("expected %s, got %s", "ello, world!", string(b))
		}
	})
}
