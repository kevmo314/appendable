package mmap

import (
	"fmt"
	"io"
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

	t.Run("ReadAt over bounds returns EOF", func(t *testing.T) {
		f, err := os.CreateTemp("", "readoverbounds")
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
		if _, err := m.ReadAt(b, 1); err != io.EOF {
			log.Fatal(err)
		}
	})

	t.Run("Read over bounds returns EOF", func(t *testing.T) {
		f, err := os.CreateTemp("", "readoverbounds")
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
		n, err := m.Seek(1, io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected %d, got %d", 1, n)
		}

		if _, err := m.Read(b); err != io.EOF {
			log.Fatal(err)
		}
	})
}

func TestMemoryMappedFile_Write(t *testing.T) {
	t.Run("Write", func(t *testing.T) {
		f, err := os.CreateTemp("", "write")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		if _, err := m.Write([]byte("Hello, world!")); err != nil {
			log.Fatal(err)
		}

		b := make([]byte, 13)
		if _, err := f.ReadAt(b, 0); err != nil {
			log.Fatal(err)
		}
		if string(b) != "Hello, world!" {
			t.Fatalf("expected %s, got %s", "Hello, world!", string(b))
		}
	})

	t.Run("Write writes at seek location", func(t *testing.T) {
		f, err := os.CreateTemp("", "writeseek")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		// write some data to the file
		if _, err := m.Write([]byte("Hello, zz")); err != nil {
			log.Fatal(err)
		}

		n, err := m.Seek(6, io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}
		if n != 6 {
			t.Fatalf("expected %d, got %d", 6, n)
		}

		if _, err := m.Write([]byte("world!")); err != nil {
			log.Fatal(err)
		}

		b := make([]byte, 12)
		if _, err := f.ReadAt(b, 0); err != nil {
			log.Fatal(err)
		}
		if string(b) != "Hello,world!" {
			t.Fatalf("expected %s, got %s", "Hello,world!", string(b))
		}
	})

	t.Run("WriteAt", func(t *testing.T) {
		f, err := os.CreateTemp("", "writeat")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		if _, err := m.WriteAt([]byte("Hello"), 0); err != nil {
			log.Fatal(err)
		}

		if _, err := m.WriteAt([]byte(", world!"), 5); err != nil {
			log.Fatal(err)
		}

		b := make([]byte, 13)
		if _, err := f.ReadAt(b, 0); err != nil {
			log.Fatal(err)
		}
		if string(b) != "Hello, world!" {
			t.Fatalf("expected %s, got %s", "Hello, world!", string(b))
		}
	})
}

func TestMemoryMappedFile_Seek(t *testing.T) {
	t.Run("SeekStart", func(t *testing.T) {
		f, err := os.CreateTemp("", "seekstart")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		if _, err := m.Write([]byte("Hello, world!")); err != nil {
			log.Fatal(err)
		}

		n, err := m.Seek(1, io.SeekStart)
		if err != nil {
			log.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("expected %d, got %d", 1, n)
		}
	})

	t.Run("SeekCurrent", func(t *testing.T) {
		f, err := os.CreateTemp("", "seekcurrent")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		if _, err := m.Write([]byte("Hello, world!")); err != nil {
			log.Fatal(err)
		}

		n1, err := m.Seek(-2, io.SeekCurrent)
		if err != nil {
			log.Fatal(err)
		}
		if n1 != 11 {
			t.Fatalf("expected %d, got %d", 1, n1)
		}

		n2, err := m.Seek(1, io.SeekCurrent)
		if err != nil {
			log.Fatal(err)
		}
		if n2 != 12 {
			t.Fatalf("expected %d, got %d", 1, n2)
		}
	})

	t.Run("SeekEnd", func(t *testing.T) {
		f, err := os.CreateTemp("", "seekend")
		if err != nil {
			log.Fatal(err)
		}
		defer f.Close()
		defer os.Remove(f.Name())

		m, err := NewMemoryMappedFile(f)
		if err != nil {
			log.Fatal(err)
		}
		defer m.Close()

		if _, err := m.Write([]byte("Hello, world!")); err != nil {
			log.Fatal(err)
		}

		n1, err := m.Seek(-2, io.SeekEnd)
		if err != nil {
			log.Fatal(err)
		}
		if n1 != 11 {
			t.Fatalf("expected %d, got %d", 1, n1)
		}

		n2, err := m.Seek(-1, io.SeekEnd)
		if err != nil {
			log.Fatal(err)
		}
		if n2 != 12 {
			t.Fatalf("expected %d, got %d", 1, n2)
		}
	})
}
