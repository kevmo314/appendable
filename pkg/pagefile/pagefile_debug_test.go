package pagefile

import (
	"testing"

	"github.com/kevmo314/appendable/pkg/buftest"
)

func TestWriteAcrossBoundaryPanicsInDebug(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	buf := buftest.NewSeekableBuffer()
	pf, err := NewPageFile(buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.Write(make([]byte, pf.PageSize()+1)); err != nil {
		t.Fatal(err)
	}
}

func TestReadAcrossBoundaryPanicsInDebug(t *testing.T) {
	defer func() {
		if r := recover(); r == nil {
			t.Errorf("The code did not panic")
		}
	}()

	buf := buftest.NewSeekableBuffer()
	pf, err := NewPageFile(buf)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := pf.Read(make([]byte, pf.PageSize()+1)); err != nil {
		t.Fatal(err)
	}
}
