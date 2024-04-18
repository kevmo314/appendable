package appendable

import (
	"github.com/kevmo314/appendable/pkg/buftest"
	"github.com/kevmo314/appendable/pkg/metapage"
	"github.com/kevmo314/appendable/pkg/pagefile"
	"reflect"
	"testing"
)

func TestMarshalMetadata(t *testing.T) {
	t.Run("file meta", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}

		tree, err := metapage.NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}

		page, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}

		fm := &FileMeta{
			Version:    1,
			Format:     1,
			ReadOffset: 69,
			Entries:    38,
		}

		buf, err := fm.MarshalBinary()
		if err != nil {
			t.Fatalf("Failed to marshal binary: %v", err)
		}

		if err := page.SetMetadata(buf); err != nil {
			t.Fatal(err)
		}

		// finished marshaling
		// <-------->
		// start unmarshal

		buf, err = page.Metadata()
		if err != nil {
			t.Fatal(err)
		}

		fm2 := &FileMeta{}

		if err := fm2.UnmarshalBinary(buf); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(fm, fm2) {
			t.Fatal("not equal")
		}
	})

	t.Run("file meta", func(t *testing.T) {
		b := buftest.NewSeekableBuffer()
		p, err := pagefile.NewPageFile(b)
		if err != nil {
			t.Fatal(err)
		}

		tree, err := metapage.NewMultiBTree(p, 0)
		if err != nil {
			t.Fatal(err)
		}

		page, err := tree.AddNext()
		if err != nil {
			t.Fatal(err)
		}

		im := &IndexMeta{
			FieldName:             "scarface",
			FieldType:             FieldTypeString,
			Width:                 0,
			TotalFieldValueLength: 938,
		}
		buf, err := im.MarshalBinary()
		if err != nil {
			t.Fatalf("Failed to marshal binary: %v", err)
		}

		if err := page.SetMetadata(buf); err != nil {
			t.Fatal(err)
		}

		// finished marshaling
		// <-------->
		// start unmarshal

		buf, err = page.Metadata()
		if err != nil {
			t.Fatal(err)
		}

		im2 := &IndexMeta{}

		if err := im2.UnmarshalBinary(buf); err != nil {
			t.Fatal(err)
		}

		if !reflect.DeepEqual(im, im2) {
			t.Fatal("not equal")
		}
	})

}
