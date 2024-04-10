package handlers

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"github.com/kevmo314/appendable/pkg/metapage"
	"github.com/kevmo314/appendable/pkg/pointer"
	"log/slog"
	"math"
	"os"
	"testing"

	"github.com/kevmo314/appendable/pkg/appendable"
	"github.com/kevmo314/appendable/pkg/btree"
)

/*
Index File Deep Equality Check
*/
func TestEquality(t *testing.T) {
	originalLogger := slog.Default()

	debugLevel := &slog.LevelVar{}
	debugLevel.Set(slog.LevelDebug)
	debugLogger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{
		Level: debugLevel,
	}))

	slog.SetDefault(debugLogger)

	defer slog.SetDefault(originalLogger)

	/* TODO! uncomment when CSV is implemented
	mockJsonl := "{\"h1\":\"test1\", \"h2\":37.3}\n"
	mockJsonl2 := "{\"h1\":\"test1\", \"h2\":37.3}\n{\"h1\":\"test3\", \"h2\":4}\n"

	mockCsv := "h1,h2\ntest1,37.3\n"
	mockCsv2 := "h1,h2\ntest1,37.3\ntest3,4\n"

	t.Run("test index files after Synchronize", func(t *testing.T) {
		jr1 := []byte(mockJsonl2)
		cr1 := []byte(mockCsv2)
		f := buftest.NewSeekableBuffer()

		jsonlI, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := jsonlI.Synchronize(jr1); err != nil {
			t.Fatal(err)
		}

		f = buftest.NewSeekableBuffer()

		csvI, err := appendable.NewIndexFile(f, CSVHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := csvI.Synchronize(cr1); err != nil {
			t.Fatal(err)
		}

		if status, response := compare(jsonlI, csvI, jr1, cr1); status == false {
			t.Errorf("Equality failed: %v", response)
		}

	})

	t.Run("test index files with appending", func(t *testing.T) {
		jr := []byte(mockJsonl)
		cr := []byte(mockCsv)
		f := buftest.NewSeekableBuffer()

		jsonlI, err := appendable.NewIndexFile(f, JSONLHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := jsonlI.Synchronize(jr); err != nil {
			t.Fatal(err)
		}

		jr = []byte(mockJsonl2)
		if err := jsonlI.Synchronize(jr); err != nil {
			t.Fatal(err)
		}

		f = buftest.NewSeekableBuffer()

		csvI, err := appendable.NewIndexFile(f, CSVHandler{})
		if err != nil {
			t.Fatal(err)
		}

		if err := csvI.Synchronize(cr); err != nil {
			t.Fatal(err)
		}

		cr = []byte(mockCsv2)
		if err := csvI.Synchronize(cr); err != nil {
			t.Fatal(err)
		}

		if status, response := compare(jsonlI, csvI, jr, cr); status == false {
			t.Errorf("Equality failed: %v", response)
		}

	})
	*/
}

func compareFieldNames(i1, i2 []string) (bool, string) {

	if len(i1) != len(i2) {
		return false, fmt.Sprintf("field name lengths do not align. i1: %v\ti2: %v\n", i1, i2)
	}

	fieldMap := make(map[string]int)
	for _, fn2 := range i2 {
		fieldMap[fn2]++
	}

	for _, fn1 := range i1 {
		if count, exists := fieldMap[fn1]; !exists || count == 0 {
			return false, fmt.Sprintf("Field name '%s' in i1 is missing or duplicated in i2\n", fn1)
		}
		fieldMap[fn1]--
	}

	return true, ""
}

func compareIndexMeta(i1, i2 []*metapage.LinkedMetaPage) (bool, string) {

	for i, collected1 := range i1 {
		buf1, err := collected1.Metadata()
		if err != nil {
			return false, fmt.Sprintf("Error collecting metadata for i1.collect()[0]: %v", err)
		}

		md1 := &appendable.IndexMeta{}
		if err := md1.UnmarshalBinary(buf1); err != nil {
			return false, fmt.Sprintf("Failed to unmarshall metadata for i1.collect()[0] %v", err)
		}

		collected2 := i2[i]
		buf2, err := collected2.Metadata()
		if err != nil {
			return false, fmt.Sprintf("Error collecting metadata for i2.collect()[%v]: %v", i, err)
		}

		md2 := &appendable.IndexMeta{}
		if err := md2.UnmarshalBinary(buf2); err != nil {
			return false, fmt.Sprintf("Failed to unmarshall metadata for i2.collect()[%v] %v", i, err)
		}

		if i == 0 {
			if md1.FieldName != "h1" || md1.FieldType != appendable.FieldTypeString {
				return false, fmt.Sprintf("expected metadata for i1.collect()[%v] fieldname = h1 and FieldTypeString. Got %v and %v", i, md1.FieldName, md1.FieldType)
			}

			if md2.FieldName != "h1" || md2.FieldType != appendable.FieldTypeString {
				return false, fmt.Sprintf("expected metadata for i2.collect()[%v] fieldname = h1 and FieldTypeString. Got %v and %v", i, md1.FieldName, md1.FieldType)
			}
		} else if i == 1 {
			if md1.FieldName != "h2" || md1.FieldType != appendable.FieldTypeFloat64 {
				return false, fmt.Sprintf("expected metadata for i1.collect()[%v] fieldname = h1 and FieldTypeString. Got %v and %v", i, md1.FieldName, md1.FieldType)
			}

			if md2.FieldName != "h2" || md2.FieldType != appendable.FieldTypeFloat64 {
				return false, fmt.Sprintf("expected metadata for i2.collect()[%v] fieldname = h1 and FieldTypeString. Got %v and %v", i, md1.FieldName, md1.FieldType)
			}
		}

	}

	return true, ""
}

func compareMetaPages(i1, i2 []*metapage.LinkedMetaPage, jr, cr []byte) (bool, string) {
	h1 := [2]string{"test1", "test3"}
	h2 := [2]float64{37.3, 4}

	for i, collected1 := range i1 {
		collected2 := i2[i]

		if i == 0 {

			for _, val := range h1 {
				rv1, mp1, err := collected1.BTree(&btree.BTree{Data: jr, DataParser: JSONLHandler{}}).Find(btree.ReferencedValue{Value: []byte(val)})

				if err != nil {
					return false, fmt.Sprintf("failed to find btree for jsonl reader %v", val)
				}
				if mp1 == (pointer.MemoryPointer{}) {
					return false, fmt.Sprintf("failed to find %v for reader", val)
				}

				rv2, mp2, err := collected2.BTree(&btree.BTree{Data: cr, DataParser: CSVHandler{}}).Find(btree.ReferencedValue{Value: []byte(val)})

				if err != nil {
					return false, fmt.Sprintf("failed to find btree for jsonl reader %v", val)
				}
				if mp2 == (pointer.MemoryPointer{}) {
					return false, fmt.Sprintf("failed to find %v for reader", val)
				}

				if !bytes.Equal(rv1.Value, rv2.Value) {
					return false, fmt.Sprintf("mismatched keys: %v, %v", rv1.Value, rv2.Value)
				}

			}

		} else if i == 1 {
			for _, val := range h2 {

				v2 := make([]byte, 8)
				binary.BigEndian.PutUint64(v2, math.Float64bits(val))
				rv1, mp1, err := collected1.BTree(&btree.BTree{Data: jr, DataParser: JSONLHandler{}}).Find(btree.ReferencedValue{Value: v2})

				if err != nil {
					return false, fmt.Sprintf("failed to find btree for jsonl reader %v", val)
				}
				if mp1 == (pointer.MemoryPointer{}) {
					return false, fmt.Sprintf("failed to find %v for josnl reader", val)
				}

				rv2, mp2, err := collected2.BTree(&btree.BTree{Data: cr, DataParser: CSVHandler{}}).Find(btree.ReferencedValue{Value: v2})

				if err != nil {
					return false, fmt.Sprintf("failed to find btree for jsonl reader %v", val)
				}
				if mp2 == (pointer.MemoryPointer{}) {
					return false, fmt.Sprintf("failed to find %v for josnl reader", val)
				}

				if !bytes.Equal(rv1.Value, rv2.Value) {
					return false, fmt.Sprintf("mismatched keys: %v, %v", rv1.Value, rv2.Value)
				}
			}
		}

	}

	return true, ""
}

func compare(i1, i2 *appendable.IndexFile, jReader, cReader []byte) (bool, string) {
	// compare field names

	i1fn, err := i1.IndexFieldNames()
	if err != nil {
		return false, "failed to get IndexFieldNames() for i1"
	}

	i2fn, err := i2.IndexFieldNames()
	if err != nil {
		return false, "failed to get IndexFieldNames() for i2"
	}

	if status, res := compareFieldNames(i1fn, i2fn); status == false {
		return status, res
	}
	indexes1, err := i1.Indexes()
	if err != nil {
		return false, "failed to get Indexes() for i1"
	}

	collected1, err := indexes1.Collect()
	if err != nil {
		return false, "failed to collect indexes for i1"
	}

	indexes2, err := i2.Indexes()
	if err != nil {
		return false, "failed to get Indexes() for i2"
	}

	collected2, err := indexes2.Collect()
	if err != nil {
		return false, "failed to collect indexes for i2"
	}

	if len(collected1) != len(collected2) {
		return false, fmt.Sprintf("collected indexes length do not line up. i1: %v \t i2: %v", len(collected1), len(collected2))
	}

	// compare index meta
	if status, res := compareIndexMeta(collected1, collected2); status == false {
		return status, res
	}

	// comparing meta pages
	if status, res := compareMetaPages(collected1, collected2, jReader, cReader); status == false {
		return status, res
	}

	return true, "Great Success!"
}
