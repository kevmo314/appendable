package appendable

// func (f *IndexFile) WriteTypescriptDefinitions(w io.Writer) error {
// 	_, err := w.Write([]byte(`// This file was generated by github.com/kevmo314/appendable/pkg/appendable/typescript.go`))
// 	if err != nil {
// 		return err
// 	}
// 	if _, err := w.Write([]byte("\n\nexport type Record = {\n")); err != nil {
// 		return err
// 	}
// 	// iterate over each field in the index header and generate a field for it
// 	for _, index := range f.Indexes {
// 		_, err := w.Write([]byte("\t\"" + index.FieldName + "\": " + index.FieldType.TypescriptType() + ";\n"))
// 		if err != nil {
// 			return err
// 		}
// 	}
// 	if _, err := w.Write([]byte("}\n")); err != nil {
// 		return err
// 	}

// 	return nil
// }
