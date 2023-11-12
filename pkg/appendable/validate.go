package appendable

func ValidateIndexFile(r io.Reader, f *IndexFile) error {
	// read the version
	version, err := encoding.ReadByte(r)
	if err != nil {
		return fmt.Errorf("failed to read version: %w", err)
	}
	f.Version = protocol.Version(version)

	switch version {
	case 1:
		// read the index file header
		ifh := protocol.IndexFileHeader{}
		if ifh.IndexLength, err = encoding.ReadUint64(r); err != nil {
			return fmt.Errorf("failed to read index file header: %w", err)
		}
		if ifh.DataCount, err = encoding.ReadUint64(r); err != nil {
			return fmt.Errorf("failed to read index file header: %w", err)
		}

		// read the index headers
		f.Indexes = []Index{}
		br := 0
		recordCounts := []uint64{}
		for br < int(ifh.IndexLength) {
			var i Index
			if i.FieldName, err = encoding.ReadString(r); err != nil {
				return fmt.Errorf("failed to read index header: %w", err)
			}
			ft, err := encoding.ReadByte(r)
			if err != nil {
				return fmt.Errorf("failed to read index header: %w", err)
			}
			i.FieldType = protocol.FieldType(ft)
			recordCount, err := encoding.ReadUint64(r)
			if err != nil {
				return fmt.Errorf("failed to read index header: %w", err)
			}
			recordCounts = append(recordCounts, recordCount)
			i.IndexRecords = btree.NewG[protocol.IndexRecord](2, f.less)
			f.Indexes = append(f.Indexes, i)
			br += encoding.SizeString(i.FieldName) + binary.Size(ft) + binary.Size(uint64(0))
		}
		if br != int(ifh.IndexLength) {
			return fmt.Errorf("expected to read %d bytes, read %d bytes", ifh.IndexLength, br)
		}

		// read the index records
		for i, index := range f.Indexes {
			br := 0
			for br < int(recordCounts[i]) {
				var ir protocol.IndexRecord
				if ir.Key, err = encoding.ReadString(r); err != nil {
					return fmt.Errorf("failed to read index record: %w", err)
				}