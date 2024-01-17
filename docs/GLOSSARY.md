# Glossary

## Overview
*For a deeper look, you can check out the source code [here](../pkg/appendable/index_file.go) and [here](../pkg/protocol/protocol.go).*

The overall index file for AppendableDB is structured as:

| *Structure*     | 
|-----------------|
| Version         |
| IndexFileHeader |
| IndexHeader     |
| Indexes         |
| EndByteOffsets  |
| Checksums       |

> We'll use the following data to illustrate how each component works. Consider some of Norm Macdonald's favorite books:
> ```json lines
> {"title": "The Hustler", "author": "Walter Tevis", "pages": 240}
> {"title": "Les Miserables", "author": "Victor Hugo", "pages":  1462}
> {"title": "Fathers and Sons", "author": "Ivan Turgenev", "pages": 226}
> {"title": "The Joke", "author": "Milan Kundera", "pages": 296}
> ```

### Version
`Version` represents the version of AppendableDB that this library is compatible with. The current implementation supports Version 1.

> Example
> ```
> Version: 1
> ```

<br />

### IndexFileHeader
`IndexFileHeader` serves as the header of the index file, containing crucial metadata. It includes: 
- `IndexLength`: The number of bytes occupied by the `IndexHeaders`.
- `DataCount`: The total number of data records indexed in this file.

> Example:
> ```json
> IndexLength: 160  // Let's assume 40 bytes per `IndexHeader`
> DataCount: 4      // total number of books
> ``` 

<br />

### IndexHeader
`IndexHeader` defines the header for each `IndexRecord`, representing the fields available in the data file. It comprises:
- `FieldName`: The name of the field.
- `FieldType`: The type of data stored in the field, aiding in Typescript typings.
- `IndexRecordCount`: The number of `IndexRecords`.

> Example:
>
> Let's assume we create an index for each field (`title`, `author`, `pages`).
>
> For `title`:
> ```json
> FieldName: "title"
> FieldType: protocol.FieldTypeString
> IndexRecordCount: 4  // one for each book
> ```

<br />

### Indexes
`Indexes` store a collection of `Index` objects. An `Index` is a representation of a single index, that is:
- `FieldName`: The name of the field.
- `FieldType`: The type of data stored in the field, aiding in Typescript typings.
- `IndexRecords`: A collection of `IndexRecord` objects.

> `Index` Example:
>
> For `title`:
> ```json
> FieldName: "title"
> FieldType: protocol.FieldTypeString
> IndexRecords: // {...} details in the next section
> ```

<br />

#### IndexRecord
`IndexRecord` denotes a record in the index, consisting of:
- `DataNumber`: A unique identifier for the data.
- `FieldStartByteOffset`: The byte offset in the data file to fetch the exact field value.
- `FieldLength`: The length of the field, encoded to be at least as long as the actual field value.

> `IndexRecord` Example (*Within each `Index`*):
>
> Example for "The Hustler":
> ```json
> IndexRecords: {
>      DataNumber: 1
>      FieldStartByteOffset: 0
>      FieldLength: 11 // length of "The Hustler"
> } 
> ```
<br />

### EndByteOffsets
`EndByteOffsets` includes the byte offset for every `Index` in `Indexes`.

### Checksums
`Checksums` ensures data integrity, providing a mechanism to verify the data's correctness. For every `Index` in `Indexes`, we store a `Checksum`.
AppendableDB uses [`xxhash`](https://github.com/cespare/xxhash) to compute checksums.
