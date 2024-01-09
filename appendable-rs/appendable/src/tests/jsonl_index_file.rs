#[cfg(test)]
mod tests {
    use crate::handler::jsonl_handler::JSONLHandler;
    use crate::index_file::IndexFile;
    use std::fs::File;
    use std::io::Write;
    use std::path::Path;
    use tempfile::NamedTempFile;

    fn mock_jsonl_file() -> std::io::Result<File> {
        // Create a temporary file
        let mut temp_file = NamedTempFile::new()?;

        writeln!(
            temp_file,
            r#"{{"name": "matteo", "id": 2, "alpha": ["a", "b", "c"]}}"#
        )?;
        writeln!(
            temp_file,
            r#"{{"name": "kevin", "id": 1, "alpha": ["x", "y", "z"]}}"#
        )?;

        // Persist the file and return the File handle
        let file = temp_file.persist(Path::new("mock_data.jsonl"))?;
        Ok(file)
    }

    #[test]
    fn create_index_file() {
        let file = mock_jsonl_file().expect("Failed to create mock file");
        let jsonl_handler = JSONLHandler::new(file);

        let index_file = IndexFile::new(Box::new(jsonl_handler));

        assert!(index_file.is_ok());

        let index_file = index_file.unwrap();
    }
}
