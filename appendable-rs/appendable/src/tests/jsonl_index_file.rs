#[cfg(test)]
mod tests {
    use crate::handler::jsonl_handler::JSONLHandler;
    use crate::index_file::IndexFile;
    use std::fs;
    use std::io::Write;
    use std::path::PathBuf;
    use tempfile::NamedTempFile;

    fn mock_jsonl_file_to_disk() -> std::io::Result<PathBuf> {
        let mut temp_file = NamedTempFile::new()?;

        writeln!(
            temp_file,
            r#"{{"name": "matteo", "id": 2, "alpha": ["a", "b", "c"]}}"#
        )?;
        writeln!(
            temp_file,
            r#"{{"name": "kevin", "id": 1, "alpha": ["x", "y", "z"]}}"#
        )?;

        let file_path = temp_file.into_temp_path();
        let persisted_file = file_path.keep()?;
        Ok(persisted_file)
    }

    #[test]
    fn create_index_file() {
        let file_path = mock_jsonl_file_to_disk().expect("Failed to create mock file");
        let data = fs::read(&file_path).expect("Unable to read mock file");

        let jsonl_handler = JSONLHandler::new(data);
        let index_file = IndexFile::new(Box::new(jsonl_handler));

        assert!(index_file.is_ok());

        let index_file = index_file.unwrap();
    }
}
