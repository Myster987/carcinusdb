use std::path::PathBuf;

#[derive(Debug)]
pub struct FileSystemManager {
    base_dir: PathBuf,
}

impl FileSystemManager {
    pub fn new(mut base_dir: PathBuf, database_name: &str) -> Self {
        base_dir.push(database_name);

        Self { base_dir }
    }

    pub fn metadata_path(&self) -> PathBuf {
        self.base_dir.join("metadata")
    }

    pub fn schema_path(&self) -> PathBuf {
        self.base_dir.join("schema")
    }

    pub fn data_dir(&self) -> PathBuf {
        self.base_dir.join("data")
    }
}
