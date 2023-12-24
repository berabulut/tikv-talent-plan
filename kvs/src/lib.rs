use failure::Error;
use std::collections::HashMap;
use std::path::PathBuf;

pub struct KvStore {
    map: HashMap<String, String>,
}

pub type CommandResult<T> = Result<T, Error>;

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }

    pub fn get(&self, key: String) -> CommandResult<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    pub fn set(&mut self, key: String, value: String) -> CommandResult<()> {
        self.map.insert(key, value);
        Ok(())
    }

    pub fn remove(&mut self, key: String) -> CommandResult<()> {
        self.map.remove(&key);
        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> CommandResult<KvStore> {
        Ok((KvStore::new()))
    }
}
