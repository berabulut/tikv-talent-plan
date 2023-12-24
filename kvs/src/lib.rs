use failure::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

pub struct KvStore {
    map: HashMap<String, String>,
}

pub type CommandResult<T> = Result<T, Error>;

const LOG_FILE: &str = "log.txt";

#[derive(Serialize, Deserialize, Debug)]
struct CommandLog {
    command: String,
    key: String,
    value: String,
}

impl KvStore {
    pub fn new() -> KvStore {
        KvStore {
            map: HashMap::new(),
        }
    }
    fn open_log_file(&self) -> Result<File, Error> {
        let f = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open(LOG_FILE)?;

        Ok(f)
    }

    fn write_command_log(&self, command_log: CommandLog) -> Result<(), Error> {
        let serialized_log = serde_json::to_string(&command_log)?;
        let mut log_file = self.open_log_file()?;
        writeln!(&mut log_file, "{}", serialized_log)?;

        Ok(())
    }

    pub fn get(&self, key: String) -> CommandResult<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    pub fn set(&mut self, key: String, value: String) -> CommandResult<()> {
        let command_log = CommandLog {
            command: "set".to_string(),
            key: key,
            value: value,
        };

        self.write_command_log(command_log)?;

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
