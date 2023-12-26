use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::PathBuf;

const LOG_FILE: &str = "log.txt";

pub struct KvStore {
    map: HashMap<String, String>,
    log_file: File,
}

pub type CommandResult<T> = Result<T, Error>;

#[derive(Serialize, Deserialize)]
enum CommandLog {
    Set { key: String, value: String },
    Remove { key: String },
}

#[derive(Fail, Debug)]
pub enum KvSError {
    #[fail(display = "Key not provided for command")]
    KeyNotProvided,
}

impl KvStore {
    pub fn new() -> Self {
        KvStore {
            map: HashMap::new(),
            log_file: OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(LOG_FILE)
                .unwrap(),
        }
    }

    fn write_command_log(&mut self, command_log: CommandLog) -> Result<(), Error> {
        let serialized_log = serde_json::to_string(&command_log)?;
        writeln!(&mut self.log_file, "{}", serialized_log)?;

        Ok(())
    }

    pub fn get(&self, key: String) -> CommandResult<Option<String>> {
        Ok(self.map.get(&key).cloned())
    }

    pub fn set(&mut self, key: String, value: String) -> CommandResult<()> {
        if key.is_empty() {
            return Err(KvSError::KeyNotProvided.into());
        }

        self.write_command_log(CommandLog::Set {
            key: key,
            value: value,
        })?;

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> CommandResult<()> {
        if key.is_empty() {
            return Err(KvSError::KeyNotProvided.into());
        }

        self.write_command_log(CommandLog::Remove { key: key })?;

        Ok(())
    }

    pub fn open(path: impl Into<PathBuf>) -> CommandResult<KvStore> {
        Ok((KvStore::new()))
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        // Flush and sync the file before closing
        if let Err(err) = self.log_file.sync_all() {
            eprintln!("Error syncing file: {:?}", err);
        }
    }
}
