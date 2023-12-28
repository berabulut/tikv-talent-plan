use chrono::prelude::*;
use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufRead;
use std::io::Write;
use std::path::PathBuf;

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
        let map = init_map_with_command_logs();

        KvStore {
            map: map,
            log_file: OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open("./cmdlogs/kvlog.cmdlog")
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

fn init_map_with_command_logs() -> HashMap<String, String> {
    let mut store = HashMap::new();
    let log_files = list_log_files().unwrap();

    for file in log_files {
        let file = File::open(file).unwrap();
        let reader = std::io::BufReader::new(file);

        for line in reader.lines() {
            let command_log: CommandLog = serde_json::from_str(&line.unwrap()).unwrap();
            match command_log {
                CommandLog::Set { key, value } => {
                    store.insert(key, value);
                }
                CommandLog::Remove { key } => {
                    store.remove(&key);
                }
            }
        }
    }

    store
}

fn list_log_files() -> Result<Vec<PathBuf>, Error> {
    // Read directory entries
    let entries = fs::read_dir("./cmdlogs/")?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();

    // Return only files with extension .cmdlog
    let log_files = entries
        .iter()
        .filter(|entry| entry.path().is_file())
        .filter(|entry| {
            entry
                .path()
                .extension()
                .map_or(false, |ext| ext == "cmdlog")
        })
        .map(|entry| entry.path())
        .collect();

    Ok(log_files)
}
