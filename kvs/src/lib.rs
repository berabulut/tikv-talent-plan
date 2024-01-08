use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

pub struct KvStore {
    map: HashMap<String, u64>,
    log_writer: BufWriter<File>,
    log_reader: BufReader<File>,
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
    #[fail(display = "Key not found")]
    KeyNotFound,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> CommandResult<KvStore> {
        let path = path.into();

        // Create directory if it doesn't exist
        fs::create_dir_all(&path)?;

        // Initialize map with command logs from previous sessions
        let map = init_map_with_command_logs(&path);

        let log_writer = BufWriter::new(
            OpenOptions::new()
                .write(true)
                .create(true)
                .append(true)
                .open(path.join("kvlog.cmdlog"))
                .unwrap(),
        );

        let log_reader = BufReader::new(
            OpenOptions::new()
                .read(true)
                .open(path.join("kvlog.cmdlog"))
                .unwrap(),
        );

        Ok(KvStore {
            map: map,
            log_writer,
            log_reader,
        })
    }

    fn write_command_log(&mut self, command_log: CommandLog) -> Result<u64, Error> {
        let serialized_log = serde_json::to_string(&command_log)?;
        let log_len = serialized_log.len();

        writeln!(&mut self.log_writer, "{}", serialized_log)?;

        let pos = self.log_writer.stream_position()? - log_len as u64 - 1;

        Ok(pos)
    }

    fn read_from_pos_to_eol(&mut self, pos: u64) -> Result<String, Error> {
        self.log_writer.flush()?;

        self.log_reader.seek(SeekFrom::Start(pos))?;

        let mut line = String::new();

        // Read characters until the newline is found:
        loop {
            let mut buf = [0; 1]; // Buffer to hold a single character
            let bytes_read = self.log_reader.read(&mut buf)?;

            if bytes_read == 0 {
                // End of file reached
                break;
            }

            if buf[0] == b'\n' {
                // Newline found, end of line reached
                break;
            }

            line.push(buf[0] as char);
        }

        Ok(line)
    }

    pub fn get(&mut self, key: String) -> CommandResult<Option<String>> {
        let res = self.map.get(&key).cloned();
        match res {
            Some(pos) => {
                let line_res = self.read_from_pos_to_eol(pos)?;
                let command_log: CommandLog = serde_json::from_str(&line_res)?;
                match command_log {
                    CommandLog::Set { value, .. } => Ok(Some(value)),
                    _ => Ok(None),
                }
            }
            _ => Ok(None),
        }
    }

    pub fn set(&mut self, key: String, value: String) -> CommandResult<()> {
        if key.is_empty() {
            return Err(KvSError::KeyNotProvided.into());
        }

        let pos = self.write_command_log(CommandLog::Set {
            key: key.clone(),
            value: value.clone(),
        })?;

        self.map.insert(key, pos);

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> CommandResult<()> {
        if key.is_empty() {
            return Err(KvSError::KeyNotProvided.into());
        }

        if !self.map.contains_key(&key) {
            return Err(KvSError::KeyNotFound.into());
        }

        self.write_command_log(CommandLog::Remove { key: key.clone() })?;

        self.map.remove(&key);

        Ok(())
    }

    fn create_compacted_log_file(&mut self) -> Result<(), Error> {
        let new_log_file = OpenOptions::new()
            .write(true)
            .create(true)
            .append(true)
            .open("kvlog_compact.cmdlog")?;

        self.log_writer = BufWriter::new(new_log_file);

        let keys: Vec<String> = self.map.keys().map(String::from).collect();

        for key in keys {
            let value = self.get(key.clone())?.unwrap();
            self.write_command_log(CommandLog::Set {
                key: key,
                value: value,
            })?;
        }

        Ok(())
    }
}

impl Drop for KvStore {
    fn drop(&mut self) {
        // Create compacted log file and if successful, delete old log file, and rename compacted log file to old log file name
        if let Err(e) = self.create_compacted_log_file() {
            eprintln!("Error creating compacted log file: {}", e);
        } else {
            if let Err(e) = fs::remove_file("kvlog.cmdlog") {
                eprintln!("Error deleting old log file: {}", e);
            }

            if let Err(e) = fs::rename("kvlog_compact.cmdlog", "kvlog.cmdlog") {
                eprintln!("Error renaming compacted log file: {}", e);
            }
        }
    }
}

fn init_map_with_command_logs(path: impl Into<PathBuf>) -> HashMap<String, u64> {
    let mut store = HashMap::new();
    let log_files = list_log_files(path).unwrap();

    for file in log_files {
        let file = File::open(file).unwrap();
        let reader = BufReader::new(file);

        let mut pos = 0;
        for line in reader.lines() {
            let line = line.unwrap();

            let command_log: CommandLog = serde_json::from_str(&line).unwrap();
            match command_log {
                CommandLog::Set { key, .. } => {
                    store.insert(key, pos);
                }
                CommandLog::Remove { key } => {
                    store.remove(&key);
                }
            }

            pos += line.len() as u64 + 1;
        }
    }

    store
}

fn list_log_files(path: impl Into<PathBuf>) -> Result<Vec<PathBuf>, Error> {
    // Read directory entries
    let entries = fs::read_dir(path.into())?
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
