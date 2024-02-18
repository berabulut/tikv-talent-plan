use chrono::prelude::*;
use failure::{Error, Fail};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs;
use std::fs::File;
use std::fs::OpenOptions;
use std::io::BufWriter;
use std::io::{BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;

const COMPACTION_THRESHOLD: usize = 1024 * 1024;
const LOG_FILE_PREFIX: &str = "kvlog";
const LOG_FILE_EXTENSION: &str = "cmdlog";

struct LogPosition {
    pos: u64,
    log_file_name: String,
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

pub struct KvStore {
    key_dir: KeyDir,
    writer_pool: WriterPool,
    reader_pool: ReaderPool,
}

impl KvStore {
    pub fn open(path: impl Into<PathBuf>) -> CommandResult<KvStore> {
        let path = path.into();

        // Create directory if it doesn't exist
        fs::create_dir_all(&path)?;

        // Initialize map with command logs from previous sessions
        let key_dir = KeyDir::init_with_command_logs(&path);
        let writer_pool = WriterPool::new(&path);
        let reader_pool = ReaderPool::new(&path);

        Ok(KvStore {
            key_dir,
            writer_pool,
            reader_pool,
        })
    }

    pub fn get(&mut self, key: String) -> CommandResult<Option<String>> {
        self.writer_pool.sync()?;

        let res = self.key_dir.get(&key).clone();
        match res {
            Some(log_pos) => {
                let line_res = self.reader_pool.read_from_pos_to_eol(log_pos)?;
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

        self.key_dir.set(key, pos);

        Ok(())
    }

    pub fn remove(&mut self, key: String) -> CommandResult<()> {
        if key.is_empty() {
            return Err(KvSError::KeyNotProvided.into());
        }

        if !self.key_dir.contains_key(&key) {
            return Err(KvSError::KeyNotFound.into());
        }

        self.write_command_log(CommandLog::Remove { key: key.clone() })?;

        self.key_dir.remove(&key);

        Ok(())
    }

    fn write_command_log(&mut self, command_log: CommandLog) -> Result<LogPosition, Error> {
        let serialized_log = serde_json::to_string(&command_log)?;
        if self.writer_pool.active_size() + serialized_log.len() >= COMPACTION_THRESHOLD {
            self.compact_log_files()?;
        }

        self.writer_pool.write(serialized_log)
    }

    fn compact_log_files(&mut self) -> Result<(), Error> {
        let reader_list = self.reader_pool.reader_list();

        self.writer_pool.new_writer();
        self.reader_pool.add_reader(self.writer_pool.curr.clone());

        let mut start_pos = 0;

        reader_list.iter().for_each(|file_name| {
            let reader = self.reader_pool.get_reader(file_name.to_string());
            let lines: Vec<String> = reader.lines().map(|line| line.unwrap()).collect();

            for line in lines {
                let command_log: CommandLog = serde_json::from_str(&line).unwrap();
                let should_remove =
                    self.should_remove_log(&command_log, file_name.clone(), start_pos);

                start_pos += line.len() as u64 + 1;

                if should_remove {
                    continue;
                }

                let serialized_log = serde_json::to_string(&command_log).unwrap();

                if self.writer_pool.active_size() + serialized_log.len() >= COMPACTION_THRESHOLD {
                    self.writer_pool.new_writer();
                    self.reader_pool.add_reader(self.writer_pool.curr.clone());
                }

                self.writer_pool.write(serialized_log).unwrap();
            }
        });

        self.reader_pool.remove_readers(reader_list);

        Ok(())
    }

    fn should_remove_log(&self, log: &CommandLog, file_name: String, start_pos: u64) -> bool {
        match log {
            CommandLog::Set { key, .. } => {
                if !self.key_dir.contains_key(&key) {
                    return true;
                }

                let log_pos = self.key_dir.get(&key).unwrap();
                if log_pos.log_file_name != file_name {
                    return true;
                }
                if log_pos.pos != start_pos {
                    return true;
                }

                return false;
            }
            CommandLog::Remove { key: _ } => true,
        }
    }
}

struct KeyDir {
    map: HashMap<String, LogPosition>,
}

impl KeyDir {
    fn init_with_command_logs(path: impl Into<PathBuf>) -> KeyDir {
        let mut store = HashMap::new();
        let log_files = list_log_files(path).unwrap();

        for file_path in log_files {
            let file = File::open(file_path.clone()).unwrap();
            let reader = BufReader::new(file);

            let mut pos = 0;
            for line in reader.lines() {
                let line = line.unwrap();

                let command_log: CommandLog = serde_json::from_str(&line).unwrap();
                match command_log {
                    CommandLog::Set { key, .. } => {
                        store.insert(
                            key,
                            LogPosition {
                                pos,
                                log_file_name: file_path
                                    .file_name()
                                    .unwrap()
                                    .to_str()
                                    .unwrap()
                                    .to_string(),
                            },
                        );
                    }
                    CommandLog::Remove { key } => {
                        store.remove(&key);
                    }
                }

                pos += line.len() as u64 + 1;
            }
        }

        KeyDir { map: store }
    }

    fn get(&self, key: &str) -> Option<&LogPosition> {
        self.map.get(key)
    }

    fn set(&mut self, key: String, log_position: LogPosition) {
        self.map.insert(key, log_position);
    }

    fn remove(&mut self, key: &str) {
        self.map.remove(key);
    }

    fn contains_key(&self, key: &str) -> bool {
        self.map.contains_key(key)
    }
}

struct WriterPool {
    path: PathBuf,
    writers: HashMap<String, NamedBufWriter>,
    curr: String,
    curr_size: usize,
}

impl WriterPool {
    // Create hash map with writers to log files, initialized with empty log file
    fn new(path: impl Into<PathBuf>) -> WriterPool {
        let mut writers = HashMap::new();
        let path = path.into();

        match latest_log_file_metadata(&path) {
            Ok((lf_name, lf_size)) => {
                if lf_size < COMPACTION_THRESHOLD as u64 {
                    writers.insert(lf_name.clone(), NamedBufWriter::new(&path, lf_name.clone()));
                    return WriterPool {
                        path,
                        writers,
                        curr: lf_name,
                        curr_size: lf_size as usize,
                    };
                }
            }
            _ => {}
        }

        let new_log_file_name = new_log_file_name();
        writers.insert(
            new_log_file_name.clone(),
            NamedBufWriter::new(&path, new_log_file_name.clone()),
        );

        WriterPool {
            path,
            writers,
            curr: new_log_file_name,
            curr_size: 0,
        }
    }

    fn new_writer(&mut self) {
        let new_log_file_name = new_log_file_name();
        self.writers.insert(
            new_log_file_name.clone(),
            NamedBufWriter::new(&self.path, new_log_file_name.clone()),
        );
        self.curr = new_log_file_name;
        self.curr_size = 0;
    }

    fn active_size(&self) -> usize {
        self.curr_size
    }

    fn sync(&mut self) -> Result<(), Error> {
        self.writers.get_mut(&self.curr).unwrap().sync()?;
        Ok(())
    }

    fn write(&mut self, s: String) -> Result<LogPosition, Error> {
        self.curr_size += s.len();
        return self.writers.get_mut(&self.curr).unwrap().write(s);
    }
}

struct ReaderPool {
    // into pathbuf
    path: String,
    readers: HashMap<String, BufReader<File>>,
}

impl ReaderPool {
    fn new(path: impl Into<PathBuf>) -> ReaderPool {
        let path = path.into();

        let mut readers = HashMap::new();
        let log_files = list_log_files(&path).unwrap();

        for file_path in log_files {
            let file_name = file_path.file_name().unwrap().to_str().unwrap();
            let file = File::open(file_path.clone()).unwrap();
            let reader = BufReader::new(file);
            readers.insert(file_name.to_string(), reader);
        }

        ReaderPool {
            path: path.to_str().unwrap().to_string(),
            readers: readers,
        }
    }

    fn add_reader(&mut self, file_name: String) {
        let file = File::open(format!("{}/{}", self.path, file_name)).unwrap();
        let reader = BufReader::new(file);
        self.readers.insert(file_name, reader);
    }

    fn get_reader(&mut self, file_name: String) -> &mut BufReader<File> {
        self.readers.get_mut(&file_name).unwrap()
    }

    fn reader_list(&self) -> Vec<String> {
        self.readers.keys().cloned().collect()
    }

    fn remove_readers(&mut self, file_names: Vec<String>) {
        for file_name in file_names {
            match fs::remove_file(format!("{}/{}", self.path, file_name)) {
                Ok(_) => {}
                Err(e) => {
                    panic!("Error removing file: {}", e.to_string());
                }
            }

            self.readers.remove(&file_name);
        }
    }

    fn read_from_pos_to_eol(&mut self, log_position: &LogPosition) -> Result<String, Error> {
        let pos = log_position.pos;
        let file_name = log_position.log_file_name.clone();

        let reader = self.get_reader(file_name);

        reader.seek(SeekFrom::Start(pos))?;

        let mut line = String::new();

        // Read characters until the newline is found:
        loop {
            let mut buf = [0; 1]; // Buffer to hold a single character
            let bytes_read = reader.read(&mut buf)?;

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
}

struct NamedBufWriter {
    writer: BufWriter<File>,
    file_name: String,
}

impl NamedBufWriter {
    fn new(path: impl Into<PathBuf>, file_name: String) -> NamedBufWriter {
        NamedBufWriter {
            writer: BufWriter::new(
                OpenOptions::new()
                    .write(true)
                    .create(true)
                    .append(true)
                    .open(path.into().join(file_name.clone()))
                    .unwrap(),
            ),
            file_name,
        }
    }

    fn write(&mut self, s: String) -> Result<LogPosition, Error> {
        let writer = &mut self.writer;
        writeln!(writer, "{}", s)?;

        let start_pos = writer.stream_position()? - s.len() as u64 - 1;

        Ok(LogPosition {
            pos: start_pos,
            log_file_name: self.file_name.clone(),
        })
    }

    fn sync(&mut self) -> Result<(), Error> {
        self.writer.flush()?;
        Ok(())
    }
}

fn list_log_files(path: impl Into<PathBuf>) -> Result<Vec<PathBuf>, Error> {
    // Read directory entries
    let entries = fs::read_dir(path.into())?
        .filter_map(|entry| entry.ok())
        .collect::<Vec<_>>();

    // Find files with .cmdlog extension
    let mut log_files: Vec<_> = entries
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

    log_files.sort();

    Ok(log_files)
}

fn new_log_file_name() -> String {
    format!(
        "{}_{}.{}",
        LOG_FILE_PREFIX,
        Utc::now().timestamp_nanos_opt().unwrap(),
        LOG_FILE_EXTENSION
    )
}

fn latest_log_file_metadata(path: impl Into<PathBuf>) -> Result<(String, u64), Error> {
    let log_files = list_log_files(path)?;
    if log_files.is_empty() {
        return Err(failure::err_msg("No log files found"));
    }

    let latest_log_file = log_files.last().unwrap();
    let metadata = latest_log_file.metadata()?;

    Ok((
        latest_log_file
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .to_string(),
        metadata.len(),
    ))
}
