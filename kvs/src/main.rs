use clap::{arg, command, Command};
use kvs::KvStore;

fn main() {
    let mut store = KvStore::new();

    let matches = command!()
        .version("0.1.0")
        .subcommand_required(true)
        .subcommand(
            Command::new("set")
                .about("Inserts a new record to key value store")
                .arg(arg!(<KEY> "Key of the record"))
                .arg(arg!(<VALUE> "Value of the record")),
        )
        .subcommand(
            Command::new("get")
                .about("Fetches record from key value store")
                .arg(arg!(<KEY> "Key of the record")),
        )
        .subcommand(
            Command::new("rm")
                .about("Remove record from key value store")
                .arg(arg!(<KEY> "Key of the record")),
        )
        .get_matches();

    match matches.subcommand() {
        Some(("set", sub_matches)) => store.set(
            sub_matches.get_one::<String>("KEY").unwrap().to_string(),
            sub_matches.get_one::<String>("VALUE").unwrap().to_string(),
        ),
        Some(("get", sub_matches)) => println!(
            "Value of key {:?} {:?}",
            sub_matches.get_one::<String>("KEY"),
            store.get(sub_matches.get_one::<String>("KEY").unwrap().to_string()),
        ),
        Some(("rm", sub_matches)) => {
            store.remove(sub_matches.get_one::<String>("KEY").unwrap().to_string())
        }
        _ => unreachable!("Provide a command"),
    }
}
