use clap::{arg, command, error, Command};
use kvs::{CommandResult, KvStore};

fn main() -> CommandResult<()> {
    let mut store = KvStore::open("./")?;

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
        Some(("get", sub_matches)) => {
            let value = store.get(sub_matches.get_one::<String>("KEY").unwrap().to_string());
            match value {
                Ok(Some(value)) => println!("{}", value),
                Ok(None) => println!("Key not found"),
                Err(e) => println!("{}", e),
            }

            Ok(())
        }
        Some(("rm", sub_matches)) => {
            let res = store.remove(sub_matches.get_one::<String>("KEY").unwrap().to_string());
            match res {
                Err(e) => {
                    println!("{}", e);
                    std::process::exit(1)
                }
                _ => (),
            }

            Ok(())
        }
        _ => unreachable!("Provide a command"),
    }
}
