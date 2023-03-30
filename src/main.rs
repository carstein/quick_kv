pub mod metadata;
pub mod storage;
pub mod page;
pub mod error;

use storage::Storage;
use std::io::{self, stdin, Write};

enum KVCommand {
    Help,
    Quit,
    List,
    Status,
    Load(String),
    Get(String),
    Put(String, String),
    Invalid
}


fn parse_command(cmd_str: &Vec<&str>) -> KVCommand {
    match cmd_str.len() {
        1 => {
            if cmd_str[0].trim() == "help" {
                return KVCommand::Help;
            }
        
            if cmd_str[0].trim() == "quit" {
                return KVCommand::Quit;
            }
    
            if cmd_str[0].trim() == "list" {
                return KVCommand::List;
            }

            if cmd_str[0].trim() == "status" {
                return KVCommand::Status;
            }
        },
        2 => {
            if cmd_str[0].trim() == "load" {
                return KVCommand::Load(String::from(cmd_str[1].trim()));
            }
    
            if cmd_str[0].trim() == "get" {
                return KVCommand::Get(String::from(cmd_str[1].trim()));
            }
        },
        3 => {
            if cmd_str[0].trim() == "put" {
                return KVCommand::Put(String::from(cmd_str[1].trim()), 
                        String::from(cmd_str[2].trim()));
            }
        },
        _ => {
            return KVCommand::Invalid;
        }
    }

    KVCommand::Invalid
}

fn help() {
    println!("## Welcome to quick_kv. Following commands are available");
    println!("help -- prints this help message");
    println!("load <namespace> -- loads given namespace");
    println!("list -- list all the keys");
    println!("get <key> -- prints the value of the key");
    println!("put <key> <value> -- adds pair <key>:<value> to the store");
    println!("quit -- quits the program");
}

fn status_metadata(storage: &Storage) {
    println!("### Metadata");
    for key in storage.list_keys() {
        println!("{key}: {:?}", storage.get_item_location(key).unwrap())
    }
}

fn status_cache(storage: &Storage) {
    println!("### Cache");
    println!("{:?}", storage.get_cache());
}

fn main() {
    let mut namespace  = String::from("");
    let mut storage: Option<Storage> = None;

    // implementing RAPL
    let mut user_input = String::new();    

    loop {
        print!("{namespace}> ");
        io::stdout().flush().unwrap();

        stdin().read_line(&mut user_input).unwrap();
        let cmd_str: Vec<&str> = user_input.split(' ').collect();

        // Parse commands
        let cmd = parse_command(&cmd_str);

        match cmd {
            KVCommand::Help => help(),
            KVCommand::Quit => {
                println!("Bye!");
                break;
            },

            KVCommand::Status => {
                println!("Status: ");
                match &mut storage {
                    Some(s) => {
                        status_metadata(&s);
                        status_cache(&s);
                    },
                    None => {
                        println!("[!] Storage not selected");
                    }
                }
            },

            KVCommand::List => {
                match &storage {
                    Some(s) => {
                        for key in s.list_keys() {
                            println!("- {key}");
                        }
                    },
                    None => {
                        println!("[!] Storage not selected");
                    }
                }
            },

            KVCommand::Get(key) => {
                match &mut storage {
                    Some(s) => {
                        match s.read(&key) {
                            Ok(v) => {
                                let val = String::from_utf8_lossy(&v);
                                println!("{key} | {val}");
                            }
                            Err(e) => {
                                println!("Error reading \"{key}\" - {e:?}");
                            }
                        }
                    },
                    None => {
                        println!("[!] Storage not selected");
                    }
                }
            },

            KVCommand::Load(ns) => {
                println!("[#] Loading storage \"{ns}\"");
                match Storage::new(&ns) {
                    Ok(s) => {
                        storage = Some(s);
                        namespace = ns;
                    }
                    Err(e) =>  {
                        println!("Failed to open a namespace: {e:?}");
                        return;
                    }
                }
            },

            KVCommand::Put(key, value) => {
                match &mut storage {
                    Some(s) => {
                        if let Err(e) = s.write(&key, &value.bytes().collect()) {
                            println!("[!] Error writing {key}:{value} - {e:?}");
                        } else {
                            println!("[#] Value stored");
                        };
                    },
                    None => {
                        println!("[!] Storage not selected");
                    }
                }
            },

            KVCommand::Invalid => {
                println!("Command invalid!");
            },
        }
        user_input.clear();
    }
    
}