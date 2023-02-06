pub mod metadata;
pub mod storage;
pub mod page;
pub mod error;

use storage::Storage;
use std::io::{self, stdin, Write};

enum KVCommand {
    HELP,
    QUIT,
    LIST,
    LOAD(String),
    GET(String),
    PUT(String, String),
    INVALID
}

impl Drop for storage::Storage {
    fn drop(&mut self) {
        println!("Dropping storage and saving metadata");
        self.save();
    }
}

fn parse_command(cmd_str: &Vec<&str>) -> KVCommand {
    match cmd_str.len() {
        1 => {
            if cmd_str[0].trim() == "help" {
                return KVCommand::HELP;
            }
        
            if cmd_str[0].trim() == "quit" {
                return KVCommand::QUIT;
            }
    
            if cmd_str[0].trim() == "list" {
                return KVCommand::LIST;
            }

        },
        2 => {
            if cmd_str[0].trim() == "load" {
                return KVCommand::LOAD(String::from(cmd_str[1].trim()));
            }
    
            if cmd_str[0].trim() == "get" {
                return KVCommand::GET(String::from(cmd_str[1].trim()));
            }

        },
        3 => {
            if cmd_str[0].trim() == "put" {
                return KVCommand::PUT(String::from(cmd_str[1].trim()), 
                        String::from(cmd_str[2].trim()));
            }

        },
        _ => {
            return KVCommand::INVALID;
        }
    }

    KVCommand::INVALID
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
            KVCommand::HELP => help(),
            KVCommand::QUIT => {
                println!("Bye!");
                break;
            },

            KVCommand::LIST => {
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

            KVCommand::GET(key) => {
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

            KVCommand::LOAD(ns) => {
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

            KVCommand::PUT(key, value) => {
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

            KVCommand::INVALID => {
                println!("Command invalid!");
            },
        }
        user_input.clear();
    }
    
}