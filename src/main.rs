use quick_kv::storage::Storage;
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

fn parse_command(cmd_str: &Vec<&str>) -> KVCommand {
    dbg!(cmd_str);

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

            KVCommand::INVALID
        },
        2 => {
            if cmd_str[0].trim() == "load" {
                return KVCommand::LOAD(String::from(cmd_str[1]));
            }
    
            if cmd_str[0].trim() == "get" {
                return KVCommand::GET(String::from(cmd_str[1]));
            }

            KVCommand::INVALID
        },
        3 => {
            if cmd_str[0].trim() == "put" {
                return KVCommand::PUT(String::from(cmd_str[1]), String::from(cmd_str[2]));
            }

            KVCommand::INVALID
        },
        _ => {
            KVCommand::INVALID
        }
    }
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
    let mut storage: Storage;

    // let values = [
    //     ("test1", vec![0x41; 8]),
    //     ("test2", vec![0x42; 12]),
    //     ("test3", vec![0x43; 1020]),
    //     ("test4", vec![0x44; 16]),
    // ];

    // for (key, value) in values {
    //     storage.write(&String::from(key), &value).unwrap();
    // }

    // let keys = [
    //     "test1",
    //     "test2",
    //     "test3",
    //     "test4",
    //     "test5"
    // ];

    // for key in keys {
    //     let value = match storage.read(&String::from(key)) {
    //         Ok(v) => v,
    //         Err(e) => {
    //             // Handle error
    //             eprintln!("[{:?}] Key \"{}\" not found", e, &key);
    //             return;
    //         }
    //     };

    //     println!("Value read: 0x{:x} times {}", value[0], value.len());
    // }

    // implementing RAPL
    let mut user_input = String::new();    

    loop {
        print!("{namespace}> ");
        io::stdout().flush().unwrap();

        stdin().read_line(&mut user_input).unwrap();
        let cmd_str: Vec<&str> = user_input.split(" ").collect();

        // Parse commands
        let cmd = parse_command(&cmd_str);

        match cmd {
            KVCommand::HELP => help(),
            KVCommand::QUIT => {
                println!("Bye!");
                break;
            },

            KVCommand::LIST => {   
                println!("List keys")
            },

            KVCommand::GET(key) => {
                println!("Get {key}");
            },

            KVCommand::LOAD(ns) => {
                namespace = String::from(ns.trim());
                println!("Loading storage \"{namespace}\"");
                storage = Storage::new(&namespace);
            },

            KVCommand::PUT(key, value) => {
                println!("Put {key}:{value}");
            },

            KVCommand::INVALID => {
                println!("Command invalid!");
            },
        }
        user_input.clear();
    }
    
}