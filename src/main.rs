use quick_kv::storage::Storage;


fn main() {
    let namespace = String::from("test");
    let mut storage = Storage::new(namespace);

    let values = [
        ("test1", vec![0x41; 8]),
        ("test2", vec![0x42; 12]),
        ("test3", vec![0x43; 1020]),
        ("test4", vec![0x44; 16]),
    ];

    for (key, value) in values {
        storage.write(&String::from(key), &value).unwrap();
    }

    let keys = [
        "test1",
        "test2",
        "test3",
        "test4",
        "test5"
    ];

    for key in keys {
        let value = match storage.read(&String::from(key)) {
            Ok(v) => v,
            Err(e) => {
                // Handle error
                eprintln!("[{:?}] Key \"{}\" not found", e, &key);
                return;
            }
        };

        println!("Value read: 0x{:x} times {}", value[0], value.len());
    }
    
}