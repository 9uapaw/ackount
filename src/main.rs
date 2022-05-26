use ackount::{processor};

use ackount::storage::{AccountStorage};

#[tokio::main(worker_threads = 1)]
async fn main() {
    let args = std::env::args();
    let file_path = args.skip(1).next();
    if file_path.is_none() {
        eprintln!("File argument is not given");
        std::process::exit(1);
    }

    let processor = processor::process_csv_file(&file_path.unwrap()).await;

    println!("client, available, held, total, locked");
    let storage = processor.storage.lock().await;
    for c in &mut storage.get_clients() {
        println!("{}, {}", c.0, c.1)
    }
}
