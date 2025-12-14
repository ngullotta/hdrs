mod data;
use std::time::{SystemTime, UNIX_EPOCH};

use data::{
    Blob, write_blob_object, Snapshot, Entry,
    write_snapshot_object, Commit, write_commit_object
};

fn main() {
    // Blob for AAPL (Note: timestamp=0 ensures identical content for testing)
    let aapl_data: Blob = Blob {
        timestamp: 0,
        open: 150.0, high: 152.0, low: 149.0, close: 151.0, volume: 100000,
    };
    let aapl_hash = write_blob_object(&aapl_data).unwrap();

    // Blob for MSFT
    let msft_data: Blob = Blob {
        timestamp: 0,
        open: 300.0, high: 305.0, low: 299.0, close: 304.0, volume: 50000,
    };
    let msft_hash = write_blob_object(&msft_data).unwrap();

    // --- 2. Create the PortfolioSnapshot ---

    let mut entries = vec![
        Entry {
            ticker: "AAPL".to_string(),
            blob_hash: aapl_hash.clone(),
        },
        Entry {
            ticker: "MSFT".to_string(),
            blob_hash: msft_hash.clone(),
        },
    ];

    // CRITICAL: Sort the entries to ensure deterministic hashing for the snapshot!
    entries.sort_by(|a, b| a.ticker.cmp(&b.ticker));

    let snapshot = Snapshot { entries };

    // --- 3. Write the Snapshot Object ---
    let snapshot_hash = write_snapshot_object(&snapshot).unwrap();

    println!("AAPL Blob Hash: {}", aapl_hash);
    println!("MSFT Blob Hash: {}", msft_hash);
    println!("PortfolioSnapshot Hash: {}", snapshot_hash);

    let commit = Commit {
        tree_hash: snapshot_hash.clone(), // Links to the PortfolioSnapshot we just made
        parent_hash: None,                // No parent, this is the first commit
        timestamp: SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_secs(),
        author: "cndl-updater".to_string(),
        message: "Initial portfolio tracking setup.".to_string(),
    };

    let commit_hash = write_commit_object(&commit).unwrap();
    println!("Commit Hash: {}", commit_hash);

    // --- 3. Update HEAD (The Pointer) ---
    // This is the final step: mark the latest commit
    std::fs::write(".cndl/HEAD", &commit_hash)
        .expect("Unable to write HEAD file.");

    println!("SUCCESS: Initial commit written to .cndl/HEAD");
}
