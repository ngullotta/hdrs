use cndl::{CompressedTimeSeries, Tick};
use std::collections::HashMap;

fn print_usage() {
    eprintln!("Usage:");
    eprintln!("  compress <output_file> [symbols] [ticks]");
    eprintln!("  decompress <input_file>");
    eprintln!("  info <input_file>");
    eprintln!();
    eprintln!("Examples:");
    eprintln!("  compress data.hdrs AAPL,GOOGL,MSFT 1000");
    eprintln!("  decompress data.hdrs");
    eprintln!("  info data.hdrs");
}

fn generate_ticks(symbols: &[&str], count: usize) -> Vec<Tick> {
    let mut ticks = Vec::with_capacity(count);
    let base_prices: HashMap<&str, f64> = [
        ("AAPL", 150.0),
        ("GOOGL", 2800.0),
        ("MSFT", 300.0),
        ("AMZN", 3200.0),
        ("TSLA", 250.0),
        ("META", 320.0),
    ]
    .iter()
    .cloned()
    .collect();

    for i in 0..count {
        let mut prices = HashMap::new();
        let vol = (i as f64 / 100.0).sin() * 0.5;

        for &sym in symbols {
            let base = base_prices.get(sym).unwrap_or(&100.0);
            let price = base + vol * (base / 100.0) + (i as f64 * 0.01);
            prices.insert(sym.to_string(), price);
        }

        ticks.push(Tick {
            timestamp: 1700000000 + i as u64,
            prices,
        });
    }

    ticks
}

fn cmd_compress(
    output: &str,
    symbols: &[&str],
    count: usize,
) -> Result<(), Box<dyn std::error::Error>> {
    println!(
        "Generating {} ticks for {} symbols...",
        count,
        symbols.len()
    );
    let ticks = generate_ticks(symbols, count);

    let orig_size = ticks.len() * symbols.len() * 16;
    println!("Original size: ~{} bytes\n", orig_size);

    let start = std::time::Instant::now();
    let compressed = CompressedTimeSeries::compress(&ticks)?;
    let elapsed = start.elapsed();

    compressed.write_to_file(output)?;

    let ser_size = std::fs::metadata(output)?.len() as usize;
    let ratio = compressed.compression_ratio(orig_size) * 100.0;

    println!("[*] Compressed in {:?}", elapsed);
    println!("[*] Output: {}", output);
    println!("[*] Size: {} bytes", ser_size);
    println!("[*] Compression: {:.2}%\n", ratio);

    let meta = compressed.metadata();
    println!("Checksums:");
    println!("  Reference: 0x{:08X}", meta.reference_checksum);
    println!("  Data:      0x{:08X}", meta.data_checksum);
    println!("  Overall:   0x{:08X}", meta.overall_checksum);

    Ok(())
}

fn cmd_decompress(input: &str) -> Result<(), Box<dyn std::error::Error>> {
    println!("Loading {}...", input);

    let start = std::time::Instant::now();
    let compressed = CompressedTimeSeries::read_from_file(input)?;
    let load_time = start.elapsed();

    let meta = compressed.metadata();
    println!("[*] Loaded in {:?}", load_time);
    println!("[*] Symbols: {}", meta.symbols.join(", "));
    println!("[*] Ticks: {}", meta.num_ticks);
    println!("[*] Size: {} bytes\n", meta.compressed_size);

    let start = std::time::Instant::now();
    let ticks = compressed.decompress()?;
    let decomp_time = start.elapsed();

    println!("Decompressed {} ticks in {:?}\n", ticks.len(), decomp_time);

    println!("Sample (first 3 ticks):");
    for (i, tick) in ticks.iter().take(3).enumerate() {
        println!("  [{}] ts={}", i, tick.timestamp);
        for (sym, &price) in &tick.prices {
            println!("      {}: ${:.2}", sym, price);
        }
    }

    Ok(())
}

fn cmd_info(input: &str) -> Result<(), Box<dyn std::error::Error>> {
    let compressed = CompressedTimeSeries::read_from_file(input)?;
    let meta = compressed.metadata();

    println!("File: {}", input);
    println!("Version: {}", meta.version);
    println!(
        "Symbols: {} ({})",
        meta.num_symbols,
        meta.symbols.join(", ")
    );
    println!("Ticks: {}", meta.num_ticks);
    println!("Compressed size: {} bytes", meta.compressed_size);
    println!("Base timestamp: {}", meta.base_timestamp);
    println!();
    println!("Checksums:");
    println!("  Reference: 0x{:08X}", meta.reference_checksum);
    println!("  Data:      0x{:08X}", meta.data_checksum);
    println!("  Overall:   0x{:08X}", meta.overall_checksum);

    Ok(())
}

fn main() {
    let args: Vec<String> = std::env::args().collect();

    if args.len() < 2 {
        print_usage();
        std::process::exit(1);
    }

    let result = match args[1].as_str() {
        "compress" => {
            if args.len() < 3 {
                eprintln!("Error: Missing output file");
                print_usage();
                std::process::exit(1);
            }

            let output = &args[2];
            let symbols_str = args.get(3).map(|s| s.as_str()).unwrap_or("AAPL,GOOGL,MSFT");
            let count = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1000);

            let symbols: Vec<&str> = symbols_str.split(',').collect();
            cmd_compress(output, &symbols, count)
        }

        "decompress" => {
            if args.len() < 3 {
                eprintln!("Error: Missing input file");
                print_usage();
                std::process::exit(1);
            }
            cmd_decompress(&args[2])
        }

        "info" => {
            if args.len() < 3 {
                eprintln!("Error: Missing input file");
                print_usage();
                std::process::exit(1);
            }
            cmd_info(&args[2])
        }

        _ => {
            eprintln!("Error: Unknown command '{}'", args[1]);
            print_usage();
            std::process::exit(1);
        }
    };

    if let Err(e) = result {
        eprintln!("Error: {}", e);
        std::process::exit(1);
    }
}
