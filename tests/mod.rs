use lz4::block;

#[test]
fn main_test() -> anyhow::Result<()> {
    let original_data = b"Example data to be compressed using LZ4. LZ4 is super fast! Example data to be compressed using LZ4. LZ4 is super fast! Example data to be compressed using LZ4. LZ4 is super fast!";

    // Original size in bytes
    let original_size = original_data.len();

    // Compress the data
    let compressed_data =
        block::compress(original_data, None, false).expect("Failed to compress data");
    let compressed_size = compressed_data.len();

    // Decompress the data
    let decompressed_data = block::decompress(&compressed_data, Some(original_size as i32))
        .expect("Failed to decompress data");

    // Verify that the decompressed data matches the original data
    assert_eq!(original_data.to_vec(), decompressed_data);

    // Calculate compression ratio and percentage
    let compression_ratio = original_size as f64 / compressed_size as f64;
    let compression_percentage = (1.0 - (compressed_size as f64 / original_size as f64)) * 100.0;

    // Display results
    println!("Original size: {} bytes", original_size);
    println!("Compressed size: {} bytes", compressed_size);
    println!("Compression ratio: {:.2}", compression_ratio);
    println!("Compression percentage: {:.2}%", compression_percentage);

    Ok(())
}
