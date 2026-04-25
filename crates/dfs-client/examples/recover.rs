//! Quick recovery test: get an object by ID, verifying it survives
//! a node failure.

use dfs_client::DfsClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut nodes = Vec::new();
    let mut object_id = String::new();
    let mut args = std::env::args().peekable();

    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--nodes" => {
                let val = args.next().expect("expected node list");
                nodes = val.split(',').map(str::to_string).collect();
            }
            "--get" => {
                object_id = args.next().expect("expected object id");
            }
            _ => {}
        }
    }

    let client = DfsClient::new(nodes);
    println!("Fetching object: {object_id}");

    match client.get(&object_id).await {
        Ok(data) => {
            let cksum = hex::encode(blake3::hash(&data).as_bytes());
            println!("Recovered {} bytes, checksum: {cksum}", data.len());
            // Write to file to manually verify with diff
            std::fs::write("/tmp/recovered.blob", &data)?;
            println!("Wrote /tmp/recovered.blob");
        }
        Err(e) => {
            eprintln!("Recovery failed: {e}");
            std::process::exit(1);
        }
    }
    Ok(())
}