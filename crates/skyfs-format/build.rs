//! Generates the on-bucket format types from `proto/skyfs/**` with `protox`,
//! a pure-Rust protobuf compiler, so building needs no `protoc`.

use std::path::PathBuf;

const PROTOS: [&str; 2] = ["skyfs/wal/v1/wal.proto", "skyfs/snapshot/v1/snapshot.proto"];

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_root = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../proto");
    let files: Vec<PathBuf> = PROTOS.iter().map(|p| proto_root.join(p)).collect();
    for file in &files {
        println!("cargo:rerun-if-changed={}", file.display());
    }

    let descriptors = protox::compile(&files, [&proto_root])?;
    prost_build::Config::new().compile_fds(descriptors)?;
    Ok(())
}
