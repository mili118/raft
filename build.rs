fn main() {
    tonic_build::configure()
        .compile_protos(&["proto/raft.proto"], &["proto"])
        .unwrap();

    println!("cargo:rerun-if-changed=proto/raft.proto");
}