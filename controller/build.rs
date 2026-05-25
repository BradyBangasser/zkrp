fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["../proto/relay.proto"], &["../proto"])
        .unwrap();
}
