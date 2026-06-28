fn main() {
    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["../proto/relay.proto"], &["../proto"])
        .unwrap();
}
