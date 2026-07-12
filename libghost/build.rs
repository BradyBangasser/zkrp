fn main() {
    prost_build::compile_protos(
        &["src/protocols/ghost/ghost.proto"],
        &["src/protocols/ghost"],
    )
    .unwrap();

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(
            &["../proto/relay.proto", "../proto/mailbox.proto"],
            &["../proto"],
        )
        .unwrap();
}
