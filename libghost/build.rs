fn main() {
    prost_build::compile_protos(
        &["src/protocols/ghost/ghost.proto"],
        &["src/protocols/ghost"],
    )
    .unwrap();
}
