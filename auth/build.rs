/// Use the vendored protoc (with its bundled well-known types) unless the
/// caller set PROTOC, so builds don't depend on the host's protobuf packages.
fn vendored_protoc() {
    if std::env::var_os("PROTOC").is_none() {
        // SAFETY: build scripts are single-threaded at this point.
        unsafe {
            std::env::set_var("PROTOC", protoc_bin_vendored::protoc_bin_path().unwrap());
            std::env::set_var("PROTOC_INCLUDE", protoc_bin_vendored::include_path().unwrap());
        }
    }
}

fn main() {
    vendored_protoc();

    tonic_prost_build::configure()
        .compile_protos(&["../proto/auth.proto"], &["../proto"])
        .unwrap();

}
