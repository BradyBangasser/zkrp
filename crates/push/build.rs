/// Use the vendored protoc unless the caller set PROTOC.
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
        .build_client(false)
        .compile_protos(&["../../proto/push.proto"], &["../../proto"])
        .unwrap();
}
