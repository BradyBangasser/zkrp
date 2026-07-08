use std::process::Command;

fn main() {
    prost_build::compile_protos(
        &["src/protocols/ghost/ghost.proto"],
        &["src/protocols/ghost"],
    )
    .unwrap();

    tonic_prost_build::configure()
        .build_client(true)
        .build_server(false)
        .compile_protos(&["../proto/relay.proto"], &["../proto"])
        .unwrap();

    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    let git_hash = String::from_utf8(output.stdout).unwrap().trim().to_string();

    let branch_output = Command::new("git")
        .args(["rev-parse", "--abbrev-ref", "HEAD"])
        .output()
        .unwrap();
    let branch = String::from_utf8(branch_output.stdout)
        .unwrap()
        .trim()
        .to_string();

    println!("cargo:rustc-env=GIT_HASH={}", git_hash);
    println!("cargo:rustc-env=GIT_BRANCH={}", branch);

    println!("cargo:rerun-if-changed=.git/HEAD");
}
