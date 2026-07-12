fn main() {
    tonic_prost_build::configure()
        .compile_protos(&["../proto/auth.proto"], &["../proto"])
        .unwrap();

    let out_dir = std::env::var("OUT_DIR").unwrap();

    match reqwest::blocking::get(
        "https://www.apple.com/certificateauthority/Apple_App_Attestation_Root_CA.pem",
    ) {
        Ok(res) => {
            let body = res.text().unwrap();
            let path = std::path::Path::new(&out_dir).join("Apple_App_Attestation_Root_CA.pem");
            std::fs::write(path, body).unwrap();
        }
        Err(e) => {}
    }
}
