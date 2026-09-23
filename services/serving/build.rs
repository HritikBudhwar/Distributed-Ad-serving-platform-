fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto = std::env::var("PROTO_FILE").unwrap_or_else(|_| {
        if std::path::Path::new("/proto/adpulse.proto").exists() {
            "/proto/adpulse.proto".into()
        } else {
            "../../proto/adpulse.proto".into()
        }
    });
    let include = std::path::Path::new(&proto)
        .parent()
        .unwrap()
        .to_string_lossy()
        .to_string();
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&[&proto], &[&include])?;
    println!("cargo:rerun-if-changed={proto}");
    Ok(())
}
