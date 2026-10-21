fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure()
        .build_server(false)
        .compile_protos(&["../api/lease/v1/lease.proto"], &["../api/lease/v1"])?;
    Ok(())
}
