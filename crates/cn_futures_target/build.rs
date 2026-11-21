fn main() -> Result<(), Box<dyn std::error::Error>> {
    tonic_build::configure().compile_protos(&["proto/position_target.proto"], &["proto"])?;
    Ok(())
}
