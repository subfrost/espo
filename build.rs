fn main() {
    prost_build::Config::new()
        .compile_protos(
            &["proto/alkanes.proto", "proto/metashrew.proto"],
            &["proto/"],
        )
        .expect("Failed to compile protobuf files");
}
