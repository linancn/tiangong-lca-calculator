fn main() {
    println!("cargo:rerun-if-env-changed=PKG_CONFIG_PATH");

    if pkg_config::Config::new()
        .cargo_metadata(true)
        .probe("UMFPACK")
        .is_ok()
    {
        return;
    }

    // Fallback linker directives for environments without UMFPACK pkg-config metadata.
    for lib in [
        "umfpack",
        "cholmod",
        "amd",
        "colamd",
        "suitesparseconfig",
        "blas",
        "lapack",
    ] {
        println!("cargo:rustc-link-lib={lib}");
    }
}
