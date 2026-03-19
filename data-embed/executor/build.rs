use std::env;
use std::path::PathBuf;

fn main() {
    // Get the workspace root (two levels up from executor crate)
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let workspace_root = manifest_dir.parent().unwrap().parent().unwrap();
    
    // Path to libduckdb.so
    let lib_path = workspace_root.join("lib/sirius/build/release/src");
    
    println!("cargo:rustc-link-search=native={}", lib_path.display());
    println!("cargo:rustc-link-lib=dylib=duckdb");
    
    // Set rpath so the binary can find libduckdb.so at runtime
    println!("cargo:rustc-link-arg=-Wl,-rpath,{}", lib_path.display());
    
    // Rerun if the library changes
    println!("cargo:rerun-if-changed={}/libduckdb.so", lib_path.display());
}
