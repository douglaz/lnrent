{
  description = "lnrent — AI-free VPS-manager + Lightning/Nostr rental control plane";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Stable Rust + the wasm32 target (web buyer / the .19 gift-wrap spike) and dev tools.
        # The daemon builds native (x86_64-gnu); musl-static was dropped (ADR-0015, RocksDB C++).
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
          targets = [ "wasm32-unknown-unknown" ];
        };
      in
      {
        devShells.default = pkgs.mkShell {
          packages = with pkgs; [
            rustToolchain

            # native C/C++ build deps:
            #  - secp256k1-sys (Nostr + Fedimint) compiles C via cc
            #  - fedimint-rocksdb / librocksdb-sys (bead .4) builds bundled RocksDB (clang + cmake)
            #  - bindgen needs libclang
            clang
            llvmPackages.libclang
            cmake
            pkg-config
            openssl

            # wasm tooling for the web buyer (.18) and the rust-nostr gift-wrap feasibility spike (.19)
            wasm-pack
            wasm-bindgen-cli
            binaryen   # wasm-opt
            trunk

            # ops convenience
            sqlite
          ];

          # bindgen (librocksdb-sys, secp256k1) needs to find libclang
          LIBCLANG_PATH = "${pkgs.llvmPackages.libclang.lib}/lib";

          shellHook = ''
            echo "lnrent devshell · $(rustc --version)"
            echo "targets: native (x86_64-gnu) + wasm32-unknown-unknown"
            echo "  (to link a system rocksdb instead of the bundled build, export"
            echo "   ROCKSDB_LIB_DIR=${pkgs.rocksdb}/lib ROCKSDB_INCLUDE_DIR=${pkgs.rocksdb}/include)"
          '';
        };
      });
}
