{
  inputs = {
    nixpkgs.url = "github:nixos/nixpkgs/nixos-25.05";
    flake-utils.url = "github:numtide/flake-utils";
    treefmt-nix.url = "github:numtide/treefmt-nix";
    treefmt-nix.inputs.nixpkgs.follows = "nixpkgs";
    rust-overlay.url = "github:oxalica/rust-overlay";
    rust-overlay.inputs.nixpkgs.follows = "nixpkgs";
  };

  outputs = {
    self,
    nixpkgs,
    flake-utils,
    treefmt-nix,
    rust-overlay,
  }:
    flake-utils.lib.eachDefaultSystem (
      system: let
        overlays = [(import rust-overlay)];
        pkgs = import nixpkgs {
          inherit system overlays;
        };

        treefmtEval = treefmt-nix.lib.evalModule pkgs ./treefmt.nix;

        toolchain = pkgs.rust-bin.fromRustupToolchainFile ./rust-toolchain.toml;
        rust = pkgs.makeRustPlatform {
          cargo = toolchain;
          rustc = toolchain;
        };

        commonArgs = {
          version = "0.1.0";
          src = ./.;
          cargoLock.lockFile = ./Cargo.lock;
          nativeBuildInputs = [
            pkgs.flatbuffers
          ];
          RUSTFLAGS = "--cap-lints warn";
          checkFlags = [
            "--skip compaction"
          ];
        };
      in {
        packages = {
          client = rust.buildRustPackage (commonArgs
            // {
              pname = "kvs-client";
            });

          server = rust.buildRustPackage (commonArgs
            // {
              pname = "kvs-server";
            });
        };

        # for `nix fmt`
        formatter = treefmtEval.config.build.wrapper;

        devShells.default = pkgs.mkShell {
          packages = [
            toolchain
          ];

          buildInputs = with pkgs; [
            rustc
            cargo
            rust-analyzer
            clippy
            flatbuffers
          ];
        };
      }
    );
}
