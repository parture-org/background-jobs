{
  description = "background-jobs";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    flake-utils.url = "github:numtide/flake-utils";
  };

  outputs = { self, nixpkgs, flake-utils }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        pkgs = import nixpkgs {
          inherit system;
        };
      in
      {
        packages.default = pkgs.hello;

        devShell = with pkgs; mkShell {
          nativeBuildInputs = [
            cargo
            cargo-outdated
            clippy
            diesel-cli
            rust-analyzer
            rustc
            rustfmt
            stdenv.cc
            taplo
          ];

          RUST_SRC_PATH = "${pkgs.rust.packages.stable.rustPlatform.rustLibSrc}";
        };
      });
}
