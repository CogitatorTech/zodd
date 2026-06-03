{
  description = "Zodd: a small embeddable Datalog engine in Zig";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
  };

  outputs = { nixpkgs, ... }:
    let
      supportedSystems = [
        "x86_64-linux"
        "aarch64-linux"
        "x86_64-darwin"
        "aarch64-darwin"
      ];
      forAllSystems = nixpkgs.lib.genAttrs supportedSystems;
    in
    {
      devShells = forAllSystems (system:
        let
          pkgs = nixpkgs.legacyPackages.${system};
        in
        {
          default = pkgs.mkShell {
            packages = with pkgs; [
              zig_0_16
              gnumake
              git
              pre-commit
              nixpkgs-fmt
            ];

            shellHook = ''
              echo "Zodd dev shell"
              echo ""
              echo "Common commands:"
              echo "  make build        Build the library"
              echo "  make test         Run Zig tests"
              echo "  make lint         Check Zig formatting"
              echo "  make format       Format Zig sources"
              echo "  make docs         Generate API documentation"
              echo "  make example      Run examples (e.g. make example EXAMPLE=e1_network_reachability)"
            '';
          };
        });

      formatter = forAllSystems (system: nixpkgs.legacyPackages.${system}.nixpkgs-fmt);
    };
}
