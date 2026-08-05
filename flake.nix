{
  description = "lnrent — AI-free VPS-manager + Lightning/Nostr rental control plane";

  inputs = {
    nixpkgs.url = "github:NixOS/nixpkgs/nixos-unstable";
    rust-overlay.url = "github:oxalica/rust-overlay";
    flake-utils.url = "github:numtide/flake-utils";
    # crane builds the DEPENDENCY tree into its own derivation (`cargoArtifacts`) which the crate
    # builds then reuse. That separation is the point: packaging/image edits that do not change the
    # artifact inputs must not recompile the fedimint tree plus bundled RocksDB C++.
    crane.url = "github:ipetkov/crane";
  };

  outputs = { self, nixpkgs, rust-overlay, flake-utils, crane }:
    flake-utils.lib.eachDefaultSystem (system:
      let
        overlays = [ (import rust-overlay) ];
        pkgs = import nixpkgs { inherit system overlays; };

        # Stable Rust + the wasm32 target (web buyer / the .19 gift-wrap spike) and dev tools.
        # The daemon builds native (x86_64-gnu); musl-static was dropped (ADR-0015, RocksDB C++).
        # ONE toolchain: the devshell and the packages below build from this same binding.
        rustToolchain = pkgs.rust-bin.stable.latest.default.override {
          extensions = [ "rust-src" "rust-analyzer" "clippy" "rustfmt" ];
          targets = [ "wasm32-unknown-unknown" ];
        };

        # native C/C++ build deps, shared by the devshell and the package build so they cannot
        # drift apart:
        #  - secp256k1-sys (Nostr + Fedimint) compiles C via cc
        #  - fedimint-rocksdb / librocksdb-sys (bead .4) builds bundled RocksDB (clang + cmake)
        #  - bindgen needs libclang
        nativeBuildDeps = with pkgs; [ clang llvmPackages.libclang cmake pkg-config ];
        buildDeps = with pkgs; [ openssl ];
        libclangPath = "${pkgs.llvmPackages.libclang.lib}/lib";

        craneLib = (crane.mkLib pkgs).overrideToolchain rustToolchain;
        src = craneLib.cleanCargoSource ./.;
        # The shared build and image tag use the daemon crate's version. Each binary's clap
        # `--version` comes from its own crate manifest.
        version = (craneLib.crateNameFromCargoToml { cargoToml = ./daemon/Cargo.toml; }).version;

        # The ONE git dependency in Cargo.lock (the douglaz/fedimint fork, see daemon/Cargo.toml).
        # Pinned by a fixed output hash so the fetch is a normal fixed-output derivation: cacheable,
        # and the crate builds themselves run in a sandbox with no network. Crane keys this map by
        # the verbatim `source =` string in Cargo.lock.
        cargoVendorDir = craneLib.vendorCargoDeps {
          inherit src;
          outputHashes = {
            "git+https://github.com/douglaz/fedimint?branch=v0.11.1-pay-idempotency#642f3ed86afeccd7932b43a35a90f5bb5c6b7282" =
              "sha256-DRvSUVmcSIcxSS5LOjHMEj1ZTIjiO/sDo0+HaeLPVW4=";
          };
        };

        commonArgs = {
          inherit src cargoVendorDir version;
          pname = "lnrent";
          strictDeps = true;
          # `clients/web` (lnrent-buyer-web) is wasm-primary — most of it is
          # `#[cfg(target_arch = "wasm32")]` — so the native build selects its packages EXPLICITLY
          # rather than building the workspace and hoping.
          cargoExtraArgs = "--locked -p lnrentd -p lnrent-buyer-cli";
          nativeBuildInputs = nativeBuildDeps;
          buildInputs = buildDeps;
          LIBCLANG_PATH = libclangPath;
          # cmake is invoked BY librocksdb-sys; nixpkgs' cmake hook must not hijack configurePhase.
          dontUseCmakeConfigure = true;
          # Packaging builds binaries only. The test suite is a devshell/CI gate (AGENTS.md) and
          # needs `recipes/` fixtures that `cleanCargoSource` filters out; leaving it off also keeps
          # dev-dependencies out of the cached artifacts.
          doCheck = false;
        };

        # R1: the fedimint + RocksDB tree compiles HERE, once. Final package/image edits that leave
        # `commonArgs` and its inputs unchanged reuse it.
        cargoArtifacts = craneLib.buildDepsOnly commonArgs;

        # One derivation for all three binaries: `lnrentd` and `lnrent` are two bins of the SAME
        # crate, so splitting them into separate builds would compile the daemon twice for nothing.
        lnrentBins = craneLib.buildPackage (commonArgs // { inherit cargoArtifacts; });

        # …and one thin attr per binary, so `nix build .#lnrent-buyer` puts just that bin in
        # `result/bin`. Presentation only, NOT a closure split: the symlink points into the shared
        # build, which therefore stays a runtime dependency — copying any one attr still ships all
        # three binaries.
        #
        # The `test -x` is load-bearing, not defensive noise: `ln -s` happily links a target that
        # does not exist, so without it a renamed `[[bin]]` or a crate dropped from
        # `cargoExtraArgs` would leave `nix build .#<attr>` exiting 0 with a DANGLING
        # `result/bin/<name>` — success reported over a broken result, and nothing in CI builds
        # these attrs to catch it. OBSERVED both ways: pointed at a name the shared build does not
        # produce, the guard fails the derivation; before it existed, the same edit built fine.
        pickBin = name: pkgs.runCommand "${name}-${version}" { meta.mainProgram = name; } ''
          test -x ${lnrentBins}/bin/${name} \
            || { echo "FAIL: the shared build produces no binary named '${name}'"; exit 1; }
          mkdir -p $out/bin
          ln -s ${lnrentBins}/bin/${name} $out/bin/${name}
        '';

        # The live recipe, so the image serves do-vps without a checkout. Do not bundle the other
        # recipes: the daemon serves the lowest valid id.
        #
        # `LNRENT_RECIPES_DIR` below points at THIS STORE PATH, not at a path under `/opt`, and that
        # is a correctness requirement rather than a preference. `buildLayeredImage` materialises
        # `contents` as a symlink farm: a directory that exists in one input stays a real directory,
        # but the FILES under it become symlinks into `/nix/store`. `Recipe::validate` canonicalises
        # each request-kind op hook and rejects any that leaves `ops/`
        # (daemon/src/recipe.rs:313-324) — so with the recipes rooted under `/opt`, `ops/` was a real
        # directory whose `status`/`restart` resolved into the store, and the daemon MEASURABLY
        # refused to start:
        #   recipe `do-vps`: operation `status` hook escapes ops/ (/nix/store/…/ops/status)
        #   lnrentd: no valid recipe found in /opt/lnrent/recipes    → exit 1
        # Inside the store path the hooks are regular files under their own `ops/`, so containment
        # holds. Keep the tree flat here so the attr IS the recipes dir.
        recipesDir = pkgs.runCommand "lnrent-recipes-${version}" { } ''
          mkdir -p $out
          cp -a ${./recipes/do-vps} $out/do-vps
        '';

        # R5 — prove the packaged binary really has the fedimint/lnv2 money path compiled in, rather
        # than assuming it. `payment_backend=fedimint` + a syntactically invalid invite drives the
        # binary INTO the feature-gated branch:
        #   * feature ON  → it reaches `build_fedimint_backend` (daemon/src/main.rs:526) and dies
        #                   inside fedimint's own invite-code parser, wrapped in that function's
        #                   context string (daemon/src/main.rs:543);
        #   * feature OFF → it refuses `payment_backend=fedimint` BEFORE bootstrap
        #                   (daemon/src/main.rs:395-404) and never reaches that code at all.
        # So the marker below is reachable only from a fedimint-enabled build, and reaching it also
        # proves fedimint code RAN (the parse error is bech32's, not ours). No network: the invite
        # never parses, so nothing is dialled. The mnemonic is the canonical all-`abandon` BIP39
        # test vector; the identity it derives lives and dies inside the build sandbox.
        #
        # OBSERVED, both directions (a probe that has never been seen to fail is not a probe):
        #   * on the packaged binary → "lnrentd: joining the configured lnv2 Fedimint federation:
        #     parsing federation invite code: parsing failed: character error: invalid character
        #     (code=i)", exit 1 → PASS;
        #   * on `lnrentdNoFedimint` with `expectMarker` flipped to `true` → "FAIL: expected the
        #     fedimint marker to be PRESENT", derivation failed. That build instead prints
        #     "payment_backend=fedimint requires building lnrentd with --features fedimint";
        #   * and the guarding half breaks too — the packaged binary with `expectMarker` flipped to
        #     `false` → "FAIL: expected the fedimint marker to be ABSENT", derivation failed. So
        #     BOTH checks below have been watched to fail, not just watched to pass;
        #   * the `refusal` assertion added below was watched to fail the same way: pointed at a
        #     string no build emits, the negative half printed "FAIL: expected the log to contain:
        #     …" against a log that did carry the real refusal, and the derivation failed.
        marker = "joining the configured lnv2 Fedimint federation";
        # …and the message the fedimint-DISABLED build stops on instead (daemon/src/main.rs:400-403,
        # reached before `prepare_data_dir`). Asserting it is what makes the negative control a
        # control: "no marker + nonzero exit" also holds for a build that died EARLIER for an
        # unrelated reason — an unusable data dir, a missing env var — so absence on its own does not
        # prove the feature-refusal branch was reached.
        refusal = "payment_backend=fedimint requires building lnrentd with --features fedimint";
        runFedimintProbe = { name, drv, expectMarker }:
          let expected = if expectMarker then marker else refusal; in
          pkgs.runCommand name { } ''
          set +e
          log=$(
            LNRENT_DATA_DIR="$TMPDIR/data" \
            LNRENT_PAYMENT_BACKEND=fedimint \
            LNRENT_FEDIMINT_INVITE=not-a-real-invite \
            LNRENT_MNEMONIC="abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon abandon about" \
            ${drv}/bin/lnrentd 2>&1
          )
          status=$?
          set -e
          echo "probe exit=$status"
          echo "$log"
          if [ "$status" -eq 0 ]; then
            echo "FAIL: lnrentd exited 0 with a garbage invite; the probe proves nothing"
            exit 1
          fi
          case "$log" in
            *"${marker}"*) found=1 ;;
            *) found=0 ;;
          esac
          if [ "$found" -ne ${if expectMarker then "1" else "0"} ]; then
            echo "FAIL: expected the fedimint marker to be ${if expectMarker then "PRESENT" else "ABSENT"}: ${marker}"
            exit 1
          fi
          # …and the message that proves WHERE this build stopped (see `refusal` above).
          case "$log" in
            *"${expected}"*) ;;
            *)
              echo "FAIL: expected the log to contain: ${expected}"
              exit 1
              ;;
          esac
          touch "$out"
        '';

        # The negative control for that probe. Built `--no-default-features` off the SAME cached
        # artifacts — and it keeps the check above honest: a probe that has never been observed to
        # fail is not a check.
        #
        # MEASURED, not assumed: this compiles 126 crates in ~44s. Dropping the feature changes
        # feature unification for shared dependencies (nostr-sdk, rusqlite, tokio-tungstenite, …),
        # so those rebuild even though the artifacts are shared. What the sharing does buy is the
        # expensive half: no fedimint crate and no librocksdb-sys appears in that build log.
        lnrentdNoFedimint = craneLib.buildPackage (commonArgs // {
          inherit cargoArtifacts;
          pname = "lnrentd-no-fedimint";
          cargoExtraArgs = "--locked --no-default-features -p lnrentd";
        });

        image = pkgs.dockerTools.buildLayeredImage {
          name = "lnrent";
          # `version` is the crate manifest's literal (daemon/Cargo.toml:3) and nothing bumps it, so
          # every revision of this image is tagged `lnrent:0.1.0`. Fine for `docker load` locally;
          # whatever eventually PUSHES this (lnrent-hyb) must retag per revision, or a node cache
          # plus Kubernetes' default `imagePullPolicy: IfNotPresent` will keep serving an old build.
          tag = version;
          # Layered so the fat store paths are shared between rebuilds.
          contents = [
            lnrentBins
            # NOTE: `recipesDir` is deliberately NOT here. Listing it would republish the tree as a
            # symlink farm under `/opt`, which is exactly the layout that fails recipe validation
            # (see `recipesDir` above). The daemon reaches it through `LNRENT_RECIPES_DIR`, and that
            # reference is what pulls the path into the image closure — so there is exactly one
            # recipe tree in the image and it is the one that works.
            # `recipes/do-vps/*` hooks are `#!/usr/bin/env bash`. They are spawned `.env_clear()`ed
            # (lnrent-y4m.7) with a fixed allowlist forwarded from the DAEMON's env, and `PATH` is
            # the first name on it (daemon/src/runner.rs:32) — so the `PATH` in the image config
            # below is the one the hooks actually get. That is load-bearing, not cosmetic: nixpkgs
            # builds bash with no usable built-in default PATH, so a hook that gets none finds
            # nothing. MEASURED in this image on `do-vps/preflight`, with DO_TOKEN set because its
            # line-9 guard is pure bash and fires before any external tool is reached:
            #   * `env -i DO_TOKEN=dummy …/preflight`   → "line 16: mktemp: command not found";
            #   * `env -i PATH=/bin:/usr/bin DO_TOKEN=dummy …/preflight` → mktemp, jq and curl all
            #     resolve, and curl reaches the DigitalOcean API over TLS (HTTP 401 on the dummy
            #     token) — which also exercises the CA bundle below, not merely its presence.
            # The packages below cover every external command the hooks invoke: jq, curl, grep, sed
            # and a handful of coreutils tools (mktemp, cat, printf, rm, tr, cut, seq, sleep) —
            # don't trust a frozen list here, re-derive it with
            # `grep -rhoE '\b(jq|curl|...)\b' recipes/*/`.
            # Two tokens that look like tools and are NOT, so nothing here needs to cover them:
            # `ssh` appears once, inside a public-key FORMAT regex (recipes/do-vps/provision:38) —
            # every other hit is an `ssh_pubkey` / `ssh_keys` / `ssh_pwauth` parameter or
            # cloud-config NAME. And `find` appears once, in prose ("find-by-tag",
            # recipes/do-vps/provision:12), so no findutils either.
            pkgs.bash
            pkgs.coreutils
            pkgs.gnugrep
            pkgs.gnused
            pkgs.curl
            pkgs.jq
            # lnrentd otherwise runs as a minimal PID 1, which cannot reap hook grandchildren that
            # are orphaned when a timed-out process group is killed (daemon/src/runner.rs:293-300).
            pkgs.tini
            # TLS to the Nostr relays, the DigitalOcean API and phoenixd. This places both
            # /etc/ssl/certs/ca-bundle.crt and ca-certificates.crt; `cacert` itself rides in on the
            # closure as their symlink target.
            pkgs.dockerTools.caCertificates
            # /usr/bin/env for the hook shebangs, /bin/sh, and an /etc/passwd + /etc/nsswitch.conf.
            pkgs.dockerTools.usrBinEnv
            pkgs.dockerTools.binSh
            pkgs.dockerTools.fakeNss
          ];
          # The hooks call `mktemp`, which needs a writable /tmp that a scratch image lacks.
          extraCommands = ''
            mkdir -p tmp
            chmod 1777 tmp
          '';
          config = {
            Entrypoint = [ "${pkgs.tini}/bin/tini" "--" ];
            Cmd = [ "lnrentd" ];
            Env = [
              "PATH=/bin:/usr/bin"
              "SSL_CERT_FILE=/etc/ssl/certs/ca-bundle.crt"
              # With no mounted volume the daemon creates this path privately. A deployment must
              # preserve a safe path chain: `reject_unsafe_components` checks every existing
              # ancestor, so an fsGroup-writable, non-sticky parent is rejected too
              # (daemon/src/config.rs:2166-2221). The Kubernetes volume layout belongs to lnrent-hyb.
              "LNRENT_DATA_DIR=/var/lib/lnrent"
              "LNRENT_RECIPES_DIR=${recipesDir}"
            ];
          };
        };
      in
      {
        packages = {
          lnrentd = pickBin "lnrentd";
          lnrent = pickBin "lnrent";
          lnrent-buyer = pickBin "lnrent-buyer";
          default = pickBin "lnrentd";
        } // pkgs.lib.optionalAttrs pkgs.stdenv.isLinux {
          inherit image;
        };

        checks = {
          # Both directions of the R5 probe. The second one is the proof that the first can fail.
          fedimint-compiled-in = runFedimintProbe {
            name = "lnrentd-fedimint-compiled-in";
            drv = lnrentBins;
            expectMarker = true;
          };
          fedimint-probe-discriminates = runFedimintProbe {
            name = "lnrentd-fedimint-probe-discriminates";
            drv = lnrentdNoFedimint;
            expectMarker = false;
          };
        };

        devShells.default = pkgs.mkShell {
          packages = [ rustToolchain ] ++ nativeBuildDeps ++ buildDeps ++ (with pkgs; [
            # wasm tooling for the web buyer (.18) and the rust-nostr gift-wrap feasibility spike (.19)
            wasm-pack
            wasm-bindgen-cli
            binaryen   # wasm-opt
            trunk

            # ops convenience
            sqlite

            # dependency hygiene: `cargo outdated` reports deps behind their latest
            # release, INCLUDING semver-major bumps that `cargo update` will never make.
            cargo-outdated
          ]);

          # bindgen (librocksdb-sys, secp256k1) needs to find libclang
          LIBCLANG_PATH = libclangPath;

          # secp256k1-sys compiles bundled C to wasm via cc-rs. The nix cc-wrapper injects host
          # hardening flags (e.g. -fzero-call-used-regs) that clang rejects for wasm32, so point
          # cc-rs at an UNWRAPPED clang/llvm-ar for the wasm target (the wrapper itself advises this).
          CC_wasm32_unknown_unknown = "${pkgs.llvmPackages.clang-unwrapped}/bin/clang";
          AR_wasm32_unknown_unknown = "${pkgs.llvmPackages.llvm}/bin/llvm-ar";

          shellHook = ''
            echo "lnrent devshell · $(rustc --version)"
            echo "targets: native (x86_64-gnu) + wasm32-unknown-unknown"
            echo "  (to link a system rocksdb instead of the bundled build, export"
            echo "   ROCKSDB_LIB_DIR=${pkgs.rocksdb}/lib ROCKSDB_INCLUDE_DIR=${pkgs.rocksdb}/include)"
          '';
        };
      });
}
