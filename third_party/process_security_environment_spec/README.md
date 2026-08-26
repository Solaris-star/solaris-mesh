# Microsoft PSEC generated bindings

This directory contains the minimum generated Rust crate needed to encode and
decode the Windows Process Security Environment (`PSEC`) FlatBuffer.

- Upstream repository: `https://github.com/microsoft/mxc`
- Source commit: `b497fd48653e01c846c6ef225e0af1c859b70122`
- Upstream crate path: `src/core/generated/process_security_environment_specification`
- Upstream schema: `external/windows-sdk/ProcessSecurityEnvironment.fbs`
- Package version at the pinned commit: `0.8.0`
- License: MIT; see `LICENSE-MIT`. The upstream text is preserved, with one
  final LF added by the Solaris text-file convention. The pinned upstream
  SHA-256 is `d9a1b1e30d633d5732ea18e3cba9538d293ebc53e1a9e4e96ab739e0c5c4f1cb`;
  the normalized local SHA-256 is
  `c2cfccb812fe482101a8f04597dfc5a9991a6b2748266c47ac91b6a5aae15383`.

Only the generated crate source is vendored. The `sandbox_spec` crate and the
BaseContainer/SBOX schema are deliberately not included. The sole source-level
change is a `#[rustfmt::skip]` attribute on the generated module declaration so
the Solaris workspace formatter leaves the upstream generated files unchanged.

To update this copy:

1. Select and review a new immutable Microsoft `mxc` commit.
2. Copy only the upstream crate's `src/lib.rs` and
   `src/process_security_environment_layout/*_generated.rs` files.
3. Copy the upstream root `LICENSE.md` to `LICENSE-MIT`, normalize it to one
   final LF, and update both license hashes above if the selected source changes.
4. Update the commit, upstream package version, and FlatBuffers version in this
   file and `Cargo.toml`.
5. Reapply the single `#[rustfmt::skip]` attribute to `src/lib.rs` and compare
   every `src/process_security_environment_layout/*_generated.rs` file
   byte-for-byte with the selected commit. Then run the Solaris PSEC codec
   tests, full `solaris-process` tests, strict Clippy, formatting, metadata, and
   offline dependency checks.
