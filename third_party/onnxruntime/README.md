# ONNX Runtime Third-Party Binaries

This repository is configured to load ONNX Runtime from a local, vendored path instead of downloading binaries during `cargo build`.

## Target

- Linux `x86_64` only (`linux-x86_64`)

## Expected layout

Place ONNX Runtime shared libraries under:

`third_party/onnxruntime/linux-x86_64/lib`

At minimum:

- `libonnxruntime.so`

Recommended (keep symlinks/versioned files together):

- `libonnxruntime.so`
- `libonnxruntime.so.<major>`
- `libonnxruntime.so.<full-version>`

## Build behavior

`mkt_model_runtime` enables `ort`'s `load-dynamic` feature, and `.cargo/config.toml` sets:

- `ORT_DYLIB_PATH=third_party/onnxruntime/linux-x86_64/lib/libonnxruntime.so`

The library is loaded only when inference is first used. This also lets non-inference tests start without requiring ONNX Runtime to be present in the process loader's default search path.

## Runtime note

Cargo commands receive `ORT_DYLIB_PATH` from `.cargo/config.toml`. When running a copied binary outside Cargo, either set `ORT_DYLIB_PATH` to the full library path or make the library discoverable through `LD_LIBRARY_PATH`:

```bash
export ORT_DYLIB_PATH="$PWD/third_party/onnxruntime/linux-x86_64/lib/libonnxruntime.so"
# or
export LD_LIBRARY_PATH="$PWD/third_party/onnxruntime/linux-x86_64/lib:${LD_LIBRARY_PATH:-}"
```

## Quick install helper

Use:

```bash
bash scripts/install_ort_third_party.sh /path/to/onnxruntime-linux-x64-<ver>.tgz
```

or pass an extracted directory:

```bash
bash scripts/install_ort_third_party.sh /path/to/onnxruntime-linux-x64-<ver>
```
