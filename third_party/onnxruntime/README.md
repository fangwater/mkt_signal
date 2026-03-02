# ONNX Runtime Third-Party Binaries

This repository is configured to link ONNX Runtime from a local, vendored path instead of downloading binaries during `cargo build`.

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

`.cargo/config.toml` sets:

- `ORT_LIB_LOCATION=third_party/onnxruntime/linux-x86_64/lib`
- `ORT_PREFER_DYNAMIC_LINK=1`
- `ORT_SKIP_DOWNLOAD=1`

So `ort-sys` links from this local path and does not perform network downloads.

## Runtime note

If your runtime loader cannot find `libonnxruntime.so`, set:

```bash
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
