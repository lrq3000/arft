# arft

**arft** stands for **Android Robust File Transfer**.

It provides robust Android file-by-file transfer over ADB with resumable behavior, atomic writes, lazy remote metadata fetching, cached file-list discovery, and `tqdm` progress reporting.

It was tested on Android 10 and Windows 11 Pro.

## Features

- atomic operations (ie, crashing in the middle of a file download ensures it will get redownloaded on resuming)
- resumes safely and fast after interruption
- never exposes partial files as completed output
- validates file size and optionally SHA-256
- preserves timestamps as closely as Android exposes them
- caches the discovered remote file list to speed up later restarts
- supports `--refresh-file-list` to append newly discovered files without forcing full re-copy
- supports `--force-all` to refresh discovery and re-copy everything

## Installation or updating

First you need to download and unzip the latest version of ADB (part of [Google's Platform Tools](https://developer.android.com/tools/releases/platform-tools)).

Secondly you need a Python interpreter. Miniconda is awesome and small.

Then install `arft` using:

```bash
pip install --upgrade arft
```

## Usage

### As a module

The internal Python module path remains `android_10_robust_file_transfer`.

```bash
python -m android_10_robust_file_transfer \
  --adb-path "C:\platform-tools\adb.exe" \
  --remote-root "/storage/emulated/0/DCIM" \
  --local-root "D:\AndroidBackup\DCIM"
```

### As a console script

```bash
arft \
  --adb-path "C:\platform-tools\adb.exe" \
  --remote-root "/storage/emulated/0/DCIM" \
  --local-root "D:\AndroidBackup\DCIM"
```

## Common options

- `--verify-hash`: enable SHA-256 verification after size verification
- `--refresh-file-list`: refresh the cached remote file list but still skip already complete local files
- `--force-all`: refresh the remote file list and re-copy all files
- `--check-all-files`: check all previous downloaded files for size matching, and optionally hash matching if combined with `--verify-hash` 
- `--dry-run`: print planned files without copying them

## License

Published under the opensource MIT License.

## Author

This project was developed by Stephen Karl Larroque with agentic AI (OpenCode + Oh-My-Openagent harness/agentic orchestration system with the model OpenAI ChatGPT Codex-5.3).

## Alternatives

* [ADB Explorer](https://github.com/Alex4SSB/ADB-Explorer) which offers a much more features complete GUI, but it fails with large files/folders and is much slower (this is why this script was made).
* [better-adbsync](https://github.com/jb2170/better-adb-sync), a rsync-like tool to synchronize files between Android and a desktop computer.
