# arft

**arft** stands for **Android Robust File Transfer**.

It provides robust Android file-by-file transfer over ADB with resumable behavior, atomic writes, lazy remote metadata fetching, cached remote file discovery, and `tqdm` progress reporting for checking, metadata prefetch, and transfer.

It was tested on Android 10 and Windows 11 Pro.

## Features

- atomic operations (ie, crashing in the middle of a file download ensures it will get redownloaded on resuming)
- resumes safely and fast after interruption
- resumes downloading even if provided with a partial download started with another app or manually (ie, can be used to incrementally update a backup, previously downloaded files will be checked and only newer/different files will be updated/added)
- never exposes partial files as completed output
- validates file size and optionally SHA-256
- preserves timestamps as closely as Android exposes them
- caches the discovered remote file list to speed up later restarts
- can rebuild its local bookkeeping state from an already partially downloaded folder
- supports `--refresh-file-list` to append newly discovered files without forcing full re-copy
- supports `--force-all` to refresh discovery and re-copy everything
- supports `--skip-all-checks` for the fastest possible resume when you trust already-downloaded files
- supports `--verbose` to log every issued ADB command

## Installation or updating

First you need to download and unzip the latest version of ADB (part of [Google's Platform Tools](https://developer.android.com/tools/releases/platform-tools)).

Secondly you need a Python interpreter. Miniconda is awesome and small.

Thirdly you need to enable `USB debugging` = adb debugging on your Android phone (so you need to enable the Developer options).

Fourthly, you can then install `arft` using:

```bash
pip install --upgrade arft
```

## Usage

### As a module

```bash
python -m arft \
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

- `--verbose`: log every issued ADB command into the console and `.arft.log`
- `--verify-hash`: enable SHA-256 verification after size verification
- `--refresh-file-list`: refresh the cached remote file list but still skip already complete local files
- `--force-all`: refresh the remote file list and re-copy all files
- `--check-all-files`: strictly revalidate already-downloaded files against the phone before skipping them; if combined with `--verify-hash`, this also re-checks hashes for those files
- `--skip-all-checks`: trust any already-present local file immediately and skip all existing-file validation checks; this takes precedence over `--check-all-files` and resume-time hash checking
- `--exclude REGEXP`: exclude remote relative paths matching a Python regular expression before metadata prefetch, checking, dry-run output, and transfer planning
- `--dry-run`: print planned files without copying them

### Excluding subpaths with regular expressions

`--exclude` uses a regular expression that is applied to each remote relative path. This is useful for skipping generated media caches or app metadata folders anywhere under the chosen remote root.

Example:

```bash
arft \
  --adb-path "C:\platform-tools\adb.exe" \
  --remote-root "/storage/emulated/0" \
  --local-root "D:\AndroidBackup" \
  --exclude "(\.thumbnails|\.Gallery2)"
```

That example excludes any remote relative path containing either `.thumbnails` or `.Gallery2`.

## Bookkeeping files created in `--local-root`

ARFT creates a few hidden bookkeeping files in the destination folder so it can resume safely and quickly:

- `.arft-local-sync-state.json`: the local sync state manifest. It records which files were already completed successfully, along with the locally known size and timestamp metadata used for fast resume decisions.
- `.arft-remote-files-list.json`: the cached recursive remote file list. It lets ARFT skip the expensive remote re-listing step on later runs unless you use `--refresh-file-list` or `--force-all`.
- `.arft-failed-files.tsv`: a tab-separated list of files that failed during the run, together with the failure reason.
- `.arft.log`: the persistent run log. With `--verbose`, it also contains every issued ADB command prefixed with `ADB CMD:`.

## Resume and recovery behavior

- Normal resume is fast by default: if `.arft-local-sync-state.json` says a file already finished successfully and the local file still matches the saved local size, ARFT skips it without querying the phone again.
- `--check-all-files` restores a stricter mode where existing local files are revalidated against remote metadata before they are skipped.
- `--skip-all-checks` is the fastest mode: any already-existing local file is trusted immediately.
- If the destination folder already contains downloaded payload files but the local sync state file is missing, ARFT automatically rebuilds its state and behaves as if `--check-all-files` were enabled for that bootstrap run.

## License

Published under the opensource MIT License.

## Author

This project was developed by Stephen Karl Larroque with agentic AI (OpenCode + Oh-My-Openagent harness/agentic orchestration system with the model OpenAI ChatGPT Codex-5.3).

## Alternatives

* [ADB Explorer](https://github.com/Alex4SSB/ADB-Explorer) which offers a much more features complete GUI, but it fails with large files/folders and is much slower (this is why this script was made).
* [better-adbsync](https://github.com/jb2170/better-adb-sync), a rsync-like tool to synchronize files between Android and a desktop computer.
