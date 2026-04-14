
#!/usr/bin/env python3
r"""
Robust Android file-by-file pull over ADB.

Design goals:
- Resume safely after interruption.
- Never leave a partially downloaded final file in place:
  downloads go to "<name>.part" and are atomically renamed only after validation.
- Validate by size by default; SHA-256 is optional because it is slower.
- Preserve timestamps on the destination as closely as Android exposes them.
- Add visible progress with tqdm.
- Emit detailed logs for later diagnosis.

Typical usage on Windows:
    python -m arft ^
      --adb-path "C:\platform-tools\adb.exe" ^
      --remote-root "/storage/emulated/0/DCIM" ^
      --local-root "D:\AndroidBackup\DCIM"

Optional hash verification:
    python -m arft ... --verify-hash

Force re-copy of everything:
    python -m arft ... --force-all
"""
from __future__ import annotations

import argparse
import ctypes
import hashlib
import json
import logging
import math
import os
import re
import shlex
import shutil
import subprocess
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Tuple

try:
    from tqdm import tqdm
except Exception as exc:  # pragma: no cover
    print("This script requires tqdm. Install it with: py -m pip install tqdm", file=sys.stderr)
    raise


# ----------------------------- Data structures ----------------------------- #

VERBOSE_ADB = False
ADB_LOGGER: Optional[logging.Logger] = None
ADB_EXECUTABLE: Optional[str] = None

@dataclass
class RemoteFile:
    relpath: str
    # Metadata is now populated lazily. The initial listing only gathers the
    # relative paths so startup stays fast on large Android trees.
    size: Optional[int] = None
    mtime: Optional[int] = None
    birth: Optional[int] = None


# ------------------------------- Utilities -------------------------------- #

def quote_remote(path: str) -> str:
    """Safely quote a remote shell path for 'adb shell sh -c ...'."""
    return shlex.quote(path)


REMOTE_STAT_META_RE = re.compile(r"([\\\s\[\](){}<>|&;*?!$`\"'#~])")


def escape_remote_stat_path(path: str) -> str:
    """
    Escape remote paths that are passed as plain 'stat' operands through adb shell.

    Even without an explicit 'sh -c' wrapper, device-side shell parsing can still
    split or interpret metacharacters before Android's plain stat sees the path.
    We therefore prefix shell-sensitive characters with backslashes while keeping
    the existing plain stat output format and parser unchanged.
    """
    return REMOTE_STAT_META_RE.sub(r"\\\1", path)


def run(
    cmd: List[str],
    *,
    text: bool = True,
    capture_output: bool = True,
    check: bool = True,
    timeout: Optional[int] = None,
) -> subprocess.CompletedProcess:
    """Thin wrapper around subprocess.run with consistent defaults."""
    if VERBOSE_ADB and ADB_LOGGER and cmd:
        executable = os.path.normcase(os.path.normpath(cmd[0]))
        configured = os.path.normcase(os.path.normpath(ADB_EXECUTABLE)) if ADB_EXECUTABLE else None
        if configured is None or executable == configured:
            # Use shell-style quoting so the emitted command is copy-pasteable and
            # easy to grep in the persistent transfer log.
            ADB_LOGGER.info("ADB CMD: %s", shlex.join(cmd))
    return subprocess.run(
        cmd,
        text=text,
        capture_output=capture_output,
        check=check,
        timeout=timeout,
    )


def adb_shell(adb_path: str, shell_cmd: str, *, check: bool = True) -> subprocess.CompletedProcess:
    """
    Run a shell command on the Android device.

    'sh -c' gives us predictable quoting and lets us use pipes/conditionals
    without depending on how adb concatenates arguments.
    """
    return run([adb_path, "shell", "sh", "-c", shell_cmd], check=check)


def adb_shell_args(adb_path: str, shell_args: List[str], *, check: bool = True) -> subprocess.CompletedProcess:
    """
    Run a direct adb shell argv call without wrapping it in 'sh -c'.

    This is more reliable for Android's plain multi-file stat output than
    constructing one large shell string.
    """
    return run([adb_path, "shell", *shell_args], check=check)


def ensure_adb_device(adb_path: str) -> None:
    """
    Fail early if adb is unavailable or no device is ready.
    """
    cp = run([adb_path, "devices"], text=True)
    lines = [line.strip() for line in cp.stdout.splitlines() if line.strip()]
    if not lines or not lines[0].lower().startswith("list of devices attached"):
        raise RuntimeError(f"Unexpected adb devices output:\n{cp.stdout}")
    states = []
    for line in lines[1:]:
        parts = line.split()
        if len(parts) >= 2:
            states.append((parts[0], parts[1]))
    if not states:
        raise RuntimeError("No Android device detected by adb.")
    bad = [s for s in states if s[1] not in {"device"}]
    if bad:
        raise RuntimeError(f"ADB device not ready: {bad}")


def choose_hash_cmd(adb_path: str) -> Optional[str]:
    """
    Detect a hash command on the Android device.

    Preference order:
    - sha256sum
    - toybox sha256sum
    """
    probes = [
        "command -v sha256sum >/dev/null 2>&1 && echo sha256sum",
        "toybox sha256sum /dev/null >/dev/null 2>&1 && echo 'toybox sha256sum'",
    ]
    for probe in probes:
        cp = adb_shell(adb_path, probe, check=False)
        out = (cp.stdout or "").strip()
        if cp.returncode == 0 and out:
            return out
    return None


def device_supports_find_printf(adb_path: str) -> bool:
    """
    Check whether the device-side find supports -printf.

    Android toybox support varies by release, so we probe at runtime and keep a
    portable shell-loop fallback when it is unavailable.
    """
    cp = adb_shell(adb_path, "find . -maxdepth 0 -printf '' >/dev/null 2>&1", check=False)
    return cp.returncode == 0


def device_supports_multi_stat(adb_path: str) -> bool:
    """
    Check whether device-side stat accepts multiple file operands.
    """
    probe = "command -v stat >/dev/null 2>&1 && stat /dev/null /dev/null >/dev/null 2>&1"
    cp = adb_shell(adb_path, probe, check=False)
    return cp.returncode == 0


def calc_local_sha256(path: Path, chunk_size: int = 1024 * 1024) -> str:
    """
    Stream the local file to avoid large RAM spikes.
    """
    digest = hashlib.sha256()
    with path.open("rb") as f:
        while True:
            chunk = f.read(chunk_size)
            if not chunk:
                break
            digest.update(chunk)
    return digest.hexdigest()


def calc_remote_sha256(adb_path: str, remote_path: str, hash_cmd: str) -> str:
    """
    Calculate SHA-256 on the Android device.
    """
    cmd = f"{hash_cmd} {quote_remote(remote_path)} | awk '{{print $1}}'"
    cp = adb_shell(adb_path, cmd)
    value = (cp.stdout or "").strip().splitlines()
    if not value:
        raise RuntimeError(f"Could not read remote hash for {remote_path!r}")
    return value[-1].strip()


def infer_remote_root_name(remote_root: str) -> str:
    """
    Use the last path component as a human-friendly progress label fallback.
    """
    remote_root = remote_root.rstrip("/")
    return Path(remote_root).name or remote_root


# ------------------------- Timestamp preservation -------------------------- #

if os.name == "nt":
    from ctypes import wintypes

    FILE_WRITE_ATTRIBUTES = 0x0100
    OPEN_EXISTING = 3

    class FILETIME(ctypes.Structure):
        _fields_ = [("dwLowDateTime", wintypes.DWORD), ("dwHighDateTime", wintypes.DWORD)]

    CreateFileW = ctypes.windll.kernel32.CreateFileW
    CreateFileW.argtypes = [
        wintypes.LPCWSTR,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.LPVOID,
        wintypes.DWORD,
        wintypes.DWORD,
        wintypes.HANDLE,
    ]
    CreateFileW.restype = wintypes.HANDLE

    SetFileTime = ctypes.windll.kernel32.SetFileTime
    SetFileTime.argtypes = [
        wintypes.HANDLE,
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
        ctypes.POINTER(FILETIME),
    ]
    SetFileTime.restype = wintypes.BOOL

    CloseHandle = ctypes.windll.kernel32.CloseHandle
    CloseHandle.argtypes = [wintypes.HANDLE]
    CloseHandle.restype = wintypes.BOOL

    INVALID_HANDLE_VALUE = wintypes.HANDLE(-1).value

    def _unix_to_filetime(unix_seconds: int) -> FILETIME:
        """
        Convert UNIX epoch seconds to Windows FILETIME (100ns ticks since 1601-01-01 UTC).
        """
        value = int((unix_seconds + 11644473600) * 10_000_000)
        return FILETIME(value & 0xFFFFFFFF, value >> 32)

    def set_windows_file_times(path: Path, *, creation: Optional[int], access: Optional[int], modification: Optional[int]) -> None:
        """
        Preserve file timestamps on Windows, including creation time if available.

        If Android does not expose a true birth time, the caller can pass the
        modification time for creation as a best-effort approximation.
        """
        handle = CreateFileW(
            str(path),
            FILE_WRITE_ATTRIBUTES,
            0,
            None,
            OPEN_EXISTING,
            0,
            None,
        )
        if handle == INVALID_HANDLE_VALUE:
            raise OSError(f"Could not open {path} to set file times")

        c_ft = _unix_to_filetime(creation) if creation is not None else None
        a_ft = _unix_to_filetime(access) if access is not None else None
        m_ft = _unix_to_filetime(modification) if modification is not None else None

        try:
            ok = SetFileTime(
                handle,
                ctypes.byref(c_ft) if c_ft else None,
                ctypes.byref(a_ft) if a_ft else None,
                ctypes.byref(m_ft) if m_ft else None,
            )
            if not ok:
                raise ctypes.WinError()
        finally:
            CloseHandle(handle)
else:
    def set_windows_file_times(path: Path, *, creation: Optional[int], access: Optional[int], modification: Optional[int]) -> None:
        """
        Non-Windows fallback: preserve atime/mtime via os.utime where possible.
        """
        atime = access if access is not None else modification if modification is not None else int(time.time())
        mtime = modification if modification is not None else atime
        os.utime(path, (atime, mtime), follow_symlinks=False)


def apply_timestamps(local_path: Path, meta: RemoteFile) -> None:
    """
    Best-effort timestamp preservation.

    Android usually gives reliable mtime. Creation time is less portable, so if
    a true birth time is unavailable we align destination creation time to mtime.
    """
    mtime = meta.mtime
    birth = meta.birth if meta.birth is not None else meta.mtime
    set_windows_file_times(local_path, creation=birth, access=mtime, modification=mtime)


# ------------------------- Remote metadata listing ------------------------- #

def list_remote_files(
    adb_path: str,
    remote_root: str,
    logger: logging.Logger,
    *,
    prefer_find_printf: bool,
) -> List[RemoteFile]:
    """
    Build a path-only file manifest from the Android device.

    We intentionally keep the initial pass minimal and only collect relative
    paths. On many phones the expensive part is per-file metadata probing over
    ADB, not the local Python bookkeeping, so size and timestamps are fetched
    only for files that actually need validation or transfer.

    Output format per line:
        relative_path

    Newlines inside filenames are not supported by this script.
    """
    remote_root = remote_root.rstrip("/")
    logger.info("Listing remote files under %s", remote_root)

    shell_script = f"""
root={quote_remote(remote_root)}
if [ ! -d "$root" ]; then
  echo "Remote root does not exist: $root" >&2
  exit 2
fi

if {'true' if prefer_find_printf else 'false'}; then
  find "$root" -type f -printf '%P\\n'
else
  find "$root" -type f -print | while IFS= read -r f; do
    rel="${{f#"$root"/}}"
    printf '%s\\n' "$rel"
  done
fi
"""
    cp = adb_shell(adb_path, shell_script)
    files: List[RemoteFile] = []
    for idx, line in enumerate((cp.stdout or "").splitlines(), start=1):
        if not line.strip():
            continue
        files.append(RemoteFile(relpath=line))

    logger.info("Remote listing complete: %d files", len(files))
    return files


def _parse_plain_stat_timestamp(value: str) -> Optional[int]:
    """
    Parse one plain 'stat' timestamp line into a UNIX timestamp.
    """
    raw = value.strip()
    if not raw:
        return None
    if "." in raw:
        head, tail = raw.split(".", 1)
        if " " in tail:
            frac, zone = tail.split(" ", 1)
            raw = f"{head}.{frac[:6]:0<6} {zone}"
    try:
        return int(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S.%f %z").timestamp())
    except ValueError:
        pass
    try:
        return int(datetime.strptime(raw, "%Y-%m-%d %H:%M:%S %z").timestamp())
    except ValueError:
        return None


def parse_plain_stat_output(output: str, remote_root: str) -> Dict[str, Tuple[int, Optional[int], Optional[int]]]:
    """
    Parse plain multi-file Android 'stat' output.

    Expected structure per file block is similar to:
      File: /path
      Size: 1234 ...
      ...
      Modify: 2026-04-11 18:19:12.000000000 +0200

    We use Modify as mtime and leave birth=None because plain Android stat does
    not expose a portable creation time field in this format.
    """
    file_re = re.compile(r"^\s*File:\s+(?P<path>.+)$")
    size_re = re.compile(r"^\s*Size:\s+(?P<size>\d+)")
    modify_re = re.compile(r"^\s*Modify:\s+(?P<modify>.+)$")

    parsed: Dict[str, Tuple[int, Optional[int], Optional[int]]] = {}
    current_path: Optional[str] = None
    current_size: Optional[int] = None
    current_mtime: Optional[int] = None

    def flush_current() -> None:
        nonlocal current_path, current_size, current_mtime
        if current_path and current_size is not None:
            root_prefix = f"{remote_root.rstrip('/')}/"
            relpath = current_path[len(root_prefix):] if current_path.startswith(root_prefix) else current_path
            parsed[relpath] = (current_size, current_mtime, None)
        current_path = None
        current_size = None
        current_mtime = None

    for line in output.splitlines():
        file_match = file_re.match(line)
        if file_match:
            flush_current()
            current_path = file_match.group("path").strip()
            continue

        if current_path is None:
            continue

        size_match = size_re.match(line)
        if size_match:
            current_size = int(size_match.group("size"))
            continue

        modify_match = modify_re.match(line)
        if modify_match:
            current_mtime = _parse_plain_stat_timestamp(modify_match.group("modify"))

    flush_current()
    return parsed


def populate_remote_metadata_batch(
    adb_path: str,
    remote_root: str,
    remote_files: List[RemoteFile],
    logger: logging.Logger,
    *,
    chunk_size: int = 128,
) -> None:
    """
    Populate metadata for many files with batched stat calls.

    This is primarily used for skip validation, where many local files may
    already exist and the per-file adb round-trip becomes the new bottleneck.
    Any path that is missing from the batched output falls back to the existing
    one-file metadata query so correctness stays the same across Android builds.
    """
    pending = [rf for rf in remote_files if rf.size is None]
    if not pending:
        return

    remote_root = remote_root.rstrip("/")
    metadata_pbar = tqdm(
        total=len(pending),
        unit="file",
        desc=f"{infer_remote_root_name(remote_root)} stat",
        dynamic_ncols=True,
        smoothing=0.1,
    )
    try:
        for start in range(0, len(pending), chunk_size):
            chunk = pending[start:start + chunk_size]
            by_relpath = {rf.relpath: rf for rf in chunk}
            remote_paths = [escape_remote_stat_path(remote_join(remote_root, rf.relpath)) for rf in chunk]
            cp = adb_shell_args(adb_path, ["stat", *remote_paths], check=False)
            parsed = parse_plain_stat_output(cp.stdout or "", remote_root)

            resolved_in_batch = 0
            for relpath, (size, mtime, birth) in parsed.items():
                rf = by_relpath.get(relpath)
                if not rf or rf.size is not None:
                    continue
                rf.size = size
                rf.mtime = mtime
                rf.birth = birth
                resolved_in_batch += 1

            if resolved_in_batch:
                metadata_pbar.update(resolved_in_batch)

            # Some devices return partial output or a non-zero exit if one path went
            # stale mid-run. We only fall back for unresolved entries so the common
            # case still benefits from batched metadata collection.
            unresolved = [rf for rf in chunk if rf.size is None]
            if unresolved:
                logger.info("Batched stat missed %d/%d files; falling back to single-file metadata queries", len(unresolved), len(chunk))
                for rf in unresolved:
                    ensure_remote_metadata(adb_path, remote_root, rf)
                    metadata_pbar.update(1)
    finally:
        metadata_pbar.close()


def ensure_remote_metadata(adb_path: str, remote_root: str, remote_meta: RemoteFile) -> RemoteFile:
    """
    Populate remote size and timestamps on demand for one file.

    We cache the result directly on the RemoteFile instance so repeated callers
    in the same run do not re-stat the same path.
    """
    if remote_meta.size is not None:
        return remote_meta

    remote_path = escape_remote_stat_path(remote_join(remote_root, remote_meta.relpath))
    cp = adb_shell_args(adb_path, ["stat", remote_path])
    parsed = parse_plain_stat_output(cp.stdout or "", remote_root)
    metadata = parsed.get(remote_meta.relpath)
    if metadata is None:
        raise RuntimeError(f"Remote metadata query returned no parseable output for {remote_meta.relpath}: {(cp.stdout or '').strip()!r}")

    size, mtime, birth = metadata

    remote_meta.size = size
    remote_meta.mtime = mtime
    remote_meta.birth = birth
    return remote_meta


# ------------------------------ Local state ------------------------------- #

def load_manifest(path: Path) -> Dict[str, Dict[str, object]]:
    """
    Load previous transfer state. This is separate from the general log so we can
    quickly answer "what is already complete?" at startup.
    """
    if not path.exists():
        return {}
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
        if isinstance(data, dict):
            return data
    except Exception:
        pass
    return {}


def save_manifest(path: Path, manifest: Dict[str, Dict[str, object]]) -> None:
    """
    Atomic manifest write to survive crashes.
    """
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(manifest, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def load_file_list_cache(path: Path, *, remote_root: str) -> Optional[List[RemoteFile]]:
    """
    Load a cached remote file list if it matches the requested remote root.

    The cache intentionally stores only relative paths so restart-time loading is
    cheap and does not freeze in stale metadata decisions.
    """
    if not path.exists():
        return None
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None
    if not isinstance(data, dict):
        return None
    if data.get("remote_root") != remote_root:
        return None
    raw_files = data.get("files")
    if not isinstance(raw_files, list):
        return None

    files: List[RemoteFile] = []
    for item in raw_files:
        if isinstance(item, str) and item:
            files.append(RemoteFile(relpath=item))
    return files


def save_file_list_cache(path: Path, *, remote_root: str, remote_files: List[RemoteFile]) -> None:
    """
    Save the remote file discovery results for restart-time reuse.
    """
    payload = {
        "remote_root": remote_root,
        "generated_at": int(time.time()),
        "files": [rf.relpath for rf in remote_files],
    }
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    tmp.replace(path)


def local_root_has_payload_files(local_root: Path, *, manifest_name: str) -> bool:
    """
    Detect whether the destination already contains real payload files.

    We ignore the tool's own bookkeeping files so a fresh run that only created
    a log file does not accidentally trigger recovery mode.
    """
    ignored_names = {
        manifest_name,
        ".arft-remote-files-list.json",
        ".arft-failed-files.tsv",
        ".arft.log",
    }
    for path in local_root.rglob("*"):
        if not path.is_file():
            continue
        if path.name in ignored_names:
            continue
        return True
    return False


def manifest_says_file_is_complete(
    local_path: Path,
    relpath: str,
    manifest: Dict[str, Dict[str, object]],
) -> bool:
    """
    Trust a previously successful manifest entry using local-only checks.

    This makes resume fast: if a file was already recorded as ok, we only check
    that the local file still exists and still matches the saved local size.
    """
    entry = manifest.get(relpath)
    if not isinstance(entry, dict):
        return False
    if entry.get("status") != "ok":
        return False
    if not local_path.exists() or not local_path.is_file():
        return False

    expected_size = entry.get("size")
    if not isinstance(expected_size, int):
        return False

    try:
        return local_path.stat().st_size == expected_size
    except OSError:
        return False


def local_file_exists_for_fast_skip(local_path: Path) -> bool:
    """
    Fastest possible resume check: trust any existing local file.

    This intentionally skips manifest, remote metadata, and hash validation.
    """
    return local_path.exists() and local_path.is_file()


def filter_remote_files(remote_files: List[RemoteFile], exclude_subpaths_re: Optional[re.Pattern[str]], logger: logging.Logger) -> List[RemoteFile]:
    """
    Filter remote files by relative path before planning or metadata prefetch.

    The regex is applied to the remote relative path, so users can exclude whole
    subtrees such as `.thumbnails` or `.Gallery2` anywhere below the chosen
    remote root.
    """
    if exclude_subpaths_re is None:
        return remote_files

    kept = [rf for rf in remote_files if not exclude_subpaths_re.search(rf.relpath)]
    excluded = len(remote_files) - len(kept)
    logger.info("Excluded %d remote files matching --exclude", excluded)
    return kept


def file_is_complete(
    local_path: Path,
    remote_meta: RemoteFile,
    *,
    verify_hash: bool,
    adb_path: str,
    remote_root: str,
    hash_cmd: Optional[str],
    logger: logging.Logger,
    check_all_files: bool,
) -> bool:
    """
    Decide whether an existing local file can be trusted and skipped.

    Base rule:
    - file exists
    - size matches remote size

    Optional stronger rule:
    - SHA-256 matches too
    """
    if not local_path.exists() or not local_path.is_file():
        return False

    try:
        st = local_path.stat()
    except OSError:
        return False

    if not check_all_files:
        return False

    ensure_remote_metadata(adb_path, remote_root, remote_meta)
    if remote_meta.size is None:
        raise RuntimeError(f"Remote size is unavailable for {remote_meta.relpath}")

    if st.st_size != remote_meta.size:
        return False

    if verify_hash:
        if not hash_cmd:
            raise RuntimeError("Hash verification requested but no remote sha256 command is available.")
        remote_path = remote_join(remote_root, remote_meta.relpath)
        local_hash = calc_local_sha256(local_path)
        remote_hash = calc_remote_sha256(adb_path, remote_path, hash_cmd)
        ok = local_hash.lower() == remote_hash.lower()
        if not ok:
            logger.warning("Hash mismatch for existing file, will re-copy: %s", remote_meta.relpath)
        return ok

    return True


def remote_join(root: str, relpath: str) -> str:
    root = root.rstrip("/")
    return f"{root}/{relpath}" if relpath else root


# ------------------------------- Transfer --------------------------------- #

def configure_logger(local_root: Path) -> logging.Logger:
    local_root.mkdir(parents=True, exist_ok=True)
    log_path = local_root / ".arft.log"

    logger = logging.getLogger("adb_pull_robust")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    fh = logging.FileHandler(log_path, encoding="utf-8")
    fh.setFormatter(fmt)
    fh.setLevel(logging.INFO)
    logger.addHandler(fh)

    sh = logging.StreamHandler(sys.stdout)
    sh.setFormatter(fmt)
    sh.setLevel(logging.INFO)
    logger.addHandler(sh)

    logger.info("Logging to %s", log_path)
    return logger


def pull_one_file(
    adb_path: str,
    remote_path: str,
    local_final: Path,
    remote_meta: RemoteFile,
    *,
    logger: logging.Logger,
    verify_hash: bool,
    hash_cmd: Optional[str],
    per_file_retries: int,
    retry_wait: float,
) -> Tuple[bool, str]:
    """
    Copy one file with validation and retries.

    Returns:
        (success, reason)
    """
    local_final.parent.mkdir(parents=True, exist_ok=True)
    local_part = local_final.with_name(local_final.name + ".part")

    # Clean up stale temp file from a previous crash so we do not validate the wrong content.
    if local_part.exists():
        try:
            local_part.unlink()
        except OSError:
            logger.warning("Could not remove stale temp file: %s", local_part)

    last_reason = "unknown"

    for attempt in range(1, per_file_retries + 1):
        attempt_prefix = f"[attempt {attempt}/{per_file_retries}]"
        try:
            # Pull directly to a temp file to avoid exposing truncated output as a completed file.
            cp = run(
                [adb_path, "pull", remote_path, str(local_part)],
                text=True,
                capture_output=True,
                check=False,
            )
            if cp.returncode != 0:
                last_reason = f"adb pull failed: {cp.stderr.strip() or cp.stdout.strip()}"
                logger.warning("%s %s -> %s | %s", attempt_prefix, remote_path, local_part, last_reason)
                safe_remove(local_part, logger)
                sleep_retry(retry_wait, attempt, per_file_retries)
                continue

            # Validation 1: local existence and exact size.
            if not local_part.exists():
                last_reason = "temp output file missing after adb pull"
                logger.warning("%s %s | %s", attempt_prefix, remote_path, last_reason)
                sleep_retry(retry_wait, attempt, per_file_retries)
                continue

            local_size = local_part.stat().st_size
            if remote_meta.size is None:
                raise RuntimeError(f"Remote size is unavailable for {remote_meta.relpath}")
            if local_size != remote_meta.size:
                last_reason = f"size mismatch: remote={remote_meta.size} local={local_size}"
                logger.warning("%s %s | %s", attempt_prefix, remote_path, last_reason)
                safe_remove(local_part, logger)
                sleep_retry(retry_wait, attempt, per_file_retries)
                continue

            # Validation 2: optional hash.
            if verify_hash:
                if not hash_cmd:
                    raise RuntimeError("Hash verification requested but no remote sha256 command is available.")
                local_hash = calc_local_sha256(local_part)
                remote_hash = calc_remote_sha256(adb_path, remote_path, hash_cmd)
                if local_hash.lower() != remote_hash.lower():
                    last_reason = f"hash mismatch: remote={remote_hash} local={local_hash}"
                    logger.warning("%s %s | %s", attempt_prefix, remote_path, last_reason)
                    safe_remove(local_part, logger)
                    sleep_retry(retry_wait, attempt, per_file_retries)
                    continue

            # Set timestamps on the temp file before rename so the final file lands already normalized.
            try:
                apply_timestamps(local_part, remote_meta)
            except Exception as exc:
                logger.warning("%s Could not apply timestamps to %s: %s", attempt_prefix, local_part, exc)

            # Replace final path atomically.
            local_part.replace(local_final)

            # Re-apply timestamps to the final file because some filesystems may normalize on rename.
            try:
                apply_timestamps(local_final, remote_meta)
            except Exception as exc:
                logger.warning("%s Could not apply timestamps to %s after rename: %s", attempt_prefix, local_final, exc)

            return True, "ok"

        except Exception as exc:
            last_reason = f"exception: {exc}"
            logger.exception("%s Unexpected failure while pulling %s", attempt_prefix, remote_path)
            safe_remove(local_part, logger)
            sleep_retry(retry_wait, attempt, per_file_retries)

    return False, last_reason


def safe_remove(path: Path, logger: logging.Logger) -> None:
    try:
        if path.exists():
            path.unlink()
    except OSError as exc:
        logger.warning("Could not remove %s: %s", path, exc)


def sleep_retry(retry_wait: float, attempt: int, total: int) -> None:
    if attempt < total and retry_wait > 0:
        time.sleep(retry_wait)


# ---------------------------------- Main ---------------------------------- #

def main(argv: Optional[List[str]] = None) -> int:
    global VERBOSE_ADB, ADB_LOGGER, ADB_EXECUTABLE

    parser = argparse.ArgumentParser(description="Robust file-by-file Android pull over ADB with tqdm, retries, and logging.")
    parser.add_argument("--adb-path", required=True, help="Path to adb.exe or adb.")
    parser.add_argument("--remote-root", required=True, help="Remote Android root, e.g. /storage/emulated/0/DCIM")
    parser.add_argument("--local-root", required=True, help="Destination directory on the PC")
    parser.add_argument("--verbose", action="store_true", help="Log every issued ADB command")
    parser.add_argument("--verify-hash", action="store_true", help="Verify SHA-256 after size verification")
    parser.add_argument("--force-all", action="store_true", help="Re-copy all files even if already present and complete")
    parser.add_argument("--refresh-file-list", action="store_true", help="Refresh the cached remote file list without forcing re-copy of already complete files")
    parser.add_argument("--check-all-files", action="store_true", help="Revalidate already downloaded files against the device before skipping them")
    parser.add_argument("--skip-all-checks", action="store_true", help="Skip all existing-file validation checks and trust any already present local file")
    parser.add_argument("--exclude", metavar="REGEXP", help="Regex applied to remote relative paths to exclude matching files and subpaths from planning and transfer")
    parser.add_argument("--dry-run", action="store_true", help="List planned actions without copying files")
    parser.add_argument("--retries", type=int, default=3, help="Retries per file on failure (default: 3)")
    parser.add_argument("--retry-wait", type=float, default=2.0, help="Seconds to wait between retries (default: 2.0)")
    parser.add_argument("--manifest-name", default=".arft-local-sync-state.json", help="Manifest filename in the destination root")
    args = parser.parse_args(argv)

    adb_path = args.adb_path
    remote_root = args.remote_root.rstrip("/")
    local_root = Path(args.local_root).expanduser().resolve()

    logger = configure_logger(local_root)
    VERBOSE_ADB = args.verbose
    ADB_LOGGER = logger
    ADB_EXECUTABLE = adb_path

    try:
        ensure_adb_device(adb_path)
    except Exception as exc:
        logger.error("ADB/device preflight failed: %s", exc)
        return 2

    supports_find_printf = device_supports_find_printf(adb_path)
    supports_multi_stat = device_supports_multi_stat(adb_path)
    logger.info(
        "Device capability probe: find -printf=%s, batched stat=%s",
        "yes" if supports_find_printf else "no",
        "yes" if supports_multi_stat else "no",
    )

    hash_cmd: Optional[str] = None
    exclude_subpaths_re: Optional[re.Pattern[str]] = None
    if args.exclude:
        try:
            exclude_subpaths_re = re.compile(args.exclude)
        except re.error as exc:
            logger.error("Invalid --exclude regex: %s", exc)
            return 2
        logger.info("Excluding remote relative paths matching regex: %s", args.exclude)

    if args.verify_hash:
        hash_cmd = choose_hash_cmd(adb_path)
        if not hash_cmd:
            logger.error("Hash verification requested, but no sha256 command is available on the phone.")
            return 2
        logger.info("Remote hash command detected: %s", hash_cmd)

    manifest_path = local_root / args.manifest_name
    manifest_exists = manifest_path.exists()
    manifest = load_manifest(manifest_path)
    file_list_cache_path = local_root / ".arft-remote-files-list.json"
    file_list_cache_exists = file_list_cache_path.exists()
    refresh_file_list = args.force_all or args.refresh_file_list
    recovery_check_all_files = (
        not args.skip_all_checks
        and not manifest_exists
        and local_root_has_payload_files(local_root, manifest_name=args.manifest_name)
    )
    effective_check_all_files = args.check_all_files or recovery_check_all_files

    if args.skip_all_checks:
        logger.info("Fast resume mode enabled: skipping all existing-file validation checks")

    if recovery_check_all_files:
        logger.info(
            "No manifest was found, but %s already contains files; rebuilding resume state with strict validation",
            local_root,
        )

    remote_files: Optional[List[RemoteFile]] = None
    if not refresh_file_list:
        remote_files = load_file_list_cache(file_list_cache_path, remote_root=remote_root)
        if remote_files is not None:
            logger.info("Loaded cached remote file list from %s: %d files", file_list_cache_path, len(remote_files))
        else:
            logger.info("No reusable cached remote file list found at %s", file_list_cache_path)
    else:
        reason = "--force-all" if args.force_all else "--refresh-file-list"
        logger.info("Refreshing remote file list because %s was supplied", reason)

    if remote_files is None:
        try:
            remote_files = list_remote_files(
                adb_path,
                remote_root,
                logger,
                prefer_find_printf=supports_find_printf,
            )
        except Exception as exc:
            logger.exception("Could not list remote files under %s", remote_root)
            return 2
        save_file_list_cache(file_list_cache_path, remote_root=remote_root, remote_files=remote_files)
        logger.info("Saved remote file list cache to %s", file_list_cache_path)

    remote_files = filter_remote_files(remote_files, exclude_subpaths_re, logger)

    if not args.force_all and not args.skip_all_checks and effective_check_all_files and supports_multi_stat:
        existing_local_files = [rf for rf in remote_files if (local_root / Path(rf.relpath)).is_file()]
        if existing_local_files:
            logger.info(
                "Prefetching remote metadata for %d existing local files because strict validation is active",
                len(existing_local_files),
            )
            try:
                populate_remote_metadata_batch(adb_path, remote_root, existing_local_files, logger)
            except Exception as exc:
                logger.warning("Batched remote metadata prefetch failed, will fall back to single-file metadata queries: %s", exc)

    # Work out which files still need action.
    todo: List[RemoteFile] = []
    skipped = 0
    checking_pbar = tqdm(
        total=max(len(remote_files), 1),
        unit="file",
        desc=f"{infer_remote_root_name(remote_root)} check",
        dynamic_ncols=True,
        smoothing=0.1,
    )
    try:
        for rf in remote_files:
            local_path = local_root / Path(rf.relpath)
            complete = False
            if not args.force_all:
                if args.skip_all_checks and local_file_exists_for_fast_skip(local_path):
                    complete = True
                elif manifest_says_file_is_complete(local_path, rf.relpath, manifest) and not effective_check_all_files:
                    complete = True
                    entry = manifest.get(rf.relpath, {})
                    if isinstance(entry.get("size"), int):
                        rf.size = entry["size"]
                    if isinstance(entry.get("mtime"), int):
                        rf.mtime = entry["mtime"]
                    if isinstance(entry.get("birth"), int):
                        rf.birth = entry["birth"]
                else:
                    try:
                        complete = file_is_complete(
                            local_path,
                            rf,
                            verify_hash=args.verify_hash,
                            adb_path=adb_path,
                            remote_root=remote_root,
                            hash_cmd=hash_cmd,
                            logger=logger,
                            check_all_files=effective_check_all_files,
                        )
                    except Exception as exc:
                        logger.warning("Could not validate existing file %s, will re-copy: %s", local_path, exc)
                        complete = False

            if complete:
                skipped += 1
                manifest[rf.relpath] = {
                    "status": "ok",
                    "size": rf.size,
                    "mtime": rf.mtime,
                    "birth": rf.birth,
                    "local_path": str(local_path),
                    "updated_at": int(time.time()),
                }
            else:
                todo.append(rf)

            # Planning progress is intentionally separate from transfer progress
            # so dry-run and resume decisions remain visible before any copy work.
            checking_pbar.update(1)
    finally:
        checking_pbar.close()

    logger.info(
        "Planned transfer: total=%d files, skipped=%d, remaining=%d",
        len(remote_files),
        skipped,
        len(todo),
    )

    save_manifest(manifest_path, manifest)

    if args.dry_run:
        for rf in todo:
            print(rf.relpath)
        return 0

    # Overall progress counts files so startup can avoid a full remote size scan.
    pbar = tqdm(
        total=max(len(remote_files), 1),
        initial=skipped,
        unit="file",
        desc=infer_remote_root_name(remote_root),
        dynamic_ncols=True,
        smoothing=0.1,
    )

    transferred_files = 0
    failed_files: List[Tuple[str, str]] = []

    try:
        for rf in todo:
            remote_path = remote_join(remote_root, rf.relpath)
            local_path = local_root / Path(rf.relpath)

            # Visible per-file context in the progress bar.
            pbar.set_postfix_str(rf.relpath[-60:] if len(rf.relpath) > 60 else rf.relpath)

            # Metadata is only fetched for files that actually need a transfer.
            # If that lookup fails we treat it as a per-file failure so one stale
            # or disappearing remote path does not abort the whole run.
            try:
                ensure_remote_metadata(adb_path, remote_root, rf)
            except Exception as exc:
                reason = f"remote metadata lookup failed: {exc}"
                failed_files.append((rf.relpath, reason))
                logger.error("FAIL %s | %s", rf.relpath, reason)
                manifest[rf.relpath] = {
                    "status": "failed",
                    "reason": reason,
                    "size": rf.size,
                    "mtime": rf.mtime,
                    "birth": rf.birth,
                    "local_path": str(local_path),
                    "updated_at": int(time.time()),
                }
                pbar.update(1)
                save_manifest(manifest_path, manifest)
                continue

            start = time.time()
            ok, reason = pull_one_file(
                adb_path,
                remote_path,
                local_path,
                rf,
                logger=logger,
                verify_hash=args.verify_hash,
                hash_cmd=hash_cmd,
                per_file_retries=max(1, args.retries),
                retry_wait=max(0.0, args.retry_wait),
            )
            elapsed = max(time.time() - start, 1e-6)

            if ok:
                transferred_files += 1
                if rf.size is None:
                    raise RuntimeError(f"Remote size is unavailable for {rf.relpath}")
                rate = rf.size / elapsed
                logger.info(
                    "OK %s | %.2f MiB in %.1fs (%.2f MiB/s)",
                    rf.relpath,
                    rf.size / (1024**2),
                    elapsed,
                    rate / (1024**2),
                )
                manifest[rf.relpath] = {
                    "status": "ok",
                    "size": rf.size,
                    "mtime": rf.mtime,
                    "birth": rf.birth,
                    "local_path": str(local_path),
                    "updated_at": int(time.time()),
                }
            else:
                failed_files.append((rf.relpath, reason))
                logger.error("FAIL %s | %s", rf.relpath, reason)
                manifest[rf.relpath] = {
                    "status": "failed",
                    "reason": reason,
                    "size": rf.size,
                    "mtime": rf.mtime,
                    "birth": rf.birth,
                    "local_path": str(local_path),
                    "updated_at": int(time.time()),
                }

            pbar.update(1)
            save_manifest(manifest_path, manifest)

    finally:
        pbar.close()

    # Summary.
    ok_count = skipped + transferred_files
    logger.info(
        "Summary: ok=%d/%d, failed=%d",
        ok_count,
        len(remote_files),
        len(failed_files),
    )

    if failed_files:
        failed_path = local_root / ".arft-failed-files.tsv"
        with failed_path.open("w", encoding="utf-8", newline="\n") as f:
            f.write("relpath\treason\n")
            for relpath, reason in failed_files:
                # Keep TSV readable even if stderr text contains tabs/newlines.
                clean_reason = reason.replace("\t", " ").replace("\r", " ").replace("\n", " ")
                f.write(f"{relpath}\t{clean_reason}\n")
        logger.error("Failure list written to %s", failed_path)
        return 1

    return 0


if __name__ == "__main__":
    sys.exit(main())
