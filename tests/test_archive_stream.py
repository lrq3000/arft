from __future__ import annotations

"""Tests for archive-stream and ADB control flow.

Each test uses small, deterministic fakes so the branch coverage comes from the
same code paths a real transfer would exercise, without needing a device.
"""

import io
import argparse
import logging
import re
import subprocess
import tarfile
from pathlib import Path

import pytest

import arft.cli as cli
from arft.cli import (
    RemoteFile,
    archive_chunk_command,
    build_parser,
    load_json_object,
    parse_size_spec,
    plan_archive_chunks,
    safe_tar_members,
    save_json_object,
    validate_archive_member,
    validate_args,
)


def parse_args(*args: str):
    """Parse a baseline ARFT CLI invocation and apply the same validation as production."""
    parser = build_parser()
    parsed = parser.parse_args([
        "--adb-path",
        "adb",
        "--remote-root",
        "/storage/emulated/0/DCIM",
        "--local-root",
        "backup",
        *args,
    ])
    validate_args(parser, parsed)
    return parsed


def completed_process(cmd, returncode: int = 0, stdout: str = "", stderr: str = ""):
    """Build a tiny subprocess result object for adb and shell fakes."""
    return subprocess.CompletedProcess(args=cmd, returncode=returncode, stdout=stdout, stderr=stderr)


def make_tar_gz_bytes(member_name: str, payload: bytes) -> bytes:
    """Create a minimal gzipped tar archive that archive-stream tests can replay."""
    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode="w:gz") as tar:
        info = tarfile.TarInfo(member_name)
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    return archive_bytes.getvalue()


def test_default_transfer_mode_is_files() -> None:
    args = parse_args()
    assert args.transfer_mode == "files"


def test_archive_stream_parser_accepts_scoped_options() -> None:
    args = parse_args(
        "--transfer-mode",
        "archive-stream",
        "--archive-chunk-size",
        "128M",
        "--archive-compression-level",
        "1",
        "--archive-max-files-per-chunk",
        "3",
    )
    assert args.transfer_mode == "archive-stream"
    assert args.archive_chunk_size == 128 * 1024 * 1024
    assert args.archive_compression_level == 1
    assert args.archive_max_files_per_chunk == 3


def test_archive_stream_rejects_verify_hash() -> None:
    with pytest.raises(SystemExit):
        parse_args("--transfer-mode", "archive-stream", "--verify-hash")


def test_validate_args_rejects_invalid_archive_chunk_size() -> None:
    parser = build_parser()
    with pytest.raises(SystemExit):
        parser.parse_args([
            "--adb-path",
            "adb",
            "--remote-root",
            "/storage/emulated/0/DCIM",
            "--local-root",
            "backup",
            "--archive-chunk-size",
            "0",
        ])


def test_validate_args_rejects_non_positive_max_files_per_chunk() -> None:
    parser = build_parser()
    parsed = parser.parse_args([
        "--adb-path",
        "adb",
        "--remote-root",
        "/storage/emulated/0/DCIM",
        "--local-root",
        "backup",
        "--archive-max-files-per-chunk",
        "0",
    ])
    with pytest.raises(SystemExit):
        validate_args(parser, parsed)


def test_size_parser_uses_binary_units() -> None:
    assert parse_size_spec("1K") == 1024
    assert parse_size_spec("2M") == 2 * 1024**2
    assert parse_size_spec("3G") == 3 * 1024**3


def test_ensure_adb_device_reports_bad_states(monkeypatch) -> None:
    # Feed three distinct adb outputs so one test covers the three failure exits.
    outputs = [
        "garbage\n",
        "List of devices attached\n\n",
        "List of devices attached\noffline123\toffline\n",
    ]

    def fake_run(cmd, *, text=True, capture_output=True, check=True, timeout=None):
        return completed_process(cmd, stdout=outputs.pop(0))

    monkeypatch.setattr(cli, "run", fake_run)

    with pytest.raises(RuntimeError, match="Unexpected adb devices output"):
        cli.ensure_adb_device("adb")
    with pytest.raises(RuntimeError, match="No Android device detected"):
        cli.ensure_adb_device("adb")
    with pytest.raises(RuntimeError, match="ADB device not ready"):
        cli.ensure_adb_device("adb")


def test_ensure_adb_device_accepts_ready_device(monkeypatch) -> None:
    def fake_run(cmd, *, text=True, capture_output=True, check=True, timeout=None):
        return completed_process(cmd, stdout="List of devices attached\n123\tdevice\n")

    monkeypatch.setattr(cli, "run", fake_run)

    cli.ensure_adb_device("adb")


def test_choose_hash_cmd_prefers_first_working_probe(monkeypatch) -> None:
    def fake_adb_shell(_adb_path: str, shell_cmd: str, *, check: bool = True):
        if "sha256sum >/dev/null" in shell_cmd:
            return completed_process(["adb"], stdout="sha256sum\n")
        return completed_process(["adb"], returncode=1)

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)
    assert cli.choose_hash_cmd("adb") == "sha256sum"


def test_choose_hash_cmd_falls_back_to_toybox(monkeypatch) -> None:
    def fake_adb_shell(_adb_path: str, shell_cmd: str, *, check: bool = True):
        if "command -v sha256sum" in shell_cmd:
            return completed_process(["adb"], returncode=1)
        return completed_process(["adb"], stdout="toybox sha256sum\n")

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)
    assert cli.choose_hash_cmd("adb") == "toybox sha256sum"


def test_choose_hash_cmd_returns_none_when_absent(monkeypatch) -> None:
    monkeypatch.setattr(cli, "adb_shell", lambda *_args, **_kwargs: completed_process(["adb"], returncode=1))
    assert cli.choose_hash_cmd("adb") is None


def test_device_capability_probes_handle_both_results(monkeypatch) -> None:
    # Reuse one fake shell shim to drive both capability probes through success and failure.
    responses = [
        completed_process(["adb"], returncode=0),
        completed_process(["adb"], returncode=1),
        completed_process(["adb"], returncode=0),
        completed_process(["adb"], returncode=1),
    ]

    def fake_adb_shell(_adb_path: str, _shell_cmd: str, *, check: bool = True):
        return responses.pop(0)

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)
    assert cli.device_supports_find_printf("adb") is True
    assert cli.device_supports_find_printf("adb") is False
    assert cli.device_supports_multi_stat("adb") is True
    assert cli.device_supports_multi_stat("adb") is False


def test_calc_remote_sha256_reads_last_output_line(monkeypatch) -> None:
    monkeypatch.setattr(cli, "adb_shell", lambda *_args, **_kwargs: completed_process(["adb"], stdout="ignored\nabc123\n"))
    assert cli.calc_remote_sha256("adb", "/sdcard/file.txt", "sha256sum") == "abc123"


def test_calc_remote_sha256_rejects_empty_output(monkeypatch) -> None:
    monkeypatch.setattr(cli, "adb_shell", lambda *_args, **_kwargs: completed_process(["adb"], stdout="\n"))
    with pytest.raises(RuntimeError, match="Could not read remote hash"):
        cli.calc_remote_sha256("adb", "/sdcard/file.txt", "sha256sum")


def test_manifest_and_cache_helpers_cover_common_branches(tmp_path) -> None:
    # Load/save helpers only need a real filesystem for atomic-write and JSON-error branches.
    manifest_path = tmp_path / "manifest.json"
    assert cli.load_manifest(manifest_path) == {}
    manifest_path.write_text("not json", encoding="utf-8")
    assert cli.load_manifest(manifest_path) == {}
    cli.save_manifest(manifest_path, {"a": {"status": "ok"}})
    assert cli.load_manifest(manifest_path)["a"]["status"] == "ok"

    cache_path = tmp_path / "cache.json"
    assert cli.load_file_list_cache(cache_path, remote_root="/root") is None
    cache_path.write_text("not json", encoding="utf-8")
    assert cli.load_file_list_cache(cache_path, remote_root="/root") is None
    cache_path.write_text('{"remote_root":"/other","files":["a"]}', encoding="utf-8")
    assert cli.load_file_list_cache(cache_path, remote_root="/root") is None
    cache_path.write_text('{"remote_root":"/root","files":"bad"}', encoding="utf-8")
    assert cli.load_file_list_cache(cache_path, remote_root="/root") is None
    cache_path.write_text('{"remote_root":"/root","files":["a","","b"]}', encoding="utf-8")
    cached = cli.load_file_list_cache(cache_path, remote_root="/root")
    assert [rf.relpath for rf in cached or []] == ["a", "b"]


def test_local_state_helpers_cover_true_false_paths(tmp_path) -> None:
    local_root = tmp_path / "backup"
    local_root.mkdir()
    (local_root / ".arft.log").write_text("log", encoding="utf-8")
    assert cli.local_root_has_payload_files(local_root, manifest_name=".arft-local-sync-state.json") is False
    (local_root / "payload.bin").write_bytes(b"x")
    assert cli.local_root_has_payload_files(local_root, manifest_name=".arft-local-sync-state.json") is True

    manifest: dict[str, dict[str, object]] = {"a": {"status": "ok", "size": 1}}
    local_file = local_root / "a"
    assert cli.manifest_says_file_is_complete(local_file, "a", manifest) is False
    local_file.write_bytes(b"x")
    assert cli.manifest_says_file_is_complete(local_file, "a", manifest) is True
    assert cli.local_file_exists_for_fast_skip(local_file) is True


def test_filter_remote_files_and_manifest_flags(monkeypatch, tmp_path) -> None:
    logger = logging.getLogger("test")
    files = [RemoteFile("Camera/a.jpg"), RemoteFile("Thumbs/.thumbnails/b.jpg")]
    assert cli.filter_remote_files(files, None, logger) == files
    filtered = cli.filter_remote_files(files, re.compile(r"\.thumbnails"), logger)
    assert [rf.relpath for rf in filtered] == ["Camera/a.jpg"]

    local_file = tmp_path / "Camera" / "a.jpg"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"1234")
    manifest: dict[str, dict[str, object]] = {"Camera/a.jpg": {"status": "ok", "size": 4}}
    assert cli.manifest_says_file_is_complete(local_file, "Camera/a.jpg", manifest) is True


def test_file_is_complete_covers_validation_paths(tmp_path, monkeypatch) -> None:
    # Size-only validation, size mismatch, and hash mismatch all flow through the same helper.
    local_file = tmp_path / "Camera" / "a.jpg"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"abcd")
    remote_meta = RemoteFile("Camera/a.jpg", size=4, mtime=1, birth=1)

    assert cli.file_is_complete(
        local_file,
        remote_meta,
        verify_hash=False,
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        hash_cmd=None,
        logger=logging.getLogger("test"),
        check_all_files=False,
    ) is False

    assert cli.file_is_complete(
        local_file,
        remote_meta,
        verify_hash=False,
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        hash_cmd=None,
        logger=logging.getLogger("test"),
        check_all_files=True,
    ) is True

    remote_meta.size = 5
    assert cli.file_is_complete(
        local_file,
        remote_meta,
        verify_hash=False,
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        hash_cmd=None,
        logger=logging.getLogger("test"),
        check_all_files=True,
    ) is False

    remote_meta.size = 4
    monkeypatch.setattr(cli, "ensure_remote_metadata", lambda *_args, **_kwargs: remote_meta)
    monkeypatch.setattr(cli, "calc_local_sha256", lambda *_args, **_kwargs: "local")
    monkeypatch.setattr(cli, "calc_remote_sha256", lambda *_args, **_kwargs: "remote")
    assert cli.file_is_complete(
        local_file,
        remote_meta,
        verify_hash=True,
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        hash_cmd="sha256sum",
        logger=logging.getLogger("test"),
        check_all_files=True,
    ) is False


def test_chunk_state_entry_recovers_from_bad_state() -> None:
    state: dict[str, object] = {"chunks": []}
    entry = cli.chunk_state_entry(state, "chunk-000001")
    entry["status"] = "ok"
    chunks = state["chunks"]
    assert isinstance(chunks, dict)
    chunk = chunks["chunk-000001"]
    assert isinstance(chunk, dict)
    assert chunk["status"] == "ok"


def test_validate_archive_member_rejects_unexpected_member(tmp_path) -> None:
    member = tarfile.TarInfo("safe.txt")
    member.size = 4
    with pytest.raises(RuntimeError, match="Unexpected archive member"):
        cli.validate_archive_member(member, {"other.txt"}, tmp_path)


def test_safe_tar_members_accepts_safe_archive(tmp_path) -> None:
    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode="w") as tar:
        info = tarfile.TarInfo("safe.txt")
        payload = b"ok"
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    archive_bytes.seek(0)
    with tarfile.open(fileobj=archive_bytes, mode="r") as tar:
        assert [member.name for member in cli.safe_tar_members(tar, tmp_path)] == ["safe.txt"]


def test_probe_archive_tools_reports_success(monkeypatch) -> None:
    def fake_adb_shell(_adb_path: str, shell_cmd: str, *, check: bool = True):
        return completed_process(["adb"], stdout="ok")

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)
    assert cli.probe_archive_tools("adb").supported is True


def test_list_remote_files_uses_both_listing_strategies(monkeypatch) -> None:
    seen_scripts: list[str] = []

    def fake_adb_shell(_adb_path: str, shell_cmd: str, *, check: bool = True):
        seen_scripts.append(shell_cmd)
        return completed_process(["adb"], stdout="one.jpg\n\nCamera/two.jpg\n")

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)

    files = cli.list_remote_files("adb", "/storage/emulated/0/DCIM", logging.getLogger("test"), prefer_find_printf=True)
    assert [rf.relpath for rf in files] == ["one.jpg", "Camera/two.jpg"]
    files = cli.list_remote_files("adb", "/storage/emulated/0/DCIM", logging.getLogger("test"), prefer_find_printf=False)
    assert [rf.relpath for rf in files] == ["one.jpg", "Camera/two.jpg"]
    assert "-printf '%P\\n'" in seen_scripts[0]
    assert "printf '%s\\n'" in seen_scripts[1]


def test_parse_plain_stat_output_and_timestamp_parser_cover_all_formats() -> None:
    assert cli._parse_plain_stat_timestamp("") is None
    assert cli._parse_plain_stat_timestamp("not a timestamp") is None

    output = """
File: /storage/emulated/0/DCIM/Camera/one.jpg
Size: 12
Modify: 2026-04-11 18:19:12.123456789 +0200

File: /storage/emulated/0/DCIM/Camera/two.jpg
Size: 34
Modify: 2026-04-11 18:19:12 +0200
"""
    parsed = cli.parse_plain_stat_output(output, "/storage/emulated/0/DCIM")
    assert parsed["Camera/one.jpg"][0] == 12
    assert parsed["Camera/two.jpg"][0] == 34


def test_run_logs_copy_pasteable_adb_command(monkeypatch) -> None:
    logger = logging.getLogger("adb-log-test")
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    logger.handlers.clear()
    logger.addHandler(handler)
    logger.setLevel(logging.INFO)

    monkeypatch.setattr(cli, "verbose_adb", True)
    monkeypatch.setattr(cli, "adb_logger", logger)
    monkeypatch.setattr(cli, "adb_executable", "adb")
    monkeypatch.setattr(cli.subprocess, "run", lambda *args, **kwargs: completed_process(args[0], stdout="ok"))

    assert cli.run(["adb", "devices"]).stdout == "ok"
    assert "ADB CMD: adb devices" in stream.getvalue()


def test_main_files_mode_skips_existing_and_transfers_remaining(tmp_path, monkeypatch) -> None:
    # Drive main() end-to-end with all device calls stubbed so only transfer planning runs.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    (local_root / "keep.txt").write_bytes(b"x")

    remote_files = [RemoteFile("keep.txt", size=1, mtime=1, birth=1), RemoteFile("copy.txt", size=2, mtime=1, birth=1)]
    seen: list[str] = []

    def fake_pull(adb_path, remote_path, local_path, remote_meta, *, logger, verify_hash, hash_cmd, per_file_retries, retry_wait):
        local_path.parent.mkdir(parents=True, exist_ok=True)
        local_path.write_bytes(b"xy")
        seen.append(remote_path)
        return True, "ok"

    def fake_file_is_complete(local_path, remote_meta, *, verify_hash, adb_path, remote_root, hash_cmd, logger, check_all_files):
        return remote_meta.relpath == "keep.txt"

    monkeypatch.setattr(cli, "ensure_adb_device", lambda _adb_path: None)
    monkeypatch.setattr(cli, "device_supports_find_printf", lambda _adb_path: True)
    monkeypatch.setattr(cli, "device_supports_multi_stat", lambda _adb_path: True)
    monkeypatch.setattr(cli, "load_file_list_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli, "list_remote_files", lambda *_args, **_kwargs: remote_files)
    monkeypatch.setattr(cli, "save_file_list_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli, "populate_remote_metadata_batch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli, "file_is_complete", fake_file_is_complete)
    monkeypatch.setattr(cli, "ensure_remote_metadata", lambda _adb_path, _remote_root, rf: rf)
    monkeypatch.setattr(cli, "pull_one_file", fake_pull)

    exit_code = cli.main([
        "--adb-path",
        "adb",
        "--remote-root",
        "/storage/emulated/0/DCIM",
        "--local-root",
        str(local_root),
    ])

    assert exit_code == 0
    assert seen == ["/storage/emulated/0/DCIM/copy.txt"]
    assert (local_root / ".arft-local-sync-state.json").exists()


def test_main_archive_stream_dispatches_to_archive_runner(tmp_path, monkeypatch) -> None:
    # Verify main() routes archive-stream mode into the dedicated runner with the same inputs.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    remote_files = [RemoteFile("a.txt", size=1, mtime=1, birth=1)]
    called: dict[str, object] = {}

    monkeypatch.setattr(cli, "ensure_adb_device", lambda _adb_path: None)
    monkeypatch.setattr(cli, "device_supports_find_printf", lambda _adb_path: True)
    monkeypatch.setattr(cli, "device_supports_multi_stat", lambda _adb_path: False)
    monkeypatch.setattr(cli, "load_file_list_cache", lambda *_args, **_kwargs: remote_files)
    monkeypatch.setattr(cli, "run_archive_stream_mode", lambda **kwargs: called.update(kwargs) or 0)

    exit_code = cli.main([
        "--adb-path",
        "adb",
        "--remote-root",
        "/storage/emulated/0/DCIM",
        "--local-root",
        str(local_root),
        "--transfer-mode",
        "archive-stream",
    ])

    assert exit_code == 0
    assert called["remote_files"] == remote_files
    assert called["supports_multi_stat"] is False


@pytest.mark.parametrize(
    "mode, expected_reason",
    [
        ("adb_failure", "adb pull failed:"),
        ("missing_temp", "temp output file missing after adb pull"),
        ("exception", "exception: boom"),
    ],
)
def test_pull_one_file_covers_failure_branches(tmp_path, monkeypatch, mode: str, expected_reason: str) -> None:
    # One parametrized fake covers the three non-happy-path adb.pull outcomes.
    local_final = tmp_path / "Camera" / "video.mp4"
    remote_meta = RemoteFile("Camera/video.mp4", size=8, mtime=1, birth=1)

    def fake_run(cmd, *, text=True, capture_output=True, check=True, timeout=None):
        local_part = Path(cmd[3])
        if mode == "exception":
            raise RuntimeError("boom")
        if mode == "missing_temp":
            return completed_process(cmd, returncode=0, stdout="", stderr="")
        return completed_process(cmd, returncode=1, stdout="", stderr="offline")

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "apply_timestamps", lambda *_args, **_kwargs: None)

    ok, reason = cli.pull_one_file(
        "adb",
        "/storage/emulated/0/DCIM/Camera/video.mp4",
        local_final,
        remote_meta,
        logger=logging.getLogger("test"),
        verify_hash=False,
        hash_cmd=None,
        per_file_retries=1,
        retry_wait=0,
    )

    assert ok is False
    assert reason.startswith(expected_reason)


def test_file_is_complete_requires_hash_command_when_verify_hash_enabled(tmp_path, monkeypatch) -> None:
    local_file = tmp_path / "Camera" / "video.mp4"
    local_file.parent.mkdir(parents=True, exist_ok=True)
    local_file.write_bytes(b"12345678")
    remote_meta = RemoteFile("Camera/video.mp4", size=8, mtime=1, birth=1)
    monkeypatch.setattr(cli, "ensure_remote_metadata", lambda *_args, **_kwargs: remote_meta)

    with pytest.raises(RuntimeError, match="Hash verification requested"):
        cli.file_is_complete(
            local_file,
            remote_meta,
            verify_hash=True,
            adb_path="adb",
            remote_root="/storage/emulated/0/DCIM",
            hash_cmd=None,
            logger=logging.getLogger("test"),
            check_all_files=True,
        )


def test_run_archive_stream_mode_rejects_missing_tools(tmp_path, monkeypatch) -> None:
    # If the probe fails, archive-stream should stop before any chunk planning.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    remote_files = [RemoteFile("a.txt", size=1, mtime=1, birth=1)]
    args = argparse.Namespace(
        archive_chunk_size=1024**3,
        archive_max_files_per_chunk=10,
        archive_compression_level=1,
        force_all=False,
        skip_all_checks=False,
        dry_run=False,
        retries=1,
        retry_wait=0,
    )
    monkeypatch.setattr(cli, "probe_archive_tools", lambda _adb_path: cli.ArchiveToolProbe(False, ["missing_tar"]))

    exit_code = cli.run_archive_stream_mode(
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        local_root=local_root,
        remote_files=remote_files,
        manifest={},
        manifest_path=tmp_path / ".arft-local-sync-state.json",
        args=args,
        logger=logging.getLogger("test"),
        supports_multi_stat=False,
        check_all_files=False,
    )

    assert exit_code == 2


def test_run_archive_stream_mode_dry_run_prints_chunks(tmp_path, monkeypatch, capsys) -> None:
    # Dry-run still plans chunks, but only prints them and exits before streaming.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    remote_files = [RemoteFile("a.txt", size=700 * 1024**2, mtime=1, birth=1), RemoteFile("b.txt", size=400 * 1024**2, mtime=1, birth=1)]
    args = argparse.Namespace(
        archive_chunk_size=1024**3,
        archive_max_files_per_chunk=10,
        archive_compression_level=1,
        force_all=False,
        skip_all_checks=False,
        dry_run=True,
        retries=1,
        retry_wait=0,
    )

    monkeypatch.setattr(cli, "probe_archive_tools", lambda _adb_path: cli.ArchiveToolProbe(True, []))
    monkeypatch.setattr(cli, "populate_remote_metadata_batch", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli, "ensure_remote_metadata", lambda _adb_path, _remote_root, rf: rf)

    exit_code = cli.run_archive_stream_mode(
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        local_root=local_root,
        remote_files=remote_files,
        manifest={},
        manifest_path=tmp_path / ".arft-local-sync-state.json",
        args=args,
        logger=logging.getLogger("test"),
        supports_multi_stat=False,
        check_all_files=False,
    )

    assert exit_code == 0
    assert "chunk-000001" in capsys.readouterr().out


def test_stream_extract_archive_chunk_covers_stdout_and_exitcode_branches(tmp_path, monkeypatch) -> None:
    local_root = tmp_path / "backup"
    local_root.mkdir()
    manifest: dict[str, dict[str, object]] = {}
    restore_journal: dict[str, object] = {}
    chunk = cli.ArchiveChunk(index=1, files=[RemoteFile("Camera/ok.jpg", size=4, mtime=1, birth=1)], estimated_size=4)

    class NoStdoutProcess:
        def __init__(self) -> None:
            self.stdout = None

        def kill(self) -> None:
            pass

        def wait(self) -> int:
            return 0

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: NoStdoutProcess())
    monkeypatch.setattr(cli, "apply_timestamps", lambda *_args, **_kwargs: None)
    ok, reason, _, _ = cli.stream_extract_archive_chunk(
        "adb",
        "/storage/emulated/0/DCIM",
        local_root,
        chunk,
        compression_level=1,
        manifest=manifest,
        manifest_path=tmp_path / ".arft-local-sync-state.json",
        restore_journal=restore_journal,
        restore_journal_path=tmp_path / ".arft-archive-restore-journal.json",
        logger=logging.getLogger("test"),
    )
    assert ok is False
    assert reason == "adb exec-out did not expose stdout"

    class NonZeroExitProcess:
        def __init__(self) -> None:
            self.stdout = io.BytesIO(make_tar_gz_bytes("./Camera/ok.jpg", b"data"))
            self.killed = False

        def kill(self) -> None:
            self.killed = True

        def wait(self) -> int:
            return 1

    monkeypatch.setattr(cli.subprocess, "Popen", lambda *args, **kwargs: NonZeroExitProcess())
    ok, reason, _, _ = cli.stream_extract_archive_chunk(
        "adb",
        "/storage/emulated/0/DCIM",
        local_root,
        chunk,
        compression_level=1,
        manifest=manifest,
        manifest_path=tmp_path / ".arft-local-sync-state.json",
        restore_journal=restore_journal,
        restore_journal_path=tmp_path / ".arft-archive-restore-journal.json",
        logger=logging.getLogger("test"),
    )
    assert ok is False
    assert reason.startswith("adb exec-out tar.gz failed:")


def test_main_returns_2_when_remote_listing_fails(tmp_path, monkeypatch) -> None:
    # A remote listing exception should be converted into the CLI's generic error code.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    monkeypatch.setattr(cli, "ensure_adb_device", lambda _adb_path: None)
    monkeypatch.setattr(cli, "device_supports_find_printf", lambda _adb_path: True)
    monkeypatch.setattr(cli, "device_supports_multi_stat", lambda _adb_path: True)
    monkeypatch.setattr(cli, "load_file_list_cache", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(cli, "list_remote_files", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("list failed")))

    assert cli.main([
        "--adb-path",
        "adb",
        "--remote-root",
        "/storage/emulated/0/DCIM",
        "--local-root",
        str(local_root),
    ]) == 2


def test_plan_archive_chunks_by_size_and_file_cap() -> None:
    files = [
        RemoteFile("a.jpg", size=60),
        RemoteFile("b.jpg", size=60),
        RemoteFile("c.jpg", size=60),
        RemoteFile("d.jpg", size=60),
    ]
    chunks = plan_archive_chunks(
        files,
        target_chunk_size=100,
        max_files_per_chunk=10,
        remote_root="/storage/emulated/0/DCIM",
        compression_level=1,
    )
    assert [[file.relpath for file in chunk.files] for chunk in chunks] == [["a.jpg"], ["b.jpg"], ["c.jpg"], ["d.jpg"]]

    chunks = plan_archive_chunks(
        files,
        target_chunk_size=1000,
        max_files_per_chunk=2,
        remote_root="/storage/emulated/0/DCIM",
        compression_level=1,
    )
    assert [[file.relpath for file in chunk.files] for chunk in chunks] == [["a.jpg", "b.jpg"], ["c.jpg", "d.jpg"]]


def test_plan_archive_chunks_splits_when_payload_exceeds_one_gibibyte() -> None:
    files = [
        RemoteFile("huge-a.bin", size=700 * 1024**2),
        RemoteFile("huge-b.bin", size=400 * 1024**2),
    ]

    chunks = plan_archive_chunks(
        files,
        target_chunk_size=1024**3,
        max_files_per_chunk=10,
        remote_root="/storage/emulated/0/DCIM",
        compression_level=1,
    )

    assert [[file.relpath for file in chunk.files] for chunk in chunks] == [["huge-a.bin"], ["huge-b.bin"]]


def test_archive_command_uses_tar_gzip_only() -> None:
    command = archive_chunk_command(
        "/storage/emulated/0/DCIM",
        [RemoteFile("Camera/one file.jpg", size=1)],
        compression_level=1,
    )
    assert "tar -cf -" in command
    assert "gzip -1" in command
    assert "zstd" not in command


def test_json_state_round_trips_atomically(tmp_path) -> None:
    path = tmp_path / ".arft-archive-state.json"
    save_json_object(path, {"schema_version": 1, "chunks": {"chunk-000001": {"status": "transferred"}}})
    payload = load_json_object(path)
    chunks = payload["chunks"]
    assert isinstance(chunks, dict)
    chunk = chunks["chunk-000001"]
    assert isinstance(chunk, dict)
    assert chunk["status"] == "transferred"


def test_safe_tar_members_rejects_path_traversal(tmp_path) -> None:
    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode="w") as tar:
        info = tarfile.TarInfo("../escape.txt")
        payload = b"bad"
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    archive_bytes.seek(0)

    with tarfile.open(fileobj=archive_bytes, mode="r") as tar:
        with pytest.raises(RuntimeError):
            safe_tar_members(tar, tmp_path)


def test_validate_archive_member_rejects_symlink(tmp_path) -> None:
    member = tarfile.TarInfo("safe.txt")
    member.type = tarfile.SYMTYPE
    member.linkname = "target.txt"
    with pytest.raises(RuntimeError):
        validate_archive_member(member, {"safe.txt"}, tmp_path)


def test_validate_archive_member_accepts_expected_regular_file(tmp_path) -> None:
    member = tarfile.TarInfo("./Camera/safe.txt")
    member.size = 4
    assert validate_archive_member(member, {"Camera/safe.txt"}, tmp_path) == "Camera/safe.txt"


def test_probe_archive_tools_reports_missing_tar_and_gzip(monkeypatch) -> None:
    def fake_adb_shell(_adb_path: str, shell_cmd: str, *, check: bool = True):
        return subprocess.CompletedProcess(
            args=["adb", "shell", shell_cmd],
            returncode=1,
            stdout="",
            stderr="missing",
        )

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)

    probe = cli.probe_archive_tools("adb")

    assert probe.supported is False
    assert probe.reasons == ["missing_tar", "missing_gzip"]


def test_probe_archive_tools_reports_smoke_failure(monkeypatch) -> None:
    def fake_adb_shell(_adb_path: str, shell_cmd: str, *, check: bool = True):
        if "command -v tar" in shell_cmd or "toybox tar" in shell_cmd:
            return subprocess.CompletedProcess(args=["adb"], returncode=0, stdout="", stderr="")
        if "command -v gzip" in shell_cmd or "toybox gzip" in shell_cmd:
            return subprocess.CompletedProcess(args=["adb"], returncode=0, stdout="", stderr="")
        return subprocess.CompletedProcess(args=["adb"], returncode=1, stdout="", stderr="smoke failed")

    monkeypatch.setattr(cli, "adb_shell", fake_adb_shell)

    probe = cli.probe_archive_tools("adb")

    assert probe.supported is False
    assert probe.reasons == ["smoke_failed"]


def test_pull_one_file_recovers_from_partial_adb_transfer(tmp_path, monkeypatch) -> None:
    local_final = tmp_path / "Camera" / "video.mp4"
    remote_meta = RemoteFile("Camera/video.mp4", size=8, mtime=1, birth=1)
    attempts: list[Path] = []

    def fake_run(cmd, *, text=True, capture_output=True, check=True, timeout=None):
        local_part = Path(cmd[3])
        attempts.append(local_part)
        local_part.parent.mkdir(parents=True, exist_ok=True)
        payload = b"partial" if len(attempts) == 1 else b"complete"
        local_part.write_bytes(payload)
        return subprocess.CompletedProcess(args=cmd, returncode=0, stdout="", stderr="")

    monkeypatch.setattr(cli, "run", fake_run)
    monkeypatch.setattr(cli, "apply_timestamps", lambda *_args, **_kwargs: None)

    ok, reason = cli.pull_one_file(
        "adb",
        "/storage/emulated/0/DCIM/Camera/video.mp4",
        local_final,
        remote_meta,
        logger=logging.getLogger("test"),
        verify_hash=False,
        hash_cmd=None,
        per_file_retries=2,
        retry_wait=0,
    )

    assert ok is True
    assert reason == "ok"
    assert local_final.read_bytes() == b"complete"
    assert attempts == [tmp_path / "Camera" / "video.mp4.part", tmp_path / "Camera" / "video.mp4.part"]


def test_stream_extract_archive_chunk_aborts_on_truncated_adb_stream(tmp_path, monkeypatch) -> None:
    # The fake process returns a truncated gzipped tar stream so tarfile fails mid-read.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    manifest: dict[str, dict[str, object]] = {}
    restore_journal: dict[str, object] = {}
    chunk = cli.ArchiveChunk(index=1, files=[RemoteFile("Camera/broken.jpg", size=4, mtime=1, birth=1)], estimated_size=4)

    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode="w:gz") as tar:
        payload = b"data"
        info = tarfile.TarInfo("./Camera/broken.jpg")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    truncated_stream = io.BytesIO(archive_bytes.getvalue()[: max(1, archive_bytes.tell() // 2)])

    class FakeProcess:
        def __init__(self) -> None:
            self.stdout = truncated_stream
            self.killed = False

        def kill(self) -> None:
            self.killed = True

        def wait(self) -> int:
            return 1

    def fake_popen(cmd, stdout=None, stderr=None):
        return FakeProcess()

    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(cli, "apply_timestamps", lambda *_args, **_kwargs: None)

    ok, reason, stream_hash, stream_bytes = cli.stream_extract_archive_chunk(
        "adb",
        "/storage/emulated/0/DCIM",
        local_root,
        chunk,
        compression_level=1,
        manifest=manifest,
        manifest_path=tmp_path / ".arft-local-sync-state.json",
        restore_journal=restore_journal,
        restore_journal_path=tmp_path / ".arft-archive-restore-journal.json",
        logger=logging.getLogger("test"),
    )

    assert ok is False
    assert reason.startswith("stream extraction failed:")
    assert stream_hash
    assert not (local_root / "Camera" / "broken.jpg").exists()


def test_run_archive_stream_mode_writes_failure_list_when_chunk_stream_breaks(tmp_path, monkeypatch) -> None:
    # Force a failed chunk run and verify the TSV failure ledger is written.
    local_root = tmp_path / "backup"
    local_root.mkdir()
    manifest: dict[str, dict[str, object]] = {}
    remote_files = [
        RemoteFile("Camera/a.jpg", size=700 * 1024**2, mtime=1, birth=1),
        RemoteFile("Camera/b.jpg", size=400 * 1024**2, mtime=1, birth=1),
    ]
    args = argparse.Namespace(
        archive_chunk_size=1024**3,
        archive_max_files_per_chunk=10,
        archive_compression_level=1,
        force_all=False,
        skip_all_checks=False,
        dry_run=False,
        retries=1,
        retry_wait=0,
    )

    archive_bytes = io.BytesIO()
    with tarfile.open(fileobj=archive_bytes, mode="w:gz") as tar:
        payload = b"data"
        info = tarfile.TarInfo("./Camera/a.jpg")
        info.size = len(payload)
        tar.addfile(info, io.BytesIO(payload))
    truncated_stream = io.BytesIO(archive_bytes.getvalue()[: max(1, archive_bytes.tell() // 2)])

    class FakeProcess:
        def __init__(self) -> None:
            self.stdout = truncated_stream

        def kill(self) -> None:
            pass

        def wait(self) -> int:
            return 1

    def fake_popen(cmd, stdout=None, stderr=None):
        return FakeProcess()

    monkeypatch.setattr(cli, "probe_archive_tools", lambda _adb_path: cli.ArchiveToolProbe(True, []))
    monkeypatch.setattr(cli, "populate_remote_metadata_batch", lambda *args, **kwargs: None)
    monkeypatch.setattr(cli, "ensure_remote_metadata", lambda _adb_path, _remote_root, rf: rf)
    monkeypatch.setattr(cli.subprocess, "Popen", fake_popen)
    monkeypatch.setattr(cli, "apply_timestamps", lambda *_args, **_kwargs: None)

    exit_code = cli.run_archive_stream_mode(
        adb_path="adb",
        remote_root="/storage/emulated/0/DCIM",
        local_root=local_root,
        remote_files=remote_files,
        manifest=manifest,
        manifest_path=tmp_path / ".arft-local-sync-state.json",
        args=args,
        logger=logging.getLogger("test"),
        supports_multi_stat=False,
        check_all_files=False,
    )

    assert exit_code == 1
    failed_path = local_root / ".arft-failed-files.tsv"
    assert failed_path.exists()
    assert failed_path.read_text(encoding="utf-8").splitlines()[1].startswith("chunk-000001\tstream extraction failed:")
