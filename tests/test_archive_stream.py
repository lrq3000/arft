from __future__ import annotations

import io
import tarfile

import pytest

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


def test_size_parser_uses_binary_units() -> None:
    assert parse_size_spec("1K") == 1024
    assert parse_size_spec("2M") == 2 * 1024**2
    assert parse_size_spec("3G") == 3 * 1024**3


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
    assert load_json_object(path)["chunks"]["chunk-000001"]["status"] == "transferred"


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
