"""Durable run metadata, checkpoints, and per-attempt files."""

from __future__ import annotations

import asyncio
import hashlib
import json
import os
import re
import uuid
from os import PathLike
from pathlib import Path
from typing import Any, BinaryIO, cast

from .contracts import ArtifactStore
from .errors import RecoveryError
from .events import json_value
from .models import Attempt, RuntimeEvent, TaskResult


def atomic_write(path: Path, data: bytes) -> None:
    """Publish a complete file only after its contents have reached the filesystem."""
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{path.name}.{uuid.uuid4().hex}.tmp")
    try:
        with temporary.open("xb") as stream:
            stream.write(data)
            stream.flush()
            os.fsync(stream.fileno())
        temporary.replace(path)
        if os.name == "posix":
            descriptor = os.open(path.parent, os.O_RDONLY)
            try:
                os.fsync(descriptor)
            finally:
                os.close(descriptor)
    finally:
        temporary.unlink(missing_ok=True)


def json_bytes(value: object) -> bytes:
    return (
        json.dumps(json_value(value), ensure_ascii=False, allow_nan=False, indent=2) + "\n"
    ).encode()


async def write_json(path: Path, value: object) -> None:
    await asyncio.to_thread(atomic_write, path, json_bytes(value))


async def write_text(path: Path, value: str) -> None:
    await asyncio.to_thread(atomic_write, path, value.encode("utf-8"))


def read_text(path: Path) -> str:
    return path.read_bytes().decode("utf-8", errors="replace") if path.is_file() else ""


def read_json(path: Path) -> dict[str, Any]:
    try:
        value: Any = json.loads(path.read_bytes())
        if not isinstance(value, dict):
            raise ValueError("Expected an object")
        return cast(dict[str, Any], value)
    except (OSError, ValueError) as exc:
        raise RecoveryError(f"Cannot read checkpoint {path}: {exc}") from exc


def lock_file(stream: BinaryIO) -> None:
    if os.name == "posix":
        import fcntl

        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    else:
        import msvcrt

        stream.seek(0)
        msvcrt.locking(stream.fileno(), msvcrt.LK_NBLCK, 1)


def attempt_metadata(attempt: Attempt) -> dict[str, Any]:
    # Large streams live in their own files rather than being copied into JSON.
    return {
        "number": attempt.number,
        "started_at": attempt.started_at,
        "finished_at": attempt.finished_at,
        "error": attempt.error,
        "session_id": attempt.session_id,
        "exit_code": attempt.exit_code,
        "artifacts": attempt.artifacts,
    }


def result_metadata(result: TaskResult[Any]) -> dict[str, Any]:
    return {
        "value": result.value,
        "attempts": [attempt_metadata(attempt) for attempt in result.attempts],
        "session_id": result.session_id,
        "exit_code": result.exit_code,
        "error": str(result.error) if result.error is not None else None,
        "artifacts": result.artifacts,
    }


class FileArtifactStore(ArtifactStore):
    """Keep local run files and automatically find unfinished matching runs.

    Run locks prevent two processes from continuing the same invocation. On POSIX,
    subprocess stdout descriptors also retain a lock if a parent is killed.
    """

    def __init__(self, directory: str | PathLike[str] = ".orchlet/runs") -> None:
        self.directory: Path = Path(directory).expanduser().resolve()
        self._locks: dict[str, BinaryIO] = {}

    def run_dir(self, run_id: str) -> Path:
        if not re.fullmatch(r"[A-Za-z0-9_-]+", run_id):
            raise ValueError("Run IDs must be nonempty filesystem-safe identifiers")
        return self.directory / run_id

    def task_dir(self, run_id: str, task_id: str) -> Path:
        label = re.sub(r"[^A-Za-z0-9_.-]", "_", task_id.removeprefix(f"{run_id}/"))[:80]
        digest = hashlib.sha256(task_id.encode()).hexdigest()[:12]
        return self.run_dir(run_id) / "tasks" / f"{label}-{digest}"

    def attempt_dir(self, run_id: str, task_id: str, attempt: int) -> Path:
        if attempt < 1:
            raise ValueError("Attempt numbers must be positive")
        return self.task_dir(run_id, task_id) / "attempts" / f"{attempt:03}"

    def open_run(self, key: str, *, resume: bool) -> tuple[str, bool]:
        key = hashlib.sha256(key.encode()).hexdigest()
        index = self.directory / ".index" / f"{key}.json"
        index.parent.mkdir(parents=True, exist_ok=True)
        stream = index.with_suffix(".lock").open("a+b")
        try:
            try:
                lock_file(stream)
            except OSError as exc:
                raise RecoveryError("This flow invocation is already running") from exc
            run_id = uuid.uuid4().hex[:12]
            resumed = False
            if resume and index.is_file():
                previous = read_json(index)["run_id"]
                metadata = self.run_dir(previous) / "run.json"
                if not metadata.is_file() or read_json(metadata).get("status") != "succeeded":
                    run_id, resumed = previous, True
                    # A killed parent can leave an agent process alive. Do not race it.
                    if os.name == "posix":
                        for output in self.run_dir(run_id).glob("tasks/*/attempts/*/stdout.log"):
                            with output.open("ab") as probe:
                                try:
                                    lock_file(probe)
                                except OSError as exc:
                                    raise RecoveryError(
                                        f"An agent from run {run_id} is still running; "
                                        "wait for it to exit before continuing"
                                    ) from exc
            self.run_dir(run_id).mkdir(parents=True, exist_ok=True)
            atomic_write(index, json_bytes({"run_id": run_id}))
            self._locks[run_id] = stream
            return run_id, resumed
        except BaseException:
            stream.close()
            raise

    def close_run(self, run_id: str) -> None:
        if (stream := self._locks.pop(run_id, None)) is not None:
            stream.close()

    async def append_event(self, run_id: str, event: RuntimeEvent) -> None:
        line = json.dumps(json_value(event), ensure_ascii=False, allow_nan=False) + "\n"
        path = self.run_dir(run_id) / "events.jsonl"

        def append() -> None:
            path.parent.mkdir(parents=True, exist_ok=True)
            with path.open("a", encoding="utf-8") as stream:
                stream.write(line)
                stream.flush()

        await asyncio.to_thread(append)
