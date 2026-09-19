"""Agent transports. Codex calls are opt-in; importing this module performs no I/O."""

from __future__ import annotations

import asyncio
import json
import os
import signal
import tempfile
from contextlib import nullcontext
from datetime import datetime, timezone
from collections.abc import Mapping, Sequence
from os import PathLike
from pathlib import Path
from typing import Any, cast

from .artifacts import lock_file, read_json, read_text, write_json, write_text
from .contracts import AgentBackend, Emit
from .errors import BackendError, TaskCancelled
from .models import AgentReply, AgentRequest, AttemptArtifacts
from .runners import CancellationToken


async def _communicate(
    command: Sequence[str],
    prompt: str,
    cancellation: CancellationToken,
    *,
    artifacts: AttemptArtifacts | None = None,
    cwd: str | PathLike[str] | None = None,
    env: Mapping[str, str] | None = None,
    grace: float = 2.0,
) -> tuple[int, str, str]:
    if artifacts is None:
        with tempfile.TemporaryDirectory(prefix="orchlet-command-") as directory:
            return await _communicate(
                command,
                prompt,
                cancellation,
                artifacts=AttemptArtifacts(Path(directory)),
                cwd=cwd,
                env=env,
                grace=grace,
            )
    artifacts.directory.mkdir(parents=True, exist_ok=True)
    await write_text(artifacts.prompt_txt, prompt)
    launch: dict[str, Any] = (
        read_json(artifacts.launch_json) if artifacts.launch_json.exists() else {}
    )
    launch.update(
        {
            "command": list(command),
            "cwd": str(Path(cwd or Path.cwd()).resolve()),
            "started_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    await write_json(artifacts.launch_json, launch)
    # Direct descriptors keep both streams on disk throughout execution, including
    # if the orchestrator dies. Do not replace these files while the child owns them.
    with (
        artifacts.stdout_log.open("wb") as stdout_file,
        artifacts.stderr_log.open("wb") as stderr_file,
    ):
        if os.name == "posix":
            lock_file(stdout_file)
        try:
            process = await asyncio.create_subprocess_exec(
                *command,
                stdin=asyncio.subprocess.PIPE,
                stdout=stdout_file,
                stderr=stderr_file,
                cwd=cwd,
                env=env,
                start_new_session=os.name == "posix",
            )
        except BaseException as exc:
            launch["error"] = f"{type(exc).__name__}: {exc}"
            await write_json(artifacts.launch_json, launch)
            raise
        io = asyncio.create_task(process.communicate(prompt.encode("utf-8")))
        cancelled = asyncio.create_task(cancellation.wait())

        def terminate(hard: bool = False) -> None:
            try:
                if os.name == "posix":
                    os.killpg(process.pid, signal.SIGKILL if hard else signal.SIGTERM)
                elif process.returncode is None:
                    process.kill() if hard else process.terminate()
            except ProcessLookupError:
                pass

        async def stop() -> None:
            terminate()
            try:
                await asyncio.wait_for(asyncio.shield(io), grace)
            except asyncio.TimeoutError:
                terminate(hard=True)
                await io
            await process.wait()
            if os.name == "posix":
                terminate(hard=True)  # Reap the leader and stop surviving group members.

        try:
            launch["pid"] = process.pid
            await write_json(artifacts.launch_json, launch)
            done, _ = await asyncio.wait((io, cancelled), return_when=asyncio.FIRST_COMPLETED)
            if cancelled in done and cancellation.requested:
                await stop()
                raise TaskCancelled("Agent process stopped")
            await asyncio.shield(io)
        finally:
            cancelled.cancel()
            await asyncio.gather(cancelled, return_exceptions=True)
            if process.returncode is None:
                await stop()
            launch["exit_code"] = process.returncode
            launch["finished_at"] = datetime.now(timezone.utc).isoformat()
            await write_json(artifacts.launch_json, launch)
    assert process.returncode is not None
    return process.returncode, read_text(artifacts.stdout_log), read_text(artifacts.stderr_log)


class CommandBackend(AgentBackend):
    """Send a prompt on stdin, receive the final response on stdout; never invokes a shell."""

    def __init__(
        self,
        command: Sequence[str],
        *,
        cwd: str | PathLike[str] | None = None,
        env: Mapping[str, str] | None = None,
    ) -> None:
        if isinstance(command, str) or not command:
            raise ValueError("command must be a nonempty sequence of argv elements")
        self.command: tuple[str, ...] = tuple(command)
        self.cwd = cwd
        self.env: dict[str, str] | None = None if env is None else {**os.environ, **env}

    async def run_turn(
        self, request: AgentRequest, emit: Emit, cancellation: CancellationToken
    ) -> AgentReply:
        if request.session_id:
            raise ValueError("CommandBackend does not implement session continuation")
        code, stdout, stderr = await _communicate(
            self.command,
            request.prompt,
            cancellation,
            cwd=self.cwd,
            env=self.env,
            artifacts=request.artifacts,
        )
        if code:
            raise BackendError(
                f"Agent command exited with {code}",
                stdout=stdout,
                stderr=stderr,
                exit_code=code,
                raw_text=stdout,
            )
        return AgentReply(stdout, stdout, stderr, exit_code=code)


class CodexBackend(AgentBackend):
    """Noninteractive CLI adapter, with explicit existing-session continuation."""

    supports_resume = True

    def __init__(
        self,
        *,
        executable: str = "codex",
        model: str | None = None,
        sandbox: str = "read-only",
        cwd: str | PathLike[str] | None = None,
        extra_args: Sequence[str] = (),
    ) -> None:
        if sandbox not in {"read-only", "workspace-write", "danger-full-access"}:
            raise ValueError("Unsupported Codex sandbox mode")
        if isinstance(extra_args, str):
            raise TypeError("extra_args must be an argv sequence")
        self.executable: str = executable
        self.model: str | None = model
        self.sandbox: str = sandbox
        self.cwd: str | PathLike[str] | None = cwd
        self.extra_args: tuple[str, ...] = tuple(extra_args)

    def command(self, output_path: str | PathLike[str], session_id: str | None = None) -> list[str]:
        command = [
            self.executable,
            "exec",
            "--json",
            "--skip-git-repo-check",
            "--sandbox",
            self.sandbox,
            "-c",
            'approval_policy="never"',
            "--output-last-message",
            str(output_path),
        ]
        if self.model:
            command += ["--model", self.model]
        command += list(self.extra_args)
        if session_id:
            command += ["resume", session_id]
        return [*command, "-"]

    async def run_turn(
        self, request: AgentRequest, emit: Emit, cancellation: CancellationToken
    ) -> AgentReply:
        context = (
            nullcontext(request.artifacts.directory)
            if request.artifacts is not None
            else tempfile.TemporaryDirectory(prefix="orchlet-codex-")
        )
        with context as directory:
            artifacts = request.artifacts or AttemptArtifacts(Path(directory))
            output = artifacts.output_txt
            code, stdout, stderr = await _communicate(
                self.command(output, request.session_id),
                request.prompt,
                cancellation,
                cwd=self.cwd,
                artifacts=artifacts,
            )
            session_id = request.session_id
            for line in stdout.splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if (
                    isinstance(event, dict)
                    and cast(dict[str, Any], event).get("type") == "thread.started"
                ):
                    session_id = cast(dict[str, Any], event).get("thread_id", session_id)
            if code:
                raise BackendError(
                    f"codex exited with {code}",
                    stdout=stdout,
                    stderr=stderr,
                    exit_code=code,
                    raw_text=read_text(output) if output.exists() else None,
                    session_id=session_id,
                )
            if not output.exists():
                raise BackendError(
                    "Codex did not write a final response",
                    stdout=stdout,
                    stderr=stderr,
                    exit_code=code,
                    session_id=session_id,
                )
            return AgentReply(read_text(output), stdout, stderr, session_id, exit_code=code)
