"""Agent transports. Codex calls are opt-in; importing this module performs no I/O."""

from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import signal
import tempfile

from .contracts import AgentBackend
from .errors import BackendError, TaskCancelled
from .models import AgentReply


async def _communicate(command, prompt, cancellation, *, cwd=None, env=None, grace=2.0):
    process = await asyncio.create_subprocess_exec(
        *command,
        stdin=asyncio.subprocess.PIPE,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
        cwd=cwd,
        env=env,
        start_new_session=os.name == "posix",
    )
    io = asyncio.create_task(process.communicate(prompt.encode("utf-8")))
    cancelled = asyncio.create_task(cancellation.wait())

    def terminate(hard=False):
        if process.returncode is not None and os.name != "posix":
            return
        try:
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL if hard else signal.SIGTERM)
            elif hard:
                process.kill()
            else:
                process.terminate()
        except ProcessLookupError:
            pass

    async def stop():
        terminate()
        try:
            await asyncio.wait_for(asyncio.shield(io), grace)
        except asyncio.TimeoutError:
            terminate(hard=True)
            await io
        await process.wait()

    try:
        done, _ = await asyncio.wait((io, cancelled), return_when=asyncio.FIRST_COMPLETED)
        if cancelled in done and cancellation.requested:
            await stop()
            raise TaskCancelled("Agent process stopped")
        stdout, stderr = await io
        return (
            process.returncode,
            stdout.decode("utf-8", errors="replace"),
            stderr.decode("utf-8", errors="replace"),
        )
    finally:
        cancelled.cancel()
        await asyncio.gather(cancelled, return_exceptions=True)
        if process.returncode is None:
            await stop()


class CommandBackend(AgentBackend):
    """Send a prompt on stdin, receive the final response on stdout; never invokes a shell."""

    def __init__(self, command, *, cwd=None, env=None):
        if isinstance(command, str) or not command:
            raise ValueError("command must be a nonempty sequence of argv elements")
        self.command = tuple(command)
        self.cwd = cwd
        self.env = None if env is None else {**os.environ, **env}

    async def run_turn(self, request, emit, cancellation):
        if request.session_id:
            raise ValueError("CommandBackend does not implement session continuation")
        code, stdout, stderr = await _communicate(
            self.command,
            request.prompt,
            cancellation,
            cwd=self.cwd,
            env=self.env,
        )
        if code:
            raise BackendError(
                f"Agent command exited with {code}", stdout=stdout, stderr=stderr, exit_code=code
            )
        return AgentReply(stdout, stdout, stderr)


class CodexBackend(AgentBackend):
    """Noninteractive CLI adapter, with explicit existing-session continuation."""

    supports_resume = True

    def __init__(
        self, *, executable="codex", model=None, sandbox="read-only", cwd=None, extra_args=()
    ):
        if sandbox not in {"read-only", "workspace-write", "danger-full-access"}:
            raise ValueError("Unsupported Codex sandbox mode")
        if isinstance(extra_args, str):
            raise TypeError("extra_args must be an argv sequence")
        self.executable, self.model, self.sandbox = executable, model, sandbox
        self.cwd, self.extra_args = cwd, tuple(extra_args)

    def command(self, output_path, session_id=None):
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

    async def run_turn(self, request, emit, cancellation):
        with tempfile.TemporaryDirectory(prefix="orchlet-codex-") as directory:
            output = Path(directory) / "response.txt"
            code, stdout, stderr = await _communicate(
                self.command(output, request.session_id),
                request.prompt,
                cancellation,
                cwd=self.cwd,
            )
            session_id = request.session_id
            for line in stdout.splitlines():
                try:
                    event = json.loads(line)
                except json.JSONDecodeError:
                    continue
                if isinstance(event, dict) and event.get("type") == "thread.started":
                    session_id = event.get("thread_id", session_id)
            if code:
                raise BackendError(
                    f"codex exited with {code}", stdout=stdout, stderr=stderr, exit_code=code
                )
            if not output.exists():
                raise BackendError(
                    "Codex did not write a final response", stdout=stdout, stderr=stderr
                )
            return AgentReply(output.read_text(encoding="utf-8"), stdout, stderr, session_id)
