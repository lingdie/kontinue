"""Dependency-free E2B-compatible Python SDK for Kontinue."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import math
import os
import secrets
import socket
import ssl
import struct
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any, Callable, Iterator


class APIError(RuntimeError):
    def __init__(self, status: int, code: str, message: str):
        super().__init__(f"kontinue API {status} {code}: {message}")
        self.status, self.code, self.message = status, code, message


class CommandExitError(RuntimeError):
    def __init__(self, result: "CommandResult"):
        super().__init__(f"command exited with code {result.exit_code}")
        self.result = result


@dataclass
class CommandResult:
    pid: int
    stdout: str
    stderr: str
    exit_code: int
    error: str | None = None
    timed_out: bool = False
    duration_ms: int = 0
    running: bool = False


@dataclass
class FileInfo:
    path: str
    is_dir: bool
    is_file: bool
    size: int
    modified_at_unix_ms: int | None = None


@dataclass
class FileEntry:
    name: str
    path: str
    is_dir: bool
    is_file: bool
    size: int


@dataclass
class ProcessInfo:
    pid: int
    command: list[str]
    started_at_unix_ms: int
    running: bool
    exit_code: int = 0


def _env(*names: str) -> str:
    for name in names:
        value = os.environ.get(name)
        if value:
            return value
    return ""


def _create_payload(template: str | dict[str, Any] | None, options: dict[str, Any]) -> dict[str, Any]:
    source: dict[str, Any] = {}
    if isinstance(template, dict):
        source.update(template)
    elif template:
        source["templateId"] = template
    source.update(options)
    if "template" in source:
        source["templateId"] = source.pop("template")
    if "template_id" in source:
        source["templateId"] = source.pop("template_id")
    if "envs" in source:
        source["envVars"] = source.pop("envs")
    if "env_vars" in source:
        source["envVars"] = source.pop("env_vars")
    if "timeout" in source:
        source["idleTimeoutSeconds"] = max(1, math.ceil(float(source.pop("timeout"))))
    if "timeout_ms" in source:
        source["idleTimeoutSeconds"] = max(1, math.ceil(int(source.pop("timeout_ms")) / 1000))
    for key in ("base_image", "cpu_millis", "memory_bytes", "allow_filesystem_only_fallback"):
        if key in source:
            parts = key.split("_")
            camel_key = parts[0] + "".join(part.capitalize() for part in parts[1:])
            source[camel_key] = source.pop(key)
    return source


class Client:
    """Low-level API client retained for callers that need the control plane directly."""

    def __init__(
        self,
        base_url: str | None = None,
        tenant_token: str = "",
        timeout: float = 30.0,
        retries: int = 3,
        gateway_url: str | None = None,
        api_key: str | None = None,
        api_url: str | None = None,
    ):
        self.base_url = (base_url or api_url or _env("KONTINUE_API_URL", "E2B_API_URL") or "http://127.0.0.1:8081").rstrip("/")
        self.gateway_url = (gateway_url or _env("KONTINUE_GATEWAY_URL", "E2B_GATEWAY_URL") or "http://127.0.0.1:8080").rstrip("/")
        self.tenant_token = tenant_token or api_key or _env("KONTINUE_TENANT_TOKEN", "E2B_API_KEY")
        self.timeout = timeout
        self.retries = retries

    def _request(self, method: str, path: str, payload: Any | None = None) -> Any:
        body = None if payload is None else json.dumps(payload).encode()
        headers = {"Content-Type": "application/json"}
        if self.tenant_token:
            headers["Authorization"] = f"Bearer {self.tenant_token}"
        return self._request_raw(self.base_url + path, method, body, headers)

    def _sandbox_request(self, sandbox_id: str, access_token: str, method: str, path: str, payload: Any | None = None) -> Any:
        body = None if payload is None else json.dumps(payload).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Kontinue-Sandbox-ID": sandbox_id,
            "X-Kontinue-Token": access_token,
        }
        return self._request_raw(self.gateway_url + path, method, body, headers)

    def _sandbox_bytes(self, sandbox_id: str, access_token: str, method: str, path: str, body: bytes | None = None) -> bytes:
        headers = {
            "X-Kontinue-Sandbox-ID": sandbox_id,
            "X-Kontinue-Token": access_token,
        }
        if body is not None:
            headers["Content-Type"] = "application/octet-stream"
        return self._request_raw(self.gateway_url + path, method, body, headers, raw=True)

    def _sandbox_stream(self, sandbox_id: str, access_token: str, path: str):
        request = urllib.request.Request(
            self.gateway_url + path,
            headers={"X-Kontinue-Sandbox-ID": sandbox_id, "X-Kontinue-Token": access_token},
            method="GET",
        )
        try:
            response = urllib.request.urlopen(request, timeout=self.timeout)
        except urllib.error.HTTPError as error:
            data = error.read()
            try:
                problem = json.loads(data or b"{}")
            except json.JSONDecodeError:
                problem = {}
            raise APIError(error.code, problem.get("code", ""), problem.get("error", data.decode(errors="replace"))) from error
        try:
            for line in response:
                if line.strip():
                    yield json.loads(line)
        finally:
            response.close()

    def _request_raw(self, url: str, method: str, body: bytes | None, headers: dict[str, str], raw: bool = False) -> Any:
        for attempt in range(self.retries + 1):
            request = urllib.request.Request(url, body, headers, method)
            try:
                with urllib.request.urlopen(request, timeout=self.timeout) as response:
                    data = response.read()
                    return data if raw else json.loads(data) if data else None
            except urllib.error.HTTPError as error:
                data = error.read()
                if error.code >= 500 and attempt < self.retries:
                    time.sleep(0.1 * 2**attempt)
                    continue
                try:
                    problem = json.loads(data or b"{}")
                except json.JSONDecodeError:
                    problem = {}
                detail = problem.get("error", data.decode(errors="replace"))
                raise APIError(error.code, problem.get("code", ""), detail) from error

    def create_sandbox(self, **options: Any) -> dict[str, Any]:
        return self._request("POST", "/v1/sandboxes", options)

    def list_sandboxes(self, *, state: str = "", template_id: str = "", cursor: str = "", limit: int = 100) -> dict[str, Any]:
        query = urllib.parse.urlencode({k: v for k, v in {"state": state, "templateId": template_id, "cursor": cursor, "limit": limit}.items() if v})
        return self._request("GET", "/v1/sandboxes?" + query)

    def connect(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("POST", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/connect")

    def suspend(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("POST", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/suspend")

    def resume(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("POST", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/resume")

    def snapshot(self, sandbox_id: str) -> dict[str, Any]:
        return self._request("POST", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/snapshots")

    def delete_sandbox(self, sandbox_id: str) -> None:
        self._request("DELETE", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}")

    def set_sandbox_timeout(self, sandbox_id: str, timeout_ms: int) -> dict[str, Any]:
        seconds = max(1, math.ceil(timeout_ms / 1000))
        return self._request("POST", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/timeout", {"idleTimeoutSeconds": seconds})

    def fork(self, sandbox_id: str, *, count: int = 0, env_vars: dict[str, str] | None = None, children: list[dict[str, Any]] | None = None) -> list[dict[str, Any]]:
        payload: dict[str, Any] = {}
        if count > 0:
            payload["count"] = count
        if env_vars:
            payload["envVars"] = env_vars
        if children:
            payload["children"] = children
        return self._request("POST", f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/fork", payload)

    def signed_file_url(self, sandbox_id: str, path: str, operation: str, expires_in_seconds: int = 300) -> dict[str, Any]:
        return self._request(
            "POST",
            f"/v1/sandboxes/{urllib.parse.quote(sandbox_id)}/files/urls",
            {"path": path, "operation": operation, "expiresInSeconds": expires_in_seconds},
        )

    def create_volume(self, **options: Any) -> dict[str, Any]:
        return self._request("POST", "/v1/volumes", options)

    def list_volumes(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/volumes")

    def create_template(self, **options: Any) -> dict[str, Any]:
        return self._request("POST", "/v1/templates", options)

    def list_templates(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/templates")

    def get_template(self, template_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/templates/{urllib.parse.quote(template_id)}")

    def rebuild_template(self, template_id: str, **options: Any) -> dict[str, Any]:
        return self._request("POST", f"/v1/templates/{urllib.parse.quote(template_id)}/builds", options)

    def template_builds(self, template_id: str) -> list[dict[str, Any]]:
        return self._request("GET", f"/v1/templates/{urllib.parse.quote(template_id)}/builds")

    def template_logs(self, template_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/templates/{urllib.parse.quote(template_id)}/logs")

    def tag_template(self, template_id: str, tag: str, snapshot_id: str = "") -> dict[str, Any]:
        payload = {"tag": tag}
        if snapshot_id:
            payload["snapshotId"] = snapshot_id
        return self._request("POST", f"/v1/templates/{urllib.parse.quote(template_id)}/tags", payload)

    def delete_template(self, template_id: str) -> None:
        self._request("DELETE", f"/v1/templates/{urllib.parse.quote(template_id)}")

    def create_webhook(self, url: str, events: list[str] | None = None, secret: str = "") -> dict[str, Any]:
        payload: dict[str, Any] = {"url": url}
        if events:
            payload["events"] = events
        if secret:
            payload["secret"] = secret
        return self._request("POST", "/v1/webhooks", payload)

    def list_webhooks(self) -> list[dict[str, Any]]:
        return self._request("GET", "/v1/webhooks")

    def get_webhook(self, webhook_id: str) -> dict[str, Any]:
        return self._request("GET", f"/v1/webhooks/{urllib.parse.quote(webhook_id)}")

    def delete_webhook(self, webhook_id: str) -> None:
        self._request("DELETE", f"/v1/webhooks/{urllib.parse.quote(webhook_id)}")

    def dashboard(self) -> dict[str, Any]:
        return self._request("GET", "/v1/dashboard")


class CommandHandle:
    def __init__(self, commands: "Commands", pid: int):
        self._commands = commands
        self.pid = pid
        self._result: CommandResult | None = None

    @property
    def stdout(self) -> str:
        return self._result.stdout if self._result else ""

    @property
    def stderr(self) -> str:
        return self._result.stderr if self._result else ""

    @property
    def exit_code(self) -> int | None:
        return self._result.exit_code if self._result else None

    def wait(self, poll_interval: float = 0.1) -> CommandResult:
        if self._result is None:
            self._result = self._commands.wait(self.pid, poll_interval=poll_interval)
        if self._result.exit_code != 0:
            raise CommandExitError(self._result)
        return self._result

    def kill(self) -> None:
        self._commands.kill(self.pid)


class Commands:
    def __init__(self, client: Client, sandbox: "Sandbox"):
        self._client = client
        self._sandbox = sandbox

    def run(
        self,
        cmd: str,
        *,
        background: bool = False,
        envs: dict[str, str] | None = None,
        cwd: str | None = None,
        timeout: float | None = None,
        stdin: str | None = None,
        on_stdout: Callable[[str], None] | None = None,
        on_stderr: Callable[[str], None] | None = None,
        raise_on_error: bool = True,
    ) -> CommandResult | CommandHandle:
        stream_output = not background and (on_stdout is not None or on_stderr is not None)
        request: dict[str, Any] = {
            "command": ["/bin/sh", "-c", cmd],
            "background": background or stream_output,
        }
        if cwd is not None:
            request["workingDir"] = cwd
        if envs is not None:
            request["env"] = envs
        if stdin is not None:
            request["stdin"] = stdin
        if timeout is not None:
            request["timeoutMs"] = max(1, math.ceil(timeout * 1000))
        result = self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "POST", "/v1/process/exec", request)
        command_result = _command_result(result)
        if background:
            return CommandHandle(self, command_result.pid)
        if stream_output:
            stdout = ""
            stderr = ""
            exit_code = command_result.exit_code
            for event in self._client._sandbox_stream(self._sandbox.sandbox_id, self._sandbox.access_token, f"/v1/process/logs/stream?pid={command_result.pid}"):
                if event.get("type") == "stdout":
                    chunk = str(event.get("data", ""))
                    stdout += chunk
                    if on_stdout:
                        on_stdout(chunk)
                elif event.get("type") == "stderr":
                    chunk = str(event.get("data", ""))
                    stderr += chunk
                    if on_stderr:
                        on_stderr(chunk)
                elif event.get("type") == "exit":
                    exit_code = int(event.get("exitCode", 0))
            command_result = CommandResult(command_result.pid, stdout, stderr, exit_code)
            if raise_on_error and command_result.exit_code != 0:
                raise CommandExitError(command_result)
            return command_result
        if on_stdout:
            on_stdout(command_result.stdout)
        if on_stderr:
            on_stderr(command_result.stderr)
        if raise_on_error and command_result.exit_code != 0:
            raise CommandExitError(command_result)
        return command_result

    def list(self) -> list[ProcessInfo]:
        return [_process_info(item) for item in self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "GET", "/v1/process/list")]

    def connect(self, pid: int) -> CommandHandle:
        return CommandHandle(self, pid)

    def kill(self, pid: int) -> None:
        self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "POST", f"/v1/process/kill?pid={pid}")

    def wait(self, pid: int, *, poll_interval: float = 0.1) -> CommandResult:
        while True:
            result = _command_result(self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "GET", f"/v1/process/logs?pid={pid}"))
            if not result.running:
                return result
            time.sleep(poll_interval)


class Filesystem:
    def __init__(self, client: Client, sandbox: "Sandbox"):
        self._client = client
        self._sandbox = sandbox

    def read(self, path: str, *, format: str = "text") -> str | bytes:
        data = self._client._sandbox_bytes(self._sandbox.sandbox_id, self._sandbox.access_token, "GET", f"/v1/files?path={urllib.parse.quote(path)}")
        return data if format == "bytes" else data.decode()

    def write(self, path: str, data: str | bytes, *, mode: int | None = None) -> None:
        content = data.encode() if isinstance(data, str) else data
        if mode is None:
            self._client._sandbox_bytes(self._sandbox.sandbox_id, self._sandbox.access_token, "PUT", f"/v1/files/stream?path={urllib.parse.quote(path)}", content)
            return
        self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "PUT", "/v1/files", {"path": path, "content": list(content), "mode": mode})

    def download_url(self, path: str, *, expires_in_seconds: int = 300) -> str:
        return self._client.signed_file_url(self._sandbox.sandbox_id, path, "download", expires_in_seconds)["url"]

    def upload_url(self, path: str, *, expires_in_seconds: int = 300) -> str:
        return self._client.signed_file_url(self._sandbox.sandbox_id, path, "upload", expires_in_seconds)["url"]

    def watch(self, path: str) -> Iterator[FileInfo]:
        for event in self._client._sandbox_stream(self._sandbox.sandbox_id, self._sandbox.access_token, f"/v1/files/watch?path={urllib.parse.quote(path)}"):
            if event.get("type") == "change" and event.get("file"):
                yield _file_info(event["file"])

    def list(self, path: str = ".") -> list[FileEntry]:
        return [_file_entry(item) for item in self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "GET", f"/v1/files/list?path={urllib.parse.quote(path)}")]

    def get_info(self, path: str) -> FileInfo:
        return _file_info(self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "GET", f"/v1/files/info?path={urllib.parse.quote(path)}"))

    def exists(self, path: str) -> bool:
        try:
            self.get_info(path)
            return True
        except APIError as error:
            if error.status == 404:
                return False
            raise

    def make_dir(self, path: str) -> None:
        self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "POST", f"/v1/files/mkdir?path={urllib.parse.quote(path)}")

    def remove(self, path: str) -> None:
        self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "DELETE", f"/v1/files?path={urllib.parse.quote(path)}")

    def rename(self, old_path: str, new_path: str) -> None:
        self._client._sandbox_request(self._sandbox.sandbox_id, self._sandbox.access_token, "POST", "/v1/files/rename", {"from": old_path, "to": new_path})


class _WebSocket:
    def __init__(self, url: str, sandbox_id: str, access_token: str):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme not in {"ws", "wss"}:
            raise ValueError("PTY URL must use ws:// or wss://")
        port = parsed.port or (443 if parsed.scheme == "wss" else 80)
        raw_socket = socket.create_connection((parsed.hostname, port), timeout=30.0)
        if parsed.scheme == "wss":
            context = ssl.create_default_context()
            raw_socket = context.wrap_socket(raw_socket, server_hostname=parsed.hostname)
        self._socket = raw_socket
        self._send_lock = threading.Lock()
        key = base64.b64encode(secrets.token_bytes(16)).decode()
        host = parsed.hostname if parsed.port is None else f"{parsed.hostname}:{parsed.port}"
        target = urllib.parse.urlunsplit(("", "", parsed.path or "/", parsed.query, ""))
        request = (
            f"GET {target} HTTP/1.1\r\n"
            f"Host: {host}\r\n"
            "Upgrade: websocket\r\n"
            "Connection: Upgrade\r\n"
            f"Sec-WebSocket-Key: {key}\r\n"
            "Sec-WebSocket-Version: 13\r\n"
            "Sec-WebSocket-Protocol: kontinue\r\n"
            f"X-Kontinue-Sandbox-ID: {sandbox_id}\r\n"
            f"X-Kontinue-Token: {access_token}\r\n\r\n"
        ).encode()
        raw_socket.sendall(request)
        response, remainder = self._read_http_headers(raw_socket)
        if not response.startswith(b"HTTP/1.1 101"):
            raise RuntimeError(f"PTY WebSocket handshake failed: {response.splitlines()[0].decode(errors='replace')}")
        expected = base64.b64encode(hashlib.sha1((key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11").encode()).digest()).decode()
        if expected.encode() not in response:
            raise RuntimeError("PTY WebSocket handshake returned an invalid accept key")
        self._buffer = remainder

    @staticmethod
    def _read_http_headers(sock: socket.socket) -> tuple[bytes, bytes]:
        data = bytearray()
        while b"\r\n\r\n" not in data:
            chunk = sock.recv(4096)
            if not chunk:
                raise RuntimeError("PTY WebSocket closed during handshake")
            data.extend(chunk)
        marker = data.index(b"\r\n\r\n") + 4
        return bytes(data[:marker]), bytes(data[marker:])

    def _read_exact(self, size: int) -> bytes:
        while len(self._buffer) < size:
            chunk = self._socket.recv(max(4096, size - len(self._buffer)))
            if not chunk:
                raise EOFError
            self._buffer += chunk
        result, self._buffer = self._buffer[:size], self._buffer[size:]
        return result

    def recv(self) -> tuple[int, bytes]:
        header = self._read_exact(2)
        opcode = header[0] & 0x0F
        length = header[1] & 0x7F
        if length == 126:
            length = struct.unpack("!H", self._read_exact(2))[0]
        elif length == 127:
            length = struct.unpack("!Q", self._read_exact(8))[0]
        if length > 1 << 20:
            raise ValueError("PTY WebSocket frame is too large")
        masked = header[1] & 0x80
        mask = self._read_exact(4) if masked else b""
        payload = bytearray(self._read_exact(length))
        if masked:
            for index in range(length):
                payload[index] ^= mask[index % 4]
        return opcode, bytes(payload)

    def send(self, payload: bytes, opcode: int = 2) -> None:
        length = len(payload)
        mask = secrets.token_bytes(4)
        header = bytearray([0x80 | opcode])
        if length < 126:
            header.append(0x80 | length)
        elif length < 65536:
            header.append(0x80 | 126)
            header.extend(struct.pack("!H", length))
        else:
            header.append(0x80 | 127)
            header.extend(struct.pack("!Q", length))
        body = bytes(byte ^ mask[index % 4] for index, byte in enumerate(payload))
        with self._send_lock:
            self._socket.sendall(bytes(header) + mask + body)

    def close(self) -> None:
        try:
            self.send(b"", opcode=8)
        except (OSError, EOFError):
            pass
        try:
            self._socket.shutdown(socket.SHUT_RDWR)
        except OSError:
            pass
        self._socket.close()


@dataclass
class PtyCreateOptions:
    cmd: str = "/bin/sh"
    args: list[str] | None = None
    envs: dict[str, str] | None = None
    cwd: str | None = None
    cols: int = 80
    rows: int = 24
    timeout_ms: int | None = None
    on_data: Callable[[str | bytes], None] | None = None
    on_exit: Callable[[int | None], None] | None = None


class PtySession:
    def __init__(self, websocket: _WebSocket, options: PtyCreateOptions):
        self._websocket = websocket
        self._options = options
        self.pid: int | None = None
        self.exit_code: int | None = None
        self._ready = threading.Event()
        self._done = threading.Event()
        self._closed = False
        self._reader = threading.Thread(target=self._read_loop, daemon=True)
        self._reader.start()
        if not self._ready.wait(30):
            self.close()
            raise TimeoutError("PTY WebSocket did not become ready")

    def _read_loop(self) -> None:
        try:
            while True:
                opcode, payload = self._websocket.recv()
                if opcode == 8:
                    return
                if opcode == 9:
                    self._websocket.send(payload, opcode=10)
                    continue
                if opcode not in {1, 2}:
                    continue
                text = payload.decode(errors="replace")
                try:
                    message = json.loads(text)
                except json.JSONDecodeError:
                    message = None
                if isinstance(message, dict) and message.get("type") == "ready":
                    self.pid = int(message["pid"])
                    self._ready.set()
                    continue
                if isinstance(message, dict) and message.get("type") == "exit":
                    self.exit_code = int(message.get("exitCode", 0))
                    if self._options.on_exit:
                        self._options.on_exit(self.exit_code)
                    continue
                if self._options.on_data:
                    self._options.on_data(text if opcode == 1 else payload)
        except (EOFError, OSError, ValueError):
            pass
        finally:
            self._done.set()
            if self._options.on_exit and self.exit_code is None:
                self._options.on_exit(None)

    def send_input(self, data: str | bytes) -> None:
        self._websocket.send(data.encode() if isinstance(data, str) else data)

    def resize(self, cols: int, rows: int) -> None:
        self._websocket.send(json.dumps({"type": "resize", "cols": cols, "rows": rows}).encode())

    def wait(self, timeout: float | None = None) -> int | None:
        self._done.wait(timeout)
        return self.exit_code

    def kill(self) -> None:
        self._websocket.send(json.dumps({"type": "kill"}).encode())
        self.close()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        self._websocket.close()
        self._done.set()


class Pty:
    def __init__(self, client: Client, sandbox: "Sandbox"):
        self._client = client
        self._sandbox = sandbox
        self._sessions: dict[int, PtySession] = {}

    def create(self, options: PtyCreateOptions | None = None, **kwargs: Any) -> PtySession:
        active = options or PtyCreateOptions(**kwargs)
        return self._open(active)

    def _open(self, options: PtyCreateOptions, pid: int | None = None) -> PtySession:
        params = {
            "cmd": options.cmd,
            "args": json.dumps(options.args or []),
            "cols": options.cols,
            "rows": options.rows,
        }
        if pid is not None:
            params = {"pid": pid}
        elif options.cwd:
            params["cwd"] = options.cwd
        if pid is None and options.envs:
            params["envs"] = json.dumps(options.envs)
        if pid is None and options.timeout_ms is not None:
            params["timeoutMs"] = options.timeout_ms
        query = urllib.parse.urlencode(params)
        base = self._client.gateway_url
        parsed = urllib.parse.urlsplit(base)
        scheme = "wss" if parsed.scheme == "https" else "ws"
        url = urllib.parse.urlunsplit((scheme, parsed.netloc, "/v1/pty", query, ""))
        session = PtySession(_WebSocket(url, self._sandbox.sandbox_id, self._sandbox.access_token), options)
        if session.pid is not None:
            self._sessions[session.pid] = session
        return session

    def run(self, cmd: str, **options: Any) -> PtySession:
        return self.create(PtyCreateOptions(cmd="/bin/sh", args=["-c", cmd], **options))

    def connect(self, pid: int, options: PtyCreateOptions | None = None) -> PtySession:
        session = self._sessions.get(pid)
        if session is not None and not session._done.is_set():
            return session
        return self._open(options or PtyCreateOptions(), pid)

    def send_input(self, pid: int, data: str | bytes) -> None:
        self.connect(pid).send_input(data)

    def resize(self, pid: int, cols: int, rows: int) -> None:
        self.connect(pid).resize(cols, rows)

    def kill(self, pid: int) -> None:
        self.connect(pid).kill()
        self._sessions.pop(pid, None)


class Sandbox:
    def __init__(self, client: Client, info: dict[str, Any]):
        self._client = client
        self._info = dict(info)
        self.commands = Commands(client, self)
        self.files = Filesystem(client, self)
        self.pty = Pty(client, self)

    def _replace_info(self, next_info: dict[str, Any]) -> None:
        self._info = {**self._info, **next_info, "accessToken": next_info.get("accessToken", self.access_token)}

    @classmethod
    def create(cls, template: str | dict[str, Any] | None = None, *, client: Client | None = None, **options: Any) -> "Sandbox":
        active_client = client or Client()
        return cls(active_client, active_client._request("POST", "/v1/sandboxes", _create_payload(template, options)))

    @classmethod
    def connect(cls, sandbox_id: str, *, client: Client | None = None) -> "Sandbox":
        active_client = client or Client()
        return cls(active_client, active_client.connect(sandbox_id))

    @classmethod
    def list(cls, *, client: Client | None = None, **options: Any) -> list["Sandbox"]:
        active_client = client or Client()
        page = active_client.list_sandboxes(
            state=options.get("state", ""),
            template_id=options.get("template_id", ""),
            cursor=options.get("cursor", ""),
            limit=options.get("limit", 100),
        )
        return [cls(active_client, info) for info in page.get("sandboxes", [])]

    @property
    def sandbox_id(self) -> str:
        return self._info["sandboxId"]

    @property
    def sandbox_domain(self) -> str:
        return self.get_host(49983)

    @property
    def access_token(self) -> str:
        return self._info.get("accessToken", "")

    @property
    def state(self) -> str:
        return self._info.get("state", "")

    @property
    def template_id(self) -> str | None:
        return self._info.get("templateId")

    @property
    def metadata(self) -> dict[str, str]:
        return self._info.get("metadata", {})

    def get_info(self) -> dict[str, Any]:
        self._replace_info(self._client._request("GET", f"/v1/sandboxes/{urllib.parse.quote(self.sandbox_id)}"))
        return dict(self._info)

    def kill(self) -> None:
        self._client.delete_sandbox(self.sandbox_id)

    def set_timeout(self, timeout: float) -> None:
        self._replace_info(self._client.set_sandbox_timeout(self.sandbox_id, max(1, math.ceil(timeout * 1000))))

    def pause(self) -> None:
        self._client.suspend(self.sandbox_id)

    def beta_pause(self) -> None:
        self.pause()

    def resume(self) -> "Sandbox":
        self._replace_info(self._client.resume(self.sandbox_id))
        return self

    def get_host(self, port: int = 49983) -> str:
        parsed = urllib.parse.urlsplit(self._client.gateway_url)
        if parsed.hostname not in {"127.0.0.1", "localhost"}:
            return urllib.parse.urlunsplit((parsed.scheme, f"{port}-{self.sandbox_id}.{parsed.hostname}", parsed.path, parsed.query, parsed.fragment)).rstrip("/")
        return f"{self._client.gateway_url}/{port}-{urllib.parse.quote(self.sandbox_id)}"

    def __enter__(self) -> "Sandbox":
        return self

    def __exit__(self, _exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        self.kill()


class AsyncCommands:
    def __init__(self, sandbox: "AsyncSandbox"):
        self._sandbox = sandbox

    async def run(self, cmd: str, **options: Any) -> CommandResult | "AsyncCommandHandle":
        result = await asyncio.to_thread(self._sandbox._sync.commands.run, cmd, **options)
        if isinstance(result, CommandHandle):
            return AsyncCommandHandle(result)
        return result

    async def list(self) -> list[ProcessInfo]:
        return await asyncio.to_thread(self._sandbox._sync.commands.list)

    async def connect(self, pid: int) -> "AsyncCommandHandle":
        return AsyncCommandHandle(self._sandbox._sync.commands.connect(pid))

    async def kill(self, pid: int) -> None:
        await asyncio.to_thread(self._sandbox._sync.commands.kill, pid)


class AsyncCommandHandle:
    def __init__(self, handle: CommandHandle):
        self._handle = handle
        self.pid = handle.pid

    async def wait(self, poll_interval: float = 0.1) -> CommandResult:
        return await asyncio.to_thread(self._handle.wait, poll_interval)

    async def kill(self) -> None:
        await asyncio.to_thread(self._handle.kill)


class AsyncFilesystem:
    def __init__(self, sandbox: "AsyncSandbox"):
        self._sandbox = sandbox

    async def read(self, path: str, **options: Any) -> str | bytes:
        return await asyncio.to_thread(self._sandbox._sync.files.read, path, **options)

    async def write(self, path: str, data: str | bytes, **options: Any) -> None:
        await asyncio.to_thread(self._sandbox._sync.files.write, path, data, **options)

    async def list(self, path: str = ".") -> list[FileEntry]:
        return await asyncio.to_thread(self._sandbox._sync.files.list, path)

    async def get_info(self, path: str) -> FileInfo:
        return await asyncio.to_thread(self._sandbox._sync.files.get_info, path)

    async def exists(self, path: str) -> bool:
        return await asyncio.to_thread(self._sandbox._sync.files.exists, path)

    async def make_dir(self, path: str) -> None:
        await asyncio.to_thread(self._sandbox._sync.files.make_dir, path)

    async def remove(self, path: str) -> None:
        await asyncio.to_thread(self._sandbox._sync.files.remove, path)

    async def rename(self, old_path: str, new_path: str) -> None:
        await asyncio.to_thread(self._sandbox._sync.files.rename, old_path, new_path)

    async def watch(self, path: str):
        iterator = self._sandbox._sync.files.watch(path)
        while True:
            item = await asyncio.to_thread(lambda: next(iterator, None))
            if item is None:
                return
            yield item


class AsyncPty:
    def __init__(self, sandbox: "AsyncSandbox"):
        self._sandbox = sandbox

    async def create(self, options: PtyCreateOptions | None = None, **kwargs: Any) -> "AsyncPtySession":
        session = await asyncio.to_thread(self._sandbox._sync.pty.create, options, **kwargs)
        return AsyncPtySession(session)

    async def run(self, cmd: str, **options: Any) -> "AsyncPtySession":
        session = await asyncio.to_thread(self._sandbox._sync.pty.run, cmd, **options)
        return AsyncPtySession(session)

    async def connect(self, pid: int, options: PtyCreateOptions | None = None) -> "AsyncPtySession":
        session = await asyncio.to_thread(self._sandbox._sync.pty.connect, pid, options)
        return AsyncPtySession(session)

    async def send_input(self, pid: int, data: str | bytes) -> None:
        await asyncio.to_thread(self._sandbox._sync.pty.send_input, pid, data)

    async def resize(self, pid: int, cols: int, rows: int) -> None:
        await asyncio.to_thread(self._sandbox._sync.pty.resize, pid, cols, rows)

    async def kill(self, pid: int) -> None:
        await asyncio.to_thread(self._sandbox._sync.pty.kill, pid)


class AsyncPtySession:
    def __init__(self, session: PtySession):
        self._session = session
        self.pid = session.pid

    async def send_input(self, data: str | bytes) -> None:
        await asyncio.to_thread(self._session.send_input, data)

    async def resize(self, cols: int, rows: int) -> None:
        await asyncio.to_thread(self._session.resize, cols, rows)

    async def wait(self, timeout: float | None = None) -> int | None:
        return await asyncio.to_thread(self._session.wait, timeout)

    async def kill(self) -> None:
        await asyncio.to_thread(self._session.kill)

    async def close(self) -> None:
        await asyncio.to_thread(self._session.close)


class AsyncSandbox:
    def __init__(self, sync_sandbox: Sandbox):
        self._sync = sync_sandbox
        self.commands = AsyncCommands(self)
        self.files = AsyncFilesystem(self)
        self.pty = AsyncPty(self)

    @classmethod
    async def create(cls, template: str | dict[str, Any] | None = None, *, client: Client | None = None, **options: Any) -> "AsyncSandbox":
        return cls(await asyncio.to_thread(Sandbox.create, template, client=client, **options))

    @classmethod
    async def connect(cls, sandbox_id: str, *, client: Client | None = None) -> "AsyncSandbox":
        return cls(await asyncio.to_thread(Sandbox.connect, sandbox_id, client=client))

    @property
    def sandbox_id(self) -> str:
        return self._sync.sandbox_id

    @property
    def sandbox_domain(self) -> str:
        return self._sync.sandbox_domain

    @property
    def access_token(self) -> str:
        return self._sync.access_token

    async def get_info(self) -> dict[str, Any]:
        return await asyncio.to_thread(self._sync.get_info)

    async def kill(self) -> None:
        await asyncio.to_thread(self._sync.kill)

    async def set_timeout(self, timeout: float) -> None:
        await asyncio.to_thread(self._sync.set_timeout, timeout)

    async def pause(self) -> None:
        await asyncio.to_thread(self._sync.pause)

    async def resume(self) -> "AsyncSandbox":
        await asyncio.to_thread(self._sync.resume)
        return self

    async def __aenter__(self) -> "AsyncSandbox":
        return self

    async def __aexit__(self, _exc_type: Any, _exc_value: Any, _traceback: Any) -> None:
        await self.kill()


class _SandboxResource:
    def __init__(self, client: Client):
        self._client = client

    def create(self, template: str | dict[str, Any] | None = None, **options: Any) -> Sandbox:
        return Sandbox.create(template, client=self._client, **options)

    def connect(self, sandbox_id: str) -> Sandbox:
        return Sandbox.connect(sandbox_id, client=self._client)

    def list(self, **options: Any) -> list[Sandbox]:
        return Sandbox.list(client=self._client, **options)


class _VolumeResource:
    def __init__(self, client: Client):
        self._client = client

    def create(self, **options: Any) -> dict[str, Any]:
        return self._client.create_volume(**options)

    def list(self) -> list[dict[str, Any]]:
        return self._client.list_volumes()


class _TemplateResource:
    def __init__(self, client: Client):
        self._client = client

    def list(self) -> list[dict[str, Any]]:
        return self._client.list_templates()

    def get(self, template_id: str) -> dict[str, Any]:
        return self._client.get_template(template_id)

    def exists(self, template_id: str) -> bool:
        try:
            self.get(template_id)
            return True
        except APIError as error:
            if error.status == 404:
                return False
            raise


class E2B:
    """Explicit client whose resource names mirror the E2B SDK."""

    def __init__(self, api_key: str | None = None, domain: str | None = None, **options: Any):
        base_url = options.pop("base_url", None) or domain
        gateway_url = options.pop("gateway_url", None) or domain
        client = Client(api_key=api_key, base_url=base_url, gateway_url=gateway_url, **options)
        self.client = client
        self.Sandbox = _SandboxResource(client)
        self.Volume = _VolumeResource(client)
        self.Template = _TemplateResource(client)


def _command_result(value: dict[str, Any]) -> CommandResult:
    return CommandResult(
        pid=int(value.get("pid", 0)),
        stdout=value.get("stdout", ""),
        stderr=value.get("stderr", ""),
        exit_code=int(value.get("exitCode", 0)),
        error=value.get("error"),
        timed_out=bool(value.get("timedOut", False)),
        duration_ms=int(value.get("durationMs", 0)),
        running=bool(value.get("running", False)),
    )


def _process_info(value: dict[str, Any]) -> ProcessInfo:
    return ProcessInfo(
        pid=int(value.get("pid", 0)),
        command=list(value.get("command", [])),
        started_at_unix_ms=int(value.get("startedAtUnixMs", 0)),
        running=bool(value.get("running", False)),
        exit_code=int(value.get("exitCode", 0)),
    )


def _file_info(value: dict[str, Any]) -> FileInfo:
    return FileInfo(
        path=value.get("path", ""),
        is_dir=bool(value.get("isDir", False)),
        is_file=bool(value.get("isFile", False)),
        size=int(value.get("size", 0)),
        modified_at_unix_ms=value.get("modifiedAtUnixMs"),
    )


def _file_entry(value: dict[str, Any]) -> FileEntry:
    return FileEntry(
        name=value.get("name", ""),
        path=value.get("path", ""),
        is_dir=bool(value.get("isDir", False)),
        is_file=bool(value.get("isFile", False)),
        size=int(value.get("size", 0)),
    )

