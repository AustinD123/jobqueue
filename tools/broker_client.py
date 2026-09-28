"""Shared connect/send-line/read-line logic for talking to a running
jobqueue broker over its newline-delimited JSON protocol. Used by
send.py (manual one-shot testing), integration_check.py (automated
lifecycle test), and crash_test.py/depth_bench.py (one-shot per call is
fine at their volumes -- for anything doing many calls back to back,
e.g. depth_bench.py's claim-timing loop, use PersistentConnection
instead so the timing measures request/response latency, not repeated
TCP connection setup).
"""
import socket


def send_line(port: int, line: str, host: str = "localhost", timeout: float = 5.0) -> str:
    """Open a connection, send one line (newline appended), read one
    newline-terminated response line, close, and return it (without the
    trailing newline). Raises on connection failure or timeout.
    """
    with socket.create_connection((host, port), timeout=timeout) as sock:
        sock.sendall((line + "\n").encode("utf-8"))
        sock.settimeout(timeout)
        response = b""
        while not response.endswith(b"\n"):
            chunk = sock.recv(4096)
            if not chunk:
                break
            response += chunk
    return response.decode("utf-8").rstrip("\n")


class PersistentConnection:
    """One socket reused across many send_request() calls -- the Python
    mirror of ClientConnection in include/jobqueue/client_conn.hpp. Use
    this instead of send_line() whenever timing or volume matters (many
    calls back to back), since send_line() pays a fresh TCP handshake on
    every single call.
    """

    def __init__(self, port: int, host: str = "localhost", timeout: float = 5.0):
        self._sock = socket.create_connection((host, port), timeout=timeout)
        self._buffer = b""

    def send_request(self, line: str) -> str:
        self._sock.sendall((line + "\n").encode("utf-8"))
        while b"\n" not in self._buffer:
            chunk = self._sock.recv(4096)
            if not chunk:
                raise ConnectionError("connection closed before a full response line was received")
            self._buffer += chunk
        newline = self._buffer.index(b"\n")
        line_out, self._buffer = self._buffer[:newline], self._buffer[newline + 1:]
        return line_out.decode("utf-8")

    def close(self) -> None:
        self._sock.close()

    def __enter__(self) -> "PersistentConnection":
        return self

    def __exit__(self, *_exc) -> None:
        self.close()
