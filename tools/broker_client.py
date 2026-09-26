"""Shared connect/send-line/read-line logic for talking to a running
jobqueue broker over its newline-delimited JSON protocol. Used by both
send.py (manual one-shot testing) and integration_check.py (automated
lifecycle test).
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
