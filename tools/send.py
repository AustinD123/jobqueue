#!/usr/bin/env python3
"""Send one JSON line to a running jobqueue broker and print the response.

Usage:
    python3 tools/send.py '{"cmd":"enqueue","queue":"default","payload":"hi","priority":0}'
    python3 tools/send.py '{"cmd":"claim","queue":"default"}' 9001   # non-default port
"""
import sys

from broker_client import send_line

DEFAULT_PORT = 9000


def main() -> int:
    if len(sys.argv) < 2:
        print(f"usage: {sys.argv[0]} '<json line>' [port]", file=sys.stderr)
        return 1

    message = sys.argv[1]
    port = int(sys.argv[2]) if len(sys.argv) > 2 else DEFAULT_PORT

    print(send_line(port, message))
    return 0


if __name__ == "__main__":
    sys.exit(main())
