"""Simple logger — timestamps to stdout, errors to stderr."""

import sys
from datetime import datetime


def log(msg: str, level: str = "info"):
    ts = datetime.utcnow().strftime("%H:%M:%S")
    prefix = {"info": "  ", "warn": "⚠ ", "error": "✗ "}.get(level, "  ")
    line = f"[{ts}] {prefix}{msg}"
    if level == "error":
        print(line, file=sys.stderr)
    else:
        print(line)