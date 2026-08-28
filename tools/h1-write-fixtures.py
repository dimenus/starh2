#!/usr/bin/env python3
"""Write testdata/h1 fixtures with explicit \\r\\n escapes. Verify bytes after write."""
from pathlib import Path


def write(root: Path, cat: str, name: str, status, conn: str, request: str, body=None) -> None:
    d = root / cat
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"{name}.txn"
    lines = [f"status: {status}", f"connection: {conn}"]
    if body is not None:
        lines.append(f"body: {body}")
    lines.append("")
    escaped = request.replace("\r", "\\r").replace("\n", "\\n")
    lines.append(escaped)
    p.write_text("\n".join(lines) + "\n", encoding="utf-8")
    text = p.read_text(encoding="utf-8")
    req = text.split("\n\n", 1)[1].rstrip("\n")
    raw = req.encode("utf-8").decode("unicode_escape").encode("latin1")
    if status != "none" and b"\n" not in raw and b"GET /\\r\\n" not in request.encode():
        raise SystemExit(f"fixture {p} has no line break after unescape: {raw!r}")


def main() -> None:
    root = Path("testdata/h1")
    write(root, "accept", "get_simple", 200, "keep", "GET / HTTP/1.1\r\nHost: example.test\r\n\r\n", "ok")
    print("wrote", root)
    n = len(list(root.rglob("*.txn")))
    if n == 0:
        raise SystemExit("wrote zero fixtures")
    print("fixtures", n)


if __name__ == "__main__":
    main()
