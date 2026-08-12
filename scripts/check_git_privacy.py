#!/usr/bin/env python3
"""Reject committed wallet identifiers, key material, and common secrets.

The scanner deliberately reports only the category and location of a match. It
never prints the matched value. Use ``--staged`` from a pre-commit hook,
``--commit-message`` from a commit-msg hook, and ``--history`` in CI.
"""

from __future__ import annotations

import argparse
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable


@dataclass(frozen=True)
class Finding:
    source: str
    line: int
    category: str


PATTERNS: tuple[tuple[str, re.Pattern[bytes]], ...] = (
    (
        "literal EVM/Hyperliquid address",
        re.compile(rb"(?<![0-9A-Fa-f])0x[0-9A-Fa-f]{40}(?![0-9A-Fa-f])"),
    ),
    (
        "literal 32-byte hex value (key/hash candidate)",
        re.compile(rb"(?<![0-9A-Fa-f])(?:0x)?[0-9A-Fa-f]{64}(?![0-9A-Fa-f])"),
    ),
    (
        "literal encoded public-key candidate",
        re.compile(
            rb"(?<![0-9A-Fa-f])(?:0x)?(?:[0-9A-Fa-f]{66}|[0-9A-Fa-f]{128}|[0-9A-Fa-f]{130})(?![0-9A-Fa-f])"
        ),
    ),
    (
        "PEM key material",
        re.compile(b"-----" + b"BEGIN (?:[A-Z0-9 ]+ )?(?:PRIVATE|PUBLIC) KEY-----"),
    ),
    (
        "OpenSSH public key",
        re.compile(b"ssh-" + b"(?:rsa|ed25519)|ecdsa-" + b"sha2-"),
    ),
    (
        "Telegram bot token candidate",
        re.compile(rb"(?<![0-9])\d{6,12}:[A-Za-z0-9_-]{30,}(?![A-Za-z0-9_-])"),
    ),
    (
        "GitHub token candidate",
        re.compile(b"gh" + rb"[opsu]_[A-Za-z0-9]{30,}"),
    ),
    (
        "AWS access-key candidate",
        re.compile(b"(?:AKIA|ASIA)" + rb"[A-Z0-9]{16}"),
    ),
)


def git(*args: str, input_data: bytes | None = None) -> bytes:
    return subprocess.run(
        ["git", *args],
        input=input_data,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        check=True,
    ).stdout


def scan(source: str, data: bytes) -> list[Finding]:
    if b"\x00" in data:
        return []
    findings: list[Finding] = []
    for category, pattern in PATTERNS:
        for match in pattern.finditer(data):
            findings.append(
                Finding(
                    source=source,
                    line=data.count(b"\n", 0, match.start()) + 1,
                    category=category,
                )
            )
    return findings


def staged_blobs() -> Iterable[tuple[str, bytes]]:
    paths = git(
        "diff",
        "--cached",
        "--name-only",
        "--diff-filter=ACMR",
        "-z",
    ).split(b"\x00")
    for raw_path in paths:
        if not raw_path:
            continue
        path = raw_path.decode("utf-8", "surrogateescape")
        yield path, git("show", f":{path}")


def tracked_blobs() -> Iterable[tuple[str, bytes]]:
    for raw_path in git("ls-files", "-z").split(b"\x00"):
        if not raw_path:
            continue
        path = raw_path.decode("utf-8", "surrogateescape")
        yield path, Path(path).read_bytes()


def history_blobs() -> Iterable[tuple[str, bytes]]:
    seen: set[str] = set()
    for line in git("rev-list", "--objects", "--all").splitlines():
        fields = line.split(b" ", 1)
        oid = fields[0].decode("ascii")
        if oid in seen or git("cat-file", "-t", oid).strip() != b"blob":
            continue
        seen.add(oid)
        path = (
            fields[1].decode("utf-8", "surrogateescape")
            if len(fields) == 2
            else "<unknown>"
        )
        yield f"{path}@{oid[:12]}", git("cat-file", "blob", oid)


def history_messages() -> Iterable[tuple[str, bytes]]:
    for raw_oid in git("rev-list", "--all").splitlines():
        oid = raw_oid.decode("ascii")
        yield f"commit-message@{oid[:12]}", git("show", "-s", "--format=%B", oid)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--staged", action="store_true", help="scan staged blobs")
    mode.add_argument("--history", action="store_true", help="scan all reachable Git history")
    mode.add_argument("--commit-message", type=Path, help="scan one commit-message file")
    args = parser.parse_args()

    if args.commit_message:
        sources: Iterable[tuple[str, bytes]] = (
            (str(args.commit_message), args.commit_message.read_bytes()),
        )
    elif args.history:
        sources = (*history_blobs(), *history_messages())
    elif args.staged:
        sources = staged_blobs()
    else:
        sources = tracked_blobs()

    findings: list[Finding] = []
    scanned = 0
    for source, data in sources:
        scanned += 1
        findings.extend(scan(source, data))

    if findings:
        print("privacy scan failed; sensitive values are not displayed", file=sys.stderr)
        for finding in sorted(set(findings), key=lambda item: (item.source, item.line, item.category)):
            print(
                f"- {finding.source}:{finding.line}: {finding.category}",
                file=sys.stderr,
            )
        return 1

    print(f"privacy scan passed ({scanned} sources)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
