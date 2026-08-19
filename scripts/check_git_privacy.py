#!/usr/bin/env python3
"""Reject committed wallet identifiers, key material, and common secrets.

The scanner deliberately reports only the category and location of a match. It
never prints the matched value. Use ``--staged`` from a pre-commit hook,
``--commit-message`` from a commit-msg hook, and ``--history`` in CI.
"""

from __future__ import annotations

import argparse
import ipaddress
import re
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from urllib.parse import urlsplit


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
    (
        "URL containing embedded credentials",
        re.compile(
            rb"[A-Za-z][A-Za-z0-9+.-]*" + b"://" + rb"[^\s:/@]+:[^\s/@]+@"
        ),
    ),
)


URL_PATTERN = re.compile(rb"(?:https?|wss?)://[^\s\"'<>`)]+", re.IGNORECASE)
ALLOWED_URL_HOSTS = {
    "api.binance.com",
    "api.hyperliquid-testnet.xyz",
    "api.hyperliquid.xyz",
    "api.telegram.org",
    "backend",
    "developers.binance.com",
    "fapi.binance.com",
    "frontend",
    "fstream.binance.com",
    "get.docker.com",
    "github.com",
    "hyperliquid.gitbook.io",
    "localhost",
    "nextjs.org",
    "raw.githubusercontent.com",
    "registry.npmjs.org",
    "stream.binancefuture.com",
    "testnet.binancefuture.com",
}
LOCAL_SECRET_KEY_PATTERN = re.compile(
    r"(?:PASSWORD|PASSWD|PRIVATE_KEY|SECRET|TOKEN|API_KEY|API_SECRET|"
    r"ACCOUNT_ADDRESS|SUBACCOUNT|VAULT_ADDRESS|ADMIN_EMAIL|ALLOWED_USER_IDS|"
    r"DATABASE_URL)",
    re.IGNORECASE,
)
SENSITIVE_ASSIGNMENT_KEYS = {
    "ADMIN_PASSWORD_BOOTSTRAP",
    "APP_SECRET_KEY",
    "BINANCE_API_KEY",
    "BINANCE_API_SECRET",
    "ENCRYPTION_MASTER_KEY",
    "HYPERLIQUID_PRIVATE_KEY",
    "HYPERLIQUID_SIGNER_PRIVATE_KEY",
    "POSTGRES_PASSWORD",
    "TELEGRAM_CONTROL_BOT_TOKEN",
}


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
    # Lockfiles contain package-registry and maintainer funding URLs generated
    # by the package manager. They are not runtime frontend configuration.
    source_path = source.split("@", 1)[0]
    url_matches = (
        ()
        if source_path.endswith(("package-lock.json", "yarn.lock", "pnpm-lock.yaml"))
        else URL_PATTERN.finditer(data)
    )
    for match in url_matches:
        raw_url = match.group(0).decode("utf-8", "replace").rstrip(".,;:")
        if any(marker in raw_url for marker in ("<", ">", "{", "}", "$")):
            continue
        try:
            host = (urlsplit(raw_url).hostname or "").lower()
        except ValueError:
            host = ""
        if _url_host_allowed(host):
            continue
        findings.append(
            Finding(
                source=source,
                line=data.count(b"\n", 0, match.start()) + 1,
                category="unapproved public URL (possible frontend endpoint)",
            )
        )
    findings.extend(_literal_sensitive_assignment_findings(source, data))
    return findings


def _url_host_allowed(host: str) -> bool:
    if host in ALLOWED_URL_HOSTS:
        return True
    if host == "hyperdash.com" or host.endswith(".hyperdash.com"):
        return True
    if host == "example.com" or host.endswith((".example", ".invalid", ".test")):
        return True
    try:
        return ipaddress.ip_address(host).is_loopback
    except ValueError:
        return False


def _literal_sensitive_assignment_findings(source: str, data: bytes) -> list[Finding]:
    findings: list[Finding] = []
    for line_number, raw_line in enumerate(data.splitlines(), start=1):
        line = raw_line.decode("utf-8", "replace").strip()
        if not line or line.startswith("#"):
            continue
        match = re.match(
            r"[\"']?([A-Za-z_][A-Za-z0-9_]*)[\"']?"
            r"\s*(?::\s*[^=]+)?=\s*(.+)$",
            line,
        )
        if match is None:
            match = re.match(
                r"[\"']?([A-Za-z_][A-Za-z0-9_]*)[\"']?\s*:\s*(.+)$",
                line,
            )
        if not match or match.group(1).upper() not in SENSITIVE_ASSIGNMENT_KEYS:
            continue
        value = match.group(2).strip().rstrip(",").strip().strip("\"").strip("'")
        lower = value.lower()
        if (
            not value
            or value.startswith(("${", "<"))
            or lower in {"none", "null"}
            or any(marker in lower for marker in ("change-me", "changeme", "placeholder", "example", "dummy", "redacted"))
            or (
                "/tests/" in f"/{source}"
                and (
                    len(value) < 32
                    or any(marker in lower for marker in ("secret", "token", "test"))
                )
            )
        ):
            continue
        findings.append(
            Finding(
                source=source,
                line=line_number,
                category=f"literal sensitive configuration value for {match.group(1).upper()}",
            )
        )
    return findings


def local_sensitive_values() -> list[tuple[str, bytes]]:
    """Load real local values for equality checks without ever printing them."""

    env_path = Path(".env")
    if not env_path.exists():
        return []
    values: list[tuple[str, bytes]] = []
    for raw_line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, raw_value = line.split("=", 1)
        if not LOCAL_SECRET_KEY_PATTERN.search(key):
            continue
        value = raw_value.strip().strip("\"").strip("'")
        if len(value) >= 8:
            values.append((key.strip(), value.encode("utf-8")))
        if key.strip().upper() == "DATABASE_URL":
            try:
                password = urlsplit(value).password or ""
            except ValueError:
                password = ""
            if len(password) >= 8:
                values.append(("DATABASE_URL_PASSWORD", password.encode("utf-8")))
    return values


def local_value_findings(
    sources: Iterable[tuple[str, bytes]],
) -> list[Finding]:
    local_values = local_sensitive_values()
    if not local_values:
        return []
    findings: list[Finding] = []
    for source, data in sources:
        if b"\x00" in data:
            continue
        for key, value in local_values:
            offset = data.find(value)
            if offset < 0:
                continue
            findings.append(
                Finding(
                    source=source,
                    line=data.count(b"\n", 0, offset) + 1,
                    category=f"matches ignored local sensitive setting {key}",
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


def worktree_blobs() -> Iterable[tuple[str, bytes]]:
    """Scan tracked and not-ignored untracked files before they are staged."""

    for raw_path in git(
        "ls-files",
        "--cached",
        "--others",
        "--exclude-standard",
        "-z",
    ).split(b"\x00"):
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
        sources = worktree_blobs()

    source_rows = list(sources)
    findings: list[Finding] = []
    scanned = 0
    for source, data in source_rows:
        scanned += 1
        findings.extend(scan(source, data))
    # The ignored production .env exists only on the deployment host.  This
    # equality check complements format-based CI scanning and reports key names
    # and locations only, never the local value.
    if not args.history and not args.commit_message:
        findings.extend(local_value_findings(source_rows))

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
