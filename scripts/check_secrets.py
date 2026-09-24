"""
Scans the git repository for committed credentials.

Checks every file tracked or staged by git (and, with --history, every
version of every file ever committed) for:
  - GCP service account key JSON (type service_account, private_key_id)
  - ADC user credentials JSON (refresh_token)
  - PEM private keys
  - Google API keys (AIza...), AWS access key IDs (AKIA...)
  - files that must never be tracked: .env, service-account-key.json

  python scripts/check_secrets.py            # working tree (tracked + staged files)
  python scripts/check_secrets.py --history  # also every past commit

Exits 1 if anything is found, 0 otherwise (usable as a pre-commit hook or CI step).
"""
import argparse
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent

PATTERNS = {
    "GCP service account key": re.compile(r'"type"\s*:\s*"service_account"'),
    "GCP private key id": re.compile(r'"private_key_id"\s*:\s*"[0-9a-f]{20,}"'),
    "ADC user refresh token": re.compile(r'"refresh_token"\s*:\s*"1//[\w-]{20,}"'),
    "PEM private key": re.compile(r"-----BEGIN (?:RSA |EC |OPENSSH |ENCRYPTED )?PRIVATE KEY-----"),
    "Google API key": re.compile(r"\bAIza[0-9A-Za-z_-]{35}\b"),
    "AWS access key id": re.compile(r"\b(?:AKIA|ASIA)[0-9A-Z]{16}\b"),
}

FORBIDDEN_FILES = re.compile(r"(^|/)(\.env|service-account-key\.json|application_default_credentials\.json)$")


def git(*args: str) -> str:
    return subprocess.run(["git", *args], cwd=REPO_ROOT, capture_output=True,
                          text=True, encoding="utf-8", errors="replace", check=True).stdout


def scan_text(text: str) -> list[tuple[int, str]]:
    """Returns (line number, pattern name) for every match in text."""
    hits = []
    for lineno, line in enumerate(text.splitlines(), start=1):
        for name, pattern in PATTERNS.items():
            if pattern.search(line):
                hits.append((lineno, name))
    return hits


def scan_working_tree() -> list[str]:
    findings = []
    files = set(git("ls-files").splitlines()) | set(git("diff", "--cached", "--name-only").splitlines())
    for rel in sorted(files):
        if FORBIDDEN_FILES.search(rel):
            findings.append(f"{rel}: file must never be tracked")
        path = REPO_ROOT / rel
        if not path.is_file():
            continue
        try:
            text = path.read_text(encoding="utf-8")
        except UnicodeDecodeError:
            continue  # binary file (e.g. parquet), not a text credential
        findings += [f"{rel}:{lineno}: {name}" for lineno, name in scan_text(text)]
    return findings


def scan_history() -> list[str]:
    findings = []
    for name in git("log", "--all", "--name-only", "--format=").splitlines():
        if name and FORBIDDEN_FILES.search(name):
            findings.append(f"history: {name} was committed at some point")
    for lineno, name in scan_text(git("log", "--all", "-p", "--format=commit %H")):
        findings.append(f"history (git log -p line {lineno}): {name}")
    return sorted(set(findings))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Fail if credentials are committed")
    parser.add_argument("--history", action="store_true", help="also scan every past commit")
    args = parser.parse_args()

    findings = scan_working_tree()
    if args.history:
        findings += scan_history()

    if findings:
        for f in findings:
            print(f"[secret] {f}")
        print(f"[fail] {len(findings)} potential credential(s) found")
        sys.exit(1)
    print("[ok] no credentials found" + (" (working tree + history)" if args.history else ""))
