import json
import re
import sys
from pathlib import Path

BLOCKED_PATTERNS = [
    re.compile(r"AGPL", re.IGNORECASE),
    re.compile(r"(^|[^A-Z])GPL([^A-Z]|$)", re.IGNORECASE),
]
ALLOWED_GPL_EXCEPTIONS = [
    "LGPL",
    "GCC-EXCEPTION",
    "GCC EXCEPTION",
]


def is_blocked(license_text: str) -> bool:
    if not license_text:
        return False
    upper = license_text.upper()
    if any(exc in upper for exc in ALLOWED_GPL_EXCEPTIONS):
        return False
    return any(pattern.search(upper) for pattern in BLOCKED_PATTERNS)


def main() -> int:
    report_path = Path(sys.argv[1]) if len(sys.argv) > 1 else Path("build/licenses.json")
    if not report_path.exists():
        print(f"License report file not found: {report_path}")
        return 2

    entries = json.loads(report_path.read_text(encoding="utf-8"))

    blocked = []
    unknown = []
    for entry in entries:
        name = entry.get("Name") or entry.get("name") or "unknown"
        license_name = entry.get("License") or entry.get("license") or ""
        if not str(license_name).strip() or str(license_name).strip().lower() in {"unknown", "n/a"}:
            unknown.append((name, license_name))
            continue
        if is_blocked(str(license_name)):
            blocked.append((name, license_name))

    if blocked:
        print("Blocked licenses detected:")
        for name, license_name in blocked:
            print(f"- {name}: {license_name}")
        return 1

    if unknown:
        print("Packages with unknown license metadata (manual review recommended):")
        for name, license_name in unknown:
            print(f"- {name}: {license_name}")

    print("License policy check passed.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
