#!/usr/bin/env python3
# -------------------------------------------------------------------------------------------------
#  Copyright (C) 2015-2026 Nautech Systems Pty Ltd. All rights reserved.
#  https://nautechsystems.io
#
#  Licensed under the GNU Lesser General Public License Version 3.0 (the "License");
#  You may not use this file except in compliance with the License.
#  You may obtain a copy of the License at https://www.gnu.org/licenses/lgpl-3.0.en.html
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# -------------------------------------------------------------------------------------------------
"""
Validate the immutable metadata and contents of a PAPI release wheel.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import re
import stat
import sys
import zipfile
from email.parser import Parser
from pathlib import Path
from typing import Any


EXPECTED_NAME = "nautilus-trader-papi"
EXPECTED_VERSION = "2.0.0rc6"
EXPECTED_REQUIRES_PYTHON = ">=3.14,<3.15"
EXPECTED_HOMEPAGE = "https://github.com/imbingox/nautilus_trader"
EXPECTED_PLATFORM = "manylinux_2_34_x86_64"
EXPECTED_OS_CLASSIFIER = "Operating System :: POSIX :: Linux"
MAX_PYPI_FILE_SIZE = 100 * 1024 * 1024
MAX_ARCHIVE_CONTENT_SIZE = 1024 * 1024 * 1024
WHEEL_PATTERN = re.compile(
    rf"^nautilus_trader_papi-(?P<version>[A-Za-z0-9._]+)-"
    rf"(?P<python>cp314)-(?P<abi>cp314)-(?P<platform>{EXPECTED_PLATFORM})\.whl$",
)
TEXT_SUFFIXES = {".json", ".md", ".py", ".pyi", ".toml", ".txt"}
FORBIDDEN_PARTS = {".git", ".venv", "target", "test_data"}
FORBIDDEN_TEXT = (
    b"-----BEGIN " + b"PRIVATE KEY-----",
    b"OfflinePapiSecret",
)
REQUIREMENT_NAME_PATTERN = re.compile(r"^\s*([A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?)")


def _sha256(path: Path) -> str:
    """
    Return the SHA-256 digest for ``path``.
    """
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _require(condition: object, message: str) -> None:
    """
    Stop validation with ``message`` when ``condition`` is false.
    """
    if not condition:
        raise ValueError(message)


def _single_member(names: list[str], suffix: str) -> str:
    """
    Return the unique archive member ending in ``suffix``.
    """
    matches = [name for name in names if name.endswith(suffix)]
    _require(len(matches) == 1, f"Expected exactly one {suffix} member, found {matches}")
    return matches[0]


def _normalized_requirement_name(value: str) -> str:
    """
    Return the normalized distribution name from a ``Requires-Dist`` value.
    """
    match = REQUIREMENT_NAME_PATTERN.match(value)
    _require(match is not None, f"Invalid Requires-Dist value: {value}")
    return re.sub(r"[-_.]+", "-", match.group(1)).lower()


def _normalized_python_specifier(value: str) -> str:
    """
    Return a Python version specifier without insignificant whitespace.
    """
    return "".join(value.split())


def validate_wheel(path: Path) -> dict[str, Any]:
    """
    Validate ``path`` and return the immutable wheel facts.
    """
    _require(path.is_file(), f"Wheel does not exist: {path}")
    match = WHEEL_PATTERN.fullmatch(path.name)
    _require(match is not None, f"Unexpected release wheel filename: {path.name}")
    _require(
        match.group("version") == EXPECTED_VERSION,
        "Wheel version is not frozen release version",
    )
    size = path.stat().st_size
    _require(
        size <= MAX_PYPI_FILE_SIZE,
        f"Wheel is {size} bytes; new PyPI projects default to a 100 MiB file limit",
    )

    with zipfile.ZipFile(path) as archive:
        infos = archive.infolist()
        names = [info.filename for info in infos]
        _require(len(names) == len(set(names)), "Wheel contains duplicate archive paths")
        _require(
            sum(info.file_size for info in infos) <= MAX_ARCHIVE_CONTENT_SIZE,
            "Wheel expands beyond the 1 GiB release limit",
        )
        corrupt = archive.testzip()
        _require(corrupt is None, f"Wheel CRC validation failed for {corrupt}")

        for info in infos:
            member = Path(info.filename)
            _require(
                not member.is_absolute() and ".." not in member.parts,
                "Wheel has unsafe paths",
            )
            _require(
                not FORBIDDEN_PARTS.intersection(member.parts),
                f"Forbidden wheel path: {member}",
            )
            mode = info.external_attr >> 16
            _require(not stat.S_ISLNK(mode), f"Wheel contains a symbolic link: {member}")
            if member.suffix in TEXT_SUFFIXES and info.file_size <= 2 * 1024 * 1024:
                payload = archive.read(info)
                for marker in FORBIDDEN_TEXT:
                    _require(marker not in payload, f"Wheel contains forbidden text in {member}")

        metadata_name = _single_member(names, ".dist-info/METADATA")
        wheel_name = _single_member(names, ".dist-info/WHEEL")
        _single_member(names, ".dist-info/RECORD")
        metadata = Parser().parsestr(archive.read(metadata_name).decode("utf-8"))
        wheel_metadata = Parser().parsestr(archive.read(wheel_name).decode("utf-8"))

        _require(metadata["Name"] == EXPECTED_NAME, "METADATA Name does not match the fork")
        _require(metadata["Version"] == EXPECTED_VERSION, "METADATA Version is not frozen")
        _require(
            _normalized_python_specifier(metadata["Requires-Python"] or "")
            == EXPECTED_REQUIRES_PYTHON,
            "METADATA Requires-Python does not match CPython 3.14 scope",
        )
        _require(
            metadata["Home-Page"] == EXPECTED_HOMEPAGE,
            "METADATA Home-Page does not identify the fork",
        )
        _require(
            metadata["Summary"] is not None and "Unofficial" in metadata["Summary"],
            "METADATA must identify the distribution as unofficial",
        )
        classifiers = metadata.get_all("Classifier", [])
        _require(
            EXPECTED_OS_CLASSIFIER in classifiers
            and "Operating System :: OS Independent" not in classifiers,
            "METADATA operating system classifier does not match the Linux-only release",
        )
        requirements = metadata.get_all("Requires-Dist", [])
        _require(
            all(_normalized_requirement_name(value) != "nautilus-trader" for value in requirements),
            "Wheel must not depend on the conflicting official distribution",
        )
        _require(
            wheel_metadata.get_all("Tag", []) == [f"cp314-cp314-{EXPECTED_PLATFORM}"],
            "WHEEL Tag does not match the frozen platform",
        )

        required_members = (
            "nautilus_trader/__init__.py",
            "nautilus_trader/_libnautilus.pyi",
            "nautilus_trader/adapters/binance_papi/__init__.py",
            "nautilus_trader/adapters/binance_papi/__init__.pyi",
        )
        for required in required_members:
            _require(required in names, f"Wheel is missing {required}")
        _require(
            any(
                name.startswith("nautilus_trader/_libnautilus.cpython-314-")
                and name.endswith(".so")
                for name in names
            ),
            "Wheel is missing the CPython 3.14 native extension",
        )
        _require(
            any(name.endswith(".dist-info/licenses/LICENSE") for name in names),
            "Wheel is missing its LGPL license file",
        )

    return {
        "archive_checks": "passed",
        "complete": False,
        "distribution": EXPECTED_NAME,
        "version": EXPECTED_VERSION,
        "python_tag": "cp314",
        "abi_tag": "cp314",
        "platform_tag": EXPECTED_PLATFORM,
        "filename": path.name,
        "size": size,
        "sha256": _sha256(path),
    }


def main() -> int:
    """
    Validate the command-line wheel and optionally write an incomplete report.
    """
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("wheel", type=Path)
    parser.add_argument("--report", type=Path)
    args = parser.parse_args()

    try:
        report = validate_wheel(args.wheel.resolve())
        if args.report is not None:
            args.report.write_text(
                json.dumps(report, indent=2, sort_keys=True) + "\n",
                encoding="utf-8",
            )
    except (OSError, UnicodeError, ValueError, zipfile.BadZipFile) as e:
        print(f"PAPI wheel validation failed: {e}", file=sys.stderr)  # noqa: T201
        return 1

    print(  # noqa: T201
        f"Validated {report['filename']} ({report['size']} bytes, sha256:{report['sha256']})",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
